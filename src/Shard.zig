//! Single-thread Shared-Nothing `std.Io` shard scheduler and full `VTable` orchestrator.
//!
//! Design invariants (AGENTS.md §2, §3, §4, §5):
//! - Strictly single-thread affine (`tls_current_shard == self` O(1) check on every
//!   `VTable` entry except `futexWake`, which may be invoked from any thread).
//! - Zero platform conditional scatter: all OS-specific multiplexer and socket
//!   state machines reside in `LinuxIo` / `WindowsIo`.
//! - Bounded `lifo_slot` (`max_lifo_streak = 3`) paired with FIFO `ready_queue`
//!   maximizes L1/L2 cache locality on same-Shard ping-pong wakeups without
//!   starving queued tasks, while `steps_since_poll` throttles nonblocking I/O
//!   polls across both direct Fiber switches and `idle_fiber` transitions.

const builtin = @import("builtin");
const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const assert = std.debug.assert;
const net = Io.net;
const File = Io.File;
const Dir = Io.Dir;
const Threaded = Io.Threaded;

const TimerHeap = @import("TimerHeap.zig");
const Mailbox = @import("Mailbox.zig");
const ResumeLane = Mailbox.ResumeLane;
const fiber = @import("fiber.zig");
const BlockingPool = @import("BlockingPool.zig");
const FutexTable = @import("FutexTable.zig");
const ShardMesh = @import("ShardMesh.zig");
const Acceptor = @import("Acceptor.zig").Acceptor;

const PlatformIo = switch (builtin.os.tag) {
    .linux => @import("LinuxIo.zig"),
    .windows => @import("WindowsIo.zig"),
    else => |os| @compileError("unsupported OS: " ++ @tagName(os)),
};

const Shard = @This();

const io_poll_cadence: u32 = 64;
const max_lifo_streak: u8 = 3;

pub const InitOptions = struct {
    /// Per-Fiber usable stack size in bytes (guarded on Linux, reserved on Windows).
    stack_size: usize = fiber.default_stack_size,
    /// Maximum idle Fiber stacks cached per Shard before watermark eviction.
    max_cached_stacks: usize = fiber.default_max_cached_stacks,
    /// Upper bound on file descriptor index per Shard on Linux.
    max_fds: u32 = 65536,
    /// Worker count for private `BlockingPool` in standalone single-Shard mode.
    blocking_threads: ?usize = null,
    /// Admission queue bound for private `BlockingPool` in standalone mode.
    blocking_queue_capacity: ?usize = null,
    /// Optional process environment block for child process spawning.
    environ: std.process.Environ = .empty,
};

pub const SharedServices = struct {
    shard_id: usize,
    shard_count: usize,
    runtime_ptr: ?*anyopaque,
    blocking_pool: *BlockingPool,
    futex_table: *FutexTable,
    mesh: ?*ShardMesh,
    acceptor: ?*Acceptor,
    stderr_mutex: *Io.Mutex,
};

pub const AttachError = fiber.InitError;

pub const InitError = Allocator.Error ||
    AttachError ||
    PlatformIo.InitError ||
    std.Thread.SpawnError;

threadlocal var tls_current_shard: ?*Shard = null;

pub inline fn current() ?*Shard {
    return tls_current_shard;
}

const CancelProtectionState = packed struct(u2) {
    user: Io.CancelProtection = .unblocked,
    acknowledged: bool = false,

    fn effective(self: CancelProtectionState) Io.CancelProtection {
        return if (self.user == .unblocked and !self.acknowledged) .unblocked else .blocked;
    }

    fn acknowledge(self: *CancelProtectionState) void {
        assert(!self.acknowledged);
        self.acknowledged = true;
    }

    fn recancel(self: *CancelProtectionState) void {
        assert(self.acknowledged);
        self.acknowledged = false;
    }
};

pub const CancelHook = struct {
    ctx: *anyopaque,
    trigger: *const fn (*Shard, *anyopaque) void,
};

const TaskKind = enum {
    main,
    future,
    group,
};

pub const Task = struct {
    raw: ?*fiber.RawFiber = null,
    kind: TaskKind,
    ready_next: ?*Task = null,
    queued_ready: bool = false,
    finished: bool = false,
    cancel_requested: bool = false,
    cancel_protection: CancelProtectionState = .{},
    cancel_hook: ?CancelHook = null,

    start_fn: union {
        none: void,
        future: *const fn (context: *const anyopaque, result: *anyopaque) void,
        group: *const fn (context: *const anyopaque) void,
    } = .{ .none = {} },

    // `Future` linkage & storage
    awaiter: ?*Task = null,
    context_ptr: [*]u8 = undefined,
    context_alloc: ?[]u8 = null,
    context_align: Alignment = .@"1",
    result_ptr: [*]u8 = undefined,
    result_alloc: ?[]u8 = null,
    result_align: Alignment = .@"1",

    // `Group` intrusive doubly-linked list
    group_owner: ?*Io.Group = null,
    group_prev: ?*Task = null,
    group_next: ?*Task = null,

    pub fn shouldCancelNow(self: *const Task) bool {
        return self.cancel_requested and self.cancel_protection.effective() == .unblocked;
    }

    pub fn acknowledgeCancel(self: *Task) void {
        self.cancel_protection.acknowledge();
    }

    fn requestCancel(self: *Task, shard: *Shard) void {
        self.cancel_requested = true;
        if (self.cancel_protection.effective() == .unblocked) {
            if (self.cancel_hook) |hook| {
                self.cancel_hook = null;
                hook.trigger(shard, hook.ctx);
            }
        }
    }
};

const ReadyQueue = struct {
    head: ?*Task = null,
    tail: ?*Task = null,

    fn push(self: *ReadyQueue, task: *Task) void {
        if (task.queued_ready) return;
        self.pushPreMarked(task);
        task.queued_ready = true;
    }

    fn pushPreMarked(self: *ReadyQueue, task: *Task) void {
        assert(task.ready_next == null);
        if (self.tail) |t| {
            t.ready_next = task;
        } else {
            self.head = task;
        }
        self.tail = task;
    }

    fn pop(self: *ReadyQueue) ?*Task {
        const t = self.head orelse return null;
        assert(t.queued_ready);
        self.head = t.ready_next;
        if (self.head == null) self.tail = null;
        t.ready_next = null;
        t.queued_ready = false;
        return t;
    }
};

gpa: Allocator,
id: usize,
shard_count: usize,
runtime_ptr: ?*anyopaque,

driver: PlatformIo = undefined,
timers: TimerHeap = .init,
stack_pool: fiber.StackPool,
resume_lane: ResumeLane = .init,

blocking_pool: *BlockingPool,
owned_blocking_pool: ?*BlockingPool = null,
futex_table: *FutexTable,
owned_futex_table: ?*FutexTable = null,
mesh: ?*ShardMesh = null,
stderr_mutex: *Io.Mutex,
owned_stderr_mutex: Io.Mutex = .init,

thread_ctx: fiber.ThreadContext = undefined,
idle_fiber: *fiber.RawFiber = undefined,
main_task: Task = .{ .kind = .main },
current_task: *Task = undefined,
lifo_slot: ?*Task = null,
lifo_streak: u8 = 0,
ready_queue: ReadyQueue = .{},
pending_recycle: ?*fiber.RawFiber = null,
active_tasks: usize = 0,
steps_since_poll: u32 = 0,
in_idle_fiber: bool = false,
idle_exit: bool = false,
attached_to_thread: bool = false,

stopping: std.atomic.Value(bool) = .init(false),
stop_node_queued: std.atomic.Value(bool) = .init(false),
stop_node: ResumeLane.Node = .{ .callback = onStopResumeNode },

csprng: Threaded.Csprng = .uninitialized,

stderr_writer: File.Writer = undefined,
stderr_mode: Io.Terminal.Mode = .no_color,
stderr_initialized: bool = false,
stderr_locker: ?*Task = null,
stderr_lock_count: usize = 0,

/// Initializes a standalone single-Shard `std.Io` backend bound to the calling OS thread.
/// Address-pinned (`*Shard` must not be moved after `init` returns).
pub fn init(self: *Shard, gpa: Allocator, options: InitOptions) InitError!void {
    if (tls_current_shard != null) {
        @panic("islet.Shard.init: calling thread already owns an active Shard");
    }

    const owned_pool = try gpa.create(BlockingPool);
    errdefer gpa.destroy(owned_pool);
    try owned_pool.init(gpa, .{
        .thread_count = options.blocking_threads,
        .queue_capacity = options.blocking_queue_capacity,
        .environ = options.environ,
    });
    errdefer owned_pool.deinit(gpa);

    const owned_futex = try gpa.create(FutexTable);
    errdefer gpa.destroy(owned_futex);
    owned_futex.* = .init;

    try self.initUnattached(gpa, options, .{
        .shard_id = 0,
        .shard_count = 1,
        .runtime_ptr = null,
        .blocking_pool = owned_pool,
        .futex_table = owned_futex,
        .mesh = null,
        .acceptor = null,
        .stderr_mutex = &self.owned_stderr_mutex,
    });
    self.owned_blocking_pool = owned_pool;
    self.owned_futex_table = owned_futex;
    errdefer {
        self.owned_blocking_pool = null;
        self.owned_futex_table = null;
        self.deinitUnattached();
    }

    try self.attachCurrentThread();
}

/// Package-internal phase-1 initializer used by `Runtime` before spawning Shard threads.
pub fn initUnattached(
    self: *Shard,
    gpa: Allocator,
    options: InitOptions,
    services: SharedServices,
) (Allocator.Error || PlatformIo.InitError)!void {
    self.* = .{
        .gpa = gpa,
        .id = services.shard_id,
        .shard_count = services.shard_count,
        .runtime_ptr = services.runtime_ptr,
        .stack_pool = fiber.StackPool.init(options.stack_size, options.max_cached_stacks),
        .blocking_pool = services.blocking_pool,
        .futex_table = services.futex_table,
        .mesh = services.mesh,
        .stderr_mutex = services.stderr_mutex,
    };
    self.stop_node.ctx = self;

    // Initialize `driver` in-place after `self.*` is address-pinned!
    try self.driver.init(
        gpa,
        options.max_fds,
        services.acceptor,
        services.shard_id,
        services.blocking_pool,
        &self.resume_lane,
        self,
        onDriverWakeTask,
    );
}

/// Package-internal phase-2 binder invoked on the Shard's dedicated OS thread.
pub fn attachCurrentThread(self: *Shard) AttachError!void {
    if (tls_current_shard != null) {
        @panic("islet.Shard: one OS thread cannot run multiple Shards");
    }
    var tctx = try fiber.ThreadContext.init();
    errdefer tctx.deinit();

    const idle_raw = try self.stack_pool.acquire(self.gpa);
    self.thread_ctx = tctx;
    idle_raw.arm(idleFiberEntry, self);
    idle_raw.exit_target = &self.thread_ctx.ctx;
    self.idle_fiber = idle_raw;
    self.main_task = .{ .kind = .main };
    self.current_task = &self.main_task;
    self.attached_to_thread = true;
    tls_current_shard = self;
}

/// Detaches the calling OS thread after all tasks on this Shard have drained.
pub fn detachCurrentThread(self: *Shard) void {
    self.assertAffinity();
    if (self.current_task != &self.main_task) {
        @panic("islet.Shard.deinit: must be called from the Shard's root context");
    }
    if (self.active_tasks != 0) {
        @panic("islet.Shard.deinit: active Future or Group tasks were not awaited or canceled");
    }

    self.timers.cancelAll();
    self.drainIncoming();

    self.idle_exit = true;
    fiber.switchContext(&self.thread_ctx.ctx, &self.idle_fiber.ctx);
    self.reclaimPendingFiber();

    self.stack_pool.release(self.gpa, self.idle_fiber);
    self.attached_to_thread = false;
    tls_current_shard = null;
    self.thread_ctx.deinit();
}

pub fn deinitUnattached(self: *Shard) void {
    assert(!self.attached_to_thread);
    self.timers.deinit(self.gpa);
    self.stack_pool.deinit(self.gpa);
    self.driver.deinit(self.gpa);
}

pub fn deinit(self: *Shard) void {
    if (self.attached_to_thread) {
        self.detachCurrentThread();
    }
    const gpa = self.gpa;
    const maybe_pool = self.owned_blocking_pool;
    const maybe_futex = self.owned_futex_table;
    self.owned_blocking_pool = null;
    self.owned_futex_table = null;

    self.deinitUnattached();

    if (maybe_pool) |pool| {
        pool.deinit(gpa);
        gpa.destroy(pool);
    }
    if (maybe_futex) |ft| {
        gpa.destroy(ft);
    }
    self.* = undefined;
}

pub fn index(self: *const Shard) usize {
    return self.id;
}

pub fn count(self: *const Shard) usize {
    return self.shard_count;
}

pub fn isStopping(self: *const Shard) bool {
    return self.stopping.load(.acquire);
}

/// Thread-safe: signals this Shard to enter stopping state via its reserved
/// control `ResumeLane` node without blocking on business admission queues.
pub fn requestStop(self: *Shard) void {
    self.stopping.store(true, .seq_cst);
    if (!self.stop_node_queued.swap(true, .acq_rel)) {
        self.resume_lane.push(&self.stop_node);
        self.notifyFromAnyThread();
    }
}

pub fn notifyFromAnyThread(self: *Shard) void {
    self.driver.notifyFromAnyThread();
}

fn notifyOpaque(ctx: ?*anyopaque) void {
    const self: *Shard = @ptrCast(@alignCast(ctx.?));
    self.notifyFromAnyThread();
}

fn onDriverWakeTask(owner_ctx: *anyopaque, task_ptr: *anyopaque) void {
    const self: *Shard = @ptrCast(@alignCast(owner_ctx));
    const task: *Task = @ptrCast(@alignCast(task_ptr));
    self.enqueueReady(task);
}

fn onStopResumeNode(node: *ResumeLane.Node) void {
    const self: *Shard = @ptrCast(@alignCast(node.ctx.?));
    self.driver.onStopRequested();
}

/// Returns the `std.Io` instance bound to this Shard.
/// Must be called on this Shard's owning OS thread (`@panic` otherwise).
pub fn io(self: *Shard) Io {
    self.assertAffinity();
    return .{
        .userdata = self,
        .vtable = &vtable,
    };
}

inline fn ioUnchecked(self: *Shard) Io {
    return .{
        .userdata = self,
        .vtable = &vtable,
    };
}

pub inline fn assertAffinity(self: *const Shard) void {
    if (tls_current_shard != self) {
        @panic("islet.Shard: std.Io used off its owning Shard thread");
    }
}

inline fn fromUserdata(userdata: ?*anyopaque) *Shard {
    const self: *Shard = @ptrCast(@alignCast(userdata.?));
    self.assertAffinity();
    return self;
}

// ── Cooperative Fiber Scheduler (Bounded LIFO Slot + FIFO ReadyQueue) ───────

inline fn contextOfTask(self: *Shard, task: *Task) *fiber.ExecContext {
    return if (task == &self.main_task) &self.thread_ctx.ctx else &task.raw.?.ctx;
}

fn reclaimPendingFiber(self: *Shard) void {
    if (self.pending_recycle) |raw| {
        self.pending_recycle = null;
        self.stack_pool.release(self.gpa, raw);
    }
}

pub fn enqueueReady(self: *Shard, task: *Task) void {
    assert(!task.finished);
    if (!self.in_idle_fiber and task == self.current_task) return;
    if (task.queued_ready) return;

    // Same-Shard Fiber wakeup: place in `lifo_slot` for hot L1/L2 cache locality
    // while displacing any prior `lifo_slot` occupant into FIFO `ready_queue`.
    if (!self.in_idle_fiber and self.lifo_streak < max_lifo_streak) {
        task.queued_ready = true;
        if (self.lifo_slot) |displaced| {
            self.ready_queue.pushPreMarked(displaced);
        }
        self.lifo_slot = task;
        return;
    }

    self.ready_queue.push(task);
}

fn popNextReady(self: *Shard) ?*Task {
    if (self.lifo_streak < max_lifo_streak) {
        if (self.lifo_slot) |t| {
            self.lifo_slot = null;
            t.queued_ready = false;
            self.lifo_streak += 1;
            return t;
        }
    }
    if (self.ready_queue.pop()) |t| {
        self.lifo_streak = 0;
        return t;
    }
    if (self.lifo_slot) |t| {
        self.lifo_slot = null;
        t.queued_ready = false;
        self.lifo_streak = 0;
        return t;
    }
    return null;
}

inline fn hasReadyTasks(self: *const Shard) bool {
    return self.lifo_slot != null or self.ready_queue.head != null;
}

/// Suspends `self.current_task` and switches to the next ready `Task` (or
/// `idle_fiber` when no tasks are ready or `io_poll_cadence` is reached).
pub fn suspendCurrent(self: *Shard) void {
    const prev_task = self.current_task;
    const prev_ctx = self.contextOfTask(prev_task);

    if (self.steps_since_poll < io_poll_cadence) {
        if (self.popNextReady()) |next_task| {
            self.steps_since_poll += 1;
            if (next_task == prev_task) return;
            self.current_task = next_task;
            fiber.switchContext(prev_ctx, self.contextOfTask(next_task));
            self.reclaimPendingFiber();
            return;
        }
    }

    fiber.switchContext(prev_ctx, &self.idle_fiber.ctx);
    self.reclaimPendingFiber();
}

fn dispatchFromIdle(self: *Shard, next_task: *Task) void {
    self.in_idle_fiber = false;
    self.current_task = next_task;
    fiber.switchContext(&self.idle_fiber.ctx, self.contextOfTask(next_task));
    self.in_idle_fiber = true;
    self.reclaimPendingFiber();
}

fn idleFiberEntry(arg: *anyopaque) void {
    const self: *Shard = @ptrCast(@alignCast(arg));
    self.in_idle_fiber = true;
    self.reclaimPendingFiber();

    while (!self.idle_exit) {
        // Fast path: dispatch ready tasks within `io_poll_cadence` without clock/poller syscalls.
        if (self.steps_since_poll < io_poll_cadence) {
            if (self.popNextReady()) |next_task| {
                self.steps_since_poll += 1;
                self.dispatchFromIdle(next_task);
                continue;
            }
        } else if (self.hasReadyTasks()) {
            // `io_poll_cadence` reached while CPU tasks remain ready: run nonblocking poll(0).
            self.steps_since_poll = 0;
            self.drainIncoming();
            if (self.timers.hasActive()) _ = self.timers.fireExpired(self.awakeNowNs());
            _ = self.driver.poll(0);
            if (self.popNextReady()) |next_task| {
                self.steps_since_poll = 1;
                self.dispatchFromIdle(next_task);
                continue;
            }
        }

        self.drainIncoming();
        var pre_poll_now: ?i96 = null;
        if (self.timers.hasActive()) {
            const now_ns = self.awakeNowNs();
            pre_poll_now = now_ns;
            _ = self.timers.fireExpired(now_ns);
        }

        if (self.popNextReady()) |next_task| {
            self.steps_since_poll = 1;
            self.dispatchFromIdle(next_task);
            continue;
        }

        // Scavenge cold physical stack pages and excess idle timer slots before sleeping.
        self.stack_pool.scavengeIdle();
        self.timers.scavengeIdle(self.gpa);

        // Arm coalesced doorbell BEFORE final `.seq_cst` emptiness check.
        self.driver.armDoorbell();
        if (!self.allIncomingEmptySeqCst()) {
            self.driver.disarmDoorbell();
            continue;
        }

        const wait_ns: ?i96 = if (self.timers.peekDeadlineNs()) |dl|
            dl -| (pre_poll_now orelse self.awakeNowNs())
        else
            null;

        self.steps_since_poll = 0;
        _ = self.driver.poll(wait_ns);
        self.driver.disarmDoorbell();

        self.drainIncoming();
        if (self.timers.hasActive()) _ = self.timers.fireExpired(self.awakeNowNs());
        if (self.popNextReady()) |next_task| {
            self.steps_since_poll = 1;
            self.dispatchFromIdle(next_task);
        }
    }
    self.in_idle_fiber = false;
    self.current_task = &self.main_task;
}

fn allIncomingEmptySeqCst(self: *const Shard) bool {
    if (!self.resume_lane.isEmptySeqCst()) return false;
    if (self.mesh) |m| {
        if (!m.isEmptyForSeqCst(self.id)) return false;
    }
    if (!self.driver.isAcceptorEmptySeqCst()) return false;
    return true;
}

pub fn drainIncoming(self: *Shard) void {
    _ = self.resume_lane.drain();
    if (self.mesh) |m| {
        _ = m.drainFor(self.id, self, onMeshFutexWake);
    }
    self.driver.drainAcceptor();
}

fn onMeshFutexWake(ctx: ?*anyopaque, waiter: *FutexTable.Waiter) void {
    const self: *Shard = @ptrCast(@alignCast(ctx.?));
    deliverFutexWaiterOnOwner(self, waiter);
}

// ── VTable: Cancellation & Protection ───────────────────────────────────────

pub fn checkCancelPoint(self: *Shard) Io.Cancelable!void {
    const task = self.current_task;
    if (task.shouldCancelNow()) {
        task.acknowledgeCancel();
        return error.Canceled;
    }
}

fn vtableCrashHandler(userdata: ?*anyopaque) void {
    const self = fromUserdata(userdata);
    const task = self.current_task;
    task.cancel_requested = true;
    task.cancel_protection = .{ .user = .blocked, .acknowledged = true };
}

fn vtableRecancel(userdata: ?*anyopaque) void {
    const self = fromUserdata(userdata);
    self.current_task.cancel_protection.recancel();
}

fn vtableSwapCancelProtection(userdata: ?*anyopaque, new: Io.CancelProtection) Io.CancelProtection {
    const self = fromUserdata(userdata);
    const old = self.current_task.cancel_protection.user;
    self.current_task.cancel_protection.user = new;
    return old;
}

fn vtableCheckCancel(userdata: ?*anyopaque) Io.Cancelable!void {
    const self = fromUserdata(userdata);
    return self.checkCancelPoint();
}

// ── VTable: `Future` (`async` / `concurrent` / `await` / `cancel`) ──────────

const TaskRunArg = struct {
    shard: *Shard,
    task: *Task,
};

fn spawnTaskFiber(
    self: *Shard,
    kind: TaskKind,
    result_len: usize,
    result_alignment: Alignment,
    context: []const u8,
    context_alignment: Alignment,
) Io.ConcurrentError!*Task {
    const raw = self.stack_pool.acquire(self.gpa) catch return error.ConcurrencyUnavailable;
    errdefer self.stack_pool.release(self.gpa, raw);

    const header_base = @intFromPtr(&raw.header);
    const header_end = header_base + fiber.header_capacity;

    const task_addr = Alignment.of(Task).forward(header_base);
    const run_arg_addr = Alignment.of(TaskRunArg).forward(task_addr + @sizeOf(Task));
    var cursor = run_arg_addr + @sizeOf(TaskRunArg);

    var result_alloc: ?[]u8 = null;
    errdefer if (result_alloc) |s| self.gpa.rawFree(s, result_alignment, @returnAddress());
    var result_ptr: [*]u8 = undefined;
    if (result_len == 0) {
        result_ptr = @ptrFromInt(result_alignment.forward(header_base));
    } else {
        const inline_res = result_alignment.forward(cursor);
        if (inline_res + result_len <= header_end) {
            result_ptr = @ptrFromInt(inline_res);
            cursor = inline_res + result_len;
        } else {
            const buf = self.gpa.rawAlloc(result_len, result_alignment, @returnAddress()) orelse
                return error.ConcurrencyUnavailable;
            result_alloc = buf[0..result_len];
            result_ptr = buf;
        }
    }

    var context_alloc: ?[]u8 = null;
    errdefer if (context_alloc) |s| self.gpa.rawFree(s, context_alignment, @returnAddress());
    var context_ptr: [*]u8 = undefined;
    if (context.len == 0) {
        context_ptr = @ptrFromInt(context_alignment.forward(header_base));
    } else {
        const inline_ctx = context_alignment.forward(cursor);
        if (inline_ctx + context.len <= header_end) {
            context_ptr = @ptrFromInt(inline_ctx);
        } else {
            const buf = self.gpa.rawAlloc(context.len, context_alignment, @returnAddress()) orelse
                return error.ConcurrencyUnavailable;
            context_alloc = buf[0..context.len];
            context_ptr = buf;
        }
        @memcpy(context_ptr[0..context.len], context);
    }

    const task: *Task = @ptrFromInt(task_addr);
    task.* = .{
        .raw = raw,
        .kind = kind,
        .context_ptr = context_ptr,
        .context_alloc = context_alloc,
        .context_align = context_alignment,
        .result_ptr = result_ptr,
        .result_alloc = result_alloc,
        .result_align = result_alignment,
    };

    const run_arg: *TaskRunArg = @ptrFromInt(run_arg_addr);
    run_arg.* = .{ .shard = self, .task = task };
    raw.arm(taskFiberEntry, run_arg);

    return task;
}

fn selectExitTarget(self: *Shard) *fiber.ExecContext {
    if (self.steps_since_poll < io_poll_cadence) {
        if (self.popNextReady()) |next_task| {
            self.steps_since_poll += 1;
            self.current_task = next_task;
            return self.contextOfTask(next_task);
        }
    }
    return &self.idle_fiber.ctx;
}

fn taskFiberEntry(arg: *anyopaque) void {
    const run_arg: *TaskRunArg = @ptrCast(@alignCast(arg));
    const self = run_arg.shard;
    const task = run_arg.task;
    self.reclaimPendingFiber();

    switch (task.kind) {
        .main => unreachable,
        .future => task.start_fn.future(task.context_ptr, task.result_ptr),
        .group => task.start_fn.group(task.context_ptr),
    }
    if (task.context_alloc) |ctx_slice| {
        self.gpa.rawFree(ctx_slice, task.context_align, @returnAddress());
        task.context_alloc = null;
    }
    task.finished = true;

    switch (task.kind) {
        .main => unreachable,
        .future => {
            if (task.awaiter) |awaiter| {
                task.awaiter = null;
                self.enqueueReady(awaiter);
            }
            // `Future` raw fiber stays alive until `vtableAwait` copies `result` out!
            task.raw.?.exit_target = self.selectExitTarget();
        },
        .group => {
            self.unlinkGroupTask(task);
            assert(self.active_tasks > 0);
            self.active_tasks -= 1;

            const raw = task.raw.?;
            const next_ctx = self.selectExitTarget();
            assert(self.pending_recycle == null);
            self.pending_recycle = raw;
            raw.exit_target = next_ctx;
        },
    }
}

fn vtableAsync(
    userdata: ?*anyopaque,
    result: []u8,
    result_alignment: Alignment,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) ?*Io.AnyFuture {
    return vtableConcurrent(
        userdata,
        result.len,
        result_alignment,
        context,
        context_alignment,
        start,
    ) catch {
        const self = fromUserdata(userdata);
        const cur = self.current_task;
        const saved_req = cur.cancel_requested;
        const saved_prot = cur.cancel_protection;
        cur.cancel_requested = false;
        cur.cancel_protection = .{};
        defer {
            cur.cancel_requested = saved_req;
            cur.cancel_protection = saved_prot;
        }
        start(context.ptr, result.ptr);
        return null;
    };
}

fn vtableConcurrent(
    userdata: ?*anyopaque,
    result_len: usize,
    result_alignment: Alignment,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) Io.ConcurrentError!*Io.AnyFuture {
    const self = fromUserdata(userdata);
    const task = try self.spawnTaskFiber(.future, result_len, result_alignment, context, context_alignment);
    task.start_fn = .{ .future = start };
    self.active_tasks += 1;
    self.enqueueReady(task);
    return @ptrCast(task);
}

fn vtableAwait(
    userdata: ?*anyopaque,
    any_future: *Io.AnyFuture,
    result: []u8,
    result_alignment: Alignment,
) void {
    _ = result_alignment;
    const self = fromUserdata(userdata);
    const child: *Task = @ptrCast(@alignCast(any_future));
    assert(child.kind == .future);

    const cur = self.current_task;
    if (!child.finished) {
        const AwaitCancelState = struct {
            parent: *Task,
            child_task: *Task,
            propagated: bool = false,
            child_pre_ack: bool = false,

            fn propagate(s: *Shard, st: *@This()) void {
                if (st.propagated or st.child_task.finished) return;
                if (st.parent.shouldCancelNow()) {
                    st.parent.acknowledgeCancel();
                    st.propagated = true;
                    st.child_pre_ack = st.child_task.cancel_protection.acknowledged;
                    st.child_task.requestCancel(s);
                }
            }

            fn onCancel(s: *Shard, raw_ctx: *anyopaque) void {
                const st: *@This() = @ptrCast(@alignCast(raw_ctx));
                propagate(s, st);
            }
        };

        var cancel_st: AwaitCancelState = .{
            .parent = cur,
            .child_task = child,
        };
        AwaitCancelState.propagate(self, &cancel_st);

        if (!child.finished) {
            cur.cancel_hook = .{ .ctx = &cancel_st, .trigger = AwaitCancelState.onCancel };
            defer cur.cancel_hook = null;

            while (!child.finished) {
                child.awaiter = cur;
                self.suspendCurrent();
            }
        }

        // Preserve single-acknowledgement cancellation semantics (`std.Io.Threaded.await`):
        // if `child` did not consume the cancel we propagated, restore it on `cur`.
        if (cancel_st.propagated) {
            const child_consumed = !cancel_st.child_pre_ack and child.cancel_protection.acknowledged;
            if (!child_consumed) {
                cur.cancel_protection.recancel();
            }
        }
    }

    if (result.len > 0) {
        @memcpy(result, child.result_ptr[0..result.len]);
    }
    if (child.result_alloc) |res_slice| {
        self.gpa.rawFree(res_slice, child.result_align, @returnAddress());
        child.result_alloc = null;
    }
    const raw = child.raw.?;
    assert(self.active_tasks > 0);
    self.active_tasks -= 1;
    self.stack_pool.release(self.gpa, raw);
}

fn vtableCancel(
    userdata: ?*anyopaque,
    any_future: *Io.AnyFuture,
    result: []u8,
    result_alignment: Alignment,
) void {
    const self = fromUserdata(userdata);
    const child: *Task = @ptrCast(@alignCast(any_future));
    assert(child.kind == .future);
    child.requestCancel(self);
    const prev = vtableSwapCancelProtection(self, .blocked);
    defer _ = vtableSwapCancelProtection(self, prev);
    vtableAwait(userdata, any_future, result, result_alignment);
}

// ── VTable: `Group` (`groupAsync` / `groupConcurrent` / `groupAwait` / `groupCancel`) ──

const group_cancel_bit: usize = 1;
const group_ptr_mask: usize = ~@as(usize, 3);

fn linkGroupTask(g: *Io.Group, task: *Task) void {
    task.group_owner = g;
    const old_head: ?*Task = @ptrCast(@alignCast(g.token.raw));
    task.group_prev = null;
    task.group_next = old_head;
    if (old_head) |h| h.group_prev = task;
    if ((g.state & group_cancel_bit) != 0) {
        task.cancel_requested = true;
    }
    g.token.store(task, .release);
}

fn unlinkGroupTask(self: *Shard, task: *Task) void {
    const g = task.group_owner.?;
    if (task.group_prev) |p| {
        p.group_next = task.group_next;
    } else {
        const next_head = task.group_next;
        if (next_head) |nh| {
            nh.group_prev = null;
            g.token.store(nh, .release);
        } else {
            const awaiter: ?*Task = @ptrFromInt(g.state & group_ptr_mask);
            g.state = 0;
            g.token.store(null, .release);
            if (awaiter) |aw| {
                aw.cancel_hook = null;
                self.enqueueReady(aw);
            }
        }
    }
    if (task.group_next) |n| {
        n.group_prev = task.group_prev;
    }
    task.group_prev = null;
    task.group_next = null;
    task.group_owner = null;
}

fn cancelGroupMembers(self: *Shard, g: *Io.Group) void {
    g.state |= group_cancel_bit;
    var cur: ?*Task = @ptrCast(@alignCast(g.token.raw));
    while (cur) |t| {
        const next = t.group_next;
        t.requestCancel(self);
        cur = next;
    }
}

fn vtableGroupAsync(
    userdata: ?*anyopaque,
    group: *Io.Group,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque) void,
) void {
    vtableGroupConcurrent(userdata, group, context, context_alignment, start) catch {
        const self = fromUserdata(userdata);
        const cur = self.current_task;
        const saved_req = cur.cancel_requested;
        const saved_prot = cur.cancel_protection;
        cur.cancel_requested = (group.state & group_cancel_bit) != 0;
        cur.cancel_protection = .{};
        defer {
            cur.cancel_requested = saved_req;
            cur.cancel_protection = saved_prot;
        }
        start(context.ptr);
    };
}

fn vtableGroupConcurrent(
    userdata: ?*anyopaque,
    group: *Io.Group,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque) void,
) Io.ConcurrentError!void {
    const self = fromUserdata(userdata);
    const task = try self.spawnTaskFiber(.group, 0, .@"1", context, context_alignment);
    task.start_fn = .{ .group = start };
    linkGroupTask(group, task);
    self.active_tasks += 1;
    self.enqueueReady(task);
}

fn vtableGroupAwait(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) Io.Cancelable!void {
    _ = token;
    const self = fromUserdata(userdata);
    if (group.token.load(.acquire) == null) return;

    const GroupAwaitState = struct {
        group: *Io.Group,
        canceled: bool = false,

        fn onCancel(s: *Shard, raw_ctx: *anyopaque) void {
            const st: *@This() = @ptrCast(@alignCast(raw_ctx));
            st.canceled = true;
            s.cancelGroupMembers(st.group);
        }
    };

    const cur = self.current_task;
    var wait_st: GroupAwaitState = .{ .group = group };
    if (cur.shouldCancelNow()) {
        wait_st.canceled = true;
        self.cancelGroupMembers(group);
    }

    assert((group.state & group_ptr_mask) == 0);
    assert((@intFromPtr(cur) & ~group_ptr_mask) == 0);
    group.state = @intFromPtr(cur) | (group.state & group_cancel_bit);
    cur.cancel_hook = .{ .ctx = &wait_st, .trigger = GroupAwaitState.onCancel };
    defer cur.cancel_hook = null;

    while (group.token.load(.acquire) != null) {
        self.suspendCurrent();
    }
    group.state = 0;

    if (wait_st.canceled) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
}

fn vtableGroupCancel(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) void {
    _ = token;
    const self = fromUserdata(userdata);
    if (group.token.load(.acquire) == null) {
        group.state = 0;
        return;
    }

    self.cancelGroupMembers(group);
    if (group.token.load(.acquire) == null) {
        group.state = 0;
        return;
    }

    const cur = self.current_task;
    assert((group.state & group_ptr_mask) == 0);
    group.state = @intFromPtr(cur) | group_cancel_bit;
    while (group.token.load(.acquire) != null) {
        self.suspendCurrent();
    }
    group.state = 0;
}

// ── VTable: Clock & `sleep` ─────────────────────────────────────────────────

pub fn awakeNowNs(self: *Shard) i96 {
    return self.driver.now(.awake).nanoseconds;
}

fn vtableNow(userdata: ?*anyopaque, clock: Io.Clock) Io.Timestamp {
    const self = fromUserdata(userdata);
    return self.driver.now(clock);
}

fn vtableClockResolution(userdata: ?*anyopaque, clock: Io.Clock) Io.Clock.ResolutionError!Io.Duration {
    const self = fromUserdata(userdata);
    return self.driver.clockResolution(clock);
}

inline fn resolveDeadlineNs(self: *Shard, timeout: Io.Timeout) ?i96 {
    return TimerHeap.resolveAwakeDeadlineNs(timeout, self, vtableNow);
}

fn vtableSleep(userdata: ?*anyopaque, timeout: Io.Timeout) Io.Cancelable!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    const maybe_deadline = self.resolveDeadlineNs(timeout);
    if (maybe_deadline) |dl| {
        if (dl <= self.awakeNowNs()) return;
    }

    const SleepState = struct {
        shard: *Shard,
        task: *Task,
        token: TimerHeap.Token = .inactive,
        done: bool = false,
        canceled: bool = false,

        fn onTimer(ctx: ?*anyopaque, outcome: TimerHeap.Outcome) void {
            const st: *@This() = @ptrCast(@alignCast(ctx.?));
            st.token = .inactive;
            st.done = true;
            if (outcome == .canceled) st.canceled = true;
            st.task.cancel_hook = null;
            st.shard.enqueueReady(st.task);
        }

        fn onCancel(s: *Shard, raw_ctx: *anyopaque) void {
            const st: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (st.done) return;
            _ = s.timers.cancel(st.token);
            st.token = .inactive;
            st.done = true;
            st.canceled = true;
            s.enqueueReady(st.task);
        }
    };

    const cur = self.current_task;
    var st: SleepState = .{ .shard = self, .task = cur };
    if (maybe_deadline) |dl| {
        st.token = self.timers.schedule(self.gpa, dl, &st, SleepState.onTimer) catch {
            while (self.awakeNowNs() < dl) {
                try self.checkCancelPoint();
                self.ready_queue.push(cur);
                self.suspendCurrent();
            }
            return;
        };
    }
    cur.cancel_hook = .{ .ctx = &st, .trigger = SleepState.onCancel };
    defer {
        cur.cancel_hook = null;
        if (st.token.isActive()) _ = self.timers.cancel(st.token);
    }

    while (!st.done) {
        self.suspendCurrent();
    }

    if (st.canceled) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
}

// ── VTable: `Futex` (`futexWait` / `futexWaitUncancelable` / `futexWake`) ────

const FutexParkState = struct {
    shard: *Shard,
    task: *Task,
    waiter: FutexTable.Waiter,
    resume_node: ResumeLane.Node,
    timer_token: TimerHeap.Token = .inactive,
    done: bool = false,
    canceled: bool = false,

    fn onCrossThreadWake(owner_ctx: ?*anyopaque, w: *FutexTable.Waiter) void {
        const target: *Shard = @ptrCast(@alignCast(owner_ctx.?));
        const st: *FutexParkState = @fieldParentPtr("waiter", w);
        if (tls_current_shard == target) {
            deliverFutexWaiterOnOwner(target, w);
            return;
        }
        if (tls_current_shard) |caller| {
            if (caller.mesh != null and caller.mesh == target.mesh) {
                switch (target.mesh.?.trySend(caller.id, target.id, w)) {
                    .ok => {
                        target.notifyFromAnyThread();
                        return;
                    },
                    .full => {},
                }
            }
        }
        target.resume_lane.push(&st.resume_node);
        target.notifyFromAnyThread();
    }

    fn onResumeNode(node: *ResumeLane.Node) void {
        const st: *FutexParkState = @fieldParentPtr("resume_node", node);
        deliverFutexWaiterOnOwner(st.shard, &st.waiter);
    }

    fn onTimer(ctx: ?*anyopaque, outcome: TimerHeap.Outcome) void {
        _ = outcome;
        const st: *FutexParkState = @ptrCast(@alignCast(ctx.?));
        st.timer_token = .inactive;
        if (st.done) return;
        if (st.shard.futex_table.unpark(&st.waiter)) {
            st.done = true;
            st.task.cancel_hook = null;
            st.shard.enqueueReady(st.task);
        }
    }

    fn onCancel(s: *Shard, raw_ctx: *anyopaque) void {
        const st: *FutexParkState = @ptrCast(@alignCast(raw_ctx));
        if (st.done) return;
        if (st.timer_token.isActive()) {
            _ = s.timers.cancel(st.timer_token);
            st.timer_token = .inactive;
        }
        st.canceled = true;
        if (s.futex_table.unpark(&st.waiter)) {
            st.done = true;
            s.enqueueReady(st.task);
        }
    }
};

fn deliverFutexWaiterOnOwner(self: *Shard, w: *FutexTable.Waiter) void {
    const st: *FutexParkState = @fieldParentPtr("waiter", w);
    if (st.timer_token.isActive()) {
        _ = self.timers.cancel(st.timer_token);
        st.timer_token = .inactive;
    }
    st.done = true;
    st.task.cancel_hook = null;
    self.enqueueReady(st.task);
}

fn futexWaitInternal(
    self: *Shard,
    ptr: *const u32,
    expected: u32,
    timeout: Io.Timeout,
    comptime cancelable: bool,
) Io.Cancelable!void {
    if (cancelable) try self.checkCancelPoint();

    if (@atomicLoad(u32, ptr, .acquire) != expected) return;

    const maybe_dl = self.resolveDeadlineNs(timeout);
    if (maybe_dl) |dl| {
        if (dl <= self.awakeNowNs()) return;
    }

    const cur = self.current_task;
    var st: FutexParkState = .{
        .shard = self,
        .task = cur,
        .waiter = .{
            .ptr = ptr,
            .expected = expected,
            .owner_ctx = self,
            .wake_fn = FutexParkState.onCrossThreadWake,
        },
        .resume_node = .{ .callback = FutexParkState.onResumeNode },
    };

    if (!self.futex_table.tryPark(&st.waiter)) {
        return;
    }

    if (maybe_dl) |dl| {
        if (self.timers.schedule(self.gpa, dl, &st, FutexParkState.onTimer)) |tok| {
            st.timer_token = tok;
        } else |_| {}
    }

    if (cancelable) {
        cur.cancel_hook = .{ .ctx = &st, .trigger = FutexParkState.onCancel };
    }
    defer {
        cur.cancel_hook = null;
        if (st.timer_token.isActive()) _ = self.timers.cancel(st.timer_token);
    }

    while (!st.done) {
        self.suspendCurrent();
    }

    if (cancelable and st.canceled) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
}

fn vtableFutexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: Io.Timeout) Io.Cancelable!void {
    const self = fromUserdata(userdata);
    return self.futexWaitInternal(ptr, expected, timeout, true);
}

fn vtableFutexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
    const self = fromUserdata(userdata);
    self.futexWaitInternal(ptr, expected, .none, false) catch unreachable;
}

fn vtableFutexWake(userdata: ?*anyopaque, ptr: *const u32, max_waiters: u32) void {
    const self: *Shard = @ptrCast(@alignCast(userdata.?));
    self.futex_table.wake(ptr, max_waiters);
}

// ── `BlockingPool` Offload Bridge ───────────────────────────────────────────

pub fn callBlocking(
    self: *Shard,
    comptime cancelable: bool,
    comptime func: anytype,
    args: anytype,
) @typeInfo(@TypeOf(func)).@"fn".return_type.? {
    const Ret = @typeInfo(@TypeOf(func)).@"fn".return_type.?;
    const can_cancel = comptime cancelable and switch (@typeInfo(Ret)) {
        .error_union => |eu| (eu.error_set || error{Canceled}) == eu.error_set,
        else => false,
    };
    if (can_cancel) try self.checkCancelPoint();

    const Args = @TypeOf(args);
    const Ctx = struct {
        shard: *Shard,
        task: *Task,
        args: Args,
        result: Ret = undefined,
        done: bool = false,
        canceled_queued: bool = false,
        job: BlockingPool.Job = undefined,

        fn exec(job: *BlockingPool.Job, threaded: *Threaded) void {
            const c: *@This() = @alignCast(@fieldParentPtr("job", job));
            c.result = @call(.auto, func, .{threaded} ++ c.args);
        }

        fn onResume(node: *ResumeLane.Node) void {
            const job: *BlockingPool.Job = @fieldParentPtr("resume_node", node);
            const c: *@This() = @alignCast(@fieldParentPtr("job", job));
            c.done = true;
            c.task.cancel_hook = null;
            c.shard.enqueueReady(c.task);
        }

        fn onCancel(s: *Shard, raw_ctx: *anyopaque) void {
            const c: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (c.done) return;
            switch (s.blocking_pool.tryCancel(&c.job)) {
                .canceled_queued => {
                    c.canceled_queued = true;
                    c.done = true;
                    s.enqueueReady(c.task);
                },
                .must_await => {},
            }
        }
    };

    const cur = self.current_task;
    var ctx: Ctx = .{
        .shard = self,
        .task = cur,
        .args = args,
    };

    while (true) {
        ctx.job = .{
            .run_fn = Ctx.exec,
            .resume_lane = &self.resume_lane,
            .resume_node = .{ .callback = Ctx.onResume },
            .notify_ctx = self,
            .notify_fn = notifyOpaque,
        };
        switch (self.blocking_pool.submitWaitable(&ctx.job)) {
            .accepted => break,
            .queue_full => {
                if (can_cancel) {
                    try vtableSleep(self, .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } });
                } else {
                    const prev = vtableSwapCancelProtection(self, .blocked);
                    defer _ = vtableSwapCancelProtection(self, prev);
                    vtableSleep(self, .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } }) catch unreachable;
                }
            },
            .closed => {
                return @call(.auto, func, .{&self.blocking_pool.threaded} ++ args);
            },
        }
    }

    if (can_cancel) {
        cur.cancel_hook = .{ .ctx = &ctx, .trigger = Ctx.onCancel };
    }
    defer cur.cancel_hook = null;

    while (!ctx.done) {
        self.suspendCurrent();
    }

    if (can_cancel and ctx.canceled_queued) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
    if (can_cancel and cur.shouldCancelNow()) {
        if (@typeInfo(Ret) == .error_union and @typeInfo(Ret).error_union.payload == void) {
            if (ctx.result) |_| {
                cur.acknowledgeCancel();
                return error.Canceled;
            } else |_| {}
        }
    }
    return ctx.result;
}

// ── VTable: Streaming `operate` & `Batch` Orchestration ─────────────────────

fn vtableOperate(userdata: ?*anyopaque, operation: Io.Operation) Io.Cancelable!Io.Operation.Result {
    const self = fromUserdata(userdata);
    return self.driver.operate(self, operation);
}

fn vtableBatchAwaitAsync(userdata: ?*anyopaque, batch: *Io.Batch) Io.Cancelable!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    self.driver.submitBatch(batch) catch {};

    // If any operation already completed or is concurrently in flight, return or await it
    // instead of synchronously blocking on remaining unsubmitted entries!
    if (batch.completed.head != .none) return;
    if (batch.pending.head != .none) {
        return self.waitBatchCompletion(batch, .none) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Timeout => unreachable,
        };
    }

    if (batch.submitted.head != .none) {
        const idx = batch.submitted.head.toIndex();
        const op = batch.storage[idx].submission.operation;
        const next_oi = batch.storage[idx].submission.node.next;
        batch.submitted.head = next_oi;
        if (next_oi == .none) batch.submitted.tail = .none;

        const oi = Io.Operation.OptionalIndex.fromIndex(idx);
        errdefer {
            const tail = batch.unused.tail;
            switch (tail) {
                .none => batch.unused.head = oi,
                else => |tail_oi| batch.storage[tail_oi.toIndex()].unused.next = oi,
            }
            batch.storage[idx] = .{ .unused = .{ .prev = tail, .next = .none } };
            batch.unused.tail = oi;
        }

        const res = try self.driver.operate(self, op);
        switch (batch.completed.tail) {
            .none => batch.completed.head = oi,
            else => |tail_oi| batch.storage[tail_oi.toIndex()].completion.node.next = oi,
        }
        batch.storage[idx] = .{ .completion = .{ .node = .{ .next = .none }, .result = res } };
        batch.completed.tail = oi;
    }
}

fn vtableBatchAwaitConcurrent(
    userdata: ?*anyopaque,
    batch: *Io.Batch,
    timeout: Io.Timeout,
) Io.Batch.AwaitConcurrentError!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    try self.driver.submitBatch(batch);

    if (batch.completed.head != .none or batch.pending.head == .none) return;
    return self.waitBatchCompletion(batch, timeout);
}

fn waitBatchCompletion(self: *Shard, batch: *Io.Batch, timeout: Io.Timeout) (Io.Cancelable || Io.Timeout.Error)!void {
    const maybe_dl = self.resolveDeadlineNs(timeout);
    if (maybe_dl) |dl| {
        if (dl <= self.awakeNowNs()) return error.Timeout;
    }

    const WaitState = struct {
        shard: *Shard,
        task: *Task,
        timer_token: TimerHeap.Token = .inactive,
        timed_out: bool = false,
        canceled: bool = false,

        fn onTimer(ctx: ?*anyopaque, outcome: TimerHeap.Outcome) void {
            _ = outcome;
            const st: *@This() = @ptrCast(@alignCast(ctx.?));
            st.timer_token = .inactive;
            st.timed_out = true;
            st.task.cancel_hook = null;
            st.shard.enqueueReady(st.task);
        }

        fn onCancel(s: *Shard, raw_ctx: *anyopaque) void {
            const st: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (st.timer_token.isActive()) {
                _ = s.timers.cancel(st.timer_token);
                st.timer_token = .inactive;
            }
            st.canceled = true;
            s.enqueueReady(st.task);
        }
    };

    const cur = self.current_task;
    var st: WaitState = .{ .shard = self, .task = cur };
    if (maybe_dl) |dl| {
        if (self.timers.schedule(self.gpa, dl, &st, WaitState.onTimer)) |tok| {
            st.timer_token = tok;
        } else |_| {}
    }

    batch.userdata = cur;
    cur.cancel_hook = .{ .ctx = &st, .trigger = WaitState.onCancel };
    defer {
        batch.userdata = null;
        cur.cancel_hook = null;
        if (st.timer_token.isActive()) _ = self.timers.cancel(st.timer_token);
    }

    while (batch.completed.head == .none and batch.pending.head != .none and !st.timed_out and !st.canceled) {
        self.suspendCurrent();
    }

    if (st.canceled) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
    if (batch.completed.head == .none and batch.pending.head != .none and st.timed_out) {
        return error.Timeout;
    }
}

fn vtableBatchCancel(userdata: ?*anyopaque, batch: *Io.Batch) void {
    const self = fromUserdata(userdata);
    self.driver.cancelBatchPending(batch);

    // Uncancelably drain all remaining in-flight `IOCP` OVERLAPPEDs & running BlockingPool jobs!
    if (batch.pending.head != .none) {
        batch.userdata = self.current_task;
        defer batch.userdata = null;
        while (batch.pending.head != .none) {
            self.suspendCurrent();
        }
    }
}

// ── VTable: Random & Stderr ─────────────────────────────────────────────────

fn vtableRandom(userdata: ?*anyopaque, buffer: []u8) void {
    const self = fromUserdata(userdata);
    if (buffer.len == 0) return;
    if (!self.csprng.isInitialized()) {
        @branchHint(.unlikely);
        var seed: [Threaded.Csprng.seed_len]u8 = undefined;
        const prev = vtableSwapCancelProtection(self, .blocked);
        defer _ = vtableSwapCancelProtection(self, prev);
        vtableRandomSecure(self, &seed) catch |err| switch (err) {
            error.Canceled => unreachable,
            error.EntropyUnavailable => Threaded.fallbackSeed(self, &seed),
        };
        self.csprng.rng = .init(seed);
    }
    self.csprng.rng.fill(buffer);
}

fn vtableRandomSecure(userdata: ?*anyopaque, buffer: []u8) Io.RandomSecureError!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    if (buffer.len == 0) return;

    // Small buffers (e.g. CSPRNG seed) can safely query OS entropy directly via `Threaded.io()`.
    if (buffer.len <= 256) {
        const t_io = self.blocking_pool.threaded.io();
        return t_io.vtable.randomSecure(t_io.userdata, buffer);
    }
    return self.callBlocking(true, struct {
        fn run(t: *Threaded, buf: []u8) Io.RandomSecureError!void {
            const t_io = t.io();
            return t_io.vtable.randomSecure(t_io.userdata, buf);
        }
    }.run, .{buffer});
}

fn ensureStderrInitialized(self: *Shard) void {
    const io_inst = self.ioUnchecked();
    if (!self.stderr_initialized) {
        const prev = vtableSwapCancelProtection(self, .blocked);
        defer _ = vtableSwapCancelProtection(self, prev);

        const stderr_file = File.stderr();
        const t = &self.blocking_pool.threaded;
        const kio = Io.Threaded.global_single_threaded.io();
        t.mutex.lockUncancelable(kio);
        if (!t.environ_initialized) {
            t.environ.scan(t.allocator);
            t.environ_initialized = true;
        }
        const no_color = t.environ.exist.NO_COLOR;
        const clicolor_force = t.environ.exist.CLICOLOR_FORCE;
        t.mutex.unlock(kio);

        self.stderr_mode = Io.Terminal.Mode.detect(
            io_inst,
            stderr_file,
            no_color,
            clicolor_force,
        ) catch |err| switch (err) {
            error.Canceled => unreachable,
        };
        self.stderr_writer = .{
            .io = io_inst,
            .interface = File.Writer.initInterface(&.{}),
            .file = stderr_file,
            .mode = .streaming,
        };
        self.stderr_initialized = true;
    } else {
        self.stderr_writer.io = io_inst;
    }
}

fn vtableLockStderr(userdata: ?*anyopaque, terminal_mode: ?Io.Terminal.Mode) Io.Cancelable!Io.LockedStderr {
    const self = fromUserdata(userdata);
    const cur = self.current_task;
    if (self.stderr_locker != cur) {
        try self.stderr_mutex.lock(self.ioUnchecked());
        assert(self.stderr_lock_count == 0);
        self.stderr_locker = cur;
    }
    self.stderr_lock_count += 1;
    self.ensureStderrInitialized();
    return .{
        .file_writer = &self.stderr_writer,
        .terminal_mode = terminal_mode orelse self.stderr_mode,
    };
}

fn vtableTryLockStderr(userdata: ?*anyopaque, terminal_mode: ?Io.Terminal.Mode) Io.Cancelable!?Io.LockedStderr {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    const cur = self.current_task;
    if (self.stderr_locker != cur) {
        if (!self.stderr_mutex.tryLock()) return null;
        assert(self.stderr_lock_count == 0);
        self.stderr_locker = cur;
    }
    self.stderr_lock_count += 1;
    self.ensureStderrInitialized();
    return .{
        .file_writer = &self.stderr_writer,
        .terminal_mode = terminal_mode orelse self.stderr_mode,
    };
}

fn vtableUnlockStderr(userdata: ?*anyopaque) void {
    const self = fromUserdata(userdata);
    assert(self.stderr_locker == self.current_task);
    assert(self.stderr_lock_count > 0);

    if (self.stderr_writer.err == null) {
        self.stderr_writer.interface.flush() catch {};
    }
    if (self.stderr_writer.err) |err| {
        switch (err) {
            error.Canceled => {
                if (self.current_task.cancel_protection.acknowledged) {
                    self.current_task.cancel_protection.recancel();
                }
            },
            else => {},
        }
        self.stderr_writer.err = null;
    }
    self.stderr_writer.interface.end = 0;
    self.stderr_writer.interface.buffer = &.{};

    self.stderr_lock_count -= 1;
    if (self.stderr_lock_count == 0) {
        self.stderr_locker = null;
        self.stderr_mutex.unlock(self.ioUnchecked());
    }
}

// ── VTable: `Dir` & `File` & `MemoryMap` & `Process` Delegation ──────────────

const ForwardMode = enum {
    pool,
    pool_uncancelable,
    inline_fast,
    inline_uncancelable,
};

fn forwardThreaded(comptime field: []const u8, comptime mode: ForwardMode) @FieldType(Io.VTable, field) {
    const FnType = @typeInfo(@FieldType(Io.VTable, field)).pointer.child;
    const fn_info = @typeInfo(FnType).@"fn";
    const Ret = fn_info.return_type.?;
    const cancelable = (mode == .pool or mode == .inline_fast);
    const use_pool = (mode == .pool or mode == .pool_uncancelable);
    const can_cancel = comptime cancelable and switch (@typeInfo(Ret)) {
        .error_union => |eu| (eu.error_set || error{Canceled}) == eu.error_set,
        else => false,
    };

    return &struct {
        fn ExtraTupleType() type {
            var types: [fn_info.param_types.len - 1]type = undefined;
            for (1..fn_info.param_types.len) |i| {
                types[i - 1] = fn_info.param_types[i].?;
            }
            return @Tuple(&types);
        }

        fn runOnThreaded(t: *Threaded, extra: ExtraTupleType()) Ret {
            const t_io = t.io();
            return @call(.auto, @field(t_io.vtable, field), .{t_io.userdata} ++ extra);
        }

        fn invoke(userdata: ?*anyopaque, extra: ExtraTupleType()) Ret {
            const self = fromUserdata(userdata);
            if (comptime use_pool) {
                return self.callBlocking(cancelable, runOnThreaded, .{extra});
            }
            if (comptime can_cancel) try self.checkCancelPoint();
            return runOnThreaded(&self.blocking_pool.threaded, extra);
        }

        const impl = switch (fn_info.param_types.len) {
            1 => struct {
                fn call(p0: fn_info.param_types[0].?) Ret {
                    return invoke(p0, .{});
                }
            }.call,
            2 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?) Ret {
                    return invoke(p0, .{p1});
                }
            }.call,
            3 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?, p2: fn_info.param_types[2].?) Ret {
                    return invoke(p0, .{ p1, p2 });
                }
            }.call,
            4 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?, p2: fn_info.param_types[2].?, p3: fn_info.param_types[3].?) Ret {
                    return invoke(p0, .{ p1, p2, p3 });
                }
            }.call,
            5 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?, p2: fn_info.param_types[2].?, p3: fn_info.param_types[3].?, p4: fn_info.param_types[4].?) Ret {
                    return invoke(p0, .{ p1, p2, p3, p4 });
                }
            }.call,
            6 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?, p2: fn_info.param_types[2].?, p3: fn_info.param_types[3].?, p4: fn_info.param_types[4].?, p5: fn_info.param_types[5].?) Ret {
                    return invoke(p0, .{ p1, p2, p3, p4, p5 });
                }
            }.call,
            else => @compileError("unexpected VTable arity"),
        };
    }.impl;
}

fn vtableFileClose(userdata: ?*anyopaque, files: []const File) void {
    const self = fromUserdata(userdata);
    self.driver.fileClose(files);
}

fn vtableDirClose(userdata: ?*anyopaque, dirs: []const Dir) void {
    const self = fromUserdata(userdata);
    self.driver.dirClose(dirs);
}

fn vtableFileWriteFileStreaming(
    userdata: ?*anyopaque,
    file: File,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
) File.Writer.WriteFileError!usize {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.fileWriteFileStreaming(self, file, header, file_reader, limit);
}

fn vtableFileWriteFilePositional(
    userdata: ?*anyopaque,
    file: File,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
    offset: u64,
) File.WriteFilePositionalError!usize {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.fileWriteFilePositional(self, file, header, file_reader, limit, offset);
}

fn vtableProcessSpawn(
    userdata: ?*anyopaque,
    options: std.process.SpawnOptions,
) std.process.SpawnError!std.process.Child {
    const self = fromUserdata(userdata);
    return self.driver.processSpawn(self, options);
}

fn vtableChildWait(
    userdata: ?*anyopaque,
    child: *std.process.Child,
) std.process.Child.WaitError!std.process.Child.Term {
    const self = fromUserdata(userdata);
    return self.driver.childWait(self, child);
}

fn vtableChildKill(userdata: ?*anyopaque, child: *std.process.Child) void {
    const self = fromUserdata(userdata);
    self.driver.childKill(self, child);
}

// ── VTable: Networking (`net*`) ─────────────────────────────────────────────

fn vtableNetListenIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.ListenOptions,
) net.IpAddress.ListenError!net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netListenIp(self, address, options);
}

fn vtableNetAccept(
    userdata: ?*anyopaque,
    server: net.Socket.Handle,
    options: net.Server.AcceptOptions,
) net.Server.AcceptError!net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netAccept(self, server, options);
}

fn vtableNetBindIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.BindOptions,
) net.IpAddress.BindError!net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netBindIp(address, options);
}

fn vtableNetConnectIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.ConnectOptions,
) net.IpAddress.ConnectError!net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    const maybe_dl = self.resolveDeadlineNs(options.timeout);
    if (maybe_dl) |dl| {
        if (dl <= self.awakeNowNs()) return error.Timeout;
    }
    return self.driver.netConnectIp(self, address, options, maybe_dl);
}

fn vtableNetListenUnix(
    userdata: ?*anyopaque,
    ua: *const net.UnixAddress,
    options: net.UnixAddress.ListenOptions,
) net.UnixAddress.ListenError!net.Socket.Handle {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netListenUnix(ua, options);
}

fn vtableNetConnectUnix(
    userdata: ?*anyopaque,
    ua: *const net.UnixAddress,
) net.UnixAddress.ConnectError!net.Socket.Handle {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netConnectUnix(self, ua);
}

fn vtableNetSocketCreatePair(
    userdata: ?*anyopaque,
    options: net.Socket.CreatePairOptions,
) net.Socket.CreatePairError![2]net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netSocketCreatePair(self, options);
}

fn vtableNetWriteFile(
    userdata: ?*anyopaque,
    handle: net.Socket.Handle,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
) net.Stream.Writer.WriteFileError!usize {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netWriteFile(self, handle, header, file_reader, limit);
}

fn vtableNetClose(userdata: ?*anyopaque, sockets: []const net.Socket) void {
    const self = fromUserdata(userdata);
    self.driver.netClose(sockets);
}

fn vtableNetShutdown(
    userdata: ?*anyopaque,
    handle: net.Socket.Handle,
    how: net.ShutdownHow,
) net.ShutdownError!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netShutdown(handle, how);
}

fn vtableNetInterfaceNameResolve(
    userdata: ?*anyopaque,
    name: *const net.Interface.Name,
) net.Interface.Name.ResolveError!net.Interface {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netInterfaceNameResolve(self, name);
}

fn vtableNetInterfaceName(
    userdata: ?*anyopaque,
    interface: net.Interface,
) net.Interface.NameError!net.Interface.Name {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    return self.driver.netInterfaceName(self, interface);
}

fn vtableNetLookup(
    userdata: ?*anyopaque,
    host_name: net.HostName,
    resolved: *Io.Queue(net.HostName.LookupResult),
    options: net.HostName.LookupOptions,
) net.HostName.LookupError!void {
    const self = fromUserdata(userdata);
    const shard_io = self.ioUnchecked();
    defer resolved.close(shard_io);

    try self.checkCancelPoint();

    const name = host_name.bytes;
    const parsed_literal: ?net.IpAddress = if (net.IpAddress.parseIp6(name, options.port)) |addr| blk: {
        if (options.family == .ip4) return error.UnknownHostName;
        break :blk addr;
    } else |_| if (net.IpAddress.parseIp4(name, options.port)) |addr| blk: {
        if (options.family == .ip6) return error.UnknownHostName;
        break :blk addr;
    } else |_| null;

    if (parsed_literal) |addr| {
        if (options.canonical_name_buffer) |cbuf| {
            @memcpy(cbuf[0..name.len], name);
            resolved.putAll(shard_io, &.{
                .{ .address = addr },
                .{ .canonical_name = .{ .bytes = cbuf[0..name.len] } },
            }) catch |err| switch (err) {
                error.Closed => unreachable,
                error.Canceled => return error.Canceled,
            };
        } else {
            resolved.putOne(shard_io, .{ .address = addr }) catch |err| switch (err) {
                error.Closed => unreachable,
                error.Canceled => return error.Canceled,
            };
        }
        return;
    }

    const lo_suffix = if (name[name.len - 1] == '.') "localhost." else "localhost";
    if (std.ascii.endsWithIgnoreCase(name, lo_suffix) and
        (name.len == lo_suffix.len or name[name.len - lo_suffix.len - 1] == '.'))
    {
        var items: [3]net.HostName.LookupResult = undefined;
        var count_items: usize = 0;
        if (options.family != .ip4) {
            items[count_items] = .{ .address = .{ .ip6 = .loopback(options.port) } };
            count_items += 1;
        }
        if (options.family != .ip6) {
            items[count_items] = .{ .address = .{ .ip4 = .loopback(options.port) } };
            count_items += 1;
        }
        if (options.canonical_name_buffer) |cbuf| {
            const canon = "localhost";
            @memcpy(cbuf[0..canon.len], canon);
            items[count_items] = .{ .canonical_name = .{ .bytes = cbuf[0..canon.len] } };
            count_items += 1;
        }
        resolved.putAll(shard_io, items[0..count_items]) catch |err| switch (err) {
            error.Closed => unreachable,
            error.Canceled => return error.Canceled,
        };
        return;
    }

    const DnsBatch = struct {
        items: [64]net.HostName.LookupResult = undefined,
        len: usize = 0,
    };
    var batch_out: DnsBatch = .{};

    try self.callBlocking(true, struct {
        fn run(
            t: *Threaded,
            hn: net.HostName,
            opts: net.HostName.LookupOptions,
            out: *DnsBatch,
        ) net.HostName.LookupError!void {
            const t_io = t.io();
            var q_buf: [64]net.HostName.LookupResult = undefined;
            var local_q: Io.Queue(net.HostName.LookupResult) = .init(&q_buf);
            try t_io.vtable.netLookup(t_io.userdata, hn, &local_q, opts);
            while (local_q.getOneUncancelable(t_io)) |item| {
                if (out.len < out.items.len) {
                    out.items[out.len] = item;
                    out.len += 1;
                }
            } else |err| switch (err) {
                error.Closed => {},
            }
        }
    }.run, .{ host_name, options, &batch_out });

    if (batch_out.len > 0) {
        resolved.putAll(shard_io, batch_out.items[0..batch_out.len]) catch |err| switch (err) {
            error.Closed => unreachable,
            error.Canceled => return error.Canceled,
        };
    }
}

const vtable: Io.VTable = .{
    .crashHandler = vtableCrashHandler,

    .async = vtableAsync,
    .concurrent = vtableConcurrent,
    .await = vtableAwait,
    .cancel = vtableCancel,

    .groupAsync = vtableGroupAsync,
    .groupConcurrent = vtableGroupConcurrent,
    .groupAwait = vtableGroupAwait,
    .groupCancel = vtableGroupCancel,

    .recancel = vtableRecancel,
    .swapCancelProtection = vtableSwapCancelProtection,
    .checkCancel = vtableCheckCancel,

    .futexWait = vtableFutexWait,
    .futexWaitUncancelable = vtableFutexWaitUncancelable,
    .futexWake = vtableFutexWake,

    .operate = vtableOperate,
    .batchAwaitAsync = vtableBatchAwaitAsync,
    .batchAwaitConcurrent = vtableBatchAwaitConcurrent,
    .batchCancel = vtableBatchCancel,

    .dirCreateDir = forwardThreaded("dirCreateDir", .pool),
    .dirCreateDirPath = forwardThreaded("dirCreateDirPath", .pool),
    .dirCreateDirPathOpen = forwardThreaded("dirCreateDirPathOpen", .pool),
    .dirOpenDir = forwardThreaded("dirOpenDir", .pool),
    .dirStat = forwardThreaded("dirStat", .pool),
    .dirStatFile = forwardThreaded("dirStatFile", .pool),
    .dirAccess = forwardThreaded("dirAccess", .pool),
    .dirCreateFile = forwardThreaded("dirCreateFile", .pool),
    .dirCreateFileAtomic = forwardThreaded("dirCreateFileAtomic", .pool),
    .dirOpenFile = forwardThreaded("dirOpenFile", .pool),
    .dirClose = vtableDirClose,
    .dirRead = forwardThreaded("dirRead", .pool),
    .dirRealPath = forwardThreaded("dirRealPath", .pool),
    .dirRealPathFile = forwardThreaded("dirRealPathFile", .pool),
    .dirDeleteFile = forwardThreaded("dirDeleteFile", .pool),
    .dirDeleteDir = forwardThreaded("dirDeleteDir", .pool),
    .dirRename = forwardThreaded("dirRename", .pool),
    .dirRenamePreserve = forwardThreaded("dirRenamePreserve", .pool),
    .dirSymLink = forwardThreaded("dirSymLink", .pool),
    .dirReadLink = forwardThreaded("dirReadLink", .pool),
    .dirSetOwner = forwardThreaded("dirSetOwner", .pool),
    .dirSetFileOwner = forwardThreaded("dirSetFileOwner", .pool),
    .dirSetPermissions = forwardThreaded("dirSetPermissions", .pool),
    .dirSetFilePermissions = forwardThreaded("dirSetFilePermissions", .pool),
    .dirSetTimestamps = forwardThreaded("dirSetTimestamps", .pool),
    .dirHardLink = forwardThreaded("dirHardLink", .pool),

    .fileStat = forwardThreaded("fileStat", .pool),
    .fileLength = forwardThreaded("fileLength", .pool),
    .fileClose = vtableFileClose,
    .fileWritePositional = forwardThreaded("fileWritePositional", .pool),
    .fileWriteFileStreaming = vtableFileWriteFileStreaming,
    .fileWriteFilePositional = vtableFileWriteFilePositional,
    .fileReadPositional = forwardThreaded("fileReadPositional", .pool),
    .fileSeekBy = forwardThreaded("fileSeekBy", .inline_fast),
    .fileSeekTo = forwardThreaded("fileSeekTo", .inline_fast),
    .fileSync = forwardThreaded("fileSync", .pool),
    .fileIsTty = forwardThreaded("fileIsTty", .pool),
    .fileEnableAnsiEscapeCodes = forwardThreaded("fileEnableAnsiEscapeCodes", .pool),
    .fileSupportsAnsiEscapeCodes = forwardThreaded("fileSupportsAnsiEscapeCodes", .pool),
    .fileSetLength = forwardThreaded("fileSetLength", .pool),
    .fileSetOwner = forwardThreaded("fileSetOwner", .pool),
    .fileSetPermissions = forwardThreaded("fileSetPermissions", .pool),
    .fileSetTimestamps = forwardThreaded("fileSetTimestamps", .pool),
    .fileLock = forwardThreaded("fileLock", .pool),
    .fileTryLock = forwardThreaded("fileTryLock", .inline_fast),
    .fileUnlock = forwardThreaded("fileUnlock", .inline_uncancelable),
    .fileDowngradeLock = forwardThreaded("fileDowngradeLock", .pool),
    .fileRealPath = forwardThreaded("fileRealPath", .pool),
    .fileHardLink = forwardThreaded("fileHardLink", .pool),

    .fileMemoryMapCreate = forwardThreaded("fileMemoryMapCreate", .pool),
    .fileMemoryMapDestroy = forwardThreaded("fileMemoryMapDestroy", .pool_uncancelable),
    .fileMemoryMapSetLength = forwardThreaded("fileMemoryMapSetLength", .pool),
    .fileMemoryMapRead = forwardThreaded("fileMemoryMapRead", .pool),
    .fileMemoryMapWrite = forwardThreaded("fileMemoryMapWrite", .pool),

    .processExecutableOpen = forwardThreaded("processExecutableOpen", .pool),
    .processExecutablePath = forwardThreaded("processExecutablePath", .pool),
    .lockStderr = vtableLockStderr,
    .tryLockStderr = vtableTryLockStderr,
    .unlockStderr = vtableUnlockStderr,
    .processCurrentPath = forwardThreaded("processCurrentPath", .inline_fast),
    .processSetCurrentDir = forwardThreaded("processSetCurrentDir", .pool),
    .processSetCurrentPath = forwardThreaded("processSetCurrentPath", .pool),
    .processReplace = forwardThreaded("processReplace", .pool),
    .processSpawn = vtableProcessSpawn,
    .childWait = vtableChildWait,
    .childKill = vtableChildKill,

    .progressParentFile = forwardThreaded("progressParentFile", .inline_fast),
    .inheritParentDir = forwardThreaded("inheritParentDir", .inline_fast),
    .inheritParentFile = forwardThreaded("inheritParentFile", .inline_fast),

    .now = vtableNow,
    .clockResolution = vtableClockResolution,
    .sleep = vtableSleep,

    .random = vtableRandom,
    .randomSecure = vtableRandomSecure,

    .netListenIp = vtableNetListenIp,
    .netAccept = vtableNetAccept,
    .netBindIp = vtableNetBindIp,
    .netConnectIp = vtableNetConnectIp,
    .netListenUnix = vtableNetListenUnix,
    .netConnectUnix = vtableNetConnectUnix,
    .netSocketCreatePair = vtableNetSocketCreatePair,
    .netWriteFile = vtableNetWriteFile,
    .netClose = vtableNetClose,
    .netShutdown = vtableNetShutdown,
    .netInterfaceNameResolve = vtableNetInterfaceNameResolve,
    .netInterfaceName = vtableNetInterfaceName,
    .netLookup = vtableNetLookup,
};
