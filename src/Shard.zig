//! Single-Shard shared-nothing event loop, cooperative Fiber scheduler, and
//! complete `std.Io.VTable` implementation (`epoll` on Linux, `IOCP` on Windows).
//!
//! Strict affinity contract (AGENTS.md §3):
//! - Every `std.Io.VTable` entry except `futexWake` requires `Shard.current() == self`
//!   and unconditionally calls `@panic` on cross-thread misuse.
//! - Fibers, `Future`s, `Group`s, `Batch`es, sockets, and timers never migrate
//!   across Shards; there is zero cross-thread work-stealing or stack-stealing.

const builtin = @import("builtin");
const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;
const net = Io.net;
const Threaded = Io.Threaded;
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const assert = std.debug.assert;
const linux = std.os.linux;
const posix = std.posix;
const windows = std.os.windows;

const time = @import("time.zig");
const TimerHeap = @import("TimerHeap.zig");
const MailboxMod = @import("Mailbox.zig");
const ResumeLane = MailboxMod.ResumeLane;
const fiber = @import("fiber.zig");
const BlockingPool = @import("BlockingPool.zig");
const FutexTable = @import("FutexTable.zig");
const ShardMesh = @import("ShardMesh.zig");
const AcceptorMod = @import("Acceptor.zig");
const Acceptor = AcceptorMod.Acceptor;
const AcceptedSocket = AcceptorMod.AcceptedSocket;
const Epoll = if (builtin.os.tag == .linux) @import("Epoll.zig") else void;
const Iocp = if (builtin.os.tag == .windows) @import("Iocp.zig") else void;
const win32 = if (builtin.os.tag == .windows) @import("win32.zig") else void;

const Shard = @This();

const Poller = switch (builtin.os.tag) {
    .linux => Epoll,
    .windows => Iocp,
    else => @compileError("unsupported OS"),
};

const io_poll_cadence: u32 = 64;

pub const InitOptions = struct {
    stack_size: usize = fiber.default_stack_size,
    max_cached_stacks: usize = fiber.default_max_cached_stacks,
    max_fds: u32 = if (builtin.os.tag == .linux) Epoll.default_max_fds else 0,
    blocking_threads: ?usize = null,
    blocking_queue_capacity: ?usize = null,
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
    Poller.InitError ||
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

const CancelHook = struct {
    ctx: *anyopaque,
    trigger: *const fn (*Shard, *anyopaque) void,
};

const TaskKind = enum {
    main,
    future,
    group,
};

const Task = struct {
    raw: ?*fiber.RawFiber = null,
    kind: TaskKind,
    ready_next: ?*Task = null,
    queued_ready: bool = false,
    finished: bool = false,
    cancel_requested: bool = false,
    cancel_protection: CancelProtectionState = .{},
    cancel_hook: ?CancelHook = null,

    // `Future` linkage & storage
    awaiter: ?*Task = null,
    start_future: *const fn (context: *const anyopaque, result: *anyopaque) void = undefined,
    context_ptr: [*]u8 = undefined,
    context_alloc: ?[]u8 = null,
    context_align: Alignment = .@"1",
    result_ptr: [*]u8 = undefined,
    result_len: usize = 0,
    result_alloc: ?[]u8 = null,
    result_align: Alignment = .@"1",

    // `Group` intrusive doubly-linked list & closure
    group_owner: ?*Io.Group = null,
    group_prev: ?*Task = null,
    group_next: ?*Task = null,
    start_group: *const fn (context: *const anyopaque) void = undefined,

    fn shouldCancelNow(self: *const Task) bool {
        return self.cancel_requested and self.cancel_protection.effective() == .unblocked;
    }

    fn acknowledgeCancel(self: *Task) void {
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
        assert(task.ready_next == null);
        task.queued_ready = true;
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

const BatchOp = struct {
    free_next: ?*BatchOp = null,
    shard: *Shard = undefined,
    batch: *Io.Batch = undefined,
    slot_index: u32 = 0,
    op: Io.Operation = undefined,
    done: bool = false,
    canceled: bool = false,
    result: ?Io.Operation.Result = null,
    backend: switch (builtin.os.tag) {
        .linux => union(enum) {
            none,
            epoll: struct {
                fd: linux.fd_t,
                dir: Epoll.Direction,
                waiter: Epoll.Waiter,
            },
            blocking: BlockingPool.Job,
        },
        .windows => union(enum) {
            none,
            iocp: struct {
                handle: win32.HANDLE,
                comp: Iocp.Completion,
                wsabufs: [Threaded.max_iovecs_len]win32.WSABUF = undefined,
                splat_buf: [Threaded.splat_buffer_size]u8 = undefined,
                dw_flags: win32.DWORD = 0,
                from_addr: Threaded.PosixAddress = undefined,
                from_len: c_int = @sizeOf(Threaded.PosixAddress),
            },
            blocking: BlockingPool.Job,
        },
        else => unreachable,
    } = .none,
};

const SharedAcceptQueue = if (builtin.os.tag == .windows) struct {
    items: std.ArrayListUnmanaged(AcceptedSocket) = .empty,
    waiter_head: ?*AcceptWaiter = null,
    waiter_tail: ?*AcceptWaiter = null,
} else void;

const AcceptWaiter = if (builtin.os.tag == .windows) struct {
    prev: ?*AcceptWaiter = null,
    next: ?*AcceptWaiter = null,
    linked: bool = false,
    canceled: bool = false,
    delivered: ?AcceptedSocket = null,
    task: *Task,
} else void;

gpa: Allocator,
id: usize,
shard_count: usize,
runtime_ptr: ?*anyopaque,

poller: Poller,
timers: TimerHeap = .init,
stack_pool: fiber.StackPool,
resume_lane: ResumeLane = .init,

blocking_pool: *BlockingPool,
owned_blocking_pool: ?*BlockingPool = null,
futex_table: *FutexTable,
owned_futex_table: ?*FutexTable = null,
mesh: ?*ShardMesh = null,
acceptor: ?*Acceptor = null,
stderr_mutex: *Io.Mutex,
owned_stderr_mutex: Io.Mutex = .init,

thread_ctx: fiber.ThreadContext = undefined,
idle_fiber: *fiber.RawFiber = undefined,
main_task: Task = .{ .kind = .main },
current_task: *Task = undefined,
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
batch_op_free: ?*BatchOp = null,

stderr_writer: File.Writer = undefined,
stderr_mode: Io.Terminal.Mode = .no_color,
stderr_initialized: bool = false,

shared_accepts: if (builtin.os.tag == .windows) [64]SharedAcceptQueue else void =
    if (builtin.os.tag == .windows) @splat(.{}) else {},

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
) (Allocator.Error || Poller.InitError)!void {
    var poller = switch (builtin.os.tag) {
        .linux => try Epoll.init(options.max_fds),
        .windows => try Iocp.init(),
        else => comptime unreachable,
    };
    errdefer switch (builtin.os.tag) {
        .linux => poller.deinit(gpa),
        .windows => poller.deinit(),
        else => comptime unreachable,
    };

    self.* = .{
        .gpa = gpa,
        .id = services.shard_id,
        .shard_count = services.shard_count,
        .runtime_ptr = services.runtime_ptr,
        .poller = poller,
        .stack_pool = fiber.StackPool.init(options.stack_size, options.max_cached_stacks),
        .blocking_pool = services.blocking_pool,
        .futex_table = services.futex_table,
        .mesh = services.mesh,
        .acceptor = services.acceptor,
        .stderr_mutex = services.stderr_mutex,
    };
    self.stop_node.ctx = self;

    if (builtin.os.tag == .windows) {
        if (self.acceptor) |acc| {
            acc.bindShard(self.id, self, notifyOpaque);
        }
    }
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
    _ = self.drainIncoming();

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

    var cur_op = self.batch_op_free;
    self.batch_op_free = null;
    while (cur_op) |op| {
        const next = op.free_next;
        self.gpa.destroy(op);
        cur_op = next;
    }

    if (builtin.os.tag == .windows) {
        for (&self.shared_accepts) |*q| {
            for (q.items.items) |item| {
                if (item.raw_sock != win32.INVALID_SOCKET) _ = win32.closesocket(item.raw_sock);
            }
            q.items.deinit(self.gpa);
        }
    }

    self.timers.deinit(self.gpa);
    self.stack_pool.deinit(self.gpa);
    switch (builtin.os.tag) {
        .linux => self.poller.deinit(self.gpa),
        .windows => self.poller.deinit(),
        else => comptime unreachable,
    }
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
    self.poller.notifyFromAnyThread();
}

fn notifyOpaque(ctx: ?*anyopaque) void {
    const self: *Shard = @ptrCast(@alignCast(ctx.?));
    self.notifyFromAnyThread();
}

fn onStopResumeNode(node: *ResumeLane.Node) void {
    const self: *Shard = @ptrCast(@alignCast(node.ctx.?));
    if (builtin.os.tag == .windows) {
        for (&self.shared_accepts) |*q| {
            var cur = q.waiter_head;
            q.waiter_head = null;
            q.waiter_tail = null;
            while (cur) |w| {
                const next = w.next;
                w.prev = null;
                w.next = null;
                w.linked = false;
                w.canceled = true;
                self.enqueueReady(w.task);
                cur = next;
            }
        }
    }
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

inline fn fromUserdataAnyThread(userdata: ?*anyopaque) *Shard {
    return @ptrCast(@alignCast(userdata.?));
}

// ── Cooperative Fiber Scheduler & Event Loop ────────────────────────────────

inline fn contextOfTask(self: *Shard, task: *Task) *fiber.ExecContext {
    return if (task == &self.main_task) &self.thread_ctx.ctx else &task.raw.?.ctx;
}

fn reclaimPendingFiber(self: *Shard) void {
    if (self.pending_recycle) |raw| {
        self.pending_recycle = null;
        self.stack_pool.release(self.gpa, raw);
    }
}

fn enqueueReady(self: *Shard, task: *Task) void {
    assert(!task.finished);
    if (!self.in_idle_fiber and task == self.current_task) return;
    self.ready_queue.push(task);
}

/// Suspends `self.current_task` and switches to the next ready `Task` (or
/// `idle_fiber` when `ready_queue` is empty or the I/O poll cadence hits).
fn suspendCurrent(self: *Shard) void {
    const prev_task = self.current_task;
    const prev_ctx = self.contextOfTask(prev_task);

    if (self.steps_since_poll < io_poll_cadence) {
        if (self.ready_queue.pop()) |next_task| {
            self.steps_since_poll += 1;
            if (next_task == prev_task) return;
            self.current_task = next_task;
            fiber.switchContext(prev_ctx, self.contextOfTask(next_task));
            self.reclaimPendingFiber();
            return;
        }
    }

    self.steps_since_poll = 0;
    fiber.switchContext(prev_ctx, &self.idle_fiber.ctx);
    self.reclaimPendingFiber();
}

fn idleFiberEntry(arg: *anyopaque) void {
    const self: *Shard = @ptrCast(@alignCast(arg));
    self.in_idle_fiber = true;
    self.reclaimPendingFiber();

    while (!self.idle_exit) {
        if (self.ready_queue.head != null) {
            // Non-blocking poll pass so ready CPU fibers cannot starve I/O or timers.
            _ = self.drainIncoming();
            _ = self.timers.fireExpired(self.awakeNowNs());
            switch (builtin.os.tag) {
                .linux => _ = self.poller.poll(0),
                .windows => _ = self.poller.poll(0),
                else => comptime unreachable,
            }
            if (self.ready_queue.pop()) |next_task| {
                self.in_idle_fiber = false;
                self.current_task = next_task;
                fiber.switchContext(&self.idle_fiber.ctx, self.contextOfTask(next_task));
                self.in_idle_fiber = true;
                self.reclaimPendingFiber();
                continue;
            }
        }

        _ = self.drainIncoming();
        const now_ns = self.awakeNowNs();
        _ = self.timers.fireExpired(now_ns);

        if (self.ready_queue.pop()) |next_task| {
            self.in_idle_fiber = false;
            self.current_task = next_task;
            fiber.switchContext(&self.idle_fiber.ctx, self.contextOfTask(next_task));
            self.in_idle_fiber = true;
            self.reclaimPendingFiber();
            continue;
        }

        // Arm coalesced doorbell BEFORE final `.seq_cst` emptiness check.
        self.poller.doorbell.armBeforeSleep();
        if (!self.allIncomingEmptySeqCst()) {
            self.poller.doorbell.disarm();
            _ = self.drainIncoming();
            continue;
        }

        const post_check_now = self.awakeNowNs();
        switch (builtin.os.tag) {
            .linux => {
                const timeout_ms: i32 = if (self.timers.peekDeadlineNs()) |dl|
                    @intCast(time.ceilWaitMs(time.subNsSat(dl, post_check_now)))
                else
                    -1;
                _ = self.poller.poll(timeout_ms);
            },
            .windows => {
                const timeout_ms: u32 = if (self.timers.peekDeadlineNs()) |dl|
                    @intCast(time.ceilWaitMs(time.subNsSat(dl, post_check_now)))
                else
                    win32.INFINITE;
                _ = self.poller.poll(timeout_ms);
            },
            else => comptime unreachable,
        }
        self.poller.doorbell.disarm();

        _ = self.drainIncoming();
        _ = self.timers.fireExpired(self.awakeNowNs());

        if (self.ready_queue.pop()) |next_task| {
            self.in_idle_fiber = false;
            self.current_task = next_task;
            fiber.switchContext(&self.idle_fiber.ctx, self.contextOfTask(next_task));
            self.in_idle_fiber = true;
            self.reclaimPendingFiber();
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
    if (builtin.os.tag == .windows) {
        if (self.acceptor) |acc| {
            if (!acc.isShardInboxEmptySeqCst(self.id)) return false;
        }
    }
    return true;
}

fn drainIncoming(self: *Shard) usize {
    var drained: usize = 0;
    drained += self.resume_lane.drain();
    if (self.mesh) |m| {
        drained += m.drainFor(self.id, self, onMeshFutexWake);
    }
    if (builtin.os.tag == .windows) {
        if (self.acceptor) |acc| {
            drained += acc.drainShardInbox(self.id, self, onWindowsAcceptedSocket);
        }
    }
    return drained;
}

fn onMeshFutexWake(ctx: ?*anyopaque, waiter: *FutexTable.Waiter) void {
    const self: *Shard = @ptrCast(@alignCast(ctx.?));
    deliverFutexWaiterOnOwner(self, waiter);
}

fn onWindowsAcceptedSocket(ctx: ?*anyopaque, accepted: AcceptedSocket) void {
    if (builtin.os.tag != .windows) return;
    const self: *Shard = @ptrCast(@alignCast(ctx.?));
    const q = &self.shared_accepts[accepted.listener_id];
    if (q.waiter_head) |w| {
        q.waiter_head = w.next;
        if (q.waiter_head) |nh| {
            nh.prev = null;
        } else {
            q.waiter_tail = null;
        }
        w.prev = null;
        w.next = null;
        w.linked = false;
        w.delivered = accepted;
        self.enqueueReady(w.task);
        return;
    }
    q.items.append(self.gpa, accepted) catch {
        self.acceptor.?.noteShardSocketAdopted(self.id);
        _ = win32.closesocket(accepted.raw_sock);
    };
}

// ── VTable: Cancellation & Protection ───────────────────────────────────────

fn checkCancelPoint(self: *Shard) Io.Cancelable!void {
    const task = self.current_task;
    if (task.shouldCancelNow()) {
        task.acknowledgeCancel();
        return error.Canceled;
    }
}

fn vtableCrashHandler(userdata: ?*anyopaque) void {
    const self = fromUserdataAnyThread(userdata);
    if (tls_current_shard != self) return;
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

    const header_base = @intFromPtr(raw.headerBytes());
    const header_end = header_base + fiber.header_capacity;

    const task_addr = Alignment.of(Task).forward(header_base);
    const run_arg_addr = Alignment.of(TaskRunArg).forward(task_addr + @sizeOf(Task));
    var cursor = run_arg_addr + @sizeOf(TaskRunArg);

    // Place result storage inline inside `raw.header` when it fits; spill to heap otherwise.
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

    // Place context copy inline when it fits; spill to heap otherwise.
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
        .result_len = result_len,
        .result_alloc = result_alloc,
        .result_align = result_alignment,
    };

    const run_arg: *TaskRunArg = @ptrFromInt(run_arg_addr);
    run_arg.* = .{ .shard = self, .task = task };
    raw.arm(taskFiberEntry, run_arg);

    return task;
}

fn taskFiberEntry(arg: *anyopaque) void {
    const run_arg: *TaskRunArg = @ptrCast(@alignCast(arg));
    const self = run_arg.shard;
    const task = run_arg.task;
    self.reclaimPendingFiber();

    switch (task.kind) {
        .main => unreachable,
        .future => {
            task.start_future(task.context_ptr, task.result_ptr);
            if (task.context_alloc) |ctx_slice| {
                self.gpa.rawFree(ctx_slice, task.context_align, @returnAddress());
                task.context_alloc = null;
            }
            task.finished = true;
            if (task.awaiter) |awaiter| {
                task.awaiter = null;
                self.enqueueReady(awaiter);
            }
            const next_ctx = if (self.ready_queue.pop()) |next_task| blk: {
                self.current_task = next_task;
                break :blk self.contextOfTask(next_task);
            } else &self.idle_fiber.ctx;
            // `Future` raw fiber stays alive until `vtableAwait` copies `result` out!
            task.raw.?.exit_target = next_ctx;
        },
        .group => {
            task.start_group(task.context_ptr);
            if (task.context_alloc) |ctx_slice| {
                self.gpa.rawFree(ctx_slice, task.context_align, @returnAddress());
                task.context_alloc = null;
            }
            task.finished = true;
            self.unlinkGroupTask(task);
            assert(self.active_tasks > 0);
            self.active_tasks -= 1;

            const raw = task.raw.?;
            const next_ctx = if (self.ready_queue.pop()) |next_task| blk: {
                self.current_task = next_task;
                break :blk self.contextOfTask(next_task);
            } else &self.idle_fiber.ctx;

            // Recycle `raw` AFTER switching off its stack (`reclaimPendingFiber`).
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
    task.start_future = start;
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

    while (!child.finished) {
        child.awaiter = self.current_task;
        self.suspendCurrent();
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
    task.start_group = start;
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

    if (wait_st.canceled) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
}

fn vtableGroupCancel(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) void {
    _ = token;
    const self = fromUserdata(userdata);
    if (group.token.load(.acquire) == null) return;

    self.cancelGroupMembers(group);
    if (group.token.load(.acquire) == null) return;

    const cur = self.current_task;
    assert((group.state & group_ptr_mask) == 0);
    group.state = @intFromPtr(cur) | group_cancel_bit;
    while (group.token.load(.acquire) != null) {
        self.suspendCurrent();
    }
}

// ── VTable: Clock & `sleep` ─────────────────────────────────────────────────

pub fn awakeNowNs(self: *Shard) i96 {
    return vtableNow(self, .awake).nanoseconds;
}

fn vtableNow(userdata: ?*anyopaque, clock: Io.Clock) Io.Timestamp {
    if (builtin.os.tag == .linux) {
        var ts: posix.timespec = undefined;
        const rc = linux.clock_gettime(Threaded.clockToPosix(clock), &ts);
        if (linux.errno(rc) != .SUCCESS) return .zero;
        return Threaded.timestampFromPosix(&ts);
    }
    const self = fromUserdataAnyThread(userdata);
    const t_io = self.blocking_pool.threaded.io();
    return t_io.vtable.now(t_io.userdata, clock);
}

fn vtableClockResolution(userdata: ?*anyopaque, clock: Io.Clock) Io.Clock.ResolutionError!Io.Duration {
    const self = fromUserdata(userdata);
    if (builtin.os.tag == .linux) {
        var ts: posix.timespec = undefined;
        const rc = linux.clock_getres(Threaded.clockToPosix(clock), &ts);
        switch (linux.errno(rc)) {
            .SUCCESS => return .{ .nanoseconds = Threaded.nanosecondsFromPosix(&ts) },
            .INVAL => return error.ClockUnavailable,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
    const t_io = self.blocking_pool.threaded.io();
    return t_io.vtable.clockResolution(t_io.userdata, clock);
}

fn vtableSleep(userdata: ?*anyopaque, timeout: Io.Timeout) Io.Cancelable!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    const maybe_deadline = time.resolveAwakeDeadlineNs(timeout, self, vtableNow);
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
            // Fall back to cooperative yield loop if OOM prevents heap growth.
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
        // Only wake immediately if `unpark` wins the race against `futexWake`.
        // If `unpark` returns `false`, `futexWake` already popped `&st.waiter`
        // and is delivering it via `ShardMesh` / `ResumeLane` — wait for it!
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

    const maybe_dl = time.resolveAwakeDeadlineNs(timeout, self, vtableNow);
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
    const self = fromUserdataAnyThread(userdata);
    self.futex_table.wake(ptr, max_waiters);
}

// ── BlockingPool Offload Bridge (`runBlockingJoined`) ───────────────────────

fn callBlocking(
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
                // Already executing on pool worker: MUST wait for `onResume` so
                // stack-resident `c.args` is never freed while borrowed!
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
        switch (self.blocking_pool.submit(&ctx.job)) {
            .accepted => break,
            .queue_full => {
                // Cooperative backpressure: yield for 1ms on Shard TimerHeap.
                if (can_cancel) {
                    try vtableSleep(self, .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } });
                } else {
                    const prev = vtableSwapCancelProtection(self, .blocked);
                    defer _ = vtableSwapCancelProtection(self, prev);
                    vtableSleep(self, .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } }) catch unreachable;
                }
            },
            .closed => {
                // Pool already stopped during final teardown: execute synchronously.
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
        // If `ctx.result` succeeded with an owned handle (e.g. `File`, `Dir`, `Socket`),
        // return it cleanly and leave cancellation unacknowledged for the next point;
        // if it failed or returned void, surface `error.Canceled` when appropriate.
        if (@typeInfo(Ret) == .error_union and @typeInfo(Ret).error_union.payload == void) {
            if (ctx.result) |_| {
                cur.acknowledgeCancel();
                return error.Canceled;
            } else |_| {}
        }
    }
    return ctx.result;
}

// ── Linux `Epoll` Single-Wait & Windows `IOCP` Single-Wait Helpers ──────────

fn waitLinuxFd(
    self: *Shard,
    fd: linux.fd_t,
    dir: Epoll.Direction,
    maybe_deadline_ns: ?i96,
) (Epoll.RegisterError || Io.Cancelable || Io.Timeout.Error)!void {
    if (builtin.os.tag != .linux) unreachable;
    try self.checkCancelPoint();

    if (maybe_deadline_ns) |dl| {
        if (dl <= self.awakeNowNs()) return error.Timeout;
    }

    const WaitState = struct {
        shard: *Shard,
        task: *Task,
        fd: linux.fd_t,
        dir: Epoll.Direction,
        waiter: Epoll.Waiter = undefined,
        timer_token: TimerHeap.Token = .inactive,
        done: bool = false,
        timed_out: bool = false,
        canceled: bool = false,

        fn onIoReady(ctx: ?*anyopaque) void {
            const st: *@This() = @ptrCast(@alignCast(ctx.?));
            if (st.done) return;
            if (st.timer_token.isActive()) {
                _ = st.shard.timers.cancel(st.timer_token);
                st.timer_token = .inactive;
            }
            st.done = true;
            st.task.cancel_hook = null;
            st.shard.enqueueReady(st.task);
        }

        fn onTimer(ctx: ?*anyopaque, outcome: TimerHeap.Outcome) void {
            _ = outcome;
            const st: *@This() = @ptrCast(@alignCast(ctx.?));
            st.timer_token = .inactive;
            if (st.done) return;
            st.shard.poller.cancelWaiter(st.fd, st.dir, &st.waiter);
            st.timed_out = true;
            st.done = true;
            st.task.cancel_hook = null;
            st.shard.enqueueReady(st.task);
        }

        fn onCancel(s: *Shard, raw_ctx: *anyopaque) void {
            const st: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (st.done) return;
            if (st.timer_token.isActive()) {
                _ = s.timers.cancel(st.timer_token);
                st.timer_token = .inactive;
            }
            s.poller.cancelWaiter(st.fd, st.dir, &st.waiter);
            st.canceled = true;
            st.done = true;
            s.enqueueReady(st.task);
        }
    };

    const cur = self.current_task;
    var st: WaitState = .{
        .shard = self,
        .task = cur,
        .fd = fd,
        .dir = dir,
    };
    st.waiter = .{ .ctx = &st, .wake_fn = WaitState.onIoReady };

    try self.poller.waitIo(self.gpa, fd, dir, &st.waiter);
    if (st.done) return;

    if (maybe_deadline_ns) |dl| {
        st.timer_token = try self.timers.schedule(self.gpa, dl, &st, WaitState.onTimer);
    }

    cur.cancel_hook = .{ .ctx = &st, .trigger = WaitState.onCancel };
    defer {
        cur.cancel_hook = null;
        if (st.timer_token.isActive()) _ = self.timers.cancel(st.timer_token);
        if (st.waiter.linked) self.poller.cancelWaiter(fd, dir, &st.waiter);
    }

    while (!st.done) {
        self.suspendCurrent();
    }

    if (st.canceled) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
    if (st.timed_out) return error.Timeout;
}

/// Windows IOCP helper obeying the `OVERLAPPED` iron law: on cancellation or
/// timeout, calls `CancelIoEx` and waits UNCANCELABLY until the completion
/// packet returns from `GetQueuedCompletionStatusEx`.
const IocpWaitResult = if (builtin.os.tag == .windows) struct {
    bytes: u32,
    nt_status: windows.NTSTATUS,
    timed_out: bool,
    canceled: bool,
} else void;

fn awaitIocpPending(
    self: *Shard,
    handle: if (builtin.os.tag == .windows) win32.HANDLE else void,
    comp: if (builtin.os.tag == .windows) *Iocp.Completion else void,
    maybe_deadline_ns: ?i96,
) Io.Cancelable!IocpWaitResult {
    if (builtin.os.tag != .windows) unreachable;

    const State = struct {
        shard: *Shard,
        task: *Task,
        handle: win32.HANDLE,
        comp: *Iocp.Completion,
        timer_token: TimerHeap.Token = .inactive,
        timed_out: bool = false,
        canceled: bool = false,

        fn onComplete(c: *Iocp.Completion) void {
            const st: *@This() = @ptrCast(@alignCast(c.ctx.?));
            if (st.timer_token.isActive()) {
                _ = st.shard.timers.cancel(st.timer_token);
                st.timer_token = .inactive;
            }
            st.task.cancel_hook = null;
            st.shard.enqueueReady(st.task);
        }

        fn onTimer(ctx: ?*anyopaque, outcome: TimerHeap.Outcome) void {
            _ = outcome;
            const st: *@This() = @ptrCast(@alignCast(ctx.?));
            st.timer_token = .inactive;
            if (st.comp.done) return;
            st.timed_out = true;
            Iocp.requestCancelIo(st.handle, st.comp);
        }

        fn onCancel(_: *Shard, raw_ctx: *anyopaque) void {
            const st: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (st.comp.done) return;
            st.canceled = true;
            Iocp.requestCancelIo(st.handle, st.comp);
        }
    };

    const cur = self.current_task;
    var st: State = .{
        .shard = self,
        .task = cur,
        .handle = handle,
        .comp = comp,
    };
    comp.ctx = &st;
    comp.callback = State.onComplete;

    if (cur.shouldCancelNow()) {
        st.canceled = true;
        Iocp.requestCancelIo(handle, comp);
    } else {
        if (maybe_deadline_ns) |dl| {
            if (dl <= self.awakeNowNs()) {
                st.timed_out = true;
                Iocp.requestCancelIo(handle, comp);
            } else if (self.timers.schedule(self.gpa, dl, &st, State.onTimer)) |tok| {
                st.timer_token = tok;
            } else |_| {}
        }
        cur.cancel_hook = .{ .ctx = &st, .trigger = State.onCancel };
    }
    defer {
        cur.cancel_hook = null;
        if (st.timer_token.isActive()) _ = self.timers.cancel(st.timer_token);
    }

    // NEVER exit this loop until `comp.done == true` (packet dequeued from IOCP)!
    while (!comp.done) {
        self.suspendCurrent();
    }

    if (st.canceled) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
    return .{
        .bytes = comp.bytes_transferred,
        .nt_status = comp.nt_status,
        .timed_out = st.timed_out,
        .canceled = false,
    };
}

// ── VTable: Streaming `operate` & `Batch` ───────────────────────────────────

fn fillWriteIovecsPosix(
    iovecs: *[Threaded.max_iovecs_len]posix.iovec_const,
    splat_buf: *[Threaded.splat_buffer_size]u8,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) usize {
    var len: usize = 0;
    const appendSlice = struct {
        fn add(out: *[Threaded.max_iovecs_len]posix.iovec_const, out_len: *usize, bytes: []const u8) void {
            if (bytes.len == 0 or out_len.* >= out.len) return;
            out[out_len.*] = .{ .base = bytes.ptr, .len = bytes.len };
            out_len.* += 1;
        }
    }.add;

    appendSlice(iovecs, &len, header);
    if (data.len == 0) return len;
    for (data[0 .. data.len - 1]) |seg| appendSlice(iovecs, &len, seg);
    const pattern = data[data.len - 1];
    if (len < iovecs.len) switch (splat) {
        0 => {},
        1 => appendSlice(iovecs, &len, pattern),
        else => switch (pattern.len) {
            0 => {},
            1 => {
                const fill_n = @min(splat_buf.len, splat);
                @memset(splat_buf[0..fill_n], pattern[0]);
                var rem = splat;
                while (rem > 0 and len < iovecs.len) {
                    const chunk = @min(rem, fill_n);
                    appendSlice(iovecs, &len, splat_buf[0..chunk]);
                    rem -= chunk;
                }
            },
            else => for (0..@min(splat, iovecs.len - len)) |_| {
                appendSlice(iovecs, &len, pattern);
            },
        },
    };
    return len;
}

fn fillWriteWsabufs(
    bufs: *[Threaded.max_iovecs_len]if (builtin.os.tag == .windows) win32.WSABUF else void,
    splat_buf: *[Threaded.splat_buffer_size]u8,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) u32 {
    if (builtin.os.tag != .windows) unreachable;
    var len: usize = 0;
    const add = struct {
        fn f(out: *[Threaded.max_iovecs_len]win32.WSABUF, out_len: *usize, bytes: []const u8) void {
            if (bytes.len == 0 or out_len.* >= out.len) return;
            out[out_len.*] = .{
                .len = @intCast(@min(bytes.len, std.math.maxInt(u32))),
                .buf = @constCast(bytes.ptr),
            };
            out_len.* += 1;
        }
    }.f;

    add(bufs, &len, header);
    if (data.len == 0) return @intCast(len);
    for (data[0 .. data.len - 1]) |seg| add(bufs, &len, seg);
    const pattern = data[data.len - 1];
    if (len < bufs.len) switch (splat) {
        0 => {},
        1 => add(bufs, &len, pattern),
        else => switch (pattern.len) {
            0 => {},
            1 => {
                const fill_n = @min(splat_buf.len, splat);
                @memset(splat_buf[0..fill_n], pattern[0]);
                var rem = splat;
                while (rem > 0 and len < bufs.len) {
                    const chunk = @min(rem, fill_n);
                    add(bufs, &len, splat_buf[0..chunk]);
                    rem -= chunk;
                }
            },
            else => for (0..@min(splat, bufs.len - len)) |_| {
                add(bufs, &len, pattern);
            },
        },
    };
    return @intCast(len);
}

const TryPosixOp = union(enum) {
    done: Io.Operation.Result,
    would_block: struct { fd: linux.fd_t, dir: Epoll.Direction },
    needs_pool,
};

fn tryExecuteLinuxNonblocking(op: Io.Operation) TryPosixOp {
    if (builtin.os.tag != .linux) unreachable;
    switch (op) {
        .file_read_streaming => |o| {
            if (!o.file.flags.nonblocking) return .needs_pool;
            var iovecs: [Threaded.max_iovecs_len]posix.iovec = undefined;
            var n_iov: usize = 0;
            for (o.data) |buf| {
                if (n_iov == iovecs.len) break;
                if (buf.len > 0) {
                    iovecs[n_iov] = .{ .base = buf.ptr, .len = buf.len };
                    n_iov += 1;
                }
            }
            if (n_iov == 0) return .{ .done = .{ .file_read_streaming = 0 } };
            while (true) {
                const rc = linux.readv(o.file.handle, &iovecs, @intCast(n_iov));
                switch (linux.errno(rc)) {
                    .SUCCESS => return .{ .done = .{
                        .file_read_streaming = if (rc == 0) error.EndOfStream else rc,
                    } },
                    .INTR => continue,
                    .AGAIN => return .{ .would_block = .{ .fd = o.file.handle, .dir = .read } },
                    .IO => return .{ .done = .{ .file_read_streaming = error.InputOutput } },
                    .ISDIR => return .{ .done = .{ .file_read_streaming = error.IsDir } },
                    .CONNRESET => return .{ .done = .{ .file_read_streaming = error.ConnectionResetByPeer } },
                    .NOTCONN => return .{ .done = .{ .file_read_streaming = error.SocketUnconnected } },
                    .ACCES => return .{ .done = .{ .file_read_streaming = error.AccessDenied } },
                    .NOMEM, .NOBUFS => return .{ .done = .{ .file_read_streaming = error.SystemResources } },
                    .BADF => return .{ .done = .{ .file_read_streaming = error.NotOpenForReading } },
                    else => |err| return .{ .done = .{ .file_read_streaming = posix.unexpectedErrno(err) } },
                }
            }
        },
        .file_write_streaming => |o| {
            if (!o.file.flags.nonblocking) return .needs_pool;
            var iovecs: [Threaded.max_iovecs_len]posix.iovec_const = undefined;
            var splat_buf: [Threaded.splat_buffer_size]u8 = undefined;
            const n_iov = fillWriteIovecsPosix(&iovecs, &splat_buf, o.header, o.data, o.splat);
            if (n_iov == 0) return .{ .done = .{ .file_write_streaming = 0 } };
            while (true) {
                const rc = linux.writev(o.file.handle, &iovecs, @intCast(n_iov));
                switch (linux.errno(rc)) {
                    .SUCCESS => return .{ .done = .{ .file_write_streaming = rc } },
                    .INTR => continue,
                    .AGAIN => return .{ .would_block = .{ .fd = o.file.handle, .dir = .write } },
                    .PIPE => return .{ .done = .{ .file_write_streaming = error.BrokenPipe } },
                    .DQUOT => return .{ .done = .{ .file_write_streaming = error.DiskQuota } },
                    .FBIG => return .{ .done = .{ .file_write_streaming = error.FileTooBig } },
                    .IO => return .{ .done = .{ .file_write_streaming = error.InputOutput } },
                    .NOSPC => return .{ .done = .{ .file_write_streaming = error.NoSpaceLeft } },
                    .ACCES => return .{ .done = .{ .file_write_streaming = error.AccessDenied } },
                    .PERM => return .{ .done = .{ .file_write_streaming = error.PermissionDenied } },
                    .NOMEM, .NOBUFS => return .{ .done = .{ .file_write_streaming = error.SystemResources } },
                    .BADF => return .{ .done = .{ .file_write_streaming = error.NotOpenForWriting } },
                    .NXIO => return .{ .done = .{ .file_write_streaming = error.NoDevice } },
                    .BUSY => return .{ .done = .{ .file_write_streaming = error.DeviceBusy } },
                    else => |err| return .{ .done = .{ .file_write_streaming = posix.unexpectedErrno(err) } },
                }
            }
        },
        .device_io_control => return .needs_pool,
        .net_read => |o| {
            var iovecs: [Threaded.max_iovecs_len]posix.iovec = undefined;
            var n_iov: usize = 0;
            for (o.data) |buf| {
                if (n_iov == iovecs.len) break;
                if (buf.len > 0) {
                    iovecs[n_iov] = .{ .base = buf.ptr, .len = buf.len };
                    n_iov += 1;
                }
            }
            if (n_iov == 0 and o.control.len == 0) {
                return .{ .done = .{ .net_read = .{ .data_len = 0 } } };
            }
            var msg: linux.msghdr = .{
                .name = null,
                .namelen = 0,
                .iov = &iovecs,
                .iovlen = @intCast(n_iov),
                .control = if (o.control.len > 0) o.control.ptr else null,
                .controllen = @intCast(o.control.len),
                .flags = 0,
            };
            while (true) {
                const rc = linux.recvmsg(o.socket_handle, &msg, linux.MSG.NOSIGNAL | linux.MSG.CMSG_CLOEXEC);
                switch (linux.errno(rc)) {
                    .SUCCESS => return .{ .done = .{ .net_read = .{
                        .data_len = rc,
                        .control_len = @intCast(msg.controllen),
                        .control_truncated = (msg.flags & linux.MSG.CTRUNC) != 0,
                    } } },
                    .INTR => continue,
                    .AGAIN => return .{ .would_block = .{ .fd = o.socket_handle, .dir = .read } },
                    .CONNRESET => return .{ .done = .{ .net_read = error.ConnectionResetByPeer } },
                    .NOTCONN => return .{ .done = .{ .net_read = error.SocketUnconnected } },
                    .ACCES => return .{ .done = .{ .net_read = error.AccessDenied } },
                    .NETDOWN => return .{ .done = .{ .net_read = error.NetworkDown } },
                    .TIMEDOUT => return .{ .done = .{ .net_read = error.ConnectionTimedOut } },
                    .NOMEM, .NOBUFS => return .{ .done = .{ .net_read = error.SystemResources } },
                    else => |err| return .{ .done = .{ .net_read = posix.unexpectedErrno(err) } },
                }
            }
        },
        .net_write => |o| {
            var iovecs: [Threaded.max_iovecs_len]posix.iovec_const = undefined;
            var splat_buf: [Threaded.splat_buffer_size]u8 = undefined;
            const n_iov = fillWriteIovecsPosix(&iovecs, &splat_buf, o.header, o.data, o.splat);
            if (n_iov == 0 and o.control.len == 0) {
                return .{ .done = .{ .net_write = 0 } };
            }
            var msg: linux.msghdr_const = .{
                .name = null,
                .namelen = 0,
                .iov = &iovecs,
                .iovlen = @intCast(n_iov),
                .control = if (o.control.len > 0) o.control.ptr else null,
                .controllen = @intCast(o.control.len),
                .flags = 0,
            };
            while (true) {
                const rc = linux.sendmsg(o.socket_handle, &msg, linux.MSG.NOSIGNAL);
                switch (linux.errno(rc)) {
                    .SUCCESS => return .{ .done = .{ .net_write = rc } },
                    .INTR => continue,
                    .AGAIN => return .{ .would_block = .{ .fd = o.socket_handle, .dir = .write } },
                    .PIPE, .NOTCONN => return .{ .done = .{ .net_write = error.SocketUnconnected } },
                    .CONNRESET => return .{ .done = .{ .net_write = error.ConnectionResetByPeer } },
                    .CONNREFUSED => return .{ .done = .{ .net_write = error.ConnectionRefused } },
                    .NETUNREACH => return .{ .done = .{ .net_write = error.NetworkUnreachable } },
                    .HOSTUNREACH => return .{ .done = .{ .net_write = error.HostUnreachable } },
                    .NETDOWN => return .{ .done = .{ .net_write = error.NetworkDown } },
                    .AFNOSUPPORT => return .{ .done = .{ .net_write = error.AddressFamilyUnsupported } },
                    .TIMEDOUT => return .{ .done = .{ .net_write = error.ConnectionTimedOut } },
                    .DESTADDRREQ => return .{ .done = .{ .net_write = error.SocketNotBound } },
                    .NOMEM, .NOBUFS => return .{ .done = .{ .net_write = error.SystemResources } },
                    else => |err| return .{ .done = .{ .net_write = posix.unexpectedErrno(err) } },
                }
            }
        },
        .net_receive => |o| {
            if (o.message_buffer.len == 0) return .{ .done = .{ .net_receive = .{ null, 0 } } };
            var msg_flags: u32 = linux.MSG.CMSG_CLOEXEC;
            if (o.flags.oob) msg_flags |= linux.MSG.OOB;
            if (o.flags.peek) msg_flags |= linux.MSG.PEEK;
            if (o.flags.trunc) msg_flags |= linux.MSG.TRUNC;

            var received_msgs: usize = 0;
            var data_offset: usize = 0;
            while (received_msgs < o.message_buffer.len) {
                const rem_buf = o.data_buffer[data_offset..];
                if (rem_buf.len == 0 and received_msgs > 0) break;

                const out_msg = &o.message_buffer[received_msgs];
                var iov: [1]posix.iovec = .{.{ .base = rem_buf.ptr, .len = rem_buf.len }};
                var peer: Threaded.PosixAddress = undefined;
                var hdr: linux.msghdr = .{
                    .name = @ptrCast(&peer.any),
                    .namelen = @sizeOf(Threaded.PosixAddress),
                    .iov = &iov,
                    .iovlen = 1,
                    .control = if (out_msg.control.len > 0) out_msg.control.ptr else null,
                    .controllen = @intCast(out_msg.control.len),
                    .flags = 0,
                };
                const rc = linux.recvmsg(o.socket_handle, &hdr, msg_flags);
                switch (linux.errno(rc)) {
                    .SUCCESS => {
                        const actual_copied = @min(rc, rem_buf.len);
                        out_msg.from = if (hdr.namelen > 0)
                            Threaded.addressFromPosix(&peer)
                        else
                            .{ .ip4 = .loopback(0) };
                        out_msg.data = rem_buf[0..actual_copied];
                        out_msg.control = out_msg.control[0..@intCast(hdr.controllen)];
                        out_msg.flags = .{
                            .eor = (hdr.flags & linux.MSG.EOR) != 0,
                            .trunc = (hdr.flags & linux.MSG.TRUNC) != 0,
                            .ctrunc = (hdr.flags & linux.MSG.CTRUNC) != 0,
                            .oob = (hdr.flags & linux.MSG.OOB) != 0,
                            .errqueue = (hdr.flags & linux.MSG.ERRQUEUE) != 0,
                        };
                        data_offset += actual_copied;
                        received_msgs += 1;
                        if (o.flags.peek) break;
                    },
                    .INTR => continue,
                    .AGAIN => {
                        if (received_msgs > 0) return .{ .done = .{ .net_receive = .{ null, received_msgs } } };
                        return .{ .would_block = .{ .fd = o.socket_handle, .dir = .read } };
                    },
                    else => |err| {
                        const mapped: net.Socket.ReceiveError = switch (err) {
                            .NOMEM, .NOBUFS => error.SystemResources,
                            .MFILE => error.ProcessFdQuotaExceeded,
                            .NFILE => error.SystemFdQuotaExceeded,
                            .NOTCONN => error.SocketUnconnected,
                            .MSGSIZE => error.MessageOversize,
                            .CONNRESET => error.ConnectionResetByPeer,
                            .NETDOWN => error.NetworkDown,
                            .CONNREFUSED => error.PortUnreachable,
                            .TIMEDOUT => error.ConnectionTimedOut,
                            else => posix.unexpectedErrno(err),
                        };
                        return .{ .done = .{ .net_receive = .{ mapped, received_msgs } } };
                    },
                }
            }
            return .{ .done = .{ .net_receive = .{ null, received_msgs } } };
        },
        .net_send => |o| {
            if (o.messages.len == 0) return .{ .done = .{ .net_send = .{ null, 0 } } };
            var send_flags: u32 = linux.MSG.NOSIGNAL;
            if (o.flags.confirm) send_flags |= linux.MSG.CONFIRM;
            if (o.flags.dont_route) send_flags |= linux.MSG.DONTROUTE;
            if (o.flags.eor) send_flags |= linux.MSG.EOR;
            if (o.flags.oob) send_flags |= linux.MSG.OOB;
            if (o.flags.fastopen) send_flags |= linux.MSG.FASTOPEN;

            var sent_msgs: usize = 0;
            while (sent_msgs < o.messages.len) {
                const msg_item = &o.messages[sent_msgs];
                var dest: Threaded.PosixAddress = undefined;
                const dest_len = Threaded.addressToPosix(msg_item.address, &dest);
                var iov: [1]posix.iovec_const = .{.{ .base = msg_item.data_ptr, .len = msg_item.data_len }};
                var hdr: linux.msghdr_const = .{
                    .name = @ptrCast(&dest.any),
                    .namelen = dest_len,
                    .iov = &iov,
                    .iovlen = 1,
                    .control = if (msg_item.control.len > 0) msg_item.control.ptr else null,
                    .controllen = @intCast(msg_item.control.len),
                    .flags = 0,
                };
                const rc = linux.sendmsg(o.socket_handle, &hdr, send_flags);
                switch (linux.errno(rc)) {
                    .SUCCESS => {
                        msg_item.data_len = rc;
                        sent_msgs += 1;
                    },
                    .INTR => continue,
                    .AGAIN => {
                        if (sent_msgs > 0) return .{ .done = .{ .net_send = .{ null, sent_msgs } } };
                        return .{ .would_block = .{ .fd = o.socket_handle, .dir = .write } };
                    },
                    else => |err| {
                        const mapped: net.Socket.SendError = switch (err) {
                            .MSGSIZE => error.MessageOversize,
                            .NOMEM, .NOBUFS => error.SystemResources,
                            .NETUNREACH => error.NetworkUnreachable,
                            .HOSTUNREACH => error.HostUnreachable,
                            .NETDOWN => error.NetworkDown,
                            .CONNREFUSED => error.ConnectionRefused,
                            .AFNOSUPPORT => error.AddressFamilyUnsupported,
                            .ALREADY => error.FastOpenAlreadyInProgress,
                            .CONNRESET => error.ConnectionResetByPeer,
                            .PIPE, .NOTCONN => error.SocketUnconnected,
                            .ACCES => error.AccessDenied,
                            .TIMEDOUT => error.ConnectionTimedOut,
                            else => posix.unexpectedErrno(err),
                        };
                        return .{ .done = .{ .net_send = .{ mapped, sent_msgs } } };
                    },
                }
            }
            return .{ .done = .{ .net_send = .{ null, sent_msgs } } };
        },
    }
}

fn blockingOperateOnThreaded(t: *Threaded, op: Io.Operation) Io.Operation.Result {
    const t_io = t.io();
    return t_io.vtable.operate(t_io.userdata, op) catch |err| switch (err) {
        error.Canceled => unreachable,
    };
}

fn vtableOperate(userdata: ?*anyopaque, operation: Io.Operation) Io.Cancelable!Io.Operation.Result {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    switch (builtin.os.tag) {
        .linux => {
            while (true) {
                switch (tryExecuteLinuxNonblocking(operation)) {
                    .done => |res| {
                        // Cascade edge-triggered readiness if socket did not hit EAGAIN.
                        switch (operation) {
                            .net_read => |o| self.poller.cascadeNext(o.socket_handle, .read),
                            .net_receive => |o| self.poller.cascadeNext(o.socket_handle, .read),
                            .net_write => |o| self.poller.cascadeNext(o.socket_handle, .write),
                            .net_send => |o| self.poller.cascadeNext(o.socket_handle, .write),
                            .file_read_streaming => |o| self.poller.cascadeNext(o.file.handle, .read),
                            .file_write_streaming => |o| self.poller.cascadeNext(o.file.handle, .write),
                            .device_io_control => {},
                        }
                        return res;
                    },
                    .would_block => |wb| {
                        self.waitLinuxFd(wb.fd, wb.dir, null) catch |err| switch (err) {
                            error.Canceled => return error.Canceled,
                            error.Timeout => unreachable,
                            error.PollUnsupported => return try self.callBlocking(true, struct {
                                fn run(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
                                    return blockingOperateOnThreaded(t, op_arg);
                                }
                            }.run, .{operation}),
                            else => return try self.callBlocking(true, struct {
                                fn run(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
                                    return blockingOperateOnThreaded(t, op_arg);
                                }
                            }.run, .{operation}),
                        };
                    },
                    .needs_pool => {
                        return try self.callBlocking(true, struct {
                            fn run(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
                                return blockingOperateOnThreaded(t, op_arg);
                            }
                        }.run, .{operation});
                    },
                }
            }
        },
        .windows => return self.operateWindows(operation, null),
        else => comptime unreachable,
    }
}

fn operateWindows(self: *Shard, operation: Io.Operation, maybe_deadline_ns: ?i96) Io.Cancelable!Io.Operation.Result {
    if (builtin.os.tag != .windows) unreachable;

    switch (operation) {
        .file_read_streaming => |o| {
            if (!o.file.flags.nonblocking) {
                return try self.callBlocking(true, struct {
                    fn run(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
                        return blockingOperateOnThreaded(t, op_arg);
                    }
                }.run, .{operation});
            }
            const dest = for (o.data) |b| {
                if (b.len > 0) break b;
            } else return .{ .file_read_streaming = 0 };

            var comp: Iocp.Completion = .{ .callback = undefined };
            const ok = win32.ReadFile(
                o.file.handle,
                dest.ptr,
                @intCast(@min(dest.len, std.math.maxInt(u32))),
                null,
                &comp.overlapped,
            );
            if (!ok.toBool()) {
                const err = windows.GetLastError();
                if (@backingInt(err) != win32.ERROR_IO_PENDING) {
                    return .{ .file_read_streaming = mapWinFileReadError(@backingInt(err)) };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitIocpPending(o.file.handle, &comp, maybe_deadline_ns);
            if (res.nt_status != .SUCCESS) {
                if (res.bytes == 0 and res.nt_status == .END_OF_FILE) {
                    return .{ .file_read_streaming = error.EndOfStream };
                }
                return .{ .file_read_streaming = error.InputOutput };
            }
            return .{ .file_read_streaming = if (res.bytes == 0) error.EndOfStream else res.bytes };
        },
        .file_write_streaming => |o| {
            if (!o.file.flags.nonblocking) {
                return try self.callBlocking(true, struct {
                    fn run(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
                        return blockingOperateOnThreaded(t, op_arg);
                    }
                }.run, .{operation});
            }
            const src: []const u8 = blk: {
                if (o.header.len > 0) break :blk o.header;
                if (o.data.len == 0) return .{ .file_write_streaming = 0 };
                for (o.data[0 .. o.data.len - 1]) |b| if (b.len > 0) break :blk b;
                if (o.splat > 0 and o.data[o.data.len - 1].len > 0) break :blk o.data[o.data.len - 1];
                return .{ .file_write_streaming = 0 };
            };
            var comp: Iocp.Completion = .{ .callback = undefined };
            const ok = win32.WriteFile(
                o.file.handle,
                src.ptr,
                @intCast(@min(src.len, std.math.maxInt(u32))),
                null,
                &comp.overlapped,
            );
            if (!ok.toBool()) {
                const err = windows.GetLastError();
                if (@backingInt(err) != win32.ERROR_IO_PENDING) {
                    return .{ .file_write_streaming = mapWinFileWriteError(@backingInt(err)) };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitIocpPending(o.file.handle, &comp, maybe_deadline_ns);
            if (res.nt_status != .SUCCESS) return .{ .file_write_streaming = error.InputOutput };
            return .{ .file_write_streaming = res.bytes };
        },
        .device_io_control => {
            return try self.callBlocking(true, struct {
                fn run(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
                    return blockingOperateOnThreaded(t, op_arg);
                }
            }.run, .{operation});
        },
        .net_read => |o| {
            var bufs: [Threaded.max_iovecs_len]win32.WSABUF = undefined;
            var n_bufs: u32 = 0;
            for (o.data) |b| {
                if (n_bufs == bufs.len) break;
                if (b.len > 0) {
                    bufs[n_bufs] = .{ .len = @intCast(@min(b.len, std.math.maxInt(u32))), .buf = b.ptr };
                    n_bufs += 1;
                }
            }
            if (n_bufs == 0) return .{ .net_read = .{ .data_len = 0 } };
            const sock = win32.handleToSocket(o.socket_handle);
            var flags: win32.DWORD = 0;
            var comp: Iocp.Completion = .{ .callback = undefined };
            const rc = win32.WSARecv(sock, &bufs, n_bufs, null, &flags, &comp.overlapped, null);
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    return .{ .net_read = mapWinNetReadError(werr) };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitIocpPending(o.socket_handle, &comp, maybe_deadline_ns);
            if (res.nt_status != .SUCCESS) {
                var transferred: win32.DWORD = 0;
                var out_flags: win32.DWORD = 0;
                _ = win32.WSAGetOverlappedResult(sock, &comp.overlapped, &transferred, .FALSE, &out_flags);
                return .{ .net_read = mapWinNetReadError(win32.WSAGetLastError()) };
            }
            return .{ .net_read = .{ .data_len = res.bytes } };
        },
        .net_write => |o| {
            var bufs: [Threaded.max_iovecs_len]win32.WSABUF = undefined;
            var splat_buf: [Threaded.splat_buffer_size]u8 = undefined;
            const n_bufs = fillWriteWsabufs(&bufs, &splat_buf, o.header, o.data, o.splat);
            if (n_bufs == 0) return .{ .net_write = 0 };
            const sock = win32.handleToSocket(o.socket_handle);
            var comp: Iocp.Completion = .{ .callback = undefined };
            const rc = win32.WSASend(sock, &bufs, n_bufs, null, 0, &comp.overlapped, null);
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    return .{ .net_write = mapWinNetWriteError(werr) };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitIocpPending(o.socket_handle, &comp, maybe_deadline_ns);
            if (res.nt_status != .SUCCESS) {
                var transferred: win32.DWORD = 0;
                var out_flags: win32.DWORD = 0;
                _ = win32.WSAGetOverlappedResult(sock, &comp.overlapped, &transferred, .FALSE, &out_flags);
                return .{ .net_write = mapWinNetWriteError(win32.WSAGetLastError()) };
            }
            return .{ .net_write = res.bytes };
        },
        .net_receive => |o| {
            if (o.message_buffer.len == 0) return .{ .net_receive = .{ null, 0 } };
            const sock = win32.handleToSocket(o.socket_handle);
            var wbuf: [1]win32.WSABUF = .{.{
                .len = @intCast(@min(o.data_buffer.len, std.math.maxInt(u32))),
                .buf = o.data_buffer.ptr,
            }};
            var flags: win32.DWORD = 0;
            if (o.flags.oob) flags |= win32.ws2_32.MSG.OOB;
            if (o.flags.peek) flags |= win32.ws2_32.MSG.PEEK;
            var peer: Threaded.PosixAddress = undefined;
            var peer_len: c_int = @sizeOf(Threaded.PosixAddress);
            var comp: Iocp.Completion = .{ .callback = undefined };
            const rc = win32.WSARecvFrom(sock, &wbuf, 1, null, &flags, &peer.any, &peer_len, &comp.overlapped, null);
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    return .{ .net_receive = .{ mapWinNetRecvError(werr), 0 } };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitIocpPending(o.socket_handle, &comp, maybe_deadline_ns);
            if (res.nt_status != .SUCCESS) {
                var transferred: win32.DWORD = 0;
                var out_flags: win32.DWORD = 0;
                _ = win32.WSAGetOverlappedResult(sock, &comp.overlapped, &transferred, .FALSE, &out_flags);
                return .{ .net_receive = .{ mapWinNetRecvError(win32.WSAGetLastError()), 0 } };
            }
            const out_msg = &o.message_buffer[0];
            out_msg.from = if (peer_len > 0) Threaded.addressFromPosix(&peer) else .{ .ip4 = .loopback(0) };
            out_msg.data = o.data_buffer[0..res.bytes];
            out_msg.control = &.{};
            out_msg.flags = .{
                .eor = false,
                .trunc = (flags & win32.ws2_32.MSG.PARTIAL) != 0,
                .ctrunc = false,
                .oob = false,
                .errqueue = false,
            };
            return .{ .net_receive = .{ null, 1 } };
        },
        .net_send => |o| {
            if (o.messages.len == 0) return .{ .net_send = .{ null, 0 } };
            const sock = win32.handleToSocket(o.socket_handle);
            var flags: win32.DWORD = 0;
            if (o.flags.dont_route) flags |= win32.ws2_32.MSG.DONTROUTE;
            if (o.flags.oob) flags |= win32.ws2_32.MSG.OOB;

            var sent_count: usize = 0;
            while (sent_count < o.messages.len) {
                const m = &o.messages[sent_count];
                var dest: Threaded.PosixAddress = undefined;
                const dest_len = Threaded.addressToPosix(m.address, &dest);
                var wbuf: [1]win32.WSABUF = .{.{
                    .len = @intCast(@min(m.data_len, std.math.maxInt(u32))),
                    .buf = @constCast(m.data_ptr),
                }};
                var comp: Iocp.Completion = .{ .callback = undefined };
                const rc = win32.WSASendTo(sock, &wbuf, 1, null, flags, &dest.any, @intCast(dest_len), &comp.overlapped, null);
                if (rc == win32.SOCKET_ERROR) {
                    const werr = win32.WSAGetLastError();
                    if (werr != win32.WSA_IO_PENDING) {
                        return .{ .net_send = .{ mapWinNetSendError(werr), sent_count } };
                    }
                }
                self.poller.recordPending(&comp);
                const res = try self.awaitIocpPending(o.socket_handle, &comp, maybe_deadline_ns);
                if (res.nt_status != .SUCCESS) {
                    var transferred: win32.DWORD = 0;
                    var out_flags: win32.DWORD = 0;
                    _ = win32.WSAGetOverlappedResult(sock, &comp.overlapped, &transferred, .FALSE, &out_flags);
                    return .{ .net_send = .{ mapWinNetSendError(win32.WSAGetLastError()), sent_count } };
                }
                m.data_len = res.bytes;
                sent_count += 1;
            }
            return .{ .net_send = .{ null, sent_count } };
        },
    }
}

// ── `Io.Batch` Implementation (`batchAwaitAsync` / `batchAwaitConcurrent` / `batchCancel`) ──

fn acquireBatchOp(self: *Shard) Allocator.Error!*BatchOp {
    if (self.batch_op_free) |op| {
        self.batch_op_free = op.free_next;
        op.* = .{};
        return op;
    }
    const op = try self.gpa.create(BatchOp);
    op.* = .{};
    return op;
}

fn releaseBatchOp(self: *Shard, op: *BatchOp) void {
    op.* = .{ .free_next = self.batch_op_free };
    self.batch_op_free = op;
}

fn batchAppendCompleted(batch: *Io.Batch, idx: u32, res: Io.Operation.Result) void {
    const oi = Io.Operation.OptionalIndex.fromIndex(idx);
    switch (batch.completed.tail) {
        .none => batch.completed.head = oi,
        else => |tail_oi| batch.storage[tail_oi.toIndex()].completion.node.next = oi,
    }
    batch.storage[idx] = .{ .completion = .{ .node = .{ .next = .none }, .result = res } };
    batch.completed.tail = oi;
}

fn batchRemovePending(batch: *Io.Batch, idx: u32) void {
    const node = batch.storage[idx].pending.node;
    switch (node.prev) {
        .none => batch.pending.head = node.next,
        else => |prev_oi| batch.storage[prev_oi.toIndex()].pending.node.next = node.next,
    }
    switch (node.next) {
        .none => batch.pending.tail = node.prev,
        else => |next_oi| batch.storage[next_oi.toIndex()].pending.node.prev = node.prev,
    }
}

fn batchReturnUnused(batch: *Io.Batch, idx: u32) void {
    const oi = Io.Operation.OptionalIndex.fromIndex(idx);
    const tail = batch.unused.tail;
    switch (tail) {
        .none => batch.unused.head = oi,
        else => |tail_oi| batch.storage[tail_oi.toIndex()].unused.next = oi,
    }
    batch.storage[idx] = .{ .unused = .{ .prev = tail, .next = .none } };
    batch.unused.tail = oi;
}

fn wakeBatchAwaiter(self: *Shard, batch: *Io.Batch) void {
    if (batch.userdata) |raw_ptr| {
        const waiting: *Task = @ptrCast(@alignCast(raw_ptr));
        self.enqueueReady(waiting);
    }
}

fn finishBatchOp(bop: *BatchOp) void {
    const self = bop.shard;
    const batch = bop.batch;
    const idx = bop.slot_index;
    const maybe_res = bop.result;
    const was_canceled = bop.canceled;

    batchRemovePending(batch, idx);
    if (!was_canceled and maybe_res != null) {
        batchAppendCompleted(batch, idx, maybe_res.?);
    } else {
        batchReturnUnused(batch, idx);
    }
    self.releaseBatchOp(bop);
    self.wakeBatchAwaiter(batch);
}

fn onBatchEpollReady(ctx: ?*anyopaque) void {
    if (builtin.os.tag != .linux) return;
    const bop: *BatchOp = @ptrCast(@alignCast(ctx.?));
    if (bop.canceled) {
        bop.done = true;
        finishBatchOp(bop);
        return;
    }
    switch (tryExecuteLinuxNonblocking(bop.op)) {
        .done => |res| {
            switch (bop.op) {
                .net_read => |o| bop.shard.poller.cascadeNext(o.socket_handle, .read),
                .net_receive => |o| bop.shard.poller.cascadeNext(o.socket_handle, .read),
                .net_write => |o| bop.shard.poller.cascadeNext(o.socket_handle, .write),
                .net_send => |o| bop.shard.poller.cascadeNext(o.socket_handle, .write),
                .file_read_streaming => |o| bop.shard.poller.cascadeNext(o.file.handle, .read),
                .file_write_streaming => |o| bop.shard.poller.cascadeNext(o.file.handle, .write),
                .device_io_control => {},
            }
            bop.result = res;
            bop.done = true;
            finishBatchOp(bop);
        },
        .would_block => |wb| {
            // Spurious edge: re-arm waiter.
            bop.backend.epoll.waiter.ready = false;
            bop.shard.poller.waitIo(bop.shard.gpa, wb.fd, wb.dir, &bop.backend.epoll.waiter) catch {
                bop.canceled = true;
                bop.done = true;
                finishBatchOp(bop);
            };
        },
        .needs_pool => unreachable,
    }
}

fn onBatchBlockingExec(job: *BlockingPool.Job, threaded: *Threaded) void {
    const bop: *BatchOp = @ptrCast(@alignCast(job.notify_ctx.?));
    bop.result = blockingOperateOnThreaded(threaded, bop.op);
}

fn onBatchBlockingNotify(ctx: ?*anyopaque) void {
    const bop: *BatchOp = @ptrCast(@alignCast(ctx.?));
    bop.shard.notifyFromAnyThread();
}

fn onBatchBlockingResume(node: *ResumeLane.Node) void {
    const bop: *BatchOp = @ptrCast(@alignCast(node.ctx.?));
    bop.done = true;
    finishBatchOp(bop);
}

fn onBatchIocpComplete(comp: if (builtin.os.tag == .windows) *Iocp.Completion else *anyopaque) void {
    if (builtin.os.tag != .windows) return;
    const bop: *BatchOp = @ptrCast(@alignCast(comp.ctx.?));
    bop.done = true;
    if (!bop.canceled) {
        const bytes = comp.bytes_transferred;
        const ok = comp.nt_status == .SUCCESS;
        bop.result = switch (bop.op) {
            .file_read_streaming => .{
                .file_read_streaming = if (!ok)
                    (if (comp.nt_status == .END_OF_FILE) error.EndOfStream else error.InputOutput)
                else if (bytes == 0)
                    error.EndOfStream
                else
                    bytes,
            },
            .file_write_streaming => .{ .file_write_streaming = if (!ok) error.InputOutput else bytes },
            .net_read => .{ .net_read = if (!ok) error.ConnectionResetByPeer else .{ .data_len = bytes } },
            .net_write => .{ .net_write = if (!ok) error.ConnectionResetByPeer else bytes },
            .net_receive => |o| blk: {
                if (!ok) break :blk .{ .net_receive = .{ error.ConnectionResetByPeer, 0 } };
                const out_msg = &o.message_buffer[0];
                out_msg.from = if (bop.backend.iocp.from_len > 0)
                    Threaded.addressFromPosix(&bop.backend.iocp.from_addr)
                else
                    .{ .ip4 = .loopback(0) };
                out_msg.data = o.data_buffer[0..bytes];
                out_msg.control = &.{};
                out_msg.flags = .{
                    .eor = false,
                    .trunc = false,
                    .ctrunc = false,
                    .oob = false,
                    .errqueue = false,
                };
                break :blk .{ .net_receive = .{ null, 1 } };
            },
            .net_send => |o| blk: {
                if (!ok) break :blk .{ .net_send = .{ error.ConnectionResetByPeer, 0 } };
                o.messages[0].data_len = bytes;
                break :blk .{ .net_send = .{ null, 1 } };
            },
            .device_io_control => unreachable,
        };
    }
    finishBatchOp(bop);
}

fn submitBatchList(self: *Shard, batch: *Io.Batch) Io.ConcurrentError!void {
    var cur_oi = batch.submitted.head;
    if (cur_oi == .none) return;
    errdefer batch.submitted.head = cur_oi;

    while (cur_oi != .none) {
        const idx = cur_oi.toIndex();
        const op = batch.storage[idx].submission.operation;
        const next_oi = batch.storage[idx].submission.node.next;

        if (builtin.os.tag == .linux) {
            switch (tryExecuteLinuxNonblocking(op)) {
                .done => |res| {
                    batchAppendCompleted(batch, idx, res);
                    cur_oi = next_oi;
                    continue;
                },
                .would_block => |wb| {
                    const bop = self.acquireBatchOp() catch return error.ConcurrencyUnavailable;
                    bop.* = .{
                        .shard = self,
                        .batch = batch,
                        .slot_index = idx,
                        .op = op,
                        .backend = .{ .epoll = .{
                            .fd = wb.fd,
                            .dir = wb.dir,
                            .waiter = .{ .ctx = bop, .wake_fn = onBatchEpollReady },
                        } },
                    };

                    // Append to `batch.pending` BEFORE arming so immediate sticky HUP wake works!
                    const prev_tail = batch.pending.tail;
                    switch (prev_tail) {
                        .none => batch.pending.head = cur_oi,
                        else => |t_oi| batch.storage[t_oi.toIndex()].pending.node.next = cur_oi,
                    }
                    batch.storage[idx] = .{ .pending = .{
                        .node = .{ .prev = prev_tail, .next = .none },
                        .tag = op,
                        .userdata = .{ @intFromPtr(bop), 0, 0, 0, 0, 0, 0 },
                    } };
                    batch.pending.tail = cur_oi;

                    self.poller.waitIo(self.gpa, wb.fd, wb.dir, &bop.backend.epoll.waiter) catch |err| switch (err) {
                        error.PollUnsupported => {
                            // Switch `bop` to `BlockingPool` job.
                            try self.submitBatchOpBlocking(bop);
                        },
                        else => {
                            batchRemovePending(batch, idx);
                            self.releaseBatchOp(bop);
                            return error.ConcurrencyUnavailable;
                        },
                    };
                    cur_oi = next_oi;
                    continue;
                },
                .needs_pool => {
                    const bop = self.acquireBatchOp() catch return error.ConcurrencyUnavailable;
                    bop.* = .{
                        .shard = self,
                        .batch = batch,
                        .slot_index = idx,
                        .op = op,
                    };
                    const prev_tail = batch.pending.tail;
                    switch (prev_tail) {
                        .none => batch.pending.head = cur_oi,
                        else => |t_oi| batch.storage[t_oi.toIndex()].pending.node.next = cur_oi,
                    }
                    batch.storage[idx] = .{ .pending = .{
                        .node = .{ .prev = prev_tail, .next = .none },
                        .tag = op,
                        .userdata = .{ @intFromPtr(bop), 0, 0, 0, 0, 0, 0 },
                    } };
                    batch.pending.tail = cur_oi;

                    self.submitBatchOpBlocking(bop) catch {
                        batchRemovePending(batch, idx);
                        self.releaseBatchOp(bop);
                        return error.ConcurrencyUnavailable;
                    };
                    cur_oi = next_oi;
                    continue;
                },
            }
        } else {
            // Windows IOCP / BlockingPool batch submission
            const bop = self.acquireBatchOp() catch return error.ConcurrencyUnavailable;
            bop.* = .{
                .shard = self,
                .batch = batch,
                .slot_index = idx,
                .op = op,
            };
            const prev_tail = batch.pending.tail;
            switch (prev_tail) {
                .none => batch.pending.head = cur_oi,
                else => |t_oi| batch.storage[t_oi.toIndex()].pending.node.next = cur_oi,
            }
            batch.storage[idx] = .{ .pending = .{
                .node = .{ .prev = prev_tail, .next = .none },
                .tag = op,
                .userdata = .{ @intFromPtr(bop), 0, 0, 0, 0, 0, 0 },
            } };
            batch.pending.tail = cur_oi;

            self.submitBatchOpWindows(bop) catch {
                batchRemovePending(batch, idx);
                self.releaseBatchOp(bop);
                return error.ConcurrencyUnavailable;
            };
            cur_oi = next_oi;
        }
    }
    batch.submitted = .empty;
}

fn submitBatchOpBlocking(self: *Shard, bop: *BatchOp) Io.ConcurrentError!void {
    bop.backend = .{ .blocking = .{
        .run_fn = onBatchBlockingExec,
        .resume_lane = &self.resume_lane,
        .resume_node = .{ .ctx = bop, .callback = onBatchBlockingResume },
        .notify_ctx = bop,
        .notify_fn = onBatchBlockingNotify,
    } };
    switch (self.blocking_pool.submit(&bop.backend.blocking)) {
        .accepted => {},
        .queue_full, .closed => return error.ConcurrencyUnavailable,
    }
}

fn submitBatchOpWindows(self: *Shard, bop: *BatchOp) Io.ConcurrentError!void {
    if (builtin.os.tag != .windows) unreachable;
    switch (bop.op) {
        .device_io_control => return self.submitBatchOpBlocking(bop),
        .file_read_streaming => |o| {
            if (!o.file.flags.nonblocking) return self.submitBatchOpBlocking(bop);
            const dest = for (o.data) |b| {
                if (b.len > 0) break b;
            } else {
                bop.result = .{ .file_read_streaming = 0 };
                bop.done = true;
                finishBatchOp(bop);
                return;
            };
            bop.backend = .{ .iocp = .{
                .handle = o.file.handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            const ok = win32.ReadFile(o.file.handle, dest.ptr, @intCast(@min(dest.len, std.math.maxInt(u32))), null, &bop.backend.iocp.comp.overlapped);
            if (!ok.toBool()) {
                const err = @backingInt(windows.GetLastError());
                if (err != win32.ERROR_IO_PENDING) {
                    bop.result = .{ .file_read_streaming = mapWinFileReadError(err) };
                    bop.done = true;
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
        },
        .file_write_streaming => |o| {
            if (!o.file.flags.nonblocking) return self.submitBatchOpBlocking(bop);
            const src: []const u8 = blk: {
                if (o.header.len > 0) break :blk o.header;
                if (o.data.len == 0) {
                    bop.result = .{ .file_write_streaming = 0 };
                    bop.done = true;
                    finishBatchOp(bop);
                    return;
                }
                for (o.data[0 .. o.data.len - 1]) |b| if (b.len > 0) break :blk b;
                if (o.splat > 0 and o.data[o.data.len - 1].len > 0) break :blk o.data[o.data.len - 1];
                bop.result = .{ .file_write_streaming = 0 };
                bop.done = true;
                finishBatchOp(bop);
                return;
            };
            bop.backend = .{ .iocp = .{
                .handle = o.file.handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            const ok = win32.WriteFile(o.file.handle, src.ptr, @intCast(@min(src.len, std.math.maxInt(u32))), null, &bop.backend.iocp.comp.overlapped);
            if (!ok.toBool()) {
                const err = @backingInt(windows.GetLastError());
                if (err != win32.ERROR_IO_PENDING) {
                    bop.result = .{ .file_write_streaming = mapWinFileWriteError(err) };
                    bop.done = true;
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
        },
        .net_read => |o| {
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            var n_bufs: u32 = 0;
            for (o.data) |b| {
                if (n_bufs == bop.backend.iocp.wsabufs.len) break;
                if (b.len > 0) {
                    bop.backend.iocp.wsabufs[n_bufs] = .{ .len = @intCast(@min(b.len, std.math.maxInt(u32))), .buf = b.ptr };
                    n_bufs += 1;
                }
            }
            if (n_bufs == 0) {
                bop.result = .{ .net_read = .{ .data_len = 0 } };
                bop.done = true;
                finishBatchOp(bop);
                return;
            }
            const sock = win32.handleToSocket(o.socket_handle);
            const rc = win32.WSARecv(sock, &bop.backend.iocp.wsabufs, n_bufs, null, &bop.backend.iocp.dw_flags, &bop.backend.iocp.comp.overlapped, null);
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    bop.result = .{ .net_read = mapWinNetReadError(werr) };
                    bop.done = true;
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
        },
        .net_write => |o| {
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            const n_bufs = fillWriteWsabufs(&bop.backend.iocp.wsabufs, &bop.backend.iocp.splat_buf, o.header, o.data, o.splat);
            if (n_bufs == 0) {
                bop.result = .{ .net_write = 0 };
                bop.done = true;
                finishBatchOp(bop);
                return;
            }
            const sock = win32.handleToSocket(o.socket_handle);
            const rc = win32.WSASend(sock, &bop.backend.iocp.wsabufs, n_bufs, null, 0, &bop.backend.iocp.comp.overlapped, null);
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    bop.result = .{ .net_write = mapWinNetWriteError(werr) };
                    bop.done = true;
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
        },
        .net_receive => |o| {
            if (o.message_buffer.len == 0) {
                bop.result = .{ .net_receive = .{ null, 0 } };
                bop.done = true;
                finishBatchOp(bop);
                return;
            }
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            bop.backend.iocp.wsabufs[0] = .{ .len = @intCast(@min(o.data_buffer.len, std.math.maxInt(u32))), .buf = o.data_buffer.ptr };
            const sock = win32.handleToSocket(o.socket_handle);
            const rc = win32.WSARecvFrom(sock, &bop.backend.iocp.wsabufs, 1, null, &bop.backend.iocp.dw_flags, &bop.backend.iocp.from_addr.any, &bop.backend.iocp.from_len, &bop.backend.iocp.comp.overlapped, null);
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    bop.result = .{ .net_receive = .{ mapWinNetRecvError(werr), 0 } };
                    bop.done = true;
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
        },
        .net_send => |o| {
            if (o.messages.len == 0) {
                bop.result = .{ .net_send = .{ null, 0 } };
                bop.done = true;
                finishBatchOp(bop);
                return;
            }
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            const m = &o.messages[0];
            bop.backend.iocp.wsabufs[0] = .{ .len = @intCast(@min(m.data_len, std.math.maxInt(u32))), .buf = @constCast(m.data_ptr) };
            const dest_len = Threaded.addressToPosix(m.address, &bop.backend.iocp.from_addr);
            const sock = win32.handleToSocket(o.socket_handle);
            const rc = win32.WSASendTo(sock, &bop.backend.iocp.wsabufs, 1, null, 0, &bop.backend.iocp.from_addr.any, @intCast(dest_len), &bop.backend.iocp.comp.overlapped, null);
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    bop.result = .{ .net_send = .{ mapWinNetSendError(werr), 0 } };
                    bop.done = true;
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
        },
    }
}

fn vtableBatchAwaitAsync(userdata: ?*anyopaque, batch: *Io.Batch) Io.Cancelable!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    self.submitBatchList(batch) catch {
        // `awaitAsync` cannot return `ConcurrencyUnavailable`: execute one submission serially.
        if (batch.submitted.head != .none) {
            const idx = batch.submitted.head.toIndex();
            const op = batch.storage[idx].submission.operation;
            const next_oi = batch.storage[idx].submission.node.next;
            batch.submitted.head = next_oi;
            if (next_oi == .none) batch.submitted.tail = .none;
            const res = try vtableOperate(self, op);
            batchAppendCompleted(batch, idx, res);
            return;
        }
    };

    if (batch.completed.head != .none or batch.pending.head == .none) return;
    return self.waitBatchCompletion(batch, .none) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        error.Timeout => unreachable,
    };
}

fn vtableBatchAwaitConcurrent(
    userdata: ?*anyopaque,
    batch: *Io.Batch,
    timeout: Io.Timeout,
) Io.Batch.AwaitConcurrentError!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    try self.submitBatchList(batch);

    if (batch.completed.head != .none or batch.pending.head == .none) return;
    return self.waitBatchCompletion(batch, timeout);
}

fn waitBatchCompletion(self: *Shard, batch: *Io.Batch, timeout: Io.Timeout) (Io.Cancelable || Io.Timeout.Error)!void {
    const maybe_dl = time.resolveAwakeDeadlineNs(timeout, self, vtableNow);
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

    // Trigger cancellation across all currently pending operations.
    var cur_oi = batch.pending.head;
    while (cur_oi != .none) {
        const idx = cur_oi.toIndex();
        const next_oi = batch.storage[idx].pending.node.next;
        const bop: *BatchOp = @ptrFromInt(batch.storage[idx].pending.userdata[0]);
        bop.canceled = true;

        switch (builtin.os.tag) {
            .linux => switch (bop.backend) {
                .none => {},
                .epoll => |*ep| {
                    self.poller.cancelWaiter(ep.fd, ep.dir, &ep.waiter);
                    finishBatchOp(bop);
                },
                .blocking => |*job| {
                    if (self.blocking_pool.tryCancel(job) == .canceled_queued) {
                        finishBatchOp(bop);
                    }
                },
            },
            .windows => switch (bop.backend) {
                .none => {},
                .iocp => |*io_st| {
                    Iocp.requestCancelIo(io_st.handle, &io_st.comp);
                },
                .blocking => |*job| {
                    if (self.blocking_pool.tryCancel(job) == .canceled_queued) {
                        finishBatchOp(bop);
                    }
                },
            },
            else => comptime unreachable,
        }
        cur_oi = next_oi;
    }

    // Uncancelably drain all remaining in-flight `IOCP` OVERLAPPEDs & running BlockingPool jobs!
    if (batch.pending.head != .none) {
        batch.userdata = self.current_task;
        defer batch.userdata = null;
        while (batch.pending.head != .none) {
            self.suspendCurrent();
        }
    }
    batch.userdata = null;
}

// ── VTable: Random & Stderr ─────────────────────────────────────────────────

fn vtableRandom(userdata: ?*anyopaque, buffer: []u8) void {
    const self = fromUserdata(userdata);
    if (!self.csprng.isInitialized()) {
        var seed: [Threaded.Csprng.seed_len]u8 = undefined;
        vtableRandomSecure(self, &seed) catch {
            Threaded.fallbackSeed(self, &seed);
        };
        self.csprng.rng = .init(seed);
    }
    self.csprng.rng.fill(buffer);
}

fn vtableRandomSecure(userdata: ?*anyopaque, buffer: []u8) Io.RandomSecureError!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    if (builtin.os.tag == .linux) {
        var offset: usize = 0;
        while (offset < buffer.len) {
            const rc = linux.getrandom(buffer[offset..].ptr, buffer.len - offset, 0);
            switch (linux.errno(rc)) {
                .SUCCESS => offset += rc,
                .INTR => continue,
                else => return error.EntropyUnavailable,
            }
        }
        return;
    }
    return self.callBlocking(true, struct {
        fn run(t: *Threaded, buf: []u8) Io.RandomSecureError!void {
            const t_io = t.io();
            return t_io.vtable.randomSecure(t_io.userdata, buf);
        }
    }.run, .{buffer});
}

fn ensureStderrInitialized(self: *Shard, maybe_mode: ?Io.Terminal.Mode) Io.Cancelable!void {
    const io_inst = self.ioUnchecked();
    if (!self.stderr_initialized) {
        const stderr_file = File.stderr();
        self.stderr_mode = maybe_mode orelse blk: {
            const is_tty = try stderr_file.isTty(io_inst);
            break :blk if (is_tty) .escape_codes else .no_color;
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
        if (maybe_mode) |m| self.stderr_mode = m;
    }
}

fn vtableLockStderr(userdata: ?*anyopaque, terminal_mode: ?Io.Terminal.Mode) Io.Cancelable!Io.LockedStderr {
    const self = fromUserdata(userdata);
    const io_inst = self.ioUnchecked();
    try self.stderr_mutex.lock(io_inst);
    errdefer self.stderr_mutex.unlock(io_inst);
    try self.ensureStderrInitialized(terminal_mode);
    return .{
        .file_writer = &self.stderr_writer,
        .terminal_mode = self.stderr_mode,
    };
}

fn vtableTryLockStderr(userdata: ?*anyopaque, terminal_mode: ?Io.Terminal.Mode) Io.Cancelable!?Io.LockedStderr {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();
    if (!self.stderr_mutex.tryLock()) return null;
    errdefer self.stderr_mutex.unlock(self.ioUnchecked());
    try self.ensureStderrInitialized(terminal_mode);
    return .{
        .file_writer = &self.stderr_writer,
        .terminal_mode = self.stderr_mode,
    };
}

fn vtableUnlockStderr(userdata: ?*anyopaque) void {
    const self = fromUserdata(userdata);
    self.stderr_mutex.unlock(self.ioUnchecked());
}

// ── VTable: `Dir` & `File` & `MemoryMap` & `Process` Delegation via `BlockingPool` ──

fn forwardThreaded(comptime field: []const u8, comptime cancelable: bool) @FieldType(Io.VTable, field) {
    const FnType = @typeInfo(@FieldType(Io.VTable, field)).pointer.child;
    const fn_info = @typeInfo(FnType).@"fn";
    const Ret = fn_info.return_type.?;

    return &struct {
        fn buildExtraTuple(all_args: std.meta.ArgsTuple(FnType)) ExtraTupleType() {
            var extra: ExtraTupleType() = undefined;
            inline for (1..fn_info.param_types.len) |i| {
                extra[i - 1] = all_args[i];
            }
            return extra;
        }

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

        const impl = switch (fn_info.param_types.len) {
            1 => struct {
                fn call(p0: fn_info.param_types[0].?) Ret {
                    const self = fromUserdata(p0);
                    return self.callBlocking(cancelable, runOnThreaded, .{buildExtraTuple(.{p0})});
                }
            }.call,
            2 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?) Ret {
                    const self = fromUserdata(p0);
                    return self.callBlocking(cancelable, runOnThreaded, .{buildExtraTuple(.{ p0, p1 })});
                }
            }.call,
            3 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?, p2: fn_info.param_types[2].?) Ret {
                    const self = fromUserdata(p0);
                    return self.callBlocking(cancelable, runOnThreaded, .{buildExtraTuple(.{ p0, p1, p2 })});
                }
            }.call,
            4 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?, p2: fn_info.param_types[2].?, p3: fn_info.param_types[3].?) Ret {
                    const self = fromUserdata(p0);
                    return self.callBlocking(cancelable, runOnThreaded, .{buildExtraTuple(.{ p0, p1, p2, p3 })});
                }
            }.call,
            5 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?, p2: fn_info.param_types[2].?, p3: fn_info.param_types[3].?, p4: fn_info.param_types[4].?) Ret {
                    const self = fromUserdata(p0);
                    return self.callBlocking(cancelable, runOnThreaded, .{buildExtraTuple(.{ p0, p1, p2, p3, p4 })});
                }
            }.call,
            6 => struct {
                fn call(p0: fn_info.param_types[0].?, p1: fn_info.param_types[1].?, p2: fn_info.param_types[2].?, p3: fn_info.param_types[3].?, p4: fn_info.param_types[4].?, p5: fn_info.param_types[5].?) Ret {
                    const self = fromUserdata(p0);
                    return self.callBlocking(cancelable, runOnThreaded, .{buildExtraTuple(.{ p0, p1, p2, p3, p4, p5 })});
                }
            }.call,
            else => @compileError("unexpected VTable arity"),
        };
    }.impl;
}

fn vtableFileClose(userdata: ?*anyopaque, files: []const File) void {
    const self = fromUserdata(userdata);
    if (builtin.os.tag == .linux) {
        for (files) |f| {
            self.poller.unregisterFd(f.handle);
            Threaded.closeFd(f.handle);
        }
        return;
    }
    self.callBlocking(false, struct {
        fn run(t: *Threaded, fs_slice: []const File) void {
            const t_io = t.io();
            t_io.vtable.fileClose(t_io.userdata, fs_slice);
        }
    }.run, .{files});
}

fn vtableDirClose(userdata: ?*anyopaque, dirs: []const Dir) void {
    const self = fromUserdata(userdata);
    if (builtin.os.tag == .linux) {
        for (dirs) |d| {
            self.poller.unregisterFd(d.handle);
            Threaded.closeFd(d.handle);
        }
        return;
    }
    self.callBlocking(false, struct {
        fn run(t: *Threaded, ds: []const Dir) void {
            const t_io = t.io();
            t_io.vtable.dirClose(t_io.userdata, ds);
        }
    }.run, .{dirs});
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
    _ = file;
    _ = header;
    _ = file_reader;
    _ = limit;
    // Returning `Unimplemented` instructs `Io.File.Writer` to perform the copy
    // safely via `file_reader` + `operate` on `shard.io()` without cross-thread `Io` access.
    return error.Unimplemented;
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
    _ = file;
    _ = header;
    _ = file_reader;
    _ = limit;
    _ = offset;
    return error.Unimplemented;
}

fn vtableProcessSpawn(
    userdata: ?*anyopaque,
    options: std.process.SpawnOptions,
) std.process.SpawnError!std.process.Child {
    const self = fromUserdata(userdata);
    // Strip `progress_node` before offloading so `Threaded` never asserts against
    // a global `std.Progress` initialized with `shard.io()`!
    var sanitized = options;
    sanitized.progress_node = .none;

    var child = try self.callBlocking(true, struct {
        fn run(t: *Threaded, opts: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
            const t_io = t.io();
            return t_io.vtable.processSpawn(t_io.userdata, opts);
        }
    }.run, .{sanitized});

    if (builtin.os.tag == .linux) {
        inline for (.{ "stdin", "stdout", "stderr" }) |pipe_field| {
            if (@field(child, pipe_field)) |*f| {
                const fl = linux.fcntl(f.handle, linux.F.GETFL, 0);
                if (linux.errno(fl) == .SUCCESS) {
                    const nonblock_bit: usize = @as(u32, @bitCast(linux.O{ .NONBLOCK = true }));
                    if (linux.errno(linux.fcntl(f.handle, linux.F.SETFL, fl | nonblock_bit)) == .SUCCESS) {
                        f.flags.nonblocking = true;
                    }
                }
            }
        }
    }
    return child;
}

fn unregisterChildPipesLinux(self: *Shard, child: *const std.process.Child) void {
    if (builtin.os.tag != .linux) return;
    if (child.stdin) |f| self.poller.unregisterFd(f.handle);
    if (child.stdout) |f| self.poller.unregisterFd(f.handle);
    if (child.stderr) |f| self.poller.unregisterFd(f.handle);
}

fn vtableChildWait(
    userdata: ?*anyopaque,
    child: *std.process.Child,
) std.process.Child.WaitError!std.process.Child.Term {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    // On Linux, wait asynchronously on `pidfd_open` via `Epoll` first so waiting
    // on long-lived child processes consumes zero `BlockingPool` threads!
    if (builtin.os.tag == .linux) {
        if (child.id) |pid| {
            const nonblock_flag: u32 = @bitCast(linux.O{ .NONBLOCK = true });
            const pfd_rc = linux.pidfd_open(pid, nonblock_flag);
            if (linux.errno(pfd_rc) == .SUCCESS) {
                const pfd: linux.fd_t = @intCast(pfd_rc);
                defer {
                    self.poller.unregisterFd(pfd);
                    Threaded.closeFd(pfd);
                }
                self.waitLinuxFd(pfd, .read, null) catch |err| switch (err) {
                    error.Canceled => {
                        self.unregisterChildPipesLinux(child);
                        if (child.stdin) |f| Threaded.closeFd(f.handle);
                        if (child.stdout) |f| Threaded.closeFd(f.handle);
                        if (child.stderr) |f| Threaded.closeFd(f.handle);
                        child.stdin = null;
                        child.stdout = null;
                        child.stderr = null;
                        child.id = null;
                        return error.Canceled;
                    },
                    else => {},
                };
            }
        }
    }

    self.unregisterChildPipesLinux(child);
    return self.callBlocking(true, struct {
        fn run(t: *Threaded, c: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
            const t_io = t.io();
            return t_io.vtable.childWait(t_io.userdata, c);
        }
    }.run, .{child});
}

fn vtableChildKill(userdata: ?*anyopaque, child: *std.process.Child) void {
    const self = fromUserdata(userdata);
    self.unregisterChildPipesLinux(child);
    self.callBlocking(false, struct {
        fn run(t: *Threaded, c: *std.process.Child) void {
            const t_io = t.io();
            t_io.vtable.childKill(t_io.userdata, c);
        }
    }.run, .{child});
}

// ── VTable: Networking (`net*` — Native `epoll` on Linux, Native `IOCP` + `Acceptor` on Windows) ──

fn vtableNetListenIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.ListenOptions,
) net.IpAddress.ListenError!net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    switch (builtin.os.tag) {
        .linux => {
            const family = Threaded.posixAddressFamily(address);
            const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
            const sock_rc = linux.socket(family, mode | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, protocol);
            const fd: linux.fd_t = switch (linux.errno(sock_rc)) {
                .SUCCESS => @intCast(sock_rc),
                .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
                .PROTOTYPE => return error.SocketModeUnsupported,
                .INVAL => return error.ProtocolUnsupportedBySystem,
                .MFILE => return error.ProcessFdQuotaExceeded,
                .NFILE => return error.SystemFdQuotaExceeded,
                .NOMEM, .NOBUFS => return error.SystemResources,
                .ACCES => return error.AccessDenied,
                else => |err| return posix.unexpectedErrno(err),
            };
            errdefer Threaded.closeFd(fd);

            if (options.reuse_address) {
                const one: c_int = 1;
                try setSockOptLinux(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, std.mem.asBytes(&one));
                try setSockOptLinux(fd, linux.SOL.SOCKET, linux.SO.REUSEPORT, std.mem.asBytes(&one));
            }

            var storage: Threaded.PosixAddress = undefined;
            const addr_len = Threaded.addressToPosix(address, &storage);
            switch (linux.errno(linux.bind(fd, &storage.any, addr_len))) {
                .SUCCESS => {},
                .ADDRINUSE => return error.AddressInUse,
                .ADDRNOTAVAIL => return error.AddressUnavailable,
                .ACCES => return error.AccessDenied,
                .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                .NOMEM, .NOBUFS => return error.SystemResources,
                else => |err| return posix.unexpectedErrno(err),
            }

            switch (linux.errno(linux.listen(fd, options.kernel_backlog))) {
                .SUCCESS => {},
                .ADDRINUSE => return error.AddressInUse,
                .OPNOTSUPP => return error.SocketModeUnsupported,
                .NOMEM, .NOBUFS => return error.SystemResources,
                else => |err| return posix.unexpectedErrno(err),
            }

            var bound: Threaded.PosixAddress = undefined;
            var bound_len: posix.socklen_t = @sizeOf(Threaded.PosixAddress);
            switch (linux.errno(linux.getsockname(fd, &bound.any, &bound_len))) {
                .SUCCESS => {},
                else => |err| return posix.unexpectedErrno(err),
            }
            return .{
                .handle = fd,
                .address = Threaded.addressFromPosix(&bound),
            };
        },
        .windows => {
            if (options.reuse_address and self.shard_count > 1) {
                if (self.acceptor) |acc| {
                    return acc.openOrJoinListener(self.id, address, options);
                }
            }
            const family = Threaded.posixAddressFamily(address);
            const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
            const sock = win32.WSASocketW(
                family,
                @intCast(mode),
                @intCast(protocol),
                null,
                0,
                win32.WSA_FLAG_OVERLAPPED | win32.WSA_FLAG_NO_HANDLE_INHERIT,
            );
            if (sock == win32.INVALID_SOCKET) return mapWinListenErr(win32.WSAGetLastError());
            errdefer _ = win32.closesocket(sock);

            const excl: c_int = 1;
            if (win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.SO_EXCLUSIVEADDRUSE, @ptrCast(&excl), @sizeOf(c_int)) == win32.SOCKET_ERROR) {
                return mapWinListenErr(win32.WSAGetLastError());
            }

            var storage: Threaded.PosixAddress = undefined;
            const addr_len = Threaded.addressToPosix(address, &storage);
            if (win32.bind(sock, &storage.any, @intCast(addr_len)) == win32.SOCKET_ERROR) {
                return mapWinListenErr(win32.WSAGetLastError());
            }
            const backlog: c_int = @intCast(@min(options.kernel_backlog, std.math.maxInt(c_int)));
            if (win32.listen(sock, backlog) == win32.SOCKET_ERROR) {
                return mapWinListenErr(win32.WSAGetLastError());
            }
            var bound: Threaded.PosixAddress = undefined;
            var bound_len: c_int = @sizeOf(Threaded.PosixAddress);
            if (win32.getsockname(sock, &bound.any, &bound_len) == win32.SOCKET_ERROR) {
                return mapWinListenErr(win32.WSAGetLastError());
            }
            const handle = win32.socketToHandle(sock);
            try self.poller.associateHandle(handle, 0);
            return .{
                .handle = handle,
                .address = Threaded.addressFromPosix(&bound),
            };
        },
        else => comptime unreachable,
    }
}

fn vtableNetAccept(
    userdata: ?*anyopaque,
    server: net.Socket.Handle,
    options: net.Server.AcceptOptions,
) net.Server.AcceptError!net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    switch (builtin.os.tag) {
        .linux => {
            while (true) {
                var peer: Threaded.PosixAddress = undefined;
                var peer_len: posix.socklen_t = @sizeOf(Threaded.PosixAddress);
                const rc = linux.accept4(server, &peer.any, &peer_len, linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC);
                switch (linux.errno(rc)) {
                    .SUCCESS => {
                        self.poller.cascadeNext(server, .read);
                        const addr = if (peer_len > 0 and (peer.any.family == posix.AF.INET or peer.any.family == posix.AF.INET6))
                            Threaded.addressFromPosix(&peer)
                        else
                            net.IpAddress{ .ip4 = .loopback(0) };
                        return .{
                            .handle = @intCast(rc),
                            .address = addr,
                        };
                    },
                    .INTR => continue,
                    .AGAIN => {
                        self.waitLinuxFd(server, .read, null) catch |err| switch (err) {
                            error.Canceled => return error.Canceled,
                            error.OutOfMemory, error.SystemResources => return error.SystemResources,
                            error.ProcessFdQuotaExceeded => return error.ProcessFdQuotaExceeded,
                            error.Timeout, error.PollUnsupported => unreachable,
                            else => return error.Unexpected,
                        };
                    },
                    .CONNABORTED => return error.ConnectionAborted,
                    .MFILE => return error.ProcessFdQuotaExceeded,
                    .NFILE => return error.SystemFdQuotaExceeded,
                    .NOMEM, .NOBUFS => return error.SystemResources,
                    .INVAL => return error.SocketNotListening,
                    .PERM => return error.BlockedByFirewall,
                    .PROTO => return error.ProtocolFailure,
                    .NETDOWN => return error.NetworkDown,
                    else => |err| return posix.unexpectedErrno(err),
                }
            }
        },
        .windows => {
            if (self.acceptor) |acc| {
                if (acc.isSharedToken(server)) |tok_idx| {
                    return self.acceptFromWindowsAcceptor(acc, acc.tokenListenerId(tok_idx));
                }
            }
            const listen_sock = win32.handleToSocket(server);
            var local_st: Threaded.PosixAddress = undefined;
            var local_len: c_int = @sizeOf(Threaded.PosixAddress);
            if (win32.getsockname(listen_sock, &local_st.any, &local_len) == win32.SOCKET_ERROR) {
                return error.SocketNotListening;
            }
            const family = local_st.any.family;
            const mode, const protocol = Threaded.posixSocketModeProtocol(family, options.mode, options.protocol) catch {
                return error.ProtocolFailure;
            };
            const accept_ex = try Iocp.resolveAcceptEx(listen_sock);
            const accept_sock = win32.WSASocketW(
                family,
                @intCast(mode),
                @intCast(protocol),
                null,
                0,
                win32.WSA_FLAG_OVERLAPPED | win32.WSA_FLAG_NO_HANDLE_INHERIT,
            );
            if (accept_sock == win32.INVALID_SOCKET) return error.SystemResources;
            errdefer _ = win32.closesocket(accept_sock);

            const addr_slot: win32.DWORD = @sizeOf(win32.ws2_32.sockaddr.storage) + 16;
            var addr_buf: [addr_slot * 2]u8 align(8) = undefined;
            var bytes_recvd: win32.DWORD = 0;
            var comp: Iocp.Completion = .{ .callback = undefined };

            const ok = accept_ex(
                listen_sock,
                accept_sock,
                &addr_buf,
                0,
                addr_slot,
                addr_slot,
                &bytes_recvd,
                &comp.overlapped,
            );
            if (!ok.toBool()) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    return switch (werr) {
                        win32.WSAECONNRESET => error.ConnectionAborted,
                        win32.WSAEINVAL => error.SocketNotListening,
                        win32.WSAEMFILE => error.ProcessFdQuotaExceeded,
                        win32.WSAENOBUFS => error.SystemResources,
                        else => error.Unexpected,
                    };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitIocpPending(server, &comp, null);
            if (res.nt_status != .SUCCESS) return error.ConnectionAborted;

            var listen_copy = listen_sock;
            _ = win32.setsockopt(
                accept_sock,
                win32.ws2_32.SOL.SOCKET,
                win32.ws2_32.SO.UPDATE_ACCEPT_CONTEXT,
                @ptrCast(&listen_copy),
                @sizeOf(win32.SOCKET),
            );
            const accepted_handle = win32.socketToHandle(accept_sock);
            try self.poller.associateHandle(accepted_handle, 0);

            var peer: Threaded.PosixAddress = undefined;
            var peer_len: c_int = @sizeOf(Threaded.PosixAddress);
            const peer_addr = if (win32.getpeername(accept_sock, &peer.any, &peer_len) == 0 and
                (peer.any.family == win32.ws2_32.AF.INET or peer.any.family == win32.ws2_32.AF.INET6))
                Threaded.addressFromPosix(&peer)
            else
                net.IpAddress{ .ip4 = .loopback(0) };

            return .{
                .handle = accepted_handle,
                .address = peer_addr,
            };
        },
        else => comptime unreachable,
    }
}

fn acceptFromWindowsAcceptor(self: *Shard, acc: *Acceptor, listener_id: u32) net.Server.AcceptError!net.Socket {
    if (builtin.os.tag != .windows) unreachable;
    _ = self.drainIncoming();

    const q = &self.shared_accepts[listener_id];
    const conn: AcceptedSocket = if (q.items.items.len > 0)
        q.items.orderedRemove(0)
    else blk: {
        if (self.stopping.load(.acquire)) return error.SocketNotListening;
        const cur = self.current_task;
        var waiter: AcceptWaiter = .{ .task = cur };
        waiter.prev = q.waiter_tail;
        waiter.linked = true;
        if (q.waiter_tail) |t| {
            t.next = &waiter;
        } else {
            q.waiter_head = &waiter;
        }
        q.waiter_tail = &waiter;

        const CancelCtx = struct {
            q_ptr: *SharedAcceptQueue,
            w_ptr: *AcceptWaiter,

            fn unlink(q_arg: *SharedAcceptQueue, w_arg: *AcceptWaiter) void {
                if (!w_arg.linked) return;
                if (w_arg.prev) |p| p.next = w_arg.next else q_arg.waiter_head = w_arg.next;
                if (w_arg.next) |n| n.prev = w_arg.prev else q_arg.waiter_tail = w_arg.prev;
                w_arg.prev = null;
                w_arg.next = null;
                w_arg.linked = false;
            }

            fn onCancel(s: *Shard, raw: *anyopaque) void {
                const c: *@This() = @ptrCast(@alignCast(raw));
                unlink(c.q_ptr, c.w_ptr);
                c.w_ptr.canceled = true;
                s.enqueueReady(c.w_ptr.task);
            }
        };
        var cctx: CancelCtx = .{ .q_ptr = q, .w_ptr = &waiter };
        cur.cancel_hook = .{ .ctx = &cctx, .trigger = CancelCtx.onCancel };
        defer {
            cur.cancel_hook = null;
            CancelCtx.unlink(q, &waiter);
        }

        while (waiter.delivered == null and !waiter.canceled) {
            self.suspendCurrent();
        }
        if (waiter.canceled) {
            if (cur.shouldCancelNow()) {
                cur.acknowledgeCancel();
                return error.Canceled;
            }
            return error.SocketNotListening;
        }
        break :blk waiter.delivered.?;
    };

    acc.noteShardSocketAdopted(self.id);
    errdefer _ = win32.closesocket(conn.raw_sock);

    // First and ONLY IOCP association of this accepted SOCKET happens right here on the target Shard!
    const handle = win32.socketToHandle(conn.raw_sock);
    try self.poller.associateHandle(handle, 0);
    return .{
        .handle = handle,
        .address = conn.peer_address,
    };
}

fn vtableNetBindIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.BindOptions,
) net.IpAddress.BindError!net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    switch (builtin.os.tag) {
        .linux => {
            const family = Threaded.posixAddressFamily(address);
            const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
            const sock_rc = linux.socket(family, mode | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, protocol);
            const fd: linux.fd_t = switch (linux.errno(sock_rc)) {
                .SUCCESS => @intCast(sock_rc),
                .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
                .PROTOTYPE => return error.SocketModeUnsupported,
                .INVAL => return error.ProtocolUnsupportedBySystem,
                .MFILE => return error.ProcessFdQuotaExceeded,
                .NFILE => return error.SystemFdQuotaExceeded,
                .NOMEM, .NOBUFS => return error.SystemResources,
                .ACCES => return error.AccessDenied,
                else => |err| return posix.unexpectedErrno(err),
            };
            errdefer Threaded.closeFd(fd);

            if (options.ip6_only) |v6only| {
                const val: c_int = @intFromBool(v6only);
                setSockOptLinux(fd, linux.IPPROTO.IPV6, linux.IPV6.V6ONLY, std.mem.asBytes(&val)) catch
                    return error.OptionUnsupported;
            }
            if (options.allow_broadcast) {
                const val: c_int = 1;
                setSockOptLinux(fd, linux.SOL.SOCKET, linux.SO.BROADCAST, std.mem.asBytes(&val)) catch
                    return error.OptionUnsupported;
            }

            var storage: Threaded.PosixAddress = undefined;
            const addr_len = Threaded.addressToPosix(address, &storage);
            switch (linux.errno(linux.bind(fd, &storage.any, addr_len))) {
                .SUCCESS => {},
                .ADDRINUSE => return error.AddressInUse,
                .ADDRNOTAVAIL => return error.AddressUnavailable,
                .ACCES => return error.AccessDenied,
                .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                .NOMEM, .NOBUFS => return error.SystemResources,
                else => |err| return posix.unexpectedErrno(err),
            }

            var bound: Threaded.PosixAddress = undefined;
            var bound_len: posix.socklen_t = @sizeOf(Threaded.PosixAddress);
            switch (linux.errno(linux.getsockname(fd, &bound.any, &bound_len))) {
                .SUCCESS => {},
                else => |err| return posix.unexpectedErrno(err),
            }
            return .{
                .handle = fd,
                .address = Threaded.addressFromPosix(&bound),
            };
        },
        .windows => {
            const family = Threaded.posixAddressFamily(address);
            const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
            const sock = win32.WSASocketW(
                family,
                @intCast(mode),
                @intCast(protocol),
                null,
                0,
                win32.WSA_FLAG_OVERLAPPED | win32.WSA_FLAG_NO_HANDLE_INHERIT,
            );
            if (sock == win32.INVALID_SOCKET) return mapWinBindErr(win32.WSAGetLastError());
            errdefer _ = win32.closesocket(sock);

            if (options.ip6_only) |v6only| {
                const val: c_int = @intFromBool(v6only);
                if (win32.setsockopt(sock, win32.IPPROTO_IPV6, win32.IPV6_V6ONLY, @ptrCast(&val), @sizeOf(c_int)) == win32.SOCKET_ERROR) {
                    return error.OptionUnsupported;
                }
            }
            if (options.allow_broadcast) {
                const val: c_int = 1;
                if (win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.ws2_32.SO.BROADCAST, @ptrCast(&val), @sizeOf(c_int)) == win32.SOCKET_ERROR) {
                    return error.OptionUnsupported;
                }
            }

            var storage: Threaded.PosixAddress = undefined;
            const addr_len = Threaded.addressToPosix(address, &storage);
            if (win32.bind(sock, &storage.any, @intCast(addr_len)) == win32.SOCKET_ERROR) {
                return mapWinBindErr(win32.WSAGetLastError());
            }
            var bound: Threaded.PosixAddress = undefined;
            var bound_len: c_int = @sizeOf(Threaded.PosixAddress);
            if (win32.getsockname(sock, &bound.any, &bound_len) == win32.SOCKET_ERROR) {
                return mapWinBindErr(win32.WSAGetLastError());
            }
            const handle = win32.socketToHandle(sock);
            try self.poller.associateHandle(handle, 0);
            return .{
                .handle = handle,
                .address = Threaded.addressFromPosix(&bound),
            };
        },
        else => comptime unreachable,
    }
}

fn vtableNetConnectIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.ConnectOptions,
) net.IpAddress.ConnectError!net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    const maybe_dl = time.resolveAwakeDeadlineNs(options.timeout, self, vtableNow);
    if (maybe_dl) |dl| {
        if (dl <= self.awakeNowNs()) return error.Timeout;
    }

    switch (builtin.os.tag) {
        .linux => {
            const family = Threaded.posixAddressFamily(address);
            const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
            const sock_rc = linux.socket(family, mode | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, protocol);
            const fd: linux.fd_t = switch (linux.errno(sock_rc)) {
                .SUCCESS => @intCast(sock_rc),
                .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
                .PROTOTYPE => return error.SocketModeUnsupported,
                .INVAL => return error.ProtocolUnsupportedBySystem,
                .MFILE => return error.ProcessFdQuotaExceeded,
                .NFILE => return error.SystemFdQuotaExceeded,
                .NOMEM, .NOBUFS => return error.SystemResources,
                .ACCES => return error.AccessDenied,
                else => |err| return posix.unexpectedErrno(err),
            };
            errdefer {
                self.poller.unregisterFd(fd);
                Threaded.closeFd(fd);
            }

            var storage: Threaded.PosixAddress = undefined;
            const addr_len = Threaded.addressToPosix(address, &storage);
            while (true) {
                const rc = linux.connect(fd, &storage.any, addr_len);
                switch (linux.errno(rc)) {
                    .SUCCESS => break,
                    .INTR => continue,
                    .INPROGRESS, .AGAIN => {
                        self.waitLinuxFd(fd, .write, maybe_dl) catch |err| switch (err) {
                            error.Canceled => return error.Canceled,
                            error.Timeout => return error.Timeout,
                            error.OutOfMemory, error.SystemResources => return error.SystemResources,
                            error.ProcessFdQuotaExceeded => return error.ProcessFdQuotaExceeded,
                            error.PollUnsupported => unreachable,
                            else => return error.Unexpected,
                        };
                        var so_err: c_int = 0;
                        var opt_len: posix.socklen_t = @sizeOf(c_int);
                        const grc = linux.getsockopt(fd, linux.SOL.SOCKET, linux.SO.ERROR, @ptrCast(&so_err), &opt_len);
                        if (linux.errno(grc) != .SUCCESS) return error.Unexpected;
                        if (so_err != 0) return mapLinuxConnectErr(@fromBackingInt(@intCast(so_err)));
                        break;
                    },
                    else => |err| return mapLinuxConnectErr(err),
                }
            }

            var local: Threaded.PosixAddress = undefined;
            var local_len: posix.socklen_t = @sizeOf(Threaded.PosixAddress);
            _ = linux.getsockname(fd, &local.any, &local_len);
            return .{
                .handle = fd,
                .address = Threaded.addressFromPosix(&local),
            };
        },
        .windows => {
            const family = Threaded.posixAddressFamily(address);
            const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
            const sock = win32.WSASocketW(
                family,
                @intCast(mode),
                @intCast(protocol),
                null,
                0,
                win32.WSA_FLAG_OVERLAPPED | win32.WSA_FLAG_NO_HANDLE_INHERIT,
            );
            if (sock == win32.INVALID_SOCKET) return mapWinConnectErr(win32.WSAGetLastError());
            errdefer _ = win32.closesocket(sock);

            // `ConnectEx` requires the socket to be bound to a local wildcard address first.
            const any_local: net.IpAddress = switch (address.*) {
                .ip4 => .{ .ip4 = .{ .bytes = .{ 0, 0, 0, 0 }, .port = 0 } },
                .ip6 => .{ .ip6 = .{ .bytes = @splat(0), .port = 0 } },
            };
            var local_st: Threaded.PosixAddress = undefined;
            const local_len = Threaded.addressToPosix(&any_local, &local_st);
            if (win32.bind(sock, &local_st.any, @intCast(local_len)) == win32.SOCKET_ERROR) {
                return mapWinConnectErr(win32.WSAGetLastError());
            }

            const handle = win32.socketToHandle(sock);
            try self.poller.associateHandle(handle, 0);
            const connect_ex = try Iocp.resolveConnectEx(sock);

            var dest_st: Threaded.PosixAddress = undefined;
            const dest_len = Threaded.addressToPosix(address, &dest_st);
            var comp: Iocp.Completion = .{ .callback = undefined };
            const ok = connect_ex(sock, &dest_st.any, @intCast(dest_len), null, 0, null, &comp.overlapped);
            if (!ok.toBool()) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) return mapWinConnectErr(werr);
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitIocpPending(handle, &comp, maybe_dl);
            if (res.timed_out) return error.Timeout;
            if (res.nt_status != .SUCCESS) {
                var transferred: win32.DWORD = 0;
                var out_flags: win32.DWORD = 0;
                _ = win32.WSAGetOverlappedResult(sock, &comp.overlapped, &transferred, .FALSE, &out_flags);
                return mapWinConnectErr(win32.WSAGetLastError());
            }

            _ = win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.ws2_32.SO.UPDATE_CONNECT_CONTEXT, null, 0);
            var bound_st: Threaded.PosixAddress = undefined;
            var bound_len: c_int = @sizeOf(Threaded.PosixAddress);
            _ = win32.getsockname(sock, &bound_st.any, &bound_len);
            return .{
                .handle = handle,
                .address = Threaded.addressFromPosix(&bound_st),
            };
        },
        else => comptime unreachable,
    }
}

fn fillUnixSockaddr(ua: *const net.UnixAddress, sun: *posix.sockaddr.un) posix.socklen_t {
    sun.* = .{ .family = posix.AF.UNIX, .path = @splat(0) };
    const dst: []u8 = @ptrCast(sun.path[0..ua.path.len]);
    @memcpy(dst, ua.path);
    const offset: posix.socklen_t = @offsetOf(posix.sockaddr.un, "path");
    if (ua.isAbstract()) {
        return offset + @as(posix.socklen_t, @intCast(ua.path.len));
    }
    return offset + @as(posix.socklen_t, @intCast(ua.path.len)) + 1;
}

fn vtableNetListenUnix(
    userdata: ?*anyopaque,
    ua: *const net.UnixAddress,
    options: net.UnixAddress.ListenOptions,
) net.UnixAddress.ListenError!net.Socket.Handle {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    switch (builtin.os.tag) {
        .linux => {
            const rc = linux.socket(linux.AF.UNIX, linux.SOCK.STREAM | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, 0);
            const fd: linux.fd_t = switch (linux.errno(rc)) {
                .SUCCESS => @intCast(rc),
                .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                .MFILE => return error.ProcessFdQuotaExceeded,
                .NFILE => return error.SystemFdQuotaExceeded,
                .NOMEM, .NOBUFS => return error.SystemResources,
                .ACCES => return error.AccessDenied,
                else => |err| return posix.unexpectedErrno(err),
            };
            errdefer Threaded.closeFd(fd);

            var sun: posix.sockaddr.un = undefined;
            const addr_len = fillUnixSockaddr(ua, &sun);
            switch (linux.errno(linux.bind(fd, @ptrCast(&sun), addr_len))) {
                .SUCCESS => {},
                .ADDRINUSE => return error.AddressInUse,
                .ADDRNOTAVAIL => return error.AddressUnavailable,
                .ACCES => return error.AccessDenied,
                .PERM => return error.PermissionDenied,
                .NOENT => return error.FileNotFound,
                .NOTDIR => return error.NotDir,
                .ROFS => return error.ReadOnlyFileSystem,
                .LOOP => return error.SymLinkLoop,
                .NOMEM, .NOBUFS => return error.SystemResources,
                else => |err| return posix.unexpectedErrno(err),
            }
            switch (linux.errno(linux.listen(fd, options.kernel_backlog))) {
                .SUCCESS => {},
                .ADDRINUSE => return error.AddressInUse,
                else => |err| return posix.unexpectedErrno(err),
            }
            return fd;
        },
        .windows => {
            if (!net.has_unix_sockets) return error.AddressFamilyUnsupported;
            const sock = win32.WSASocketW(win32.ws2_32.AF.UNIX, win32.ws2_32.SOCK.STREAM, 0, null, 0, win32.WSA_FLAG_OVERLAPPED | win32.WSA_FLAG_NO_HANDLE_INHERIT);
            if (sock == win32.INVALID_SOCKET) return error.AddressFamilyUnsupported;
            errdefer _ = win32.closesocket(sock);
            var sun: posix.sockaddr.un = undefined;
            const addr_len = fillUnixSockaddr(ua, &sun);
            if (win32.bind(sock, @ptrCast(&sun), @intCast(addr_len)) == win32.SOCKET_ERROR) return error.AddressInUse;
            if (win32.listen(sock, @intCast(@min(options.kernel_backlog, std.math.maxInt(c_int)))) == win32.SOCKET_ERROR) return error.Unexpected;
            const handle = win32.socketToHandle(sock);
            try self.poller.associateHandle(handle, 0);
            return handle;
        },
        else => comptime unreachable,
    }
}

fn vtableNetConnectUnix(
    userdata: ?*anyopaque,
    ua: *const net.UnixAddress,
) net.UnixAddress.ConnectError!net.Socket.Handle {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    switch (builtin.os.tag) {
        .linux => {
            const rc = linux.socket(linux.AF.UNIX, linux.SOCK.STREAM | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, 0);
            const fd: linux.fd_t = switch (linux.errno(rc)) {
                .SUCCESS => @intCast(rc),
                .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                .MFILE => return error.ProcessFdQuotaExceeded,
                .NFILE => return error.SystemFdQuotaExceeded,
                .NOMEM, .NOBUFS => return error.SystemResources,
                .ACCES => return error.AccessDenied,
                else => |err| return posix.unexpectedErrno(err),
            };
            errdefer {
                self.poller.unregisterFd(fd);
                Threaded.closeFd(fd);
            }

            var sun: posix.sockaddr.un = undefined;
            const addr_len = fillUnixSockaddr(ua, &sun);
            while (true) {
                const crc = linux.connect(fd, @ptrCast(&sun), addr_len);
                switch (linux.errno(crc)) {
                    .SUCCESS => return fd,
                    .INTR => continue,
                    .AGAIN, .INPROGRESS => {
                        self.waitLinuxFd(fd, .write, null) catch |err| switch (err) {
                            error.Canceled => return error.Canceled,
                            else => return error.Unexpected,
                        };
                        var so_err: c_int = 0;
                        var opt_len: posix.socklen_t = @sizeOf(c_int);
                        _ = linux.getsockopt(fd, linux.SOL.SOCKET, linux.SO.ERROR, @ptrCast(&so_err), &opt_len);
                        if (so_err == 0) return fd;
                        return switch (@as(linux.E, @fromBackingInt(@intCast(so_err)))) {
                            .CONNREFUSED => error.ConnectionRefused,
                            .NOENT => error.FileNotFound,
                            .ACCES => error.AccessDenied,
                            else => error.Unexpected,
                        };
                    },
                    .CONNREFUSED => return error.ConnectionRefused,
                    .NOENT => return error.FileNotFound,
                    .NOTDIR => return error.NotDir,
                    .ACCES => return error.AccessDenied,
                    .PERM => return error.PermissionDenied,
                    .LOOP => return error.SymLinkLoop,
                    .ROFS => return error.ReadOnlyFileSystem,
                    else => |err| return posix.unexpectedErrno(err),
                }
            }
        },
        .windows => {
            if (!net.has_unix_sockets) return error.AddressFamilyUnsupported;
            return error.AddressFamilyUnsupported;
        },
        else => comptime unreachable,
    }
}

fn vtableNetSocketCreatePair(
    userdata: ?*anyopaque,
    options: net.Socket.CreatePairOptions,
) net.Socket.CreatePairError![2]net.Socket {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    switch (builtin.os.tag) {
        .linux => {
            const mode, const protocol = Threaded.posixSocketModeProtocol(linux.AF.UNIX, options.mode, options.protocol) catch {
                return error.SocketModeUnsupported;
            };
            var fds: [2]linux.fd_t = undefined;
            const rc = linux.socketpair(linux.AF.UNIX, mode | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, protocol, &fds);
            switch (linux.errno(rc)) {
                .SUCCESS => return .{
                    .{ .handle = fds[0], .address = .{ .ip4 = .loopback(0) } },
                    .{ .handle = fds[1], .address = .{ .ip4 = .loopback(0) } },
                },
                .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                .OPNOTSUPP => return error.OperationUnsupported,
                .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
                .PROTOTYPE => return error.SocketModeUnsupported,
                .MFILE => return error.ProcessFdQuotaExceeded,
                .NFILE => return error.SystemFdQuotaExceeded,
                .NOMEM, .NOBUFS => return error.SystemResources,
                .ACCES => return error.AccessDenied,
                else => |err| return posix.unexpectedErrno(err),
            }
        },
        .windows => {
            if (options.mode != .stream) return error.OperationUnsupported;
            const lo: net.IpAddress = switch (options.family) {
                .ip4 => .{ .ip4 = .loopback(0) },
                .ip6 => .{ .ip6 = .loopback(0) },
            };
            const listen_sock = vtableNetListenIp(self, &lo, .{
                .mode = .stream,
                .protocol = options.protocol orelse .tcp,
            }) catch return error.SystemResources;
            defer vtableNetClose(self, (&listen_sock)[0..1]);

            const client = vtableNetConnectIp(self, &listen_sock.address, .{
                .mode = .stream,
                .protocol = options.protocol,
            }) catch return error.SystemResources;
            errdefer vtableNetClose(self, (&client)[0..1]);

            const accepted = vtableNetAccept(self, listen_sock.handle, .{
                .mode = .stream,
                .protocol = options.protocol,
            }) catch return error.SystemResources;
            return .{ client, accepted };
        },
        else => comptime unreachable,
    }
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
    _ = handle;
    _ = header;
    _ = file_reader;
    _ = limit;
    // Instructs `Io.Writer.sendFile` to copy safely via `shard.io()` buffered reads + `net_write`.
    return error.Unimplemented;
}

fn vtableNetClose(userdata: ?*anyopaque, sockets: []const net.Socket) void {
    const self = fromUserdata(userdata);
    switch (builtin.os.tag) {
        .linux => {
            for (sockets) |s| {
                self.poller.unregisterFd(s.handle);
                Threaded.closeFd(s.handle);
            }
        },
        .windows => {
            for (sockets) |s| {
                if (self.acceptor) |acc| {
                    if (acc.isSharedToken(s.handle)) |tok_idx| {
                        acc.closeToken(tok_idx);
                        continue;
                    }
                }
                _ = win32.closesocket(win32.handleToSocket(s.handle));
            }
        },
        else => comptime unreachable,
    }
}

fn vtableNetShutdown(
    userdata: ?*anyopaque,
    handle: net.Socket.Handle,
    how: net.ShutdownHow,
) net.ShutdownError!void {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    switch (builtin.os.tag) {
        .linux => {
            const lhow: u32 = switch (how) {
                .recv => linux.SHUT.RD,
                .send => linux.SHUT.WR,
                .both => linux.SHUT.RDWR,
            };
            switch (linux.errno(linux.shutdown(handle, @intCast(lhow)))) {
                .SUCCESS => {},
                .NOTCONN => return error.SocketUnconnected,
                .CONNRESET => return error.ConnectionResetByPeer,
                .CONNABORTED => return error.ConnectionAborted,
                .NOMEM, .NOBUFS => return error.SystemResources,
                else => |err| return posix.unexpectedErrno(err),
            }
        },
        .windows => {
            const whow: c_int = switch (how) {
                .recv => win32.SD_RECEIVE,
                .send => win32.SD_SEND,
                .both => win32.SD_BOTH,
            };
            if (win32.shutdown(win32.handleToSocket(handle), whow) == win32.SOCKET_ERROR) {
                return switch (win32.WSAGetLastError()) {
                    win32.WSAENOTCONN => error.SocketUnconnected,
                    win32.WSAECONNRESET => error.ConnectionResetByPeer,
                    win32.WSAECONNABORTED => error.ConnectionAborted,
                    win32.WSAENETDOWN => error.NetworkDown,
                    win32.WSAENOBUFS => error.SystemResources,
                    else => error.Unexpected,
                };
            }
        },
        else => comptime unreachable,
    }
}

fn vtableNetInterfaceNameResolve(
    userdata: ?*anyopaque,
    name: *const net.Interface.Name,
) net.Interface.Name.ResolveError!net.Interface {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    if (builtin.os.tag == .linux) {
        const sock_rc = linux.socket(linux.AF.INET, linux.SOCK.DGRAM | linux.SOCK.CLOEXEC, 0);
        if (linux.errno(sock_rc) != .SUCCESS) return error.SystemResources;
        const fd: linux.fd_t = @intCast(sock_rc);
        defer Threaded.closeFd(fd);

        var ifr: linux.ifreq = std.mem.zeroes(linux.ifreq);
        const slice = name.toSlice();
        @memcpy(ifr.ifrn.name[0..slice.len], slice);
        switch (linux.errno(linux.ioctl(fd, linux.SIOCGIFINDEX, @intFromPtr(&ifr)))) {
            .SUCCESS => return .{ .index = @intCast(ifr.ifru.ivalue) },
            .NODEV, .NXIO => return error.InterfaceNotFound,
            .ACCES, .PERM => return error.AccessDenied,
            .NOMEM, .NOBUFS => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
    return self.callBlocking(true, struct {
        fn run(t: *Threaded, n: *const net.Interface.Name) net.Interface.Name.ResolveError!net.Interface {
            const t_io = t.io();
            return t_io.vtable.netInterfaceNameResolve(t_io.userdata, n);
        }
    }.run, .{name});
}

fn vtableNetInterfaceName(
    userdata: ?*anyopaque,
    interface: net.Interface,
) net.Interface.NameError!net.Interface.Name {
    const self = fromUserdata(userdata);
    try self.checkCancelPoint();

    if (builtin.os.tag == .linux) {
        const sock_rc = linux.socket(linux.AF.INET, linux.SOCK.DGRAM | linux.SOCK.CLOEXEC, 0);
        if (linux.errno(sock_rc) != .SUCCESS) return error.Unexpected;
        const fd: linux.fd_t = @intCast(sock_rc);
        defer Threaded.closeFd(fd);

        var ifr: linux.ifreq = std.mem.zeroes(linux.ifreq);
        ifr.ifru.ivalue = @intCast(interface.index);
        switch (linux.errno(linux.ioctl(fd, linux.SIOCGIFNAME, @intFromPtr(&ifr)))) {
            .SUCCESS => {
                const raw_bytes: []const u8 = std.mem.sliceTo(&ifr.ifrn.name, 0);
                return net.Interface.Name.fromSlice(raw_bytes);
            },
            .NODEV, .NXIO => return error.InterfaceNotFound,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
    return self.callBlocking(true, struct {
        fn run(t: *Threaded, iface: net.Interface) net.Interface.NameError!net.Interface.Name {
            const t_io = t.io();
            return t_io.vtable.netInterfaceName(t_io.userdata, iface);
        }
    }.run, .{interface});
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
    // Fast-path 1: IPv6 literal
    if (net.IpAddress.parseIp6(name, options.port)) |addr| {
        if (options.family == .ip4) return error.UnknownHostName;
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
    } else |_| {}

    // Fast-path 2: IPv4 literal
    if (net.IpAddress.parseIp4(name, options.port)) |addr| {
        if (options.family == .ip6) return error.UnknownHostName;
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
    } else |_| {}

    // Fast-path 3: RFC 6761 `localhost`
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

    // Offload DNS resolution into a value-owned buffer on `BlockingPool`, then push
    // into caller's `resolved` queue ON THIS SHARD so `FutexTable` wakes properly!
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

// ── Error Translation Helpers ───────────────────────────────────────────────

fn setSockOptLinux(fd: linux.fd_t, level: i32, optname: u32, val: []const u8) !void {
    switch (linux.errno(linux.setsockopt(fd, level, optname, val.ptr, @intCast(val.len)))) {
        .SUCCESS => {},
        .NOPROTOOPT, .INVAL => return error.OptionUnsupported,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

fn mapLinuxConnectErr(err: linux.E) net.IpAddress.ConnectError {
    return switch (err) {
        .ADDRNOTAVAIL => error.AddressUnavailable,
        .AFNOSUPPORT => error.AddressFamilyUnsupported,
        .ALREADY => error.ConnectionPending,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionResetByPeer,
        .HOSTUNREACH => error.HostUnreachable,
        .NETUNREACH => error.NetworkUnreachable,
        .TIMEDOUT => error.Timeout,
        .ACCES, .PERM => error.AccessDenied,
        .NETDOWN => error.NetworkDown,
        .NOMEM, .NOBUFS => error.SystemResources,
        else => posix.unexpectedErrno(err),
    };
}

fn mapWinFileReadError(code: u32) Io.Operation.FileReadStreaming.Error {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.ERROR_HANDLE_EOF, win32.ERROR_BROKEN_PIPE => error.EndOfStream,
        win32.ERROR_NETNAME_DELETED => error.ConnectionResetByPeer,
        else => error.InputOutput,
    };
}

fn mapWinFileWriteError(code: u32) Io.Operation.FileWriteStreaming.Error {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.ERROR_BROKEN_PIPE => error.BrokenPipe,
        else => error.InputOutput,
    };
}

fn mapWinNetReadError(code: c_int) Io.Operation.NetRead.Error {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.WSAECONNRESET, win32.WSAECONNABORTED, win32.WSAENETRESET => error.ConnectionResetByPeer,
        win32.WSAENOTCONN, win32.WSAESHUTDOWN => error.SocketUnconnected,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAETIMEDOUT => error.ConnectionTimedOut,
        win32.WSAENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

fn mapWinNetWriteError(code: c_int) Io.Operation.NetWrite.Error {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.WSAECONNRESET, win32.WSAECONNABORTED, win32.WSAENETRESET => error.ConnectionResetByPeer,
        win32.WSAENOTCONN, win32.WSAESHUTDOWN => error.SocketUnconnected,
        win32.WSAECONNREFUSED => error.ConnectionRefused,
        win32.WSAENETUNREACH => error.NetworkUnreachable,
        win32.WSAEHOSTUNREACH => error.HostUnreachable,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAETIMEDOUT => error.ConnectionTimedOut,
        win32.WSAEDESTADDRREQ, win32.WSAEINVAL => error.SocketNotBound,
        win32.WSAENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

fn mapWinNetRecvError(code: c_int) net.Socket.ReceiveError {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.WSAEMSGSIZE => error.MessageOversize,
        win32.WSAECONNRESET => error.PortUnreachable,
        win32.WSAENOTCONN, win32.WSAESHUTDOWN => error.SocketUnconnected,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAETIMEDOUT => error.ConnectionTimedOut,
        win32.WSAENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

fn mapWinNetSendError(code: c_int) net.Socket.SendError {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.WSAEMSGSIZE => error.MessageOversize,
        win32.WSAECONNREFUSED => error.ConnectionRefused,
        win32.WSAECONNRESET => error.ConnectionResetByPeer,
        win32.WSAENETUNREACH => error.NetworkUnreachable,
        win32.WSAEHOSTUNREACH => error.HostUnreachable,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAENOTCONN, win32.WSAESHUTDOWN => error.SocketUnconnected,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAETIMEDOUT => error.ConnectionTimedOut,
        win32.WSAENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

fn mapWinListenErr(code: c_int) net.IpAddress.ListenError {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.WSAEADDRINUSE => error.AddressInUse,
        win32.WSAEADDRNOTAVAIL => error.AddressUnavailable,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAEPROTONOSUPPORT => error.ProtocolUnsupportedByAddressFamily,
        win32.WSAEPROTOTYPE, win32.WSAESOCKTNOSUPPORT => error.SocketModeUnsupported,
        win32.WSAEMFILE => error.ProcessFdQuotaExceeded,
        win32.WSAENOBUFS => error.SystemResources,
        win32.WSAENETDOWN => error.NetworkDown,
        else => error.Unexpected,
    };
}

fn mapWinBindErr(code: c_int) net.IpAddress.BindError {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.WSAEADDRINUSE => error.AddressInUse,
        win32.WSAEADDRNOTAVAIL => error.AddressUnavailable,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAEPROTONOSUPPORT => error.ProtocolUnsupportedByAddressFamily,
        win32.WSAEPROTOTYPE, win32.WSAESOCKTNOSUPPORT => error.SocketModeUnsupported,
        win32.WSAEMFILE => error.ProcessFdQuotaExceeded,
        win32.WSAENOBUFS => error.SystemResources,
        win32.WSAENETDOWN => error.NetworkDown,
        else => error.Unexpected,
    };
}

fn mapWinConnectErr(code: c_int) net.IpAddress.ConnectError {
    if (builtin.os.tag != .windows) unreachable;
    return switch (code) {
        win32.WSAEADDRNOTAVAIL => error.AddressUnavailable,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAEALREADY, win32.WSAEINPROGRESS => error.ConnectionPending,
        win32.WSAECONNREFUSED, @as(c_int, win32.ERROR_CONNECTION_REFUSED) => error.ConnectionRefused,
        win32.WSAECONNRESET, @as(c_int, win32.ERROR_NETNAME_DELETED) => error.ConnectionResetByPeer,
        win32.WSAEHOSTUNREACH => error.HostUnreachable,
        win32.WSAENETUNREACH => error.NetworkUnreachable,
        win32.WSAETIMEDOUT, @as(c_int, win32.ERROR_SEM_TIMEOUT) => error.Timeout,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAEMFILE => error.ProcessFdQuotaExceeded,
        win32.WSAENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

// ── Complete 68-Entry `std.Io.VTable` ───────────────────────────────────────

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

    .dirCreateDir = forwardThreaded("dirCreateDir", true),
    .dirCreateDirPath = forwardThreaded("dirCreateDirPath", true),
    .dirCreateDirPathOpen = forwardThreaded("dirCreateDirPathOpen", true),
    .dirOpenDir = forwardThreaded("dirOpenDir", true),
    .dirStat = forwardThreaded("dirStat", true),
    .dirStatFile = forwardThreaded("dirStatFile", true),
    .dirAccess = forwardThreaded("dirAccess", true),
    .dirCreateFile = forwardThreaded("dirCreateFile", true),
    .dirCreateFileAtomic = forwardThreaded("dirCreateFileAtomic", true),
    .dirOpenFile = forwardThreaded("dirOpenFile", true),
    .dirClose = vtableDirClose,
    .dirRead = forwardThreaded("dirRead", true),
    .dirRealPath = forwardThreaded("dirRealPath", true),
    .dirRealPathFile = forwardThreaded("dirRealPathFile", true),
    .dirDeleteFile = forwardThreaded("dirDeleteFile", true),
    .dirDeleteDir = forwardThreaded("dirDeleteDir", true),
    .dirRename = forwardThreaded("dirRename", true),
    .dirRenamePreserve = forwardThreaded("dirRenamePreserve", true),
    .dirSymLink = forwardThreaded("dirSymLink", true),
    .dirReadLink = forwardThreaded("dirReadLink", true),
    .dirSetOwner = forwardThreaded("dirSetOwner", true),
    .dirSetFileOwner = forwardThreaded("dirSetFileOwner", true),
    .dirSetPermissions = forwardThreaded("dirSetPermissions", true),
    .dirSetFilePermissions = forwardThreaded("dirSetFilePermissions", true),
    .dirSetTimestamps = forwardThreaded("dirSetTimestamps", true),
    .dirHardLink = forwardThreaded("dirHardLink", true),

    .fileStat = forwardThreaded("fileStat", true),
    .fileLength = forwardThreaded("fileLength", true),
    .fileClose = vtableFileClose,
    .fileWritePositional = forwardThreaded("fileWritePositional", true),
    .fileWriteFileStreaming = vtableFileWriteFileStreaming,
    .fileWriteFilePositional = vtableFileWriteFilePositional,
    .fileReadPositional = forwardThreaded("fileReadPositional", true),
    .fileSeekBy = forwardThreaded("fileSeekBy", true),
    .fileSeekTo = forwardThreaded("fileSeekTo", true),
    .fileSync = forwardThreaded("fileSync", true),
    .fileIsTty = forwardThreaded("fileIsTty", true),
    .fileEnableAnsiEscapeCodes = forwardThreaded("fileEnableAnsiEscapeCodes", true),
    .fileSupportsAnsiEscapeCodes = forwardThreaded("fileSupportsAnsiEscapeCodes", true),
    .fileSetLength = forwardThreaded("fileSetLength", true),
    .fileSetOwner = forwardThreaded("fileSetOwner", true),
    .fileSetPermissions = forwardThreaded("fileSetPermissions", true),
    .fileSetTimestamps = forwardThreaded("fileSetTimestamps", true),
    .fileLock = forwardThreaded("fileLock", true),
    .fileTryLock = forwardThreaded("fileTryLock", true),
    .fileUnlock = forwardThreaded("fileUnlock", false),
    .fileDowngradeLock = forwardThreaded("fileDowngradeLock", true),
    .fileRealPath = forwardThreaded("fileRealPath", true),
    .fileHardLink = forwardThreaded("fileHardLink", true),

    .fileMemoryMapCreate = forwardThreaded("fileMemoryMapCreate", true),
    .fileMemoryMapDestroy = forwardThreaded("fileMemoryMapDestroy", false),
    .fileMemoryMapSetLength = forwardThreaded("fileMemoryMapSetLength", true),
    .fileMemoryMapRead = forwardThreaded("fileMemoryMapRead", true),
    .fileMemoryMapWrite = forwardThreaded("fileMemoryMapWrite", true),

    .processExecutableOpen = forwardThreaded("processExecutableOpen", true),
    .processExecutablePath = forwardThreaded("processExecutablePath", true),
    .lockStderr = vtableLockStderr,
    .tryLockStderr = vtableTryLockStderr,
    .unlockStderr = vtableUnlockStderr,
    .processCurrentPath = forwardThreaded("processCurrentPath", true),
    .processSetCurrentDir = forwardThreaded("processSetCurrentDir", true),
    .processSetCurrentPath = forwardThreaded("processSetCurrentPath", true),
    .processReplace = forwardThreaded("processReplace", true),
    .processSpawn = vtableProcessSpawn,
    .childWait = vtableChildWait,
    .childKill = vtableChildKill,

    .progressParentFile = forwardThreaded("progressParentFile", true),
    .inheritParentDir = forwardThreaded("inheritParentDir", true),
    .inheritParentFile = forwardThreaded("inheritParentFile", true),

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
