//! Fixed-topology Shared-Nothing multi-Shard `std.Io` runtime.
//!
//! Lifecycle contract:
//! - Fixed homogeneous `shard_count` resolved at `init`; never mutated at runtime.
//! - Two-phase startup:
//!   1. `init`: allocates Shards, `epoll`/`IOCP` instances, `ShardMesh`, `FutexTable`,
//!      `BlockingPool`, and (on Windows) `Acceptor`, with exact `errdefer` rollback.
//!   2. `start` / `run`: spawns OS threads, barrier-synchronizes readiness across
//!      all Shards, and runs the user entry function on each Shard's own `std.Io`.
//! - Ordered teardown: `requestStop` -> Shard tasks drain -> `join` (panics on
//!   self-join deadlock) -> `Acceptor`, `ShardMesh`, and `BlockingPool` cleanup.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const Shard = @import("Shard.zig");
const BlockingPool = @import("BlockingPool.zig");
const FutexTable = @import("FutexTable.zig");
const ShardMesh = @import("ShardMesh.zig");
const SpscRingMod = @import("SpscRing.zig");
const Acceptor = @import("Acceptor.zig").Acceptor;

const Runtime = @This();

pub const InitOptions = struct {
    /// Number of homogeneous Shards (`null` defaults to logical CPU count >= 1).
    shard_count: ?usize = null,
    /// Per-ordered-pair SPSC ring capacity in `ShardMesh` (power of two >= 2).
    mesh_ring_capacity: usize = ShardMesh.default_ring_capacity,
    /// Per-Shard Windows Acceptor handoff inbox capacity.
    acceptor_inbox_capacity: u32 = 256,
    /// Per-Fiber stack size in bytes.
    stack_size: usize = 512 * 1024,
    /// Maximum idle Fiber stacks retained per Shard before watermark eviction.
    max_cached_stacks: usize = 64,
    /// Maximum file descriptor table bound per Shard on Linux.
    max_fds: u32 = 65536,
    /// Worker thread stack size for each Shard's OS thread.
    thread_stack_size: usize = 1024 * 1024,
    /// Shared `BlockingPool` thread count (`null` defaults to `clamp((cpus+1)/2, 2, 8)`).
    blocking_threads: ?usize = null,
    /// Shared `BlockingPool` bounded queue capacity.
    blocking_queue_capacity: ?usize = null,
    /// Optional process environment block for child process spawning.
    environ: std.process.Environ = .empty,
};

pub const InitError = Shard.InitError ||
    SpscRingMod.InitError ||
    std.Thread.SpawnError ||
    error{InvalidCapacity};

const State = enum(u8) {
    initialized,
    running,
    joined,
};

inline fn kernelIo() Io {
    return Io.Threaded.global_single_threaded.io();
}

gpa: Allocator,
shards: []Shard,
threads: []std.Thread,
spawned_threads: usize = 0,
thread_stack_size: usize,

blocking_pool: BlockingPool = undefined,
futex_table: FutexTable = .init,
mesh: ShardMesh = undefined,
acceptor: Acceptor = undefined,
stderr_mutex: Io.Mutex = .init,

state: State = .initialized,
stopping: std.atomic.Value(bool) = .init(false),

barrier_mutex: Io.Mutex = .init,
barrier_cond: Io.Condition = .init,
barrier_ready_count: usize = 0,
barrier_aborted: bool = false,
barrier_attach_err: ?Shard.AttachError = null,

active_runner_ctx: ?*anyopaque = null,
cleanup_runner_fn: ?*const fn (Allocator, *anyopaque) void = null,

/// Phase 1: Allocates fixed Shard array and all kernel/shared runtime resources.
/// Address-pinned: `*Runtime` must not be moved after `init` returns!
pub fn init(self: *Runtime, gpa: Allocator, options: InitOptions) InitError!void {
    const resolved_count = if (options.shard_count) |n|
        (if (n == 0) return error.InvalidCapacity else n)
    else
        @max(@as(usize, 1), std.Thread.getCpuCount() catch 1);

    const shards = try gpa.alloc(Shard, resolved_count);
    errdefer gpa.free(shards);

    const threads = try gpa.alloc(std.Thread, resolved_count);
    errdefer gpa.free(threads);

    self.* = .{
        .gpa = gpa,
        .shards = shards,
        .threads = threads,
        .thread_stack_size = options.thread_stack_size,
    };

    try self.blocking_pool.init(gpa, .{
        .thread_count = options.blocking_threads,
        .queue_capacity = options.blocking_queue_capacity,
        .environ = options.environ,
    });
    errdefer self.blocking_pool.deinit(gpa);

    self.mesh = try ShardMesh.init(gpa, resolved_count, options.mesh_ring_capacity);
    errdefer self.mesh.deinit(gpa);

    try self.acceptor.init(gpa, resolved_count, options.acceptor_inbox_capacity);
    errdefer self.acceptor.deinit(gpa);

    var initialized_shards: usize = 0;
    errdefer {
        for (self.shards[0..initialized_shards]) |*s| s.deinit();
    }

    const shard_opts: Shard.InitOptions = .{
        .stack_size = options.stack_size,
        .max_cached_stacks = options.max_cached_stacks,
        .max_fds = options.max_fds,
    };

    for (self.shards, 0..) |*s, idx| {
        try s.init(gpa, shard_opts, .{
            .shard_id = idx,
            .runtime_ptr = self,
            .blocking_pool = &self.blocking_pool,
            .futex_table = &self.futex_table,
            .mesh = if (resolved_count > 1) &self.mesh else null,
            .acceptor = self.acceptor.shardPtr(),
            .stderr_mutex = &self.stderr_mutex,
        });
        initialized_shards += 1;
    }
}

pub fn deinit(self: *Runtime) void {
    if (self.state == .running) {
        self.requestStop();
        self.joinThreadsOnly();
    }

    const gpa = self.gpa;
    for (self.shards) |*s| s.deinit();
    self.acceptor.deinit(gpa);
    self.mesh.deinit(gpa);
    self.blocking_pool.deinit(gpa);
    gpa.free(self.threads);
    gpa.free(self.shards);
    self.* = undefined;
}

pub fn shardCount(self: *const Runtime) usize {
    return self.shards.len;
}

pub fn isStopping(self: *const Runtime) bool {
    return self.stopping.load(.acquire);
}

/// Thread-safe: initiates graceful stop across all Shards and Windows Acceptor.
pub fn requestStop(self: *Runtime) void {
    self.stopping.store(true, .seq_cst);
    self.acceptor.requestStop();
    for (self.shards) |*s| s.requestStop();
}

fn entryPrefixParams(comptime Fn: type) usize {
    const info = @typeInfo(Fn).@"fn";
    if (info.param_types.len == 0 or info.param_types[0].? != Io) {
        @compileError("Runtime entry function must accept `std.Io` as its first parameter");
    }
    if (info.param_types.len >= 2 and info.param_types[1].? == usize) {
        return 2;
    }
    return 1;
}

fn ExtraArgsTuple(comptime Fn: type) type {
    const info = @typeInfo(Fn).@"fn";
    const prefix = comptime entryPrefixParams(Fn);
    const extra_len = info.param_types.len - prefix;
    var types: [extra_len]type = undefined;
    for (0..extra_len) |i| {
        types[i] = info.param_types[prefix + i].?;
    }
    return @Tuple(&types);
}

fn EntryReturnErrorSet(comptime Fn: type) ?type {
    const Ret = @typeInfo(Fn).@"fn".return_type.?;
    return switch (@typeInfo(Ret)) {
        .void => null,
        .error_union => |eu| blk: {
            if (eu.payload != void) @compileError("Runtime entry function must return `void` or `!void`");
            break :blk eu.error_set;
        },
        else => @compileError("Runtime entry function must return `void` or `!void`"),
    };
}

fn ErrorCapture(comptime E: type) type {
    return struct {
        lock: Io.Mutex = .init,
        first_err: ?E = null,
    };
}

/// Phase 2 (non-blocking): spawns all Shard threads, waits on the startup
/// readiness barrier until every Shard loop is attached, and returns while
/// Shards execute `function` concurrently. Call `join()` to await completion.
///
/// Supported `function` signatures:
/// - `fn (io: std.Io, ...extra_args) void` or `!void`
/// - `fn (io: std.Io, shard_index: usize, ...extra_args) void` or `!void`
pub fn start(
    self: *Runtime,
    comptime function: anytype,
    extra_args: ExtraArgsTuple(@TypeOf(function)),
) (Allocator.Error || Shard.AttachError || std.Thread.SpawnError)!void {
    const MaybeErr = comptime EntryReturnErrorSet(@TypeOf(function));
    return self.startInternal(function, extra_args, if (MaybeErr != null) null else {});
}

fn startInternal(
    self: *Runtime,
    comptime function: anytype,
    extra_args: ExtraArgsTuple(@TypeOf(function)),
    err_capture: if (EntryReturnErrorSet(@TypeOf(function))) |E| ?*ErrorCapture(E) else void,
) (Allocator.Error || Shard.AttachError || std.Thread.SpawnError)!void {
    if (self.state != .initialized) {
        @panic("islet.Runtime.start: Runtime can only be started once");
    }

    const Fn = @TypeOf(function);
    const prefix_params = comptime entryPrefixParams(Fn);
    const MaybeErr = comptime EntryReturnErrorSet(Fn);
    const Extra = ExtraArgsTuple(Fn);

    const Runner = struct {
        rt: *Runtime,
        extra: Extra,
        err_capture: if (MaybeErr) |E| ?*ErrorCapture(E) else void,

        fn cleanup(gpa: Allocator, raw: *anyopaque) void {
            const r: *@This() = @ptrCast(@alignCast(raw));
            gpa.destroy(r);
        }

        fn threadMain(r: *@This(), shard_idx: usize) void {
            const rt = r.rt;
            const shard = &rt.shards[shard_idx];
            const kio = kernelIo();

            shard.attachCurrentThread() catch |err| {
                rt.barrier_mutex.lockUncancelable(kio);
                rt.barrier_aborted = true;
                if (rt.barrier_attach_err == null) rt.barrier_attach_err = err;
                rt.barrier_ready_count += 1;
                rt.barrier_mutex.unlock(kio);
                rt.barrier_cond.broadcast(kio);
                return;
            };
            defer shard.detachCurrentThread();

            // Two-phase startup barrier: wait until ALL Shards are attached (or one failed).
            {
                rt.barrier_mutex.lockUncancelable(kio);
                rt.barrier_ready_count += 1;
                if (rt.barrier_ready_count == rt.shards.len) {
                    rt.barrier_cond.broadcast(kio);
                } else {
                    while (rt.barrier_ready_count < rt.shards.len and !rt.barrier_aborted) {
                        rt.barrier_cond.waitUncancelable(kio, &rt.barrier_mutex);
                    }
                }
                const abort_startup = rt.barrier_aborted;
                rt.barrier_mutex.unlock(kio);
                if (abort_startup) return;
            }

            const full_args = if (prefix_params == 2)
                .{ shard.io(), shard_idx } ++ r.extra
            else
                .{shard.io()} ++ r.extra;

            if (MaybeErr != null) {
                @call(.auto, function, full_args) catch |err| {
                    if (r.err_capture) |cap| {
                        cap.lock.lockUncancelable(kio);
                        if (cap.first_err == null) cap.first_err = err;
                        cap.lock.unlock(kio);
                    }
                    rt.requestStop();
                };
            } else {
                @call(.auto, function, full_args);
            }
        }
    };

    const runner = try self.gpa.create(Runner);
    runner.* = .{
        .rt = self,
        .extra = extra_args,
        .err_capture = err_capture,
    };

    self.active_runner_ctx = runner;
    self.cleanup_runner_fn = Runner.cleanup;
    self.state = .running;
    self.barrier_ready_count = 0;
    self.barrier_aborted = false;
    self.barrier_attach_err = null;
    self.spawned_threads = 0;

    const kio = kernelIo();
    for (self.threads, 0..) |*slot, idx| {
        slot.* = std.Thread.spawn(.{
            .stack_size = self.thread_stack_size,
            .allocator = self.gpa,
        }, Runner.threadMain, .{ runner, idx }) catch |err| {
            self.barrier_mutex.lockUncancelable(kio);
            self.barrier_aborted = true;
            self.barrier_mutex.unlock(kio);
            self.barrier_cond.broadcast(kio);
            self.joinThreadsOnly();
            return err;
        };
        self.spawned_threads += 1;
    }

    // Wait on caller thread until all Shard threads pass the attach barrier.
    self.barrier_mutex.lockUncancelable(kio);
    while (self.barrier_ready_count < self.shards.len and !self.barrier_aborted) {
        self.barrier_cond.waitUncancelable(kio, &self.barrier_mutex);
    }
    const attach_err = self.barrier_attach_err;
    self.barrier_mutex.unlock(kio);

    if (attach_err) |err| {
        self.joinThreadsOnly();
        return err;
    }
}

/// Joins all Shard threads and cleans up the Windows Acceptor.
/// Panics if called from inside one of this Runtime's own Shard threads!
pub fn join(self: *Runtime) void {
    self.assertNotSelfJoin();
    if (self.state != .running) return;
    self.joinThreadsOnly();
}

/// Starts all Shards with `function`, joins them on completion, and propagates
/// the first returned error if `function` returns `!void`.
pub fn run(
    self: *Runtime,
    comptime function: anytype,
    extra_args: ExtraArgsTuple(@TypeOf(function)),
) RunResult(@TypeOf(function)) {
    self.assertNotSelfJoin();
    const MaybeErr = comptime EntryReturnErrorSet(@TypeOf(function));
    if (MaybeErr) |E| {
        var capture: ErrorCapture(E) = .{};
        try self.startInternal(function, extra_args, &capture);
        self.joinThreadsOnly();
        if (capture.first_err) |e| return e;
    } else {
        try self.startInternal(function, extra_args, {});
        self.joinThreadsOnly();
    }
}

fn RunResult(comptime Fn: type) type {
    const StartErr = Allocator.Error || Shard.AttachError || std.Thread.SpawnError;
    if (EntryReturnErrorSet(Fn)) |E| {
        return (StartErr || E)!void;
    }
    return StartErr!void;
}

fn assertNotSelfJoin(self: *const Runtime) void {
    if (Shard.current()) |cur| {
        if (cur.runtime_ptr == @as(*const anyopaque, self)) {
            @panic("islet.Runtime.join: self-join deadlock from a Shard thread");
        }
    }
}

fn joinThreadsOnly(self: *Runtime) void {
    for (self.threads[0..self.spawned_threads]) |t| t.join();
    self.spawned_threads = 0;
    self.acceptor.join();
    if (self.active_runner_ctx) |ctx| {
        self.cleanup_runner_fn.?(self.gpa, ctx);
        self.active_runner_ctx = null;
        self.cleanup_runner_fn = null;
    }
    self.state = .joined;
}
