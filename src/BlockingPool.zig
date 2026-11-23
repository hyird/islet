//! Bounded worker thread pool for synchronous/blocking syscall offload.
//!
//! Design invariants (AGENTS.md §3, §4):
//! - Pre-sized worker threads and bounded admission (`max_queued`); returns
//!   explicit `.queue_full` or `.closed` backpressure status on rejection.
//! - Zero heap allocation per submitted job: intrusive `Job` nodes live on the
//!   calling Fiber's stack frame.
//! - Safe cancellation (`tryCancel` / `runBlockingJoined`):
//!   - If still queued, unlinks `Job` in O(1) under the mutex so worker threads
//!     never touch the stack frame (`canceled_queued`).
//!   - If already popped and running on a worker thread, returns `.must_await`
//!     so the calling Fiber waits uncancelably until completion before unwinding.
//! - Owns a single shared `std.Io.Threaded` initialized via `.init_single_threaded`
//!   (no global signal handlers, zero extra threads, serialized `processSpawn`).
//! - Self-submission or self-`join` from inside a pool worker triggers `@panic`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const Mailbox = @import("Mailbox.zig");
const ResumeLane = Mailbox.ResumeLane;

const BlockingPool = @This();

pub const Options = struct {
    thread_count: ?usize = null,
    queue_capacity: ?usize = null,
    stack_size: usize = 2 * 1024 * 1024,
    environ: std.process.Environ = .empty,
};

pub const SubmitStatus = enum {
    accepted,
    queue_full,
    closed,
};

pub const CancelOutcome = enum {
    canceled_queued,
    must_await,
};

pub const JobState = enum(u8) {
    idle,
    queued,
    running,
    canceled,
    completed,
};

pub const Job = struct {
    prev: ?*Job = null,
    next: ?*Job = null,
    state: JobState = .idle,
    run_fn: *const fn (*Job, *std.Io.Threaded) void,
    resume_lane: *ResumeLane,
    resume_node: ResumeLane.Node,
    notify_ctx: ?*anyopaque = null,
    notify_fn: *const fn (?*anyopaque) void,
};

threadlocal var tls_current_pool: ?*const BlockingPool = null;

inline fn kernelIo() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

threaded: std.Io.Threaded,
threads: []std.Thread,
mutex: std.Io.Mutex = .init,
cond: std.Io.Condition = .init,
head: ?*Job = null,
tail: ?*Job = null,
queued_count: usize = 0,
queue_capacity: usize,
stopping: bool = false,
joined: bool = false,

/// Initializes `self` in-place (address-pinned: worker threads hold `*BlockingPool`!).
pub fn init(
    self: *BlockingPool,
    gpa: Allocator,
    options: Options,
) (Allocator.Error || std.Thread.SpawnError)!void {
    const cpu_count = std.Thread.getCpuCount() catch 2;
    const thread_count = if (options.thread_count) |n|
        @max(@as(usize, 1), n)
    else
        std.math.clamp((cpu_count + 1) / 2, @as(usize, 2), @as(usize, 8));

    const queue_capacity = if (options.queue_capacity) |cap|
        @max(@as(usize, 1), cap)
    else
        thread_count *| 64;

    const threads = try gpa.alloc(std.Thread, thread_count);
    errdefer gpa.free(threads);

    var threaded: std.Io.Threaded = .init_single_threaded;
    threaded.allocator = gpa;
    if (!options.environ.block.isEmpty()) {
        threaded.environ = .{ .process_environ = options.environ };
        threaded.environ_initialized = false;
    }

    self.* = .{
        .threaded = threaded,
        .threads = threads,
        .queue_capacity = queue_capacity,
    };
    errdefer self.threaded.deinit();

    var spawned: usize = 0;
    errdefer {
        self.requestStop();
        for (self.threads[0..spawned]) |t| t.join();
    }

    for (self.threads) |*slot| {
        slot.* = try std.Thread.spawn(.{
            .stack_size = options.stack_size,
            .allocator = gpa,
        }, workerLoop, .{self});
        spawned += 1;
    }
}

pub fn deinit(self: *BlockingPool, gpa: Allocator) void {
    self.join();
    self.threaded.deinit();
    gpa.free(self.threads);
    self.* = undefined;
}

pub fn requestStop(self: *BlockingPool) void {
    const kio = kernelIo();
    self.mutex.lockUncancelable(kio);
    self.stopping = true;
    self.mutex.unlock(kio);
    self.cond.broadcast(kio);
}

pub fn join(self: *BlockingPool) void {
    if (tls_current_pool == self) {
        @panic("islet.BlockingPool.join: self-join deadlock from pool worker thread");
    }
    self.requestStop();
    const kio = kernelIo();
    self.mutex.lockUncancelable(kio);
    if (self.joined) {
        self.mutex.unlock(kio);
        return;
    }
    self.joined = true;
    self.mutex.unlock(kio);

    for (self.threads) |t| t.join();
}

/// Submits an address-stable `Job`. Never blocks; executes callbacks and wakeup
/// notifications strictly outside `self.mutex`.
pub fn submit(self: *BlockingPool, job: *Job) SubmitStatus {
    if (tls_current_pool == self) {
        @panic("islet.BlockingPool.submit: self-submission from pool worker thread");
    }
    assert(job.state == .idle);
    const kio = kernelIo();

    {
        self.mutex.lockUncancelable(kio);
        if (self.stopping) {
            self.mutex.unlock(kio);
            return .closed;
        }
        if (self.queued_count >= self.queue_capacity) {
            self.mutex.unlock(kio);
            return .queue_full;
        }
        job.prev = self.tail;
        job.next = null;
        job.state = .queued;
        if (self.tail) |t| {
            t.next = job;
        } else {
            self.head = job;
        }
        self.tail = job;
        self.queued_count += 1;
        self.mutex.unlock(kio);
    }
    self.cond.signal(kio);
    return .accepted;
}

/// Attempts to withdraw a queued `Job` in O(1).
/// - `.canceled_queued`: unlinked before any worker touched it; caller owns `job` immediately.
/// - `.must_await`: already running/completed; caller MUST await `resume_node` before unwinding!
pub fn tryCancel(self: *BlockingPool, job: *Job) CancelOutcome {
    const kio = kernelIo();
    self.mutex.lockUncancelable(kio);
    defer self.mutex.unlock(kio);

    if (job.state == .queued) {
        if (job.prev) |p| {
            p.next = job.next;
        } else {
            self.head = job.next;
        }
        if (job.next) |n| {
            n.prev = job.prev;
        } else {
            self.tail = job.prev;
        }
        job.prev = null;
        job.next = null;
        job.state = .canceled;
        self.queued_count -= 1;
        return .canceled_queued;
    }
    return .must_await;
}

fn workerLoop(self: *BlockingPool) void {
    tls_current_pool = self;
    defer tls_current_pool = null;
    const kio = kernelIo();

    while (true) {
        const job: *Job = blk: {
            self.mutex.lockUncancelable(kio);
            while (self.head == null and !self.stopping) {
                self.cond.waitUncancelable(kio, &self.mutex);
            }
            if (self.head) |h| {
                self.head = h.next;
                if (self.head) |nh| {
                    nh.prev = null;
                } else {
                    self.tail = null;
                }
                h.prev = null;
                h.next = null;
                assert(h.state == .queued);
                h.state = .running;
                self.queued_count -= 1;
                self.mutex.unlock(kio);
                break :blk h;
            }
            assert(self.stopping);
            self.mutex.unlock(kio);
            return;
        };

        // Run blocking callable completely outside `self.mutex`.
        job.run_fn(job, &self.threaded);

        self.mutex.lockUncancelable(kio);
        job.state = .completed;
        self.mutex.unlock(kio);

        // Deliver completion back to origin Shard's unbounded ResumeLane & Doorbell.
        const lane = job.resume_lane;
        const notify_fn = job.notify_fn;
        const notify_ctx = job.notify_ctx;
        lane.push(&job.resume_node);
        notify_fn(notify_ctx);
    }
}

test "BlockingPool executes jobs, enforces backpressure, and cancels queued work safely" {
    const gpa = std.testing.allocator;
    const kio = kernelIo();
    var pool: BlockingPool = undefined;
    try pool.init(gpa, .{ .thread_count = 1, .queue_capacity = 2 });
    defer pool.deinit(gpa);

    var lane: ResumeLane = .init;
    var door_rings: std.atomic.Value(usize) = .init(0);

    const Work = struct {
        job: Job,
        gate: ?*std.Io.Event = null,
        started: std.Io.Event = .unset,
        done_event: std.Io.Event = .unset,
        ran: bool = false,

        fn exec(j: *Job, _: *std.Io.Threaded) void {
            const self: *@This() = @fieldParentPtr("job", j);
            const wio = kernelIo();
            self.started.set(wio);
            if (self.gate) |g| g.waitUncancelable(wio);
            self.ran = true;
        }

        fn onResume(n: *ResumeLane.Node) void {
            const j: *Job = @fieldParentPtr("resume_node", n);
            const self: *@This() = @fieldParentPtr("job", j);
            self.done_event.set(kernelIo());
        }

        fn ring(ctx: ?*anyopaque) void {
            const counter: *std.atomic.Value(usize) = @ptrCast(@alignCast(ctx.?));
            _ = counter.fetchAdd(1, .seq_cst);
        }
    };

    var gate: std.Io.Event = .unset;
    var w1: Work = .{ .gate = &gate, .job = .{
        .run_fn = Work.exec,
        .resume_lane = &lane,
        .resume_node = .{ .callback = Work.onResume },
        .notify_ctx = &door_rings,
        .notify_fn = Work.ring,
    } };
    var w2: Work = .{ .job = .{
        .run_fn = Work.exec,
        .resume_lane = &lane,
        .resume_node = .{ .callback = Work.onResume },
        .notify_ctx = &door_rings,
        .notify_fn = Work.ring,
    } };
    var w3: Work = .{ .job = .{
        .run_fn = Work.exec,
        .resume_lane = &lane,
        .resume_node = .{ .callback = Work.onResume },
        .notify_ctx = &door_rings,
        .notify_fn = Work.ring,
    } };
    var w4: Work = .{ .job = .{
        .run_fn = Work.exec,
        .resume_lane = &lane,
        .resume_node = .{ .callback = Work.onResume },
        .notify_ctx = &door_rings,
        .notify_fn = Work.ring,
    } };

    // Hold the single worker busy with w1, then fill queue with w2 and w3.
    try std.testing.expectEqual(SubmitStatus.accepted, pool.submit(&w1.job));
    w1.started.waitUncancelable(kio);

    try std.testing.expectEqual(SubmitStatus.accepted, pool.submit(&w2.job));
    try std.testing.expectEqual(SubmitStatus.accepted, pool.submit(&w3.job));
    try std.testing.expectEqual(SubmitStatus.queue_full, pool.submit(&w4.job));

    // Cancel w2 while queued: must unlink immediately and free up 1 queue slot!
    try std.testing.expectEqual(CancelOutcome.canceled_queued, pool.tryCancel(&w2.job));
    // w1 is running: must report `.must_await`.
    try std.testing.expectEqual(CancelOutcome.must_await, pool.tryCancel(&w1.job));

    gate.set(kio);
    while (door_rings.load(.seq_cst) < 2) {
        std.Thread.yield() catch {};
    }
    try std.testing.expectEqual(@as(usize, 2), lane.drain());
    try std.testing.expect(w1.ran and !w2.ran and w3.ran);
}
