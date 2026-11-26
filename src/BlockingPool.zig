//! Bounded worker thread pool for blocking syscall offload.
//!
//! - Pre-sized worker threads and bounded admission (`queue_capacity`); `submit`
//!   returns `.queue_full` / `.closed` immediately for `Io.Batch.awaitConcurrent`,
//!   while `submitWaitable` parks Fiber-resident `Job` nodes in FIFO admission order
//!   and promotes them in O(1) when a worker slot opens.
//! - Intrusive `Job` nodes reside on the calling Fiber's stack with zero heap allocs.
//! - `tryCancel` unlinks `.waiting_admission` / `.queued` jobs in O(1) under the lock,
//!   or returns `.must_await` when a worker has already popped the job.
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
    waiting_admission,
    queued,
    running,
    canceled,
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
wait_head: ?*Job = null,
wait_tail: ?*Job = null,
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

/// Non-blocking bounded submission (`Io.Batch.awaitConcurrent`): rejects immediately
/// with `.queue_full` when `queued_count >= queue_capacity`.
pub fn submit(self: *BlockingPool, job: *Job) SubmitStatus {
    return self.submitInternal(job, false);
}

/// Event-driven Fiber submission (`Shard.callBlocking`): if the active worker queue
/// is full, links `job` into the bounded FIFO admission waiter list so a worker
/// promotes and runs `job` directly in O(1) as soon as a slot opens, avoiding
/// polling timer sleeps.
pub fn submitWaitable(self: *BlockingPool, job: *Job) SubmitStatus {
    return self.submitInternal(job, true);
}

fn submitInternal(self: *BlockingPool, job: *Job, allow_wait: bool) SubmitStatus {
    if (tls_current_pool == self) {
        @panic("islet.BlockingPool.submit: self-submission from pool worker thread");
    }
    assert(job.state == .idle);
    const kio = kernelIo();

    var should_signal = false;
    {
        self.mutex.lockUncancelable(kio);
        if (self.stopping) {
            self.mutex.unlock(kio);
            return .closed;
        }
        if (self.queued_count < self.queue_capacity) {
            assert(self.wait_head == null);
            self.pushQueuedLocked(job);
            should_signal = true;
            self.mutex.unlock(kio);
        } else if (allow_wait) {
            self.pushWaiterLocked(job);
            self.mutex.unlock(kio);
        } else {
            self.mutex.unlock(kio);
            return .queue_full;
        }
    }
    if (should_signal) {
        self.cond.signal(kio);
    }
    return .accepted;
}

fn pushQueuedLocked(self: *BlockingPool, job: *Job) void {
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
}

fn pushWaiterLocked(self: *BlockingPool, job: *Job) void {
    job.prev = self.wait_tail;
    job.next = null;
    job.state = .waiting_admission;
    if (self.wait_tail) |t| {
        t.next = job;
    } else {
        self.wait_head = job;
    }
    self.wait_tail = job;
}

fn promoteOneWaiterLocked(self: *BlockingPool) bool {
    if (self.queued_count >= self.queue_capacity) return false;
    const w = self.wait_head orelse return false;
    self.wait_head = w.next;
    if (self.wait_head) |nh| {
        nh.prev = null;
    } else {
        self.wait_tail = null;
    }
    assert(w.state == .waiting_admission);
    self.pushQueuedLocked(w);
    return true;
}

/// Attempts to withdraw a queued or admission-waiting `Job` in O(1).
/// - `.canceled_queued`: unlinked before any worker touched it; caller owns `job` immediately.
/// - `.must_await`: already running/completed; caller MUST await `resume_node` before unwinding!
pub fn tryCancel(self: *BlockingPool, job: *Job) CancelOutcome {
    const kio = kernelIo();
    var promoted = false;
    const outcome: CancelOutcome = blk: {
        self.mutex.lockUncancelable(kio);
        defer self.mutex.unlock(kio);

        switch (job.state) {
            .queued => {
                if (job.prev) |p| p.next = job.next else self.head = job.next;
                if (job.next) |n| n.prev = job.prev else self.tail = job.prev;
                job.prev = null;
                job.next = null;
                job.state = .canceled;
                self.queued_count -= 1;
                promoted = self.promoteOneWaiterLocked();
                break :blk .canceled_queued;
            },
            .waiting_admission => {
                if (job.prev) |p| p.next = job.next else self.wait_head = job.next;
                if (job.next) |n| n.prev = job.prev else self.wait_tail = job.prev;
                job.prev = null;
                job.next = null;
                job.state = .canceled;
                break :blk .canceled_queued;
            },
            else => break :blk .must_await,
        }
    };
    if (promoted) {
        self.cond.signal(kio);
    }
    return outcome;
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
                _ = self.promoteOneWaiterLocked();
                self.mutex.unlock(kio);
                break :blk h;
            }
            assert(self.stopping);
            assert(self.wait_head == null);
            self.mutex.unlock(kio);
            return;
        };

        // Run blocking callable completely outside `self.mutex`.
        job.run_fn(job, &self.threaded);

        // Deliver completion back to origin Shard's unbounded ResumeLane & Doorbell.
        const lane = job.resume_lane;
        const notify_fn = job.notify_fn;
        const notify_ctx = job.notify_ctx;
        lane.push(&job.resume_node);
        notify_fn(notify_ctx);
    }
}

test "BlockingPool executes jobs, enforces backpressure, promotes waitable admission, and cancels safely" {
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
    defer gate.set(kio);
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
    var w5: Work = .{ .job = .{
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
    // Non-waitable `submit` immediately rejects with `.queue_full`.
    try std.testing.expectEqual(SubmitStatus.queue_full, pool.submit(&w4.job));
    // Waitable `submitWaitable` admits w4 and w5 into `waiting_admission`!
    try std.testing.expectEqual(SubmitStatus.accepted, pool.submitWaitable(&w4.job));
    try std.testing.expectEqual(SubmitStatus.accepted, pool.submitWaitable(&w5.job));

    // Cancel w5 while still in `waiting_admission`:
    try std.testing.expectEqual(CancelOutcome.canceled_queued, pool.tryCancel(&w5.job));
    // Cancel w2 while `.queued`: must promote w4 directly into `.queued`!
    try std.testing.expectEqual(CancelOutcome.canceled_queued, pool.tryCancel(&w2.job));
    try std.testing.expectEqual(JobState.queued, w4.job.state);
    // w1 is running: must report `.must_await`.
    try std.testing.expectEqual(CancelOutcome.must_await, pool.tryCancel(&w1.job));

    // Burst 70 Fiber-resident `submitWaitable` jobs (> old 64 `wait_capacity`) must all
    // park event-driven in FIFO order without falling back to `.queue_full` 1ms polling!
    var burst: [70]Work = undefined;
    var queued_burst: usize = 0;
    defer for (burst[0..queued_burst]) |*bw| {
        _ = pool.tryCancel(&bw.job);
    };
    for (&burst) |*bw| {
        bw.* = .{ .job = .{
            .run_fn = Work.exec,
            .resume_lane = &lane,
            .resume_node = .{ .callback = Work.onResume },
            .notify_ctx = &door_rings,
            .notify_fn = Work.ring,
        } };
        try std.testing.expectEqual(SubmitStatus.accepted, pool.submitWaitable(&bw.job));
        queued_burst += 1;
    }
    for (&burst) |*bw| {
        try std.testing.expectEqual(CancelOutcome.canceled_queued, pool.tryCancel(&bw.job));
    }
    queued_burst = 0;

    gate.set(kio);
    while (door_rings.load(.seq_cst) < 3) {
        std.Thread.yield() catch {};
    }
    try std.testing.expectEqual(@as(usize, 3), lane.drain());
    try std.testing.expect(w1.ran and !w2.ran and w3.ran and w4.ran and !w5.ran);
}
