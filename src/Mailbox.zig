//! Cross-thread coalesced wakeup doorbell, bounded external admission `Mailbox`,
//! and zero-allocation intrusive `ResumeLane`.
//!
//! Lock discipline:
//! - Internal mutexes ONLY swap indices or splice intrusive pointer chains.
//! - Callbacks, payload cleanup, and kernel doorbell syscalls ALWAYS run outside locks.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const cache_line = std.atomic.cache_line;

/// Coalesces cross-thread kernel wakeups (`eventfd` write / `PostQueuedCompletionStatus`)
/// to at most one syscall per consumer sleep cycle.
///
/// Store-Buffering sequential-consistency proof:
/// - Producer: (1) publishes work with `.seq_cst` (`SpscRing.tryPush` or
///   `pending_seq_cst.fetchAdd`), then (2) calls `claimWakeup()` (`armed.swap(false, .seq_cst)`).
/// - Consumer: (1) calls `armBeforeSleep()` (`armed.store(true, .seq_cst)`), then
///   (2) checks all incoming queues with `.seq_cst` loads; if non-empty, calls
///   `disarm()` and skips sleeping.
/// Because all four steps are `.seq_cst`, Producer observing `armed == false` and
/// Consumer observing empty queues cannot occur simultaneously.
pub const Doorbell = struct {
    armed: std.atomic.Value(bool) align(cache_line) = .init(false),

    pub const init: Doorbell = .{};

    pub inline fn armBeforeSleep(self: *Doorbell) void {
        self.armed.store(true, .seq_cst);
    }

    pub inline fn disarm(self: *Doorbell) void {
        self.armed.store(false, .monotonic);
    }

    /// Returns `true` if the caller transitioned `armed` from `true` to `false`
    /// and must now invoke the OS wakeup syscall outside any lock.
    pub inline fn claimWakeup(self: *Doorbell) bool {
        if (!self.armed.load(.seq_cst)) return false;
        return self.armed.swap(false, .seq_cst);
    }
};

inline fn kernelIo() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

/// Bounded MPSC admission queue backed by a `2 * capacity` contiguous ping-pong
/// double buffer: producers admit in O(1) under a single mutex acquisition (with
/// zero torn window against `close()`), while the consumer swaps the active slice
/// in O(1) under a single mutex acquisition and invokes `consume_fn` unlocked
/// while up to `capacity` new items continue to arrive in the alternate buffer.
pub fn Mailbox(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const PostResult = union(enum) {
            accepted,
            queue_full: T,
            closed: T,
        };

        mutex: std.Io.Mutex = .init,
        published_seq: std.atomic.Value(usize) align(cache_line) = .init(0),
        storage: []T,
        capacity: u32,
        len: u32 = 0,
        active_half: u1 = 0,
        closed: bool = false,
        draining: bool = false,

        pub fn init(gpa: Allocator, capacity: u32) (Allocator.Error || error{InvalidCapacity})!Self {
            if (capacity == 0 or capacity > std.math.maxInt(u32) / 2) {
                return error.InvalidCapacity;
            }
            const storage = try gpa.alloc(T, @as(usize, capacity) * 2);
            return .{
                .storage = storage,
                .capacity = capacity,
            };
        }

        pub fn deinit(self: *Self, gpa: Allocator, cleanup_fn: ?*const fn (*T) void) void {
            if (cleanup_fn) |cleanup| {
                for (self.takeActiveSlice()) |*item| cleanup(item);
            }
            gpa.free(self.storage);
            self.* = undefined;
        }

        pub fn close(self: *Self) void {
            const kio = kernelIo();
            self.mutex.lockUncancelable(kio);
            self.closed = true;
            self.mutex.unlock(kio);
        }

        /// Admits `payload` in O(1) under a single lock; on backpressure or closure
        /// returns `payload` untouched. Caller wakes the target `Doorbell` outside any lock.
        pub fn tryPost(self: *Self, payload: T) PostResult {
            const kio = kernelIo();
            self.mutex.lockUncancelable(kio);
            if (self.closed) {
                self.mutex.unlock(kio);
                return .{ .closed = payload };
            }
            if (self.len == self.capacity) {
                self.mutex.unlock(kio);
                return .{ .queue_full = payload };
            }
            const base = @as(usize, self.active_half) * self.capacity;
            self.storage[base + self.len] = payload;
            self.len += 1;
            _ = self.published_seq.fetchAdd(1, .seq_cst);
            self.mutex.unlock(kio);
            return .accepted;
        }

        pub fn isEmptySeqCst(self: *const Self) bool {
            return self.published_seq.load(.seq_cst) == 0;
        }

        fn takeActiveSlice(self: *Self) []T {
            const kio = kernelIo();
            self.mutex.lockUncancelable(kio);
            const count = self.len;
            if (count == 0) {
                self.mutex.unlock(kio);
                return &.{};
            }
            const base = @as(usize, self.active_half) * self.capacity;
            self.active_half ^= 1;
            self.len = 0;
            self.published_seq.store(0, .seq_cst);
            self.mutex.unlock(kio);
            return self.storage[base .. base + count];
        }

        /// Swaps out all ready items in O(1) under a single lock and invokes
        /// `consume_fn` on every payload outside the lock. Single-consumer, non-reentrant.
        pub fn drain(self: *Self, ctx: ?*anyopaque, consume_fn: *const fn (?*anyopaque, T) void) usize {
            assert(!self.draining);
            self.draining = true;
            defer self.draining = false;

            const batch = self.takeActiveSlice();
            for (batch) |value| {
                consume_fn(ctx, value);
            }
            return batch.len;
        }
    };
}

/// Independent zero-allocation intrusive MPSC control and resume channel.
/// Never rejects due to business queue saturation; used exclusively for
/// `requestStop`, cross-thread cancellation, `futexWake`, and `BlockingPool`
/// completion delivery.
pub const ResumeLane = struct {
    pub const Node = struct {
        next: ?*Node = null,
        callback: *const fn (*Node) void,
    };

    mutex: std.Io.Mutex = .init,
    published_seq: std.atomic.Value(usize) align(cache_line) = .init(0),
    head: ?*Node = null,
    tail: ?*Node = null,

    pub const init: ResumeLane = .{};

    /// Pushes `node` in O(1) (pointer swap only). Caller must keep `node`
    /// address-stable until `node.callback(node)` runs on the owning Shard.
    pub fn push(self: *ResumeLane, node: *Node) void {
        const kio = kernelIo();
        node.next = null;
        self.mutex.lockUncancelable(kio);
        if (self.tail) |t| {
            t.next = node;
        } else {
            self.head = node;
        }
        self.tail = node;
        _ = self.published_seq.fetchAdd(1, .seq_cst);
        self.mutex.unlock(kio);
    }

    pub fn isEmptySeqCst(self: *const ResumeLane) bool {
        return self.published_seq.load(.seq_cst) == 0;
    }

    /// Detaches the entire pending chain in O(1) under the mutex and executes
    /// every callback outside the mutex.
    pub fn drain(self: *ResumeLane) usize {
        const kio = kernelIo();
        var list_head: ?*Node = null;
        {
            self.mutex.lockUncancelable(kio);
            list_head = self.head;
            if (list_head != null) {
                self.head = null;
                self.tail = null;
                self.published_seq.store(0, .seq_cst);
            }
            self.mutex.unlock(kio);
        }

        var ran: usize = 0;
        var cur = list_head;
        while (cur) |node| {
            const next = node.next;
            node.next = null;
            ran += 1;
            node.callback(node);
            cur = next;
        }
        return ran;
    }
};

test "Mailbox enforces capacity, returns rejected payload, and runs callbacks unlocked" {
    const gpa = std.testing.allocator;
    var mbox = try Mailbox(u32).init(gpa, 2);
    defer mbox.deinit(gpa, null);

    try std.testing.expectEqual(Mailbox(u32).PostResult.accepted, mbox.tryPost(10));
    try std.testing.expectEqual(Mailbox(u32).PostResult.accepted, mbox.tryPost(20));
    switch (mbox.tryPost(30)) {
        .queue_full => |v| try std.testing.expectEqual(@as(u32, 30), v),
        else => return error.TestUnexpectedResult,
    }

    const Collector = struct {
        sum: u32 = 0,
        mbox_ptr: *Mailbox(u32),

        fn onMsg(ctx: ?*anyopaque, val: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.sum += val;
            // Reentrant post inside callback proves `drain` runs outside `mutex`
            // and `2 * capacity` nodes allow admission during active batch drain.
            if (val == 10) {
                _ = self.mbox_ptr.tryPost(5);
            }
        }
    };

    var col: Collector = .{ .mbox_ptr = &mbox };
    try std.testing.expectEqual(@as(usize, 2), mbox.drain(&col, Collector.onMsg));
    try std.testing.expectEqual(@as(usize, 1), mbox.drain(&col, Collector.onMsg));
    try std.testing.expectEqual(@as(u32, 35), col.sum);

    mbox.close();
    switch (mbox.tryPost(99)) {
        .closed => |v| try std.testing.expectEqual(@as(u32, 99), v),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect(mbox.isEmptySeqCst());

    // Verify `deinit` cleans up unconsumed items and concurrent `tryPost` vs `close()` loses zero items.
    var mbox2 = try Mailbox(u32).init(gpa, 8);
    const State = struct {
        cleaned: std.atomic.Value(usize) = .init(0),
        consumed: std.atomic.Value(usize) = .init(0),
        rejected: std.atomic.Value(usize) = .init(0),
        var cleaned_ref: *std.atomic.Value(usize) = undefined;

        fn onCleanup(_: *u32) void {
            _ = cleaned_ref.fetchAdd(1, .monotonic);
        }
        fn onDrain(ctx: ?*anyopaque, _: u32) void {
            const s: *@This() = @ptrCast(@alignCast(ctx.?));
            _ = s.consumed.fetchAdd(1, .monotonic);
        }
        fn producer(m: *Mailbox(u32), s: *@This()) void {
            for (0..500) |i| {
                switch (m.tryPost(@intCast(i))) {
                    .accepted => {},
                    .queue_full, .closed => _ = s.rejected.fetchAdd(1, .monotonic),
                }
            }
        }
    };
    var st: State = .{};
    State.cleaned_ref = &st.cleaned;
    const p1 = try std.Thread.spawn(.{}, State.producer, .{ &mbox2, &st });
    const p2 = try std.Thread.spawn(.{}, State.producer, .{ &mbox2, &st });
    _ = mbox2.drain(&st, State.onDrain);
    mbox2.close();
    p1.join();
    p2.join();
    mbox2.deinit(gpa, State.onCleanup);
    try std.testing.expectEqual(
        @as(usize, 1000),
        st.consumed.load(.monotonic) + st.cleaned.load(.monotonic) + st.rejected.load(.monotonic),
    );
}

test "ResumeLane and Doorbell coalesce wakeups and execute callbacks outside lock" {
    var lane: ResumeLane = .init;
    var bell: Doorbell = .init;

    const Task = struct {
        node: ResumeLane.Node,
        ran: bool = false,

        fn run(n: *ResumeLane.Node) void {
            const self: *@This() = @fieldParentPtr("node", n);
            self.ran = true;
        }
    };

    var t1: Task = .{ .node = .{ .callback = Task.run } };
    var t2: Task = .{ .node = .{ .callback = Task.run } };

    bell.armBeforeSleep();
    try std.testing.expect(lane.isEmptySeqCst());

    lane.push(&t1.node);
    try std.testing.expect(bell.claimWakeup());
    lane.push(&t2.node);
    // Second push in same sleep cycle must coalesce (not claim a second wakeup).
    try std.testing.expect(!bell.claimWakeup());

    try std.testing.expect(!lane.isEmptySeqCst());
    try std.testing.expectEqual(@as(usize, 2), lane.drain());
    try std.testing.expect(t1.ran and t2.ran);
    try std.testing.expect(lane.isEmptySeqCst());
}
