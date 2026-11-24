//! Cross-thread coalesced wakeup doorbell, bounded external admission `Mailbox`,
//! and unbounded zero-allocation intrusive `ResumeLane`.
//!
//! Lock discipline (AGENTS.md §4):
//! - Internal mutexes ONLY swap indices or splice intrusive pointer chains.
//! - User callbacks, payload cleanup, and kernel doorbell syscalls ALWAYS run
//!   outside any mutex.

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

/// Bounded MPSC admission queue using `2 * capacity` preallocated slots (borrowed
/// from Ruvia's `worker_dispatcher`): while up to `capacity` items are spliced out
/// and processed outside the mutex by the consumer, up to `capacity` new items can
/// still be admitted without ever exhausting free nodes or allocating on the heap.
pub fn Mailbox(comptime T: type) type {
    return struct {
        const Self = @This();
        const no_node: u32 = std.math.maxInt(u32);

        pub const PostResult = union(enum) {
            accepted,
            queue_full: T,
            closed: T,
        };

        const Node = struct {
            next: u32 = no_node,
            payload: T = undefined,
        };

        mutex: std.Io.Mutex = .init,
        published_seq: std.atomic.Value(usize) align(cache_line) = .init(0),
        nodes: []Node,
        capacity: u32,
        pending_count: u32 = 0,
        free_head: u32 = 0,
        ready_head: u32 = no_node,
        ready_tail: u32 = no_node,
        closed: bool = false,

        pub fn init(gpa: Allocator, capacity: u32) (Allocator.Error || error{InvalidCapacity})!Self {
            if (capacity == 0 or capacity > std.math.maxInt(u32) / 2) {
                return error.InvalidCapacity;
            }
            const total_nodes: usize = @as(usize, capacity) * 2;
            const nodes = try gpa.alloc(Node, total_nodes);
            for (nodes, 0..) |*node, i| {
                node.* = .{
                    .next = if (i + 1 < total_nodes) @intCast(i + 1) else no_node,
                };
            }
            return .{
                .nodes = nodes,
                .capacity = capacity,
            };
        }

        pub fn deinit(self: *Self, gpa: Allocator, cleanup_fn: ?*const fn (*T) void) void {
            if (cleanup_fn) |cleanup| {
                self.drainAllWith(cleanup);
            }
            gpa.free(self.nodes);
            self.* = undefined;
        }

        pub fn close(self: *Self) void {
            const kio = kernelIo();
            self.mutex.lockUncancelable(kio);
            self.closed = true;
            self.mutex.unlock(kio);
        }

        /// Reserves capacity first; on backpressure or closure returns `payload`
        /// untouched to the caller. Caller should wake the target Shard's
        /// `Doorbell` after `.accepted` outside any lock.
        pub fn tryPost(self: *Self, payload: T) PostResult {
            const kio = kernelIo();
            var index: u32 = no_node;
            {
                self.mutex.lockUncancelable(kio);
                if (self.closed) {
                    self.mutex.unlock(kio);
                    return .{ .closed = payload };
                }
                if (self.pending_count == self.capacity) {
                    self.mutex.unlock(kio);
                    return .{ .queue_full = payload };
                }
                index = self.free_head;
                assert(index != no_node);
                self.free_head = self.nodes[index].next;
                self.pending_count += 1;
                self.mutex.unlock(kio);
            }

            // Copy payload outside the mutex.
            self.nodes[index].payload = payload;
            self.nodes[index].next = no_node;

            {
                self.mutex.lockUncancelable(kio);
                if (self.ready_tail == no_node) {
                    self.ready_head = index;
                } else {
                    self.nodes[self.ready_tail].next = index;
                }
                self.ready_tail = index;
                _ = self.published_seq.fetchAdd(1, .seq_cst);
                self.mutex.unlock(kio);
            }
            return .accepted;
        }

        pub fn isEmptySeqCst(self: *const Self) bool {
            return self.published_seq.load(.seq_cst) == 0;
        }

        /// Splices all ready nodes in O(1) under the mutex, invokes `consume_fn` on
        /// every payload outside the mutex, and returns the entire physical node
        /// chain to `free_head` in a single O(1) splice (`2 * capacity` nodes
        /// guarantee full concurrent admission capacity while draining).
        pub fn drain(self: *Self, ctx: ?*anyopaque, consume_fn: *const fn (?*anyopaque, T) void) usize {
            const kio = kernelIo();
            var batch_head: u32 = no_node;
            var batch_tail: u32 = no_node;
            var batch_len: u32 = 0;
            {
                self.mutex.lockUncancelable(kio);
                batch_head = self.ready_head;
                if (batch_head != no_node) {
                    batch_tail = self.ready_tail;
                    self.ready_head = no_node;
                    self.ready_tail = no_node;
                    var cur = batch_head;
                    while (cur != no_node) : (cur = self.nodes[cur].next) {
                        batch_len += 1;
                    }
                    self.pending_count -= batch_len;
                    _ = self.published_seq.fetchSub(batch_len, .seq_cst);
                }
                self.mutex.unlock(kio);
            }
            if (batch_head == no_node) return 0;
            defer self.releaseChain(batch_head, batch_tail);

            var cur = batch_head;
            while (cur != no_node) {
                const next = self.nodes[cur].next;
                const value = self.nodes[cur].payload;
                consume_fn(ctx, value);
                cur = next;
            }
            return batch_len;
        }

        fn drainAllWith(self: *Self, cleanup: *const fn (*T) void) void {
            const kio = kernelIo();
            var batch_head: u32 = no_node;
            var batch_tail: u32 = no_node;
            {
                self.mutex.lockUncancelable(kio);
                batch_head = self.ready_head;
                batch_tail = self.ready_tail;
                self.ready_head = no_node;
                self.ready_tail = no_node;
                self.pending_count = 0;
                self.published_seq.store(0, .seq_cst);
                self.mutex.unlock(kio);
            }
            if (batch_head == no_node) return;
            defer self.releaseChain(batch_head, batch_tail);

            var cur = batch_head;
            while (cur != no_node) {
                const next = self.nodes[cur].next;
                var value = self.nodes[cur].payload;
                cleanup(&value);
                cur = next;
            }
        }

        fn releaseChain(self: *Self, chain_head: u32, chain_tail: u32) void {
            const kio = kernelIo();
            self.mutex.lockUncancelable(kio);
            self.nodes[chain_tail].next = self.free_head;
            self.free_head = chain_head;
            self.mutex.unlock(kio);
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
        ctx: ?*anyopaque = null,
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
