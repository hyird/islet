//! Bounded wait-free single-producer single-consumer (SPSC) ring buffer.
//!
//! - Capacity is a power of two >= 2.
//! - Producer (`tail`, `cached_head`) and consumer (`head`, `cached_tail`)
//!   occupy separate cache lines (`std.atomic.cache_line`) to eliminate false sharing.
//! - Monotonic wrapping indices require no CAS and are immune to ABA.
//! - Slot storage is left `undefined` on allocation so idle rings do not fault pages.
//!
//! Memory-ordering proof:
//! - `tryPush` writes `slots[idx]` then publishes `tail +% 1` with `.seq_cst`
//!   (participates in both `release` slot publication to `tryPop`'s `.acquire`
//!   load of `tail` AND total `.seq_cst` ordering with `Doorbell.armed`).
//! - `tryPop` reads `slots[idx]` after `.acquire` load of `tail`, then hands the
//!   slot back via `.release` store of `head +% 1` paired with `tryPush`'s
//!   `.acquire` reload of `head`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const cache_line = std.atomic.cache_line;

pub const InitError = Allocator.Error || error{InvalidCapacity};

pub fn SpscRing(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const PushResult = union(enum) {
            ok,
            full: T,
        };

        const Producer = struct {
            tail: std.atomic.Value(usize) = .init(0),
            cached_head: usize = 0,
        };

        const Consumer = struct {
            head: std.atomic.Value(usize) = .init(0),
            cached_tail: usize = 0,
        };

        producer: Producer align(cache_line) = .{},
        consumer: Consumer align(cache_line) = .{},
        mask: usize,
        slots: []T,

        pub fn init(gpa: Allocator, ring_capacity: usize) InitError!Self {
            if (ring_capacity < 2 or !std.math.isPowerOfTwo(ring_capacity)) {
                return error.InvalidCapacity;
            }
            const slots = try gpa.alloc(T, ring_capacity);
            return .{
                .mask = ring_capacity - 1,
                .slots = slots,
            };
        }

        pub fn deinit(self: *Self, gpa: Allocator, cleanup_fn: ?*const fn (*T) void) void {
            if (cleanup_fn) |cleanup| {
                while (self.tryPop()) |item| {
                    var copy = item;
                    cleanup(&copy);
                }
            }
            gpa.free(self.slots);
            self.* = undefined;
        }

        pub fn capacity(self: *const Self) usize {
            return self.slots.len;
        }

        /// Producer thread only: attempts to enqueue `value` without blocking.
        /// Returns `.{ .full = value }` on backpressure so caller retains ownership.
        pub fn tryPush(self: *Self, value: T) PushResult {
            const cap = self.slots.len;
            const tail = self.producer.tail.load(.monotonic);
            if (tail -% self.producer.cached_head == cap) {
                self.producer.cached_head = self.consumer.head.load(.acquire);
                if (tail -% self.producer.cached_head == cap) {
                    return .{ .full = value };
                }
            }
            self.slots[tail & self.mask] = value;
            // `.seq_cst` acts as a release store for `slots` AND orders before
            // `Doorbell.notify()`'s `.seq_cst` swap on the producer side.
            self.producer.tail.store(tail +% 1, .seq_cst);
            return .ok;
        }

        /// Consumer thread only: pops the oldest item if available.
        pub fn tryPop(self: *Self) ?T {
            const head = self.consumer.head.load(.monotonic);
            if (head == self.consumer.cached_tail) {
                self.consumer.cached_tail = self.producer.tail.load(.acquire);
                if (head == self.consumer.cached_tail) {
                    return null;
                }
            }
            const value = self.slots[head & self.mask];
            self.consumer.head.store(head +% 1, .release);
            return value;
        }

        /// Safe from consumer sleep-check after `Doorbell.prepareSleep()`.
        /// Uses `.seq_cst` load of `tail` to pair with `tryPush` + `Doorbell`.
        pub fn isEmptySeqCst(self: *const Self) bool {
            return self.producer.tail.load(.seq_cst) == self.consumer.head.load(.acquire);
        }
    };
}

test "SpscRing single-thread backpressure, wrap-around, and cleanup" {
    const gpa = std.testing.allocator;
    try std.testing.expectError(error.InvalidCapacity, SpscRing(u64).init(gpa, 0));
    try std.testing.expectError(error.InvalidCapacity, SpscRing(u64).init(gpa, 3));

    var ring = try SpscRing(u64).init(gpa, 4);
    var cleaned_sum: u64 = 0;
    _ = &cleaned_sum;

    for (0..4) |i| {
        try std.testing.expectEqual(SpscRing(u64).PushResult.ok, ring.tryPush(i + 10));
    }
    switch (ring.tryPush(99)) {
        .ok => return error.TestUnexpectedResult,
        .full => |returned| try std.testing.expectEqual(@as(u64, 99), returned),
    }

    try std.testing.expectEqual(@as(?u64, 10), ring.tryPop());
    try std.testing.expectEqual(@as(?u64, 11), ring.tryPop());
    try std.testing.expectEqual(SpscRing(u64).PushResult.ok, ring.tryPush(14));
    try std.testing.expectEqual(SpscRing(u64).PushResult.ok, ring.tryPush(15));

    try std.testing.expectEqual(@as(?u64, 12), ring.tryPop());
    try std.testing.expectEqual(@as(?u64, 13), ring.tryPop());
    try std.testing.expectEqual(@as(?u64, 14), ring.tryPop());
    try std.testing.expect(!ring.isEmptySeqCst());
    try std.testing.expectEqual(@as(?u64, 15), ring.tryPop());
    try std.testing.expect(ring.isEmptySeqCst());
    try std.testing.expectEqual(@as(?u64, null), ring.tryPop());

    const Cleanup = struct {
        var count: usize = 0;
        fn drop(v: *u64) void {
            count += @intCast(v.*);
        }
    };
    Cleanup.count = 0;
    _ = ring.tryPush(7);
    _ = ring.tryPush(8);
    ring.deinit(gpa, Cleanup.drop);
    try std.testing.expectEqual(@as(usize, 15), Cleanup.count);
}

test "SpscRing concurrent producer and consumer integrity" {
    const gpa = std.testing.allocator;
    var ring = try SpscRing(u64).init(gpa, 256);
    defer ring.deinit(gpa, null);

    const total: u64 = 20_000;
    const ProducerRunner = struct {
        fn run(r: *SpscRing(u64)) void {
            var i: u64 = 1;
            while (i <= total) {
                switch (r.tryPush(i)) {
                    .ok => i += 1,
                    .full => std.Thread.yield() catch {},
                }
            }
        }
    };

    const prod = try std.Thread.spawn(.{}, ProducerRunner.run, .{&ring});
    var expected: u64 = 1;
    while (expected <= total) {
        if (ring.tryPop()) |val| {
            try std.testing.expectEqual(expected, val);
            expected += 1;
        } else {
            std.Thread.yield() catch {};
        }
    }
    prod.join();
    try std.testing.expect(ring.isEmptySeqCst());
}
