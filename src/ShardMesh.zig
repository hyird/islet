//! Directed lock-free SPSC ring mesh connecting all ordered `(from, to)` Shard pairs.
//!
//! Used internally by `Runtime` so inter-Shard `futexWake` notifications travel
//! over dedicated cache-line isolated SPSC rings without touching any mutex in
//! steady state, falling back to `ResumeLane` only when a ring is temporarily full.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const SpscRingMod = @import("SpscRing.zig");
const SpscRing = SpscRingMod.SpscRing;
const FutexTable = @import("FutexTable.zig");

const ShardMesh = @This();

pub const default_ring_capacity: usize = 1024;
pub const Message = *FutexTable.Waiter;
pub const Ring = SpscRing(Message);

shard_count: usize,
rings: []Ring,

pub fn init(
    gpa: Allocator,
    shard_count: usize,
    ring_capacity: usize,
) SpscRingMod.InitError!ShardMesh {
    if (shard_count <= 1) {
        return .{
            .shard_count = shard_count,
            .rings = &.{},
        };
    }

    const cap = if (ring_capacity == 0) default_ring_capacity else ring_capacity;
    const pair_count = shard_count * (shard_count - 1);
    const rings = try gpa.alloc(Ring, pair_count);
    errdefer gpa.free(rings);

    var initialized: usize = 0;
    errdefer {
        for (rings[0..initialized]) |*r| r.deinit(gpa, null);
    }

    for (rings) |*slot| {
        slot.* = try Ring.init(gpa, cap);
        initialized += 1;
    }

    return .{
        .shard_count = shard_count,
        .rings = rings,
    };
}

pub fn deinit(self: *ShardMesh, gpa: Allocator) void {
    for (self.rings) |*r| {
        r.deinit(gpa, null);
    }
    if (self.rings.len != 0) {
        gpa.free(self.rings);
    }
    self.* = undefined;
}

inline fn pairIndex(self: *const ShardMesh, from: usize, to: usize) usize {
    assert(from < self.shard_count and to < self.shard_count and from != to);
    const col = if (to > from) to - 1 else to;
    return from * (self.shard_count - 1) + col;
}

/// Producer `from` thread only: pushes `msg` toward `to` without locking.
pub fn trySend(self: *ShardMesh, from: usize, to: usize, msg: Message) Ring.PushResult {
    return self.rings[self.pairIndex(from, to)].tryPush(msg);
}

/// Consumer `to` thread only: drains all incoming rings addressed to `to`.
pub fn drainFor(
    self: *ShardMesh,
    to: usize,
    ctx: ?*anyopaque,
    handler: *const fn (?*anyopaque, Message) void,
) usize {
    if (self.shard_count <= 1) return 0;
    var count: usize = 0;
    for (0..self.shard_count) |from| {
        if (from == to) continue;
        const ring = &self.rings[self.pairIndex(from, to)];
        while (ring.tryPop()) |msg| {
            count += 1;
            handler(ctx, msg);
        }
    }
    return count;
}

/// Consumer `to` sleep check after `Doorbell.armBeforeSleep()`.
pub fn isEmptyForSeqCst(self: *const ShardMesh, to: usize) bool {
    if (self.shard_count <= 1) return true;
    for (0..self.shard_count) |from| {
        if (from == to) continue;
        if (!self.rings[self.pairIndex(from, to)].isEmptySeqCst()) return false;
    }
    return true;
}

test "ShardMesh routes messages across all ordered shard pairs without collision" {
    const gpa = std.testing.allocator;
    var mesh = try ShardMesh.init(gpa, 3, 8);
    defer mesh.deinit(gpa);

    var dummy_word: u32 = 0;
    const noopWake = struct {
        fn w(_: ?*anyopaque, _: *FutexTable.Waiter) void {}
    }.w;

    var w01: FutexTable.Waiter = .{ .ptr = &dummy_word, .expected = 0, .owner_ctx = null, .wake_fn = noopWake };
    var w21: FutexTable.Waiter = .{ .ptr = &dummy_word, .expected = 0, .owner_ctx = null, .wake_fn = noopWake };
    var w10: FutexTable.Waiter = .{ .ptr = &dummy_word, .expected = 0, .owner_ctx = null, .wake_fn = noopWake };

    try std.testing.expectEqual(Ring.PushResult.ok, mesh.trySend(0, 1, &w01));
    try std.testing.expectEqual(Ring.PushResult.ok, mesh.trySend(2, 1, &w21));
    try std.testing.expectEqual(Ring.PushResult.ok, mesh.trySend(1, 0, &w10));

    try std.testing.expect(!mesh.isEmptyForSeqCst(1));
    try std.testing.expect(!mesh.isEmptyForSeqCst(0));
    try std.testing.expect(mesh.isEmptyForSeqCst(2));

    const Collector = struct {
        got: [4]Message = undefined,
        len: usize = 0,
        fn push(ctx: ?*anyopaque, m: Message) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.got[self.len] = m;
            self.len += 1;
        }
    };
    var col1: Collector = .{};
    try std.testing.expectEqual(@as(usize, 2), mesh.drainFor(1, &col1, Collector.push));
    try std.testing.expectEqual(@as(usize, 2), col1.len);
    try std.testing.expectEqual(&w01, col1.got[0]);
    try std.testing.expectEqual(&w21, col1.got[1]);
    try std.testing.expect(mesh.isEmptyForSeqCst(1));

    var col0: Collector = .{};
    try std.testing.expectEqual(@as(usize, 1), mesh.drainFor(0, &col0, Collector.push));
    try std.testing.expectEqual(&w10, col0.got[0]);
}
