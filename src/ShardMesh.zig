//! Directed lock-free SPSC ring mesh connecting all ordered `(from, to)` Shard pairs,
//! augmented with per-target cache-line isolated active-sender bitmaps and round-robin
//! drain budgets (adapted from Seastar `smp_message_queue` & Tokio `active_senders`).
//!
//! Design invariants (AGENTS.md §3):
//! - Dedicated SPSC ring per ordered `(from, to)` Shard pair (`N * (N - 1)` rings).
//! - Each target Shard owns a cache-line aligned `TargetInbox` with atomic `u64`
//!   active-sender bitmap words:
//!   - Producers only execute a modifying `.seq_cst` `fetchOr` when the sender bit
//!     transitions `0 -> 1`, avoiding cache-line RMW ping-pong during bursts.
//!   - Consumers check `isEmptyForSeqCst` in O(1) (1 local word read for <= 64 Shards)
//!     instead of scanning `N - 1` remote rings.
//!   - Consumers drain active rings starting from `rr_cursor` with a bounded per-ring
//!     `drain_budget` per pass, re-arming bits for any ring that still has items so
//!     high-rate low-index Shards can never starve higher-index Shards.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const cache_line = std.atomic.cache_line;
const SpscRingMod = @import("SpscRing.zig");
const SpscRing = SpscRingMod.SpscRing;
const FutexTable = @import("FutexTable.zig");

const ShardMesh = @This();

pub const default_ring_capacity: usize = 1024;
pub const drain_budget_per_ring: usize = 64;
pub const Message = *FutexTable.Waiter;
pub const Ring = SpscRing(Message);

const AtomicWord = std.atomic.Value(u64);

const TargetInbox = struct {
    /// Consumer-private round-robin starting shard index for fair draining.
    rr_cursor: usize align(cache_line) = 0,
    words_offset: usize = 0,
};

shard_count: usize,
words_per_target: usize,
rings: []Ring,
inboxes: []TargetInbox,
active_words: []align(cache_line) AtomicWord,

pub fn init(
    gpa: Allocator,
    shard_count: usize,
    ring_capacity: usize,
) SpscRingMod.InitError!ShardMesh {
    if (shard_count <= 1) {
        return .{
            .shard_count = shard_count,
            .words_per_target = 0,
            .rings = &.{},
            .inboxes = &.{},
            .active_words = &.{},
        };
    }

    const cap = if (ring_capacity == 0) default_ring_capacity else ring_capacity;
    const pair_count = shard_count * (shard_count - 1);
    const words_per_target = (shard_count + 63) / 64;
    // Pad each target's bitmap slice to cache-line granularity (`cache_line / 8` u64s)
    // so concurrent `fetchOr` on different target Shards never false-share!
    const words_per_line = @max(@as(usize, 1), cache_line / @sizeOf(AtomicWord));
    const stride = std.mem.alignForward(usize, words_per_target, words_per_line);
    const total_words = shard_count * stride;

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

    const inboxes = try gpa.alloc(TargetInbox, shard_count);
    errdefer gpa.free(inboxes);

    const active_words = try gpa.alignedAlloc(AtomicWord, .fromByteUnits(cache_line), total_words);
    for (active_words) |*w| w.* = .init(0);

    for (inboxes, 0..) |*inbox, tid| {
        inbox.* = .{
            .rr_cursor = 0,
            .words_offset = tid * stride,
        };
    }

    return .{
        .shard_count = shard_count,
        .words_per_target = words_per_target,
        .rings = rings,
        .inboxes = inboxes,
        .active_words = active_words,
    };
}

pub fn deinit(self: *ShardMesh, gpa: Allocator) void {
    for (self.rings) |*r| {
        r.deinit(gpa, null);
    }
    if (self.rings.len != 0) {
        gpa.free(self.active_words);
        gpa.free(self.inboxes);
        gpa.free(self.rings);
    }
    self.* = undefined;
}

inline fn pairIndex(self: *const ShardMesh, from: usize, to: usize) usize {
    assert(from < self.shard_count and to < self.shard_count and from != to);
    const col = if (to > from) to - 1 else to;
    return from * (self.shard_count - 1) + col;
}

inline fn targetWords(self: *const ShardMesh, to: usize) []AtomicWord {
    const base = self.inboxes[to].words_offset;
    return self.active_words[base .. base + self.words_per_target];
}

/// Producer `from` thread only: pushes `msg` toward `to` without locking and marks
/// `from` active in `to`'s bitmap if not already flagged.
pub fn trySend(self: *ShardMesh, from: usize, to: usize, msg: Message) Ring.PushResult {
    const res = self.rings[self.pairIndex(from, to)].tryPush(msg);
    if (res == .ok) {
        const words = self.targetWords(to);
        const word_idx = from >> 6;
        const mask = @as(u64, 1) << @intCast(from & 63);
        // `tryPush` stored `tail` with `.seq_cst`. If a `.seq_cst` load already
        // observes `mask` set, any future consumer `swap(0, .seq_cst)` is ordered
        // after our `tail` store and is guaranteed to drain this item!
        if ((words[word_idx].load(.seq_cst) & mask) == 0) {
            _ = words[word_idx].fetchOr(mask, .seq_cst);
        }
    }
    return res;
}

/// Consumer `to` thread only: drains active incoming rings starting from `rr_cursor`,
/// enforcing `drain_budget_per_ring` per sender and re-arming bits for non-empty rings.
pub fn drainFor(
    self: *ShardMesh,
    to: usize,
    ctx: ?*anyopaque,
    handler: *const fn (?*anyopaque, Message) void,
) usize {
    if (self.shard_count <= 1) return 0;

    const inbox = &self.inboxes[to];
    const words = self.targetWords(to);
    const start_shard = inbox.rr_cursor;
    const start_word = start_shard >> 6;
    const start_bit: u6 = @intCast(start_shard & 63);

    var total_drained: usize = 0;
    var next_cursor = start_shard;

    for (0..self.words_per_target) |w_off| {
        const w_idx = (start_word + w_off) % self.words_per_target;
        const raw_bits = words[w_idx].swap(0, .seq_cst);
        if (raw_bits == 0) continue;

        const rot: u6 = if (w_off == 0) start_bit else 0;
        var rotated = std.math.rotr(u64, raw_bits, rot);
        var rearm_mask: u64 = 0;

        while (rotated != 0) {
            const tz: u6 = @intCast(@ctz(rotated));
            rotated &= rotated - 1;
            const bit_idx: u6 = tz +% rot;
            const from = (w_idx << 6) + @as(usize, bit_idx);
            if (from >= self.shard_count or from == to) continue;

            const ring = &self.rings[self.pairIndex(from, to)];
            var ring_drained: usize = 0;
            while (ring_drained < drain_budget_per_ring) {
                const msg = ring.tryPop() orelse break;
                ring_drained += 1;
                total_drained += 1;
                handler(ctx, msg);
            }
            if (ring_drained == drain_budget_per_ring and !ring.isEmptySeqCst()) {
                rearm_mask |= @as(u64, 1) << bit_idx;
            }
            next_cursor = (from + 1) % self.shard_count;
        }

        if (rearm_mask != 0) {
            _ = words[w_idx].fetchOr(rearm_mask, .seq_cst);
        }
    }

    inbox.rr_cursor = next_cursor;
    return total_drained;
}

/// Consumer `to` O(1) sleep check after `Doorbell.armBeforeSleep()`.
pub fn isEmptyForSeqCst(self: *const ShardMesh, to: usize) bool {
    if (self.shard_count <= 1) return true;
    for (self.targetWords(to)) |*w| {
        if (w.load(.seq_cst) != 0) return false;
    }
    return true;
}

test "ShardMesh active bitmap routes messages, enforces per-ring budget, and rotates fairly" {
    const gpa = std.testing.allocator;
    var mesh = try ShardMesh.init(gpa, 3, 128);
    defer mesh.deinit(gpa);

    var dummy_word: u32 = 0;
    const noopWake = struct {
        fn w(_: ?*anyopaque, _: *FutexTable.Waiter) void {}
    }.w;

    var w_from0: FutexTable.Waiter = .{ .ptr = &dummy_word, .expected = 0, .owner_ctx = null, .wake_fn = noopWake };
    var w_from2: FutexTable.Waiter = .{ .ptr = &dummy_word, .expected = 0, .owner_ctx = null, .wake_fn = noopWake };

    try std.testing.expect(mesh.isEmptyForSeqCst(1));

    // Flood 80 messages (> drain_budget_per_ring = 64) from Shard 0 -> 1, and 1 message from Shard 2 -> 1.
    for (0..80) |_| {
        try std.testing.expectEqual(Ring.PushResult.ok, mesh.trySend(0, 1, &w_from0));
    }
    try std.testing.expectEqual(Ring.PushResult.ok, mesh.trySend(2, 1, &w_from2));
    try std.testing.expect(!mesh.isEmptyForSeqCst(1));

    const Counter = struct {
        from0: usize = 0,
        from2: usize = 0,
        w0_ref: *FutexTable.Waiter,
        fn onMsg(ctx: ?*anyopaque, m: Message) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            if (m == self.w0_ref) self.from0 += 1 else self.from2 += 1;
        }
    };
    var cnt: Counter = .{ .w0_ref = &w_from0 };

    // First drain pass: Shard 0 is capped at 64, so Shard 2's message is NOT starved!
    try std.testing.expectEqual(@as(usize, 65), mesh.drainFor(1, &cnt, Counter.onMsg));
    try std.testing.expectEqual(@as(usize, 64), cnt.from0);
    try std.testing.expectEqual(@as(usize, 1), cnt.from2);
    // Remaining 16 messages from Shard 0 kept Shard 1's active bitmap armed!
    try std.testing.expect(!mesh.isEmptyForSeqCst(1));

    // Second drain pass collects the remaining 16 messages and leaves bitmap empty.
    try std.testing.expectEqual(@as(usize, 16), mesh.drainFor(1, &cnt, Counter.onMsg));
    try std.testing.expectEqual(@as(usize, 80), cnt.from0);
    try std.testing.expect(mesh.isEmptyForSeqCst(1));
}
