//! Shard-local monotonic 4-ary timer min-heap backed by an index-tracked,
//! generation-stamped slot pool.
//!
//! Design invariants (AGENTS.md §4, §5):
//! - 4-ary min-heap (`d = 4`, adapted from Go's runtime timer heap): 4 sibling
//!   entries sit contiguously per level, halving tree height and cache misses.
//! - Generation-stamped `Slot`s prevent ABA mis-cancellation across slot reuse,
//!   while `Slot.heap_index` (stored in an existing 4-byte struct padding hole
//!   at zero extra memory cost) enables exact O(log_4 N) in-place removal on
//!   `cancel()` — zero stale heap bloat and zero O(N) compaction scans.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const TimerHeap = @This();

const heap_arity = 4;

pub const Outcome = enum {
    expired,
    canceled,
};

pub const Callback = *const fn (ctx: ?*anyopaque, outcome: Outcome) void;

pub const Token = struct {
    slot: u32,
    generation: u64,

    pub const inactive: Token = .{ .slot = 0, .generation = 0 };

    pub fn isActive(self: Token) bool {
        return self.generation != 0;
    }
};

const no_slot: u32 = std.math.maxInt(u32);

const Slot = struct {
    generation: u64 = 0,
    next_free: u32 = no_slot,
    heap_index: u32 = no_slot,
    occupied: bool = false,
    ctx: ?*anyopaque = null,
    callback: Callback = undefined,
};

const Entry = struct {
    deadline_ns: i96,
    sequence: u64,
    slot: u32,

    inline fn comesBefore(a: Entry, b: Entry) bool {
        if (a.deadline_ns != b.deadline_ns) return a.deadline_ns < b.deadline_ns;
        return a.sequence < b.sequence;
    }
};

slots: std.ArrayListUnmanaged(Slot) = .empty,
heap: std.ArrayListUnmanaged(Entry) = .empty,
free_head: u32 = no_slot,
active_count: usize = 0,
next_sequence: u64 = 1,
dispatching: bool = false,
dispatch_round_ns: i96 = std.math.minInt(i96),

pub const init: TimerHeap = .{};

pub fn deinit(self: *TimerHeap, gpa: Allocator) void {
    assert(self.active_count == 0);
    assert(self.heap.items.len == 0);
    self.heap.deinit(gpa);
    self.slots.deinit(gpa);
    self.* = undefined;
}

pub inline fn hasActive(self: *const TimerHeap) bool {
    return self.heap.items.len != 0;
}

/// Schedules a callback at `deadline_ns` (monotonic `.awake` nanoseconds).
/// Must be called on the owning Shard thread.
pub fn schedule(
    self: *TimerHeap,
    gpa: Allocator,
    deadline_ns: i96,
    ctx: ?*anyopaque,
    callback: Callback,
) Allocator.Error!Token {
    try self.heap.ensureUnusedCapacity(gpa, 1);

    const slot_index: u32 = if (self.free_head != no_slot) blk: {
        const idx = self.free_head;
        self.free_head = self.slots.items[idx].next_free;
        break :blk idx;
    } else blk: {
        const idx: u32 = @intCast(self.slots.items.len);
        try self.slots.append(gpa, .{});
        break :blk idx;
    };

    const heap_idx = self.heap.items.len;
    const slot = &self.slots.items[slot_index];
    slot.generation +%= 1;
    if (slot.generation == 0) slot.generation = 1;
    slot.next_free = no_slot;
    slot.heap_index = @intCast(heap_idx);
    slot.occupied = true;
    slot.ctx = ctx;
    slot.callback = callback;

    // Prevent timers scheduled inside an expiry callback from starving the
    // current dispatch round even if their requested deadline is already due.
    const effective_deadline = if (self.dispatching)
        @max(deadline_ns, self.dispatch_round_ns)
    else
        deadline_ns;

    const seq = self.next_sequence;
    self.next_sequence +%= 1;

    self.heap.appendAssumeCapacity(.{
        .deadline_ns = effective_deadline,
        .sequence = seq,
        .slot = slot_index,
    });
    self.siftUp(heap_idx);
    self.active_count += 1;

    return .{
        .slot = slot_index,
        .generation = slot.generation,
    };
}

/// Cancels an active timer in O(log_4 N) without invoking its callback.
/// Generation validation guarantees stale tokens never cancel a reused slot.
pub fn cancel(self: *TimerHeap, token: Token) bool {
    if (!token.isActive() or token.slot >= self.slots.items.len) return false;
    const slot = &self.slots.items[token.slot];
    if (!slot.occupied or slot.generation != token.generation) return false;

    const h_idx = slot.heap_index;
    assert(h_idx < self.heap.items.len);
    _ = self.removeHeapAt(h_idx);
    self.releaseSlot(token.slot);
    return true;
}

/// Returns the earliest live deadline in O(1) with zero stale scanning.
pub inline fn peekDeadlineNs(self: *const TimerHeap) ?i96 {
    if (self.heap.items.len == 0) return null;
    return self.heap.items[0].deadline_ns;
}

/// Fires all live timers with `deadline_ns <= now_ns` that were registered
/// before this round began. Returns the number of callbacks invoked.
pub fn fireExpired(self: *TimerHeap, now_ns: i96) usize {
    assert(!self.dispatching);
    if (self.heap.items.len == 0 or self.heap.items[0].deadline_ns > now_ns) return 0;

    self.dispatching = true;
    self.dispatch_round_ns = now_ns;
    defer {
        self.dispatching = false;
        self.dispatch_round_ns = std.math.minInt(i96);
    }

    const seq_limit = self.next_sequence;
    var fired: usize = 0;

    while (self.heap.items.len != 0) {
        const top = self.heap.items[0];
        if (top.deadline_ns > now_ns or top.sequence >= seq_limit) break;

        _ = self.removeHeapAt(0);
        const slot = &self.slots.items[top.slot];
        assert(slot.occupied);
        const cb = slot.callback;
        const ctx = slot.ctx;
        self.releaseSlot(top.slot);

        fired += 1;
        cb(ctx, .expired);
    }

    return fired;
}

/// Cancels all remaining timers during Shard teardown, invoking each callback
/// with `.canceled` after releasing its slot.
pub fn cancelAll(self: *TimerHeap) void {
    while (self.heap.items.len != 0) {
        const top = self.removeHeapAt(0);
        const slot = &self.slots.items[top.slot];
        assert(slot.occupied);
        const cb = slot.callback;
        const ctx = slot.ctx;
        self.releaseSlot(top.slot);
        cb(ctx, .canceled);
    }
    assert(self.active_count == 0);
}

fn releaseSlot(self: *TimerHeap, slot_index: u32) void {
    const slot = &self.slots.items[slot_index];
    assert(slot.occupied);
    slot.occupied = false;
    slot.heap_index = no_slot;
    slot.ctx = null;
    slot.next_free = self.free_head;
    self.free_head = slot_index;
    self.active_count -= 1;
}

inline fn placeAt(self: *TimerHeap, idx: usize, entry: Entry) void {
    self.heap.items[idx] = entry;
    self.slots.items[entry.slot].heap_index = @intCast(idx);
}

fn removeHeapAt(self: *TimerHeap, remove_idx: usize) Entry {
    const items = self.heap.items;
    const removed = items[remove_idx];
    self.slots.items[removed.slot].heap_index = no_slot;

    const last_idx = items.len - 1;
    self.heap.items.len = last_idx;
    if (remove_idx < last_idx) {
        const moved = items[last_idx];
        self.placeAt(remove_idx, moved);
        if (remove_idx > 0 and moved.comesBefore(self.heap.items[(remove_idx - 1) / heap_arity])) {
            self.siftUp(remove_idx);
        } else {
            self.siftDown(remove_idx);
        }
    }
    return removed;
}

fn siftUp(self: *TimerHeap, start_index: usize) void {
    var idx = start_index;
    const items = self.heap.items;
    const moving = items[idx];
    while (idx > 0) {
        const parent = (idx - 1) / heap_arity;
        if (!moving.comesBefore(items[parent])) break;
        self.placeAt(idx, items[parent]);
        idx = parent;
    }
    self.placeAt(idx, moving);
}

fn siftDown(self: *TimerHeap, start_index: usize) void {
    const items = self.heap.items;
    const len = items.len;
    var idx = start_index;
    const moving = items[idx];
    while (true) {
        const first_child = idx * heap_arity + 1;
        if (first_child >= len) break;
        const end_child = @min(len, first_child + heap_arity);
        var best_child = first_child;
        for (first_child + 1..end_child) |c| {
            if (items[c].comesBefore(items[best_child])) {
                best_child = c;
            }
        }
        if (!items[best_child].comesBefore(moving)) break;
        self.placeAt(idx, items[best_child]);
        idx = best_child;
    }
    self.placeAt(idx, moving);
}

test "TimerHeap 4-ary heap orders deadlines, removes interior cancellations in-place, and resists stale ABA" {
    const gpa = std.testing.allocator;
    var heap: TimerHeap = .init;
    defer heap.deinit(gpa);

    const Recorder = struct {
        log: [64]u32 = undefined,
        len: usize = 0,

        fn push(self: *@This(), id: u32) void {
            self.log[self.len] = id;
            self.len += 1;
        }
    };

    const Hook = struct {
        rec: *Recorder,
        id: u32,
        rearm_heap: ?*TimerHeap = null,

        fn run(ctx: ?*anyopaque, outcome: Outcome) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            if (outcome == .expired) {
                self.rec.push(self.id);
                if (self.rearm_heap) |h| {
                    self.rearm_heap = null;
                    _ = h.schedule(std.testing.allocator, 0, self, run) catch unreachable;
                }
            }
        }
    };

    var rec: Recorder = .{};
    var h1: Hook = .{ .rec = &rec, .id = 1 };
    var h2: Hook = .{ .rec = &rec, .id = 2 };
    var h3: Hook = .{ .rec = &rec, .id = 3, .rearm_heap = &heap };
    var h4: Hook = .{ .rec = &rec, .id = 4 };

    const t1 = try heap.schedule(gpa, 30, &h1, Hook.run);
    const t2 = try heap.schedule(gpa, 10, &h2, Hook.run);
    _ = try heap.schedule(gpa, 10, &h3, Hook.run);
    try std.testing.expectEqual(@as(usize, 3), heap.heap.items.len);

    // Cancel t2 and verify immediate in-place removal + ABA generation protection on reuse.
    try std.testing.expect(heap.cancel(t2));
    try std.testing.expectEqual(@as(usize, 2), heap.heap.items.len);
    try std.testing.expect(!heap.cancel(t2));
    const t4 = try heap.schedule(gpa, 20, &h4, Hook.run);
    try std.testing.expectEqual(t2.slot, t4.slot);
    try std.testing.expect(t2.generation != t4.generation);
    try std.testing.expect(!heap.cancel(t2));

    try std.testing.expectEqual(@as(?i96, 10), heap.peekDeadlineNs());
    try std.testing.expectEqual(@as(usize, 2), heap.fireExpired(25));
    try std.testing.expectEqualSlices(u32, &.{ 3, 4 }, rec.log[0..rec.len]);

    try std.testing.expectEqual(@as(usize, 1), heap.fireExpired(25));
    try std.testing.expectEqualSlices(u32, &.{ 3, 4, 3 }, rec.log[0..rec.len]);

    try std.testing.expect(heap.cancel(t1));
    try std.testing.expect(!heap.hasActive());
    try std.testing.expectEqual(@as(?i96, null), heap.peekDeadlineNs());

    // Stress multi-level 4-ary siftUp/siftDown & out-of-order interior cancellation across 32 nodes.
    var hooks: [32]Hook = undefined;
    var tokens: [32]Token = undefined;
    for (0..32) |i| {
        const dl: i96 = @intCast(((i * 17) % 32) + 100);
        hooks[i] = .{ .rec = &rec, .id = @intCast(100 + i) };
        tokens[i] = try heap.schedule(gpa, dl, &hooks[i], Hook.run);
    }
    try std.testing.expectEqual(@as(usize, 32), heap.heap.items.len);
    for (0..16) |k| {
        try std.testing.expect(heap.cancel(tokens[k * 2]));
    }
    try std.testing.expectEqual(@as(usize, 16), heap.heap.items.len);
    rec.len = 0;
    try std.testing.expectEqual(@as(usize, 16), heap.fireExpired(500));
    try std.testing.expect(!heap.hasActive());
}
