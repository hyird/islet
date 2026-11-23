//! Shard-local monotonic timer min-heap backed by a generation-stamped slot pool.
//!
//! Cancellation invalidates the slot generation in O(1) without scanning the
//! heap; stale heap entries are discarded lazily on pop or compacted when stale
//! density exceeds half the heap.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const TimerHeap = @This();

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
    occupied: bool = false,
    ctx: ?*anyopaque = null,
    callback: Callback = undefined,
};

const Entry = struct {
    deadline_ns: i96,
    sequence: u64,
    slot: u32,
    generation: u64,

    fn comesBefore(a: Entry, b: Entry) bool {
        if (a.deadline_ns != b.deadline_ns) return a.deadline_ns < b.deadline_ns;
        return a.sequence < b.sequence;
    }
};

slots: std.ArrayListUnmanaged(Slot) = .empty,
heap: std.ArrayListUnmanaged(Entry) = .empty,
free_head: u32 = no_slot,
active_count: usize = 0,
stale_count: usize = 0,
next_sequence: u64 = 1,
dispatching: bool = false,
dispatch_round_ns: i96 = std.math.minInt(i96),

pub const init: TimerHeap = .{};

pub fn deinit(self: *TimerHeap, gpa: Allocator) void {
    assert(self.active_count == 0);
    self.heap.deinit(gpa);
    self.slots.deinit(gpa);
    self.* = undefined;
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

    const slot = &self.slots.items[slot_index];
    slot.generation +%= 1;
    if (slot.generation == 0) slot.generation = 1;
    slot.next_free = no_slot;
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
        .generation = slot.generation,
    });
    self.siftUp(self.heap.items.len - 1);
    self.active_count += 1;

    return .{
        .slot = slot_index,
        .generation = slot.generation,
    };
}

/// Cancels an active timer in O(1) without invoking its callback.
/// Returns `true` if the timer was still active and is now disarmed.
pub fn cancel(self: *TimerHeap, token: Token) bool {
    if (!token.isActive() or token.slot >= self.slots.items.len) return false;
    const slot = &self.slots.items[token.slot];
    if (!slot.occupied or slot.generation != token.generation) return false;

    self.releaseSlot(token.slot);
    self.stale_count += 1;
    self.compactIfNeeded();
    return true;
}

/// Returns the earliest live deadline in nanoseconds, lazily discarding stale
/// entries from the top of the heap.
pub fn peekDeadlineNs(self: *TimerHeap) ?i96 {
    self.popStaleTop();
    if (self.heap.items.len == 0) return null;
    return self.heap.items[0].deadline_ns;
}

/// Fires all live timers with `deadline_ns <= now_ns` that were registered
/// before this round began. Returns the number of callbacks invoked.
pub fn fireExpired(self: *TimerHeap, now_ns: i96) usize {
    assert(!self.dispatching);
    self.dispatching = true;
    self.dispatch_round_ns = now_ns;
    defer {
        self.dispatching = false;
        self.dispatch_round_ns = std.math.minInt(i96);
    }

    const seq_limit = self.next_sequence;
    var fired: usize = 0;

    while (true) {
        self.popStaleTop();
        if (self.heap.items.len == 0) break;
        const top = self.heap.items[0];
        if (top.deadline_ns > now_ns or top.sequence >= seq_limit) break;

        _ = self.popRoot();
        const slot = &self.slots.items[top.slot];
        assert(slot.occupied and slot.generation == top.generation);
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
    while (true) {
        self.popStaleTop();
        if (self.heap.items.len == 0) break;
        const top = self.popRoot();
        const slot = &self.slots.items[top.slot];
        if (!slot.occupied or slot.generation != top.generation) continue;
        const cb = slot.callback;
        const ctx = slot.ctx;
        self.releaseSlot(top.slot);
        cb(ctx, .canceled);
    }
    assert(self.active_count == 0);
}

fn isLiveEntry(self: *const TimerHeap, entry: Entry) bool {
    if (entry.slot >= self.slots.items.len) return false;
    const slot = &self.slots.items[entry.slot];
    return slot.occupied and slot.generation == entry.generation;
}

fn releaseSlot(self: *TimerHeap, slot_index: u32) void {
    const slot = &self.slots.items[slot_index];
    assert(slot.occupied);
    slot.occupied = false;
    slot.ctx = null;
    slot.next_free = self.free_head;
    self.free_head = slot_index;
    self.active_count -= 1;
}

fn popStaleTop(self: *TimerHeap) void {
    while (self.heap.items.len != 0 and !self.isLiveEntry(self.heap.items[0])) {
        _ = self.popRoot();
        if (self.stale_count != 0) self.stale_count -= 1;
    }
}

fn compactIfNeeded(self: *TimerHeap) void {
    if (self.heap.items.len < 64 or self.stale_count * 2 <= self.heap.items.len) return;
    var write_idx: usize = 0;
    for (self.heap.items) |entry| {
        if (self.isLiveEntry(entry)) {
            self.heap.items[write_idx] = entry;
            write_idx += 1;
        }
    }
    self.heap.items.len = write_idx;
    self.stale_count = 0;
    if (write_idx > 1) {
        var i: usize = write_idx / 2;
        while (i > 0) {
            i -= 1;
            self.siftDown(i);
        }
    }
}

fn popRoot(self: *TimerHeap) Entry {
    const items = self.heap.items;
    const root = items[0];
    const last = items[items.len - 1];
    self.heap.items.len = items.len - 1;
    if (self.heap.items.len != 0) {
        self.heap.items[0] = last;
        self.siftDown(0);
    }
    return root;
}

fn siftUp(self: *TimerHeap, start_index: usize) void {
    var idx = start_index;
    const items = self.heap.items;
    const moving = items[idx];
    while (idx > 0) {
        const parent = (idx - 1) / 2;
        if (!moving.comesBefore(items[parent])) break;
        items[idx] = items[parent];
        idx = parent;
    }
    items[idx] = moving;
}

fn siftDown(self: *TimerHeap, start_index: usize) void {
    const items = self.heap.items;
    const len = items.len;
    var idx = start_index;
    const moving = items[idx];
    while (true) {
        const left = idx * 2 + 1;
        if (left >= len) break;
        const right = left + 1;
        const best_child = if (right < len and items[right].comesBefore(items[left])) right else left;
        if (!items[best_child].comesBefore(moving)) break;
        items[idx] = items[best_child];
        idx = best_child;
    }
    items[idx] = moving;
}

test "TimerHeap orders deadlines, preserves FIFO ties, and resists stale ABA" {
    const gpa = std.testing.allocator;
    var heap: TimerHeap = .init;
    defer heap.deinit(gpa);

    const Recorder = struct {
        log: [8]u32 = undefined,
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

    // Cancel t2 and reuse its slot immediately for h4 at deadline 20.
    try std.testing.expect(heap.cancel(t2));
    try std.testing.expect(!heap.cancel(t2));
    const t4 = try heap.schedule(gpa, 20, &h4, Hook.run);
    try std.testing.expectEqual(t2.slot, t4.slot);
    try std.testing.expect(t2.generation != t4.generation);

    try std.testing.expectEqual(@as(?i96, 10), heap.peekDeadlineNs());

    // At now=25: h3 (10) and h4 (20) fire; h3 re-arms at deadline 0, which must
    // be deferred to the NEXT fireExpired round instead of spinning forever.
    try std.testing.expectEqual(@as(usize, 2), heap.fireExpired(25));
    try std.testing.expectEqualSlices(u32, &.{ 3, 4 }, rec.log[0..rec.len]);

    // Next round at now=25 fires the re-armed h3, leaving h1 (30).
    try std.testing.expectEqual(@as(usize, 1), heap.fireExpired(25));
    try std.testing.expectEqualSlices(u32, &.{ 3, 4, 3 }, rec.log[0..rec.len]);

    try std.testing.expect(heap.cancel(t1));
    try std.testing.expectEqual(@as(?i96, null), heap.peekDeadlineNs());
}
