//! 256-way cache-line striped Futex wait table powering `std.Io` synchronization
//! across Fibers within a Shard and across Shards/threads.
//!
//! Weak-memory (`aarch64` / `x86_64`) modification-order proof:
//! - Single-word packed `state: std.atomic.Value(u64)` (`(wake_epoch << 32) | waiters`):
//!   - `tryPark` executes `state.fetchAdd(1, .seq_cst)` BEFORE locking `stripe.mutex`
//!     and checking `@atomicLoad(u32, waiter.ptr, .seq_cst)`.
//!   - `wake` (after caller's `.release`/`.seq_cst` store to `*ptr`) executes a
//!     modifying RMW `const prev = state.fetchAdd(1 << 32, .seq_cst)` on the same
//!     `state` word and returns in O(1) if `@truncate(u32, prev) == 0`.
//!   - If `wake`'s RMW precedes `tryPark`'s RMW, `wake`'s release synchronizes-with
//!     `tryPark`'s acquire so `*ptr` mutation is visible to `tryPark`; otherwise
//!     `wake` observes `waiters >= 1` and enters `stripe.mutex`.
//! - Stripe mutex ONLY links/unlinks stack-resident `Waiter` nodes; all wakeup
//!   callbacks and doorbell syscalls run outside the mutex.

const std = @import("std");
const assert = std.debug.assert;
const cache_line = std.atomic.cache_line;

const FutexTable = @This();

const stripe_bits = 8;
const stripe_count = 1 << stripe_bits;
const wake_epoch_inc: u64 = @as(u64, 1) << 32;
const fibonacci_multiplier: usize = if (@bitSizeOf(usize) == 64)
    0x9E3779B97F4A7C15
else
    0x9E3779B9;

pub const Waiter = struct {
    prev: ?*Waiter = null,
    next: ?*Waiter = null,
    linked: bool = false,
    ptr: *const u32,
    expected: u32,
    wake_fn: *const fn (waiter: *Waiter) void,
};

const Stripe = struct {
    /// Low 32 bits: parked waiter count; high 32 bits: monotonic `wake` RMW epoch.
    state: std.atomic.Value(u64) align(cache_line) = .init(0),
    mutex: std.Io.Mutex = .init,
    head: ?*Waiter = null,
    tail: ?*Waiter = null,
};

inline fn kernelIo() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

stripes: [stripe_count]Stripe = @splat(.{}),

pub const init: FutexTable = .{};

inline fn stripeFor(self: *FutexTable, ptr: *const u32) *Stripe {
    const addr = @intFromPtr(ptr);
    const idx = (addr *% fibonacci_multiplier) >> (@bitSizeOf(usize) - stripe_bits);
    return &self.stripes[idx];
}

/// Atomically verifies `ptr.* == expected` under the stripe lock and links `waiter`.
/// Returns `true` if `waiter` is now parked (caller must suspend), or `false` if
/// `ptr.* != expected` (caller continues immediately without suspending).
pub fn tryPark(self: *FutexTable, waiter: *Waiter) bool {
    assert(!waiter.linked);
    const stripe = self.stripeFor(waiter.ptr);
    const kio = kernelIo();

    const prev = stripe.state.fetchAdd(1, .seq_cst);
    assert(@as(u32, @truncate(prev)) < std.math.maxInt(u32));

    stripe.mutex.lockUncancelable(kio);
    if (@atomicLoad(u32, waiter.ptr, .seq_cst) != waiter.expected) {
        _ = stripe.state.fetchSub(1, .seq_cst);
        stripe.mutex.unlock(kio);
        return false;
    }

    waiter.prev = stripe.tail;
    waiter.next = null;
    waiter.linked = true;
    if (stripe.tail) |t| {
        t.next = waiter;
    } else {
        stripe.head = waiter;
    }
    stripe.tail = waiter;
    stripe.mutex.unlock(kio);
    return true;
}

/// Unparks `waiter` due to timeout or cancellation.
/// - Returns `true` if `waiter` was still in the stripe and is now safely unlinked.
/// - Returns `false` if a concurrent `wake()` already detached `waiter` and is
///   delivering `waiter.wake_fn` outside the stripe lock (caller MUST await that
///   delivery before popping `waiter` off the stack!).
pub fn unpark(self: *FutexTable, waiter: *Waiter) bool {
    const stripe = self.stripeFor(waiter.ptr);
    const kio = kernelIo();
    stripe.mutex.lockUncancelable(kio);
    if (!waiter.linked) {
        stripe.mutex.unlock(kio);
        return false;
    }

    if (waiter.prev) |p| {
        p.next = waiter.next;
    } else {
        stripe.head = waiter.next;
    }
    if (waiter.next) |n| {
        n.prev = waiter.prev;
    } else {
        stripe.tail = waiter.prev;
    }
    waiter.prev = null;
    waiter.next = null;
    waiter.linked = false;
    _ = stripe.state.fetchSub(1, .monotonic);
    stripe.mutex.unlock(kio);
    return true;
}

/// Wakes up to `max_waiters` parked on `ptr`. Detaches matching nodes under the
/// stripe lock and invokes `wake_fn` strictly outside the stripe lock.
pub fn wake(self: *FutexTable, ptr: *const u32, max_waiters: u32) void {
    if (max_waiters == 0) return;
    const stripe = self.stripeFor(ptr);

    // Single-word modifying `.seq_cst` RMW increments high 32-bit epoch and reads
    // low 32-bit waiter count in one indivisible modification-order step.
    const prev = stripe.state.fetchAdd(wake_epoch_inc, .seq_cst);
    if (@as(u32, @truncate(prev)) == 0) {
        @branchHint(.likely);
        return;
    }

    var detached_head: ?*Waiter = null;
    var detached_tail: ?*Waiter = null;
    var removed: u64 = 0;
    const kio = kernelIo();

    {
        stripe.mutex.lockUncancelable(kio);
        var cur = stripe.head;
        while (cur != null and removed < max_waiters) {
            const w = cur.?;
            const next = w.next;
            if (w.ptr == ptr) {
                if (w.prev) |p| {
                    p.next = next;
                } else {
                    stripe.head = next;
                }
                if (next) |n| {
                    n.prev = w.prev;
                } else {
                    stripe.tail = w.prev;
                }
                w.prev = null;
                w.next = null;
                w.linked = false;

                if (detached_tail) |dt| {
                    dt.next = w;
                } else {
                    detached_head = w;
                }
                detached_tail = w;
                removed += 1;
            }
            cur = next;
        }
        if (removed > 0) {
            _ = stripe.state.fetchSub(removed, .monotonic);
        }
        stripe.mutex.unlock(kio);
    }

    var cur = detached_head;
    while (cur) |w| {
        const next = w.next;
        w.next = null;
        w.wake_fn(w);
        cur = next;
    }
}

test "FutexTable single-word RMW fast-path mismatch, FIFO partial wake, and unpark race verdict" {
    var table: FutexTable = .init;
    var word: u32 align(4) = 1;

    const State = struct {
        woken_order: [4]u32 = undefined,
        len: usize = 0,
    };
    const Hook = struct {
        st: *State,
        id: u32,
        waiter: Waiter,

        fn onWake(w: *Waiter) void {
            const self: *@This() = @fieldParentPtr("waiter", w);
            self.st.woken_order[self.st.len] = self.id;
            self.st.len += 1;
        }
    };

    var st: State = .{};
    var h1: Hook = .{ .st = &st, .id = 10, .waiter = .{ .ptr = &word, .expected = 0, .wake_fn = Hook.onWake } };
    // Value is 1 != expected 0 -> does not park and restores low-32 waiter count to 0.
    try std.testing.expect(!table.tryPark(&h1.waiter));

    h1.waiter.expected = 1;
    var h2: Hook = .{ .st = &st, .id = 20, .waiter = .{ .ptr = &word, .expected = 1, .wake_fn = Hook.onWake } };
    var h3: Hook = .{ .st = &st, .id = 30, .waiter = .{ .ptr = &word, .expected = 1, .wake_fn = Hook.onWake } };

    try std.testing.expect(table.tryPark(&h1.waiter));
    try std.testing.expect(table.tryPark(&h2.waiter));
    try std.testing.expect(table.tryPark(&h3.waiter));

    try std.testing.expect(table.unpark(&h2.waiter));
    try std.testing.expect(!table.unpark(&h2.waiter));

    @atomicStore(u32, &word, 2, .release);
    table.wake(&word, 1);
    try std.testing.expectEqualSlices(u32, &.{10}, st.woken_order[0..st.len]);
    try std.testing.expect(!table.unpark(&h1.waiter));

    table.wake(&word, 10);
    try std.testing.expectEqualSlices(u32, &.{ 10, 30 }, st.woken_order[0..st.len]);
}
