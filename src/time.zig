//! Monotonic and clock duration utilities with saturating arithmetic and
//! ceiling sub-millisecond timeout conversion.

const std = @import("std");
const Io = std.Io;

/// Maximum finite millisecond timeout accepted by `epoll_wait` (`i32` >= 0)
/// and safely below Windows `INFINITE` (`0xFFFFFFFF`).
pub const max_wait_ms: u31 = std.math.maxInt(i32);

/// Saturating addition on 96-bit signed nanosecond timestamps/durations.
pub inline fn addNsSat(a: i96, b: i96) i96 {
    return a +| b;
}

/// Saturating subtraction on 96-bit signed nanosecond timestamps/durations.
pub inline fn subNsSat(a: i96, b: i96) i96 {
    return a -| b;
}

/// Converts a remaining nanosecond duration into an `epoll_wait` / `IOCP`
/// millisecond wait budget using ceiling rounding (`ceil`).
///
/// - `<= 0` returns `0` (non-blocking poll).
/// - `(0, 1ms]` returns `1` (never truncates positive sub-millisecond waits to 0).
/// - Clamps values above `max_wait_ms` to `max_wait_ms`.
pub fn ceilWaitMs(remaining_ns: i96) u31 {
    if (remaining_ns <= 0) return 0;
    const ns_per_ms: i96 = std.time.ns_per_ms;
    const rounded = @divTrunc(remaining_ns - 1, ns_per_ms) +| 1;
    if (rounded >= max_wait_ms) return max_wait_ms;
    return @intCast(rounded);
}

/// Resolves an `Io.Timeout` into an optional target nanosecond deadline on the
/// monotonic `.awake` clock, using `now_fn` to query clocks when needed.
pub fn resolveAwakeDeadlineNs(
    timeout: Io.Timeout,
    userdata: ?*anyopaque,
    now_fn: *const fn (?*anyopaque, Io.Clock) Io.Timestamp,
) ?i96 {
    return switch (timeout) {
        .none => null,
        .duration => |dur| {
            const awake_now = now_fn(userdata, .awake).nanoseconds;
            return addNsSat(awake_now, dur.raw.nanoseconds);
        },
        .deadline => |dl| {
            if (dl.clock == .awake) return dl.raw.nanoseconds;
            const clock_now = now_fn(userdata, dl.clock).nanoseconds;
            const awake_now = now_fn(userdata, .awake).nanoseconds;
            const remaining = subNsSat(dl.raw.nanoseconds, clock_now);
            return addNsSat(awake_now, remaining);
        },
    };
}

test "ceilWaitMs never truncates positive sub-millisecond durations to zero" {
    try std.testing.expectEqual(@as(u31, 0), ceilWaitMs(-100));
    try std.testing.expectEqual(@as(u31, 0), ceilWaitMs(0));
    try std.testing.expectEqual(@as(u31, 1), ceilWaitMs(1));
    try std.testing.expectEqual(@as(u31, 1), ceilWaitMs(std.time.ns_per_ms - 1));
    try std.testing.expectEqual(@as(u31, 1), ceilWaitMs(std.time.ns_per_ms));
    try std.testing.expectEqual(@as(u31, 2), ceilWaitMs(std.time.ns_per_ms + 1));
    try std.testing.expectEqual(max_wait_ms, ceilWaitMs(std.math.maxInt(i96)));
}

test "saturating nanosecond arithmetic clamps at i96 bounds" {
    try std.testing.expectEqual(std.math.maxInt(i96), addNsSat(std.math.maxInt(i96), 42));
    try std.testing.expectEqual(std.math.minInt(i96), subNsSat(std.math.minInt(i96), 42));
}
