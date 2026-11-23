//! Shared-nothing `std.Io` backend for Zig with an independent I/O loop per shard.
//!
//! Supported multiplexer backends:
//! - Linux: `epoll`
//! - Windows: `iocp`

const builtin = @import("builtin");
const std = @import("std");

pub const Io = std.Io;

/// Native kernel I/O multiplexer selected at compile time for the target OS.
pub const Backend = enum {
    epoll,
    iocp,
};

pub const native_backend: Backend = switch (builtin.os.tag) {
    .linux => .epoll,
    .windows => .iocp,
    else => |tag| @compileError("islet supports only Linux (epoll) and Windows (IOCP), got: " ++ @tagName(tag)),
};

test "native backend matches target OS" {
    switch (builtin.os.tag) {
        .linux => try std.testing.expectEqual(Backend.epoll, native_backend),
        .windows => try std.testing.expectEqual(Backend.iocp, native_backend),
        else => unreachable,
    }
}
