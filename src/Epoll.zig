//! Linux `epoll` + `eventfd` edge-triggered readiness multiplexer per Shard.
//!
//! Design invariants (AGENTS.md §4):
//! - Exclusive `epoll_fd` and `event_fd` per Shard; zero cross-thread fd sharing.
//! - Each fd registers `EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET` ONCE on first
//!   `WouldBlock`, avoiding per-operation `EPOLL_CTL_MOD` syscalls.
//! - Separate intrusive read/write waiter queues per fd slot with `generation`
//!   tagging to ignore stale events from closed/reused fds within the same batch.
//! - `EPERM` from `epoll_ctl` (regular disk files / `/dev/null`) surfaces as
//!   `error.PollUnsupported` so callers offload to `BlockingPool` transparently.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const linux = std.os.linux;
const posix = std.posix;
const Mailbox = @import("Mailbox.zig");
const Doorbell = Mailbox.Doorbell;

const Epoll = @This();

pub const default_max_fds: u32 = 65536;
const eventfd_sentinel: u64 = std.math.maxInt(u64);

pub const Direction = enum {
    read,
    write,
};

pub const InitError = error{
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    Unexpected,
};

pub const RegisterError = Allocator.Error || error{
    PollUnsupported,
    ProcessFdQuotaExceeded,
    SystemResources,
    Unexpected,
};

/// Intrusive waiter node residing on a suspended Fiber's stack or `Batch` slot.
pub const Waiter = struct {
    prev: ?*Waiter = null,
    next: ?*Waiter = null,
    linked: bool = false,
    ready: bool = false,
    ctx: ?*anyopaque = null,
    wake_fn: *const fn (?*anyopaque) void,
};

const WaitQueue = struct {
    head: ?*Waiter = null,
    tail: ?*Waiter = null,

    fn pushBack(self: *WaitQueue, w: *Waiter) void {
        assert(!w.linked);
        w.prev = self.tail;
        w.next = null;
        w.linked = true;
        if (self.tail) |t| {
            t.next = w;
        } else {
            self.head = w;
        }
        self.tail = w;
    }

    fn popFront(self: *WaitQueue) ?*Waiter {
        const w = self.head orelse return null;
        assert(w.linked);
        self.head = w.next;
        if (self.head) |h| {
            h.prev = null;
        } else {
            self.tail = null;
        }
        w.prev = null;
        w.next = null;
        w.linked = false;
        return w;
    }

    fn remove(self: *WaitQueue, w: *Waiter) void {
        if (!w.linked) return;
        if (w.prev) |p| {
            p.next = w.next;
        } else {
            self.head = w.next;
        }
        if (w.next) |n| {
            n.prev = w.prev;
        } else {
            self.tail = w.prev;
        }
        w.prev = null;
        w.next = null;
        w.linked = false;
    }

    fn wakeAll(self: *WaitQueue) usize {
        var count: usize = 0;
        while (self.popFront()) |w| {
            w.ready = true;
            count += 1;
            w.wake_fn(w.ctx);
        }
        return count;
    }

    fn wakeOne(self: *WaitQueue) bool {
        const w = self.popFront() orelse return false;
        w.ready = true;
        w.wake_fn(w.ctx);
        return true;
    }
};

const FdSlot = struct {
    generation: u32 = 1,
    registered: bool = false,
    hup_or_err: bool = false,
    read_waiters: WaitQueue = .{},
    write_waiters: WaitQueue = .{},
};

epoll_fd: linux.fd_t,
event_fd: linux.fd_t,
max_fds: u32,
doorbell: Doorbell = .init,
slots: std.ArrayListUnmanaged(FdSlot) = .empty,
events_buf: [128]linux.epoll_event = undefined,

pub fn init(max_fds: u32) InitError!Epoll {
    const ep_rc = linux.epoll_create1(linux.EPOLL.CLOEXEC);
    const ep_fd: linux.fd_t = switch (linux.errno(ep_rc)) {
        .SUCCESS => @intCast(ep_rc),
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    };
    errdefer _ = linux.close(ep_fd);

    const ev_rc = linux.eventfd(0, linux.EFD.NONBLOCK | linux.EFD.CLOEXEC);
    const ev_fd: linux.fd_t = switch (linux.errno(ev_rc)) {
        .SUCCESS => @intCast(ev_rc),
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    };
    errdefer _ = linux.close(ev_fd);

    var ev: linux.epoll_event = .{
        .events = linux.EPOLL.IN | linux.EPOLL.ET,
        .data = .{ .u64 = eventfd_sentinel },
    };
    switch (linux.errno(linux.epoll_ctl(ep_fd, linux.EPOLL.CTL_ADD, ev_fd, &ev))) {
        .SUCCESS => {},
        .NOMEM, .NOSPC => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }

    return .{
        .epoll_fd = ep_fd,
        .event_fd = ev_fd,
        .max_fds = if (max_fds == 0) default_max_fds else max_fds,
    };
}

pub fn deinit(self: *Epoll, gpa: Allocator) void {
    self.slots.deinit(gpa);
    _ = linux.close(self.event_fd);
    _ = linux.close(self.epoll_fd);
    self.* = undefined;
}

/// Safe to call from any thread: coalesces wakeups via `Doorbell` and writes to
/// `event_fd` at most once per consumer sleep cycle.
pub fn notifyFromAnyThread(self: *Epoll) void {
    if (!self.doorbell.claimWakeup()) return;
    const val: u64 = 1;
    while (true) {
        const rc = linux.write(self.event_fd, @ptrCast(&val), @sizeOf(u64));
        switch (linux.errno(rc)) {
            .SUCCESS, .AGAIN => return,
            .INTR => continue,
            else => unreachable,
        }
    }
}

/// Parks `waiter` for `dir` readiness on `fd`.
/// If `fd` already observed sticky `EPOLLERR | EPOLLHUP`, marks `waiter.ready = true`
/// and invokes `waiter.wake_fn` immediately.
pub fn waitIo(
    self: *Epoll,
    gpa: Allocator,
    fd: linux.fd_t,
    dir: Direction,
    waiter: *Waiter,
) RegisterError!void {
    assert(fd >= 0);
    const ufd: usize = @intCast(fd);
    if (ufd >= self.max_fds) return error.ProcessFdQuotaExceeded;

    if (ufd >= self.slots.items.len) {
        const new_len = ufd + 1;
        try self.slots.ensureTotalCapacity(gpa, new_len);
        const old_len = self.slots.items.len;
        self.slots.items.len = new_len;
        @memset(self.slots.items[old_len..new_len], .{});
    }

    const slot = &self.slots.items[ufd];
    if (!slot.registered) {
        var ev: linux.epoll_event = .{
            .events = linux.EPOLL.IN | linux.EPOLL.OUT | linux.EPOLL.RDHUP | linux.EPOLL.ET,
            .data = .{ .u64 = packData(fd, slot.generation) },
        };
        const ctl_rc = linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_ADD, fd, &ev);
        switch (linux.errno(ctl_rc)) {
            .SUCCESS => {},
            .EXIST => {
                const mod_rc = linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_MOD, fd, &ev);
                switch (linux.errno(mod_rc)) {
                    .SUCCESS => {},
                    .NOMEM, .NOSPC => return error.SystemResources,
                    else => |err| return posix.unexpectedErrno(err),
                }
            },
            .PERM => return error.PollUnsupported,
            .NOMEM, .NOSPC => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
        slot.registered = true;
        slot.hup_or_err = false;
    }

    waiter.ready = false;
    if (slot.hup_or_err) {
        waiter.ready = true;
        waiter.wake_fn(waiter.ctx);
        return;
    }

    switch (dir) {
        .read => slot.read_waiters.pushBack(waiter),
        .write => slot.write_waiters.pushBack(waiter),
    }
}

pub fn cancelWaiter(self: *Epoll, fd: linux.fd_t, dir: Direction, waiter: *Waiter) void {
    if (!waiter.linked) return;
    assert(fd >= 0);
    const ufd: usize = @intCast(fd);
    assert(ufd < self.slots.items.len);
    const slot = &self.slots.items[ufd];
    switch (dir) {
        .read => slot.read_waiters.remove(waiter),
        .write => slot.write_waiters.remove(waiter),
    }
}

/// Cascades edge-triggered readiness to the next queued waiter when a woken
/// operation completes without draining the socket to `EAGAIN`.
pub fn cascadeNext(self: *Epoll, fd: linux.fd_t, dir: Direction) void {
    if (fd < 0) return;
    const ufd: usize = @intCast(fd);
    if (ufd >= self.slots.items.len) return;
    const slot = &self.slots.items[ufd];
    if (!slot.registered) return;
    _ = switch (dir) {
        .read => slot.read_waiters.wakeOne(),
        .write => slot.write_waiters.wakeOne(),
    };
}

/// Must be called BEFORE `close(fd)` so duplicated descriptors do not keep a
/// stale entry in `epoll_fd` and fd-number reuse cannot collide with old events.
pub fn unregisterFd(self: *Epoll, fd: linux.fd_t) void {
    if (fd < 0) return;
    const ufd: usize = @intCast(fd);
    if (ufd >= self.slots.items.len) return;
    const slot = &self.slots.items[ufd];
    if (slot.registered) {
        _ = linux.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null);
        slot.registered = false;
        slot.hup_or_err = false;
        slot.generation +%= 1;
        if (slot.generation == 0 or slot.generation == std.math.maxInt(u32)) {
            slot.generation = 1;
        }
    }
    _ = slot.read_waiters.wakeAll();
    _ = slot.write_waiters.wakeAll();
}

/// Polls `epoll_wait` once with `timeout_ms` (`-1` = infinite, `0` = immediate).
/// Returns total number of kernel events processed.
pub fn poll(self: *Epoll, timeout_ms: i32) usize {
    const rc = linux.epoll_wait(self.epoll_fd, &self.events_buf, self.events_buf.len, timeout_ms);
    const n: usize = switch (linux.errno(rc)) {
        .SUCCESS => rc,
        .INTR => return 0,
        else => unreachable,
    };

    for (self.events_buf[0..n]) |ev| {
        const tag = ev.data.u64;
        if (tag == eventfd_sentinel) {
            self.drainEventFd();
            continue;
        }
        const fd: linux.fd_t = @bitCast(@as(u32, @truncate(tag)));
        const gen: u32 = @truncate(tag >> 32);
        if (fd < 0) continue;
        const ufd: usize = @intCast(fd);
        if (ufd >= self.slots.items.len) continue;
        const slot = &self.slots.items[ufd];
        if (!slot.registered or slot.generation != gen) continue;

        const flags = ev.events;
        if ((flags & (linux.EPOLL.ERR | linux.EPOLL.HUP)) != 0) {
            slot.hup_or_err = true;
            _ = slot.read_waiters.wakeAll();
            _ = slot.write_waiters.wakeAll();
            continue;
        }
        if ((flags & (linux.EPOLL.IN | linux.EPOLL.RDHUP | linux.EPOLL.PRI)) != 0) {
            _ = slot.read_waiters.wakeOne();
        }
        if ((flags & linux.EPOLL.OUT) != 0) {
            _ = slot.write_waiters.wakeOne();
        }
    }
    return n;
}

fn drainEventFd(self: *Epoll) void {
    var counter: u64 = 0;
    while (true) {
        const rc = linux.read(self.event_fd, @ptrCast(&counter), @sizeOf(u64));
        switch (linux.errno(rc)) {
            .SUCCESS => continue,
            .AGAIN => return,
            .INTR => continue,
            else => unreachable,
        }
    }
}

inline fn packData(fd: linux.fd_t, generation: u32) u64 {
    const raw_fd: u32 = @bitCast(fd);
    return (@as(u64, generation) << 32) | @as(u64, raw_fd);
}

test "Epoll wakes on pipe readiness, rejects regular file with PollUnsupported, and handles doorbell" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;

    const gpa = std.testing.allocator;
    var ep = try Epoll.init(1024);
    defer ep.deinit(gpa);

    // Doorbell wakeup test
    ep.doorbell.armBeforeSleep();
    ep.notifyFromAnyThread();
    try std.testing.expectEqual(@as(usize, 1), ep.poll(50));

    // Pipe read/write readiness test
    const fds = try std.Io.Threaded.pipe2(.{ .NONBLOCK = true, .CLOEXEC = true });
    defer {
        ep.unregisterFd(fds[0]);
        ep.unregisterFd(fds[1]);
        std.Io.Threaded.closeFd(fds[0]);
        std.Io.Threaded.closeFd(fds[1]);
    }

    const Flag = struct {
        woken: bool = false,
        fn wake(ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.woken = true;
        }
    };
    var flag: Flag = .{};
    var waiter: Waiter = .{ .ctx = &flag, .wake_fn = Flag.wake };
    try ep.waitIo(gpa, fds[0], .read, &waiter);
    try std.testing.expect(!flag.woken);

    const msg = "ok";
    _ = linux.write(fds[1], msg.ptr, msg.len);
    _ = ep.poll(50);
    try std.testing.expect(flag.woken);
    try std.testing.expect(waiter.ready);

    // Regular file (/dev/null) returns EPERM -> error.PollUnsupported
    const null_rc = linux.openat(linux.AT.FDCWD, "/dev/null", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    try std.testing.expect(linux.errno(null_rc) == .SUCCESS);
    const null_fd: linux.fd_t = @intCast(null_rc);
    defer std.Io.Threaded.closeFd(null_fd);

    var file_waiter: Waiter = .{ .ctx = &flag, .wake_fn = Flag.wake };
    try std.testing.expectError(error.PollUnsupported, ep.waitIo(gpa, null_fd, .read, &file_waiter));
}
