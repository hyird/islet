//! Linux `epoll` data-plane I/O driver and zero-allocation `Io.Batch` engine.
//!
//! Design invariants (AGENTS.md §2, §4, §5):
//! - Concentrates all Linux-specific socket, streaming `operate`, `pidfd` process
//!   waiting, and `Io.Batch` state machines out of generic `Shard` scheduling.
//! - Common `Io.Batch` operations (`file_read_streaming`, `file_write_streaming`,
//!   `net_read`, `net_write`, `net_receive`, `net_send`) pack their `Epoll.Waiter`
//!   and operation descriptor directly inside `Io.Operation.Storage.Pending.userdata`
//!   (`56` bytes on 64-bit Linux) with **zero heap allocation**.
//! - Exotic multi-header/ancillary-control or `BlockingPool`-offloaded `Batch`
//!   entries fall back to a watermark-bounded `OverflowOp` cache (`max_cached = 64`).

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const linux = std.os.linux;
const posix = std.posix;
const net = Io.net;
const File = Io.File;
const Dir = Io.Dir;
const Threaded = Io.Threaded;

const Epoll = @import("Epoll.zig");
const BlockingPool = @import("BlockingPool.zig");
const Mailbox = @import("Mailbox.zig");
const ResumeLane = Mailbox.ResumeLane;
const TimerHeap = @import("TimerHeap.zig");
const Acceptor = @import("Acceptor.zig").Acceptor;
const time = @import("time.zig");

const LinuxIo = @This();

const max_cached_overflow_ops: usize = 64;

pub const InitError = Epoll.InitError;

threadlocal var tls_active_driver: ?*LinuxIo = null;

/// Compact 56-byte (`7 * usize`) inline `Batch` pending state living directly inside
/// `Io.Operation.Storage.Pending.userdata` with zero heap allocation.
const InlineBatchSlot = struct {
    waiter: Epoll.Waiter,
    /// Low 3 bits store `net.ReceiveFlags` when `tag == .net_receive`; high bits store `*Io.Batch`.
    batch_packed: usize,
    payload: Payload,

    const Payload = extern union {
        slice_op: extern struct {
            ptr: usize,
            len: usize,
            fd: linux.fd_t,
            aux: u32,
        },
        recv_op: extern struct {
            msg_ptr: *net.IncomingMessage,
            data_ptr: [*]u8,
            fd: linux.fd_t,
            data_len: u32,
        },
    };

    comptime {
        if (@sizeOf(usize) == 8) {
            assert(@sizeOf(@This()) == @sizeOf(Io.Operation.Storage.Pending.Userdata));
            assert(@alignOf(@This()) <= @alignOf(Io.Operation.Storage.Pending.Userdata));
        }
    }
};

const OverflowOp = struct {
    free_next: ?*OverflowOp = null,
    driver: *LinuxIo = undefined,
    batch: *Io.Batch = undefined,
    slot_index: u32 = 0,
    op: Io.Operation = undefined,
    canceled: bool = false,
    result: ?Io.Operation.Result = null,
    backend: union(enum) {
        none,
        epoll: struct {
            fd: linux.fd_t,
            dir: Epoll.Direction,
            waiter: Epoll.Waiter,
        },
        blocking: BlockingPool.Job,
    } = .none,
};

gpa: Allocator,
poller: Epoll,
owner_ctx: *anyopaque,
wake_task_fn: *const fn (owner_ctx: *anyopaque, task_ptr: *anyopaque) void,
blocking_pool: *BlockingPool,
resume_lane: *ResumeLane,
overflow_free: ?*OverflowOp = null,
cached_overflow_count: usize = 0,

/// Initializes `self` in-place (address-pinned).
pub fn init(
    self: *LinuxIo,
    gpa: Allocator,
    max_fds: u32,
    acceptor: ?*Acceptor,
    shard_id: usize,
    blocking_pool: *BlockingPool,
    resume_lane: *ResumeLane,
    owner_ctx: *anyopaque,
    wake_task_fn: *const fn (*anyopaque, *anyopaque) void,
) InitError!void {
    _ = acceptor;
    _ = shard_id;
    const poller = try Epoll.init(max_fds);
    self.* = .{
        .gpa = gpa,
        .poller = poller,
        .owner_ctx = owner_ctx,
        .wake_task_fn = wake_task_fn,
        .blocking_pool = blocking_pool,
        .resume_lane = resume_lane,
    };
}

pub fn deinit(self: *LinuxIo, gpa: Allocator) void {
    var cur = self.overflow_free;
    self.overflow_free = null;
    self.cached_overflow_count = 0;
    while (cur) |op| {
        const next = op.free_next;
        gpa.destroy(op);
        cur = next;
    }
    self.poller.deinit(gpa);
    self.* = undefined;
}

pub inline fn notifyFromAnyThread(self: *LinuxIo) void {
    self.poller.notifyFromAnyThread();
}

pub inline fn armDoorbell(self: *LinuxIo) void {
    self.poller.doorbell.armBeforeSleep();
}

pub inline fn disarmDoorbell(self: *LinuxIo) void {
    self.poller.doorbell.disarm();
}

pub inline fn isAcceptorEmptySeqCst(_: *const LinuxIo) bool {
    return true;
}

pub inline fn drainAcceptor(_: *LinuxIo) usize {
    return 0;
}

pub inline fn onStopRequested(_: *LinuxIo) void {}

pub fn poll(self: *LinuxIo, wait_ns: ?i96) usize {
    const prev = tls_active_driver;
    tls_active_driver = self;
    defer tls_active_driver = prev;

    const timeout_ms: i32 = if (wait_ns) |ns|
        @intCast(time.ceilWaitMs(ns))
    else
        -1;
    return self.poller.poll(timeout_ms);
}

// ── Clock ───────────────────────────────────────────────────────────────────

pub fn now(_: *LinuxIo, clock: Io.Clock) Io.Timestamp {
    var ts: posix.timespec = undefined;
    const rc = linux.clock_gettime(Threaded.clockToPosix(clock), &ts);
    if (linux.errno(rc) != .SUCCESS) return .zero;
    return Threaded.timestampFromPosix(&ts);
}

pub fn clockResolution(_: *LinuxIo, clock: Io.Clock) Io.Clock.ResolutionError!Io.Duration {
    var ts: posix.timespec = undefined;
    const rc = linux.clock_getres(Threaded.clockToPosix(clock), &ts);
    switch (linux.errno(rc)) {
        .SUCCESS => return .{ .nanoseconds = Threaded.nanosecondsFromPosix(&ts) },
        .INVAL => return error.ClockUnavailable,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ── Single-Wait `Epoll` Helper ──────────────────────────────────────────────

fn waitFd(
    self: *LinuxIo,
    shard: anytype,
    fd: linux.fd_t,
    dir: Epoll.Direction,
    maybe_deadline_ns: ?i96,
) (Epoll.RegisterError || Io.Cancelable || Io.Timeout.Error)!void {
    try shard.checkCancelPoint();

    if (maybe_deadline_ns) |dl| {
        if (dl <= shard.awakeNowNs()) return error.Timeout;
    }

    const TaskPtr = @TypeOf(shard.current_task);
    const ShardPtr = @TypeOf(shard);

    const WaitState = struct {
        driver: *LinuxIo,
        shard_ptr: ShardPtr,
        task: TaskPtr,
        fd: linux.fd_t,
        dir: Epoll.Direction,
        waiter: Epoll.Waiter = undefined,
        timer_token: TimerHeap.Token = .inactive,
        done: bool = false,
        woken_by_io: bool = false,
        timed_out: bool = false,
        canceled: bool = false,

        fn onIoReady(w: *Epoll.Waiter) void {
            const st: *@This() = @fieldParentPtr("waiter", w);
            if (st.done) return;
            if (st.timer_token.isActive()) {
                _ = st.shard_ptr.timers.cancel(st.timer_token);
                st.timer_token = .inactive;
            }
            st.woken_by_io = true;
            st.done = true;
            st.task.cancel_hook = null;
            st.shard_ptr.enqueueReady(st.task);
        }

        fn onTimer(ctx: ?*anyopaque, outcome: TimerHeap.Outcome) void {
            _ = outcome;
            const st: *@This() = @ptrCast(@alignCast(ctx.?));
            st.timer_token = .inactive;
            if (st.done) return;
            st.driver.poller.cancelWaiter(st.fd, st.dir, &st.waiter);
            st.timed_out = true;
            st.done = true;
            st.task.cancel_hook = null;
            st.shard_ptr.enqueueReady(st.task);
        }

        fn onCancel(s: ShardPtr, raw_ctx: *anyopaque) void {
            const st: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (st.done) return;
            if (st.timer_token.isActive()) {
                _ = s.timers.cancel(st.timer_token);
                st.timer_token = .inactive;
            }
            st.driver.poller.cancelWaiter(st.fd, st.dir, &st.waiter);
            st.canceled = true;
            st.done = true;
            s.enqueueReady(st.task);
        }
    };

    const cur = shard.current_task;
    var st: WaitState = .{
        .driver = self,
        .shard_ptr = shard,
        .task = cur,
        .fd = fd,
        .dir = dir,
        .waiter = .{ .wake_fn = WaitState.onIoReady },
    };

    try self.poller.waitIo(self.gpa, fd, dir, &st.waiter);

    if (maybe_deadline_ns) |dl| {
        errdefer self.poller.cancelWaiter(fd, dir, &st.waiter);
        st.timer_token = try shard.timers.schedule(self.gpa, dl, &st, WaitState.onTimer);
    }

    cur.cancel_hook = .{ .ctx = &st, .trigger = WaitState.onCancel };
    defer {
        cur.cancel_hook = null;
        if (st.timer_token.isActive()) _ = shard.timers.cancel(st.timer_token);
        self.poller.cancelWaiter(fd, dir, &st.waiter);
    }

    while (!st.done) {
        shard.suspendCurrent();
    }

    if (st.canceled or cur.shouldCancelNow()) {
        if (st.woken_by_io) {
            // Pass the `EPOLLET` readiness baton to any sibling waiter queued behind us!
            self.poller.cascadeNext(fd, dir);
        }
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
    if (st.timed_out) return error.Timeout;
}

// ── File / Dir / Child Process Lifecycle ────────────────────────────────────

pub fn fileClose(self: *LinuxIo, _: anytype, files: []const File) void {
    for (files) |f| {
        self.poller.unregisterFd(f.handle);
        Threaded.closeFd(f.handle);
    }
}

pub fn dirClose(self: *LinuxIo, _: anytype, dirs: []const Dir) void {
    for (dirs) |d| {
        self.poller.unregisterFd(d.handle);
        Threaded.closeFd(d.handle);
    }
}

fn mapSendErrorToNetSend(err: linux.E) net.Socket.SendError {
    return switch (err) {
        .MSGSIZE => error.MessageOversize,
        .NOMEM, .NOBUFS => error.SystemResources,
        .NETUNREACH => error.NetworkUnreachable,
        .HOSTUNREACH => error.HostUnreachable,
        .NETDOWN => error.NetworkDown,
        .CONNREFUSED => error.ConnectionRefused,
        .AFNOSUPPORT => error.AddressFamilyUnsupported,
        .ALREADY => error.FastOpenAlreadyInProgress,
        .CONNRESET => error.ConnectionResetByPeer,
        .PIPE, .NOTCONN => error.SocketUnconnected,
        .ACCES => error.AccessDenied,
        .TIMEDOUT => error.ConnectionTimedOut,
        else => posix.unexpectedErrno(err),
    };
}

const CopyRangeArgs = struct {
    in_fd: linux.fd_t,
    has_in_off: bool,
    in_off: linux.off_t,
    out_fd: linux.fd_t,
    has_out_off: bool,
    out_off: linux.off_t,
    len: usize,
};

fn execCopyFileRange(_: *Threaded, args: CopyRangeArgs) File.WriteFilePositionalError!usize {
    var in_off = args.in_off;
    var out_off = args.out_off;
    while (true) {
        const rc = linux.copy_file_range(
            args.in_fd,
            if (args.has_in_off) &in_off else null,
            args.out_fd,
            if (args.has_out_off) &out_off else null,
            args.len,
            0,
        );
        switch (linux.errno(rc)) {
            .SUCCESS => return rc,
            .INTR => continue,
            .OPNOTSUPP, .INVAL, .NOSYS, .XDEV, .BADF, .ISDIR => return error.Unimplemented,
            .SPIPE, .NXIO => return error.Unseekable,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOMEM => return error.SystemResources,
            .NOSPC => return error.NoSpaceLeft,
            .PERM => return error.PermissionDenied,
            .BUSY => return error.DeviceBusy,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn fileWriteFileStreaming(
    self: *LinuxIo,
    shard: anytype,
    file: File,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
) File.Writer.WriteFileError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    const reader_buffered = file_reader.interface.buffered();

    if (reader_buffered.len >= @backingInt(limit)) {
        const op_res = try self.operate(shard, .{ .file_write_streaming = .{
            .file = file,
            .header = header,
            .data = &.{limit.slice(reader_buffered)},
            .splat = 1,
        } });
        const n = try op_res.file_write_streaming;
        file_reader.interface.toss(n -| header.len);
        return n;
    }

    if (file_reader.size) |size| {
        if (size - file_reader.pos == 0) {
            if (header.len != 0 or reader_buffered.len != 0) {
                const op_res = try self.operate(shard, .{ .file_write_streaming = .{
                    .file = file,
                    .header = header,
                    .data = &.{limit.slice(reader_buffered)},
                    .splat = 1,
                } });
                const n = try op_res.file_write_streaming;
                file_reader.interface.toss(n -| header.len);
                return n;
            }
            return error.EndOfStream;
        }
    }

    if (header.len != 0 or reader_buffered.len != 0) {
        const op_res = try self.operate(shard, .{ .file_write_streaming = .{
            .file = file,
            .header = header,
            .data = &.{limit.slice(reader_buffered)},
            .splat = 1,
        } });
        const n = try op_res.file_write_streaming;
        file_reader.interface.toss(n -| header.len);
        return n;
    }

    const max_count: usize = 0x7ffff000;
    var in_off: linux.off_t = 0;
    const has_in_off: bool, const count: usize = switch (file_reader.mode) {
        .positional => blk: {
            const size = file_reader.getSize() catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return error.Unimplemented,
            };
            if (size <= file_reader.pos) {
                file_reader.size = file_reader.pos;
                return error.EndOfStream;
            }
            in_off = std.math.cast(linux.off_t, file_reader.pos) orelse return error.ReadFailed;
            break :blk .{ true, @min(@backingInt(limit), size - file_reader.pos, max_count) };
        },
        .streaming => .{ false, @min(@backingInt(limit), max_count) },
        .positional_simple, .streaming_simple => return error.Unimplemented,
        .failure => return error.ReadFailed,
    };
    if (count == 0) return 0;

    if (file.flags.nonblocking) {
        while (true) {
            const rc = linux.sendfile(file.handle, file_reader.file.handle, if (has_in_off) &in_off else null, count);
            switch (linux.errno(rc)) {
                .SUCCESS => {
                    self.poller.cascadeNext(file.handle, .write);
                    if (rc == 0) {
                        file_reader.size = file_reader.pos;
                        return error.EndOfStream;
                    }
                    file_reader.pos += rc;
                    return rc;
                },
                .INTR => continue,
                .AGAIN => {
                    self.waitFd(shard, file.handle, .write, null) catch |err| switch (err) {
                        error.Canceled => return error.Canceled,
                        error.OutOfMemory, error.SystemResources => return error.SystemResources,
                        else => return error.Unexpected,
                    };
                },
                .INVAL, .NOSYS, .OPNOTSUPP, .SPIPE, .NXIO => return error.Unimplemented,
                .PIPE, .NOTCONN => return error.BrokenPipe,
                .IO => return error.InputOutput,
                .NOMEM, .NOBUFS => return error.SystemResources,
                else => |err| return posix.unexpectedErrno(err),
            }
        }
    }

    const copied = shard.callBlocking(true, execCopyFileRange, .{CopyRangeArgs{
        .in_fd = file_reader.file.handle,
        .has_in_off = has_in_off,
        .in_off = in_off,
        .out_fd = file.handle,
        .has_out_off = false,
        .out_off = 0,
        .len = count,
    }}) catch |err| switch (err) {
        error.Unseekable => return error.Unimplemented,
        else => |e| return e,
    };
    if (copied == 0) {
        file_reader.size = file_reader.pos;
        return error.EndOfStream;
    }
    file_reader.pos += copied;
    return copied;
}

pub fn fileWriteFilePositional(
    _: *LinuxIo,
    shard: anytype,
    file: File,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
    offset: u64,
) File.WriteFilePositionalError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    const reader_buffered = file_reader.interface.buffered();
    const shard_io = shard.io();

    if (header.len != 0 or reader_buffered.len != 0) {
        const n = try shard_io.vtable.fileWritePositional(
            shard_io.userdata,
            file,
            header,
            &.{limit.slice(reader_buffered)},
            1,
            offset,
        );
        file_reader.interface.toss(n -| header.len);
        return n;
    }

    if (file_reader.size) |size| {
        if (size - file_reader.pos == 0) return error.EndOfStream;
    }

    const out_off = std.math.cast(linux.off_t, offset) orelse return error.Unseekable;
    const max_count: usize = 0x7ffff000;
    var in_off: linux.off_t = 0;
    const has_in_off: bool, const count: usize = switch (file_reader.mode) {
        .positional => blk: {
            const size = file_reader.getSize() catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return error.Unimplemented,
            };
            if (size <= file_reader.pos) {
                file_reader.size = file_reader.pos;
                return error.EndOfStream;
            }
            in_off = std.math.cast(linux.off_t, file_reader.pos) orelse return error.ReadFailed;
            break :blk .{ true, @min(@backingInt(limit), size - file_reader.pos, max_count) };
        },
        .streaming => .{ false, @min(@backingInt(limit), max_count) },
        .positional_simple, .streaming_simple => return error.Unimplemented,
        .failure => return error.ReadFailed,
    };
    if (count == 0) return 0;

    const copied = try shard.callBlocking(true, execCopyFileRange, .{CopyRangeArgs{
        .in_fd = file_reader.file.handle,
        .has_in_off = has_in_off,
        .in_off = in_off,
        .out_fd = file.handle,
        .has_out_off = true,
        .out_off = out_off,
        .len = count,
    }});
    if (copied == 0) {
        file_reader.size = file_reader.pos;
        return error.EndOfStream;
    }
    file_reader.pos += copied;
    return copied;
}

pub fn processSpawn(_: *LinuxIo, shard: anytype, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
    var sanitized = options;
    sanitized.progress_node = .none;

    var child = try shard.callBlocking(true, struct {
        fn run(t: *Threaded, opts: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
            const t_io = t.io();
            return t_io.vtable.processSpawn(t_io.userdata, opts);
        }
    }.run, .{sanitized});

    inline for (.{ "stdin", "stdout", "stderr" }) |pipe_field| {
        if (@field(child, pipe_field)) |*f| {
            const fl = linux.fcntl(f.handle, linux.F.GETFL, 0);
            if (linux.errno(fl) == .SUCCESS) {
                const nonblock_bit: usize = @as(u32, @bitCast(linux.O{ .NONBLOCK = true }));
                if (linux.errno(linux.fcntl(f.handle, linux.F.SETFL, fl | nonblock_bit)) == .SUCCESS) {
                    f.flags.nonblocking = true;
                }
            }
        }
    }
    return child;
}

fn unregisterChildPipes(self: *LinuxIo, child: *const std.process.Child) void {
    if (child.stdin) |f| self.poller.unregisterFd(f.handle);
    if (child.stdout) |f| self.poller.unregisterFd(f.handle);
    if (child.stderr) |f| self.poller.unregisterFd(f.handle);
}

pub fn childWait(self: *LinuxIo, shard: anytype, child: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
    try shard.checkCancelPoint();

    if (child.id) |pid| {
        const nonblock_flag: u32 = @bitCast(linux.O{ .NONBLOCK = true });
        const pfd_rc = linux.pidfd_open(pid, nonblock_flag);
        if (linux.errno(pfd_rc) == .SUCCESS) {
            const pfd: linux.fd_t = @intCast(pfd_rc);
            defer {
                self.poller.unregisterFd(pfd);
                Threaded.closeFd(pfd);
            }
            self.waitFd(shard, pfd, .read, null) catch |err| switch (err) {
                error.Canceled => {
                    self.unregisterChildPipes(child);
                    if (child.stdin) |f| Threaded.closeFd(f.handle);
                    if (child.stdout) |f| Threaded.closeFd(f.handle);
                    if (child.stderr) |f| Threaded.closeFd(f.handle);
                    child.stdin = null;
                    child.stdout = null;
                    child.stderr = null;
                    child.id = null;
                    return error.Canceled;
                },
                else => {},
            };
        }
    }

    self.unregisterChildPipes(child);
    return shard.callBlocking(true, struct {
        fn run(t: *Threaded, c: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
            const t_io = t.io();
            return t_io.vtable.childWait(t_io.userdata, c);
        }
    }.run, .{child});
}

pub fn childKill(self: *LinuxIo, shard: anytype, child: *std.process.Child) void {
    self.unregisterChildPipes(child);
    shard.callBlocking(false, struct {
        fn run(t: *Threaded, c: *std.process.Child) void {
            const t_io = t.io();
            t_io.vtable.childKill(t_io.userdata, c);
        }
    }.run, .{child});
}

// ── Nonblocking Streaming `operate` ─────────────────────────────────────────

const TryPosixOp = union(enum) {
    done: Io.Operation.Result,
    would_block: struct { fd: linux.fd_t, dir: Epoll.Direction },
    needs_pool,
};

fn fillWriteIovecsPosix(
    iovecs: *[Threaded.max_iovecs_len]posix.iovec_const,
    splat_buf: *[Threaded.splat_buffer_size]u8,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) usize {
    var len: usize = 0;
    const appendSlice = struct {
        fn add(out: *[Threaded.max_iovecs_len]posix.iovec_const, out_len: *usize, bytes: []const u8) void {
            if (bytes.len == 0 or out_len.* >= out.len) return;
            out[out_len.*] = .{ .base = bytes.ptr, .len = bytes.len };
            out_len.* += 1;
        }
    }.add;

    appendSlice(iovecs, &len, header);
    if (data.len == 0) return len;
    for (data[0 .. data.len - 1]) |seg| appendSlice(iovecs, &len, seg);
    const pattern = data[data.len - 1];
    if (len < iovecs.len) switch (splat) {
        0 => {},
        1 => appendSlice(iovecs, &len, pattern),
        else => switch (pattern.len) {
            0 => {},
            1 => {
                const fill_n = @min(splat_buf.len, splat);
                @memset(splat_buf[0..fill_n], pattern[0]);
                var rem = splat;
                while (rem > 0 and len < iovecs.len) {
                    const chunk = @min(rem, fill_n);
                    appendSlice(iovecs, &len, splat_buf[0..chunk]);
                    rem -= chunk;
                }
            },
            else => for (0..@min(splat, iovecs.len - len)) |_| {
                appendSlice(iovecs, &len, pattern);
            },
        },
    };
    return len;
}

fn tryExecuteNonblocking(op: Io.Operation) TryPosixOp {
    switch (op) {
        .file_read_streaming => |o| {
            if (!o.file.flags.nonblocking) return .needs_pool;
            var iovecs: [Threaded.max_iovecs_len]posix.iovec = undefined;
            var n_iov: usize = 0;
            for (o.data) |buf| {
                if (n_iov == iovecs.len) break;
                if (buf.len > 0) {
                    iovecs[n_iov] = .{ .base = buf.ptr, .len = buf.len };
                    n_iov += 1;
                }
            }
            if (n_iov == 0) return .{ .done = .{ .file_read_streaming = 0 } };
            while (true) {
                const rc = linux.readv(o.file.handle, &iovecs, @intCast(n_iov));
                switch (linux.errno(rc)) {
                    .SUCCESS => return .{ .done = .{
                        .file_read_streaming = if (rc == 0) error.EndOfStream else rc,
                    } },
                    .INTR => continue,
                    .AGAIN => return .{ .would_block = .{ .fd = o.file.handle, .dir = .read } },
                    .IO => return .{ .done = .{ .file_read_streaming = error.InputOutput } },
                    .ISDIR => return .{ .done = .{ .file_read_streaming = error.IsDir } },
                    .CONNRESET => return .{ .done = .{ .file_read_streaming = error.ConnectionResetByPeer } },
                    .NOTCONN => return .{ .done = .{ .file_read_streaming = error.SocketUnconnected } },
                    .ACCES => return .{ .done = .{ .file_read_streaming = error.AccessDenied } },
                    .NOMEM, .NOBUFS => return .{ .done = .{ .file_read_streaming = error.SystemResources } },
                    .BADF => return .{ .done = .{ .file_read_streaming = error.NotOpenForReading } },
                    else => |err| return .{ .done = .{ .file_read_streaming = posix.unexpectedErrno(err) } },
                }
            }
        },
        .file_write_streaming => |o| {
            if (!o.file.flags.nonblocking) return .needs_pool;
            var iovecs: [Threaded.max_iovecs_len]posix.iovec_const = undefined;
            var splat_buf: [Threaded.splat_buffer_size]u8 = undefined;
            const n_iov = fillWriteIovecsPosix(&iovecs, &splat_buf, o.header, o.data, o.splat);
            if (n_iov == 0) return .{ .done = .{ .file_write_streaming = 0 } };
            while (true) {
                const rc = linux.writev(o.file.handle, &iovecs, @intCast(n_iov));
                switch (linux.errno(rc)) {
                    .SUCCESS => return .{ .done = .{ .file_write_streaming = rc } },
                    .INTR => continue,
                    .AGAIN => return .{ .would_block = .{ .fd = o.file.handle, .dir = .write } },
                    .PIPE => return .{ .done = .{ .file_write_streaming = error.BrokenPipe } },
                    .DQUOT => return .{ .done = .{ .file_write_streaming = error.DiskQuota } },
                    .FBIG => return .{ .done = .{ .file_write_streaming = error.FileTooBig } },
                    .IO => return .{ .done = .{ .file_write_streaming = error.InputOutput } },
                    .NOSPC => return .{ .done = .{ .file_write_streaming = error.NoSpaceLeft } },
                    .ACCES => return .{ .done = .{ .file_write_streaming = error.AccessDenied } },
                    .PERM => return .{ .done = .{ .file_write_streaming = error.PermissionDenied } },
                    .NOMEM, .NOBUFS => return .{ .done = .{ .file_write_streaming = error.SystemResources } },
                    .BADF => return .{ .done = .{ .file_write_streaming = error.NotOpenForWriting } },
                    .NXIO => return .{ .done = .{ .file_write_streaming = error.NoDevice } },
                    .BUSY => return .{ .done = .{ .file_write_streaming = error.DeviceBusy } },
                    else => |err| return .{ .done = .{ .file_write_streaming = posix.unexpectedErrno(err) } },
                }
            }
        },
        .device_io_control => return .needs_pool,
        .net_read => |o| {
            var iovecs: [Threaded.max_iovecs_len]posix.iovec = undefined;
            var n_iov: usize = 0;
            for (o.data) |buf| {
                if (n_iov == iovecs.len) break;
                if (buf.len > 0) {
                    iovecs[n_iov] = .{ .base = buf.ptr, .len = buf.len };
                    n_iov += 1;
                }
            }
            if (n_iov == 0 and o.control.len == 0) {
                return .{ .done = .{ .net_read = .{ .data_len = 0 } } };
            }
            var msg: linux.msghdr = .{
                .name = null,
                .namelen = 0,
                .iov = &iovecs,
                .iovlen = @intCast(n_iov),
                .control = if (o.control.len > 0) o.control.ptr else null,
                .controllen = @intCast(o.control.len),
                .flags = 0,
            };
            while (true) {
                const rc = linux.recvmsg(o.socket_handle, &msg, linux.MSG.NOSIGNAL | linux.MSG.CMSG_CLOEXEC);
                switch (linux.errno(rc)) {
                    .SUCCESS => return .{ .done = .{ .net_read = .{
                        .data_len = rc,
                        .control_len = @intCast(msg.controllen),
                        .control_truncated = (msg.flags & linux.MSG.CTRUNC) != 0,
                    } } },
                    .INTR => continue,
                    .AGAIN => return .{ .would_block = .{ .fd = o.socket_handle, .dir = .read } },
                    .CONNRESET => return .{ .done = .{ .net_read = error.ConnectionResetByPeer } },
                    .NOTCONN => return .{ .done = .{ .net_read = error.SocketUnconnected } },
                    .ACCES => return .{ .done = .{ .net_read = error.AccessDenied } },
                    .NETDOWN => return .{ .done = .{ .net_read = error.NetworkDown } },
                    .TIMEDOUT => return .{ .done = .{ .net_read = error.ConnectionTimedOut } },
                    .NOMEM, .NOBUFS => return .{ .done = .{ .net_read = error.SystemResources } },
                    else => |err| return .{ .done = .{ .net_read = posix.unexpectedErrno(err) } },
                }
            }
        },
        .net_write => |o| {
            var iovecs: [Threaded.max_iovecs_len]posix.iovec_const = undefined;
            var splat_buf: [Threaded.splat_buffer_size]u8 = undefined;
            const n_iov = fillWriteIovecsPosix(&iovecs, &splat_buf, o.header, o.data, o.splat);
            if (n_iov == 0 and o.control.len == 0) {
                return .{ .done = .{ .net_write = 0 } };
            }
            var msg: linux.msghdr_const = .{
                .name = null,
                .namelen = 0,
                .iov = &iovecs,
                .iovlen = @intCast(n_iov),
                .control = if (o.control.len > 0) o.control.ptr else null,
                .controllen = @intCast(o.control.len),
                .flags = 0,
            };
            while (true) {
                const rc = linux.sendmsg(o.socket_handle, &msg, linux.MSG.NOSIGNAL);
                switch (linux.errno(rc)) {
                    .SUCCESS => return .{ .done = .{ .net_write = rc } },
                    .INTR => continue,
                    .AGAIN => return .{ .would_block = .{ .fd = o.socket_handle, .dir = .write } },
                    .PIPE, .NOTCONN => return .{ .done = .{ .net_write = error.SocketUnconnected } },
                    .CONNRESET => return .{ .done = .{ .net_write = error.ConnectionResetByPeer } },
                    .CONNREFUSED => return .{ .done = .{ .net_write = error.ConnectionRefused } },
                    .NETUNREACH => return .{ .done = .{ .net_write = error.NetworkUnreachable } },
                    .HOSTUNREACH => return .{ .done = .{ .net_write = error.HostUnreachable } },
                    .NETDOWN => return .{ .done = .{ .net_write = error.NetworkDown } },
                    .AFNOSUPPORT => return .{ .done = .{ .net_write = error.AddressFamilyUnsupported } },
                    .TIMEDOUT => return .{ .done = .{ .net_write = error.ConnectionTimedOut } },
                    .DESTADDRREQ => return .{ .done = .{ .net_write = error.SocketNotBound } },
                    .NOMEM, .NOBUFS => return .{ .done = .{ .net_write = error.SystemResources } },
                    else => |err| return .{ .done = .{ .net_write = posix.unexpectedErrno(err) } },
                }
            }
        },
        .net_receive => |o| {
            if (o.message_buffer.len == 0) return .{ .done = .{ .net_receive = .{ null, 0 } } };
            var msg_flags: u32 = linux.MSG.CMSG_CLOEXEC;
            if (o.flags.oob) msg_flags |= linux.MSG.OOB;
            if (o.flags.peek) msg_flags |= linux.MSG.PEEK;
            if (o.flags.trunc) msg_flags |= linux.MSG.TRUNC;

            var received_msgs: usize = 0;
            var data_offset: usize = 0;
            while (received_msgs < o.message_buffer.len) {
                const rem_buf = o.data_buffer[data_offset..];
                if (rem_buf.len == 0 and received_msgs > 0) break;

                const out_msg = &o.message_buffer[received_msgs];
                var iov: [1]posix.iovec = .{.{ .base = rem_buf.ptr, .len = rem_buf.len }};
                var peer: Threaded.PosixAddress = undefined;
                var hdr: linux.msghdr = .{
                    .name = @ptrCast(&peer.any),
                    .namelen = @sizeOf(Threaded.PosixAddress),
                    .iov = &iov,
                    .iovlen = 1,
                    .control = if (out_msg.control.len > 0) out_msg.control.ptr else null,
                    .controllen = @intCast(out_msg.control.len),
                    .flags = 0,
                };
                const rc = linux.recvmsg(o.socket_handle, &hdr, msg_flags);
                switch (linux.errno(rc)) {
                    .SUCCESS => {
                        const actual_copied = @min(rc, rem_buf.len);
                        out_msg.from = if (hdr.namelen > 0)
                            Threaded.addressFromPosix(&peer)
                        else
                            .{ .ip4 = .loopback(0) };
                        out_msg.data = rem_buf[0..actual_copied];
                        out_msg.control = out_msg.control[0..@intCast(hdr.controllen)];
                        out_msg.flags = .{
                            .eor = (hdr.flags & linux.MSG.EOR) != 0,
                            .trunc = (hdr.flags & linux.MSG.TRUNC) != 0,
                            .ctrunc = (hdr.flags & linux.MSG.CTRUNC) != 0,
                            .oob = (hdr.flags & linux.MSG.OOB) != 0,
                            .errqueue = (hdr.flags & linux.MSG.ERRQUEUE) != 0,
                        };
                        data_offset += actual_copied;
                        received_msgs += 1;
                        if (o.flags.peek) break;
                    },
                    .INTR => continue,
                    .AGAIN => {
                        if (received_msgs > 0) return .{ .done = .{ .net_receive = .{ null, received_msgs } } };
                        return .{ .would_block = .{ .fd = o.socket_handle, .dir = .read } };
                    },
                    else => |err| {
                        const mapped: net.Socket.ReceiveError = switch (err) {
                            .NOMEM, .NOBUFS => error.SystemResources,
                            .MFILE => error.ProcessFdQuotaExceeded,
                            .NFILE => error.SystemFdQuotaExceeded,
                            .NOTCONN => error.SocketUnconnected,
                            .MSGSIZE => error.MessageOversize,
                            .CONNRESET => error.ConnectionResetByPeer,
                            .NETDOWN => error.NetworkDown,
                            .CONNREFUSED => error.PortUnreachable,
                            .TIMEDOUT => error.ConnectionTimedOut,
                            else => posix.unexpectedErrno(err),
                        };
                        return .{ .done = .{ .net_receive = .{ mapped, received_msgs } } };
                    },
                }
            }
            return .{ .done = .{ .net_receive = .{ null, received_msgs } } };
        },
        .net_send => |o| {
            if (o.messages.len == 0) return .{ .done = .{ .net_send = .{ null, 0 } } };
            var send_flags: u32 = linux.MSG.NOSIGNAL;
            if (o.flags.confirm) send_flags |= linux.MSG.CONFIRM;
            if (o.flags.dont_route) send_flags |= linux.MSG.DONTROUTE;
            if (o.flags.eor) send_flags |= linux.MSG.EOR;
            if (o.flags.oob) send_flags |= linux.MSG.OOB;
            if (o.flags.fastopen) send_flags |= linux.MSG.FASTOPEN;

            // Single-message fast path: direct `sendmsg` without `mmsghdr` array setup.
            if (o.messages.len == 1) {
                const msg_item = &o.messages[0];
                var dest: Threaded.PosixAddress = undefined;
                const dest_len = Threaded.addressToPosix(msg_item.address, &dest);
                var iov: [1]posix.iovec_const = .{.{ .base = msg_item.data_ptr, .len = msg_item.data_len }};
                var hdr: linux.msghdr_const = .{
                    .name = @ptrCast(&dest.any),
                    .namelen = dest_len,
                    .iov = &iov,
                    .iovlen = 1,
                    .control = if (msg_item.control.len > 0) msg_item.control.ptr else null,
                    .controllen = @intCast(msg_item.control.len),
                    .flags = 0,
                };
                while (true) {
                    const rc = linux.sendmsg(o.socket_handle, &hdr, send_flags);
                    switch (linux.errno(rc)) {
                        .SUCCESS => {
                            msg_item.data_len = rc;
                            return .{ .done = .{ .net_send = .{ null, 1 } } };
                        },
                        .INTR => continue,
                        .AGAIN => return .{ .would_block = .{ .fd = o.socket_handle, .dir = .write } },
                        else => |err| return .{ .done = .{ .net_send = .{ mapSendErrorToNetSend(err), 0 } } },
                    }
                }
            }

            // Multi-datagram fast path: batch up to 16 datagrams per `sendmmsg(2)` syscall.
            const max_mmsg = 16;
            var sent_msgs: usize = 0;
            while (sent_msgs < o.messages.len) {
                const batch_n = @min(max_mmsg, o.messages.len - sent_msgs);
                var addrs: [max_mmsg]Threaded.PosixAddress = undefined;
                var iovs: [max_mmsg][1]posix.iovec = undefined;
                var mhdrs: [max_mmsg]linux.mmsghdr = undefined;

                for (0..batch_n) |k| {
                    const msg_item = &o.messages[sent_msgs + k];
                    const dest_len = Threaded.addressToPosix(msg_item.address, &addrs[k]);
                    iovs[k][0] = .{ .base = @constCast(msg_item.data_ptr), .len = msg_item.data_len };
                    mhdrs[k] = .{
                        .hdr = .{
                            .name = @ptrCast(&addrs[k].any),
                            .namelen = dest_len,
                            .iov = &iovs[k],
                            .iovlen = 1,
                            .control = if (msg_item.control.len > 0) @constCast(msg_item.control.ptr) else null,
                            .controllen = @intCast(msg_item.control.len),
                            .flags = 0,
                        },
                        .len = 0,
                    };
                }

                while (true) {
                    const rc = linux.sendmmsg(o.socket_handle, &mhdrs, @intCast(batch_n), send_flags);
                    switch (linux.errno(rc)) {
                        .SUCCESS => {
                            for (0..rc) |k| {
                                o.messages[sent_msgs + k].data_len = mhdrs[k].len;
                            }
                            sent_msgs += rc;
                            if (rc < batch_n) {
                                return .{ .done = .{ .net_send = .{ null, sent_msgs } } };
                            }
                            break;
                        },
                        .INTR => continue,
                        .AGAIN => {
                            if (sent_msgs > 0) return .{ .done = .{ .net_send = .{ null, sent_msgs } } };
                            return .{ .would_block = .{ .fd = o.socket_handle, .dir = .write } };
                        },
                        else => |err| return .{ .done = .{ .net_send = .{ mapSendErrorToNetSend(err), sent_msgs } } },
                    }
                }
            }
            return .{ .done = .{ .net_send = .{ null, sent_msgs } } };
        },
    }
}

fn cascadeAfterSuccess(self: *LinuxIo, op: Io.Operation) void {
    switch (op) {
        .net_read => |o| self.poller.cascadeNext(o.socket_handle, .read),
        .net_receive => |o| self.poller.cascadeNext(o.socket_handle, .read),
        .net_write => |o| self.poller.cascadeNext(o.socket_handle, .write),
        .net_send => |o| self.poller.cascadeNext(o.socket_handle, .write),
        .file_read_streaming => |o| self.poller.cascadeNext(o.file.handle, .read),
        .file_write_streaming => |o| self.poller.cascadeNext(o.file.handle, .write),
        .device_io_control => {},
    }
}

fn blockingOperateOnThreaded(t: *Threaded, op: Io.Operation) Io.Operation.Result {
    const t_io = t.io();
    return t_io.vtable.operate(t_io.userdata, op) catch |err| switch (err) {
        error.Canceled => unreachable,
    };
}

pub fn operate(self: *LinuxIo, shard: anytype, operation: Io.Operation) Io.Cancelable!Io.Operation.Result {
    try shard.checkCancelPoint();

    while (true) {
        switch (tryExecuteNonblocking(operation)) {
            .done => |res| {
                self.cascadeAfterSuccess(operation);
                return res;
            },
            .would_block => |wb| {
                self.waitFd(shard, wb.fd, wb.dir, null) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.Timeout => unreachable,
                    else => return try shard.callBlocking(true, struct {
                        fn run(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
                            return blockingOperateOnThreaded(t, op_arg);
                        }
                    }.run, .{operation}),
                };
            },
            .needs_pool => {
                return try shard.callBlocking(true, struct {
                    fn run(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
                        return blockingOperateOnThreaded(t, op_arg);
                    }
                }.run, .{operation});
            },
        }
    }
}

// ── Zero-Allocation Inline `Io.Batch` + Bounded Overflow Pool ───────────────

fn acquireOverflowOp(self: *LinuxIo) Allocator.Error!*OverflowOp {
    if (self.overflow_free) |op| {
        self.overflow_free = op.free_next;
        self.cached_overflow_count -= 1;
        op.* = .{};
        return op;
    }
    const op = try self.gpa.create(OverflowOp);
    op.* = .{};
    return op;
}

fn releaseOverflowOp(self: *LinuxIo, op: *OverflowOp) void {
    if (self.cached_overflow_count < max_cached_overflow_ops) {
        op.* = .{ .free_next = self.overflow_free };
        self.overflow_free = op;
        self.cached_overflow_count += 1;
    } else {
        self.gpa.destroy(op);
    }
}

fn batchAppendCompleted(batch: *Io.Batch, idx: u32, res: Io.Operation.Result) void {
    const oi = Io.Operation.OptionalIndex.fromIndex(idx);
    switch (batch.completed.tail) {
        .none => batch.completed.head = oi,
        else => |tail_oi| batch.storage[tail_oi.toIndex()].completion.node.next = oi,
    }
    batch.storage[idx] = .{ .completion = .{ .node = .{ .next = .none }, .result = res } };
    batch.completed.tail = oi;
}

fn batchAppendPending(batch: *Io.Batch, idx: u32, tag: Io.Operation.Tag) *Io.Operation.Storage.Pending.Userdata {
    const cur_oi = Io.Operation.OptionalIndex.fromIndex(idx);
    const prev_tail = batch.pending.tail;
    switch (prev_tail) {
        .none => batch.pending.head = cur_oi,
        else => |t_oi| batch.storage[t_oi.toIndex()].pending.node.next = cur_oi,
    }
    batch.storage[idx] = .{ .pending = .{
        .node = .{ .prev = prev_tail, .next = .none },
        .tag = tag,
        .userdata = @splat(0),
    } };
    batch.pending.tail = cur_oi;
    return &batch.storage[idx].pending.userdata;
}

fn batchRemovePending(batch: *Io.Batch, idx: u32) void {
    const node = batch.storage[idx].pending.node;
    switch (node.prev) {
        .none => batch.pending.head = node.next,
        else => |prev_oi| batch.storage[prev_oi.toIndex()].pending.node.next = node.next,
    }
    switch (node.next) {
        .none => batch.pending.tail = node.prev,
        else => |next_oi| batch.storage[next_oi.toIndex()].pending.node.prev = node.prev,
    }
}

fn batchReturnUnused(batch: *Io.Batch, idx: u32) void {
    const oi = Io.Operation.OptionalIndex.fromIndex(idx);
    const tail = batch.unused.tail;
    switch (tail) {
        .none => batch.unused.head = oi,
        else => |tail_oi| batch.storage[tail_oi.toIndex()].unused.next = oi,
    }
    batch.storage[idx] = .{ .unused = .{ .prev = tail, .next = .none } };
    batch.unused.tail = oi;
}

inline fn wakeBatchAwaiter(self: *LinuxIo, batch: *Io.Batch) void {
    if (batch.userdata) |raw_task| {
        self.wake_task_fn(self.owner_ctx, raw_task);
    }
}

fn packSendFlags(flags: net.SendFlags) u32 {
    var v: u32 = 0;
    if (flags.confirm) v |= 1 << 0;
    if (flags.dont_route) v |= 1 << 1;
    if (flags.eor) v |= 1 << 2;
    if (flags.oob) v |= 1 << 3;
    if (flags.fastopen) v |= 1 << 4;
    return v;
}

fn unpackSendFlags(v: u32) net.SendFlags {
    return .{
        .confirm = (v & (1 << 0)) != 0,
        .dont_route = (v & (1 << 1)) != 0,
        .eor = (v & (1 << 2)) != 0,
        .oob = (v & (1 << 3)) != 0,
        .fastopen = (v & (1 << 4)) != 0,
    };
}

fn packRecvFlagsLow3(flags: net.ReceiveFlags) usize {
    var v: usize = 0;
    if (flags.oob) v |= 1 << 0;
    if (flags.peek) v |= 1 << 1;
    if (flags.trunc) v |= 1 << 2;
    return v;
}

fn unpackRecvFlagsLow3(v: usize) net.ReceiveFlags {
    return .{
        .oob = (v & (1 << 0)) != 0,
        .peek = (v & (1 << 1)) != 0,
        .trunc = (v & (1 << 2)) != 0,
    };
}

fn tryPackInlineBatchOp(batch: *Io.Batch, op: Io.Operation) ?InlineBatchSlot {
    if (@sizeOf(usize) != 8) return null;
    const batch_addr = @intFromPtr(batch);
    assert((batch_addr & 0b111) == 0);

    return switch (op) {
        .file_read_streaming => |o| .{
            .waiter = .{ .wake_fn = onInlineBatchEpollReady },
            .batch_packed = batch_addr,
            .payload = .{ .slice_op = .{
                .ptr = @intFromPtr(o.data.ptr),
                .len = o.data.len,
                .fd = o.file.handle,
                .aux = @intFromBool(o.file.flags.nonblocking),
            } },
        },
        .file_write_streaming => |o| if (o.header.len == 0 and o.splat <= std.math.maxInt(u32)) .{
            .waiter = .{ .wake_fn = onInlineBatchEpollReady },
            .batch_packed = batch_addr,
            .payload = .{ .slice_op = .{
                .ptr = @intFromPtr(o.data.ptr),
                .len = o.data.len,
                .fd = o.file.handle,
                .aux = @intCast(o.splat),
            } },
        } else null,
        .net_read => |o| if (o.control.len == 0) .{
            .waiter = .{ .wake_fn = onInlineBatchEpollReady },
            .batch_packed = batch_addr,
            .payload = .{ .slice_op = .{
                .ptr = @intFromPtr(o.data.ptr),
                .len = o.data.len,
                .fd = o.socket_handle,
                .aux = 0,
            } },
        } else null,
        .net_write => |o| if (o.header.len == 0 and o.control.len == 0 and o.splat <= std.math.maxInt(u32)) .{
            .waiter = .{ .wake_fn = onInlineBatchEpollReady },
            .batch_packed = batch_addr,
            .payload = .{ .slice_op = .{
                .ptr = @intFromPtr(o.data.ptr),
                .len = o.data.len,
                .fd = o.socket_handle,
                .aux = @intCast(o.splat),
            } },
        } else null,
        .net_receive => |o| if (o.message_buffer.len == 1 and o.data_buffer.len <= std.math.maxInt(u32)) .{
            .waiter = .{ .wake_fn = onInlineBatchEpollReady },
            .batch_packed = batch_addr | packRecvFlagsLow3(o.flags),
            .payload = .{ .recv_op = .{
                .msg_ptr = &o.message_buffer[0],
                .data_ptr = o.data_buffer.ptr,
                .fd = o.socket_handle,
                .data_len = @intCast(o.data_buffer.len),
            } },
        } else null,
        .net_send => |o| .{
            .waiter = .{ .wake_fn = onInlineBatchEpollReady },
            .batch_packed = batch_addr,
            .payload = .{ .slice_op = .{
                .ptr = @intFromPtr(o.messages.ptr),
                .len = o.messages.len,
                .fd = o.socket_handle,
                .aux = packSendFlags(o.flags),
            } },
        },
        .device_io_control => null,
    };
}

fn unpackInlineOperation(tag: Io.Operation.Tag, slot: InlineBatchSlot) Io.Operation {
    return switch (tag) {
        .file_read_streaming => .{ .file_read_streaming = .{
            .file = .{ .handle = slot.payload.slice_op.fd, .flags = .{ .nonblocking = slot.payload.slice_op.aux != 0 } },
            .data = @as([*][]u8, @ptrFromInt(slot.payload.slice_op.ptr))[0..slot.payload.slice_op.len],
        } },
        .file_write_streaming => .{ .file_write_streaming = .{
            .file = .{ .handle = slot.payload.slice_op.fd, .flags = .{ .nonblocking = true } },
            .header = &.{},
            .data = @as([*]const []const u8, @ptrFromInt(slot.payload.slice_op.ptr))[0..slot.payload.slice_op.len],
            .splat = slot.payload.slice_op.aux,
        } },
        .net_read => .{ .net_read = .{
            .socket_handle = slot.payload.slice_op.fd,
            .data = @as([*][]u8, @ptrFromInt(slot.payload.slice_op.ptr))[0..slot.payload.slice_op.len],
            .control = &.{},
        } },
        .net_write => .{ .net_write = .{
            .socket_handle = slot.payload.slice_op.fd,
            .header = &.{},
            .data = @as([*]const []const u8, @ptrFromInt(slot.payload.slice_op.ptr))[0..slot.payload.slice_op.len],
            .splat = slot.payload.slice_op.aux,
            .control = &.{},
        } },
        .net_receive => .{ .net_receive = .{
            .socket_handle = slot.payload.recv_op.fd,
            .message_buffer = slot.payload.recv_op.msg_ptr[0..1],
            .data_buffer = slot.payload.recv_op.data_ptr[0..slot.payload.recv_op.data_len],
            .flags = unpackRecvFlagsLow3(slot.batch_packed & 0b111),
        } },
        .net_send => .{ .net_send = .{
            .socket_handle = slot.payload.slice_op.fd,
            .messages = @as([*]net.OutgoingMessage, @ptrFromInt(slot.payload.slice_op.ptr))[0..slot.payload.slice_op.len],
            .flags = unpackSendFlags(slot.payload.slice_op.aux),
        } },
        .device_io_control => unreachable,
    };
}

fn inlineDirection(tag: Io.Operation.Tag) Epoll.Direction {
    return switch (tag) {
        .file_read_streaming, .net_read, .net_receive => .read,
        .file_write_streaming, .net_write, .net_send => .write,
        .device_io_control => unreachable,
    };
}

fn inlineFd(tag: Io.Operation.Tag, slot: *const InlineBatchSlot) linux.fd_t {
    return switch (tag) {
        .net_receive => slot.payload.recv_op.fd,
        else => slot.payload.slice_op.fd,
    };
}

fn onInlineBatchEpollReady(w: *Epoll.Waiter) void {
    const self = tls_active_driver.?;
    const slot: *InlineBatchSlot = @fieldParentPtr("waiter", w);
    const userdata_ptr: *Io.Operation.Storage.Pending.Userdata = @ptrCast(@alignCast(slot));
    const pending: *Io.Operation.Storage.Pending = @fieldParentPtr("userdata", userdata_ptr);
    const storage: *Io.Operation.Storage = @fieldParentPtr("pending", pending);

    // Snapshot inline state BEFORE mutating `batch.storage[idx]`!
    const snap = slot.*;
    const tag = pending.tag;
    const batch: *Io.Batch = @ptrFromInt(snap.batch_packed & ~@as(usize, 0b111));
    const idx: u32 = @intCast((@intFromPtr(storage) - @intFromPtr(batch.storage.ptr)) / @sizeOf(Io.Operation.Storage));
    const op = unpackInlineOperation(tag, snap);

    switch (tryExecuteNonblocking(op)) {
        .done => |res| {
            self.cascadeAfterSuccess(op);
            batchRemovePending(batch, idx);
            batchAppendCompleted(batch, idx, res);
            self.wakeBatchAwaiter(batch);
        },
        .would_block => |wb| {
            // Spurious edge: re-arm inline waiter in place.
            self.poller.waitIo(self.gpa, wb.fd, wb.dir, &slot.waiter) catch {
                batchRemovePending(batch, idx);
                batchReturnUnused(batch, idx);
                self.wakeBatchAwaiter(batch);
            };
        },
        .needs_pool => unreachable,
    }
}

fn finishOverflowOp(bop: *OverflowOp) void {
    const self = bop.driver;
    const batch = bop.batch;
    const idx = bop.slot_index;
    const maybe_res = bop.result;
    const was_canceled = bop.canceled;

    batchRemovePending(batch, idx);
    if (!was_canceled and maybe_res != null) {
        batchAppendCompleted(batch, idx, maybe_res.?);
    } else {
        batchReturnUnused(batch, idx);
    }
    self.releaseOverflowOp(bop);
    self.wakeBatchAwaiter(batch);
}

fn onOverflowEpollReady(w: *Epoll.Waiter) void {
    const ep_ptr: *@FieldType(@TypeOf(@as(OverflowOp, undefined).backend), "epoll") = @fieldParentPtr("waiter", w);
    // Recover `*OverflowOp` from `backend.epoll` via active union parent pointer:
    const backend_ptr: *@TypeOf(@as(OverflowOp, undefined).backend) = @alignCast(@fieldParentPtr("epoll", ep_ptr));
    const bop: *OverflowOp = @fieldParentPtr("backend", backend_ptr);

    if (bop.canceled) {
        finishOverflowOp(bop);
        return;
    }
    switch (tryExecuteNonblocking(bop.op)) {
        .done => |res| {
            bop.driver.cascadeAfterSuccess(bop.op);
            bop.result = res;
            finishOverflowOp(bop);
        },
        .would_block => |wb| {
            bop.driver.poller.waitIo(bop.driver.gpa, wb.fd, wb.dir, &bop.backend.epoll.waiter) catch {
                bop.canceled = true;
                finishOverflowOp(bop);
            };
        },
        .needs_pool => unreachable,
    }
}

fn onOverflowBlockingExec(job: *BlockingPool.Job, threaded: *Threaded) void {
    const bop: *OverflowOp = @ptrCast(@alignCast(job.notify_ctx.?));
    bop.result = blockingOperateOnThreaded(threaded, bop.op);
}

fn onOverflowBlockingNotify(ctx: ?*anyopaque) void {
    const bop: *OverflowOp = @ptrCast(@alignCast(ctx.?));
    bop.driver.notifyFromAnyThread();
}

fn onOverflowBlockingResume(node: *ResumeLane.Node) void {
    const bop: *OverflowOp = @ptrCast(@alignCast(node.ctx.?));
    finishOverflowOp(bop);
}

fn submitOverflowBlocking(self: *LinuxIo, bop: *OverflowOp) Io.ConcurrentError!void {
    bop.backend = .{ .blocking = .{
        .run_fn = onOverflowBlockingExec,
        .resume_lane = self.resume_lane,
        .resume_node = .{ .ctx = bop, .callback = onOverflowBlockingResume },
        .notify_ctx = bop,
        .notify_fn = onOverflowBlockingNotify,
    } };
    switch (self.blocking_pool.submit(&bop.backend.blocking)) {
        .accepted => {},
        .queue_full, .closed => return error.ConcurrencyUnavailable,
    }
}

pub fn submitBatch(self: *LinuxIo, _: anytype, batch: *Io.Batch) Io.ConcurrentError!void {
    var cur_oi = batch.submitted.head;
    if (cur_oi == .none) return;

    const prev_tls = tls_active_driver;
    tls_active_driver = self;
    defer tls_active_driver = prev_tls;

    errdefer {
        batch.submitted.head = cur_oi;
        if (cur_oi == .none) batch.submitted.tail = .none;
    }

    while (cur_oi != .none) {
        const idx = cur_oi.toIndex();
        const saved_sub = batch.storage[idx].submission;
        const op = saved_sub.operation;
        const next_oi = saved_sub.node.next;

        switch (tryExecuteNonblocking(op)) {
            .done => |res| {
                self.cascadeAfterSuccess(op);
                batchAppendCompleted(batch, idx, res);
                cur_oi = next_oi;
                continue;
            },
            .would_block => |wb| {
                // Zero-allocation fast path: store directly inside `batch.storage[idx].pending.userdata`!
                if (tryPackInlineBatchOp(batch, op)) |packed_inline| {
                    const ud = batchAppendPending(batch, idx, op);
                    const slot: *InlineBatchSlot = @ptrCast(@alignCast(ud));
                    slot.* = packed_inline;
                    self.poller.waitIo(self.gpa, wb.fd, wb.dir, &slot.waiter) catch |err| switch (err) {
                        error.PollUnsupported => {
                            batchRemovePending(batch, idx);
                            batch.storage[idx] = .{ .submission = saved_sub };
                            try self.submitBatchPoolFallback(batch, idx, saved_sub);
                        },
                        else => {
                            batchRemovePending(batch, idx);
                            batch.storage[idx] = .{ .submission = saved_sub };
                            return error.ConcurrencyUnavailable;
                        },
                    };
                    cur_oi = next_oi;
                    continue;
                }

                // Overflow path for exotic multi-header / ancillary-control operations.
                const bop = self.acquireOverflowOp() catch return error.ConcurrencyUnavailable;
                bop.* = .{
                    .driver = self,
                    .batch = batch,
                    .slot_index = idx,
                    .op = op,
                    .backend = .{ .epoll = .{
                        .fd = wb.fd,
                        .dir = wb.dir,
                        .waiter = .{ .wake_fn = onOverflowEpollReady },
                    } },
                };
                const ud = batchAppendPending(batch, idx, op);
                ud[0] = @intFromPtr(bop);
                self.poller.waitIo(self.gpa, wb.fd, wb.dir, &bop.backend.epoll.waiter) catch |err| switch (err) {
                    error.PollUnsupported => {
                        self.submitOverflowBlocking(bop) catch {
                            batchRemovePending(batch, idx);
                            batch.storage[idx] = .{ .submission = saved_sub };
                            self.releaseOverflowOp(bop);
                            return error.ConcurrencyUnavailable;
                        };
                    },
                    else => {
                        batchRemovePending(batch, idx);
                        batch.storage[idx] = .{ .submission = saved_sub };
                        self.releaseOverflowOp(bop);
                        return error.ConcurrencyUnavailable;
                    },
                };
                cur_oi = next_oi;
            },
            .needs_pool => {
                try self.submitBatchPoolFallback(batch, idx, saved_sub);
                cur_oi = next_oi;
            },
        }
    }
    batch.submitted = .empty;
}

fn submitBatchPoolFallback(
    self: *LinuxIo,
    batch: *Io.Batch,
    idx: u32,
    saved_sub: Io.Operation.Storage.Submission,
) Io.ConcurrentError!void {
    const bop = self.acquireOverflowOp() catch return error.ConcurrencyUnavailable;
    bop.* = .{
        .driver = self,
        .batch = batch,
        .slot_index = idx,
        .op = saved_sub.operation,
    };
    const ud = batchAppendPending(batch, idx, saved_sub.operation);
    ud[0] = @intFromPtr(bop);
    self.submitOverflowBlocking(bop) catch {
        batchRemovePending(batch, idx);
        batch.storage[idx] = .{ .submission = saved_sub };
        self.releaseOverflowOp(bop);
        return error.ConcurrencyUnavailable;
    };
}

pub fn cancelBatchPending(self: *LinuxIo, batch: *Io.Batch) void {
    var cur_oi = batch.pending.head;
    while (cur_oi != .none) {
        const idx = cur_oi.toIndex();
        const pending = &batch.storage[idx].pending;
        const next_oi = pending.node.next;

        // `userdata[2]` holds `InlineBatchSlot.waiter.wake_fn` when inline (`!= 0`) vs `0` for `OverflowOp`.
        if (@sizeOf(usize) == 8 and pending.userdata[2] != 0) {
            const slot: *InlineBatchSlot = @ptrCast(@alignCast(&pending.userdata));
            const tag = pending.tag;
            self.poller.cancelWaiter(inlineFd(tag, slot), inlineDirection(tag), &slot.waiter);
            batchRemovePending(batch, idx);
            batchReturnUnused(batch, idx);
        } else {
            const bop: *OverflowOp = @ptrFromInt(pending.userdata[0]);
            bop.canceled = true;
            switch (bop.backend) {
                .none => {},
                .epoll => |*ep| {
                    self.poller.cancelWaiter(ep.fd, ep.dir, &ep.waiter);
                    finishOverflowOp(bop);
                },
                .blocking => |*job| {
                    if (self.blocking_pool.tryCancel(job) == .canceled_queued) {
                        finishOverflowOp(bop);
                    }
                },
            }
        }
        cur_oi = next_oi;
    }
}

// ── Networking (`net*` — Native Linux `epoll` + `SO_REUSEPORT`) ──────────────

fn setSockOpt(fd: linux.fd_t, level: i32, optname: u32, val: []const u8) !void {
    switch (linux.errno(linux.setsockopt(fd, level, optname, val.ptr, @intCast(val.len)))) {
        .SUCCESS => {},
        .NOPROTOOPT, .INVAL => return error.OptionUnsupported,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

fn mapConnectErr(err: linux.E) net.IpAddress.ConnectError {
    return switch (err) {
        .ADDRNOTAVAIL => error.AddressUnavailable,
        .AFNOSUPPORT => error.AddressFamilyUnsupported,
        .ALREADY => error.ConnectionPending,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionResetByPeer,
        .HOSTUNREACH => error.HostUnreachable,
        .NETUNREACH => error.NetworkUnreachable,
        .TIMEDOUT => error.Timeout,
        .ACCES, .PERM => error.AccessDenied,
        .NETDOWN => error.NetworkDown,
        .NOMEM, .NOBUFS => error.SystemResources,
        else => posix.unexpectedErrno(err),
    };
}

fn fillUnixSockaddr(ua: *const net.UnixAddress, sun: *posix.sockaddr.un) posix.socklen_t {
    sun.* = .{ .family = posix.AF.UNIX, .path = @splat(0) };
    const dst: []u8 = @ptrCast(sun.path[0..ua.path.len]);
    @memcpy(dst, ua.path);
    const offset: posix.socklen_t = @offsetOf(posix.sockaddr.un, "path");
    if (ua.isAbstract()) {
        return offset + @as(posix.socklen_t, @intCast(ua.path.len));
    }
    return offset + @as(posix.socklen_t, @intCast(ua.path.len)) + 1;
}

pub fn netListenIp(
    _: *LinuxIo,
    _: anytype,
    address: *const net.IpAddress,
    options: net.IpAddress.ListenOptions,
) net.IpAddress.ListenError!net.Socket {
    const family = Threaded.posixAddressFamily(address);
    const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
    const sock_rc = linux.socket(family, mode | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, protocol);
    const fd: linux.fd_t = switch (linux.errno(sock_rc)) {
        .SUCCESS => @intCast(sock_rc),
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
        .PROTOTYPE => return error.SocketModeUnsupported,
        .INVAL => return error.ProtocolUnsupportedBySystem,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM, .NOBUFS => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        else => |err| return posix.unexpectedErrno(err),
    };
    errdefer Threaded.closeFd(fd);

    if (options.reuse_address) {
        const one: c_int = 1;
        try setSockOpt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, std.mem.asBytes(&one));
        try setSockOpt(fd, linux.SOL.SOCKET, linux.SO.REUSEPORT, std.mem.asBytes(&one));
    }

    var storage: Threaded.PosixAddress = undefined;
    const addr_len = Threaded.addressToPosix(address, &storage);
    switch (linux.errno(linux.bind(fd, &storage.any, addr_len))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        .ADDRNOTAVAIL => return error.AddressUnavailable,
        .ACCES => return error.AccessDenied,
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }

    switch (linux.errno(linux.listen(fd, options.kernel_backlog))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        .OPNOTSUPP => return error.SocketModeUnsupported,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }

    var bound: Threaded.PosixAddress = undefined;
    var bound_len: posix.socklen_t = @sizeOf(Threaded.PosixAddress);
    switch (linux.errno(linux.getsockname(fd, &bound.any, &bound_len))) {
        .SUCCESS => {},
        else => |err| return posix.unexpectedErrno(err),
    }
    return .{
        .handle = fd,
        .address = Threaded.addressFromPosix(&bound),
    };
}

pub fn netAccept(
    self: *LinuxIo,
    shard: anytype,
    server: net.Socket.Handle,
    _: net.Server.AcceptOptions,
) net.Server.AcceptError!net.Socket {
    while (true) {
        var peer: Threaded.PosixAddress = undefined;
        var peer_len: posix.socklen_t = @sizeOf(Threaded.PosixAddress);
        const rc = linux.accept4(server, &peer.any, &peer_len, linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC);
        switch (linux.errno(rc)) {
            .SUCCESS => {
                self.poller.cascadeNext(server, .read);
                const addr = if (peer_len > 0 and (peer.any.family == posix.AF.INET or peer.any.family == posix.AF.INET6))
                    Threaded.addressFromPosix(&peer)
                else
                    net.IpAddress{ .ip4 = .loopback(0) };
                return .{
                    .handle = @intCast(rc),
                    .address = addr,
                };
            },
            .INTR => continue,
            .AGAIN => {
                self.waitFd(shard, server, .read, null) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.OutOfMemory, error.SystemResources => return error.SystemResources,
                    error.ProcessFdQuotaExceeded => return error.ProcessFdQuotaExceeded,
                    error.Timeout, error.PollUnsupported => unreachable,
                    else => return error.Unexpected,
                };
            },
            .CONNABORTED => {
                self.poller.cascadeNext(server, .read);
                return error.ConnectionAborted;
            },
            .MFILE => {
                self.poller.cascadeNext(server, .read);
                return error.ProcessFdQuotaExceeded;
            },
            .NFILE => {
                self.poller.cascadeNext(server, .read);
                return error.SystemFdQuotaExceeded;
            },
            .NOMEM, .NOBUFS => {
                self.poller.cascadeNext(server, .read);
                return error.SystemResources;
            },
            .INVAL => return error.SocketNotListening,
            .PERM => {
                self.poller.cascadeNext(server, .read);
                return error.BlockedByFirewall;
            },
            .PROTO => {
                self.poller.cascadeNext(server, .read);
                return error.ProtocolFailure;
            },
            .NETDOWN => return error.NetworkDown,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn netBindIp(
    _: *LinuxIo,
    _: anytype,
    address: *const net.IpAddress,
    options: net.IpAddress.BindOptions,
) net.IpAddress.BindError!net.Socket {
    const family = Threaded.posixAddressFamily(address);
    const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
    const sock_rc = linux.socket(family, mode | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, protocol);
    const fd: linux.fd_t = switch (linux.errno(sock_rc)) {
        .SUCCESS => @intCast(sock_rc),
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
        .PROTOTYPE => return error.SocketModeUnsupported,
        .INVAL => return error.ProtocolUnsupportedBySystem,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM, .NOBUFS => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        else => |err| return posix.unexpectedErrno(err),
    };
    errdefer Threaded.closeFd(fd);

    if (options.ip6_only) |v6only| {
        const val: c_int = @intFromBool(v6only);
        setSockOpt(fd, linux.IPPROTO.IPV6, linux.IPV6.V6ONLY, std.mem.asBytes(&val)) catch
            return error.OptionUnsupported;
    }
    if (options.allow_broadcast) {
        const val: c_int = 1;
        setSockOpt(fd, linux.SOL.SOCKET, linux.SO.BROADCAST, std.mem.asBytes(&val)) catch
            return error.OptionUnsupported;
    }

    var storage: Threaded.PosixAddress = undefined;
    const addr_len = Threaded.addressToPosix(address, &storage);
    switch (linux.errno(linux.bind(fd, &storage.any, addr_len))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        .ADDRNOTAVAIL => return error.AddressUnavailable,
        .ACCES => return error.AccessDenied,
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }

    var bound: Threaded.PosixAddress = undefined;
    var bound_len: posix.socklen_t = @sizeOf(Threaded.PosixAddress);
    switch (linux.errno(linux.getsockname(fd, &bound.any, &bound_len))) {
        .SUCCESS => {},
        else => |err| return posix.unexpectedErrno(err),
    }
    return .{
        .handle = fd,
        .address = Threaded.addressFromPosix(&bound),
    };
}

pub fn netConnectIp(
    self: *LinuxIo,
    shard: anytype,
    address: *const net.IpAddress,
    options: net.IpAddress.ConnectOptions,
    maybe_dl: ?i96,
) net.IpAddress.ConnectError!net.Socket {
    const family = Threaded.posixAddressFamily(address);
    const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
    const sock_rc = linux.socket(family, mode | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, protocol);
    const fd: linux.fd_t = switch (linux.errno(sock_rc)) {
        .SUCCESS => @intCast(sock_rc),
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
        .PROTOTYPE => return error.SocketModeUnsupported,
        .INVAL => return error.ProtocolUnsupportedBySystem,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM, .NOBUFS => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        else => |err| return posix.unexpectedErrno(err),
    };
    errdefer {
        self.poller.unregisterFd(fd);
        Threaded.closeFd(fd);
    }

    var storage: Threaded.PosixAddress = undefined;
    const addr_len = Threaded.addressToPosix(address, &storage);
    while (true) {
        const rc = linux.connect(fd, &storage.any, addr_len);
        switch (linux.errno(rc)) {
            .SUCCESS => break,
            .INTR => continue,
            .INPROGRESS, .AGAIN => {
                self.waitFd(shard, fd, .write, maybe_dl) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.Timeout => return error.Timeout,
                    error.OutOfMemory, error.SystemResources => return error.SystemResources,
                    error.ProcessFdQuotaExceeded => return error.ProcessFdQuotaExceeded,
                    error.PollUnsupported => unreachable,
                    else => return error.Unexpected,
                };
                var so_err: c_int = 0;
                var opt_len: posix.socklen_t = @sizeOf(c_int);
                const grc = linux.getsockopt(fd, linux.SOL.SOCKET, linux.SO.ERROR, @ptrCast(&so_err), &opt_len);
                if (linux.errno(grc) != .SUCCESS) return error.Unexpected;
                if (so_err != 0) return mapConnectErr(@fromBackingInt(@intCast(so_err)));
                break;
            },
            else => |err| return mapConnectErr(err),
        }
    }

    var local: Threaded.PosixAddress = undefined;
    var local_len: posix.socklen_t = @sizeOf(Threaded.PosixAddress);
    _ = linux.getsockname(fd, &local.any, &local_len);
    return .{
        .handle = fd,
        .address = Threaded.addressFromPosix(&local),
    };
}

pub fn netListenUnix(
    _: *LinuxIo,
    _: anytype,
    ua: *const net.UnixAddress,
    options: net.UnixAddress.ListenOptions,
) net.UnixAddress.ListenError!net.Socket.Handle {
    const rc = linux.socket(linux.AF.UNIX, linux.SOCK.STREAM | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, 0);
    const fd: linux.fd_t = switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM, .NOBUFS => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        else => |err| return posix.unexpectedErrno(err),
    };
    errdefer Threaded.closeFd(fd);

    var sun: posix.sockaddr.un = undefined;
    const addr_len = fillUnixSockaddr(ua, &sun);
    switch (linux.errno(linux.bind(fd, @ptrCast(&sun), addr_len))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        .ADDRNOTAVAIL => return error.AddressUnavailable,
        .ACCES => return error.AccessDenied,
        .PERM => return error.PermissionDenied,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .ROFS => return error.ReadOnlyFileSystem,
        .LOOP => return error.SymLinkLoop,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
    switch (linux.errno(linux.listen(fd, options.kernel_backlog))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        else => |err| return posix.unexpectedErrno(err),
    }
    return fd;
}

fn mapUnixConnectErr(err: linux.E) net.UnixAddress.ConnectError {
    return switch (err) {
        .CONNREFUSED => error.ConnectionRefused,
        .NOENT => error.FileNotFound,
        .NOTDIR => error.NotDir,
        .ACCES => error.AccessDenied,
        .PERM => error.PermissionDenied,
        .LOOP => error.SymLinkLoop,
        .ROFS => error.ReadOnlyFileSystem,
        .AGAIN => error.WouldBlock,
        .NETDOWN => error.NetworkDown,
        .AFNOSUPPORT => error.AddressFamilyUnsupported,
        .PROTOTYPE => error.SocketModeUnsupported,
        .MFILE => error.ProcessFdQuotaExceeded,
        .NFILE => error.SystemFdQuotaExceeded,
        .NOMEM, .NOBUFS => error.SystemResources,
        else => posix.unexpectedErrno(err),
    };
}

pub fn netConnectUnix(
    self: *LinuxIo,
    shard: anytype,
    ua: *const net.UnixAddress,
) net.UnixAddress.ConnectError!net.Socket.Handle {
    const rc = linux.socket(linux.AF.UNIX, linux.SOCK.STREAM | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, 0);
    const fd: linux.fd_t = switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        else => |err| return mapUnixConnectErr(err),
    };
    errdefer {
        self.poller.unregisterFd(fd);
        Threaded.closeFd(fd);
    }

    var sun: posix.sockaddr.un = undefined;
    const addr_len = fillUnixSockaddr(ua, &sun);
    while (true) {
        const crc = linux.connect(fd, @ptrCast(&sun), addr_len);
        switch (linux.errno(crc)) {
            .SUCCESS => return fd,
            .INTR => continue,
            .AGAIN, .INPROGRESS => {
                self.waitFd(shard, fd, .write, null) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.OutOfMemory, error.SystemResources => return error.SystemResources,
                    error.ProcessFdQuotaExceeded => return error.ProcessFdQuotaExceeded,
                    else => return error.Unexpected,
                };
                var so_err: c_int = 0;
                var opt_len: posix.socklen_t = @sizeOf(c_int);
                const grc = linux.getsockopt(fd, linux.SOL.SOCKET, linux.SO.ERROR, @ptrCast(&so_err), &opt_len);
                if (linux.errno(grc) != .SUCCESS) return error.Unexpected;
                if (so_err == 0) return fd;
                return mapUnixConnectErr(@fromBackingInt(@intCast(so_err)));
            },
            else => |err| return mapUnixConnectErr(err),
        }
    }
}

pub fn netSocketCreatePair(
    _: *LinuxIo,
    _: anytype,
    options: net.Socket.CreatePairOptions,
) net.Socket.CreatePairError![2]net.Socket {
    const mode, const protocol = Threaded.posixSocketModeProtocol(linux.AF.UNIX, options.mode, options.protocol) catch {
        return error.SocketModeUnsupported;
    };
    var fds: [2]linux.fd_t = undefined;
    const rc = linux.socketpair(linux.AF.UNIX, mode | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, protocol, &fds);
    switch (linux.errno(rc)) {
        .SUCCESS => return .{
            .{ .handle = fds[0], .address = .{ .ip4 = .loopback(0) } },
            .{ .handle = fds[1], .address = .{ .ip4 = .loopback(0) } },
        },
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .OPNOTSUPP => return error.OperationUnsupported,
        .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
        .PROTOTYPE => return error.SocketModeUnsupported,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM, .NOBUFS => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn netWriteFile(
    self: *LinuxIo,
    shard: anytype,
    socket_handle: net.Socket.Handle,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
) net.Stream.Writer.WriteFileError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    const reader_buffered = file_reader.interface.buffered();

    if (reader_buffered.len >= @backingInt(limit)) {
        const op_res = try self.operate(shard, .{ .net_write = .{
            .socket_handle = socket_handle,
            .header = header,
            .data = &.{limit.slice(reader_buffered)},
            .splat = 1,
        } });
        const n = try op_res.net_write;
        file_reader.interface.toss(n -| header.len);
        return n;
    }

    if (file_reader.size) |size| {
        if (size - file_reader.pos == 0) {
            if (header.len != 0 or reader_buffered.len != 0) {
                const op_res = try self.operate(shard, .{ .net_write = .{
                    .socket_handle = socket_handle,
                    .header = header,
                    .data = &.{limit.slice(reader_buffered)},
                    .splat = 1,
                } });
                const n = try op_res.net_write;
                file_reader.interface.toss(n -| header.len);
                return n;
            }
            return error.EndOfStream;
        }
    }

    if (header.len != 0 or reader_buffered.len != 0) {
        const op_res = try self.operate(shard, .{ .net_write = .{
            .socket_handle = socket_handle,
            .header = header,
            .data = &.{limit.slice(reader_buffered)},
            .splat = 1,
        } });
        const n = try op_res.net_write;
        file_reader.interface.toss(n -| header.len);
        return n;
    }

    const max_count: usize = 0x7ffff000;
    var off: linux.off_t = 0;
    const use_offset: bool, const count: usize = switch (file_reader.mode) {
        .positional => blk: {
            const size = file_reader.getSize() catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return error.Unimplemented,
            };
            if (size <= file_reader.pos) {
                file_reader.size = file_reader.pos;
                return error.EndOfStream;
            }
            off = std.math.cast(linux.off_t, file_reader.pos) orelse return error.ReadFailed;
            break :blk .{ true, @min(@backingInt(limit), size - file_reader.pos, max_count) };
        },
        .streaming => .{ false, @min(@backingInt(limit), max_count) },
        .positional_simple, .streaming_simple => return error.Unimplemented,
        .failure => return error.ReadFailed,
    };
    if (count == 0) return 0;

    while (true) {
        const rc = linux.sendfile(socket_handle, file_reader.file.handle, if (use_offset) &off else null, count);
        switch (linux.errno(rc)) {
            .SUCCESS => {
                self.poller.cascadeNext(socket_handle, .write);
                if (rc == 0) {
                    file_reader.size = file_reader.pos;
                    return error.EndOfStream;
                }
                file_reader.pos += rc;
                return rc;
            },
            .INTR => continue,
            .AGAIN => {
                self.waitFd(shard, socket_handle, .write, null) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.OutOfMemory, error.SystemResources => return error.SystemResources,
                    else => return error.Unexpected,
                };
            },
            .INVAL, .NOSYS, .OPNOTSUPP, .SPIPE, .NXIO => return error.Unimplemented,
            .PIPE, .NOTCONN => return error.SocketUnconnected,
            .CONNRESET => return error.ConnectionResetByPeer,
            .TIMEDOUT => return error.ConnectionTimedOut,
            .NETDOWN => return error.NetworkDown,
            .IO => return error.ReadFailed,
            .NOMEM, .NOBUFS => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn netClose(self: *LinuxIo, sockets: []const net.Socket) void {
    for (sockets) |s| {
        self.poller.unregisterFd(s.handle);
        Threaded.closeFd(s.handle);
    }
}

pub fn netShutdown(_: *LinuxIo, handle: net.Socket.Handle, how: net.ShutdownHow) net.ShutdownError!void {
    const lhow: u32 = switch (how) {
        .recv => linux.SHUT.RD,
        .send => linux.SHUT.WR,
        .both => linux.SHUT.RDWR,
    };
    switch (linux.errno(linux.shutdown(handle, @intCast(lhow)))) {
        .SUCCESS => {},
        .NOTCONN => return error.SocketUnconnected,
        .CONNRESET => return error.ConnectionResetByPeer,
        .CONNABORTED => return error.ConnectionAborted,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn netInterfaceNameResolve(
    _: *LinuxIo,
    _: anytype,
    name: *const net.Interface.Name,
) net.Interface.Name.ResolveError!net.Interface {
    const sock_rc = linux.socket(linux.AF.INET, linux.SOCK.DGRAM | linux.SOCK.CLOEXEC, 0);
    if (linux.errno(sock_rc) != .SUCCESS) return error.SystemResources;
    const fd: linux.fd_t = @intCast(sock_rc);
    defer Threaded.closeFd(fd);

    var ifr: linux.ifreq = std.mem.zeroes(linux.ifreq);
    const slice = name.toSlice();
    @memcpy(ifr.ifrn.name[0..slice.len], slice);
    switch (linux.errno(linux.ioctl(fd, linux.SIOCGIFINDEX, @intFromPtr(&ifr)))) {
        .SUCCESS => return .{ .index = @intCast(ifr.ifru.ivalue) },
        .NODEV, .NXIO => return error.InterfaceNotFound,
        .ACCES, .PERM => return error.AccessDenied,
        .NOMEM, .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn netInterfaceName(
    _: *LinuxIo,
    _: anytype,
    interface: net.Interface,
) net.Interface.NameError!net.Interface.Name {
    const sock_rc = linux.socket(linux.AF.INET, linux.SOCK.DGRAM | linux.SOCK.CLOEXEC, 0);
    if (linux.errno(sock_rc) != .SUCCESS) return error.Unexpected;
    const fd: linux.fd_t = @intCast(sock_rc);
    defer Threaded.closeFd(fd);

    var ifr: linux.ifreq = std.mem.zeroes(linux.ifreq);
    ifr.ifru.ivalue = @intCast(interface.index);
    switch (linux.errno(linux.ioctl(fd, linux.SIOCGIFNAME, @intFromPtr(&ifr)))) {
        .SUCCESS => {
            const raw_bytes: []const u8 = std.mem.sliceTo(&ifr.ifrn.name, 0);
            return net.Interface.Name.fromSlice(raw_bytes);
        },
        .NODEV, .NXIO => return error.InterfaceNotFound,
        else => |err| return posix.unexpectedErrno(err),
    }
}
