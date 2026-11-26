//! Windows `IOCP` data-plane I/O driver, `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`
//! synchronous fast path, shared `Acceptor` adoption, and bounded `Io.Batch` engine.
//!
//! - Concentrates all Win32/Winsock2/IOCP `OVERLAPPED` state machines out of generic
//!   `Shard` scheduling.
//! - Enforces the `OVERLAPPED` lifetime contract: canceled or timed-out pending
//!   I/O issues `CancelIoEx` and waits uncancelably until the completion packet
//!   exits `GetQueuedCompletionStatusEx`.
//! - Exploits `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` on IFS sockets and overlapped
//!   pipe/file handles so immediate I/O completes synchronously without IOCP traffic.
//! - Uses `RtlNtStatusToDosError` unified error translation across `operate` and `Io.Batch`.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const posix = std.posix;
const windows = std.os.windows;
const net = Io.net;
const File = Io.File;
const Dir = Io.Dir;
const Threaded = Io.Threaded;

const win32 = @import("win32.zig");
const Iocp = @import("Iocp.zig");
const BlockingPool = @import("BlockingPool.zig");
const Mailbox = @import("Mailbox.zig");
const ResumeLane = Mailbox.ResumeLane;
const TimerHeap = @import("TimerHeap.zig");
const AcceptorMod = @import("Acceptor.zig");
const Acceptor = AcceptorMod.Acceptor;
const AcceptedSocket = AcceptorMod.AcceptedSocket;

const WindowsIo = @This();

pub const InitError = Iocp.InitError;

const AcceptWaiter = struct {
    prev: ?*AcceptWaiter = null,
    next: ?*AcceptWaiter = null,
    linked: bool = false,
    canceled: bool = false,
    delivered: ?AcceptedSocket = null,
    task_ptr: *anyopaque,
};

const SharedAcceptQueue = struct {
    ring: []AcceptedSocket = &.{},
    head: usize = 0,
    tail: usize = 0,
    waiter_head: ?*AcceptWaiter = null,
    waiter_tail: ?*AcceptWaiter = null,

    inline fn len(self: *const SharedAcceptQueue) usize {
        return self.tail -% self.head;
    }

    fn pushBounded(self: *SharedAcceptQueue, gpa: Allocator, max_cap: u32, item: AcceptedSocket) bool {
        if (self.ring.len == 0) {
            const pow2_cap = std.math.ceilPowerOfTwoAssert(usize, @max(@as(usize, 16), max_cap));
            self.ring = gpa.alloc(AcceptedSocket, pow2_cap) catch return false;
        }
        if (self.len() >= self.ring.len) return false;
        self.ring[self.tail & (self.ring.len - 1)] = item;
        self.tail +%= 1;
        return true;
    }

    fn popFront(self: *SharedAcceptQueue) ?AcceptedSocket {
        if (self.head == self.tail) return null;
        const item = self.ring[self.head & (self.ring.len - 1)];
        self.head +%= 1;
        return item;
    }

    fn deinit(self: *SharedAcceptQueue, gpa: Allocator) void {
        while (self.popFront()) |item| {
            if (item.raw_sock != win32.INVALID_SOCKET) _ = win32.closesocket(item.raw_sock);
        }
        if (self.ring.len != 0) gpa.free(self.ring);
        self.* = .{};
    }
};

const BatchOp = struct {
    driver: *WindowsIo = undefined,
    batch: *Io.Batch = undefined,
    slot_index: u32 = 0,
    op: Io.Operation = undefined,
    withdrawn: bool = false,
    result: ?Io.Operation.Result = null,
    backend: union(enum) {
        none,
        iocp: struct {
            handle: win32.HANDLE,
            comp: Iocp.Completion,
            wsabufs: [Threaded.max_iovecs_len]win32.WSABUF = undefined,
            splat_buf: [Threaded.splat_buffer_size]u8 = undefined,
            wmsg: win32.WSAMSG = undefined,
            dw_flags: win32.DWORD = 0,
            from_addr: Threaded.PosixAddress = undefined,
            from_len: c_int = @sizeOf(Threaded.PosixAddress),
        },
        blocking: BlockingPool.Job,
    } = .none,
};

gpa: Allocator,
shard_id: usize,
poller: Iocp,
acceptor: ?*Acceptor,
blocking_pool: *BlockingPool,
resume_lane: *ResumeLane,
owner_ctx: *anyopaque,
wake_task_fn: *const fn (owner_ctx: *anyopaque, task_ptr: *anyopaque) void,
pending_batch_count: usize = 0,
subscribed_listeners: u64 = 0,
shared_accepts: [64]SharedAcceptQueue = @splat(.{}),

/// Initializes `self` in-place (address-pinned).
pub fn init(
    self: *WindowsIo,
    gpa: Allocator,
    _: u32,
    acceptor: ?*Acceptor,
    shard_id: usize,
    blocking_pool: *BlockingPool,
    resume_lane: *ResumeLane,
    owner_ctx: *anyopaque,
    wake_task_fn: *const fn (*anyopaque, *anyopaque) void,
) InitError!void {
    const poller = try Iocp.init();
    self.* = .{
        .gpa = gpa,
        .shard_id = shard_id,
        .poller = poller,
        .acceptor = acceptor,
        .blocking_pool = blocking_pool,
        .resume_lane = resume_lane,
        .owner_ctx = owner_ctx,
        .wake_task_fn = wake_task_fn,
    };
    if (acceptor) |acc| {
        acc.bindShard(shard_id, self, notifyOpaque);
    }
}

pub fn assertQuiescent(self: *const WindowsIo) void {
    if (self.pending_batch_count != 0) {
        @panic("islet.WindowsIo: Io.Batch pending operations were not awaited or canceled");
    }
    if (self.poller.in_flight_count != 0) {
        @panic("islet.WindowsIo: in-flight OVERLAPPED operations not drained");
    }
}

pub fn deinit(self: *WindowsIo, gpa: Allocator) void {
    self.assertQuiescent();
    for (&self.shared_accepts) |*q| {
        q.deinit(gpa);
    }
    self.poller.deinit(gpa);
    self.* = undefined;
}

fn notifyOpaque(ctx: ?*anyopaque) void {
    const self: *WindowsIo = @ptrCast(@alignCast(ctx.?));
    self.poller.notifyFromAnyThread();
}

pub inline fn notifyFromAnyThread(self: *WindowsIo) void {
    self.poller.notifyFromAnyThread();
}

pub inline fn armDoorbell(self: *WindowsIo) void {
    self.poller.doorbell.armBeforeSleep();
}

pub inline fn disarmDoorbell(self: *WindowsIo) void {
    self.poller.doorbell.disarm();
}

pub inline fn isAcceptorEmptySeqCst(self: *const WindowsIo) bool {
    if (self.acceptor) |acc| {
        return acc.isShardInboxEmptySeqCst(self.shard_id);
    }
    return true;
}

pub fn drainAcceptor(self: *WindowsIo) void {
    if (self.acceptor) |acc| {
        _ = acc.drainShardInbox(self.shard_id, self, onAcceptedSocket);
    }
}

fn onAcceptedSocket(ctx: ?*anyopaque, accepted: AcceptedSocket) void {
    const self: *WindowsIo = @ptrCast(@alignCast(ctx.?));
    const acc = self.acceptor.?;
    if ((self.subscribed_listeners & (@as(u64, 1) << @intCast(accepted.listener_id))) == 0) {
        acc.releaseShardLoad(self.shard_id);
        _ = win32.closesocket(accepted.raw_sock);
        return;
    }
    const q = &self.shared_accepts[accepted.listener_id];
    if (q.waiter_head) |w| {
        q.waiter_head = w.next;
        if (q.waiter_head) |nh| {
            nh.prev = null;
        } else {
            q.waiter_tail = null;
        }
        w.prev = null;
        w.next = null;
        w.linked = false;
        w.delivered = accepted;
        self.wake_task_fn(self.owner_ctx, w.task_ptr);
        return;
    }
    if (!q.pushBounded(self.gpa, acc.inboxCapacity(), accepted)) {
        acc.releaseShardLoad(self.shard_id);
        _ = win32.closesocket(accepted.raw_sock);
    }
}

pub fn onStopRequested(self: *WindowsIo) void {
    for (&self.shared_accepts) |*q| {
        self.wakeAllAcceptWaiters(q);
    }
}

pub fn scavengeIdle(self: *WindowsIo, gpa: Allocator) void {
    self.poller.scavengeIdle(gpa);
    for (&self.shared_accepts, 0..) |*q, idx| {
        if (q.ring.len != 0 and q.len() == 0 and (self.subscribed_listeners & (@as(u64, 1) << @intCast(idx))) == 0) {
            gpa.free(q.ring);
            q.ring = &.{};
            q.head = 0;
            q.tail = 0;
        }
    }
}

fn wakeAllAcceptWaiters(self: *WindowsIo, q: *SharedAcceptQueue) void {
    var cur = q.waiter_head;
    q.waiter_head = null;
    q.waiter_tail = null;
    while (cur) |w| {
        const next = w.next;
        w.prev = null;
        w.next = null;
        w.linked = false;
        w.canceled = true;
        self.wake_task_fn(self.owner_ctx, w.task_ptr);
        cur = next;
    }
}

pub fn poll(self: *WindowsIo, wait_ns: ?i96) usize {
    const timeout_ms: u32 = if (wait_ns) |ns|
        @intCast(TimerHeap.ceilWaitMs(ns))
    else
        win32.INFINITE;
    return self.poller.poll(timeout_ms);
}

// ── File / Dir / Child Process Lifecycle ────────────────────────────────────

pub fn fileClose(self: *WindowsIo, files: []const File) void {
    for (files) |f| {
        self.poller.unregisterHandle(f.handle);
        windows.CloseHandle(f.handle);
    }
}

pub fn dirClose(_: *WindowsIo, dirs: []const Dir) void {
    for (dirs) |d| {
        windows.CloseHandle(d.handle);
    }
}

pub fn fileWriteFileStreaming(
    _: *WindowsIo,
    _: anytype,
    _: File,
    _: []const u8,
    file_reader: *File.Reader,
    _: Io.Limit,
) File.Writer.WriteFileError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    return error.Unimplemented;
}

pub fn fileWriteFilePositional(
    _: *WindowsIo,
    _: anytype,
    _: File,
    _: []const u8,
    file_reader: *File.Reader,
    _: Io.Limit,
    _: u64,
) File.WriteFilePositionalError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    return error.Unimplemented;
}

pub fn adoptNonblockingFile(self: *WindowsIo, file: *File) void {
    if (!file.flags.nonblocking) return;
    self.poller.associateOverlappedHandle(self.gpa, file.handle) catch {
        file.flags.nonblocking = false;
    };
}

pub fn afterProcessSpawn(self: *WindowsIo, child: *std.process.Child) void {
    inline for (.{ "stdin", "stdout", "stderr" }) |pipe_field| {
        if (@field(child, pipe_field)) |*f| {
            self.adoptNonblockingFile(f);
        }
    }
}

pub inline fn waitChildFast(_: *WindowsIo, _: anytype, _: *std.process.Child) Io.Cancelable!bool {
    return false;
}

// ── IOCP Single-Operation Wait (`OVERLAPPED` Iron Law) ──────────────────────

const IocpWaitResult = struct {
    bytes: u32,
    nt_status: windows.NTSTATUS,
    timed_out: bool,
    oom: bool,
};

fn awaitPending(
    shard: anytype,
    handle: win32.HANDLE,
    comp: *Iocp.Completion,
    maybe_deadline_ns: ?i96,
) Io.Cancelable!IocpWaitResult {
    const TaskPtr = @TypeOf(shard.current_task);
    const ShardPtr = @TypeOf(shard);

    const State = struct {
        shard_ptr: ShardPtr,
        task: TaskPtr,
        handle: win32.HANDLE,
        comp: *Iocp.Completion,
        timer_token: TimerHeap.Token = .inactive,
        timed_out: bool = false,
        oom: bool = false,
        canceled: bool = false,

        fn onComplete(c: *Iocp.Completion) void {
            const st: *@This() = @ptrCast(@alignCast(c.ctx.?));
            if (st.timer_token.isActive()) {
                _ = st.shard_ptr.timers.cancel(st.timer_token);
                st.timer_token = .inactive;
            }
            st.task.cancel_hook = null;
            st.shard_ptr.enqueueReady(st.task);
        }

        fn onTimer(ctx: ?*anyopaque, outcome: TimerHeap.Outcome) void {
            _ = outcome;
            const st: *@This() = @ptrCast(@alignCast(ctx.?));
            st.timer_token = .inactive;
            if (st.comp.done) return;
            st.timed_out = true;
            Iocp.requestCancelIo(st.handle, st.comp);
        }

        fn onCancel(_: ShardPtr, raw_ctx: *anyopaque) void {
            const st: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (st.comp.done) return;
            st.canceled = true;
            Iocp.requestCancelIo(st.handle, st.comp);
        }
    };

    const cur = shard.current_task;
    var st: State = .{
        .shard_ptr = shard,
        .task = cur,
        .handle = handle,
        .comp = comp,
    };
    comp.ctx = &st;
    comp.callback = State.onComplete;

    if (cur.shouldCancelNow()) {
        st.canceled = true;
        Iocp.requestCancelIo(handle, comp);
    } else {
        if (maybe_deadline_ns) |dl| {
            if (dl <= shard.awakeNowNs()) {
                st.timed_out = true;
                Iocp.requestCancelIo(handle, comp);
            } else if (shard.timers.schedule(shard.gpa, dl, &st, State.onTimer)) |tok| {
                st.timer_token = tok;
            } else |_| {
                st.oom = true;
                Iocp.requestCancelIo(handle, comp);
            }
        }
        cur.cancel_hook = .{ .ctx = &st, .trigger = State.onCancel };
    }
    defer {
        cur.cancel_hook = null;
        if (st.timer_token.isActive()) _ = shard.timers.cancel(st.timer_token);
    }

    // NEVER exit this loop until `comp.done == true` (packet dequeued from IOCP)!
    while (!comp.done) {
        shard.suspendCurrent();
    }

    if (st.canceled) {
        if (cur.shouldCancelNow()) cur.acknowledgeCancel();
        return error.Canceled;
    }
    return .{
        .bytes = comp.bytes_transferred,
        .nt_status = comp.nt_status,
        .timed_out = st.timed_out,
        .oom = st.oom,
    };
}

// ── Overlapped Streaming `operate` (with `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`) ──

fn fillWriteWsabufs(
    bufs: *[Threaded.max_iovecs_len]win32.WSABUF,
    splat_buf: *[Threaded.splat_buffer_size]u8,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) u32 {
    var len: usize = 0;
    const add = struct {
        fn f(out: *[Threaded.max_iovecs_len]win32.WSABUF, out_len: *usize, bytes: []const u8) void {
            if (bytes.len == 0 or out_len.* >= out.len) return;
            out[out_len.*] = .{
                .len = @intCast(@min(bytes.len, std.math.maxInt(u32))),
                .buf = @constCast(bytes.ptr),
            };
            out_len.* += 1;
        }
    }.f;

    add(bufs, &len, header);
    if (data.len == 0) return @intCast(len);
    for (data[0 .. data.len - 1]) |seg| add(bufs, &len, seg);
    const pattern = data[data.len - 1];
    if (len < bufs.len) switch (splat) {
        0 => {},
        1 => add(bufs, &len, pattern),
        else => switch (pattern.len) {
            0 => {},
            1 => {
                const fill_n = @min(splat_buf.len, splat);
                @memset(splat_buf[0..fill_n], pattern[0]);
                var rem = splat;
                while (rem > 0 and len < bufs.len) {
                    const chunk = @min(rem, fill_n);
                    add(bufs, &len, splat_buf[0..chunk]);
                    rem -= chunk;
                }
            },
            else => for (0..@min(splat, bufs.len - len)) |_| {
                add(bufs, &len, pattern);
            },
        },
    };
    return @intCast(len);
}

fn fillReadWsabufs(bufs: *[Threaded.max_iovecs_len]win32.WSABUF, data: []const []u8) u32 {
    var n_bufs: u32 = 0;
    for (data) |b| {
        if (n_bufs == bufs.len) break;
        if (b.len > 0) {
            bufs[n_bufs] = .{ .len = @intCast(@min(b.len, std.math.maxInt(u32))), .buf = b.ptr };
            n_bufs += 1;
        }
    }
    return n_bufs;
}

fn firstWriteSlice(header: []const u8, data: []const []const u8, splat: usize) ?[]const u8 {
    if (header.len > 0) return header;
    if (data.len == 0) return null;
    for (data[0 .. data.len - 1]) |b| if (b.len > 0) return b;
    if (splat > 0 and data[data.len - 1].len > 0) return data[data.len - 1];
    return null;
}

const OverlappedLaunch = union(enum) {
    sync_done,
    pending,
    win32_err: u32,
};

inline fn classifyWin32Launch(self: *const WindowsIo, handle: win32.HANDLE, ok: win32.BOOL) OverlappedLaunch {
    if (ok.toBool()) {
        return if (self.poller.hasSkipOnSuccess(handle)) .sync_done else .pending;
    }
    const err = @backingInt(windows.GetLastError());
    return if (err == win32.ERROR_IO_PENDING) .pending else .{ .win32_err = err };
}

const WinsockLaunch = union(enum) {
    sync_done,
    pending,
    wsa_err: c_int,
};

inline fn classifyWinsockLaunch(self: *const WindowsIo, handle: win32.HANDLE, rc: c_int) WinsockLaunch {
    if (rc == 0) {
        return if (self.poller.hasSkipOnSuccess(handle)) .sync_done else .pending;
    }
    const werr = win32.WSAGetLastError();
    return if (werr == win32.WSA_IO_PENDING) .pending else .{ .wsa_err = werr };
}

fn blockingOperateOnThreaded(t: *Threaded, op: Io.Operation) Io.Cancelable!Io.Operation.Result {
    const t_io = t.io();
    return t_io.vtable.operate(t_io.userdata, op);
}

pub fn operate(self: *WindowsIo, shard: anytype, operation: Io.Operation) Io.Cancelable!Io.Operation.Result {
    try shard.checkCancelPoint();

    switch (operation) {
        .file_read_streaming => |o| {
            if (!o.file.flags.nonblocking) {
                return shard.callBlocking(true, blockingOperateOnThreaded, .{operation});
            }
            const dest = for (o.data) |b| {
                if (b.len > 0) break b;
            } else return .{ .file_read_streaming = 0 };

            var sync_bytes: win32.DWORD = 0;
            var comp: Iocp.Completion = .{ .callback = undefined };
            const ok = win32.ReadFile(
                o.file.handle,
                dest.ptr,
                @intCast(@min(dest.len, std.math.maxInt(u32))),
                &sync_bytes,
                &comp.overlapped,
            );
            switch (self.classifyWin32Launch(o.file.handle, ok)) {
                .sync_done => return .{ .file_read_streaming = if (sync_bytes == 0) error.EndOfStream else sync_bytes },
                .win32_err => |err| return .{ .file_read_streaming = mapFileReadError(err) },
                .pending => {},
            }
            self.poller.recordPending(&comp);
            const res = try awaitPending(shard, o.file.handle, &comp, null);
            if (res.nt_status != .SUCCESS) {
                return .{ .file_read_streaming = mapFileReadError(Iocp.ntStatusToWin32Error(res.nt_status)) };
            }
            return .{ .file_read_streaming = if (res.bytes == 0) error.EndOfStream else res.bytes };
        },
        .file_write_streaming => |o| {
            if (!o.file.flags.nonblocking) {
                return shard.callBlocking(true, blockingOperateOnThreaded, .{operation});
            }
            const src = firstWriteSlice(o.header, o.data, o.splat) orelse return .{ .file_write_streaming = 0 };
            var sync_bytes: win32.DWORD = 0;
            var comp: Iocp.Completion = .{ .callback = undefined };
            const ok = win32.WriteFile(
                o.file.handle,
                src.ptr,
                @intCast(@min(src.len, std.math.maxInt(u32))),
                &sync_bytes,
                &comp.overlapped,
            );
            switch (self.classifyWin32Launch(o.file.handle, ok)) {
                .sync_done => return .{ .file_write_streaming = sync_bytes },
                .win32_err => |err| return .{ .file_write_streaming = mapFileWriteError(err) },
                .pending => {},
            }
            self.poller.recordPending(&comp);
            const res = try awaitPending(shard, o.file.handle, &comp, null);
            if (res.nt_status != .SUCCESS) {
                return .{ .file_write_streaming = mapFileWriteError(Iocp.ntStatusToWin32Error(res.nt_status)) };
            }
            return .{ .file_write_streaming = res.bytes };
        },
        .device_io_control => {
            return shard.callBlocking(true, blockingOperateOnThreaded, .{operation});
        },
        .net_read => |o| {
            var bufs: [Threaded.max_iovecs_len]win32.WSABUF = undefined;
            const n_bufs = fillReadWsabufs(&bufs, o.data);
            if (n_bufs == 0 and o.control.len == 0) return .{ .net_read = .{ .data_len = 0 } };
            const sock = win32.handleToSocket(o.socket_handle);
            if (o.control.len > 0) {
                const recv_msg = Iocp.resolveExtension(win32.LPFN_WSARECVMSG, sock, win32.WSAID_WSARECVMSG) catch return .{ .net_read = error.Unexpected };
                var wmsg: win32.WSAMSG = .{
                    .name = null,
                    .namelen = 0,
                    .lpBuffers = &bufs,
                    .dwBufferCount = n_bufs,
                    .Control = .{
                        .len = @intCast(@min(o.control.len, std.math.maxInt(u32))),
                        .buf = o.control.ptr,
                    },
                    .dwFlags = 0,
                };
                var sync_bytes: win32.DWORD = 0;
                var comp: Iocp.Completion = .{ .callback = undefined };
                const rc = recv_msg(sock, &wmsg, &sync_bytes, &comp.overlapped, null);
                const final_bytes: u32 = switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                    .sync_done => sync_bytes,
                    .wsa_err => |werr| return .{ .net_read = mapNetReadError(werr) },
                    .pending => blk: {
                        self.poller.recordPending(&comp);
                        const res = try awaitPending(shard, o.socket_handle, &comp, null);
                        if (res.nt_status != .SUCCESS) {
                            return .{ .net_read = mapNetReadError(@intCast(Iocp.ntStatusToWin32Error(res.nt_status))) };
                        }
                        break :blk res.bytes;
                    },
                };
                return .{ .net_read = .{
                    .data_len = final_bytes,
                    .control_len = @intCast(wmsg.Control.len),
                    .control_truncated = (wmsg.dwFlags & win32.ws2_32.MSG.CTRUNC) != 0,
                } };
            }
            var flags: win32.DWORD = 0;
            var sync_bytes: win32.DWORD = 0;
            var comp: Iocp.Completion = .{ .callback = undefined };
            const rc = win32.WSARecv(sock, &bufs, n_bufs, &sync_bytes, &flags, &comp.overlapped, null);
            switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                .sync_done => return .{ .net_read = .{ .data_len = sync_bytes } },
                .wsa_err => |werr| return .{ .net_read = mapNetReadError(werr) },
                .pending => {},
            }
            self.poller.recordPending(&comp);
            const res = try awaitPending(shard, o.socket_handle, &comp, null);
            if (res.nt_status != .SUCCESS) {
                return .{ .net_read = mapNetReadError(@intCast(Iocp.ntStatusToWin32Error(res.nt_status))) };
            }
            return .{ .net_read = .{ .data_len = res.bytes } };
        },
        .net_write => |o| {
            var bufs: [Threaded.max_iovecs_len]win32.WSABUF = undefined;
            var splat_buf: [Threaded.splat_buffer_size]u8 = undefined;
            const n_bufs = fillWriteWsabufs(&bufs, &splat_buf, o.header, o.data, o.splat);
            if (n_bufs == 0 and o.control.len == 0) return .{ .net_write = 0 };
            const sock = win32.handleToSocket(o.socket_handle);
            var sync_bytes: win32.DWORD = 0;
            var comp: Iocp.Completion = .{ .callback = undefined };
            var wmsg: win32.WSAMSG = undefined;
            const rc = if (o.control.len > 0) blk: {
                wmsg = .{
                    .name = null,
                    .namelen = 0,
                    .lpBuffers = &bufs,
                    .dwBufferCount = n_bufs,
                    .Control = .{
                        .len = @intCast(@min(o.control.len, std.math.maxInt(u32))),
                        .buf = @constCast(o.control.ptr),
                    },
                    .dwFlags = 0,
                };
                break :blk win32.WSASendMsg(sock, &wmsg, 0, &sync_bytes, &comp.overlapped, null);
            } else win32.WSASend(sock, &bufs, n_bufs, &sync_bytes, 0, &comp.overlapped, null);

            switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                .sync_done => return .{ .net_write = sync_bytes },
                .wsa_err => |werr| return .{ .net_write = mapNetWriteError(werr) },
                .pending => {},
            }
            self.poller.recordPending(&comp);
            const res = try awaitPending(shard, o.socket_handle, &comp, null);
            if (res.nt_status != .SUCCESS) {
                return .{ .net_write = mapNetWriteError(@intCast(Iocp.ntStatusToWin32Error(res.nt_status))) };
            }
            return .{ .net_write = res.bytes };
        },
        .net_receive => |o| {
            if (o.message_buffer.len == 0) return .{ .net_receive = .{ null, 0 } };
            const sock = win32.handleToSocket(o.socket_handle);
            var wbuf: [1]win32.WSABUF = .{.{
                .len = @intCast(@min(o.data_buffer.len, std.math.maxInt(u32))),
                .buf = o.data_buffer.ptr,
            }};
            var flags: win32.DWORD = 0;
            if (o.flags.oob) flags |= win32.ws2_32.MSG.OOB;
            if (o.flags.peek) flags |= win32.ws2_32.MSG.PEEK;
            var peer: Threaded.PosixAddress = undefined;
            var peer_len: c_int = @sizeOf(Threaded.PosixAddress);
            var sync_bytes: win32.DWORD = 0;
            var comp: Iocp.Completion = .{ .callback = undefined };
            const rc = win32.WSARecvFrom(sock, &wbuf, 1, &sync_bytes, &flags, &peer.any, &peer_len, &comp.overlapped, null);
            const final_bytes: u32 = switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                .sync_done => sync_bytes,
                .wsa_err => |werr| return .{ .net_receive = .{ mapNetRecvError(werr), 0 } },
                .pending => blk: {
                    self.poller.recordPending(&comp);
                    const res = try awaitPending(shard, o.socket_handle, &comp, null);
                    if (res.nt_status != .SUCCESS) {
                        return .{ .net_receive = .{ mapNetRecvError(@intCast(Iocp.ntStatusToWin32Error(res.nt_status))), 0 } };
                    }
                    break :blk res.bytes;
                },
            };
            const out_msg = &o.message_buffer[0];
            out_msg.from = if (peer_len > 0) Threaded.addressFromPosix(&peer) else .{ .ip4 = .loopback(0) };
            out_msg.data = o.data_buffer[0..final_bytes];
            out_msg.control = &.{};
            out_msg.flags = .{
                .eor = false,
                .trunc = (flags & win32.ws2_32.MSG.PARTIAL) != 0,
                .ctrunc = false,
                .oob = false,
                .errqueue = false,
            };
            return .{ .net_receive = .{ null, 1 } };
        },
        .net_send => |o| {
            if (o.messages.len == 0) return .{ .net_send = .{ null, 0 } };
            const sock = win32.handleToSocket(o.socket_handle);
            var flags: win32.DWORD = 0;
            if (o.flags.dont_route) flags |= win32.ws2_32.MSG.DONTROUTE;
            if (o.flags.oob) flags |= win32.ws2_32.MSG.OOB;

            var sent_count: usize = 0;
            while (sent_count < o.messages.len) {
                if (sent_count > 0 and shard.current_task.shouldCancelNow()) {
                    break;
                }
                const m = &o.messages[sent_count];
                var dest: Threaded.PosixAddress = undefined;
                const dest_len = Threaded.addressToPosix(m.address, &dest);
                var wbuf: [1]win32.WSABUF = .{.{
                    .len = @intCast(@min(m.data_len, std.math.maxInt(u32))),
                    .buf = @constCast(m.data_ptr),
                }};
                var sync_bytes: win32.DWORD = 0;
                var comp: Iocp.Completion = .{ .callback = undefined };
                const rc = win32.WSASendTo(sock, &wbuf, 1, &sync_bytes, flags, &dest.any, @intCast(dest_len), &comp.overlapped, null);
                switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                    .sync_done => {
                        m.data_len = sync_bytes;
                        sent_count += 1;
                        continue;
                    },
                    .wsa_err => |werr| return .{ .net_send = .{ mapNetSendError(werr), sent_count } },
                    .pending => {},
                }
                self.poller.recordPending(&comp);
                const res = awaitPending(shard, o.socket_handle, &comp, null) catch |err| switch (err) {
                    error.Canceled => {
                        if (sent_count > 0) {
                            shard.current_task.recancel();
                            return .{ .net_send = .{ null, sent_count } };
                        }
                        return error.Canceled;
                    },
                };
                if (res.nt_status != .SUCCESS) {
                    return .{ .net_send = .{ mapNetSendError(@intCast(Iocp.ntStatusToWin32Error(res.nt_status))), sent_count } };
                }
                m.data_len = res.bytes;
                sent_count += 1;
            }
            return .{ .net_send = .{ null, sent_count } };
        },
    }
}

// ── `Io.Batch` Engine (with Sync Skip-On-Success Fast Path) ─────────────────

pub fn batchAppendCompleted(batch: *Io.Batch, idx: u32, res: Io.Operation.Result) void {
    const oi = Io.Operation.OptionalIndex.fromIndex(idx);
    switch (batch.completed.tail) {
        .none => batch.completed.head = oi,
        else => |tail_oi| batch.storage[tail_oi.toIndex()].completion.node.next = oi,
    }
    batch.storage[idx] = .{ .completion = .{ .node = .{ .next = .none }, .result = res } };
    batch.completed.tail = oi;
}

fn batchAppendPending(self: *WindowsIo, batch: *Io.Batch, idx: u32, tag: Io.Operation.Tag, bop: *BatchOp) void {
    const cur_oi = Io.Operation.OptionalIndex.fromIndex(idx);
    const prev_tail = batch.pending.tail;
    switch (prev_tail) {
        .none => batch.pending.head = cur_oi,
        else => |t_oi| batch.storage[t_oi.toIndex()].pending.node.next = cur_oi,
    }
    batch.storage[idx] = .{ .pending = .{
        .node = .{ .prev = prev_tail, .next = .none },
        .tag = tag,
        .userdata = .{ @intFromPtr(bop), 0, 0, 0, 0, 0, 0 },
    } };
    batch.pending.tail = cur_oi;
    self.pending_batch_count += 1;
}

fn batchRemovePending(self: *WindowsIo, batch: *Io.Batch, idx: u32) void {
    assert(self.pending_batch_count > 0);
    self.pending_batch_count -= 1;
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

pub fn batchReturnUnused(batch: *Io.Batch, idx: u32) void {
    const oi = Io.Operation.OptionalIndex.fromIndex(idx);
    const tail = batch.unused.tail;
    switch (tail) {
        .none => batch.unused.head = oi,
        else => |tail_oi| batch.storage[tail_oi.toIndex()].unused.next = oi,
    }
    batch.storage[idx] = .{ .unused = .{ .prev = tail, .next = .none } };
    batch.unused.tail = oi;
}

fn batchAppendSubmitted(batch: *Io.Batch, idx: u32, op: Io.Operation) void {
    const oi = Io.Operation.OptionalIndex.fromIndex(idx);
    switch (batch.submitted.tail) {
        .none => batch.submitted.head = oi,
        else => |tail_oi| batch.storage[tail_oi.toIndex()].submission.node.next = oi,
    }
    batch.storage[idx] = .{ .submission = .{ .node = .{ .next = .none }, .operation = op } };
    batch.submitted.tail = oi;
}

inline fn wakeBatchAwaiter(self: *WindowsIo, batch: *Io.Batch) void {
    if (batch.userdata) |raw_task| {
        self.wake_task_fn(self.owner_ctx, raw_task);
    }
}

/// A performed operation surfaces through `completed`; one withdrawn before running goes
/// back to `submitted` for the next await (or for `Batch.cancel` to discard).
fn finishBatchOp(bop: *BatchOp) void {
    const self = bop.driver;
    const batch = bop.batch;
    const idx = bop.slot_index;

    self.batchRemovePending(batch, idx);
    if (bop.result) |res| {
        batchAppendCompleted(batch, idx, res);
    } else {
        batchAppendSubmitted(batch, idx, bop.op);
    }
    self.gpa.destroy(bop);
    self.wakeBatchAwaiter(batch);
}

fn translateCompletedBatchResult(bop: *BatchOp, status: windows.NTSTATUS, bytes: u32) Io.Operation.Result {
    const ok = status == .SUCCESS;
    const dos_err = if (!ok) Iocp.ntStatusToWin32Error(status) else 0;

    return switch (bop.op) {
        .file_read_streaming => .{
            .file_read_streaming = if (!ok)
                mapFileReadError(dos_err)
            else if (bytes == 0)
                error.EndOfStream
            else
                bytes,
        },
        .file_write_streaming => .{
            .file_write_streaming = if (!ok) mapFileWriteError(dos_err) else bytes,
        },
        .net_read => |o| .{
            .net_read = if (!ok) mapNetReadError(@intCast(dos_err)) else .{
                .data_len = bytes,
                .control_len = if (o.control.len > 0) @intCast(bop.backend.iocp.wmsg.Control.len) else 0,
                .control_truncated = if (o.control.len > 0) (bop.backend.iocp.wmsg.dwFlags & win32.ws2_32.MSG.CTRUNC) != 0 else false,
            },
        },
        .net_write => .{
            .net_write = if (!ok) mapNetWriteError(@intCast(dos_err)) else bytes,
        },
        .net_receive => |o| blk: {
            if (!ok) break :blk .{ .net_receive = .{ mapNetRecvError(@intCast(dos_err)), 0 } };
            const out_msg = &o.message_buffer[0];
            out_msg.from = if (bop.backend.iocp.from_len > 0)
                Threaded.addressFromPosix(&bop.backend.iocp.from_addr)
            else
                .{ .ip4 = .loopback(0) };
            out_msg.data = o.data_buffer[0..bytes];
            out_msg.control = &.{};
            out_msg.flags = .{
                .eor = false,
                .trunc = (bop.backend.iocp.dw_flags & win32.ws2_32.MSG.PARTIAL) != 0,
                .ctrunc = false,
                .oob = false,
                .errqueue = false,
            };
            break :blk .{ .net_receive = .{ null, 1 } };
        },
        .net_send => |o| blk: {
            if (!ok) break :blk .{ .net_send = .{ mapNetSendError(@intCast(dos_err)), 0 } };
            o.messages[0].data_len = bytes;
            break :blk .{ .net_send = .{ null, 1 } };
        },
        .device_io_control => unreachable,
    };
}

fn onBatchIocpComplete(comp: *Iocp.Completion) void {
    const bop: *BatchOp = @ptrCast(@alignCast(comp.ctx.?));
    // Only an abort caused by our own `CancelIoEx` means "never ran"; any other outcome of a
    // withdrawn operation already took effect in the kernel and must be reported.
    if (!(bop.withdrawn and comp.nt_status == .CANCELLED)) {
        bop.result = translateCompletedBatchResult(bop, comp.nt_status, comp.bytes_transferred);
    }
    finishBatchOp(bop);
}

inline fn batchOpFromJob(job: *BlockingPool.Job) *BatchOp {
    const backend_ptr: *@FieldType(BatchOp, "backend") = @alignCast(@fieldParentPtr("blocking", job));
    return @fieldParentPtr("backend", backend_ptr);
}

fn onBatchBlockingExec(job: *BlockingPool.Job, threaded: *Threaded) void {
    const bop = batchOpFromJob(job);
    bop.result = blockingOperateOnThreaded(threaded, bop.op) catch |err| switch (err) {
        error.Canceled => unreachable,
    };
}

fn onBatchBlockingResume(node: *ResumeLane.Node) void {
    const job: *BlockingPool.Job = @fieldParentPtr("resume_node", node);
    finishBatchOp(batchOpFromJob(job));
}

fn submitBatchOpBlocking(self: *WindowsIo, bop: *BatchOp) Io.ConcurrentError!void {
    bop.backend = .{ .blocking = .{
        .run_fn = onBatchBlockingExec,
        .resume_lane = self.resume_lane,
        .resume_node = .{ .callback = onBatchBlockingResume },
        .notify_ctx = self,
        .notify_fn = notifyOpaque,
    } };
    switch (self.blocking_pool.submit(&bop.backend.blocking)) {
        .accepted => {},
        .queue_full, .closed => return error.ConcurrencyUnavailable,
    }
}

fn submitSingleBatchOp(self: *WindowsIo, bop: *BatchOp) Io.ConcurrentError!void {
    switch (bop.op) {
        .device_io_control => return self.submitBatchOpBlocking(bop),
        .file_read_streaming => |o| {
            if (!o.file.flags.nonblocking) return self.submitBatchOpBlocking(bop);
            const dest = for (o.data) |b| {
                if (b.len > 0) break b;
            } else {
                bop.result = .{ .file_read_streaming = 0 };
                finishBatchOp(bop);
                return;
            };
            bop.backend = .{ .iocp = .{
                .handle = o.file.handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            var sync_bytes: win32.DWORD = 0;
            const ok = win32.ReadFile(o.file.handle, dest.ptr, @intCast(@min(dest.len, std.math.maxInt(u32))), &sync_bytes, &bop.backend.iocp.comp.overlapped);
            switch (self.classifyWin32Launch(o.file.handle, ok)) {
                .sync_done => {
                    bop.result = .{ .file_read_streaming = if (sync_bytes == 0) error.EndOfStream else sync_bytes };
                    finishBatchOp(bop);
                },
                .win32_err => |err| {
                    bop.result = .{ .file_read_streaming = mapFileReadError(err) };
                    finishBatchOp(bop);
                },
                .pending => self.poller.recordPending(&bop.backend.iocp.comp),
            }
        },
        .file_write_streaming => |o| {
            if (!o.file.flags.nonblocking) return self.submitBatchOpBlocking(bop);
            const src = firstWriteSlice(o.header, o.data, o.splat) orelse {
                bop.result = .{ .file_write_streaming = 0 };
                finishBatchOp(bop);
                return;
            };
            bop.backend = .{ .iocp = .{
                .handle = o.file.handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            var sync_bytes: win32.DWORD = 0;
            const ok = win32.WriteFile(o.file.handle, src.ptr, @intCast(@min(src.len, std.math.maxInt(u32))), &sync_bytes, &bop.backend.iocp.comp.overlapped);
            switch (self.classifyWin32Launch(o.file.handle, ok)) {
                .sync_done => {
                    bop.result = .{ .file_write_streaming = sync_bytes };
                    finishBatchOp(bop);
                },
                .win32_err => |err| {
                    bop.result = .{ .file_write_streaming = mapFileWriteError(err) };
                    finishBatchOp(bop);
                },
                .pending => self.poller.recordPending(&bop.backend.iocp.comp),
            }
        },
        .net_read => |o| {
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            const n_bufs = fillReadWsabufs(&bop.backend.iocp.wsabufs, o.data);
            if (n_bufs == 0 and o.control.len == 0) {
                bop.result = .{ .net_read = .{ .data_len = 0 } };
                finishBatchOp(bop);
                return;
            }
            const sock = win32.handleToSocket(o.socket_handle);
            var sync_bytes: win32.DWORD = 0;
            const rc = if (o.control.len > 0) blk: {
                const recv_msg = Iocp.resolveExtension(win32.LPFN_WSARECVMSG, sock, win32.WSAID_WSARECVMSG) catch {
                    bop.result = .{ .net_read = error.Unexpected };
                    finishBatchOp(bop);
                    return;
                };
                bop.backend.iocp.wmsg = .{
                    .name = null,
                    .namelen = 0,
                    .lpBuffers = &bop.backend.iocp.wsabufs,
                    .dwBufferCount = n_bufs,
                    .Control = .{
                        .len = @intCast(@min(o.control.len, std.math.maxInt(u32))),
                        .buf = o.control.ptr,
                    },
                    .dwFlags = 0,
                };
                break :blk recv_msg(sock, &bop.backend.iocp.wmsg, &sync_bytes, &bop.backend.iocp.comp.overlapped, null);
            } else win32.WSARecv(sock, &bop.backend.iocp.wsabufs, n_bufs, &sync_bytes, &bop.backend.iocp.dw_flags, &bop.backend.iocp.comp.overlapped, null);

            switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                .sync_done => {
                    bop.result = translateCompletedBatchResult(bop, .SUCCESS, sync_bytes);
                    finishBatchOp(bop);
                },
                .wsa_err => |werr| {
                    bop.result = .{ .net_read = mapNetReadError(werr) };
                    finishBatchOp(bop);
                },
                .pending => self.poller.recordPending(&bop.backend.iocp.comp),
            }
        },
        .net_write => |o| {
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            const n_bufs = fillWriteWsabufs(&bop.backend.iocp.wsabufs, &bop.backend.iocp.splat_buf, o.header, o.data, o.splat);
            if (n_bufs == 0 and o.control.len == 0) {
                bop.result = .{ .net_write = 0 };
                finishBatchOp(bop);
                return;
            }
            const sock = win32.handleToSocket(o.socket_handle);
            var sync_bytes: win32.DWORD = 0;
            const rc = if (o.control.len > 0) blk: {
                bop.backend.iocp.wmsg = .{
                    .name = null,
                    .namelen = 0,
                    .lpBuffers = &bop.backend.iocp.wsabufs,
                    .dwBufferCount = n_bufs,
                    .Control = .{
                        .len = @intCast(@min(o.control.len, std.math.maxInt(u32))),
                        .buf = @constCast(o.control.ptr),
                    },
                    .dwFlags = 0,
                };
                break :blk win32.WSASendMsg(sock, &bop.backend.iocp.wmsg, 0, &sync_bytes, &bop.backend.iocp.comp.overlapped, null);
            } else win32.WSASend(sock, &bop.backend.iocp.wsabufs, n_bufs, &sync_bytes, 0, &bop.backend.iocp.comp.overlapped, null);

            switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                .sync_done => {
                    bop.result = .{ .net_write = sync_bytes };
                    finishBatchOp(bop);
                },
                .wsa_err => |werr| {
                    bop.result = .{ .net_write = mapNetWriteError(werr) };
                    finishBatchOp(bop);
                },
                .pending => self.poller.recordPending(&bop.backend.iocp.comp),
            }
        },
        .net_receive => |o| {
            if (o.message_buffer.len == 0) {
                bop.result = .{ .net_receive = .{ null, 0 } };
                finishBatchOp(bop);
                return;
            }
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            if (o.flags.oob) bop.backend.iocp.dw_flags |= win32.ws2_32.MSG.OOB;
            if (o.flags.peek) bop.backend.iocp.dw_flags |= win32.ws2_32.MSG.PEEK;
            bop.backend.iocp.wsabufs[0] = .{ .len = @intCast(@min(o.data_buffer.len, std.math.maxInt(u32))), .buf = o.data_buffer.ptr };
            const sock = win32.handleToSocket(o.socket_handle);
            var sync_bytes: win32.DWORD = 0;
            const rc = win32.WSARecvFrom(sock, &bop.backend.iocp.wsabufs, 1, &sync_bytes, &bop.backend.iocp.dw_flags, &bop.backend.iocp.from_addr.any, &bop.backend.iocp.from_len, &bop.backend.iocp.comp.overlapped, null);
            switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                .sync_done => {
                    bop.result = translateCompletedBatchResult(bop, .SUCCESS, sync_bytes);
                    finishBatchOp(bop);
                },
                .wsa_err => |werr| {
                    bop.result = .{ .net_receive = .{ mapNetRecvError(werr), 0 } };
                    finishBatchOp(bop);
                },
                .pending => self.poller.recordPending(&bop.backend.iocp.comp),
            }
        },
        .net_send => |o| {
            if (o.messages.len == 0) {
                bop.result = .{ .net_send = .{ null, 0 } };
                finishBatchOp(bop);
                return;
            }
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            var send_flags: win32.DWORD = 0;
            if (o.flags.dont_route) send_flags |= win32.ws2_32.MSG.DONTROUTE;
            if (o.flags.oob) send_flags |= win32.ws2_32.MSG.OOB;
            const m = &o.messages[0];
            bop.backend.iocp.wsabufs[0] = .{ .len = @intCast(@min(m.data_len, std.math.maxInt(u32))), .buf = @constCast(m.data_ptr) };
            const dest_len = Threaded.addressToPosix(m.address, &bop.backend.iocp.from_addr);
            const sock = win32.handleToSocket(o.socket_handle);
            var sync_bytes: win32.DWORD = 0;
            const rc = win32.WSASendTo(sock, &bop.backend.iocp.wsabufs, 1, &sync_bytes, send_flags, &bop.backend.iocp.from_addr.any, @intCast(dest_len), &bop.backend.iocp.comp.overlapped, null);
            switch (self.classifyWinsockLaunch(o.socket_handle, rc)) {
                .sync_done => {
                    m.data_len = sync_bytes;
                    bop.result = .{ .net_send = .{ null, 1 } };
                    finishBatchOp(bop);
                },
                .wsa_err => |werr| {
                    bop.result = .{ .net_send = .{ mapNetSendError(werr), 0 } };
                    finishBatchOp(bop);
                },
                .pending => self.poller.recordPending(&bop.backend.iocp.comp),
            }
        },
    }
}

pub fn submitBatch(self: *WindowsIo, batch: *Io.Batch) Io.ConcurrentError!void {
    var cur_oi = batch.submitted.head;
    if (cur_oi == .none) return;
    errdefer {
        batch.submitted.head = cur_oi;
        if (cur_oi == .none) batch.submitted.tail = .none;
    }

    while (cur_oi != .none) {
        const idx = cur_oi.toIndex();
        const saved_sub = batch.storage[idx].submission;
        const op = saved_sub.operation;
        const next_oi = saved_sub.node.next;

        const bop = self.gpa.create(BatchOp) catch return error.ConcurrencyUnavailable;
        bop.* = .{
            .driver = self,
            .batch = batch,
            .slot_index = idx,
            .op = op,
        };
        self.batchAppendPending(batch, idx, op, bop);

        self.submitSingleBatchOp(bop) catch {
            self.batchRemovePending(batch, idx);
            batch.storage[idx] = .{ .submission = saved_sub };
            self.gpa.destroy(bop);
            return error.ConcurrencyUnavailable;
        };
        cur_oi = next_oi;
    }
    batch.submitted = .empty;
}

/// Requests withdrawal of every pending operation: queued `BlockingPool` jobs return to
/// `batch.submitted` at once, in-flight `OVERLAPPED`s get `CancelIoEx` and return there (or to
/// `completed`) when their packet is reaped, and running jobs stay pending until delivered.
pub fn withdrawBatchPending(self: *WindowsIo, batch: *Io.Batch) void {
    var cur_oi = batch.pending.head;
    while (cur_oi != .none) {
        const idx = cur_oi.toIndex();
        cur_oi = batch.storage[idx].pending.node.next;
        const bop: *BatchOp = @ptrFromInt(batch.storage[idx].pending.userdata[0]);
        bop.withdrawn = true;

        switch (bop.backend) {
            .none => unreachable,
            .iocp => |*io_st| Iocp.requestCancelIo(io_st.handle, &io_st.comp),
            .blocking => |*job| {
                if (self.blocking_pool.tryCancel(job) == .canceled_queued) {
                    finishBatchOp(bop);
                }
            },
        }
    }
}

// ── Networking (`net*` — Native Windows `IOCP` + `Acceptor` Adoption) ────────

inline fn openOverlappedSocket(family: c_int, mode: u32, protocol: u32) win32.SOCKET {
    return win32.WSASocketW(
        family,
        @intCast(mode),
        @intCast(protocol),
        null,
        0,
        win32.WSA_FLAG_OVERLAPPED | win32.WSA_FLAG_NO_HANDLE_INHERIT,
    );
}

fn bindIpSocket(sock: win32.SOCKET, address: *const net.IpAddress) WinsockBindListenError!void {
    var storage: Threaded.PosixAddress = undefined;
    const addr_len = Threaded.addressToPosix(address, &storage);
    if (win32.bind(sock, &storage.any, @intCast(addr_len)) == win32.SOCKET_ERROR) {
        return mapBindOrListenErr(win32.WSAGetLastError());
    }
}

fn querySocketAddress(sock: win32.SOCKET) Io.UnexpectedError!net.Socket {
    var bound: Threaded.PosixAddress = undefined;
    var bound_len: c_int = @sizeOf(Threaded.PosixAddress);
    if (win32.getsockname(sock, &bound.any, &bound_len) == win32.SOCKET_ERROR) {
        return win32.unexpectedWin32(@intCast(win32.WSAGetLastError()));
    }
    return .{
        .handle = win32.socketToHandle(sock),
        .address = Threaded.addressFromPosix(&bound),
    };
}

pub fn netListenIp(
    self: *WindowsIo,
    address: *const net.IpAddress,
    options: net.IpAddress.ListenOptions,
) net.IpAddress.ListenError!net.Socket {
    if (options.reuse_address) {
        if (self.acceptor) |acc| {
            const shared_sock = try acc.openOrJoinListener(self.shard_id, address, options);
            const lid = acc.tokenListenerId(acc.isSharedToken(shared_sock.handle).?);
            self.subscribed_listeners |= (@as(u64, 1) << @intCast(lid));
            return shared_sock;
        }
    }
    const family = Threaded.posixAddressFamily(address);
    const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
    const sock = openOverlappedSocket(family, mode, protocol);
    if (sock == win32.INVALID_SOCKET) return mapBindOrListenErr(win32.WSAGetLastError());
    errdefer _ = win32.closesocket(sock);

    const excl: c_int = 1;
    if (win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.SO_EXCLUSIVEADDRUSE, @ptrCast(&excl), @sizeOf(c_int)) == win32.SOCKET_ERROR) {
        return mapBindOrListenErr(win32.WSAGetLastError());
    }

    try bindIpSocket(sock, address);
    const backlog: c_int = @intCast(@min(options.kernel_backlog, std.math.maxInt(c_int)));
    if (win32.listen(sock, backlog) == win32.SOCKET_ERROR) {
        return mapBindOrListenErr(win32.WSAGetLastError());
    }
    const result = try querySocketAddress(sock);
    try self.poller.associateListenHandle(result.handle, 0);
    return result;
}

pub fn netAccept(
    self: *WindowsIo,
    shard: anytype,
    server: net.Socket.Handle,
    options: net.Server.AcceptOptions,
) net.Server.AcceptError!net.Socket {
    if (self.acceptor) |acc| {
        if (acc.isSharedToken(server)) |tok_idx| {
            return self.acceptFromAcceptor(shard, acc, acc.tokenListenerId(tok_idx));
        }
    }
    const listen_sock = win32.handleToSocket(server);
    var local_st: Threaded.PosixAddress = undefined;
    var local_len: c_int = @sizeOf(Threaded.PosixAddress);
    if (win32.getsockname(listen_sock, &local_st.any, &local_len) == win32.SOCKET_ERROR) {
        return error.SocketNotListening;
    }
    const family = local_st.any.family;
    const mode, const protocol = Threaded.posixSocketModeProtocol(family, options.mode, options.protocol) catch {
        return error.ProtocolFailure;
    };
    const accept_ex = try Iocp.resolveExtension(win32.LPFN_ACCEPTEX, listen_sock, win32.WSAID_ACCEPTEX);
    const accept_sock = openOverlappedSocket(family, mode, protocol);
    if (accept_sock == win32.INVALID_SOCKET) return error.SystemResources;
    errdefer _ = win32.closesocket(accept_sock);

    const addr_slot: win32.DWORD = @sizeOf(win32.ws2_32.sockaddr.storage) + 16;
    var addr_buf: [addr_slot * 2]u8 align(8) = undefined;
    var bytes_recvd: win32.DWORD = 0;
    var comp: Iocp.Completion = .{ .callback = undefined };

    const ok = accept_ex(
        listen_sock,
        accept_sock,
        &addr_buf,
        0,
        addr_slot,
        addr_slot,
        &bytes_recvd,
        &comp.overlapped,
    );
    if (!ok.toBool()) {
        const werr = win32.WSAGetLastError();
        if (werr != win32.WSA_IO_PENDING) {
            return switch (werr) {
                win32.WSAECONNRESET => error.ConnectionAborted,
                win32.WSAEINVAL => error.SocketNotListening,
                win32.WSAEMFILE => error.ProcessFdQuotaExceeded,
                win32.WSAENOBUFS => error.SystemResources,
                else => win32.unexpectedWin32(@intCast(werr)),
            };
        }
    }
    // `listen_sock` uses `associateListenHandle` (no skip-on-success) so `AcceptEx` always queues to IOCP.
    self.poller.recordPending(&comp);
    const res = try awaitPending(shard, server, &comp, null);
    if (res.nt_status != .SUCCESS) {
        return switch (Iocp.ntStatusToWin32Error(res.nt_status)) {
            win32.ERROR_OPERATION_ABORTED,
            @as(u32, @intCast(win32.WSAENOTSOCK)),
            @as(u32, @intCast(win32.WSAEINVAL)),
            => error.SocketNotListening,
            @as(u32, @intCast(win32.WSAENETDOWN)) => error.NetworkDown,
            else => error.ConnectionAborted,
        };
    }

    var listen_copy = listen_sock;
    _ = win32.setsockopt(
        accept_sock,
        win32.ws2_32.SOL.SOCKET,
        win32.ws2_32.SO.UPDATE_ACCEPT_CONTEXT,
        @ptrCast(&listen_copy),
        @sizeOf(win32.SOCKET),
    );
    self.poller.associateSocket(self.gpa, accept_sock, 0) catch return error.SystemResources;

    var peer: Threaded.PosixAddress = undefined;
    var peer_len: c_int = @sizeOf(Threaded.PosixAddress);
    const peer_addr = if (win32.getpeername(accept_sock, &peer.any, &peer_len) == 0 and
        (peer.any.family == win32.ws2_32.AF.INET or peer.any.family == win32.ws2_32.AF.INET6))
        Threaded.addressFromPosix(&peer)
    else
        net.IpAddress{ .ip4 = .loopback(0) };

    return .{
        .handle = win32.socketToHandle(accept_sock),
        .address = peer_addr,
    };
}

fn acceptFromAcceptor(self: *WindowsIo, shard: anytype, acc: *Acceptor, listener_id: u32) net.Server.AcceptError!net.Socket {
    shard.drainIncoming();

    const ShardPtr = @TypeOf(shard);
    const q = &self.shared_accepts[listener_id];
    const conn: AcceptedSocket = if (q.popFront()) |queued|
        queued
    else blk: {
        if (shard.isStopping()) return error.SocketNotListening;
        const cur = shard.current_task;
        var waiter: AcceptWaiter = .{ .task_ptr = cur };
        waiter.prev = q.waiter_tail;
        waiter.linked = true;
        if (q.waiter_tail) |t| {
            t.next = &waiter;
        } else {
            q.waiter_head = &waiter;
        }
        q.waiter_tail = &waiter;

        const CancelCtx = struct {
            q_ptr: *SharedAcceptQueue,
            w_ptr: *AcceptWaiter,

            fn unlink(q_arg: *SharedAcceptQueue, w_arg: *AcceptWaiter) void {
                if (!w_arg.linked) return;
                if (w_arg.prev) |p| p.next = w_arg.next else q_arg.waiter_head = w_arg.next;
                if (w_arg.next) |n| n.prev = w_arg.prev else q_arg.waiter_tail = w_arg.prev;
                w_arg.prev = null;
                w_arg.next = null;
                w_arg.linked = false;
            }

            fn onCancel(s: ShardPtr, raw: *anyopaque) void {
                const c: *@This() = @ptrCast(@alignCast(raw));
                unlink(c.q_ptr, c.w_ptr);
                c.w_ptr.canceled = true;
                s.enqueueReady(@ptrCast(@alignCast(c.w_ptr.task_ptr)));
            }
        };
        var cctx: CancelCtx = .{ .q_ptr = q, .w_ptr = &waiter };
        cur.cancel_hook = .{ .ctx = &cctx, .trigger = CancelCtx.onCancel };
        defer {
            cur.cancel_hook = null;
            CancelCtx.unlink(q, &waiter);
        }

        while (waiter.delivered == null and !waiter.canceled) {
            shard.suspendCurrent();
        }
        if (waiter.canceled) {
            if (cur.shouldCancelNow()) {
                cur.acknowledgeCancel();
                return error.Canceled;
            }
            return error.SocketNotListening;
        }
        break :blk waiter.delivered.?;
    };

    acc.releaseShardLoad(self.shard_id);
    errdefer _ = win32.closesocket(conn.raw_sock);

    // First and ONLY IOCP association of this accepted SOCKET happens right here on the target Shard!
    self.poller.associateSocket(self.gpa, conn.raw_sock, 0) catch return error.SystemResources;
    return .{
        .handle = win32.socketToHandle(conn.raw_sock),
        .address = conn.peer_address,
    };
}

pub fn netBindIp(
    self: *WindowsIo,
    address: *const net.IpAddress,
    options: net.IpAddress.BindOptions,
) net.IpAddress.BindError!net.Socket {
    const family = Threaded.posixAddressFamily(address);
    const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
    const sock = openOverlappedSocket(family, mode, protocol);
    if (sock == win32.INVALID_SOCKET) return mapBindOrListenErr(win32.WSAGetLastError());
    errdefer _ = win32.closesocket(sock);

    if (options.ip6_only) |v6only| {
        const val: c_int = @intFromBool(v6only);
        if (win32.setsockopt(sock, win32.IPPROTO_IPV6, win32.IPV6_V6ONLY, @ptrCast(&val), @sizeOf(c_int)) == win32.SOCKET_ERROR) {
            return error.OptionUnsupported;
        }
    }
    if (options.allow_broadcast) {
        const val: c_int = 1;
        if (win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.ws2_32.SO.BROADCAST, @ptrCast(&val), @sizeOf(c_int)) == win32.SOCKET_ERROR) {
            return error.OptionUnsupported;
        }
    }

    try bindIpSocket(sock, address);
    const result = try querySocketAddress(sock);
    self.poller.associateSocket(self.gpa, sock, 0) catch return error.SystemResources;
    return result;
}

pub fn netConnectIp(
    self: *WindowsIo,
    shard: anytype,
    address: *const net.IpAddress,
    options: net.IpAddress.ConnectOptions,
    maybe_dl: ?i96,
) net.IpAddress.ConnectError!net.Socket {
    const family = Threaded.posixAddressFamily(address);
    const mode, const protocol = try Threaded.posixSocketModeProtocol(family, options.mode, options.protocol);
    const sock = openOverlappedSocket(family, mode, protocol);
    if (sock == win32.INVALID_SOCKET) return mapConnectErr(win32.WSAGetLastError());
    errdefer _ = win32.closesocket(sock);

    const any_local: net.IpAddress = switch (address.*) {
        .ip4 => .{ .ip4 = .{ .bytes = .{ 0, 0, 0, 0 }, .port = 0 } },
        .ip6 => .{ .ip6 = .{ .bytes = @splat(0), .port = 0 } },
    };
    var local_st: Threaded.PosixAddress = undefined;
    const local_len = Threaded.addressToPosix(&any_local, &local_st);
    if (win32.bind(sock, &local_st.any, @intCast(local_len)) == win32.SOCKET_ERROR) {
        return mapConnectErr(win32.WSAGetLastError());
    }

    const handle = win32.socketToHandle(sock);
    self.poller.associateSocket(self.gpa, sock, 0) catch return error.SystemResources;
    errdefer self.poller.unregisterHandle(handle);

    const connect_ex = try Iocp.resolveExtension(win32.LPFN_CONNECTEX, sock, win32.WSAID_CONNECTEX);
    var dest_st: Threaded.PosixAddress = undefined;
    const dest_len = Threaded.addressToPosix(address, &dest_st);
    var comp: Iocp.Completion = .{ .callback = undefined };
    const ok = connect_ex(sock, &dest_st.any, @intCast(dest_len), null, 0, null, &comp.overlapped);
    if (!(ok.toBool() and self.poller.hasSkipOnSuccess(handle))) {
        if (!ok.toBool()) {
            const werr = win32.WSAGetLastError();
            if (werr != win32.WSA_IO_PENDING) return mapConnectErr(werr);
        }
        self.poller.recordPending(&comp);
        const res = try awaitPending(shard, handle, &comp, maybe_dl);
        if (res.timed_out) return error.Timeout;
        if (res.nt_status != .SUCCESS) {
            if (res.oom) return error.SystemResources;
            return mapConnectErr(@intCast(Iocp.ntStatusToWin32Error(res.nt_status)));
        }
    }

    _ = win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.ws2_32.SO.UPDATE_CONNECT_CONTEXT, null, 0);
    return querySocketAddress(sock);
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

pub fn netListenUnix(
    self: *WindowsIo,
    ua: *const net.UnixAddress,
    options: net.UnixAddress.ListenOptions,
) net.UnixAddress.ListenError!net.Socket.Handle {
    if (!net.has_unix_sockets) return error.AddressFamilyUnsupported;
    const sock = openOverlappedSocket(win32.ws2_32.AF.UNIX, win32.ws2_32.SOCK.STREAM, 0);
    if (sock == win32.INVALID_SOCKET) return error.AddressFamilyUnsupported;
    errdefer _ = win32.closesocket(sock);
    var sun: posix.sockaddr.un = undefined;
    const addr_len = fillUnixSockaddr(ua, &sun);
    if (win32.bind(sock, @ptrCast(&sun), @intCast(addr_len)) == win32.SOCKET_ERROR) {
        return switch (win32.WSAGetLastError()) {
            win32.WSAEADDRINUSE => error.AddressInUse,
            win32.WSAEADDRNOTAVAIL, win32.WSAEINVAL => error.AddressUnavailable,
            win32.WSAEACCES => error.AccessDenied,
            win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
            win32.WSAENOBUFS => error.SystemResources,
            else => |code| switch (@as(u32, @intCast(code))) {
                win32.ERROR_FILE_NOT_FOUND, win32.ERROR_PATH_NOT_FOUND => error.FileNotFound,
                win32.ERROR_ACCESS_DENIED => error.AccessDenied,
                else => |u| win32.unexpectedWin32(u),
            },
        };
    }
    if (win32.listen(sock, @intCast(@min(options.kernel_backlog, std.math.maxInt(c_int)))) == win32.SOCKET_ERROR) {
        return switch (win32.WSAGetLastError()) {
            win32.WSAEADDRINUSE => error.AddressInUse,
            win32.WSAENOBUFS => error.SystemResources,
            else => |code| win32.unexpectedWin32(@intCast(code)),
        };
    }
    const handle = win32.socketToHandle(sock);
    try self.poller.associateListenHandle(handle, 0);
    return handle;
}

pub fn netConnectUnix(
    self: *WindowsIo,
    shard: anytype,
    ua: *const net.UnixAddress,
) net.UnixAddress.ConnectError!net.Socket.Handle {
    if (!net.has_unix_sockets) return error.AddressFamilyUnsupported;
    const sock = openOverlappedSocket(win32.ws2_32.AF.UNIX, win32.ws2_32.SOCK.STREAM, 0);
    if (sock == win32.INVALID_SOCKET) return error.AddressFamilyUnsupported;
    errdefer _ = win32.closesocket(sock);

    var local_un: posix.sockaddr.un = .{ .family = win32.ws2_32.AF.UNIX, .path = @splat(0) };
    const unnamed_len: c_int = @intCast(@offsetOf(posix.sockaddr.un, "path"));
    if (win32.bind(sock, @ptrCast(&local_un), unnamed_len) == win32.SOCKET_ERROR) {
        return error.Unexpected;
    }

    const handle = win32.socketToHandle(sock);
    self.poller.associateSocket(self.gpa, sock, 0) catch return error.SystemResources;
    errdefer self.poller.unregisterHandle(handle);

    const connect_ex = try Iocp.resolveExtension(win32.LPFN_CONNECTEX, sock, win32.WSAID_CONNECTEX);
    var dest_un: posix.sockaddr.un = undefined;
    const dest_len = fillUnixSockaddr(ua, &dest_un);
    var comp: Iocp.Completion = .{ .callback = undefined };
    const ok = connect_ex(sock, @ptrCast(&dest_un), @intCast(dest_len), null, 0, null, &comp.overlapped);
    if (!(ok.toBool() and self.poller.hasSkipOnSuccess(handle))) {
        if (!ok.toBool()) {
            const werr = win32.WSAGetLastError();
            if (werr != win32.WSA_IO_PENDING) return mapUnixConnectErr(@intCast(werr));
        }
        self.poller.recordPending(&comp);
        const res = try awaitPending(shard, handle, &comp, null);
        if (res.nt_status != .SUCCESS) {
            if (res.oom) return error.SystemResources;
            return mapUnixConnectErr(Iocp.ntStatusToWin32Error(res.nt_status));
        }
    }
    _ = win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.ws2_32.SO.UPDATE_CONNECT_CONTEXT, null, 0);
    return handle;
}

pub fn netWriteFile(
    _: *WindowsIo,
    _: anytype,
    _: net.Socket.Handle,
    _: []const u8,
    file_reader: *File.Reader,
    _: Io.Limit,
) net.Stream.Writer.WriteFileError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    return error.Unimplemented;
}

pub fn netSocketCreatePair(
    self: *WindowsIo,
    shard: anytype,
    options: net.Socket.CreatePairOptions,
) net.Socket.CreatePairError![2]net.Socket {
    if (options.mode != .stream) return error.OperationUnsupported;
    const lo: net.IpAddress = switch (options.family) {
        .ip4 => .{ .ip4 = .loopback(0) },
        .ip6 => .{ .ip6 = .loopback(0) },
    };
    const listen_sock = self.netListenIp(&lo, .{
        .mode = .stream,
        .protocol = options.protocol orelse .tcp,
    }) catch return error.SystemResources;
    defer self.netClose((&listen_sock)[0..1]);

    const client = self.netConnectIp(shard, &listen_sock.address, .{
        .mode = .stream,
        .protocol = options.protocol,
    }, null) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => return error.SystemResources,
    };
    errdefer self.netClose((&client)[0..1]);

    const accepted = self.netAccept(shard, listen_sock.handle, .{
        .mode = .stream,
        .protocol = options.protocol,
    }) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => return error.SystemResources,
    };
    return .{ client, accepted };
}

pub fn netClose(self: *WindowsIo, sockets: []const net.Socket) void {
    for (sockets) |s| {
        if (self.acceptor) |acc| {
            if (acc.isSharedToken(s.handle)) |tok_idx| {
                if (acc.closeToken(tok_idx)) |lid| {
                    self.subscribed_listeners &= ~(@as(u64, 1) << @intCast(lid));
                    self.drainAcceptor();
                    const q = &self.shared_accepts[lid];
                    while (q.popFront()) |orphan| {
                        acc.releaseShardLoad(self.shard_id);
                        if (orphan.raw_sock != win32.INVALID_SOCKET) {
                            _ = win32.closesocket(orphan.raw_sock);
                        }
                    }
                    self.wakeAllAcceptWaiters(q);
                }
                continue;
            }
        }
        self.poller.unregisterHandle(s.handle);
        _ = win32.closesocket(win32.handleToSocket(s.handle));
    }
}

pub fn netShutdown(_: *WindowsIo, handle: net.Socket.Handle, how: net.ShutdownHow) net.ShutdownError!void {
    const whow: c_int = switch (how) {
        .recv => win32.SD_RECEIVE,
        .send => win32.SD_SEND,
        .both => win32.SD_BOTH,
    };
    if (win32.shutdown(win32.handleToSocket(handle), whow) == win32.SOCKET_ERROR) {
        return switch (win32.WSAGetLastError()) {
            win32.WSAENOTCONN => error.SocketUnconnected,
            win32.WSAECONNRESET => error.ConnectionResetByPeer,
            win32.WSAECONNABORTED => error.ConnectionAborted,
            win32.WSAENETDOWN => error.NetworkDown,
            win32.WSAENOBUFS => error.SystemResources,
            else => |code| win32.unexpectedWin32(@intCast(code)),
        };
    }
}

// ── Unified Win32 / Winsock Error Translators ───────────────────────────────

fn mapUnixConnectErr(code: u32) net.UnixAddress.ConnectError {
    return switch (code) {
        @as(u32, @intCast(win32.WSAECONNREFUSED)), win32.ERROR_CONNECTION_REFUSED => error.ConnectionRefused,
        win32.ERROR_FILE_NOT_FOUND, win32.ERROR_PATH_NOT_FOUND => error.FileNotFound,
        @as(u32, @intCast(win32.WSAEACCES)), win32.ERROR_ACCESS_DENIED => error.AccessDenied,
        @as(u32, @intCast(win32.WSAEMFILE)) => error.ProcessFdQuotaExceeded,
        @as(u32, @intCast(win32.WSAENOBUFS)), win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_NO_SYSTEM_RESOURCES => error.SystemResources,
        @as(u32, @intCast(win32.WSAENETDOWN)) => error.NetworkDown,
        @as(u32, @intCast(win32.WSAEAFNOSUPPORT)), @as(u32, @intCast(win32.WSAEINVAL)) => error.AddressFamilyUnsupported,
        @as(u32, @intCast(win32.WSAEPROTOTYPE)) => error.SocketModeUnsupported,
        @as(u32, @intCast(win32.WSAEWOULDBLOCK)) => error.WouldBlock,
        else => win32.unexpectedWin32(code),
    };
}

fn mapFileReadError(code: u32) Io.Operation.FileReadStreaming.Error {
    return switch (code) {
        win32.ERROR_HANDLE_EOF, win32.ERROR_BROKEN_PIPE => error.EndOfStream,
        win32.ERROR_NETNAME_DELETED => error.ConnectionResetByPeer,
        win32.ERROR_ACCESS_DENIED => error.AccessDenied,
        win32.ERROR_INVALID_HANDLE => error.NotOpenForReading,
        win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_NO_SYSTEM_RESOURCES => error.SystemResources,
        else => win32.unexpectedWin32(code),
    };
}

fn mapFileWriteError(code: u32) Io.Operation.FileWriteStreaming.Error {
    return switch (code) {
        win32.ERROR_BROKEN_PIPE => error.BrokenPipe,
        win32.ERROR_DISK_FULL => error.NoSpaceLeft,
        win32.ERROR_ACCESS_DENIED => error.AccessDenied,
        win32.ERROR_INVALID_HANDLE => error.NotOpenForWriting,
        win32.ERROR_NOT_ENOUGH_MEMORY, win32.ERROR_NO_SYSTEM_RESOURCES => error.SystemResources,
        else => win32.unexpectedWin32(code),
    };
}

fn mapNetReadError(code: c_int) Io.Operation.NetRead.Error {
    return switch (code) {
        win32.WSAECONNRESET, win32.WSAECONNABORTED, win32.WSAENETRESET => error.ConnectionResetByPeer,
        win32.WSAENOTCONN, win32.WSAESHUTDOWN => error.SocketUnconnected,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAETIMEDOUT => error.ConnectionTimedOut,
        win32.WSAENOBUFS => error.SystemResources,
        else => win32.unexpectedWin32(@intCast(code)),
    };
}

fn mapNetWriteError(code: c_int) Io.Operation.NetWrite.Error {
    return switch (code) {
        win32.WSAECONNRESET, win32.WSAECONNABORTED, win32.WSAENETRESET => error.ConnectionResetByPeer,
        win32.WSAENOTCONN, win32.WSAESHUTDOWN => error.SocketUnconnected,
        win32.WSAECONNREFUSED => error.ConnectionRefused,
        win32.WSAENETUNREACH => error.NetworkUnreachable,
        win32.WSAEHOSTUNREACH => error.HostUnreachable,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAETIMEDOUT => error.ConnectionTimedOut,
        win32.WSAEDESTADDRREQ, win32.WSAEINVAL => error.SocketNotBound,
        win32.WSAENOBUFS => error.SystemResources,
        else => win32.unexpectedWin32(@intCast(code)),
    };
}

fn mapNetRecvError(code: c_int) net.Socket.ReceiveError {
    return switch (code) {
        win32.WSAEMSGSIZE => error.MessageOversize,
        win32.WSAECONNRESET, win32.ERROR_PORT_UNREACHABLE => error.PortUnreachable,
        win32.WSAENOTCONN, win32.WSAESHUTDOWN => error.SocketUnconnected,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAETIMEDOUT => error.ConnectionTimedOut,
        win32.WSAENOBUFS => error.SystemResources,
        else => win32.unexpectedWin32(@intCast(code)),
    };
}

fn mapNetSendError(code: c_int) net.Socket.SendError {
    return switch (code) {
        win32.WSAEMSGSIZE => error.MessageOversize,
        win32.WSAECONNREFUSED, win32.ERROR_PORT_UNREACHABLE => error.ConnectionRefused,
        win32.WSAECONNRESET => error.ConnectionResetByPeer,
        win32.WSAENETUNREACH => error.NetworkUnreachable,
        win32.WSAEHOSTUNREACH => error.HostUnreachable,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAENOTCONN, win32.WSAESHUTDOWN => error.SocketUnconnected,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAETIMEDOUT => error.ConnectionTimedOut,
        win32.WSAENOBUFS => error.SystemResources,
        else => win32.unexpectedWin32(@intCast(code)),
    };
}

const WinsockBindListenError = error{
    AddressInUse,
    AddressUnavailable,
    AccessDenied,
    AddressFamilyUnsupported,
    ProtocolUnsupportedByAddressFamily,
    SocketModeUnsupported,
    ProcessFdQuotaExceeded,
    SystemResources,
    NetworkDown,
    Unexpected,
};

fn mapBindOrListenErr(code: c_int) WinsockBindListenError {
    return switch (code) {
        win32.WSAEADDRINUSE => error.AddressInUse,
        win32.WSAEADDRNOTAVAIL => error.AddressUnavailable,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAEPROTONOSUPPORT => error.ProtocolUnsupportedByAddressFamily,
        win32.WSAEPROTOTYPE, win32.WSAESOCKTNOSUPPORT => error.SocketModeUnsupported,
        win32.WSAEMFILE => error.ProcessFdQuotaExceeded,
        win32.WSAENOBUFS => error.SystemResources,
        win32.WSAENETDOWN => error.NetworkDown,
        else => win32.unexpectedWin32(@intCast(code)),
    };
}

fn mapConnectErr(code: c_int) net.IpAddress.ConnectError {
    return switch (code) {
        win32.WSAEADDRNOTAVAIL => error.AddressUnavailable,
        win32.WSAEAFNOSUPPORT => error.AddressFamilyUnsupported,
        win32.WSAEALREADY, win32.WSAEINPROGRESS => error.ConnectionPending,
        win32.WSAECONNREFUSED => error.ConnectionRefused,
        win32.WSAECONNRESET => error.ConnectionResetByPeer,
        win32.WSAEHOSTUNREACH => error.HostUnreachable,
        win32.WSAENETUNREACH => error.NetworkUnreachable,
        win32.WSAETIMEDOUT, win32.ERROR_SEM_TIMEOUT => error.Timeout,
        win32.WSAEACCES => error.AccessDenied,
        win32.WSAENETDOWN => error.NetworkDown,
        win32.WSAENOBUFS => error.SystemResources,
        else => win32.unexpectedWin32(@intCast(code)),
    };
}
