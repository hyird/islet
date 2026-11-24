//! Windows `IOCP` data-plane I/O driver, `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`
//! synchronous fast path, shared `Acceptor` adoption, and bounded `Io.Batch` engine.
//!
//! Design invariants (AGENTS.md §2, §4, §5):
//! - Concentrates all Win32/Winsock2/IOCP `OVERLAPPED` state machines out of generic
//!   `Shard` scheduling.
//! - Enforces the `OVERLAPPED` lifetime iron law: cancelled or timed-out pending
//!   I/O issues `CancelIoEx` and waits UNCANCELABLY until the completion packet
//!   exits `GetQueuedCompletionStatusEx`.
//! - Exploits `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` on IFS handles so immediate
//!   reads/writes/sends/receives complete synchronously with zero Fiber switches
//!   and zero heap allocations (including inside `Io.Batch`).
//! - Uses `RtlNtStatusToDosError` unified error translation across single `operate`
//!   and `Io.Batch`, and bounds cached `BatchOp` descriptors (`max_cached = 64`).

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

const max_cached_batch_ops: usize = 64;

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
    free_next: ?*BatchOp = null,
    driver: *WindowsIo = undefined,
    batch: *Io.Batch = undefined,
    slot_index: u32 = 0,
    op: Io.Operation = undefined,
    canceled: bool = false,
    result: ?Io.Operation.Result = null,
    backend: union(enum) {
        none,
        iocp: struct {
            handle: win32.HANDLE,
            comp: Iocp.Completion,
            wsabufs: [Threaded.max_iovecs_len]win32.WSABUF = undefined,
            splat_buf: [Threaded.splat_buffer_size]u8 = undefined,
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
batch_op_free: ?*BatchOp = null,
cached_batch_ops: usize = 0,
pending_batch_count: usize = 0,
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

pub fn deinit(self: *WindowsIo, gpa: Allocator) void {
    if (self.pending_batch_count != 0) {
        @panic("islet.WindowsIo.deinit: Io.Batch pending operations were not awaited or canceled");
    }
    var cur = self.batch_op_free;
    self.batch_op_free = null;
    self.cached_batch_ops = 0;
    while (cur) |op| {
        const next = op.free_next;
        gpa.destroy(op);
        cur = next;
    }
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
    const acc = self.acceptor.?;
    if (!q.pushBounded(self.gpa, acc.inboxCapacity(), accepted)) {
        acc.releaseShardLoad(self.shard_id);
        _ = win32.closesocket(accepted.raw_sock);
    }
}

pub fn onStopRequested(self: *WindowsIo) void {
    for (&self.shared_accepts) |*q| {
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
}

pub fn poll(self: *WindowsIo, wait_ns: ?i96) usize {
    const timeout_ms: u32 = if (wait_ns) |ns|
        @intCast(TimerHeap.ceilWaitMs(ns))
    else
        win32.INFINITE;
    return self.poller.poll(timeout_ms);
}

// ── Clock ───────────────────────────────────────────────────────────────────

pub fn now(self: *WindowsIo, clock: Io.Clock) Io.Timestamp {
    const t_io = self.blocking_pool.threaded.io();
    return t_io.vtable.now(t_io.userdata, clock);
}

pub fn clockResolution(self: *WindowsIo, clock: Io.Clock) Io.Clock.ResolutionError!Io.Duration {
    const t_io = self.blocking_pool.threaded.io();
    return t_io.vtable.clockResolution(t_io.userdata, clock);
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
    self: *WindowsIo,
    shard: anytype,
    file: File,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
) File.Writer.WriteFileError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    const reader_buffered = file_reader.interface.buffered();
    const at_eof = if (file_reader.size) |size| (size - file_reader.pos == 0) else false;
    if (reader_buffered.len >= @backingInt(limit) or (at_eof and (header.len != 0 or reader_buffered.len != 0))) {
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
    return if (at_eof) error.EndOfStream else error.Unimplemented;
}

pub fn fileWriteFilePositional(
    _: *WindowsIo,
    shard: anytype,
    file: File,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
    offset: u64,
) File.WriteFilePositionalError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    const reader_buffered = file_reader.interface.buffered();
    const at_eof = if (file_reader.size) |size| (size - file_reader.pos == 0) else false;
    if (reader_buffered.len >= @backingInt(limit) or (at_eof and (header.len != 0 or reader_buffered.len != 0))) {
        const shard_io = shard.io();
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
    return if (at_eof) error.EndOfStream else error.Unimplemented;
}

pub fn processSpawn(_: *WindowsIo, shard: anytype, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
    var sanitized = options;
    sanitized.progress_node = .none;

    return shard.callBlocking(true, struct {
        fn run(t: *Threaded, opts: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
            const t_io = t.io();
            return t_io.vtable.processSpawn(t_io.userdata, opts);
        }
    }.run, .{sanitized});
}

pub fn childWait(_: *WindowsIo, shard: anytype, child: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
    try shard.checkCancelPoint();
    return shard.callBlocking(true, struct {
        fn run(t: *Threaded, c: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
            const t_io = t.io();
            return t_io.vtable.childWait(t_io.userdata, c);
        }
    }.run, .{child});
}

pub fn childKill(_: *WindowsIo, shard: anytype, child: *std.process.Child) void {
    shard.callBlocking(false, struct {
        fn run(t: *Threaded, c: *std.process.Child) void {
            const t_io = t.io();
            t_io.vtable.childKill(t_io.userdata, c);
        }
    }.run, .{child});
}

// ── IOCP Single-Operation Wait (`OVERLAPPED` Iron Law) ──────────────────────

const IocpWaitResult = struct {
    bytes: u32,
    nt_status: windows.NTSTATUS,
    timed_out: bool,
};

fn awaitPending(
    self: *WindowsIo,
    shard: anytype,
    handle: win32.HANDLE,
    comp: *Iocp.Completion,
    maybe_deadline_ns: ?i96,
) Io.Cancelable!IocpWaitResult {
    _ = self;
    const TaskPtr = @TypeOf(shard.current_task);
    const ShardPtr = @TypeOf(shard);

    const State = struct {
        shard_ptr: ShardPtr,
        task: TaskPtr,
        handle: win32.HANDLE,
        comp: *Iocp.Completion,
        timer_token: TimerHeap.Token = .inactive,
        timed_out: bool = false,
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
            } else |_| {}
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

fn blockingOperateOnThreaded(t: *Threaded, op: Io.Operation) Io.Operation.Result {
    const t_io = t.io();
    return t_io.vtable.operate(t_io.userdata, op) catch |err| switch (err) {
        error.Canceled => unreachable,
    };
}

fn runBlockingOperate(t: *Threaded, op_arg: Io.Operation) Io.Cancelable!Io.Operation.Result {
    return blockingOperateOnThreaded(t, op_arg);
}

pub fn operate(self: *WindowsIo, shard: anytype, operation: Io.Operation) Io.Cancelable!Io.Operation.Result {
    try shard.checkCancelPoint();

    switch (operation) {
        .file_read_streaming => |o| {
            if (!o.file.flags.nonblocking) {
                return shard.callBlocking(true, runBlockingOperate, .{operation});
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
            if (ok.toBool()) {
                if (self.poller.hasSkipOnSuccess(o.file.handle)) {
                    return .{ .file_read_streaming = if (sync_bytes == 0) error.EndOfStream else sync_bytes };
                }
            } else {
                const err = @backingInt(windows.GetLastError());
                if (err != win32.ERROR_IO_PENDING) {
                    return .{ .file_read_streaming = mapFileReadError(err) };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitPending(shard, o.file.handle, &comp, null);
            if (res.nt_status != .SUCCESS) {
                return .{ .file_read_streaming = mapFileReadError(Iocp.ntStatusToWin32Error(res.nt_status)) };
            }
            return .{ .file_read_streaming = if (res.bytes == 0) error.EndOfStream else res.bytes };
        },
        .file_write_streaming => |o| {
            if (!o.file.flags.nonblocking) {
                return shard.callBlocking(true, runBlockingOperate, .{operation});
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
            if (ok.toBool()) {
                if (self.poller.hasSkipOnSuccess(o.file.handle)) {
                    return .{ .file_write_streaming = sync_bytes };
                }
            } else {
                const err = @backingInt(windows.GetLastError());
                if (err != win32.ERROR_IO_PENDING) {
                    return .{ .file_write_streaming = mapFileWriteError(err) };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitPending(shard, o.file.handle, &comp, null);
            if (res.nt_status != .SUCCESS) {
                return .{ .file_write_streaming = mapFileWriteError(Iocp.ntStatusToWin32Error(res.nt_status)) };
            }
            return .{ .file_write_streaming = res.bytes };
        },
        .device_io_control => {
            return shard.callBlocking(true, runBlockingOperate, .{operation});
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
                const final_bytes: u32 = if (rc == 0 and self.poller.hasSkipOnSuccess(o.socket_handle))
                    sync_bytes
                else blk: {
                    if (rc == win32.SOCKET_ERROR) {
                        const werr = win32.WSAGetLastError();
                        if (werr != win32.WSA_IO_PENDING) return .{ .net_read = mapNetReadError(werr) };
                    }
                    self.poller.recordPending(&comp);
                    const res = try self.awaitPending(shard, o.socket_handle, &comp, null);
                    if (res.nt_status != .SUCCESS) {
                        return .{ .net_read = mapNetReadError(@intCast(Iocp.ntStatusToWin32Error(res.nt_status))) };
                    }
                    break :blk res.bytes;
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
            if (rc == 0) {
                if (self.poller.hasSkipOnSuccess(o.socket_handle)) {
                    return .{ .net_read = .{ .data_len = sync_bytes } };
                }
            } else {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    return .{ .net_read = mapNetReadError(werr) };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitPending(shard, o.socket_handle, &comp, null);
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
            if (o.control.len > 0) {
                var wmsg: win32.WSAMSG = .{
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
                var sync_bytes: win32.DWORD = 0;
                var comp: Iocp.Completion = .{ .callback = undefined };
                const rc = win32.WSASendMsg(sock, &wmsg, 0, &sync_bytes, &comp.overlapped, null);
                if (rc == 0 and self.poller.hasSkipOnSuccess(o.socket_handle)) {
                    return .{ .net_write = sync_bytes };
                }
                if (rc == win32.SOCKET_ERROR) {
                    const werr = win32.WSAGetLastError();
                    if (werr != win32.WSA_IO_PENDING) return .{ .net_write = mapNetWriteError(werr) };
                }
                self.poller.recordPending(&comp);
                const res = try self.awaitPending(shard, o.socket_handle, &comp, null);
                if (res.nt_status != .SUCCESS) {
                    return .{ .net_write = mapNetWriteError(@intCast(Iocp.ntStatusToWin32Error(res.nt_status))) };
                }
                return .{ .net_write = res.bytes };
            }
            var sync_bytes: win32.DWORD = 0;
            var comp: Iocp.Completion = .{ .callback = undefined };
            const rc = win32.WSASend(sock, &bufs, n_bufs, &sync_bytes, 0, &comp.overlapped, null);
            if (rc == 0) {
                if (self.poller.hasSkipOnSuccess(o.socket_handle)) {
                    return .{ .net_write = sync_bytes };
                }
            } else {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    return .{ .net_write = mapNetWriteError(werr) };
                }
            }
            self.poller.recordPending(&comp);
            const res = try self.awaitPending(shard, o.socket_handle, &comp, null);
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
            const final_bytes: u32 = if (rc == 0 and self.poller.hasSkipOnSuccess(o.socket_handle))
                sync_bytes
            else blk: {
                if (rc == win32.SOCKET_ERROR) {
                    const werr = win32.WSAGetLastError();
                    if (werr != win32.WSA_IO_PENDING) {
                        return .{ .net_receive = .{ mapNetRecvError(werr), 0 } };
                    }
                }
                self.poller.recordPending(&comp);
                const res = try self.awaitPending(shard, o.socket_handle, &comp, null);
                if (res.nt_status != .SUCCESS) {
                    return .{ .net_receive = .{ mapNetRecvError(@intCast(Iocp.ntStatusToWin32Error(res.nt_status))), 0 } };
                }
                break :blk res.bytes;
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

            const skip_ok = self.poller.hasSkipOnSuccess(o.socket_handle);
            var sent_count: usize = 0;
            while (sent_count < o.messages.len) {
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
                if (rc == 0 and skip_ok) {
                    m.data_len = sync_bytes;
                    sent_count += 1;
                    continue;
                }
                if (rc == win32.SOCKET_ERROR) {
                    const werr = win32.WSAGetLastError();
                    if (werr != win32.WSA_IO_PENDING) {
                        return .{ .net_send = .{ mapNetSendError(werr), sent_count } };
                    }
                }
                self.poller.recordPending(&comp);
                const res = try self.awaitPending(shard, o.socket_handle, &comp, null);
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

// ── Bounded `Io.Batch` Engine (with Sync Skip-On-Success Fast Path) ─────────

fn acquireBatchOp(self: *WindowsIo) Allocator.Error!*BatchOp {
    if (self.batch_op_free) |op| {
        self.batch_op_free = op.free_next;
        self.cached_batch_ops -= 1;
        op.* = .{};
        return op;
    }
    const op = try self.gpa.create(BatchOp);
    op.* = .{};
    return op;
}

fn releaseBatchOp(self: *WindowsIo, op: *BatchOp) void {
    if (self.cached_batch_ops < max_cached_batch_ops) {
        op.* = .{ .free_next = self.batch_op_free };
        self.batch_op_free = op;
        self.cached_batch_ops += 1;
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

inline fn wakeBatchAwaiter(self: *WindowsIo, batch: *Io.Batch) void {
    if (batch.userdata) |raw_task| {
        self.wake_task_fn(self.owner_ctx, raw_task);
    }
}

fn finishBatchOp(bop: *BatchOp) void {
    const self = bop.driver;
    const batch = bop.batch;
    const idx = bop.slot_index;
    const maybe_res = bop.result;
    const was_canceled = bop.canceled;

    self.batchRemovePending(batch, idx);
    if (!was_canceled and maybe_res != null) {
        batchAppendCompleted(batch, idx, maybe_res.?);
    } else {
        batchReturnUnused(batch, idx);
    }
    self.releaseBatchOp(bop);
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
        .net_read => .{
            .net_read = if (!ok) mapNetReadError(@intCast(dos_err)) else .{ .data_len = bytes },
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
    if (!bop.canceled) {
        bop.result = translateCompletedBatchResult(bop, comp.nt_status, comp.bytes_transferred);
    }
    finishBatchOp(bop);
}

fn onBatchBlockingExec(job: *BlockingPool.Job, threaded: *Threaded) void {
    const bop: *BatchOp = @ptrCast(@alignCast(job.notify_ctx.?));
    bop.result = blockingOperateOnThreaded(threaded, bop.op);
}

fn onBatchBlockingNotify(ctx: ?*anyopaque) void {
    const bop: *BatchOp = @ptrCast(@alignCast(ctx.?));
    bop.driver.notifyFromAnyThread();
}

fn onBatchBlockingResume(node: *ResumeLane.Node) void {
    const bop: *BatchOp = @ptrCast(@alignCast(node.ctx.?));
    finishBatchOp(bop);
}

fn submitBatchOpBlocking(self: *WindowsIo, bop: *BatchOp) Io.ConcurrentError!void {
    bop.backend = .{ .blocking = .{
        .run_fn = onBatchBlockingExec,
        .resume_lane = self.resume_lane,
        .resume_node = .{ .ctx = bop, .callback = onBatchBlockingResume },
        .notify_ctx = bop,
        .notify_fn = onBatchBlockingNotify,
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
            if (ok.toBool()) {
                if (self.poller.hasSkipOnSuccess(o.file.handle)) {
                    bop.result = .{ .file_read_streaming = if (sync_bytes == 0) error.EndOfStream else sync_bytes };
                    finishBatchOp(bop);
                    return;
                }
            } else {
                const err = @backingInt(windows.GetLastError());
                if (err != win32.ERROR_IO_PENDING) {
                    bop.result = .{ .file_read_streaming = mapFileReadError(err) };
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
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
            if (ok.toBool()) {
                if (self.poller.hasSkipOnSuccess(o.file.handle)) {
                    bop.result = .{ .file_write_streaming = sync_bytes };
                    finishBatchOp(bop);
                    return;
                }
            } else {
                const err = @backingInt(windows.GetLastError());
                if (err != win32.ERROR_IO_PENDING) {
                    bop.result = .{ .file_write_streaming = mapFileWriteError(err) };
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
        },
        .net_read => |o| {
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            const n_bufs = fillReadWsabufs(&bop.backend.iocp.wsabufs, o.data);
            if (n_bufs == 0) {
                bop.result = .{ .net_read = .{ .data_len = 0 } };
                finishBatchOp(bop);
                return;
            }
            const sock = win32.handleToSocket(o.socket_handle);
            var sync_bytes: win32.DWORD = 0;
            const rc = win32.WSARecv(sock, &bop.backend.iocp.wsabufs, n_bufs, &sync_bytes, &bop.backend.iocp.dw_flags, &bop.backend.iocp.comp.overlapped, null);
            if (rc == 0 and self.poller.hasSkipOnSuccess(o.socket_handle)) {
                bop.result = .{ .net_read = .{ .data_len = sync_bytes } };
                finishBatchOp(bop);
                return;
            }
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    bop.result = .{ .net_read = mapNetReadError(werr) };
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
        },
        .net_write => |o| {
            bop.backend = .{ .iocp = .{
                .handle = o.socket_handle,
                .comp = .{ .ctx = bop, .callback = onBatchIocpComplete },
            } };
            const n_bufs = fillWriteWsabufs(&bop.backend.iocp.wsabufs, &bop.backend.iocp.splat_buf, o.header, o.data, o.splat);
            if (n_bufs == 0) {
                bop.result = .{ .net_write = 0 };
                finishBatchOp(bop);
                return;
            }
            const sock = win32.handleToSocket(o.socket_handle);
            var sync_bytes: win32.DWORD = 0;
            const rc = win32.WSASend(sock, &bop.backend.iocp.wsabufs, n_bufs, &sync_bytes, 0, &bop.backend.iocp.comp.overlapped, null);
            if (rc == 0 and self.poller.hasSkipOnSuccess(o.socket_handle)) {
                bop.result = .{ .net_write = sync_bytes };
                finishBatchOp(bop);
                return;
            }
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    bop.result = .{ .net_write = mapNetWriteError(werr) };
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
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
            if (rc == 0 and self.poller.hasSkipOnSuccess(o.socket_handle)) {
                bop.result = translateCompletedBatchResult(bop, .SUCCESS, sync_bytes);
                finishBatchOp(bop);
                return;
            }
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    bop.result = .{ .net_receive = .{ mapNetRecvError(werr), 0 } };
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
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
            if (rc == 0 and self.poller.hasSkipOnSuccess(o.socket_handle)) {
                m.data_len = sync_bytes;
                bop.result = .{ .net_send = .{ null, 1 } };
                finishBatchOp(bop);
                return;
            }
            if (rc == win32.SOCKET_ERROR) {
                const werr = win32.WSAGetLastError();
                if (werr != win32.WSA_IO_PENDING) {
                    bop.result = .{ .net_send = .{ mapNetSendError(werr), 0 } };
                    finishBatchOp(bop);
                    return;
                }
            }
            self.poller.recordPending(&bop.backend.iocp.comp);
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

        const bop = self.acquireBatchOp() catch return error.ConcurrencyUnavailable;
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
            self.releaseBatchOp(bop);
            return error.ConcurrencyUnavailable;
        };
        cur_oi = next_oi;
    }
    batch.submitted = .empty;
}

pub fn cancelBatchPending(self: *WindowsIo, batch: *Io.Batch) void {
    var cur_oi = batch.pending.head;
    while (cur_oi != .none) {
        const idx = cur_oi.toIndex();
        const next_oi = batch.storage[idx].pending.node.next;
        const bop: *BatchOp = @ptrFromInt(batch.storage[idx].pending.userdata[0]);
        bop.canceled = true;

        switch (bop.backend) {
            .none => {},
            .iocp => |*io_st| {
                Iocp.requestCancelIo(io_st.handle, &io_st.comp);
            },
            .blocking => |*job| {
                if (self.blocking_pool.tryCancel(job) == .canceled_queued) {
                    finishBatchOp(bop);
                }
            },
        }
        cur_oi = next_oi;
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

pub fn netListenIp(
    self: *WindowsIo,
    shard: anytype,
    address: *const net.IpAddress,
    options: net.IpAddress.ListenOptions,
) net.IpAddress.ListenError!net.Socket {
    if (options.reuse_address and shard.shard_count > 1) {
        if (self.acceptor) |acc| {
            return acc.openOrJoinListener(self.shard_id, address, options);
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

    var storage: Threaded.PosixAddress = undefined;
    const addr_len = Threaded.addressToPosix(address, &storage);
    if (win32.bind(sock, &storage.any, @intCast(addr_len)) == win32.SOCKET_ERROR) {
        return mapBindOrListenErr(win32.WSAGetLastError());
    }
    const backlog: c_int = @intCast(@min(options.kernel_backlog, std.math.maxInt(c_int)));
    if (win32.listen(sock, backlog) == win32.SOCKET_ERROR) {
        return mapBindOrListenErr(win32.WSAGetLastError());
    }
    var bound: Threaded.PosixAddress = undefined;
    var bound_len: c_int = @sizeOf(Threaded.PosixAddress);
    if (win32.getsockname(sock, &bound.any, &bound_len) == win32.SOCKET_ERROR) {
        return mapBindOrListenErr(win32.WSAGetLastError());
    }
    const handle = win32.socketToHandle(sock);
    try self.poller.associateListenHandle(handle, 0);
    return .{
        .handle = handle,
        .address = Threaded.addressFromPosix(&bound),
    };
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
                else => error.Unexpected,
            };
        }
    }
    // `listen_sock` uses `associateListenHandle` (no skip-on-success) so `AcceptEx` always queues to IOCP.
    self.poller.recordPending(&comp);
    const res = try self.awaitPending(shard, server, &comp, null);
    if (res.nt_status != .SUCCESS) return error.ConnectionAborted;

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

    var storage: Threaded.PosixAddress = undefined;
    const addr_len = Threaded.addressToPosix(address, &storage);
    if (win32.bind(sock, &storage.any, @intCast(addr_len)) == win32.SOCKET_ERROR) {
        return mapBindOrListenErr(win32.WSAGetLastError());
    }
    var bound: Threaded.PosixAddress = undefined;
    var bound_len: c_int = @sizeOf(Threaded.PosixAddress);
    if (win32.getsockname(sock, &bound.any, &bound_len) == win32.SOCKET_ERROR) {
        return mapBindOrListenErr(win32.WSAGetLastError());
    }
    self.poller.associateSocket(self.gpa, sock, 0) catch return error.SystemResources;
    return .{
        .handle = win32.socketToHandle(sock),
        .address = Threaded.addressFromPosix(&bound),
    };
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
        const res = try self.awaitPending(shard, handle, &comp, maybe_dl);
        if (res.timed_out) return error.Timeout;
        if (res.nt_status != .SUCCESS) {
            return mapConnectErr(@intCast(Iocp.ntStatusToWin32Error(res.nt_status)));
        }
    }

    _ = win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.ws2_32.SO.UPDATE_CONNECT_CONTEXT, null, 0);
    var bound_st: Threaded.PosixAddress = undefined;
    var bound_len: c_int = @sizeOf(Threaded.PosixAddress);
    _ = win32.getsockname(sock, &bound_st.any, &bound_len);
    return .{
        .handle = handle,
        .address = Threaded.addressFromPosix(&bound_st),
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
    if (win32.bind(sock, @ptrCast(&sun), @intCast(addr_len)) == win32.SOCKET_ERROR) return error.AddressInUse;
    if (win32.listen(sock, @intCast(@min(options.kernel_backlog, std.math.maxInt(c_int)))) == win32.SOCKET_ERROR) return error.Unexpected;
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
            if (werr != win32.WSA_IO_PENDING) {
                return switch (werr) {
                    win32.WSAECONNREFUSED => error.ConnectionRefused,
                    win32.WSAEACCES => error.AccessDenied,
                    win32.WSAENOBUFS => error.SystemResources,
                    else => error.FileNotFound,
                };
            }
        }
        self.poller.recordPending(&comp);
        const res = try self.awaitPending(shard, handle, &comp, null);
        if (res.nt_status != .SUCCESS) {
            return switch (@as(c_int, @intCast(Iocp.ntStatusToWin32Error(res.nt_status)))) {
                win32.WSAECONNREFUSED => error.ConnectionRefused,
                win32.WSAEACCES => error.AccessDenied,
                else => error.FileNotFound,
            };
        }
    }
    _ = win32.setsockopt(sock, win32.ws2_32.SOL.SOCKET, win32.ws2_32.SO.UPDATE_CONNECT_CONTEXT, null, 0);
    return handle;
}

pub fn netWriteFile(
    self: *WindowsIo,
    shard: anytype,
    socket_handle: net.Socket.Handle,
    header: []const u8,
    file_reader: *File.Reader,
    limit: Io.Limit,
) net.Stream.Writer.WriteFileError!usize {
    if (file_reader.mode == .failure) return error.ReadFailed;
    const reader_buffered = file_reader.interface.buffered();
    const at_eof = if (file_reader.size) |size| (size - file_reader.pos == 0) else false;
    if (reader_buffered.len >= @backingInt(limit) or (at_eof and (header.len != 0 or reader_buffered.len != 0))) {
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
    return if (at_eof) error.EndOfStream else error.Unimplemented;
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
    const listen_sock = self.netListenIp(shard, &lo, .{
        .mode = .stream,
        .protocol = options.protocol orelse .tcp,
    }) catch return error.SystemResources;
    defer self.netClose((&listen_sock)[0..1]);

    const client = self.netConnectIp(shard, &listen_sock.address, .{
        .mode = .stream,
        .protocol = options.protocol,
    }, null) catch return error.SystemResources;
    errdefer self.netClose((&client)[0..1]);

    const accepted = self.netAccept(shard, listen_sock.handle, .{
        .mode = .stream,
        .protocol = options.protocol,
    }) catch return error.SystemResources;
    return .{ client, accepted };
}

pub fn netClose(self: *WindowsIo, sockets: []const net.Socket) void {
    for (sockets) |s| {
        if (self.acceptor) |acc| {
            if (acc.isSharedToken(s.handle)) |tok_idx| {
                acc.closeToken(tok_idx);
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
            else => error.Unexpected,
        };
    }
}

pub fn netInterfaceNameResolve(
    _: *WindowsIo,
    shard: anytype,
    name: *const net.Interface.Name,
) net.Interface.Name.ResolveError!net.Interface {
    return shard.callBlocking(true, struct {
        fn run(t: *Threaded, n: *const net.Interface.Name) net.Interface.Name.ResolveError!net.Interface {
            const t_io = t.io();
            return t_io.vtable.netInterfaceNameResolve(t_io.userdata, n);
        }
    }.run, .{name});
}

pub fn netInterfaceName(
    _: *WindowsIo,
    shard: anytype,
    interface: net.Interface,
) net.Interface.NameError!net.Interface.Name {
    return shard.callBlocking(true, struct {
        fn run(t: *Threaded, iface: net.Interface) net.Interface.NameError!net.Interface.Name {
            const t_io = t.io();
            return t_io.vtable.netInterfaceName(t_io.userdata, iface);
        }
    }.run, .{interface});
}

// ── Unified Win32 / Winsock Error Translators ───────────────────────────────

fn mapFileReadError(code: u32) Io.Operation.FileReadStreaming.Error {
    return switch (code) {
        win32.ERROR_HANDLE_EOF, win32.ERROR_BROKEN_PIPE => error.EndOfStream,
        win32.ERROR_NETNAME_DELETED => error.ConnectionResetByPeer,
        else => error.InputOutput,
    };
}

fn mapFileWriteError(code: u32) Io.Operation.FileWriteStreaming.Error {
    return switch (code) {
        win32.ERROR_BROKEN_PIPE => error.BrokenPipe,
        else => error.InputOutput,
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
        else => error.Unexpected,
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
        else => error.Unexpected,
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
        else => error.Unexpected,
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
        else => error.Unexpected,
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
        else => error.Unexpected,
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
        else => error.Unexpected,
    };
}
