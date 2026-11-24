//! Windows `IOCP` completion multiplexer per Shard.
//!
//! Design invariants (AGENTS.md §4):
//! - Exclusive IOCP handle (`NumberOfConcurrentThreads = 1`) per Shard, harvested
//!   in batches via `GetQueuedCompletionStatusEx`.
//! - Enables `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE`
//!   on IFS sockets and overlapped file/pipe handles so synchronous completions
//!   return immediately in user space without Fiber context switching or IOCP traffic,
//!   while tracking non-IFS legacy handles safely.
//! - Coalesced cross-thread doorbell via `PostQueuedCompletionStatus`.
//! - Tracks `in_flight_count` strictly: every kernel-owned `OVERLAPPED` increments
//!   `in_flight_count` on pending submission and decrements ONLY when popped from
//!   IOCP. Destroying `Iocp` while `in_flight_count != 0` triggers `@panic`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const windows = std.os.windows;
const win32 = @import("win32.zig");
const Mailbox = @import("Mailbox.zig");
const Doorbell = Mailbox.Doorbell;

const Iocp = @This();

const wakeup_key: win32.ULONG_PTR = std.math.maxInt(win32.ULONG_PTR);

pub const InitError = error{
    SystemResources,
    Unexpected,
};

/// Address-stable completion block owning a kernel `OVERLAPPED`.
/// Must NOT be moved, reused, or freed until `done == true`.
pub const Completion = struct {
    overlapped: win32.OVERLAPPED = .{},
    done: bool = false,
    bytes_transferred: u32 = 0,
    nt_status: windows.NTSTATUS = .SUCCESS,
    ctx: ?*anyopaque = null,
    callback: *const fn (*Completion) void,
};

iocp_handle: win32.HANDLE,
doorbell: Doorbell = .init,
in_flight_count: usize = 0,
/// Handles where `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` could not be enabled
/// (e.g. legacy non-IFS Winsock LSP). Empty in steady state on modern Windows.
legacy_handles: std.ArrayListUnmanaged(win32.HANDLE) = .empty,
entries_buf: [64]win32.OVERLAPPED_ENTRY = undefined,

pub fn init() InitError!Iocp {
    try win32.ensureWsaInitialized();
    const handle = win32.CreateIoCompletionPort(
        win32.INVALID_HANDLE_VALUE,
        null,
        0,
        1,
    ) orelse switch (windows.GetLastError()) {
        .NOT_ENOUGH_MEMORY, .NO_SYSTEM_RESOURCES => return error.SystemResources,
        else => |err| return windows.unexpectedError(err),
    };
    return .{
        .iocp_handle = handle,
    };
}

pub fn deinit(self: *Iocp, gpa: Allocator) void {
    if (self.in_flight_count != 0) {
        @panic("islet.Iocp.deinit: in-flight OVERLAPPED operations not drained");
    }
    self.legacy_handles.deinit(gpa);
    windows.CloseHandle(self.iocp_handle);
    self.* = undefined;
}

/// Associates a listening socket (where `AcceptEx` uniform completion queueing is desired)
/// without enabling `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`.
pub fn associateListenHandle(self: *Iocp, handle: win32.HANDLE, key: usize) error{Unexpected}!void {
    if (win32.CreateIoCompletionPort(handle, self.iocp_handle, key, 0) == null) {
        return windows.unexpectedError(windows.GetLastError());
    }
}

/// Associates an overlapped file/pipe `HANDLE` with this Shard's IOCP and attempts
/// `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE`.
pub fn associateFileHandle(
    self: *Iocp,
    gpa: Allocator,
    handle: win32.HANDLE,
    key: usize,
) (Allocator.Error || error{Unexpected})!void {
    if (win32.CreateIoCompletionPort(handle, self.iocp_handle, key, 0) == null) {
        return windows.unexpectedError(windows.GetLastError());
    }
    const mode = win32.FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | win32.FILE_SKIP_SET_EVENT_ON_HANDLE;
    if (!win32.SetFileCompletionNotificationModes(handle, mode).toBool()) {
        try self.legacy_handles.append(gpa, handle);
    }
}

/// Associates a Winsock `SOCKET` with this Shard's IOCP for the first and only time,
/// enabling `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` iff `XP1_IFS_HANDLES` is advertised.
pub fn associateSocket(
    self: *Iocp,
    gpa: Allocator,
    sock: win32.SOCKET,
    key: usize,
) (Allocator.Error || error{Unexpected})!void {
    const handle = win32.socketToHandle(sock);
    if (win32.CreateIoCompletionPort(handle, self.iocp_handle, key, 0) == null) {
        return windows.unexpectedError(windows.GetLastError());
    }
    var proto_info: win32.WSAPROTOCOL_INFOW = undefined;
    var info_len: c_int = @sizeOf(win32.WSAPROTOCOL_INFOW);
    const is_ifs = win32.getsockopt(
        sock,
        win32.ws2_32.SOL.SOCKET,
        win32.SO_PROTOCOL_INFOW,
        @ptrCast(&proto_info),
        &info_len,
    ) == 0 and (proto_info.dwServiceFlags1 & win32.XP1_IFS_HANDLES) != 0;

    if (is_ifs) {
        const mode = win32.FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | win32.FILE_SKIP_SET_EVENT_ON_HANDLE;
        if (win32.SetFileCompletionNotificationModes(handle, mode).toBool()) {
            return;
        }
    }
    try self.legacy_handles.append(gpa, handle);
}

pub fn unregisterHandle(self: *Iocp, handle: win32.HANDLE) void {
    if (self.legacy_handles.items.len == 0) {
        @branchHint(.likely);
        return;
    }
    for (self.legacy_handles.items, 0..) |item, idx| {
        if (item == handle) {
            _ = self.legacy_handles.swapRemove(idx);
            return;
        }
    }
}

/// O(1) fast check in steady state (`legacy_handles.items.len == 0`).
pub inline fn hasSkipOnSuccess(self: *const Iocp, handle: win32.HANDLE) bool {
    if (self.legacy_handles.items.len == 0) {
        @branchHint(.likely);
        return true;
    }
    for (self.legacy_handles.items) |item| {
        if (item == handle) return false;
    }
    return true;
}

pub inline fn ntStatusToWin32Error(status: windows.NTSTATUS) u32 {
    if (status == .SUCCESS) return 0;
    return win32.RtlNtStatusToDosError(status);
}

/// Safe from any thread: coalesces wakeups via `Doorbell` and posts a single
/// sentinel completion packet per sleep cycle.
pub fn notifyFromAnyThread(self: *Iocp) void {
    if (!self.doorbell.claimWakeup()) return;
    if (!win32.PostQueuedCompletionStatus(self.iocp_handle, 0, wakeup_key, null).toBool()) {
        @panic("islet.Iocp: PostQueuedCompletionStatus failed");
    }
}

pub inline fn recordPending(self: *Iocp, comp: *Completion) void {
    assert(!comp.done);
    self.in_flight_count += 1;
}

/// Initiates kernel cancellation for an in-flight `Completion`.
/// Caller MUST still wait uncancelably until `comp.done == true` before freeing
/// `comp` or any buffer borrowed by the kernel!
pub fn requestCancelIo(handle: win32.HANDLE, comp: *Completion) void {
    if (comp.done) return;
    _ = win32.CancelIoEx(handle, &comp.overlapped);
}

/// Dequeues up to `entries_buf.len` completion packets (`timeout_ms = win32.INFINITE`
/// for unbounded sleep). Returns the number of packets harvested.
pub fn poll(self: *Iocp, timeout_ms: u32) usize {
    var removed: win32.ULONG = 0;
    const ok = win32.GetQueuedCompletionStatusEx(
        self.iocp_handle,
        &self.entries_buf,
        self.entries_buf.len,
        &removed,
        timeout_ms,
        .FALSE,
    );
    if (!ok.toBool()) {
        const err = windows.GetLastError();
        if (@backingInt(err) == win32.WAIT_TIMEOUT) return 0;
        @panic("islet.Iocp: GetQueuedCompletionStatusEx failed");
    }

    const count: usize = removed;
    for (self.entries_buf[0..count]) |entry| {
        if (entry.lpCompletionKey == wakeup_key and entry.lpOverlapped == null) {
            continue;
        }
        const ov = entry.lpOverlapped orelse continue;
        const comp: *Completion = @fieldParentPtr("overlapped", ov);
        assert(!comp.done);
        assert(self.in_flight_count > 0);
        self.in_flight_count -= 1;
        comp.bytes_transferred = entry.dwNumberOfBytesTransferred;
        comp.nt_status = @fromBackingInt(@intCast(@as(u32, @truncate(entry.Internal))));
        comp.done = true;
        comp.callback(comp);
    }
    return count;
}

pub fn resolveAcceptEx(sock: win32.SOCKET) error{Unexpected}!win32.LPFN_ACCEPTEX {
    var guid = win32.WSAID_ACCEPTEX;
    var fn_ptr: ?win32.LPFN_ACCEPTEX = null;
    var bytes: win32.DWORD = 0;
    const rc = win32.WSAIoctl(
        sock,
        win32.SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid,
        @sizeOf(win32.GUID),
        @ptrCast(&fn_ptr),
        @sizeOf(?win32.LPFN_ACCEPTEX),
        &bytes,
        null,
        null,
    );
    if (rc == win32.SOCKET_ERROR or fn_ptr == null) return error.Unexpected;
    return fn_ptr.?;
}

pub fn resolveConnectEx(sock: win32.SOCKET) error{Unexpected}!win32.LPFN_CONNECTEX {
    var guid = win32.WSAID_CONNECTEX;
    var fn_ptr: ?win32.LPFN_CONNECTEX = null;
    var bytes: win32.DWORD = 0;
    const rc = win32.WSAIoctl(
        sock,
        win32.SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid,
        @sizeOf(win32.GUID),
        @ptrCast(&fn_ptr),
        @sizeOf(?win32.LPFN_CONNECTEX),
        &bytes,
        null,
        null,
    );
    if (rc == win32.SOCKET_ERROR or fn_ptr == null) return error.Unexpected;
    return fn_ptr.?;
}

pub fn resolveWsaRecvMsg(sock: win32.SOCKET) error{Unexpected}!win32.LPFN_WSARECVMSG {
    var guid = win32.WSAID_WSARECVMSG;
    var fn_ptr: ?win32.LPFN_WSARECVMSG = null;
    var bytes: win32.DWORD = 0;
    const rc = win32.WSAIoctl(
        sock,
        win32.SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid,
        @sizeOf(win32.GUID),
        @ptrCast(&fn_ptr),
        @sizeOf(?win32.LPFN_WSARECVMSG),
        &bytes,
        null,
        null,
    );
    if (rc == win32.SOCKET_ERROR or fn_ptr == null) return error.Unexpected;
    return fn_ptr.?;
}
