//! Win32, IOCP, Win32 Fiber, and Winsock2 extern bindings for Zig 0.17.1.
//!
//! Zig 0.17.1 stripped IOCP, Fiber, and Winsock2 `extern` declarations from
//! `std.os.windows.kernel32` and `std.os.windows.ws2_32`. Note that `AcceptEx`,
//! `ConnectEx`, and `WSARecvMsg` are not exported by `ws2_32.dll` and must be
//! queried via `WSAIoctl(..., SIO_GET_EXTENSION_FUNCTION_POINTER, ...)`.

const std = @import("std");
const windows = std.os.windows;
pub const ws2_32 = windows.ws2_32;

pub const BOOL = windows.BOOL;
pub const BYTE = windows.BYTE;
pub const UCHAR = windows.UCHAR;
pub const WORD = windows.WORD;
pub const DWORD = windows.DWORD;
pub const ULONG = windows.ULONG;
pub const ULONG_PTR = windows.ULONG_PTR;
pub const SIZE_T = windows.SIZE_T;
pub const INT = windows.INT;
pub const HANDLE = windows.HANDLE;
pub const PVOID = windows.PVOID;
pub const LPVOID = windows.LPVOID;
pub const GUID = windows.GUID;
pub const INVALID_HANDLE_VALUE = windows.INVALID_HANDLE_VALUE;

pub const INFINITE: DWORD = 0xFFFFFFFF;
pub const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: UCHAR = 0x1;
pub const FILE_SKIP_SET_EVENT_ON_HANDLE: UCHAR = 0x2;

pub const ERROR_ALREADY_FIBER: u32 = 1280;
pub const ERROR_IO_PENDING: u32 = 997;
pub const ERROR_OPERATION_ABORTED: u32 = 995;
pub const ERROR_NOT_FOUND: u32 = 1168;
pub const ERROR_HANDLE_EOF: u32 = 38;
pub const ERROR_BROKEN_PIPE: u32 = 109;
pub const ERROR_NETNAME_DELETED: u32 = 64;
pub const ERROR_CONNECTION_REFUSED: u32 = 1225;
pub const ERROR_PORT_UNREACHABLE: u32 = 1234;
pub const ERROR_CONNECTION_ABORTED: u32 = 1236;
pub const ERROR_SEM_TIMEOUT: u32 = 121;
pub const WAIT_TIMEOUT: u32 = 258;

// ── IOCP (`kernel32`) ────────────────────────────────────────────────────────

pub const OVERLAPPED = extern struct {
    Internal: ULONG_PTR = 0,
    InternalHigh: ULONG_PTR = 0,
    DUMMYUNIONNAME: extern union {
        DUMMYSTRUCTNAME: extern struct {
            Offset: DWORD = 0,
            OffsetHigh: DWORD = 0,
        },
        Pointer: ?PVOID,
    } = .{ .Pointer = null },
    hEvent: ?HANDLE = null,
};

pub const OVERLAPPED_ENTRY = extern struct {
    lpCompletionKey: ULONG_PTR,
    lpOverlapped: ?*OVERLAPPED,
    Internal: ULONG_PTR,
    dwNumberOfBytesTransferred: DWORD,
};

pub extern "kernel32" fn CreateIoCompletionPort(
    FileHandle: HANDLE,
    ExistingCompletionPort: ?HANDLE,
    CompletionKey: ULONG_PTR,
    NumberOfConcurrentThreads: DWORD,
) callconv(.winapi) ?HANDLE;

pub extern "kernel32" fn GetQueuedCompletionStatusEx(
    CompletionPort: HANDLE,
    lpCompletionPortEntries: [*]OVERLAPPED_ENTRY,
    ulCount: ULONG,
    ulNumEntriesRemoved: *ULONG,
    dwMilliseconds: DWORD,
    fAlertable: BOOL,
) callconv(.winapi) BOOL;

pub extern "kernel32" fn PostQueuedCompletionStatus(
    CompletionPort: HANDLE,
    dwNumberOfBytesTransferred: DWORD,
    dwCompletionKey: ULONG_PTR,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) BOOL;

pub extern "kernel32" fn CancelIoEx(
    hFile: HANDLE,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) BOOL;

pub extern "kernel32" fn SetFileCompletionNotificationModes(
    FileHandle: HANDLE,
    Flags: UCHAR,
) callconv(.winapi) BOOL;

pub extern "ntdll" fn RtlNtStatusToDosError(
    Status: windows.NTSTATUS,
) callconv(.winapi) ULONG;

pub extern "kernel32" fn ReadFile(
    hFile: HANDLE,
    lpBuffer: ?[*]u8,
    nNumberOfBytesToRead: DWORD,
    lpNumberOfBytesRead: ?*DWORD,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) BOOL;

pub extern "kernel32" fn WriteFile(
    hFile: HANDLE,
    lpBuffer: ?[*]const u8,
    nNumberOfBytesToWrite: DWORD,
    lpNumberOfBytesWritten: ?*DWORD,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) BOOL;

// ── Win32 Fibers (`kernel32`) ────────────────────────────────────────────────

pub const FIBER_FLAG_FLOAT_SWITCH: DWORD = 0x1;
pub const LPFIBER_START_ROUTINE = *const fn (lpFiberParameter: ?LPVOID) callconv(.winapi) void;

pub extern "kernel32" fn ConvertThreadToFiberEx(
    lpParameter: ?LPVOID,
    dwFlags: DWORD,
) callconv(.winapi) ?LPVOID;

pub extern "kernel32" fn ConvertFiberToThread() callconv(.winapi) BOOL;

pub extern "kernel32" fn CreateFiberEx(
    dwStackCommitSize: SIZE_T,
    dwStackReserveSize: SIZE_T,
    dwFlags: DWORD,
    lpStartAddress: LPFIBER_START_ROUTINE,
    lpParameter: ?LPVOID,
) callconv(.winapi) ?LPVOID;

pub extern "kernel32" fn SwitchToFiber(lpFiber: LPVOID) callconv(.winapi) void;
pub extern "kernel32" fn DeleteFiber(lpFiber: LPVOID) callconv(.winapi) void;

// ── Winsock2 (`ws2_32`) ──────────────────────────────────────────────────────

pub const SOCKET = usize;
pub const INVALID_SOCKET: SOCKET = std.math.maxInt(SOCKET);
pub const SOCKET_ERROR: c_int = -1;

pub const WSA_FLAG_OVERLAPPED: DWORD = 0x01;
pub const WSA_FLAG_NO_HANDLE_INHERIT: DWORD = 0x80;

pub const SO_EXCLUSIVEADDRUSE: c_int = ~@as(c_int, ws2_32.SO.REUSEADDR);
pub const SO_PROTOCOL_INFOW: c_int = 0x2005;
pub const XP1_IFS_HANDLES: DWORD = 0x00020000;
pub const IPPROTO_IPV6: c_int = 41;
pub const IPV6_V6ONLY: c_int = 27;

pub const WSAPROTOCOLCHAIN = extern struct {
    ChainLen: c_int,
    ChainEntries: [7]DWORD,
};

pub const WSAPROTOCOL_INFOW = extern struct {
    dwServiceFlags1: DWORD,
    dwServiceFlags2: DWORD,
    dwServiceFlags3: DWORD,
    dwServiceFlags4: DWORD,
    dwProviderFlags: DWORD,
    ProviderId: GUID,
    dwCatalogEntryId: DWORD,
    ProtocolChain: WSAPROTOCOLCHAIN,
    iVersion: c_int,
    iAddressFamily: c_int,
    iMaxSockAddr: c_int,
    iMinSockAddr: c_int,
    iSocketType: c_int,
    iProtocol: c_int,
    iProtocolMaxOffset: c_int,
    iNetworkByteOrder: c_int,
    iSecurityScheme: c_int,
    dwMessageSize: DWORD,
    dwProviderReserved: DWORD,
    szProtocol: [256]u16,
};

pub const SD_RECEIVE: c_int = 0;
pub const SD_SEND: c_int = 1;
pub const SD_BOTH: c_int = 2;

pub const IOC_OUT: DWORD = 0x40000000;
pub const IOC_IN: DWORD = 0x80000000;
pub const IOC_INOUT: DWORD = IOC_IN | IOC_OUT;
pub const IOC_WS2: DWORD = 0x08000000;
pub const SIO_GET_EXTENSION_FUNCTION_POINTER: DWORD = IOC_INOUT | IOC_WS2 | 6;

pub const WSA_IO_PENDING: c_int = 997;
pub const WSA_OPERATION_ABORTED: c_int = 995;
pub const WSAEINTR: c_int = 10004;
pub const WSAEACCES: c_int = 10013;
pub const WSAEFAULT: c_int = 10014;
pub const WSAEINVAL: c_int = 10022;
pub const WSAEMFILE: c_int = 10024;
pub const WSAEWOULDBLOCK: c_int = 10035;
pub const WSAEINPROGRESS: c_int = 10036;
pub const WSAEALREADY: c_int = 10037;
pub const WSAENOTSOCK: c_int = 10038;
pub const WSAEDESTADDRREQ: c_int = 10039;
pub const WSAEMSGSIZE: c_int = 10040;
pub const WSAEPROTOTYPE: c_int = 10041;
pub const WSAENOPROTOOPT: c_int = 10042;
pub const WSAEPROTONOSUPPORT: c_int = 10043;
pub const WSAESOCKTNOSUPPORT: c_int = 10044;
pub const WSAEOPNOTSUPP: c_int = 10045;
pub const WSAEAFNOSUPPORT: c_int = 10047;
pub const WSAEADDRINUSE: c_int = 10048;
pub const WSAEADDRNOTAVAIL: c_int = 10049;
pub const WSAENETDOWN: c_int = 10050;
pub const WSAENETUNREACH: c_int = 10051;
pub const WSAENETRESET: c_int = 10052;
pub const WSAECONNABORTED: c_int = 10053;
pub const WSAECONNRESET: c_int = 10054;
pub const WSAENOBUFS: c_int = 10055;
pub const WSAEISCONN: c_int = 10056;
pub const WSAENOTCONN: c_int = 10057;
pub const WSAESHUTDOWN: c_int = 10058;
pub const WSAETIMEDOUT: c_int = 10060;
pub const WSAECONNREFUSED: c_int = 10061;
pub const WSAEHOSTUNREACH: c_int = 10065;

pub const WSADATA = if (@sizeOf(usize) == 8) extern struct {
    wVersion: WORD,
    wHighVersion: WORD,
    iMaxSockets: u16,
    iMaxUdpDg: u16,
    lpVendorInfo: ?[*]u8,
    szDescription: [257]u8,
    szSystemStatus: [129]u8,
} else extern struct {
    wVersion: WORD,
    wHighVersion: WORD,
    szDescription: [257]u8,
    szSystemStatus: [129]u8,
    iMaxSockets: u16,
    iMaxUdpDg: u16,
    lpVendorInfo: ?[*]u8,
};

pub const WSABUF = extern struct {
    len: ULONG,
    buf: ?[*]u8,
};

pub const WSAMSG = extern struct {
    name: ?*ws2_32.sockaddr,
    namelen: INT,
    lpBuffers: [*]WSABUF,
    dwBufferCount: ULONG,
    Control: WSABUF,
    dwFlags: ULONG,
};

pub const LPWSAOVERLAPPED_COMPLETION_ROUTINE = *const fn (
    dwError: DWORD,
    cbTransferred: DWORD,
    lpOverlapped: *OVERLAPPED,
    dwFlags: DWORD,
) callconv(.winapi) void;

pub const WSAID_ACCEPTEX: GUID = .{
    .Data1 = 0xb5367df1,
    .Data2 = 0xcbac,
    .Data3 = 0x11cf,
    .Data4 = .{ 0x95, 0xca, 0x00, 0x80, 0x5f, 0x48, 0xa1, 0x92 },
};

pub const WSAID_CONNECTEX: GUID = .{
    .Data1 = 0x25a207b9,
    .Data2 = 0xddf3,
    .Data3 = 0x4660,
    .Data4 = .{ 0x8e, 0xe9, 0x76, 0xe5, 0x8c, 0x74, 0x06, 0x3e },
};

pub const WSAID_WSARECVMSG: GUID = .{
    .Data1 = 0xf689d7c8,
    .Data2 = 0x6f1f,
    .Data3 = 0x436b,
    .Data4 = .{ 0x8a, 0x53, 0xe5, 0x4f, 0xe3, 0x51, 0xc3, 0x22 },
};

pub const LPFN_ACCEPTEX = *const fn (
    sListenSocket: SOCKET,
    sAcceptSocket: SOCKET,
    lpOutputBuffer: PVOID,
    dwReceiveDataLength: DWORD,
    dwLocalAddressLength: DWORD,
    dwRemoteAddressLength: DWORD,
    lpdwBytesReceived: *DWORD,
    lpOverlapped: *OVERLAPPED,
) callconv(.winapi) BOOL;

pub const LPFN_CONNECTEX = *const fn (
    s: SOCKET,
    name: *const ws2_32.sockaddr,
    namelen: c_int,
    lpSendBuffer: ?*const anyopaque,
    dwSendDataLength: DWORD,
    lpdwBytesSent: ?*DWORD,
    lpOverlapped: *OVERLAPPED,
) callconv(.winapi) BOOL;

pub const LPFN_WSARECVMSG = *const fn (
    s: SOCKET,
    lpMsg: *WSAMSG,
    lpdwNumberOfBytesRecvd: ?*DWORD,
    lpOverlapped: *OVERLAPPED,
    lpCompletionRoutine: ?LPWSAOVERLAPPED_COMPLETION_ROUTINE,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn WSAStartup(wVersionRequested: WORD, lpWSAData: *WSADATA) callconv(.winapi) c_int;
pub extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;
pub extern "ws2_32" fn WSASocketW(
    af: c_int,
    @"type": c_int,
    protocol: c_int,
    lpProtocolInfo: ?*anyopaque,
    g: ws2_32.GROUP,
    dwFlags: DWORD,
) callconv(.winapi) SOCKET;
pub extern "ws2_32" fn closesocket(s: SOCKET) callconv(.winapi) c_int;
pub extern "ws2_32" fn shutdown(s: SOCKET, how: c_int) callconv(.winapi) c_int;
pub extern "ws2_32" fn bind(s: SOCKET, name: *const ws2_32.sockaddr, namelen: c_int) callconv(.winapi) c_int;
pub extern "ws2_32" fn listen(s: SOCKET, backlog: c_int) callconv(.winapi) c_int;
pub extern "ws2_32" fn setsockopt(s: SOCKET, level: c_int, optname: c_int, optval: ?[*]const u8, optlen: c_int) callconv(.winapi) c_int;
pub extern "ws2_32" fn getsockopt(s: SOCKET, level: c_int, optname: c_int, optval: [*]u8, optlen: *c_int) callconv(.winapi) c_int;
pub extern "ws2_32" fn getsockname(s: SOCKET, name: *ws2_32.sockaddr, namelen: *c_int) callconv(.winapi) c_int;
pub extern "ws2_32" fn getpeername(s: SOCKET, name: *ws2_32.sockaddr, namelen: *c_int) callconv(.winapi) c_int;

pub extern "ws2_32" fn WSARecv(
    s: SOCKET,
    lpBuffers: [*]WSABUF,
    dwBufferCount: DWORD,
    lpNumberOfBytesRecvd: ?*DWORD,
    lpFlags: *DWORD,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?LPWSAOVERLAPPED_COMPLETION_ROUTINE,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn WSASend(
    s: SOCKET,
    lpBuffers: [*]const WSABUF,
    dwBufferCount: DWORD,
    lpNumberOfBytesSent: ?*DWORD,
    dwFlags: DWORD,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?LPWSAOVERLAPPED_COMPLETION_ROUTINE,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn WSARecvFrom(
    s: SOCKET,
    lpBuffers: [*]WSABUF,
    dwBufferCount: DWORD,
    lpNumberOfBytesRecvd: ?*DWORD,
    lpFlags: *DWORD,
    lpFrom: ?*ws2_32.sockaddr,
    lpFromlen: ?*c_int,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?LPWSAOVERLAPPED_COMPLETION_ROUTINE,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn WSASendTo(
    s: SOCKET,
    lpBuffers: [*]const WSABUF,
    dwBufferCount: DWORD,
    lpNumberOfBytesSent: ?*DWORD,
    dwFlags: DWORD,
    lpTo: ?*const ws2_32.sockaddr,
    iTolen: c_int,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?LPWSAOVERLAPPED_COMPLETION_ROUTINE,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn WSASendMsg(
    s: SOCKET,
    lpMsg: *const WSAMSG,
    dwFlags: DWORD,
    lpNumberOfBytesSent: ?*DWORD,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?LPWSAOVERLAPPED_COMPLETION_ROUTINE,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn WSAIoctl(
    s: SOCKET,
    dwIoControlCode: DWORD,
    lpvInBuffer: ?*const anyopaque,
    cbInBuffer: DWORD,
    lpvOutBuffer: ?*anyopaque,
    cbOutBuffer: DWORD,
    lpcbBytesReturned: *DWORD,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?LPWSAOVERLAPPED_COMPLETION_ROUTINE,
) callconv(.winapi) c_int;

pub inline fn socketToHandle(s: SOCKET) HANDLE {
    assert(s != INVALID_SOCKET);
    return @ptrFromInt(s);
}

pub inline fn handleToSocket(h: HANDLE) SOCKET {
    return @intFromPtr(h);
}

const assert = std.debug.assert;

var wsa_state: std.atomic.Value(u8) = .init(0); // 0 = uninit, 1 = initing, 2 = ready

pub fn ensureWsaInitialized() error{Unexpected}!void {
    while (true) {
        switch (wsa_state.load(.acquire)) {
            2 => return,
            0 => if (wsa_state.cmpxchgWeak(0, 1, .acq_rel, .acquire) == null) {
                var data: WSADATA = undefined;
                if (WSAStartup(0x0202, &data) != 0) {
                    wsa_state.store(0, .release);
                    return error.Unexpected;
                }
                wsa_state.store(2, .release);
                return;
            },
            1 => std.atomic.spinLoopHint(),
            else => unreachable,
        }
    }
}
