//! Dedicated Windows `SO_EXCLUSIVEADDRUSE` Acceptor transparently backing
//! multi-Shard `netListenIp(..., .{ .reuse_address = true })` and `netAccept`.
//!
//! Why this exists (AGENTS.md §2):
//! - Windows lacks `SO_REUSEPORT` TCP load-balancing (`SO_REUSEADDR` permits port
//!   stealing rather than kernel distribution) and forbids rebinding a `SOCKET`
//!   once associated with an IOCP.
//! - The Acceptor owns one exclusive listener `SOCKET` per shared endpoint on its
//!   own IOCP, keeps `accepts_per_listener` `AcceptEx` calls in flight using raw
//!   unassociated `SOCKET`s, reserves a slot in the least-loaded subscribed Shard's
//!   bounded `Mailbox` before handoff (closing and counting refused sockets when
//!   full), and lets the target Shard associate the `SOCKET` with its own IOCP
//!   for the first and only time in `netAccept`.
//! - On Linux (where kernel `SO_REUSEPORT` distributes directly to each Shard's
//!   `epoll`), `Acceptor` compiles to a zero-sized no-op struct.

const builtin = @import("builtin");
const std = @import("std");
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

pub const Acceptor = if (builtin.os.tag == .windows) WindowsAcceptor else NoopAcceptor;

pub const AcceptedSocket = if (builtin.os.tag == .windows) struct {
    listener_id: u32,
    raw_sock: @import("win32.zig").SOCKET,
    peer_address: net.IpAddress,
} else void;

const NoopAcceptor = struct {
    pub fn init(_: *NoopAcceptor, _: Allocator, _: usize, _: u32) Allocator.Error!void {}
    pub fn deinit(_: *NoopAcceptor, _: Allocator) void {}
    pub fn bindShard(_: *NoopAcceptor, _: usize, _: ?*anyopaque, _: *const fn (?*anyopaque) void) void {}
    pub fn requestStop(_: *NoopAcceptor) void {}
    pub fn join(_: *NoopAcceptor) void {}
    pub fn isSharedToken(_: *const NoopAcceptor, _: net.Socket.Handle) ?u32 {
        return null;
    }
};

const WindowsAcceptor = struct {
    const win32 = @import("win32.zig");
    const ws2_32 = win32.ws2_32;
    const Iocp = @import("Iocp.zig");
    const MailboxMod = @import("Mailbox.zig");
    const Mailbox = MailboxMod.Mailbox;
    const Threaded = Io.Threaded;

    const max_shared_listeners = 64;
    const accepts_per_listener = 8;
    const addr_slot_bytes: win32.DWORD = @sizeOf(ws2_32.sockaddr.storage) + 16;

    pub const ShardTarget = struct {
        inbox: Mailbox(AcceptedSocket),
        active_load: std.atomic.Value(usize) = .init(0),
        notify_ctx: ?*anyopaque = null,
        notify_fn: ?*const fn (?*anyopaque) void = null,
    };

    pub const Token = struct {
        in_use: bool = false,
        shard_id: u32 = 0,
        listener_id: u32 = 0,
    };

    const AcceptSlot = struct {
        comp: Iocp.Completion,
        owner: *WindowsAcceptor,
        listener_id: u32,
        accept_sock: win32.SOCKET = win32.INVALID_SOCKET,
        addr_buf: [addr_slot_bytes * 2]u8 align(8) = undefined,
    };

    const SharedListener = struct {
        active: bool = false,
        closing: bool = false,
        requested_addr: net.IpAddress = .{ .ip4 = .loopback(0) },
        bound_addr: net.IpAddress = .{ .ip4 = .loopback(0) },
        family: c_int = 0,
        sock_type: c_int = 0,
        protocol: c_int = 0,
        listen_sock: win32.SOCKET = win32.INVALID_SOCKET,
        accept_ex: win32.LPFN_ACCEPTEX = undefined,
        subscribed_mask: u64 = 0,
        ref_count: u32 = 0,
        rr_cursor: u32 = 0,
        in_flight_accepts: u32 = 0,
        slots: [accepts_per_listener]AcceptSlot = undefined,
    };

    gpa: Allocator,
    shard_count: usize,
    shards: []ShardTarget,
    tokens: []Token,
    listeners: []SharedListener,
    iocp: Iocp,
    mutex: std.Io.Mutex = .init,
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),
    joined: bool = false,
    refused_count: std.atomic.Value(usize) = .init(0),

    inline fn kernelIo() std.Io {
        return std.Io.Threaded.global_single_threaded.io();
    }

    pub fn init(
        self: *WindowsAcceptor,
        gpa: Allocator,
        shard_count: usize,
        inbox_capacity: u32,
    ) (Allocator.Error || Iocp.InitError || error{InvalidCapacity})!void {
        assert(shard_count > 0 and shard_count <= 64);

        const shards = try gpa.alloc(ShardTarget, shard_count);
        errdefer gpa.free(shards);

        var init_shards: usize = 0;
        errdefer {
            for (shards[0..init_shards]) |*s| s.inbox.deinit(gpa, closeAcceptedSocket);
        }
        for (shards) |*s| {
            s.* = .{ .inbox = try Mailbox(AcceptedSocket).init(gpa, @max(@as(u32, 16), inbox_capacity)) };
            init_shards += 1;
        }

        const max_tokens = shard_count * max_shared_listeners;
        const tokens = try gpa.alloc(Token, max_tokens);
        errdefer gpa.free(tokens);
        @memset(tokens, .{});

        const listeners = try gpa.alloc(SharedListener, max_shared_listeners);
        errdefer gpa.free(listeners);
        for (listeners) |*l| l.* = .{};

        var iocp = try Iocp.init();
        errdefer iocp.deinit(gpa);

        self.* = .{
            .gpa = gpa,
            .shard_count = shard_count,
            .shards = shards,
            .tokens = tokens,
            .listeners = listeners,
            .iocp = iocp,
        };
    }

    pub fn deinit(self: *WindowsAcceptor, gpa: Allocator) void {
        self.join();
        for (self.shards) |*s| {
            s.inbox.deinit(gpa, closeAcceptedSocket);
        }
        self.iocp.deinit(gpa);
        gpa.free(self.listeners);
        gpa.free(self.tokens);
        gpa.free(self.shards);
        self.* = undefined;
    }

    pub fn bindShard(
        self: *WindowsAcceptor,
        shard_id: usize,
        notify_ctx: ?*anyopaque,
        notify_fn: *const fn (?*anyopaque) void,
    ) void {
        self.shards[shard_id].notify_ctx = notify_ctx;
        self.shards[shard_id].notify_fn = notify_fn;
    }

    pub fn isSharedToken(self: *const WindowsAcceptor, handle: net.Socket.Handle) ?u32 {
        const addr = @intFromPtr(handle);
        const start = @intFromPtr(self.tokens.ptr);
        const end = start + self.tokens.len * @sizeOf(Token);
        if (addr < start or addr >= end) return null;
        if ((addr - start) % @sizeOf(Token) != 0) return null;
        const idx: u32 = @intCast((addr - start) / @sizeOf(Token));
        return idx;
    }

    pub fn tokenListenerId(self: *const WindowsAcceptor, token_idx: u32) u32 {
        return self.tokens[token_idx].listener_id;
    }

    pub fn openOrJoinListener(
        self: *WindowsAcceptor,
        shard_id: usize,
        address: *const net.IpAddress,
        options: net.IpAddress.ListenOptions,
    ) net.IpAddress.ListenError!net.Socket {
        const kio = kernelIo();
        self.mutex.lockUncancelable(kio);
        defer self.mutex.unlock(kio);

        if (self.stopping.load(.acquire)) return error.NetworkDown;

        const af = Threaded.posixAddressFamily(address);
        const mode_proto = try Threaded.posixSocketModeProtocol(af, options.mode, options.protocol);

        // Match existing shared listener if already bound for this endpoint.
        var found_id: ?u32 = null;
        for (self.listeners, 0..) |*l, idx| {
            if (l.active and !l.closing and ipAddressEql(l.requested_addr, address.*) and
                l.sock_type == @as(c_int, @intCast(mode_proto[0])) and
                l.protocol == @as(c_int, @intCast(mode_proto[1])))
            {
                found_id = @intCast(idx);
                break;
            }
        }

        const listener_id: u32 = if (found_id) |id| id else blk: {
            const free_id: u32 = for (self.listeners, 0..) |*l, idx| {
                if (!l.active and l.in_flight_accepts == 0) break @intCast(idx);
            } else return error.SystemResources;

            const sock = win32.WSASocketW(
                af,
                @intCast(mode_proto[0]),
                @intCast(mode_proto[1]),
                null,
                0,
                win32.WSA_FLAG_OVERLAPPED | win32.WSA_FLAG_NO_HANDLE_INHERIT,
            );
            if (sock == win32.INVALID_SOCKET) return mapWinsockListenError(win32.WSAGetLastError());
            errdefer _ = win32.closesocket(sock);

            const excl: c_int = 1;
            if (win32.setsockopt(sock, ws2_32.SOL.SOCKET, win32.SO_EXCLUSIVEADDRUSE, @ptrCast(&excl), @sizeOf(c_int)) == win32.SOCKET_ERROR) {
                return mapWinsockListenError(win32.WSAGetLastError());
            }

            if (address.* == .ip6) {
                const v6only: c_int = 0;
                _ = win32.setsockopt(sock, win32.IPPROTO_IPV6, win32.IPV6_V6ONLY, @ptrCast(&v6only), @sizeOf(c_int));
            }

            var storage: Threaded.PosixAddress = undefined;
            const socklen = Threaded.addressToPosix(address, &storage);
            if (win32.bind(sock, &storage.any, @intCast(socklen)) == win32.SOCKET_ERROR) {
                return mapWinsockListenError(win32.WSAGetLastError());
            }

            const backlog: c_int = @intCast(@min(options.kernel_backlog, std.math.maxInt(c_int)));
            if (win32.listen(sock, backlog) == win32.SOCKET_ERROR) {
                return mapWinsockListenError(win32.WSAGetLastError());
            }

            var bound_storage: Threaded.PosixAddress = undefined;
            var bound_len: c_int = @sizeOf(Threaded.PosixAddress);
            if (win32.getsockname(sock, &bound_storage.any, &bound_len) == win32.SOCKET_ERROR) {
                return mapWinsockListenError(win32.WSAGetLastError());
            }
            const bound_addr = Threaded.addressFromPosix(&bound_storage);

            try self.iocp.associateListenHandle(win32.socketToHandle(sock), free_id);
            const accept_ex = try Iocp.resolveAcceptEx(sock);

            const l = &self.listeners[free_id];
            l.* = .{
                .active = true,
                .closing = false,
                .requested_addr = address.*,
                .bound_addr = bound_addr,
                .family = af,
                .sock_type = @intCast(mode_proto[0]),
                .protocol = @intCast(mode_proto[1]),
                .listen_sock = sock,
                .accept_ex = accept_ex,
            };

            if (self.thread == null) {
                self.thread = std.Thread.spawn(.{ .allocator = self.gpa }, acceptorLoop, .{self}) catch {
                    l.* = .{};
                    return error.SystemResources;
                };
            }

            for (&l.slots) |*slot| {
                slot.* = .{
                    .comp = .{ .callback = onAcceptComplete },
                    .owner = self,
                    .listener_id = free_id,
                };
                self.startAcceptLocked(l, slot);
            }
            break :blk free_id;
        };

        const token_idx: u32 = for (self.tokens, 0..) |*t, idx| {
            if (!t.in_use) break @intCast(idx);
        } else return error.SystemResources;

        const l = &self.listeners[listener_id];
        const bit = @as(u64, 1) << @intCast(shard_id);
        if ((l.subscribed_mask & bit) == 0) {
            l.subscribed_mask |= bit;
            l.ref_count += 1;
        }

        self.tokens[token_idx] = .{
            .in_use = true,
            .shard_id = @intCast(shard_id),
            .listener_id = listener_id,
        };

        return .{
            .handle = @ptrCast(&self.tokens[token_idx]),
            .address = l.bound_addr,
        };
    }

    pub fn closeToken(self: *WindowsAcceptor, token_idx: u32) void {
        const kio = kernelIo();
        self.mutex.lockUncancelable(kio);
        defer self.mutex.unlock(kio);

        const tok = &self.tokens[token_idx];
        if (!tok.in_use) return;
        const shard_id = tok.shard_id;
        const listener_id = tok.listener_id;
        tok.* = .{};

        const l = &self.listeners[listener_id];
        if (!l.active) return;

        // Check whether this shard still holds another token on the same listener.
        for (self.tokens) |other| {
            if (other.in_use and other.listener_id == listener_id and other.shard_id == shard_id) {
                return;
            }
        }

        const bit = @as(u64, 1) << @intCast(shard_id);
        if ((l.subscribed_mask & bit) != 0) {
            l.subscribed_mask &= ~bit;
            l.ref_count -= 1;
            if (l.ref_count == 0 and !l.closing) {
                l.closing = true;
                if (l.listen_sock != win32.INVALID_SOCKET) {
                    _ = win32.closesocket(l.listen_sock);
                    l.listen_sock = win32.INVALID_SOCKET;
                }
                if (l.in_flight_accepts == 0) {
                    l.active = false;
                }
            }
        }
    }

    pub fn drainShardInbox(
        self: *WindowsAcceptor,
        shard_id: usize,
        ctx: ?*anyopaque,
        handler: *const fn (?*anyopaque, AcceptedSocket) void,
    ) usize {
        return self.shards[shard_id].inbox.drain(ctx, handler);
    }

    pub fn isShardInboxEmptySeqCst(self: *const WindowsAcceptor, shard_id: usize) bool {
        return self.shards[shard_id].inbox.isEmptySeqCst();
    }

    pub fn noteShardSocketAdopted(self: *WindowsAcceptor, shard_id: usize) void {
        _ = self.shards[shard_id].active_load.fetchSub(1, .monotonic);
    }

    pub fn requestStop(self: *WindowsAcceptor) void {
        if (self.stopping.swap(true, .seq_cst)) return;
        const kio = kernelIo();
        self.mutex.lockUncancelable(kio);
        for (self.shards) |*s| s.inbox.close();
        for (self.listeners) |*l| {
            if (l.active and !l.closing) {
                l.closing = true;
                if (l.listen_sock != win32.INVALID_SOCKET) {
                    _ = win32.closesocket(l.listen_sock);
                    l.listen_sock = win32.INVALID_SOCKET;
                }
                if (l.in_flight_accepts == 0) {
                    l.active = false;
                }
            }
        }
        self.mutex.unlock(kio);
        self.iocp.doorbell.armBeforeSleep();
        self.iocp.notifyFromAnyThread();
    }

    pub fn join(self: *WindowsAcceptor) void {
        self.requestStop();
        const kio = kernelIo();
        self.mutex.lockUncancelable(kio);
        if (self.joined) {
            self.mutex.unlock(kio);
            return;
        }
        self.joined = true;
        const maybe_thread = self.thread;
        self.thread = null;
        self.mutex.unlock(kio);

        if (maybe_thread) |t| t.join();
    }

    fn acceptorLoop(self: *WindowsAcceptor) void {
        const kio = kernelIo();
        while (true) {
            self.iocp.doorbell.armBeforeSleep();
            const done = blk: {
                self.mutex.lockUncancelable(kio);
                defer self.mutex.unlock(kio);
                break :blk self.stopping.load(.seq_cst) and self.iocp.in_flight_count == 0;
            };
            if (done) {
                self.iocp.doorbell.disarm();
                return;
            }
            _ = self.iocp.poll(win32.INFINITE);
        }
    }

    fn startAcceptLocked(self: *WindowsAcceptor, l: *SharedListener, slot: *AcceptSlot) void {
        if (l.closing or l.listen_sock == win32.INVALID_SOCKET) return;

        // Create raw unassociated SOCKET: target business Shard will associate
        // it with its own IOCP for the first and only time upon adoption!
        const accept_sock = win32.WSASocketW(
            l.family,
            l.sock_type,
            l.protocol,
            null,
            0,
            win32.WSA_FLAG_OVERLAPPED | win32.WSA_FLAG_NO_HANDLE_INHERIT,
        );
        if (accept_sock == win32.INVALID_SOCKET) return;

        slot.accept_sock = accept_sock;
        slot.comp = .{ .callback = onAcceptComplete };

        var bytes_recvd: win32.DWORD = 0;
        const ok = l.accept_ex(
            l.listen_sock,
            accept_sock,
            &slot.addr_buf,
            0,
            addr_slot_bytes,
            addr_slot_bytes,
            &bytes_recvd,
            &slot.comp.overlapped,
        );
        if (!ok.toBool()) {
            const err = win32.WSAGetLastError();
            if (err != win32.WSA_IO_PENDING) {
                _ = win32.closesocket(accept_sock);
                slot.accept_sock = win32.INVALID_SOCKET;
                return;
            }
        }
        // Without FILE_SKIP_COMPLETION_PORT_ON_SUCCESS on `listen_sock`, both
        // immediate success and WSA_IO_PENDING post an IOCP packet uniformly.
        self.iocp.recordPending(&slot.comp);
        l.in_flight_accepts += 1;
    }

    fn onAcceptComplete(comp: *Iocp.Completion) void {
        const slot: *AcceptSlot = @fieldParentPtr("comp", comp);
        const self = slot.owner;
        var chosen_shard: ?usize = null;
        var notify_ctx: ?*anyopaque = null;
        var notify_fn: ?*const fn (?*anyopaque) void = null;
        var handoff: AcceptedSocket = undefined;
        var raw_to_close: win32.SOCKET = win32.INVALID_SOCKET;

        const kio = kernelIo();
        {
            self.mutex.lockUncancelable(kio);
            const l = &self.listeners[slot.listener_id];
            assert(l.in_flight_accepts > 0);
            l.in_flight_accepts -= 1;

            const raw_sock = slot.accept_sock;
            slot.accept_sock = win32.INVALID_SOCKET;

            if (!l.closing and comp.nt_status == .SUCCESS and raw_sock != win32.INVALID_SOCKET and l.subscribed_mask != 0) {
                var listen_copy = l.listen_sock;
                if (win32.setsockopt(
                    raw_sock,
                    ws2_32.SOL.SOCKET,
                    ws2_32.SO.UPDATE_ACCEPT_CONTEXT,
                    @ptrCast(&listen_copy),
                    @sizeOf(win32.SOCKET),
                ) == 0) {
                    var peer_storage: Threaded.PosixAddress = undefined;
                    var peer_len: c_int = @sizeOf(Threaded.PosixAddress);
                    const peer_addr = if (win32.getpeername(raw_sock, &peer_storage.any, &peer_len) == 0)
                        Threaded.addressFromPosix(&peer_storage)
                    else
                        l.bound_addr;

                    if (self.pickShardLocked(l)) |sid| {
                        handoff = .{
                            .listener_id = slot.listener_id,
                            .raw_sock = raw_sock,
                            .peer_address = peer_addr,
                        };
                        chosen_shard = sid;
                    } else {
                        raw_to_close = raw_sock;
                    }
                } else {
                    raw_to_close = raw_sock;
                }
            } else {
                raw_to_close = raw_sock;
            }

            if (!l.closing and !self.stopping.load(.acquire)) {
                self.startAcceptLocked(l, slot);
            } else if (l.in_flight_accepts == 0) {
                l.active = false;
            }
            if (chosen_shard) |sid| {
                notify_ctx = self.shards[sid].notify_ctx;
                notify_fn = self.shards[sid].notify_fn;
            }
            self.mutex.unlock(kio);
        }

        // Socket closure, Mailbox admission, and Shard doorbell happen OUTSIDE `self.mutex`.
        if (raw_to_close != win32.INVALID_SOCKET) {
            _ = win32.closesocket(raw_to_close);
        }
        if (chosen_shard) |sid| {
            _ = self.shards[sid].active_load.fetchAdd(1, .monotonic);
            switch (self.shards[sid].inbox.tryPost(handoff)) {
                .accepted => {
                    if (notify_fn) |nf| nf(notify_ctx);
                },
                .queue_full, .closed => |rejected| {
                    _ = self.shards[sid].active_load.fetchSub(1, .monotonic);
                    _ = self.refused_count.fetchAdd(1, .monotonic);
                    _ = win32.closesocket(rejected.raw_sock);
                },
            }
        }
    }

    fn pickShardLocked(self: *WindowsAcceptor, l: *SharedListener) ?usize {
        if (l.subscribed_mask == 0) return null;
        var best_shard: ?usize = null;
        var best_load: usize = std.math.maxInt(usize);
        for (0..self.shard_count) |offset| {
            const sid = (@as(usize, l.rr_cursor) + offset) % self.shard_count;
            const bit = @as(u64, 1) << @intCast(sid);
            if ((l.subscribed_mask & bit) == 0) continue;
            const load = self.shards[sid].active_load.load(.monotonic);
            if (load < best_load) {
                best_load = load;
                best_shard = sid;
            }
        }
        if (best_shard) |sid| {
            l.rr_cursor = @intCast((sid + 1) % self.shard_count);
        }
        return best_shard;
    }

    fn closeAcceptedSocket(item: *AcceptedSocket) void {
        if (item.raw_sock != win32.INVALID_SOCKET) {
            _ = win32.closesocket(item.raw_sock);
            item.raw_sock = win32.INVALID_SOCKET;
        }
    }

    fn ipAddressEql(a: net.IpAddress, b: net.IpAddress) bool {
        return std.meta.eql(a, b);
    }

    fn mapWinsockListenError(code: c_int) net.IpAddress.ListenError {
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
};
