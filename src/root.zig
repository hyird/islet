//! Pure shared-nothing `std.Io` backend for Zig (`epoll` on Linux, `IOCP` on Windows).
//!
//! Public contract (AGENTS.md §1):
//! Only lifecycle entry points (`Runtime`, `Shard`) and `Io` (`std.Io`) are
//! exposed. All internal primitives (`SpscRing`, `Mailbox`, `ShardMesh`,
//! `TimerHeap`, `FutexTable`, `BlockingPool`, `Epoll`, `Iocp`, `fiber`,
//! `Acceptor`) remain strictly private.

const builtin = @import("builtin");
const std = @import("std");

pub const Io = std.Io;
pub const Shard = @import("Shard.zig");
pub const Runtime = @import("Runtime.zig");

comptime {
    switch (builtin.os.tag) {
        .linux, .windows => {},
        else => |tag| @compileError("islet supports only Linux (epoll) and Windows (IOCP), got: " ++ @tagName(tag)),
    }
}

test {
    // Pull in internal unit tests without exposing internal submodules publicly.
    _ = @import("time.zig");
    _ = @import("TimerHeap.zig");
    _ = @import("SpscRing.zig");
    _ = @import("Mailbox.zig");
    _ = @import("fiber.zig");
    _ = @import("BlockingPool.zig");
    _ = @import("FutexTable.zig");
    _ = @import("ShardMesh.zig");
    if (builtin.os.tag == .linux) {
        _ = @import("Epoll.zig");
    }
}

test "public namespace exposes only Io, Shard, and Runtime" {
    const decl_names = @typeInfo(@This()).@"struct".decl_names;
    try std.testing.expectEqual(@as(usize, 3), decl_names.len);
}

test "Shard: Future async/concurrent, Group, Select, sleep, cancel, and CancelProtection" {
    var shard: Shard = undefined;
    try shard.init(std.testing.allocator, .{});
    defer shard.deinit();

    const io = shard.io();

    // 1. Concurrent Future computation + sleep
    const Work = struct {
        fn compute(inner_io: Io, delta: u32) Io.Cancelable!u32 {
            try inner_io.sleep(.fromMilliseconds(2), .awake);
            return delta + 40;
        }

        fn slowTask(inner_io: Io, cleaned_up: *bool) Io.Cancelable!u32 {
            errdefer {
                // Verify CancelProtection(.blocked) allows cleanup sleep without secondary Canceled!
                const prev = inner_io.swapCancelProtection(.blocked);
                defer _ = inner_io.swapCancelProtection(prev);
                inner_io.sleep(.fromMilliseconds(1), .awake) catch unreachable;
                cleaned_up.* = true;
            }
            try inner_io.sleep(.fromSeconds(10), .awake);
            return 999;
        }
    };

    var f1 = try io.concurrent(Work.compute, .{ io, 2 });
    try std.testing.expectEqual(@as(u32, 42), try f1.await(io));

    // 2. Future cancellation + CancelProtection cleanup
    var cleaned_up = false;
    var f_cancel = try io.concurrent(Work.slowTask, .{ io, &cleaned_up });
    try io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expectError(error.Canceled, f_cancel.cancel(io));
    try std.testing.expect(cleaned_up);

    // 3. Io.Group & Io.Select structured concurrency
    const Choice = union(enum) {
        fast: u32,
        slow: u32,
    };
    var select_buf: [2]Choice = undefined;
    var sel: Io.Select(Choice) = .init(io, &select_buf);
    try sel.concurrent(.slow, struct {
        fn run(s_io: Io) u32 {
            s_io.sleep(.fromSeconds(5), .awake) catch return 0;
            return 2;
        }
    }.run, .{io});
    try sel.concurrent(.fast, struct {
        fn run(s_io: Io) u32 {
            s_io.sleep(.fromMilliseconds(1), .awake) catch return 0;
            return 77;
        }
    }.run, .{io});

    const winner = try sel.await();
    try std.testing.expectEqual(Choice{ .fast = 77 }, winner);
    sel.cancelDiscard();
}

test "Shard E2E: TCP loopback segmented stream, half-close, UDP datagrams, and Io.Batch" {
    var shard: Shard = undefined;
    try shard.init(std.testing.allocator, .{});
    defer shard.deinit();
    const io = shard.io();

    // TCP Loopback Server + Client on Fibers
    const lo: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try lo.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const EchoServer = struct {
        fn serveOne(s_io: Io, srv: *Io.net.Server) !usize {
            const conn = try srv.accept(s_io);
            defer conn.close(s_io);

            var rbuf: [64]u8 = undefined;
            var wbuf: [64]u8 = undefined;
            var reader = conn.reader(s_io, &rbuf);
            var writer = conn.writer(s_io, &wbuf);

            var total: usize = 0;
            while (true) {
                var chunk: [17]u8 = undefined;
                var vec: [1][]u8 = .{&chunk};
                const n = reader.interface.readVec(&vec) catch |err| switch (err) {
                    error.EndOfStream => break,
                    error.ReadFailed => return reader.err.?,
                };
                try writer.interface.writeAll(chunk[0..n]);
                total += n;
            }
            try writer.interface.flush();
            try conn.shutdown(s_io, .send);
            return total;
        }
    };

    var srv_future = try io.concurrent(EchoServer.serveOne, .{ io, &server });
    errdefer _ = srv_future.cancel(io) catch {};

    const client = try server.socket.address.connect(io, .{ .mode = .stream });
    defer client.close(io);

    const payload = "shared-nothing-epoll-iocp-segmented-payload-0123456789abcdef";
    var cwbuf: [32]u8 = undefined;
    var cw = client.writer(io, &cwbuf);
    // Segmented writes + flush + half-close (.send)
    try cw.interface.writeAll(payload[0..19]);
    try cw.interface.flush();
    try cw.interface.writeAll(payload[19..]);
    try cw.interface.flush();
    try client.shutdown(io, .send);

    var crbuf: [32]u8 = undefined;
    var cr = client.reader(io, &crbuf);
    var echoed: [payload.len]u8 = undefined;
    try cr.interface.readSliceAll(&echoed);
    try std.testing.expectEqualStrings(payload, &echoed);
    try std.testing.expectEqual(payload.len, try srv_future.await(io));

    // UDP Loopback + Io.Batch concurrent receive
    const udp_a = try lo.bind(io, .{ .mode = .dgram });
    defer udp_a.close(io);
    const udp_b = try lo.bind(io, .{ .mode = .dgram });
    defer udp_b.close(io);

    var storage: [1]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);

    var in_msg: [1]Io.net.IncomingMessage = .{.init};
    var dgram_buf: [64]u8 = undefined;
    const op_idx = batch.add(.{ .net_receive = .{
        .socket_handle = udp_a.handle,
        .message_buffer = &in_msg,
        .data_buffer = &dgram_buf,
        .flags = .{},
    } });

    try udp_b.send(io, &udp_a.address, "ping-dgram");
    try batch.awaitConcurrent(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    const comp = batch.next().?;
    try std.testing.expectEqual(op_idx, comp.index);
    const maybe_err, const msg_count = comp.result.net_receive;
    try std.testing.expectEqual(@as(?Io.net.Socket.ReceiveError, null), maybe_err);
    try std.testing.expectEqual(@as(usize, 1), msg_count);
    try std.testing.expectEqualStrings("ping-dgram", in_msg[0].data);
}

test "Shard E2E: File/Dir operations, MemoryMap, random, and HostName localhost lookup" {
    var shard: Shard = undefined;
    try shard.init(std.testing.allocator, .{});
    defer shard.deinit();
    const io = shard.io();

    // Ensure test temp directory stays inside workspace `.zig-cache/test-tmp` (NEVER `/tmp`!).
    const cwd = Io.Dir.cwd();
    try cwd.createDirPath(io, ".zig-cache/test-tmp");
    var tmp_dir = try cwd.openDir(io, ".zig-cache/test-tmp", .{});
    defer {
        tmp_dir.close(io);
        cwd.deleteDir(io, ".zig-cache/test-tmp") catch {};
    }

    var file = try tmp_dir.createFile(io, "sample.bin", .{ .read = true, .truncate = true });
    defer {
        file.close(io);
        tmp_dir.deleteFile(io, "sample.bin") catch {};
    }

    const written = try file.writePositional(io, &.{ "hello-", "islet" }, 0);
    try std.testing.expectEqual(@as(usize, 11), written);
    try file.sync(io);
    try std.testing.expectEqual(@as(u64, 11), try file.length(io));

    var rbuf: [11]u8 = undefined;
    const n_read = try file.readPositional(io, &.{&rbuf}, 0);
    try std.testing.expectEqual(@as(usize, 11), n_read);
    try std.testing.expectEqualStrings("hello-islet", &rbuf);

    var mm = try Io.File.MemoryMap.create(io, file, .{ .len = 11 });
    defer mm.destroy(io);
    try mm.read(io);
    try std.testing.expectEqualStrings("hello-islet", mm.memory);

    // CSPRNG & localhost lookup
    var rnd: [16]u8 = @splat(0);
    io.random(&rnd);
    var qbuf: [8]Io.net.HostName.LookupResult = undefined;
    var q: Io.Queue(Io.net.HostName.LookupResult) = .init(&qbuf);
    const hn = try Io.net.HostName.init("localhost");
    try hn.lookup(io, &q, .{ .port = 8080 });
    const first = try q.getOne(io);
    try std.testing.expect(first == .address);
}

test "Runtime E2E: multi-Shard shared-nothing execution, cross-Shard Io.Queue via ShardMesh, and SO_REUSEPORT TCP" {
    const gpa = std.testing.allocator;

    // 1. Cross-Shard producer/consumer coordination purely via `std.Io.Queue` & `std.Io.Event`
    var rt: Runtime = undefined;
    try rt.init(gpa, .{ .shard_count = 4, .mesh_ring_capacity = 64 });
    defer rt.deinit();

    const Shared = struct {
        q_buf: [16]u64 = undefined,
        queue: Io.Queue(u64) = undefined,
        ready: Io.Event = .unset,
        sum: std.atomic.Value(u64) = .init(0),
    };
    var shared: Shared = .{};
    shared.queue = .init(&shared.q_buf);

    try rt.run(struct {
        fn shardEntry(io: Io, shard_id: usize, st: *Shared) !void {
            if (shard_id == 0) {
                st.ready.set(io);
                // Shard 0 consumes 300 messages produced by Shards 1, 2, 3
                var total: u64 = 0;
                for (0..300) |_| {
                    total += try st.queue.getOne(io);
                }
                st.sum.store(total, .release);
            } else {
                try st.ready.wait(io);
                for (0..100) |i| {
                    try st.queue.putOne(io, @as(u64, shard_id) * 1000 + i);
                }
            }
        }
    }.shardEntry, .{&shared});

    // Sum over shard_id in {1,2,3}, i in 0..99:
    // (1000 + 2000 + 3000) * 100 + 3 * (99 * 100 / 2) = 600_000 + 14_850 = 614_850
    try std.testing.expectEqual(@as(u64, 614_850), shared.sum.load(.acquire));

    // 2. Two-phase `start` / `requestStop` / `join` with multi-Shard `reuse_address` TCP listeners
    var srv_rt: Runtime = undefined;
    try srv_rt.init(gpa, .{ .shard_count = 2 });
    defer srv_rt.deinit();

    // Reserve an ephemeral port first or let Shard 0 publish the bound port via `Io.Event`.
    const ServerShared = struct {
        port_ready: Io.Event = .unset,
        stop_event: Io.Event = .unset,
        wake_io: Io = undefined,
        all_listening: std.atomic.Value(usize) = .init(0),
        bound_addr: Io.net.IpAddress = .{ .ip4 = .loopback(0) },
        handled: std.atomic.Value(usize) = .init(0),
    };
    var srv_st: ServerShared = .{};

    try srv_rt.start(struct {
        fn worker(shard: *Shard, st: *ServerShared) !void {
            const io = shard.io();
            var server = if (shard.index() == 0) blk: {
                const lo: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
                const s = try lo.listen(io, .{ .reuse_address = true });
                st.bound_addr = s.socket.address;
                st.wake_io = io;
                st.port_ready.set(io);
                break :blk s;
            } else blk: {
                try st.port_ready.wait(io);
                break :blk try st.bound_addr.listen(io, .{ .reuse_address = true });
            };
            defer server.deinit(io);

            _ = st.all_listening.fetchAdd(1, .release);

            const AcceptOrStop = union(enum) {
                accepted: Io.net.Server.AcceptError!Io.net.Stream,
                stopped: Io.Cancelable!void,
            };

            while (!st.stop_event.isSet()) {
                var sel_buf: [2]AcceptOrStop = undefined;
                var sel: Io.Select(AcceptOrStop) = .init(io, &sel_buf);
                try sel.concurrent(.accepted, Io.net.Server.accept, .{ &server, io });
                try sel.concurrent(.stopped, Io.Event.wait, .{ &st.stop_event, io });

                const winner = try sel.await();
                switch (winner) {
                    .accepted => |conn_res| {
                        while (sel.cancel()) |extra| {
                            if (extra == .accepted) {
                                if (extra.accepted) |c| c.close(io) else |_| {}
                            }
                        }
                        const conn = conn_res catch |err| switch (err) {
                            error.Canceled, error.SocketNotListening => break,
                            else => return err,
                        };
                        defer conn.close(io);
                        var wbuf: [16]u8 = undefined;
                        var w = conn.writer(io, &wbuf);
                        try w.interface.writeAll("ok");
                        try w.interface.flush();
                        _ = st.handled.fetchAdd(1, .acq_rel);
                    },
                    .stopped => {
                        while (sel.cancel()) |extra| {
                            if (extra == .accepted) {
                                if (extra.accepted) |c| c.close(io) else |_| {}
                            }
                        }
                        break;
                    },
                }
            }
        }
    }.worker, .{&srv_st});

    // Client shard connects 4 times to the multi-shard server, then signals stop + joins.
    {
        var cli_shard: Shard = undefined;
        try cli_shard.init(gpa, .{});
        defer cli_shard.deinit();
        const cio = cli_shard.io();

        while (srv_st.all_listening.load(.acquire) < 2) {
            try cio.sleep(.fromMilliseconds(1), .awake);
        }

        for (0..4) |_| {
            const stream = try srv_st.bound_addr.connect(cio, .{ .mode = .stream });
            defer stream.close(cio);
            var rbuf: [8]u8 = undefined;
            var r = stream.reader(cio, &rbuf);
            var out: [2]u8 = undefined;
            try r.interface.readSliceAll(&out);
            try std.testing.expectEqualStrings("ok", &out);
        }
        // Cross-thread `futexWake` (`Io.Event.set`) into `srv_rt`'s FutexTable!
        srv_st.stop_event.set(srv_st.wake_io);
    }

    srv_rt.requestStop();
    srv_rt.join();
    try std.testing.expectEqual(@as(usize, 4), srv_st.handled.load(.acquire));
}

test "Shard & Runtime E2E: ConnectionRefused, recancel, Batch timeout, socketpair, child process pipes, and SPSC ring saturation fallback" {
    const gpa = std.testing.allocator;

    {
        var shard: Shard = undefined;
        try shard.init(gpa, .{});
        defer shard.deinit();
        const io = shard.io();

        // 1. `recancel()` re-arms single-shot cancellation acknowledgment
        var recancel_observed = false;
        var f_rec = try io.concurrent(struct {
            fn run(inner_io: Io, flag: *bool) Io.Cancelable!void {
                inner_io.sleep(.fromSeconds(5), .awake) catch |err| switch (err) {
                    error.Canceled => {
                        // First Canceled acknowledged -> subsequent checkCancel passes until recancel()!
                        try inner_io.checkCancel();
                        inner_io.recancel();
                        inner_io.checkCancel() catch |err2| switch (err2) {
                            error.Canceled => {
                                flag.* = true;
                                return error.Canceled;
                            },
                        };
                        unreachable;
                    },
                };
            }
        }.run, .{ io, &recancel_observed });
        try io.sleep(.fromMilliseconds(1), .awake);
        try std.testing.expectError(error.Canceled, f_rec.cancel(io));
        try std.testing.expect(recancel_observed);

        // 2. TCP ConnectionRefused on closed ephemeral port
        const lo: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
        var closed_addr: Io.net.IpAddress = undefined;
        {
            var tmp_srv = try lo.listen(io, .{ .reuse_address = true });
            closed_addr = tmp_srv.socket.address;
            tmp_srv.deinit(io);
        }
        try std.testing.expectError(error.ConnectionRefused, closed_addr.connect(io, .{ .mode = .stream }));

        // 3. Io.Batch awaitConcurrent Timeout + clean cancel
        const udp_idle = try lo.bind(io, .{ .mode = .dgram });
        defer udp_idle.close(io);
        var storage: [1]Io.Operation.Storage = undefined;
        var batch: Io.Batch = .init(&storage);
        defer batch.cancel(io);

        var in_msg: [1]Io.net.IncomingMessage = .{.init};
        var dgram_buf: [32]u8 = undefined;
        _ = batch.add(.{ .net_receive = .{
            .socket_handle = udp_idle.handle,
            .message_buffer = &in_msg,
            .data_buffer = &dgram_buf,
            .flags = .{},
        } });
        try std.testing.expectError(
            error.Timeout,
            batch.awaitConcurrent(io, .{ .duration = .{ .raw = .fromMilliseconds(5), .clock = .awake } }),
        );
        batch.cancel(io);

        // 4. Linux socketpair + child process spawn / non-blocking pipe MultiReader / pidfd wait
        if (builtin.os.tag == .linux) {
            const pair = try Io.net.Socket.createPair(io, .{ .mode = .stream });
            var s0: Io.net.Stream = .{ .socket = pair[0] };
            var s1: Io.net.Stream = .{ .socket = pair[1] };
            defer s0.close(io);
            defer s1.close(io);

            var wb: [16]u8 = undefined;
            var rb: [16]u8 = undefined;
            var w = s0.writer(io, &wb);
            var r = s1.reader(io, &rb);
            try w.interface.writeAll("pair-ok");
            try w.interface.flush();
            var got: [7]u8 = undefined;
            try r.interface.readSliceAll(&got);
            try std.testing.expectEqualStrings("pair-ok", &got);

            const res = try std.process.run(gpa, io, .{
                .argv = &.{ "/bin/echo", "-n", "child-pipe-ok" },
            });
            defer gpa.free(res.stdout);
            defer gpa.free(res.stderr);
            try std.testing.expectEqual(std.process.Child.Term{ .exited = 0 }, res.term);
            try std.testing.expectEqualStrings("child-pipe-ok", res.stdout);
        }
    }

    // 5. Tiny `mesh_ring_capacity = 2` saturation fallback to `ResumeLane` + `Runtime.run` error propagation
    {
        var rt: Runtime = undefined;
        try rt.init(gpa, .{ .shard_count = 3, .mesh_ring_capacity = 2 });
        defer rt.deinit();

        const FloodState = struct {
            gate: Io.Event = .unset,
            parked: std.atomic.Value(usize) = .init(0),
            woken: std.atomic.Value(usize) = .init(0),
        };
        var fst: FloodState = .{};

        try rt.run(struct {
            fn entry(io: Io, shard_id: usize, st: *FloodState) !void {
                if (shard_id == 0) {
                    // Spawn 8 fibers on Shard 0 all waiting on `st.gate` (> mesh_ring_capacity = 2!)
                    var grp: Io.Group = .init;
                    defer grp.cancel(io);
                    for (0..8) |_| {
                        try grp.concurrent(io, struct {
                            fn waiter(w_io: Io, s: *FloodState) Io.Cancelable!void {
                                _ = s.parked.fetchAdd(1, .acq_rel);
                                try s.gate.wait(w_io);
                                _ = s.woken.fetchAdd(1, .acq_rel);
                            }
                        }.waiter, .{ io, st });
                    }
                    try grp.await(io);
                } else if (shard_id == 1) {
                    while (st.parked.load(.acquire) < 8) {
                        try io.sleep(.fromMilliseconds(1), .awake);
                    }
                    try io.sleep(.fromMilliseconds(2), .awake);
                    // Wakes all 8 waiters on Shard 0: 2 via SpscRing, remaining 6 via ResumeLane!
                    st.gate.set(io);
                }
            }
        }.entry, .{&fst});
        try std.testing.expectEqual(@as(usize, 8), fst.woken.load(.acquire));

        var err_rt: Runtime = undefined;
        try err_rt.init(gpa, .{ .shard_count = 2 });
        defer err_rt.deinit();
        const CustomErr = error{SyntheticShardFailure};
        try std.testing.expectError(CustomErr.SyntheticShardFailure, err_rt.run(struct {
            fn failOnOne(_: Io, idx: usize) CustomErr!void {
                if (idx == 1) return error.SyntheticShardFailure;
            }
        }.failOnOne, .{}));
    }
}

test "Refactor regressions: zero-heap steady-state Io.Batch and LIFO ping-pong anti-starvation" {
    // Counting/gating allocator proving Linux `Io.Batch` allocates 0 bytes on the heap in steady state!
    const GuardedAllocator = struct {
        backing: std.mem.Allocator,
        forbid_alloc: bool = false,
        alloc_calls_while_forbidden: usize = 0,

        fn allocator(self: *@This()) std.mem.Allocator {
            return .{
                .ptr = self,
                .vtable = &.{
                    .alloc = alloc,
                    .resize = resize,
                    .remap = remap,
                    .free = free,
                },
            };
        }

        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.forbid_alloc) {
                self.alloc_calls_while_forbidden += 1;
                return null;
            }
            return self.backing.rawAlloc(len, alignment, ret_addr);
        }

        fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.forbid_alloc and new_len > memory.len) {
                self.alloc_calls_while_forbidden += 1;
                return false;
            }
            return self.backing.rawResize(memory, alignment, new_len, ret_addr);
        }

        fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.forbid_alloc and new_len > memory.len) {
                self.alloc_calls_while_forbidden += 1;
                return null;
            }
            return self.backing.rawRemap(memory, alignment, new_len, ret_addr);
        }

        fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.backing.rawFree(memory, alignment, ret_addr);
        }
    };

    var guarded: GuardedAllocator = .{ .backing = std.testing.allocator };
    var shard: Shard = undefined;
    try shard.init(guarded.allocator(), .{});
    defer shard.deinit();
    const io = shard.io();

    const lo: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    const udp_rx = try lo.bind(io, .{ .mode = .dgram });
    defer udp_rx.close(io);
    const udp_tx = try lo.bind(io, .{ .mode = .dgram });
    defer udp_tx.close(io);

    // Warm fd slot table once on `udp_rx` before freezing heap allocations.
    {
        var warm_st: [1]Io.Operation.Storage = undefined;
        var warm_batch: Io.Batch = .init(&warm_st);
        defer warm_batch.cancel(io);
        var warm_msg: [1]Io.net.IncomingMessage = .{.init};
        var warm_buf: [16]u8 = undefined;
        _ = warm_batch.add(.{ .net_receive = .{
            .socket_handle = udp_rx.handle,
            .message_buffer = &warm_msg,
            .data_buffer = &warm_buf,
            .flags = .{},
        } });
        // Park once before sending so `Epoll.slots` pre-grows its fd slot table on warmup.
        try std.testing.expectError(
            error.Timeout,
            warm_batch.awaitConcurrent(io, .{ .duration = .{ .raw = .zero, .clock = .awake } }),
        );
        try udp_tx.send(io, &udp_rx.address, "warm");
        try warm_batch.awaitConcurrent(io, .none);
        _ = warm_batch.next().?;
    }

    if (builtin.os.tag == .linux) {
        // Freeze heap allocations: `net_receive` must park in `Epoll` (`EAGAIN` first!) and complete with 0 allocs!
        var high_socks: [16]Io.net.Socket = undefined;
        var opened_high: usize = 0;
        defer for (high_socks[0..opened_high]) |*s| s.close(io);
        while (opened_high < high_socks.len) : (opened_high += 1) {
            high_socks[opened_high] = try lo.bind(io, .{ .mode = .dgram });
        }

        guarded.forbid_alloc = true;
        defer guarded.forbid_alloc = false;

        var st: [1]Io.Operation.Storage = undefined;
        var batch: Io.Batch = .init(&st);
        defer batch.cancel(io);

        var msg: [1]Io.net.IncomingMessage = .{.init};
        var buf: [32]u8 = undefined;
        _ = batch.add(.{ .net_receive = .{
            .socket_handle = udp_rx.handle,
            .message_buffer = &msg,
            .data_buffer = &buf,
            .flags = .{},
        } });
        // Submit BEFORE sending so `submitBatch` hits `.would_block` and parks `InlineBatchSlot` in `Epoll`!
        try std.testing.expectError(
            error.Timeout,
            batch.awaitConcurrent(io, .{ .duration = .{ .raw = .zero, .clock = .awake } }),
        );
        try udp_tx.send(io, &udp_rx.address, "zero-alloc-ok");
        try batch.awaitConcurrent(io, .none);
        const comp = batch.next().?;
        try std.testing.expectEqual(@as(?Io.net.Socket.ReceiveError, null), comp.result.net_receive[0]);
        try std.testing.expectEqualStrings("zero-alloc-ok", msg[0].data);
        try std.testing.expectEqual(@as(usize, 0), guarded.alloc_calls_while_forbidden);

        // Also verify `submitBatch` OOM rollback restores `.submission` cleanly for `batch.cancel`:
        var oom_st: [1]Io.Operation.Storage = undefined;
        var oom_batch: Io.Batch = .init(&oom_st);
        defer oom_batch.cancel(io);
        var oom_msg: [1]Io.net.IncomingMessage = .{.init};
        var oom_buf: [16]u8 = undefined;
        _ = oom_batch.add(.{ .net_receive = .{
            .socket_handle = high_socks[high_socks.len - 1].handle,
            .message_buffer = &oom_msg,
            .data_buffer = &oom_buf,
            .flags = .{},
        } });
        try std.testing.expectError(
            error.ConcurrencyUnavailable,
            oom_batch.awaitConcurrent(io, .{ .duration = .{ .raw = .zero, .clock = .awake } }),
        );
    }

    // Verify bounded `lifo_slot` (`max_lifo_streak = 3`) does NOT starve queued FIFO tasks during tight ping-pong!
    const PingPong = struct {
        q_ab_buf: [1]u32 = undefined,
        q_ba_buf: [1]u32 = undefined,
        q_ab: Io.Queue(u32) = undefined,
        q_ba: Io.Queue(u32) = undefined,
        fifo_progress: usize = 0,
    };
    var pp: PingPong = .{};
    pp.q_ab = .init(&pp.q_ab_buf);
    pp.q_ba = .init(&pp.q_ba_buf);

    var grp: Io.Group = .init;
    defer grp.cancel(io);

    try grp.concurrent(io, struct {
        fn responder(g_io: Io, s: *PingPong) Io.Cancelable!void {
            for (0..16) |_| {
                const v = s.q_ab.getOne(g_io) catch return;
                s.q_ba.putOne(g_io, v + 1) catch return;
            }
        }
    }.responder, .{ io, &pp });

    try grp.concurrent(io, struct {
        fn backgroundFifo(g_io: Io, s: *PingPong) Io.Cancelable!void {
            // Wakes via immediate 0ns sleep / futex so it sits in FIFO ready_queue alongside ping-pong
            try g_io.sleep(.zero, .awake);
            s.fifo_progress += 1;
        }
    }.backgroundFifo, .{ io, &pp });

    for (0..16) |i| {
        try pp.q_ab.putOne(io, @intCast(i));
        const echoed = try pp.q_ba.getOne(io);
        try std.testing.expectEqual(@as(u32, @intCast(i + 1)), echoed);
    }
    try grp.await(io);
    try std.testing.expectEqual(@as(usize, 1), pp.fifo_progress);
}

test "Round 2 E2E: nested Future.await cancel propagation, reentrant stderr, zero-copy sendfile/copy_file_range, and batched sendmmsg" {
    const gpa = std.testing.allocator;
    var shard: Shard = undefined;
    try shard.init(gpa, .{
        .blocking_threads = 1,
        .blocking_queue_capacity = 2,
    });
    defer shard.deinit();
    const io = shard.io();

    // 1. Nested `Future.await` parent->child cancellation propagation (no deadlock + single-ack contract):
    {
        const Nested = struct {
            inner_started: Io.Event = .unset,
            never_event: Io.Event = .unset,
            outer_saw_extra_cancel: bool = false,

            fn innerTask(task_io: Io, st: *@This()) Io.Cancelable!u32 {
                st.inner_started.set(task_io);
                try st.never_event.wait(task_io);
                return 7;
            }

            fn outerTask(task_io: Io, st: *@This()) !u32 {
                var child = try task_io.concurrent(innerTask, .{ task_io, st });
                const child_res = child.await(task_io);
                // Because `innerTask` consumed the propagated cancel, `outerTask` must NOT
                // observe a duplicate second `error.Canceled` at its own checkCancel!
                task_io.checkCancel() catch {
                    st.outer_saw_extra_cancel = true;
                };
                return child_res;
            }
        };

        var nst: Nested = .{};
        var outer = try io.concurrent(Nested.outerTask, .{ io, &nst });
        try nst.inner_started.wait(io);
        try std.testing.expectError(error.Canceled, outer.cancel(io));
        try std.testing.expect(!nst.outer_saw_extra_cancel);

        // Conversely, if `child` finished WITHOUT consuming cancel, `parent` MUST retain its cancel:
        const Unconsumed = struct {
            gate: Io.Event = .unset,
            ready: Io.Event = .unset,

            fn fastChild(_: Io) u32 {
                return 42;
            }

            fn parentTask(task_io: Io, st: *@This()) !u32 {
                var child = try task_io.concurrent(fastChild, .{task_io});
                // Let `fastChild` finish before `parentTask` is canceled, then test `await` while canceled:
                try task_io.sleep(.fromMilliseconds(1), .awake);
                st.ready.set(task_io);
                // Wait in `.blocked` until outer sets cancel, then restore `.unblocked` and call `child.await`:
                {
                    const prev = task_io.swapCancelProtection(.blocked);
                    defer _ = task_io.swapCancelProtection(prev);
                    st.gate.waitUncancelable(task_io);
                }
                const v = child.await(task_io);
                try std.testing.expectEqual(@as(u32, 42), v);
                // Cancel was NOT consumed by `fastChild`, so `checkCancel` MUST fire here!
                try task_io.checkCancel();
                return v;
            }
        };
        var ust: Unconsumed = .{};
        var p_fut = try io.concurrent(Unconsumed.parentTask, .{ io, &ust });
        try ust.ready.wait(io);
        // Request cancel on `p_fut`: first wake `gate` inside `cancel`? Set `gate` right after queueing wake!
        var releaser = try io.concurrent(struct {
            fn run(t_io: Io, g: *Io.Event) void {
                t_io.sleep(.fromMilliseconds(1), .awake) catch {};
                g.set(t_io);
            }
        }.run, .{ io, &ust.gate });
        defer _ = releaser.await(io);
        try std.testing.expectError(error.Canceled, p_fut.cancel(io));
    }

    // 2. Reentrant `lockStderr` + `tryLockStderr` + `unlockStderr` buffer hygiene:
    {
        var outer_buf: [64]u8 = undefined;
        var inner_buf: [32]u8 = undefined;
        const ls1 = try io.lockStderr(&outer_buf, null);
        try std.testing.expectEqual(@as(usize, 64), ls1.file_writer.interface.buffer.len);
        {
            const ls2 = (try io.tryLockStderr(&inner_buf, .no_color)).?;
            try std.testing.expectEqual(Io.Terminal.Mode.no_color, ls2.terminal_mode);
            try std.testing.expectEqual(@as(usize, 32), ls2.file_writer.interface.buffer.len);
            io.unlockStderr();
            // Inner `unlockStderr` must clear dangling `inner_buf` slice immediately!
            try std.testing.expectEqual(@as(usize, 0), ls1.file_writer.interface.buffer.len);
        }
        io.unlockStderr();
        try std.testing.expectEqual(@as(usize, 0), shard.stderr_writer.interface.buffer.len);
    }

    // 3. Batched `sendmmsg` multi-datagram UDP + zero-copy `sendfile` / `copy_file_range`:
    {
        const lo: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
        const rx = try lo.bind(io, .{ .mode = .dgram });
        defer rx.close(io);
        const tx = try lo.bind(io, .{ .mode = .dgram });
        defer tx.close(io);

        var payloads: [20][4]u8 = undefined;
        var out_msgs: [20]Io.net.OutgoingMessage = undefined;
        for (0..20) |i| {
            payloads[i] = .{ 'P', 'K', @intCast('0' + (i / 10)), @intCast('0' + (i % 10)) };
            out_msgs[i] = .{
                .address = &rx.address,
                .data_ptr = &payloads[i],
                .data_len = payloads[i].len,
            };
        }
        try tx.sendMany(io, &out_msgs, .{});

        var in_msgs: [20]Io.net.IncomingMessage = @splat(.init);
        var rx_buf: [256]u8 = undefined;
        var total_rx: usize = 0;
        while (total_rx < 20) {
            const err_opt, const n = rx.receiveManyTimeout(
                io,
                in_msgs[total_rx..],
                &rx_buf,
                .{},
                .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } },
            );
            try std.testing.expectEqual(@as(?Io.net.Socket.ReceiveError, null), err_opt);
            total_rx += n;
        }
        try std.testing.expectEqual(@as(usize, 20), total_rx);

        // Real-kernel zero-copy file -> file (`copy_file_range`) and file -> TCP socket (`sendfile`):
        const cwd = Io.Dir.cwd();
        try cwd.createDirPath(io, ".zig-cache/islet-zc-test");
        defer cwd.deleteDir(io, ".zig-cache/islet-zc-test") catch {};

        const src_path = ".zig-cache/islet-zc-test/src.bin";
        const dst_path = ".zig-cache/islet-zc-test/dst.bin";
        defer cwd.deleteFile(io, src_path) catch {};
        defer cwd.deleteFile(io, dst_path) catch {};

        const pattern_len = 4096;
        var pattern: [pattern_len]u8 = undefined;
        for (&pattern, 0..) |*b, idx| b.* = @truncate(idx *% 37 +% 11);

        {
            const f = try cwd.createFile(io, src_path, .{ .truncate = true });
            defer f.close(io);
            var wbuf: [512]u8 = undefined;
            var fw = f.writer(io, &wbuf);
            try fw.interface.writeAll(&pattern);
            try fw.interface.flush();
        }

        // File -> File zero-copy via `File.Writer.interface.sendFileAll`:
        {
            const src_f = try cwd.openFile(io, src_path, .{});
            defer src_f.close(io);
            const dst_f = try cwd.createFile(io, dst_path, .{ .truncate = true, .read = true });
            defer dst_f.close(io);

            var rbuf: [256]u8 = undefined;
            var fr = src_f.reader(io, &rbuf);
            var wbuf: [256]u8 = undefined;
            var fw = dst_f.writer(io, &wbuf);
            try fw.interface.writeAll("HDR:");
            const sent_f = try fw.interface.sendFileAll(&fr, .unlimited);
            try fw.interface.flush();
            try std.testing.expectEqual(pattern_len, sent_f);

            var verify_buf: [4 + pattern_len]u8 = undefined;
            const n_read = try dst_f.readPositionalAll(io, &verify_buf, 0);
            try std.testing.expectEqual(verify_buf.len, n_read);
            try std.testing.expectEqualStrings("HDR:", verify_buf[0..4]);
            try std.testing.expectEqualSlices(u8, &pattern, verify_buf[4..]);
        }

        // File -> TCP Socket zero-copy (`sendfile`) with buffered HTTP-like header:
        {
            var srv = try lo.listen(io, .{ .reuse_address = true });
            defer srv.deinit(io);

            const Receiver = struct {
                srv_ptr: *Io.net.Server,
                got: [6 + pattern_len]u8 = undefined,
                got_len: usize = 0,

                fn run(t_io: Io, self: *@This()) !void {
                    const peer = try self.srv_ptr.accept(t_io);
                    defer peer.close(t_io);
                    var rbuf: [512]u8 = undefined;
                    var sr = peer.reader(t_io, &rbuf);
                    try sr.interface.readSliceAll(&self.got);
                    self.got_len = self.got.len;
                }
            };
            var rec: Receiver = .{ .srv_ptr = &srv };
            var srv_fut = try io.concurrent(Receiver.run, .{ io, &rec });

            const cli = try srv.socket.address.connect(io, .{ .mode = .stream });
            defer cli.close(io);

            const src_f = try cwd.openFile(io, src_path, .{});
            defer src_f.close(io);
            var fr_buf: [128]u8 = undefined;
            var fr = src_f.reader(io, &fr_buf);
            // Pre-read 10 bytes into `fr`'s user buffer so BOTH buffered drain + kernel `sendfile` execute!
            const peeked = try fr.interface.peek(10);
            try std.testing.expectEqualSlices(u8, pattern[0..10], peeked);

            var sw_buf: [128]u8 = undefined;
            var sw = cli.writer(io, &sw_buf);
            try sw.interface.writeAll("200OK\n");
            const sent_sock = try sw.interface.sendFileAll(&fr, .unlimited);
            try sw.interface.flush();
            try std.testing.expectEqual(pattern.len, sent_sock);

            try srv_fut.await(io);
            try std.testing.expectEqual(6 + pattern.len, rec.got_len);
            try std.testing.expectEqualStrings("200OK\n", rec.got[0..6]);
            try std.testing.expectEqualSlices(u8, &pattern, rec.got[6..]);
        }
    }
}

test "Round 3 E2E: Batch.awaitAsync partial OOM progress, Group multi-round reuse, zero-hop inline descriptor ops, and EPOLLET baton cascade" {
    const Guard = struct {
        backing: std.mem.Allocator,
        forbid: bool = false,

        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.forbid) return null;
            return self.backing.rawAlloc(len, alignment, ret);
        }
        fn resize(ctx: *anyopaque, mem: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.forbid and new_len > mem.len) return false;
            return self.backing.rawResize(mem, alignment, new_len, ret);
        }
        fn remap(ctx: *anyopaque, mem: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.forbid and new_len > mem.len) return null;
            return self.backing.rawRemap(mem, alignment, new_len, ret);
        }
        fn free(ctx: *anyopaque, mem: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.backing.rawFree(mem, alignment, ret);
        }
    };

    var guard: Guard = .{ .backing = std.testing.allocator };
    var shard: Shard = undefined;
    try shard.init(guard.allocator(), .{
        .max_cached_stacks = 2,
        .blocking_threads = 1,
        .blocking_queue_capacity = 2,
    });
    defer shard.deinit();
    const io = shard.io();

    // 1. `Io.Group` multi-round reuse (`cancel` -> reuse -> `await`):
    {
        var grp: Io.Group = .init;
        var ev: Io.Event = .unset;
        var ran_second: usize = 0;

        try grp.concurrent(io, struct {
            fn stuck(t_io: Io, e: *Io.Event) Io.Cancelable!void {
                try e.wait(t_io);
            }
        }.stuck, .{ io, &ev });
        grp.cancel(io);

        // Reuse the EXACT same `Io.Group` instance for a second round:
        try grp.concurrent(io, struct {
            fn second(_: Io, cnt: *usize) Io.Cancelable!void {
                cnt.* += 1;
            }
        }.second, .{ io, &ran_second });
        try grp.await(io);
        try std.testing.expectEqual(@as(usize, 1), ran_second);
    }

    // 2. `Batch.awaitAsync` partial `submitBatch` progress without hanging on unsubmitted tail:
    if (builtin.os.tag == .linux) {
        const lo: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
        const warm_rx = try lo.bind(io, .{ .mode = .dgram });
        defer warm_rx.close(io);
        const tx = try lo.bind(io, .{ .mode = .dgram });
        defer tx.close(io);

        // Warm `Epoll.slots` for `warm_rx`:
        {
            var wst: [1]Io.Operation.Storage = undefined;
            var wb: Io.Batch = .init(&wst);
            defer wb.cancel(io);
            var wm: [1]Io.net.IncomingMessage = .{.init};
            var wbuf: [8]u8 = undefined;
            _ = wb.add(.{ .net_receive = .{
                .socket_handle = warm_rx.handle,
                .message_buffer = &wm,
                .data_buffer = &wbuf,
                .flags = .{},
            } });
            try std.testing.expectError(error.Timeout, wb.awaitConcurrent(io, .{ .duration = .{ .raw = .zero, .clock = .awake } }));
        }

        // Open unseen higher-fd sockets that have NOT grown `Epoll.slots` yet:
        var unseen: [16]Io.net.Socket = undefined;
        var n_unseen: usize = 0;
        defer for (unseen[0..n_unseen]) |*s| s.close(io);
        while (n_unseen < unseen.len) : (n_unseen += 1) {
            unseen[n_unseen] = try lo.bind(io, .{ .mode = .dgram });
        }

        guard.forbid = true;
        defer guard.forbid = false;

        var st: [2]Io.Operation.Storage = undefined;
        var batch: Io.Batch = .init(&st);
        defer batch.cancel(io);

        var m0: [1]Io.net.IncomingMessage = .{.init};
        var b0: [16]u8 = undefined;
        var m1: [1]Io.net.IncomingMessage = .{.init};
        var b1: [16]u8 = undefined;

        // Op 0 parks in `batch.pending` (warm fd), Op 1 fails `Epoll.slots` alloc (`ConcurrencyUnavailable`)
        // and stays in `batch.submitted` (never receives any packet!).
        _ = batch.add(.{ .net_receive = .{
            .socket_handle = warm_rx.handle,
            .message_buffer = &m0,
            .data_buffer = &b0,
            .flags = .{},
        } });
        _ = batch.add(.{ .net_receive = .{
            .socket_handle = unseen[unseen.len - 1].handle,
            .message_buffer = &m1,
            .data_buffer = &b1,
            .flags = .{},
        } });

        try tx.send(io, &warm_rx.address, "op0-done");
        // `awaitAsync` MUST return Op 0 immediately without blocking on never-ready Op 1!
        try batch.awaitAsync(io);
        const c0 = batch.next().?;
        try std.testing.expectEqual(@as(?Io.net.Socket.ReceiveError, null), c0.result.net_receive[0]);
        try std.testing.expectEqualStrings("op0-done", m0[0].data);
        batch.cancel(io);
    }

    // 3. Zero-hop inline descriptor & process operations (`seekTo`, `seekBy`, `tryLock`, `unlock`, `currentPath`)
    //    complete instantaneously even while the sole `BlockingPool` worker thread is 100% occupied!
    {
        const cwd = Io.Dir.cwd();
        try cwd.createDirPath(io, ".zig-cache/islet-r3-test");
        defer cwd.deleteDir(io, ".zig-cache/islet-r3-test") catch {};
        const fpath = ".zig-cache/islet-r3-test/lockseek.bin";
        defer cwd.deleteFile(io, fpath) catch {};

        const f = try cwd.createFile(io, fpath, .{ .truncate = true, .read = true });
        defer f.close(io);
        try f.writePositionalAll(io, "abcdefgh", 0);

        // Pin the ONLY `BlockingPool` worker inside a gate until we finish all inline ops!
        const Blocker = struct {
            entered: Io.Event = .unset,
            release: Io.Event = .unset,

            fn hold(_: *Io.Threaded, self: *@This()) void {
                const kio = Io.Threaded.global_single_threaded.io();
                self.entered.set(kio);
                self.release.waitUncancelable(kio);
            }

            fn fiberEntry(s: *Shard, self: *@This()) void {
                s.callBlocking(false, hold, .{self});
            }
        };
        var blk_st: Blocker = .{};
        var holder = try io.concurrent(Blocker.fiberEntry, .{ &shard, &blk_st });
        defer {
            blk_st.release.set(Io.Threaded.global_single_threaded.io());
            holder.await(io);
        }
        while (!blk_st.entered.isSet()) {
            try io.sleep(.fromMilliseconds(1), .awake);
        }

        // All of these MUST execute inline in ~80ns without touching the blocked worker!
        try io.vtable.fileSeekTo(io.userdata, f, 4);
        try io.vtable.fileSeekBy(io.userdata, f, -2);
        if (try f.tryLock(io, .exclusive)) {
            f.unlock(io);
        }
        var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
        const cwd_len = try std.process.currentPath(io, &cwd_buf);
        try std.testing.expect(cwd_len > 0);
    }

    // 4. Multi-waiter `EPOLLET` baton cascade when front reader is canceled after readiness:
    if (builtin.os.tag == .linux) {
        const pair = try Io.net.Socket.createPair(io, .{ .mode = .stream });
        defer pair[0].close(io);
        defer pair[1].close(io);

        const Readers = struct {
            fd: Io.net.Socket.Handle,
            r1_parked: Io.Event = .unset,
            r2_parked: Io.Event = .unset,
            r2_bytes: usize = 0,

            fn r1(t_io: Io, self: *@This()) Io.Cancelable!void {
                self.r1_parked.set(t_io);
                var buf: [8]u8 = undefined;
                var slices = [_][]u8{&buf};
                const res = try t_io.operate(.{ .net_read = .{ .socket_handle = self.fd, .data = &slices } });
                _ = res.net_read catch {};
            }

            fn r2(t_io: Io, self: *@This()) Io.Cancelable!void {
                self.r2_parked.set(t_io);
                var buf: [8]u8 = undefined;
                var slices = [_][]u8{&buf};
                const res = try t_io.operate(.{ .net_read = .{ .socket_handle = self.fd, .data = &slices } });
                const out = res.net_read catch return;
                self.r2_bytes = out.data_len;
            }
        };

        var rst: Readers = .{ .fd = pair[0].handle };
        var f1 = try io.concurrent(Readers.r1, .{ io, &rst });
        try rst.r1_parked.wait(io);
        try io.sleep(.fromMilliseconds(1), .awake);

        var f2 = try io.concurrent(Readers.r2, .{ io, &rst });
        try rst.r2_parked.wait(io);
        try io.sleep(.fromMilliseconds(1), .awake);

        // Cancel `f1` and send 5 bytes: `f2` must receive the payload without hanging!
        try std.testing.expectError(error.Canceled, f1.cancel(io));
        const msg = "baton";
        const wres = try io.operate(.{ .net_write = .{
            .socket_handle = pair[1].handle,
            .header = msg,
            .data = &.{},
            .splat = 0,
        } });
        try std.testing.expectEqual(@as(usize, 5), try wres.net_write);
        try f2.await(io);
        try std.testing.expectEqual(@as(usize, 5), rst.r2_bytes);
    }
}
