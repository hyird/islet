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
