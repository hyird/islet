//! Dual-platform stackful Fiber primitives and Shard-local watermark `StackPool`.
//!
//! - Linux (`x86_64`, `aarch64`, `riscv64`): `mmap`-backed bounded stacks with a
//!   leading `PROT_NONE` guard page, delegating register switching to upstream
//!   `std.Io.fiber` and reclaiming cold stack physical pages via `MADV_DONTNEED`.
//! - Windows (`x86_64`, `aarch64`): native Win32 Fibers (`ConvertThreadToFiberEx`,
//!   `CreateFiberEx`, `SwitchToFiber`, `DeleteFiber`), preserving `NT_TIB` stack
//!   bounds, `__chkstk` probes, SEH unwind chains, and non-volatile XMM state.

const builtin = @import("builtin");
const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const linux = std.os.linux;
const win32 = if (builtin.os.tag == .windows) @import("win32.zig") else void;

pub const default_stack_size: usize = 512 * 1024;
pub const default_max_cached_stacks: usize = 64;
pub const header_capacity: usize = 4096;

/// Top portion of each cached Linux stack kept resident in physical RAM across
/// reuses; deeper cold pages below this threshold are scavenged on Shard idle.
const hot_stack_bytes: usize = 64 * 1024;

pub const InitError = Allocator.Error || error{SystemResources};

/// Saved execution context of an inactive Fiber or the Shard's thread/idle context.
pub const ExecContext = switch (builtin.os.tag) {
    .linux => std.Io.fiber.Context,
    .windows => struct {
        os_fiber: win32.LPVOID = undefined,
    },
    else => |os| @compileError("unsupported OS: " ++ @tagName(os)),
};

pub const zero_context: ExecContext = switch (builtin.os.tag) {
    .linux => std.mem.zeroes(std.Io.fiber.Context),
    .windows => .{},
    else => unreachable,
};

/// Guard converting the current OS thread into a scheduling root context on Windows
/// (no-op wrapper around `ExecContext` on Linux).
pub const ThreadContext = struct {
    ctx: ExecContext = zero_context,
    converted_by_us: if (builtin.os.tag == .windows) bool else void =
        if (builtin.os.tag == .windows) false else {},

    pub fn init() error{SystemResources}!ThreadContext {
        if (builtin.os.tag == .windows) {
            if (win32.ConvertThreadToFiberEx(null, win32.FIBER_FLAG_FLOAT_SWITCH)) |f| {
                return .{
                    .ctx = .{ .os_fiber = f },
                    .converted_by_us = true,
                };
            }
            if (std.os.windows.GetLastError() == @as(std.os.windows.Win32Error, @fromBackingInt(@intCast(win32.ERROR_ALREADY_FIBER)))) {
                const existing = std.os.windows.teb().NtTib.DUMMYUNIONNAME.FiberData;
                return .{
                    .ctx = .{ .os_fiber = existing },
                    .converted_by_us = false,
                };
            }
            return error.SystemResources;
        }
        return .{};
    }

    pub fn deinit(self: *ThreadContext) void {
        if (builtin.os.tag == .windows) {
            if (self.converted_by_us) {
                _ = win32.ConvertFiberToThread();
            }
        }
        self.* = undefined;
    }
};

/// Switches CPU execution from `from` to `to`. Both contexts must belong to the
/// calling OS thread.
pub inline fn switchContext(from: *ExecContext, to: *ExecContext) void {
    switch (builtin.os.tag) {
        .linux => {
            const s: std.Io.fiber.Switch = .{ .old = from, .new = to };
            _ = std.Io.fiber.contextSwitch(&s);
        },
        .windows => {
            win32.SwitchToFiber(to.os_fiber);
        },
        else => comptime unreachable,
    }
}

pub const EntryFn = *const fn (arg: *anyopaque) void;

/// Reusable execution stack + `header_capacity` scratch region for Shard task metadata,
/// closure arguments, and return value storage.
///
/// The task's `EntryFn` runs to completion and returns normally; `RawFiber` then
/// switches to `exit_target` (`assert(exit_target != null)`), leaving the fiber
/// parked cleanly at root loop depth ready for zero-syscall `StackPool` reuse.
pub const RawFiber = struct {
    header: [header_capacity]u8 align(16) = undefined,
    ctx: ExecContext = zero_context,
    free_next: ?*RawFiber = null,
    entry_fn: EntryFn = undefined,
    entry_arg: *anyopaque = undefined,
    exit_target: ?*ExecContext = null,
    cold_scavenged: bool = true,
    backing: Backing,

    const Backing = switch (builtin.os.tag) {
        .linux => struct {
            map_ptr: [*]align(std.heap.page_size_min) u8,
            map_len: usize,
            usable_stack: usize,
            page_size: usize,
        },
        .windows => struct {},
        else => unreachable,
    };

    pub fn arm(self: *RawFiber, entry_fn: EntryFn, entry_arg: *anyopaque) void {
        self.entry_fn = entry_fn;
        self.entry_arg = entry_arg;
        self.exit_target = null;
    }

    /// Releases physical pages for stack memory below the top `hot_stack_bytes`
    /// while keeping the virtual mapping intact (`MADV_DONTNEED`).
    pub fn scavengeColdPages(self: *RawFiber) void {
        if (builtin.os.tag == .linux) {
            const keep = std.mem.alignForward(usize, hot_stack_bytes, self.backing.page_size);
            if (self.backing.usable_stack > keep) {
                const cold_ptr: [*]u8 = self.backing.map_ptr + self.backing.page_size;
                const cold_len = self.backing.usable_stack - keep;
                _ = linux.madvise(cold_ptr, cold_len, linux.MADV.DONTNEED);
            }
        }
        self.cold_scavenged = true;
    }

    fn create(gpa: Allocator, stack_size: usize) InitError!*RawFiber {
        switch (builtin.os.tag) {
            .linux => {
                const page_size = std.heap.pageSize();
                const usable_stack = std.mem.alignForward(usize, @max(stack_size, 64 * 1024), page_size);
                const meta_bytes = std.mem.alignForward(usize, @sizeOf(RawFiber), page_size);
                const total_len = page_size + usable_stack + meta_bytes;

                // Map entire region PROT_NONE first, then unprotect usable stack + metadata.
                // Low page `[0..page_size]` remains a hardware PROT_NONE guard page.
                const map_rc = linux.mmap(
                    null,
                    total_len,
                    .{},
                    .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .STACK = true },
                    -1,
                    0,
                );
                if (linux.errno(map_rc) != .SUCCESS) return error.SystemResources;
                const map_ptr: [*]align(std.heap.page_size_min) u8 = @ptrFromInt(map_rc);
                errdefer _ = linux.munmap(map_ptr, total_len);

                const rw_ptr = map_ptr + page_size;
                const rw_len = total_len - page_size;
                if (linux.errno(linux.mprotect(rw_ptr, rw_len, .{ .READ = true, .WRITE = true })) != .SUCCESS) {
                    return error.SystemResources;
                }

                // Place `RawFiber` metadata at the high end, above `stack_top`.
                const raw_addr = @intFromPtr(map_ptr) + page_size + usable_stack;
                const raw: *RawFiber = @ptrFromInt(raw_addr);
                raw.* = .{
                    .cold_scavenged = false,
                    .backing = .{
                        .map_ptr = map_ptr,
                        .map_len = total_len,
                        .usable_stack = usable_stack,
                        .page_size = page_size,
                    },
                };

                // Prepare initial 16-byte aligned stack slot holding `raw` pointer.
                const stack_top = raw_addr;
                assert(stack_top % 16 == 0);
                const init_sp = stack_top - 16;
                const arg_slot: *usize = @ptrFromInt(init_sp);
                arg_slot.* = @intFromPtr(raw);

                raw.ctx = switch (builtin.cpu.arch) {
                    .x86_64 => .{
                        .rsp = init_sp,
                        .rbp = 0,
                        .rip = @intFromPtr(&linuxFiberEntry),
                    },
                    .aarch64, .riscv64 => .{
                        .sp = init_sp,
                        .fp = 0,
                        .pc = @intFromPtr(&linuxFiberEntry),
                    },
                    else => |arch| @compileError("unsupported Linux architecture: " ++ @tagName(arch)),
                };
                return raw;
            },
            .windows => {
                const raw = try gpa.create(RawFiber);
                errdefer gpa.destroy(raw);
                const reserve_size = @max(stack_size, 64 * 1024);
                const commit_size = @min(@as(usize, 64 * 1024), reserve_size);
                const os_fiber = win32.CreateFiberEx(
                    commit_size,
                    reserve_size,
                    win32.FIBER_FLAG_FLOAT_SWITCH,
                    windowsFiberLoop,
                    raw,
                ) orelse return error.SystemResources;
                raw.* = .{
                    .ctx = .{ .os_fiber = os_fiber },
                    .backing = .{},
                };
                return raw;
            },
            else => comptime unreachable,
        }
    }

    /// Must never be called while executing on `self`'s own stack.
    fn destroy(self: *RawFiber, gpa: Allocator) void {
        switch (builtin.os.tag) {
            .linux => {
                const ptr = self.backing.map_ptr;
                const len = self.backing.map_len;
                _ = linux.munmap(ptr, len);
            },
            .windows => {
                const os_fiber = self.ctx.os_fiber;
                self.* = undefined;
                gpa.destroy(self);
                win32.DeleteFiber(os_fiber);
            },
            else => comptime unreachable,
        }
    }
};

fn linuxFiberEntry() callconv(.naked) void {
    switch (builtin.cpu.arch) {
        .x86_64 => asm volatile (
            \\ xorl %%ebp, %%ebp
            \\ movq (%%rsp), %%rdi
            \\ callq %[loop:P]
            \\ ud2
            :
            : [loop] "X" (&linuxFiberLoop),
        ),
        .aarch64 => asm volatile (
            \\ mov x29, #0
            \\ mov x30, #0
            \\ ldr x0, [sp]
            \\ bl %[loop]
            \\ brk #1
            :
            : [loop] "X" (&linuxFiberLoop),
        ),
        .riscv64 => asm volatile (
            \\ li s0, 0
            \\ li ra, 0
            \\ ld a0, 0(sp)
            \\ tail %[loop]
            :
            : [loop] "X" (&linuxFiberLoop),
        ),
        else => comptime unreachable,
    }
}

inline fn runFiberLoop(raw: *RawFiber) noreturn {
    while (true) {
        raw.entry_fn(raw.entry_arg);
        switchContext(&raw.ctx, raw.exit_target.?);
    }
}

fn linuxFiberLoop(raw: *RawFiber) callconv(.c) noreturn {
    runFiberLoop(raw);
}

fn windowsFiberLoop(param: ?win32.LPVOID) callconv(.winapi) void {
    if (builtin.os.tag != .windows) unreachable;
    runFiberLoop(@ptrCast(@alignCast(param.?)));
}

/// Shard-local lock-free (single-thread) LIFO Fiber stack cache with watermark
/// eviction and amortized idle `MADV_DONTNEED` physical page scavenging.
pub const StackPool = struct {
    stack_size: usize = default_stack_size,
    max_cached: usize = default_max_cached_stacks,
    cached_count: usize = 0,
    free_head: ?*RawFiber = null,

    pub fn init(stack_size: usize, max_cached: usize) StackPool {
        return .{
            .stack_size = if (stack_size == 0) default_stack_size else stack_size,
            .max_cached = max_cached,
        };
    }

    pub fn deinit(self: *StackPool, gpa: Allocator) void {
        var cur = self.free_head;
        while (cur) |raw| {
            const next = raw.free_next;
            raw.free_next = null;
            raw.destroy(gpa);
            cur = next;
        }
        self.* = undefined;
    }

    pub fn acquire(self: *StackPool, gpa: Allocator) InitError!*RawFiber {
        if (self.free_head) |raw| {
            self.free_head = raw.free_next;
            raw.free_next = null;
            raw.cold_scavenged = false;
            self.cached_count -= 1;
            return raw;
        }
        return RawFiber.create(gpa, self.stack_size);
    }

    /// Returns `raw` to the local LIFO free list if below `max_cached`; otherwise
    /// immediately releases the stack back to the OS/allocator.
    /// Must NOT be called from `raw`'s own stack!
    pub fn release(self: *StackPool, gpa: Allocator, raw: *RawFiber) void {
        assert(raw.free_next == null);
        if (self.cached_count < self.max_cached) {
            raw.free_next = self.free_head;
            self.free_head = raw;
            self.cached_count += 1;
        } else {
            raw.destroy(gpa);
        }
    }

    /// Invoked before a Shard blocks in `epoll_wait`: walks only newly returned
    /// LIFO head fibers (`!cold_scavenged`), zaps cold pages below the top 64 KiB,
    /// and stops at the first already-scavenged fiber in O(k) amortized O(1).
    pub fn scavengeIdle(self: *StackPool) void {
        if (builtin.os.tag != .linux) return;
        var cur = self.free_head;
        while (cur) |raw| : (cur = raw.free_next) {
            if (raw.cold_scavenged) break;
            raw.scavengeColdPages();
        }
    }
};

test "Fiber ping-pong context switch, stack reuse, watermark eviction, and idle cold-page scavenging" {
    const gpa = std.testing.allocator;
    var root_thread = try ThreadContext.init();
    defer root_thread.deinit();

    var pool = StackPool.init(256 * 1024, 1);
    defer pool.deinit(gpa);

    const State = struct {
        root_ctx: *ExecContext,
        raw_ctx: *ExecContext,
        step: u32 = 0,

        noinline fn touchDeepStack() void {
            var buf: [128 * 1024]u8 = undefined;
            @memset(&buf, 0xA5);
            std.mem.doNotOptimizeAway(&buf);
        }

        fn run(arg: *anyopaque) void {
            const s: *@This() = @ptrCast(@alignCast(arg));
            touchDeepStack();
            s.step += 10;
            switchContext(s.raw_ctx, s.root_ctx);
            s.step += 20;
        }
    };

    const f1 = try pool.acquire(gpa);
    var st1: State = .{ .root_ctx = &root_thread.ctx, .raw_ctx = &f1.ctx };
    f1.arm(State.run, &st1);
    f1.exit_target = &root_thread.ctx;

    switchContext(&root_thread.ctx, &f1.ctx);
    try std.testing.expectEqual(@as(u32, 10), st1.step);
    switchContext(&root_thread.ctx, &f1.ctx);
    try std.testing.expectEqual(@as(u32, 30), st1.step);

    // Release f1 (dirty deep stack), scavenge cold pages, then reuse f1 and touch deep stack again!
    pool.release(gpa, f1);
    try std.testing.expect(!f1.cold_scavenged);
    pool.scavengeIdle();
    try std.testing.expect(f1.cold_scavenged);
    // Second scavengeIdle hits `cold_scavenged == true` immediately with zero syscalls.
    pool.scavengeIdle();

    const f1_reused = try pool.acquire(gpa);
    try std.testing.expectEqual(f1, f1_reused);
    try std.testing.expect(!f1_reused.cold_scavenged);

    var st2: State = .{ .root_ctx = &root_thread.ctx, .raw_ctx = &f1_reused.ctx };
    f1_reused.arm(State.run, &st2);
    f1_reused.exit_target = &root_thread.ctx;
    switchContext(&root_thread.ctx, &f1_reused.ctx);
    switchContext(&root_thread.ctx, &f1_reused.ctx);
    try std.testing.expectEqual(@as(u32, 30), st2.step);

    // Acquire a second fiber concurrently so releasing both triggers watermark eviction.
    const f2 = try pool.acquire(gpa);
    pool.release(gpa, f1_reused);
    pool.release(gpa, f2);
    try std.testing.expectEqual(@as(usize, 1), pool.cached_count);
}
