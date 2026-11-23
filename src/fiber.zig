//! Dual-platform stackful Fiber primitives and Shard-local watermark `StackPool`.
//!
//! - Linux (`x86_64`, `aarch64`): `mmap`-backed bounded stacks with a leading
//!   `PROT_NONE` guard page and user-space ABI register switch.
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

pub const InitError = Allocator.Error || error{SystemResources};

/// Saved execution context of an inactive Fiber or the Shard's thread/idle context.
pub const ExecContext = switch (builtin.os.tag) {
    .linux => switch (builtin.cpu.arch) {
        .x86_64 => extern struct {
            rsp: u64 = 0,
            rbp: u64 = 0,
            rip: u64 = 0,
        },
        .aarch64 => extern struct {
            sp: u64 = 0,
            fp: u64 = 0,
            pc: u64 = 0,
        },
        else => |arch| @compileError("unsupported Linux architecture: " ++ @tagName(arch)),
    },
    .windows => struct {
        os_fiber: win32.LPVOID = undefined,
    },
    else => |os| @compileError("unsupported OS: " ++ @tagName(os)),
};

/// Guard converting the current OS thread into a scheduling root context on Windows
/// (no-op wrapper around `ExecContext` on Linux).
pub const ThreadContext = struct {
    ctx: ExecContext = .{},
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

const LinuxSwitchPair = extern struct {
    old: *ExecContext,
    new: *ExecContext,
};

/// Switches CPU execution from `from` to `to`. Both contexts must belong to the
/// calling OS thread.
pub inline fn switchContext(from: *ExecContext, to: *ExecContext) void {
    switch (builtin.os.tag) {
        .linux => {
            const pair: LinuxSwitchPair = .{ .old = from, .new = to };
            linuxContextSwitch(&pair);
        },
        .windows => {
            win32.SwitchToFiber(to.os_fiber);
        },
        else => comptime unreachable,
    }
}

inline fn linuxContextSwitch(pair: *const LinuxSwitchPair) void {
    switch (builtin.cpu.arch) {
        .x86_64 => asm volatile (
            \\ movq 0(%%rsi), %%rax
            \\ movq 8(%%rsi), %%rcx
            \\ leaq 0f(%%rip), %%rdx
            \\ movq %%rsp, 0(%%rax)
            \\ movq %%rbp, 8(%%rax)
            \\ movq %%rdx, 16(%%rax)
            \\ movq 0(%%rcx), %%rsp
            \\ movq 8(%%rcx), %%rbp
            \\ jmpq *16(%%rcx)
            \\0:
            :
            : [p] "{rsi}" (pair),
            : .{
              .rax = true,
              .rcx = true,
              .rdx = true,
              .rbx = true,
              .rsi = true,
              .rdi = true,
              .r8 = true,
              .r9 = true,
              .r10 = true,
              .r11 = true,
              .r12 = true,
              .r13 = true,
              .r14 = true,
              .r15 = true,
              .mm0 = true,
              .mm1 = true,
              .mm2 = true,
              .mm3 = true,
              .mm4 = true,
              .mm5 = true,
              .mm6 = true,
              .mm7 = true,
              .zmm0 = true,
              .zmm1 = true,
              .zmm2 = true,
              .zmm3 = true,
              .zmm4 = true,
              .zmm5 = true,
              .zmm6 = true,
              .zmm7 = true,
              .zmm8 = true,
              .zmm9 = true,
              .zmm10 = true,
              .zmm11 = true,
              .zmm12 = true,
              .zmm13 = true,
              .zmm14 = true,
              .zmm15 = true,
              .zmm16 = true,
              .zmm17 = true,
              .zmm18 = true,
              .zmm19 = true,
              .zmm20 = true,
              .zmm21 = true,
              .zmm22 = true,
              .zmm23 = true,
              .zmm24 = true,
              .zmm25 = true,
              .zmm26 = true,
              .zmm27 = true,
              .zmm28 = true,
              .zmm29 = true,
              .zmm30 = true,
              .zmm31 = true,
              .fpsr = true,
              .fpcr = true,
              .mxcsr = true,
              .rflags = true,
              .dirflag = true,
              .memory = true,
            }),
        .aarch64 => asm volatile (
            \\ ldp x0, x2, [x1]
            \\ ldr x3, [x2, #16]
            \\ mov x4, sp
            \\ stp x4, fp, [x0]
            \\ adr x5, 0f
            \\ ldp x4, fp, [x2]
            \\ str x5, [x0, #16]
            \\ mov sp, x4
            \\ br x3
            \\0:
            :
            : [p] "{x1}" (pair),
            : .{
              .x0 = true,
              .x1 = true,
              .x2 = true,
              .x3 = true,
              .x4 = true,
              .x5 = true,
              .x6 = true,
              .x7 = true,
              .x8 = true,
              .x9 = true,
              .x10 = true,
              .x11 = true,
              .x12 = true,
              .x13 = true,
              .x14 = true,
              .x15 = true,
              .x16 = true,
              .x17 = true,
              .x19 = true,
              .x20 = true,
              .x21 = true,
              .x22 = true,
              .x23 = true,
              .x24 = true,
              .x25 = true,
              .x26 = true,
              .x27 = true,
              .x28 = true,
              .x30 = true,
              .z0 = true,
              .z1 = true,
              .z2 = true,
              .z3 = true,
              .z4 = true,
              .z5 = true,
              .z6 = true,
              .z7 = true,
              .z8 = true,
              .z9 = true,
              .z10 = true,
              .z11 = true,
              .z12 = true,
              .z13 = true,
              .z14 = true,
              .z15 = true,
              .z16 = true,
              .z17 = true,
              .z18 = true,
              .z19 = true,
              .z20 = true,
              .z21 = true,
              .z22 = true,
              .z23 = true,
              .z24 = true,
              .z25 = true,
              .z26 = true,
              .z27 = true,
              .z28 = true,
              .z29 = true,
              .z30 = true,
              .z31 = true,
              .p0 = true,
              .p1 = true,
              .p2 = true,
              .p3 = true,
              .p4 = true,
              .p5 = true,
              .p6 = true,
              .p7 = true,
              .p8 = true,
              .p9 = true,
              .p10 = true,
              .p11 = true,
              .p12 = true,
              .p13 = true,
              .p14 = true,
              .p15 = true,
              .fpcr = true,
              .fpsr = true,
              .ffr = true,
              .memory = true,
            }),
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
    ctx: ExecContext = .{},
    free_next: ?*RawFiber = null,
    entry_fn: EntryFn = undefined,
    entry_arg: *anyopaque = undefined,
    exit_target: ?*ExecContext = null,
    backing: Backing,

    const Backing = switch (builtin.os.tag) {
        .linux => struct {
            map_ptr: [*]align(std.heap.page_size_min) u8,
            map_len: usize,
        },
        .windows => struct {},
        else => unreachable,
    };

    pub fn headerBytes(self: *RawFiber) *[header_capacity]u8 {
        return &self.header;
    }

    pub fn arm(self: *RawFiber, entry_fn: EntryFn, entry_arg: *anyopaque) void {
        self.entry_fn = entry_fn;
        self.entry_arg = entry_arg;
        self.exit_target = null;
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
                    .backing = .{ .map_ptr = map_ptr, .map_len = total_len },
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
                    .aarch64 => .{
                        .sp = init_sp,
                        .fp = 0,
                        .pc = @intFromPtr(&linuxFiberEntry),
                    },
                    else => comptime unreachable,
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
        else => comptime unreachable,
    }
}

fn linuxFiberLoop(raw: *RawFiber) callconv(.c) noreturn {
    while (true) {
        const func = raw.entry_fn;
        const arg = raw.entry_arg;
        func(arg);
        const target = raw.exit_target.?;
        switchContext(&raw.ctx, target);
    }
}

fn windowsFiberLoop(param: ?win32.LPVOID) callconv(.winapi) void {
    if (builtin.os.tag != .windows) unreachable;
    const raw: *RawFiber = @ptrCast(@alignCast(param.?));
    while (true) {
        const func = raw.entry_fn;
        const arg = raw.entry_arg;
        func(arg);
        const target = raw.exit_target.?;
        switchContext(&raw.ctx, target);
    }
}

/// Shard-local lock-free (single-thread) Fiber stack cache with high-water-mark eviction.
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
        self.trimAll(gpa);
        self.* = undefined;
    }

    pub fn acquire(self: *StackPool, gpa: Allocator) InitError!*RawFiber {
        if (self.free_head) |raw| {
            self.free_head = raw.free_next;
            raw.free_next = null;
            self.cached_count -= 1;
            return raw;
        }
        return RawFiber.create(gpa, self.stack_size);
    }

    /// Returns `raw` to the local free list if below `max_cached`; otherwise
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

    pub fn trimAll(self: *StackPool, gpa: Allocator) void {
        var cur = self.free_head;
        self.free_head = null;
        self.cached_count = 0;
        while (cur) |raw| {
            const next = raw.free_next;
            raw.free_next = null;
            raw.destroy(gpa);
            cur = next;
        }
    }
};

test "Fiber ping-pong context switch, stack reuse, and watermark eviction" {
    const gpa = std.testing.allocator;
    var root_thread = try ThreadContext.init();
    defer root_thread.deinit();

    var pool = StackPool.init(64 * 1024, 1);
    defer pool.deinit(gpa);

    const State = struct {
        root_ctx: *ExecContext,
        raw_ctx: *ExecContext,
        step: u32 = 0,

        fn run(arg: *anyopaque) void {
            const s: *@This() = @ptrCast(@alignCast(arg));
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

    // Release f1 (cached_count becomes 1 == max_cached) and verify reuse returns same pointer.
    pool.release(gpa, f1);
    try std.testing.expectEqual(@as(usize, 1), pool.cached_count);

    const f1_reused = try pool.acquire(gpa);
    try std.testing.expectEqual(f1, f1_reused);

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
