const std = @import("std");

const cross_targets = [_]std.Target.Query{
    .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .gnu },
    .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .gnu },
    .{ .cpu_arch = .x86_64, .os_tag = .windows, .abi = .gnu },
    .{ .cpu_arch = .aarch64, .os_tag = .windows, .abi = .gnu },
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const islet_mod = b.addModule("islet", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{
        .root_module = islet_mod,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const test_step = b.step("test", "Run host unit and integration tests");
    test_step.dependOn(&run_unit_tests.step);

    const fmt_check = b.addFmt(.{
        .paths = b.pathList(&.{ "build.zig", "build.zig.zon", "src" }),
        .check = true,
    });
    const fmt_step = b.step("fmt-check", "Check Zig source formatting");
    fmt_step.dependOn(&fmt_check.step);

    const check_step = b.step("check", "Verify formatting and multi-target compilation");
    check_step.dependOn(&fmt_check.step);

    for (cross_targets) |query| {
        const resolved = b.resolveTargetQuery(query);
        const cross_mod = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = resolved,
            .optimize = optimize,
        });
        const cross_test = b.addTest(.{
            .name = b.fmt("check-{t}-{t}", .{ query.cpu_arch.?, query.os_tag.? }),
            .root_module = cross_mod,
        });
        check_step.dependOn(&cross_test.step);
    }
}
