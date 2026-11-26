# islet

面向 Zig `0.17.1` 的 Shared-Nothing `std.Io` 后端实现，每个分片（Shard）独占独立 OS 线程与原生多路复用循环（Linux `epoll`、Windows `IOCP`）。

`islet` 仅对外暴露唯一生命周期入口 `islet.Runtime`，一切并发、网络、文件系统与同步能力均经标准 `std.Io` 透明承载，不暴露任何内部队列、多路复用器、分片调度器或线程池子模块。架构边界与并发契约见 `AGENTS.md`。

## 环境依赖

- Zig `0.17.1`（由仓库根 `mise.toml` 固定）

## 快速接入

```zig
const std = @import("std");
const islet = @import("islet");

fn shardMain(io: std.Io) !void {
    try io.sleep(.fromMilliseconds(10), .awake);
}

pub fn main() !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();

    var rt: islet.Runtime = undefined;
    try rt.init(gpa_state.allocator(), .{ .shard_count = 4 });
    defer rt.deinit();

    try rt.run(shardMain, .{});
}
```

## 构建与验证

```bash
# 格式校验与四目标交叉编译（x86_64/aarch64 Linux epoll + Windows IOCP）
mise x -- zig build check --summary all

# 本机单元与真实内核 E2E 测试
mise x -- zig build test --summary all
```
