# islet

A shared-nothing `std.Io` backend for Zig `0.17.1`, with an independent native I/O loop per shard (`epoll` on Linux, `IOCP` on Windows).

## Prerequisites

- Zig `0.17.1` (managed via [mise](https://mise.jdx.dev/) using `mise.toml`)

## Build & Verify

```bash
# Check formatting and cross-target compilation (Linux epoll + Windows IOCP)
mise x -- zig build check --summary all

# Run host unit and integration tests
mise x -- zig build test --summary all
```
