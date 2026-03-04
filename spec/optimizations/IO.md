# I/O Optimizations

- [x] **Response write coalescing** - Buffer multiple responses and flush together to reduce syscalls
  - Implemented in `crates/server/src/connection.rs` via `feed_response()` method
- [x] **Per-request tracing span gating** - Reduced hot-path tracing overhead by ~13% CPU
  - `per_request_spans` config option (default: `false`) gates per-request tracing spans in production
  - See `crates/server/src/config/logging.rs`
- [ ] **io_uring / completion-based I/O** - Replace tokio with compio for thread-per-core completion-based I/O
  - Expected: 30-50% throughput improvement
  - Compio recommended over monoio/glommio (most active development, richest ecosystem, cross-platform)
  - See [IO_URING.md](IO_URING.md) for detailed design and runtime comparison
