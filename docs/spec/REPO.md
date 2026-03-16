# FrogDB Repository Structure

This document specifies the repository layout, Cargo workspace configuration, crate organization, and development workflow.

---

## Directory Layout

```
frogdb/
├── Cargo.toml                 # Workspace manifest (25 members)
├── Cargo.lock                 # Committed (binary project)
├── Justfile                   # Local dev commands (fmt, lint, test)
├── rust-toolchain.toml        # Pinned Rust version
├── .cargo/
│   └── config.toml            # Cargo settings, linker, sccache
├── frogdb-server/
│   ├── crates/                # All Rust crates (see Cargo.toml for full list)
│   │   ├── server/            # Binary: networking, runtime, main()
│   │   ├── core/              # Core engine: Command trait, Store, shard worker
│   │   ├── commands/          # Data-structure command implementations
│   │   ├── protocol/          # RESP2/RESP3 wire protocol
│   │   ├── types/             # Shared value types and errors
│   │   ├── persistence/       # RocksDB storage, WAL, snapshots
│   │   ├── scripting/         # Lua scripting (Functions API)
│   │   ├── search/            # RediSearch-compatible full-text search
│   │   ├── acl/               # Redis 7.0 ACL system
│   │   ├── cluster/           # Raft-based cluster coordination
│   │   ├── replication/       # Primary-replica streaming
│   │   ├── vll/               # Very Lightweight Locking
│   │   ├── telemetry/         # Prometheus metrics, OpenTelemetry tracing
│   │   ├── debug/             # Debug web UI
│   │   ├── frogdb-macros/     # #[derive(Command)] proc macro
│   │   ├── metrics-derive/    # Typed metrics proc macro
│   │   ├── test-harness/      # TestServer, ClusterHarness
│   │   ├── testing/           # Consistency checker, test models
│   │   ├── redis-regression/  # Redis compat regression tests
│   │   ├── browser-tests/     # Browser integration tests
│   │   └── tokio-coz/         # Causal profiler for Tokio
│   ├── benchmarks/            # Criterion benchmarks
│   └── ops/                   # Operational tooling
│       ├── helm/helm-gen/     # Helm chart generator
│       └── grafana/dashboard-gen/  # Grafana dashboard generator
├── docs/
│   ├── spec/                  # Design specifications
│   └── todo/                  # Future/unimplemented work
├── testing/
│   ├── jepsen/                # Jepsen distributed systems tests
│   ├── redis-compat/          # Redis TCL test suite runner
│   └── load-test/             # Load testing scripts
└── fuzz/                      # cargo-fuzz targets
```

**Note:** Directory names are short (`server/`, `core/`, `protocol/`) but package names
are prefixed (`frogdb-server`, `frogdb-core`, `frogdb-protocol`) to avoid conflicts
with Rust's `core` crate and keep options open for future publishing.

---

## Cargo Workspace

### Root Cargo.toml

```toml
[workspace]
resolver = "2"
members = [
    "frogdb-server/crates/protocol",
    "frogdb-server/crates/types",
    "frogdb-server/crates/cluster",
    "frogdb-server/crates/persistence",
    "frogdb-server/crates/acl",
    "frogdb-server/crates/scripting",
    "frogdb-server/crates/vll",
    "frogdb-server/crates/replication",
    "frogdb-server/crates/commands",
    "frogdb-server/crates/core",
    "frogdb-server/crates/server",
    "frogdb-server/crates/debug",
    "frogdb-server/crates/telemetry",
    "frogdb-server/crates/metrics-derive",
    "frogdb-server/crates/testing",
    "frogdb-server/crates/browser-tests",
    "frogdb-server/crates/frogdb-macros",
    "frogdb-server/benchmarks",
    "frogdb-server/ops/helm/helm-gen",
    "frogdb-server/ops/grafana/dashboard-gen",
    "frogdb-server/crates/tokio-coz",
    "frogdb-server/crates/test-harness",
    "frogdb-server/crates/redis-regression",
    "frogdb-server/crates/search",
]

# Shared package metadata
[workspace.package]
version = "0.1.0"
edition = "2024"
license = "BSL-1.1"

# Shared dependency versions
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
bytes = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "2"
anyhow = "1"

# Dev dependencies
tempfile = "3"
redis = { version = "0.24", features = ["tokio-comp", "aio"] }

[workspace.lints.rust]
unsafe_code = "deny"
missing_docs = "warn"

[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
# Allow these pedantic lints
module_name_repetitions = "allow"
must_use_candidate = "allow"
missing_errors_doc = "allow"
missing_panics_doc = "allow"

# Release profile optimizations
[profile.release]
lto = true             # Link-time optimization
codegen-units = 1      # Better optimization, slower compile
strip = true           # Strip symbols from binary

# Release with debug info (for profiling)
[profile.release-with-debug]
inherits = "release"
debug = true
strip = false
```

### Crate Cargo.toml Convention

All crates inherit shared metadata from the workspace:

```toml
[package]
name = "frogdb-<crate>"
version.workspace = true
edition.workspace = true
```

Dependencies are shared via `[workspace.dependencies]` in the root `Cargo.toml`.
See the actual `Cargo.toml` files in `frogdb-server/crates/*/` for current dependencies.

---

## Crate Responsibilities

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed crate architecture, dependency layers, and component boundaries. The workspace contains 25 crates organized in layers:

| Layer | Crates | Role |
|-------|--------|------|
| **Server Binary** | `frogdb-server` | Networking, runtime, main() |
| **Commands & Observability** | `frogdb-commands`, `frogdb-telemetry`, `frogdb-debug` | Command impls, metrics, tracing |
| **Core Engine** | `frogdb-core` | Command trait, Store trait, shard worker |
| **Features** | `frogdb-acl`, `frogdb-scripting`, `frogdb-search`, `frogdb-replication`, `frogdb-cluster`, `frogdb-persistence`, `frogdb-vll` | Feature modules |
| **Foundation** | `frogdb-types`, `frogdb-protocol` | Value types, RESP protocol |
| **Macros** | `frogdb-macros`, `frogdb-metrics-derive` | Proc macros (no internal deps) |
| **Testing** | `frogdb-test-harness`, `frogdb-testing`, `frogdb-redis-regression`, `frogdb-browser-tests` | Test infrastructure |
| **Tooling** | `frogdb-benches`, `helm-gen`, `dashboard-gen`, `tokio-coz` | Benchmarks, ops tooling |

---

## Module Organization

### Public vs Internal

- **Public API**: Expose via `lib.rs` re-exports
- **Internal modules**: Use `pub(crate)` for crate-internal visibility
- **Private helpers**: Default private visibility

```rust
// crates/core/src/lib.rs
pub mod store;
pub mod value;
pub mod command;
pub mod error;
pub mod commands;

// Re-export commonly used types
pub use store::{Store, HashMapStore};
pub use value::Value;
pub use command::{Command, Arity, CommandFlags};
pub use error::CommandError;
```

### Test Organization

- **Unit tests**: In-file with `#[cfg(test)]` module
- **Integration tests**: In workspace `tests/` directory
- **Test utilities**: In `tests/common/mod.rs`

```rust
// crates/core/src/store.rs
pub struct HashMapStore { /* ... */ }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let mut store = HashMapStore::new();
        // ...
    }
}
```

---

## Code Style

### rustfmt.toml

```toml
edition = "2024"
max_width = 100
use_small_heuristics = "Default"
imports_granularity = "Module"
group_imports = "StdExternalCrate"
```

### rust-toolchain.toml

Pin the Rust version for reproducible builds across all developers and CI:

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

**Note:** Update periodically to latest stable. When updating, run `just check` to ensure compatibility.

### .gitignore

```gitignore
# Build artifacts
/target/

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Environment
.env
.env.local

# Debug
*.log
core

# Cargo.lock is COMMITTED (binary project)
# See: https://doc.rust-lang.org/cargo/faq.html#why-have-cargolock-in-version-control
```

### .cargo/config.toml

Cargo configuration for faster builds and convenient aliases:

```toml
[alias]
# Shortcuts
b = "build"
t = "test"
r = "run"
c = "check"

# Use mold linker on Linux for faster linking (optional)
# Uncomment if mold is installed: https://github.com/rui314/mold
# [target.x86_64-unknown-linux-gnu]
# linker = "clang"
# rustflags = ["-C", "link-arg=-fuse-ld=mold"]

# Use lld on macOS for faster linking (optional)
# Uncomment if lld is installed via: brew install llvm
# [target.aarch64-apple-darwin]
# rustflags = ["-C", "link-arg=-fuse-ld=lld"]

[build]
# Incremental compilation (default, but explicit)
incremental = true

[net]
# Use sparse registry protocol (faster downloads)
git-fetch-with-cli = true
```

### deny.toml

[cargo-deny](https://github.com/EmbarkStudios/cargo-deny) configuration for dependency auditing:

```toml
[advisories]
db-path = "~/.cargo/advisory-db"
vulnerability = "deny"
unmaintained = "warn"
yanked = "warn"
notice = "warn"

[licenses]
unlicensed = "deny"
allow = [
    "MIT",
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "Zlib",
    "CC0-1.0",
    "Unicode-DFS-2016",
]
copyleft = "warn"

[bans]
multiple-versions = "warn"
wildcards = "deny"
highlight = "all"
# Skip specific duplicate version checks if needed:
# skip = [
#     { name = "windows-sys" },  # Common transitive duplicate
# ]

[sources]
unknown-registry = "deny"
unknown-git = "deny"
allow-registry = ["https://github.com/rust-lang/crates.io-index"]
```

**Installation:**
```bash
cargo install cargo-deny
```

### Linting

Strict linting with clippy pedantic, enforced locally before commits:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Warnings are denied (`-D warnings`), ensuring clean builds.

### Error Handling

- **Library crates** (`frogdb-core`, `frogdb-protocol`): Use `thiserror` for typed errors
- **Binary crate** (`frogdb-server`): Use `anyhow` for error propagation in `main()`

```rust
// In frogdb-core (library)
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("wrong number of arguments for '{command}' command")]
    WrongArity { command: &'static str },
    // ...
}

// In frogdb-server (binary)
fn main() -> anyhow::Result<()> {
    // anyhow::Result allows ? on any error type
    let config = Config::load()?;
    run_server(config)?;
    Ok(())
}
```

### Async

- Use Tokio with `#[tokio::main]`
- Prefer `tokio::spawn` for concurrent tasks
- Use channels (`mpsc`, `oneshot`) for inter-task communication

### Logging

- Use `tracing` crate for structured logging
- Log levels: `error`, `warn`, `info`, `debug`, `trace`
- Include context via spans

```rust
use tracing::{info, debug, instrument};

#[instrument(skip(store))]
fn execute_command(cmd: &ParsedCommand, store: &mut Store) -> Response {
    debug!(command = ?cmd.name, "executing");
    // ...
}
```

---

## Development Workflow

### Build

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Build specific crate
cargo build -p frogdb-server
```

### Test

```bash
# Run all tests
cargo test --all

# Run tests for specific crate
cargo test -p frogdb-core

# Run specific test
cargo test test_set_get

# Run with output
cargo test -- --nocapture

# Run sequentially (for debugging)
cargo test -- --test-threads=1
```

### Format

```bash
# Check formatting
cargo fmt --all -- --check

# Apply formatting
cargo fmt --all
```

### Lint

```bash
# Run clippy
cargo clippy --all-targets --all-features -- -D warnings
```

### Run Server

```bash
# Development
cargo run -p frogdb-server

# Release
cargo run --release -p frogdb-server

# With configuration
cargo run -p frogdb-server -- --port 6380 --bind 0.0.0.0
```

### Justfile

[Just](https://github.com/casey/just) is used for development commands. It's a modern command runner with cleaner syntax than Make.

**Installation:**
```bash
# macOS
brew install just

# cargo
cargo install just

# Other platforms: https://github.com/casey/just#installation
```

**Justfile:**
```just
# Default recipe - show available commands
default:
    @just --list

# Build debug
build:
    cargo build

# Build release
release:
    cargo build --release

# Run all tests
test:
    cargo test --all

# Run tests (optionally for a specific crate and/or matching a pattern)
test crate="" pattern="":
    cargo test {{ if crate != "" { "-p " + crate } else { "--all" } }} {{ if pattern != "" { pattern + " -- --nocapture" } else { "" } }}

# Format code
fmt:
    cargo fmt --all

# Check formatting (CI)
fmt-check:
    cargo fmt --all -- --check

# Run clippy lints
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Run cargo-deny (license/security audit)
deny:
    cargo deny check

# Run all checks (CI)
check: fmt-check lint deny test

# Run the server (debug)
run *args:
    cargo run -p frogdb-server -- {{args}}

# Run the server (release)
run-release *args:
    cargo run --release -p frogdb-server -- {{args}}

# Clean build artifacts
clean:
    cargo clean

# Watch for changes and run tests (requires cargo-watch)
watch:
    cargo watch -x 'test --all'

# Generate documentation
doc:
    cargo doc --all --no-deps --open
```

**Usage:**
```bash
just              # Show available commands
just build        # Build debug
just test         # Run all tests
just test frogdb-core        # Test specific crate
just check        # Run all CI checks
just run          # Run server
just run --port 6380  # Run with args
```

---

## Rust Version

**Policy:** Track latest stable Rust. No MSRV (Minimum Supported Rust Version) initially.

Update Rust toolchain as needed:
```bash
rustup update stable
```

---

## CI/CD

### Local-First Approach

CI is run locally during development. No GitHub Actions initially.

Before committing:
```bash
just check  # runs: fmt-check, lint, test
```

### Future GitHub Actions

When the repository goes public, add `.github/workflows/ci.yml`:

```yaml
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: taiki-e/install-action@cargo-deny
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets --all-features -- -D warnings
      - run: cargo deny check
      - run: cargo test --all
```

---

## References

- [INDEX.md](INDEX.md) - Crate structure overview
- [TESTING.md](TESTING.md) - Testing strategy and TestServer
- [ROADMAP.md](ROADMAP.md) - Phase 1 implementation checklist
