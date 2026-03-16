# FrogDB Repository Structure

This document specifies the repository layout, Cargo workspace configuration, crate organization, and development workflow.

---

## Directory Layout

```
frogdb/
├── Cargo.toml              # Workspace manifest
├── Cargo.lock              # Committed (binary project)
├── Justfile                # Local dev commands (fmt, lint, test)
├── rust-toolchain.toml     # Pinned Rust version
├── rustfmt.toml            # Code formatting rules
├── clippy.toml             # Lint configuration (if needed)
├── deny.toml               # Dependency auditing (cargo-deny)
├── .cargo/
│   └── config.toml         # Cargo settings, faster linker
├── .gitignore              # Git ignores
├── crates/
│   ├── server/             # Binary: networking, runtime, main()
│   │   ├── Cargo.toml      # name = "frogdb-server"
│   │   └── src/
│   │       ├── main.rs
│   │       ├── acceptor.rs
│   │       ├── connection.rs
│   │       └── shard.rs
│   ├── core/               # Library: Store trait, commands, types
│   │   ├── Cargo.toml      # name = "frogdb-core"
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── store.rs
│   │       ├── value.rs
│   │       ├── command.rs
│   │       ├── error.rs
│   │       └── commands/
│   │           ├── mod.rs
│   │           ├── string.rs
│   │           └── generic.rs
│   ├── protocol/           # Library: RESP2 parsing, frame codec
│   │   ├── Cargo.toml      # name = "frogdb-protocol"
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── resp2.rs
│   │       ├── frame.rs
│   │       └── codec.rs
│   ├── lua/                # Library: Lua scripting support
│   │   ├── Cargo.toml      # name = "frogdb-scripting"
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── vm.rs       # Lua VM management
│   │       ├── bindings.rs # redis.call() bindings
│   │       └── sandbox.rs  # Script sandboxing
│   └── persistence/        # Library: RocksDB persistence layer
│       ├── Cargo.toml      # name = "frogdb-persistence"
│       └── src/
│           ├── lib.rs
│           ├── wal.rs      # Write-ahead log
│           ├── snapshot.rs # Snapshot management
│           └── recovery.rs # Crash recovery
├── tests/                  # Integration tests
│   ├── common/
│   │   └── mod.rs          # TestServer and shared utilities
│   ├── string_commands.rs
│   └── connection.rs
├── benches/                # Criterion benchmarks (future)
└── spec/                   # Design specifications
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

### Crate Cargo.toml Examples

**crates/server/Cargo.toml:**
```toml
[package]
name = "frogdb-server"
version.workspace = true
edition.workspace = true
publish.workspace = true

[[bin]]
name = "frogdb-server"
path = "src/main.rs"

[dependencies]
frogdb-core = { path = "../core" }
frogdb-protocol = { path = "../protocol" }
tokio = { workspace = true }
bytes = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
anyhow = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
redis = { workspace = true }

[lints]
workspace = true
```

**crates/core/Cargo.toml:**
```toml
[package]
name = "frogdb-core"
version.workspace = true
edition.workspace = true
publish.workspace = true

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }
griddle = "0.5"

[dev-dependencies]
# None for now

[lints]
workspace = true
```

**crates/protocol/Cargo.toml:**
```toml
[package]
name = "frogdb-protocol"
version.workspace = true
edition.workspace = true
publish.workspace = true

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }
tokio-util = { version = "0.7", features = ["codec"] }

[lints]
workspace = true
```

**crates/scripting/Cargo.toml:**
```toml
[package]
name = "frogdb-scripting"
version.workspace = true
edition.workspace = true
publish.workspace = true

[dependencies]
frogdb-core = { path = "../core" }
mlua = { version = "0.10", features = ["lua54", "vendored", "serialize", "send"] }
bytes = { workspace = true }
thiserror = { workspace = true }

[lints]
workspace = true
```

**crates/persistence/Cargo.toml:**
```toml
[package]
name = "frogdb-persistence"
version.workspace = true
edition.workspace = true
publish.workspace = true

[dependencies]
frogdb-core = { path = "../core" }
rocksdb = { version = "0.24", default-features = false, features = ["multi-threaded-cf", "lz4", "snappy", "zstd"] }
bytes = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[lints]
workspace = true
```

---

## Crate Responsibilities

### frogdb-protocol

**Purpose:** Wire protocol handling only. Zero knowledge of server internals.

**Contains:**
- RESP2 parser and encoder
- Frame types (`Frame`, `ParsedCommand`)
- Tokio codec implementation (`Resp2Codec`)
- Protocol errors

**Does NOT contain:**
- Command execution logic
- Storage access
- Networking code

**Public API:**
```rust
pub use frame::{Frame, ParsedCommand};
pub use codec::Resp2Codec;
pub use error::ProtocolError;
```

### frogdb-core

**Purpose:** Data structures, traits, and command implementations. No async runtime dependency.

**Contains:**
- `Store` trait and `HashMapStore` implementation
- `Value` enum and type implementations (StringValue, ListValue, etc.)
- `Command` trait and command implementations
- `CommandError` enum
- Key metadata, expiry index

**Does NOT contain:**
- Tokio runtime
- TCP networking
- Server lifecycle

**Public API:**
```rust
// Traits
pub use store::Store;
pub use command::{Command, Arity, CommandFlags};

// Types
pub use value::{Value, StringValue};
pub use error::CommandError;
pub use metadata::KeyMetadata;

// Implementations
pub use store::HashMapStore;

// Command registry
pub mod commands;
```

### frogdb-server

**Purpose:** Binary that wires everything together. Owns the async runtime.

**Contains:**
- `main()` function and server lifecycle
- TCP acceptor
- Connection handler
- Shard worker tasks
- Configuration loading
- Signal handling

**Does NOT export:** This is a binary crate, not a library.

### frogdb-scripting

**Purpose:** Lua scripting engine. Isolated from server networking.

**Contains:**
- Lua VM pool management (one per shard)
- `redis.call()` / `redis.pcall()` bindings
- Script sandbox and resource limits
- Determinism enforcement (forbidden functions)

**Does NOT contain:**
- RESP protocol handling
- Storage implementation

**Public API:**
```rust
pub use vm::LuaVmPool;
pub use script::{Script, ScriptResult};
pub use error::ScriptError;
```

### frogdb-persistence

**Purpose:** RocksDB persistence layer. Isolated from server networking.

**Contains:**
- RocksDB column family management
- WAL (Write-Ahead Log) writing
- Snapshot creation and management
- Crash recovery logic
- Key serialization/deserialization

**Does NOT contain:**
- RESP protocol handling
- Command execution logic

**Public API:**
```rust
pub use engine::PersistenceEngine;
pub use wal::WalWriter;
pub use snapshot::SnapshotManager;
pub use recovery::Recovery;
pub use error::PersistenceError;
```

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
