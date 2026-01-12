# FrogDB Repository Structure

This document specifies the repository layout, Cargo workspace configuration, crate organization, and development workflow.

---

## Directory Layout

```
frogdb/
├── Cargo.toml              # Workspace manifest
├── Makefile                # Local dev commands (fmt, lint, test)
├── rustfmt.toml            # Code formatting rules
├── clippy.toml             # Lint configuration (if needed)
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
│   └── protocol/           # Library: RESP2 parsing, frame codec
│       ├── Cargo.toml      # name = "frogdb-protocol"
│       └── src/
│           ├── lib.rs
│           ├── resp2.rs
│           ├── frame.rs
│           └── codec.rs
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
    "crates/server",
    "crates/core",
    "crates/protocol",
]

# Shared dependency versions
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
bytes = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "1"
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
```

### Crate Cargo.toml Examples

**crates/server/Cargo.toml:**
```toml
[package]
name = "frogdb-server"
version = "0.1.0"
edition = "2021"

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
version = "0.1.0"
edition = "2021"

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
version = "0.1.0"
edition = "2021"

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }
tokio-util = { version = "0.7", features = ["codec"] }

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
- `FrogValue` enum and type implementations
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
pub use value::{FrogValue, FrogString};
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
pub use value::FrogValue;
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
edition = "2021"
max_width = 100
use_small_heuristics = "Default"
imports_granularity = "Module"
group_imports = "StdExternalCrate"
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

### Makefile (Optional)

For convenience, a Makefile can wrap common commands:

```makefile
.PHONY: build test fmt lint check run

build:
	cargo build

test:
	cargo test --all

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

lint:
	cargo clippy --all-targets --all-features -- -D warnings

check: fmt-check lint test

run:
	cargo run -p frogdb-server

release:
	cargo build --release
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
make check  # or: cargo fmt --check && cargo clippy ... && cargo test
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
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets --all-features -- -D warnings
      - run: cargo test --all
```

---

## References

- [INDEX.md](INDEX.md) - Crate structure overview
- [TESTING.md](TESTING.md) - Testing strategy and TestServer
- [ROADMAP.md](ROADMAP.md) - Phase 1 implementation checklist
