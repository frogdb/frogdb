# FrogDB

## System RocksDB (Faster Builds)

**IMPORTANT:** Always set `FROGDB_SYSTEM_ROCKSDB=1` when running `just` commands to dynamically link
against the system-installed RocksDB instead of building from source. This significantly speeds up
builds.

All `just` recipes that invoke cargo respect this flag automatically.

## Build System

This project uses `just` (see `Justfile`) as the command runner. Always use `just` instead of running `cargo` directly.

Run `just` with no arguments to see all available recipes. Common examples:

```bash
just check              # cargo check --all-targets (fastest error checking, no codegen)
just build              # cargo build
just release            # cargo build --release
just test               # cargo test --all
just test-crate <name>  # cargo test -p <name>
just test-one <name>    # cargo test <name> -- --nocapture
just fmt                # cargo fmt --all
just lint               # cargo clippy --all-targets --all-features -- -D warnings
just deny               # cargo deny
just concurrency        # Shuttle + Turmoil concurrency tests
```

- builds can consume many Gigabytes of space, free space after completing a task by running `just clean-stale`

## Verification Before Completing Code Changes

**IMPORTANT:** Before marking any code change as complete, you MUST run all of the following commands and confirm they pass. Do NOT skip any of these steps.

```bash
# 1. Type-check the entire workspace (fast, no codegen)
just check

# 2. Check
just fmt

# 3. Run clippy lints (must pass with no warnings)
just lint

# 4. Run all tests
just test
```

## Jepsen Tests (Conditional)

Run Jepsen tests when changes touch core server subsystems: **persistence, clustering, Raft consensus, replication, connections, runtime, or consistency guarantees**. These require Docker and take significantly longer.

```bash
# Build the Docker image for Jepsen
just docker-build

# Run a single test (auto-starts the required topology)
just jepsen register --time-limit 30
just jepsen append-crash --time-limit 60
just jepsen split-brain --time-limit 60

# Run predefined suites (auto-builds and manages topology lifecycle)
just jepsen-suite all            # single + crash + replication + raft
just jepsen-suite replication    # replication tests only
just jepsen-suite raft           # raft cluster tests only

# List all available tests and suites
just jepsen-list
```

Or run suites directly: `uv run jepsen/run.py run --suite crash --build`

## Design Documentation

The `spec/` directory contains design documentation. Consult it before making architectural changes. Key docs:

- `INDEX.md` - Master design overview
- `ARCHITECTURE.md` - Component relationships and boundaries
- `CONCURRENCY.md` - Shard worker architecture, scatter-gather
- `CONSISTENCY.md` - Linearizability and consistency guarantees
- `PERSISTENCE.md` - Storage engine and durability
- `REPLICATION.md` - Replication protocol
- `CLUSTER.md` - Clustering design
- `TESTING.md` - Test strategy

## Redis Compatibility Tests (Conditional)

Run `just redis-compat` when changes modify command behavior, argument parsing, or error responses.
This validates compatibility with the Redis protocol and expected command semantics.

## Agent Guidelines

- **ALWAYS** ensure all packages build, lint, and pass tests before finishing a task
- Prefer `just` commands over raw CLI tools
- If a `just` command doesn't work for a task, we should fix it so it does
