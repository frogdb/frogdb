# FrogDB - CLAUDE.md

## Build System

This project uses `just` (see `Justfile`) as the command runner. Always use `just` instead of running `cargo` directly.

Run `just` with no arguments to see all available recipes. Common examples:

```bash
just build              # cargo build
just release            # cargo build --release
just test               # cargo test --all
just test-crate <name>  # cargo test -p <name>
just test-one <name>    # cargo test <name> -- --nocapture
just fmt                # cargo fmt --all
just fmt-check          # cargo fmt --all -- --check
just lint               # cargo clippy --all-targets --all-features -- -D warnings
just check              # fmt-check + lint + deny + test
just concurrency        # Shuttle + Turmoil concurrency tests
```

## System RocksDB (Faster Builds)

**IMPORTANT:** Always set `FROGDB_SYSTEM_ROCKSDB=1` when running local builds to dynamically link against the system-installed RocksDB instead of building from source. This significantly speeds up builds. Optionally set `FROGDB_LIB_DIR` to override the library search path (default: `/opt/homebrew/lib`).

All `just` recipes that invoke cargo respect this flag automatically.

## Verification Before Completing Code Changes

**IMPORTANT:** Before marking any code change as complete, you MUST run all of the following commands and confirm they pass. Do NOT skip any of these steps.

```bash
# 1. Build the entire workspace
just build

# 2. Check formatting
just fmt-check

# 3. Run clippy lints (must pass with no warnings)
just lint

# 4. Run all tests
just test

# 5. Run concurrency tests (Shuttle + Turmoil)
just concurrency
```

If any command fails, fix the issue and re-run all commands before considering the task done.

## Jepsen Tests (Conditional)

Run Jepsen tests when changes touch core server subsystems: **persistence, clustering, Raft consensus, replication, connections, runtime, or consistency guarantees**. These require Docker and take significantly longer.

```bash
# Build the Docker image for Jepsen
just jepsen-build

# Run a single test (auto-starts the required topology)
just jepsen register --time-limit 30
just jepsen append-crash --time-limit 60
just jepsen split-brain --time-limit 60

# Run predefined suites (auto-builds and manages topology lifecycle)
just jepsen-all                # single + crash + replication + raft
just jepsen-replication-all    # replication tests only
just jepsen-raft-all           # raft cluster tests only

# List all available tests and suites
just jepsen-list
```

Or run suites directly: `uv run jepsen/run.py run --suite crash --build`

## Code Generation

Three code generation tools derive files from source definitions. Re-run them when their inputs change:

- `just helm-gen` - Regenerates Helm chart (`values.yaml`, `Chart.yaml`, `values.schema.json`) from server config structs. Run after changing `frogdb-server` config.
- `just dashboard-gen` - Regenerates Grafana dashboard JSON from `frogdb-metrics` metric definitions. Run after adding/modifying metrics.
- `just workflow-gen` - Regenerates GitHub Actions workflows. Run after changing CI/workflow logic.
- `just generate` - Runs all three.
- `just generate-check` - Validates all generated files are up to date (used in CI).

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

Run `just redis-compat` when changes modify command behavior, argument parsing, or error responses. This validates compatibility with the Redis protocol and expected command semantics.

## Agent Guidelines

- **ALWAYS** ensure all packages build, lint, and pass tests before finishing a task
- Prefer `just` commands over raw CLI tools
- If a `just` command doesn't work for a task, we should fix it so it does
