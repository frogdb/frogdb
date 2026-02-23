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

# Start the Jepsen environment
just jepsen-up

# Core workloads (run all of these)
just jepsen-register --time-limit 30
just jepsen-counter --time-limit 30
just jepsen-append --time-limit 30
just jepsen-transaction --time-limit 30
just jepsen-queue --time-limit 30
just jepsen-set --time-limit 30
just jepsen-hash --time-limit 30
just jepsen-sortedset --time-limit 30
just jepsen-expiry --time-limit 30
just jepsen-blocking --time-limit 30

# Crash recovery workloads
just jepsen-crash --time-limit 60
just jepsen-append-crash --time-limit 60
just jepsen-transaction-crash --time-limit 60
just jepsen-sortedset-crash --time-limit 60
just jepsen-expiry-crash --time-limit 60
just jepsen-blocking-crash --time-limit 60

# Tear down
just jepsen-down
```

Or run the full suite with: `just jepsen-all`

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
