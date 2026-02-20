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
just test-shuttle       # Shuttle concurrency tests
just test-turmoil       # Turmoil simulation tests
just test-dst           # Both Shuttle + Turmoil
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

# 5. Run Shuttle concurrency tests
just test-shuttle

# 6. Run Turmoil simulation tests
just test-turmoil
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
