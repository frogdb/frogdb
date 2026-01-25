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

# Run tests for a specific crate
test-crate crate:
    cargo test -p {{crate}}

# Run a specific test
test-one name:
    cargo test {{name}} -- --nocapture

# Run property-based tests (proptest)
proptest:
    cargo test proptest --all

# Run concurrency tests (requires shuttle feature)
concurrency:
    cargo test -p frogdb-core --features shuttle --test concurrency

# Run Shuttle concurrency tests (alias for concurrency)
test-shuttle:
    cargo test -p frogdb-core --features shuttle --test concurrency

# Run Turmoil simulation tests
test-turmoil:
    cargo test -p frogdb-server --features turmoil --test simulation

# Run all deterministic simulation tests (Shuttle + Turmoil)
test-dst: test-shuttle test-turmoil

# Run linearizability checker tests
test-linearizability:
    cargo test -p frogdb-testing

# Run all benchmarks
bench:
    cargo bench -p frogdb-benches

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

# Show size of target directory
target-size:
    @echo "Target directory size:"
    @du -sh target 2>/dev/null || echo "No target directory found"
    @echo "\nBreakdown by subdirectory:"
    @du -sh target/*/ 2>/dev/null || echo "No subdirectories found"

# Clean build artifacts
clean:
    cargo clean

# Watch for changes and run tests (requires cargo-watch)
watch:
    cargo watch -x 'test --all'

# Generate documentation
doc:
    cargo doc --all --no-deps --open

# =============================================================================
# Redis Compatibility Testing
# =============================================================================

# Run Redis compatibility tests
redis-compat *args:
    uv run compat/redis-compat/run_tests.py {{args}}

# Run specific Redis test unit
redis-compat-unit unit *args:
    uv run compat/redis-compat/run_tests.py --single unit/{{unit}} {{args}}

# Clean Redis test cache
redis-compat-clean:
    rm -rf .redis-tests/

# Show Redis compatibility coverage
redis-compat-coverage:
    uv run compat/redis-compat/coverage.py

# =============================================================================
# Profiling (requires: cargo-flamegraph, samply, heaptrack)
# Install: brew bundle / nix-shell
# =============================================================================

# Build with profiling symbols
build-profile:
    cargo build --profile profiling

# Generate CPU flamegraph (requires cargo-flamegraph)
profile-flamegraph *args:
    cargo flamegraph --profile profiling --bin frogdb-server -- {{args}}

# Profile with samply (requires samply)
profile-samply *args:
    samply record ./target/profiling/frogdb-server {{args}}

# Profile with perf (Linux only, requires perf)
profile-perf *args:
    perf record -g --call-graph dwarf ./target/profiling/frogdb-server {{args}}

# Memory profiling with heaptrack (Linux only, requires heaptrack)
profile-heap *args:
    heaptrack ./target/profiling/frogdb-server {{args}}

# =============================================================================
# Profiling with Load Testing
# =============================================================================

# Profile FrogDB under load (full workflow)
# Usage: just profile-load [workload] [requests]
# Example: just profile-load mixed 50000
profile-load workload="mixed" requests="10000" *args:
    uv run tools/loadtest/scripts/profile_load.py -w {{workload}} -n {{requests}} {{args}}

# Profile with save-only (no auto-open)
profile-load-save workload="mixed" requests="10000" *args:
    uv run tools/loadtest/scripts/profile_load.py -w {{workload}} -n {{requests}} --save-only {{args}}

# Build FrogDB for Jepsen testing
jepsen-build:
    uv run jepsen/build.py

# Start Jepsen test environment (FrogDB node only)
jepsen-up:
    docker compose -f jepsen/docker-compose.yml up -d n1

# Stop Jepsen test environment
jepsen-down:
    docker compose -f jepsen/docker-compose.yml down -v

# Enter Jepsen control node shell
jepsen-shell:
    docker compose -f jepsen/docker-compose.yml exec control bash

# Run Jepsen register workload (no failures)
jepsen-register *args:
    cd jepsen/frogdb && lein run test --workload register --nemesis none {{args}}

# Run Jepsen counter workload (no failures)
jepsen-counter *args:
    cd jepsen/frogdb && lein run test --workload counter --nemesis none {{args}}

# Run Jepsen append workload (durability/crash recovery testing)
jepsen-append *args:
    cd jepsen/frogdb && lein run test --workload append --nemesis none {{args}}

# Run Jepsen transaction workload (multi-key atomicity)
jepsen-transaction *args:
    cd jepsen/frogdb && lein run test --workload transaction --nemesis none {{args}}

# Run Jepsen queue workload (FIFO ordering)
jepsen-queue *args:
    cd jepsen/frogdb && lein run test --workload queue --nemesis none {{args}}

# Run Jepsen set workload (membership consistency)
jepsen-set *args:
    cd jepsen/frogdb && lein run test --workload set --nemesis none {{args}}

# Run Jepsen hash workload (field-level atomicity)
jepsen-hash *args:
    cd jepsen/frogdb && lein run test --workload hash --nemesis none {{args}}

# Run Jepsen sorted set workload (score/ranking consistency)
jepsen-sortedset *args:
    cd jepsen/frogdb && lein run test --workload sortedset --nemesis none {{args}}

# Run Jepsen expiry workload (TTL/expiration testing)
jepsen-expiry *args:
    cd jepsen/frogdb && lein run test --workload expiry --nemesis none {{args}}

# Run Jepsen blocking workload (BLPOP/BRPOP semantics)
jepsen-blocking *args:
    cd jepsen/frogdb && lein run test --workload blocking --nemesis none {{args}}

# Run Jepsen register workload with crash testing
jepsen-crash *args:
    cd jepsen/frogdb && lein run test --workload register --nemesis kill {{args}}

# Run Jepsen counter workload with crash testing
jepsen-counter-crash *args:
    cd jepsen/frogdb && lein run test --workload counter --nemesis kill {{args}}

# Run Jepsen append workload with crash testing (primary durability test)
jepsen-append-crash *args:
    cd jepsen/frogdb && lein run test --workload append --nemesis kill {{args}}

# Run Jepsen append workload with rapid-kill (stress test durability)
jepsen-append-rapid *args:
    cd jepsen/frogdb && lein run test --workload append --nemesis rapid-kill {{args}}

# Run Jepsen transaction workload with crash testing
jepsen-transaction-crash *args:
    cd jepsen/frogdb && lein run test --workload transaction --nemesis kill {{args}}

# Run Jepsen sorted set workload with crash testing
jepsen-sortedset-crash *args:
    cd jepsen/frogdb && lein run test --workload sortedset --nemesis kill {{args}}

# Run Jepsen expiry workload with crash testing
jepsen-expiry-crash *args:
    cd jepsen/frogdb && lein run test --workload expiry --nemesis kill {{args}}

# Run Jepsen expiry workload with rapid-kill (stress test TTL under crashes)
jepsen-expiry-rapid *args:
    cd jepsen/frogdb && lein run test --workload expiry --nemesis rapid-kill {{args}}

# Run Jepsen blocking workload with crash testing
jepsen-blocking-crash *args:
    cd jepsen/frogdb && lein run test --workload blocking --nemesis kill {{args}}

# Run nemesis test with kill (Docker mode)
jepsen-nemesis-kill *args: jepsen-up
    cd jepsen/frogdb && lein run test --docker --workload register --nemesis kill {{args}}

# Run nemesis test with pause (Docker mode)
jepsen-nemesis-pause *args: jepsen-up
    cd jepsen/frogdb && lein run test --docker --workload register --nemesis pause {{args}}

# Full nemesis test suite (build + kill + pause tests)
jepsen-nemesis: jepsen-build
    just jepsen-nemesis-kill --time-limit 60
    just jepsen-nemesis-pause --time-limit 60
    just jepsen-down

# Run all Jepsen tests (build + all workloads)
jepsen-all: jepsen-build jepsen-up
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
    just jepsen-crash --time-limit 60
    just jepsen-append-crash --time-limit 60
    just jepsen-transaction-crash --time-limit 60
    just jepsen-sortedset-crash --time-limit 60
    just jepsen-expiry-crash --time-limit 60
    just jepsen-blocking-crash --time-limit 60

# Clean Jepsen test results
jepsen-clean:
    rm -rf jepsen/frogdb/store/

# Open Jepsen results in browser (macOS)
jepsen-results:
    open jepsen/frogdb/store/latest/
