# NOTE: We may move to mise (https://mise.jdx.dev/) for toolchain management in the future.

# libclang is required by bindgen (used by librocksdb-sys). macOS: brew install llvm
export LIBCLANG_PATH := "/opt/homebrew/opt/llvm/lib"

# DYLD_LIBRARY_PATH needed at runtime for librocksdb-sys build script to find libclang.dylib
# Note: just's export doesn't propagate DYLD_* vars on macOS (SIP strips them), so this is
# used inline in recipes that need it
dyld-env := "DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib"

# System RocksDB: set FROGDB_SYSTEM_ROCKSDB=1 to link against system-installed RocksDB
# Optionally set FROGDB_LIB_DIR to override the library path (default: /opt/homebrew/lib)
use-system-rocksdb := env("FROGDB_SYSTEM_ROCKSDB", "1")
system-lib-dir := env("FROGDB_LIB_DIR", "/opt/homebrew/lib")
# ROCKSDB_LIB_DIR and SNAPPY_LIB_DIR tell librocksdb-sys to use system libraries.
# lz4-sys always compiles from vendored C source (4 small files, unavoidable).
# zstd-sys can use system zstd via ZSTD_SYS_USE_PKG_CONFIG=1 (set in Dockerfile.builder for Alpine;
# on macOS the zstd compilation is fast so we don't bother).
rocksdb-env := if use-system-rocksdb != "" { "ROCKSDB_LIB_DIR=" + system-lib-dir + " SNAPPY_LIB_DIR=" + system-lib-dir } else { "" }

# =============================================================================
# System RocksDB Verification
# =============================================================================

# Check that system libraries (RocksDB, Snappy) exist at the configured path
linkcheck-libs:
    uv run scripts/linkcheck_libs.py {{if use-system-rocksdb != "" { "--system-rocksdb --lib-dir " + system-lib-dir } else { "" } }}

# Check that a built binary dynamically links system libraries (post-build)
linkcheck-binary:
    uv run scripts/linkcheck_binary.py {{if use-system-rocksdb != "" { "--system-rocksdb" } else { "" } }}

# Verify build output files confirm system library linking (post-build)
linkcheck-build profile="debug":
    uv run scripts/linkcheck_build.py --profile {{profile}} {{if use-system-rocksdb != "" { "--system-rocksdb" } else { "" } }}

# Full link verification (libs + build output + binary linking)
linkcheck: linkcheck-libs linkcheck-build linkcheck-binary

cargo-sweep-install:
    cargo install cargo-sweep

# Default recipe - show available commands
default:
    @just --list

# Type-check the workspace (no codegen, fastest error checking)
check:
    {{dyld-env}} {{rocksdb-env}} cargo check --all-targets

# Type-check a specific crate
check-crate crate:
    {{dyld-env}} {{rocksdb-env}} cargo check -p {{crate}}

# Type-check a specific crate including tests and integration tests
check-crate-tests crate:
    {{dyld-env}} {{rocksdb-env}} cargo check -p {{crate}} --all-targets

# Alias: short form of check
alias c := check

# Build debug
build:
    {{dyld-env}} {{rocksdb-env}} cargo build

# Build with full debug info (for lldb/gdb variable inspection)
build-debug:
    {{dyld-env}} {{rocksdb-env}} CARGO_PROFILE_DEV_DEBUG=2 cargo build

# Build release
release:
    {{dyld-env}} {{rocksdb-env}} cargo build --release

# Run all tests
test:
    {{dyld-env}} {{rocksdb-env}} cargo test --all

# Run tests for a specific crate
test-crate crate:
    {{dyld-env}} {{rocksdb-env}} cargo test -p {{crate}}

# Run a specific test
test-one name:
    {{dyld-env}} {{rocksdb-env}} cargo test {{name}} -- --nocapture

# Run property-based tests (proptest)
proptest:
    {{dyld-env}} {{rocksdb-env}} cargo test proptest --all

# Run concurrency tests (Shuttle + Turmoil)
concurrency:
    {{dyld-env}} {{rocksdb-env}} cargo test -p frogdb-core --features shuttle --test concurrency
    {{dyld-env}} {{rocksdb-env}} cargo test -p frogdb-server --features turmoil --test simulation

# Run tokio-coz causal profiler tests (requires tokio_unstable)
test-coz:
    -cargo sweep --stamp
    RUSTFLAGS="--cfg tokio_unstable" cargo test -p tokio-coz
    -cargo sweep --time 0

# Run browser integration tests (requires chromedriver running on port 9515)
test-browser:
    {{dyld-env}} {{rocksdb-env}} cargo test -p frogdb-browser-tests --features browser-tests

# Run all benchmarks
bench:
    {{dyld-env}} {{rocksdb-env}} cargo bench -p frogdb-benches

# Format code
fmt:
    cargo fmt --all
    uvx ruff format

# Check formatting (CI)
fmt-check:
    cargo fmt --all -- --check
    uvx ruff format --check

# Run clippy lints
lint:
    {{dyld-env}} {{rocksdb-env}} cargo clippy --all-targets -- -D warnings
    uvx ruff check

# Run clippy lints for a specific crate
lint-crate crate:
    -cargo sweep --stamp
    {{dyld-env}} {{rocksdb-env}} cargo clippy -p {{crate}} -- -D warnings
    -cargo sweep --time 0

# Run cargo-deny (license/security audit)
deny:
    cargo deny check

# Run all checks (CI)
check-all: fmt-check lint deny test

# Run the server (debug)
run *args:
    {{dyld-env}} {{rocksdb-env}} cargo run -p frogdb-server -- {{args}}

# Run the server (release)
run-release *args:
    {{dyld-env}} {{rocksdb-env}} cargo run --release -p frogdb-server -- {{args}}

# Show size of target directory
target-size:
    @echo "Target directory size:"
    @du -sh target 2>/dev/null || echo "No target directory found"
    @echo "\nBreakdown by subdirectory:"
    @du -sh target/*/ 2>/dev/null || echo "No subdirectories found"

# Clean build artifacts
clean:
    cargo clean

# Clean stale build artifacts (keeps current build intact)
clean-stale:
    @echo "Target directory size before:"
    @du -sh target 2>/dev/null || true
    # Remove stale librocksdb-sys from-source build dirs (1.7GB+ each), keeping the newest
    @for dir in $(ls -dt target/debug/build/librocksdb-sys-*/ 2>/dev/null | tail -n +2); do \
        size=$$(du -sm "$$dir" | cut -f1); \
        if [ "$$size" -gt 100 ]; then \
            echo "Removing stale rocksdb build: $$dir ($${size}MB)"; \
            rm -rf "$$dir"; \
        fi; \
    done
    # Sweep stale dep artifacts (not touched in 7 days)
    -cargo sweep --time 7
    @echo "Target directory size after:"
    @du -sh target 2>/dev/null || true

# Clean stale build artifacts across all worktrees (requires: cargo install cargo-sweep)
clean-worktrees:
    #!/usr/bin/env bash
    for dir in $(git worktree list --porcelain | grep '^worktree ' | cut -d' ' -f2); do
        if [ -d "$dir/target" ]; then
            echo "Sweeping $dir/target..."
            cargo sweep --time 0 "$dir"
        fi
    done

# Watch for changes and type-check (requires: cargo install cargo-watch)
watch:
    {{dyld-env}} {{rocksdb-env}} cargo watch -x 'check --all-targets'

# Watch for changes and run tests (requires: cargo install cargo-watch)
watch-test:
    {{dyld-env}} {{rocksdb-env}} cargo watch -x 'test --all'

# Generate documentation
doc:
    {{dyld-env}} {{rocksdb-env}} cargo doc --all --no-deps --open

# =============================================================================
# Redis Compatibility Testing
# =============================================================================

# Run Redis compatibility tests
redis-compat *args:
    uv run redis-compat/run_tests.py {{args}}

# Run a single Redis compatibility test by name (requires --single <suite>)
# Example: just redis-compat-one unit/sort "SORT extracts multiple STORE correctly"
redis-compat-one suite test:
    uv run redis-compat/run_tests.py --single {{suite}} --test {{quote(test)}} --skip-build --verbose

# Clean Redis test cache
redis-compat-clean:
    rm -rf .redis-tests/

# Show Redis compatibility coverage
redis-compat-coverage:
    uv run redis-compat/coverage.py

# =============================================================================
# Causal Profiling (tokio-coz)
# =============================================================================

# Build with causal profiling support (tokio_unstable + causal-profile feature)
# Usage: just build-causal [profile]  (debug or release, default: debug)
build-causal profile="debug":
    -cargo sweep --stamp
    RUSTFLAGS="--cfg tokio_unstable" {{dyld-env}} {{rocksdb-env}} cargo build -p frogdb-server --features causal-profile {{ if profile == "release" { "--release" } else { "" } }}
    -cargo sweep --time 0

# Causal-profile FrogDB under load (tokio-coz)
# Usage: just causal-profile [workload] [duration_secs] [--profile release]
causal-profile workload="mixed" duration="90" *args:
    uv run loadtest/scripts/causal_profile.py -w {{workload}} --duration {{duration}} {{args}}

# =============================================================================
# Profiling (requires: cargo-flamegraph, samply, heaptrack)
# Install: brew bundle / nix-shell
# =============================================================================

# Build with profiling symbols
build-profile:
    {{dyld-env}} {{rocksdb-env}} cargo build --profile profiling

# Generate CPU flamegraph (requires cargo-flamegraph)
profile-flamegraph *args:
    {{dyld-env}} {{rocksdb-env}} cargo flamegraph --profile profiling --bin frogdb-server -- {{args}}

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
# Example: just profile-load mixed 50000 --shards 3 -t 8 -c 50
profile-load workload="mixed" requests="10000" *args:
    uv run loadtest/scripts/profile_load.py -w {{workload}} -n {{requests}} {{args}}

# Run Docker benchmarks against FrogDB, Redis, Valkey, and Dragonfly
# Usage: just benchmark [workload] [requests]
# Example: just benchmark ycsb-a
# Example: just benchmark write-heavy 200000
benchmark workload="ycsb-a" requests="100000" *args:
    uv run loadtest/scripts/benchmark.py -w {{workload}} --all --start-docker -n {{requests}} {{args}}

# =============================================================================
# Cross-Compilation (for faster Jepsen builds)
# =============================================================================

# Install cargo-zigbuild for native cross-compilation
cross-install:
    cargo install cargo-zigbuild

# Cross-compile for Linux x86_64 using zig
cross-build:
    cargo zigbuild --release --target x86_64-unknown-linux-gnu --bin frogdb-server

# Cross-compile for Linux ARM64 using zig (for benchmarks on Apple Silicon)
cross-build-arm:
    cargo zigbuild --release --target aarch64-unknown-linux-gnu --bin frogdb-server

# Verify binary is valid Linux ELF
cross-verify:
    @file target/x86_64-unknown-linux-gnu/release/frogdb-server

# Build Docker image (requires cross-build first)
docker-build: cross-build
    docker build -t frogdb:latest .

# Build benchmark Docker image (ARM-native, for Apple Silicon)
docker-build-bench: cross-build-arm
    docker build -f Dockerfile.bench -t frogdb:latest .

# Build Docker image entirely inside Docker (no cross-compilation needed, uses system RocksDB)
docker-build-full:
    docker build -f Dockerfile.builder -t frogdb:latest .

# Run a Jepsen test: just jepsen register --time-limit 30
jepsen test *args:
    uv run jepsen/run.py run {{test}} {{args}}

# Run a Jepsen test suite (all, single, crash, replication, raft, raft-extended)
jepsen-suite suite *args:
    uv run jepsen/run.py run --suite {{suite}} --build {{args}}

# Start a Jepsen topology (single, replication, raft)
jepsen-up topology:
    uv run jepsen/run.py up {{topology}}

# Stop a Jepsen topology (single, replication, raft; omit to stop all)
jepsen-down *topology:
    uv run jepsen/run.py down {{topology}}

# Clean Jepsen test results
jepsen-clean:
    uv run jepsen/run.py clean

# Open Jepsen results in browser
jepsen-results:
    uv run jepsen/run.py results

# List available Jepsen tests and suites
jepsen-list:
    uv run jepsen/run.py list

# Enter Jepsen control node shell
jepsen-shell:
    docker compose -f jepsen/docker-compose.yml exec control bash

# =============================================================================
# Helm Chart Generation
# =============================================================================

# Generate Helm chart files from FrogDB config (pass --check to verify)
helm-gen *args:
    {{dyld-env}} {{rocksdb-env}} cargo run -p helm-gen -- {{args}}

# =============================================================================
# Dashboard Generation
# =============================================================================

# Generate Grafana dashboard from FrogDB metrics (pass --check to verify)
dashboard-gen *args:
    {{dyld-env}} {{rocksdb-env}} cargo run -p dashboard-gen -- {{args}}

# =============================================================================
# Workflow Generation
# =============================================================================

# Generate GitHub Actions workflow files (pass --check to verify)
workflow-gen *args:
    uv run tools/workflow-gen.py {{args}}

# =============================================================================
# Generate All
# =============================================================================

# Generate all derived files (Helm chart + dashboard + workflows)
generate: helm-gen dashboard-gen workflow-gen

# Check all derived files are up to date (for CI)
generate-check:
    just helm-gen --check
    just dashboard-gen --check
    just workflow-gen --check

# =============================================================================
# Documentation Site
# =============================================================================

# Install documentation site dependencies
docs-install:
    cd docs-site && bun install

# Run documentation site development server
docs-dev:
    cd docs-site && bun run dev

# Build documentation site for production
docs-build:
    cd docs-site && bun run build

# Preview production build of documentation site
docs-preview:
    cd docs-site && bun run preview

# =============================================================================
# Debug UI Assets
# =============================================================================

# Install and vendor JS/CSS assets for the debug web UI
debug-assets:
    cd crates/debug && bun install && bun run vendor
