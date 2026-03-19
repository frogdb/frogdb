# FrogDB Justfile

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

# sccache: automatically use as rustc wrapper if installed (speeds up clean builds, branch/worktree switches)
# Disable with: RUSTC_WRAPPER="" just <recipe>
sccache-default := `which sccache 2>/dev/null || echo ""`
export RUSTC_WRAPPER := env("RUSTC_WRAPPER", sccache-default)

# Shorthand for frogdb-server subdirectory
server-dir := justfile_directory() / "frogdb-server"

# Default recipe - show available commands
default:
    @just --list

# =============================================================================
# Rust: Build & Check
# =============================================================================

# Type-check the workspace or a specific crate
check crate="":
    {{dyld-env}} {{rocksdb-env}} cargo check {{ if crate != "" { "-p " + crate } else { "" } }} --all-targets

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

# =============================================================================
# Rust: Test
# =============================================================================

# Run tests (optionally for a specific crate and/or matching a pattern)
test crate="" pattern="":
    {{dyld-env}} {{rocksdb-env}} cargo nextest run {{ if crate != "" { "-p " + crate } else { "--all" } }} {{ if pattern != "" { "-E 'test(/" + pattern + "/)'" } else { "" } }}

# Run concurrency tests (Shuttle + Turmoil)
concurrency:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'

# Run the full test suite (unit + integration + concurrency + simulation)
test-all: test concurrency

# Run tokio-coz causal profiler tests (requires tokio_unstable)
test-coz:
    -cargo sweep --stamp
    RUSTFLAGS="--cfg tokio_unstable" cargo test -p tokio-coz
    -cargo sweep --time 0

# Run browser integration tests (requires chromedriver running on port 9515)
test-browser:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-browser-tests --features browser-tests

# Run all benchmarks
bench:
    {{dyld-env}} {{rocksdb-env}} cargo bench -p frogdb-benches

# =============================================================================
# Rust: Format & Lint
# =============================================================================

# Format Rust code (optionally for a specific crate)
fmt crate="":
    cargo fmt {{ if crate != "" { "-p " + crate } else { "--all" } }}

# Check Rust formatting (CI)
fmt-check crate="":
    cargo fmt {{ if crate != "" { "-p " + crate } else { "--all" } }} -- --check

# Run clippy lints (optionally for a specific crate)
lint crate="":
    {{dyld-env}} {{rocksdb-env}} cargo clippy {{ if crate != "" { "-p " + crate } else { "--all-targets" } }} -- -D warnings

# Run cargo-deny (license/security audit)
deny:
    cargo deny --config {{server-dir}}/deny.toml check

# Generate documentation
doc:
    {{dyld-env}} {{rocksdb-env}} cargo doc --all --no-deps --open

# =============================================================================
# Python Tooling
# =============================================================================

# Format Python code
fmt-py:
    uvx ruff format

# Check Python formatting (CI)
fmt-py-check:
    uvx ruff format --check

# Run Python lints
lint-py:
    uvx ruff check

# =============================================================================
# Run
# =============================================================================

# Run the server (debug)
run *args:
    {{dyld-env}} {{rocksdb-env}} cargo run -p frogdb-server -- {{args}}

# Run the server (release)
run-release *args:
    {{dyld-env}} {{rocksdb-env}} cargo run --release -p frogdb-server -- {{args}}

# =============================================================================
# Causal Profiling (tokio-coz)
# =============================================================================

# Build with causal profiling support (tokio_unstable + causal-profile feature)
# Usage: just build-causal [profile]  (debug or release, default: debug)
build-causal profile="debug":
    -cargo sweep --stamp
    RUSTFLAGS="--cfg tokio_unstable" {{dyld-env}} {{rocksdb-env}} cargo build -p frogdb-server --features causal-profile {{ if profile == "release" { "--release" } else { "" } }}
    -cargo sweep --time 0

# =============================================================================
# Profiling (requires: cargo-flamegraph, samply, heaptrack)
# =============================================================================

# Build with tracing-flame profiling feature
build-profiling:
    {{dyld-env}} {{rocksdb-env}} cargo build -p frogdb-server --features profiling

# Run with tracing-flame profiling feature
run-profiling *args:
    {{dyld-env}} {{rocksdb-env}} cargo run -p frogdb-server --features profiling -- {{args}}

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
profile-load workload="mixed" requests="10000" *args:
    uv run testing/load-test/scripts/profile_load.py -w {{workload}} -n {{requests}} {{args}}

# Causal-profile FrogDB under load (tokio-coz)
# Usage: just causal-profile [workload] [duration_secs] [--profile release]
causal-profile workload="mixed" duration="90" *args:
    uv run testing/load-test/scripts/causal_profile.py -w {{workload}} --duration {{duration}} {{args}}

# Analyze a samply profile JSON
# Usage: just analyze-profile <profile-json> [--top 40]
analyze-profile profile *args:
    uv run testing/load-test/scripts/analyze_profile.py {{profile}} {{args}}

# =============================================================================
# Benchmarking
# =============================================================================

# Run Docker benchmarks against FrogDB, Redis, Valkey, and Dragonfly
# Usage: just benchmark [workload] [requests]
benchmark workload="ycsb-a" requests="100000" *args:
    uv run testing/load-test/scripts/benchmark.py -w {{workload}} --all --start-docker -n {{requests}} {{args}}

# Stop and remove benchmark Docker containers
benchmark-stop:
    uv run testing/load-test/scripts/benchmark.py --stop-docker

# Run standalone memtier_benchmark against FrogDB
# Usage: just memtier [workload] [requests]
memtier workload="mixed" requests="10000" *args:
    uv run testing/load-test/scripts/run_memtier.py -w {{workload}} -n {{requests}} {{args}}

# Quick sanity check with redis-benchmark
# Usage: just redis-bench [workload] [requests]
redis-bench workload="all" requests="100000" *args:
    uv run testing/load-test/scripts/run_redis_benchmark.py -w {{workload}} -n {{requests}} {{args}}

# Compare FrogDB vs Redis (local instances)
# Usage: just compare-redis [workload] [requests]
compare-redis workload="mixed" requests="10000" *args:
    uv run testing/load-test/scripts/compare_redis.py -w {{workload}} -n {{requests}} {{args}}

# Full multi-backend comparison with CPU isolation + scaling
# Usage: just compare-all [workload] [requests] [--isolate] [--scaling]
compare-all workload="mixed" requests="10000" *args:
    uv run testing/load-test/scripts/compare_all.py -w {{workload}} --all --start-docker -n {{requests}} {{args}}

# Cluster-mode benchmark comparison (FrogDB vs Redis/Valkey/Dragonfly clusters)
# Usage: just compare-cluster [workload] [requests]
compare-cluster workload="mixed" requests="10000" *args:
    uv run testing/load-test/scripts/compare_cluster.py -w {{workload}} -n {{requests}} {{args}}

# Generate Markdown report from benchmark results JSON
# Usage: just benchmark-report <input-json> [--cpus N] [--isolated]
benchmark-report input *args:
    uv run testing/load-test/scripts/generate_report.py --input {{input}} {{args}}

# Parse memtier_benchmark result files
# Usage: just benchmark-parse <frogdb-json> [--redis <redis-json>] [--json]
benchmark-parse frogdb *args:
    uv run testing/load-test/scripts/parse_results.py --frogdb {{frogdb}} {{args}}

# =============================================================================
# Fuzz Testing
# =============================================================================

# Run a fuzz target for a given duration (default: 60s)
# Usage: just fuzz resp_parse [duration]
fuzz target duration="60":
    RUSTC_WRAPPER="" LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib {{rocksdb-env}} cargo +nightly fuzz run {{target}} --fuzz-dir testing/fuzz -- -max_total_time={{duration}}

# Run all fuzz targets (default: 30s each)
fuzz-all duration="30":
    #!/usr/bin/env bash
    set -e
    targets=$(RUSTC_WRAPPER="" cargo +nightly fuzz list --fuzz-dir testing/fuzz 2>/dev/null)
    for target in $targets; do
        echo "=== Fuzzing $target for {{duration}}s ==="
        just fuzz "$target" {{duration}}
    done

# List available fuzz targets
fuzz-list:
    RUSTC_WRAPPER="" cargo +nightly fuzz list --fuzz-dir testing/fuzz

# =============================================================================
# Redis Compatibility Testing
# =============================================================================

# Run Redis compatibility tests
redis-compat *args:
    uv run testing/redis-compat/run_tests.py {{args}}

# Run a single Redis compatibility test by name
# Example: just redis-compat-one unit/sort "SORT extracts multiple STORE correctly"
redis-compat-one suite test:
    uv run testing/redis-compat/run_tests.py --single {{suite}} --test {{quote(test)}} --skip-build --verbose

# Clean Redis test cache
redis-compat-clean:
    rm -rf .redis-tests/

# Show Redis compatibility coverage
redis-compat-coverage:
    uv run testing/redis-compat/coverage.py

# =============================================================================
# Jepsen Testing
# =============================================================================

# Run a Jepsen test: just jepsen register --time-limit 30
jepsen test *args:
    uv run testing/jepsen/run.py run {{test}} {{args}}

# Run a Jepsen test suite (all, single, crash, replication, raft, raft-extended)
jepsen-suite suite *args:
    uv run testing/jepsen/run.py run --suite {{suite}} --build {{args}}

# Start a Jepsen topology (single, replication, raft)
jepsen-up topology:
    uv run testing/jepsen/run.py up {{topology}}

# Stop a Jepsen topology (single, replication, raft; omit to stop all)
jepsen-down *topology:
    uv run testing/jepsen/run.py down {{topology}}

# Clean Jepsen test results
jepsen-clean:
    uv run testing/jepsen/run.py clean

# Open Jepsen results in browser
jepsen-results:
    uv run testing/jepsen/run.py results

# List available Jepsen tests and suites
jepsen-list:
    uv run testing/jepsen/run.py list

# Print pass/fail summary from latest Jepsen test results
jepsen-summary:
    uv run testing/jepsen/run.py summary

# Show Docker image labels (git hash, build info)
jepsen-image-info:
    docker inspect --format '{{{{.Config.Labels}}' frogdb:latest

# Enter Jepsen control node shell
jepsen-shell:
    docker compose -f testing/jepsen/docker-compose.yml exec control bash

# =============================================================================
# Cross-Compilation
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

# =============================================================================
# Docker
# =============================================================================

# Build Docker image via cross-compilation (requires zigbuild)
docker-cross-build: cross-build
    docker build -f {{server-dir}}/docker/Dockerfile -t frogdb:latest .

# Build benchmark Docker image (ARM-native, for Apple Silicon)
docker-build-bench: cross-build-arm
    docker build -f {{server-dir}}/docker/Dockerfile.bench -t frogdb:latest .

# Build production Docker image (in-Docker, system libs, minimal runtime)
docker-build-prod:
    docker build -f {{server-dir}}/docker/Dockerfile.builder --build-arg BUILD_TARGET=prod -t frogdb:latest .

# Build debug Docker image for Jepsen/benchmarking (in-Docker, includes debug tools)
docker-build-debug:
    docker build -f {{server-dir}}/docker/Dockerfile.builder --build-arg BUILD_TARGET=debug -t frogdb:latest .

# =============================================================================
# Admin CLI
# =============================================================================

# Run frogdb-admin CLI (pass args after --)
admin *args:
    cargo run -p frogdb-admin -- {{args}}

# =============================================================================
# Codegen
# =============================================================================

# Generate Helm chart files from FrogDB config (pass --check to verify)
helm-gen *args:
    {{dyld-env}} {{rocksdb-env}} cargo run -p helm-gen -- -o frogdb-server/ops/deploy/helm/frogdb {{args}}

# Generate Grafana dashboard from FrogDB metrics (pass --check to verify)
dashboard-gen *args:
    {{dyld-env}} {{rocksdb-env}} cargo run -p dashboard-gen -- -o frogdb-server/ops/grafana/frogdb-overview.json {{args}}

# Generate GitHub Actions workflow files (pass --check to verify)
workflow-gen *args:
    uv run .github/workflows/workflow-gen.py {{args}}

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
    cd website && bun install

# Run documentation site development server
docs-dev:
    cd website && bun run dev

# Build documentation site for production
docs-build:
    cd website && bun run build

# Preview production build of documentation site
docs-preview:
    cd website && bun run preview

# =============================================================================
# Maintenance
# =============================================================================

# Install cargo-nextest (test runner with timeouts and better output)
nextest-install:
    cargo binstall cargo-nextest --secure

cargo-sweep-install:
    cargo install cargo-sweep

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

# Show sccache statistics
sccache-stats:
    sccache --show-stats

# Clear the sccache cache
sccache-clear:
    sccache --stop-server 2>/dev/null || true
    rm -rf "$(sccache --show-stats 2>/dev/null | grep 'Cache location' | awk '{print $NF}')" || true
    @echo "sccache cache cleared"

# Zero sccache counters (keep cache, reset hit/miss stats)
sccache-zero:
    sccache --zero-stats

# Watch for changes and type-check (requires: cargo install cargo-watch)
watch:
    {{dyld-env}} {{rocksdb-env}} cargo watch -x 'check --all-targets'

# Watch for changes and run tests (requires: cargo install cargo-watch)
watch-test:
    {{dyld-env}} {{rocksdb-env}} cargo watch -s 'cargo nextest run --all'

# =============================================================================
# Debug UI Assets
# =============================================================================

# Install and vendor JS/CSS assets for the debug web UI
debug-assets:
    cd {{server-dir}}/crates/debug && bun install && bun run vendor

# =============================================================================
# Aggregate CI
# =============================================================================

# Run all checks (CI)
check-all: fmt-check fmt-py-check lint lint-py deny test-all generate-check

# Alias: CI
alias ci := check-all
