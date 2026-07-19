# FrogDB Justfile

# libclang is required by bindgen (used by librocksdb-sys). macOS: brew install llvm
export LIBCLANG_PATH := env("LIBCLANG_PATH", "/opt/homebrew/opt/llvm/lib")

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

# Generate code coverage report (unit tests only)
coverage crate="" pattern="":
    {{dyld-env}} {{rocksdb-env}} cargo llvm-cov nextest --all {{ if crate != "" { "-p " + crate } else { "" } }} {{ if pattern != "" { "-E 'test(/" + pattern + "/)'" } else { "" } }} --html
    @echo "Report: target/llvm-cov/html/index.html"

# Generate lcov coverage data (for CI upload)
coverage-lcov:
    {{dyld-env}} {{rocksdb-env}} cargo llvm-cov nextest --all --lcov --output-path target/llvm-cov/lcov.info

# Run concurrency tests (Shuttle + Turmoil + generated workload sweep)
concurrency:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/seed_sweep_short_workloads/)'

# Replay a single concurrency repro file (seed + profile + config)
concurrency-repro FILE:
    {{dyld-env}} {{rocksdb-env}} REPRO_FILE={{FILE}} cargo nextest run -p frogdb-server --features turmoil --run-ignored all -E 'test(/replay_repro/)'

# Run turmoil-featured tests matching PATTERN (default: the generated-workload sweep)
concurrency-turmoil PATTERN='seed_sweep':
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/{{PATTERN}}/)'

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
lint crate="": lint-info-seam lint-redirect-seam lint-pubsub-confirmation-seam lint-failover-atomicity lint-metrics-chokepoint
    {{dyld-env}} {{rocksdb-env}} cargo clippy {{ if crate != "" { "-p " + crate } else { "--all-targets" } }} -- -D warnings

# Gate: INFO section content must come from a renderer (crates/server/src/info),
# never a post-hoc string patch. Rejects placeholder-anchor rewrites in the
# shard-local INFO builder and the scatter handlers.
lint-info-seam:
    #!/usr/bin/env bash
    set -euo pipefail
    files=( \
        "{{server-dir}}/crates/server/src/commands/info.rs" \
        "{{server-dir}}/crates/server/src/connection/handlers/scatter.rs" \
        "{{server-dir}}/crates/server/src/connection/handlers/info.rs" \
    )
    bad=0
    for f in "${files[@]}"; do
        [ -f "$f" ] || continue
        if grep -nE '\.replace\("[a-z_]+:0\\r\\n"|\.replace_range\(' "$f"; then
            echo "error: $f patches INFO output with string replacement;" >&2
            echo "       render the value in its InfoSection instead (crates/server/src/info)." >&2
            bad=1
        fi
    done
    exit $bad

# Gate: every MOVED / ASK / CROSSSLOT reply must come from the redirect seam
# (frogdb-types/src/redirect.rs), the single owner of these wire formats. An
# inline `Response::error("CROSSSLOT ...")` re-opens the drift the seam closed;
# an inline `Response::error(format!("MOVED {..." / "ASK {...")` re-opens the
# IPv6 bracketing bug (unbracketed `ip():port()` is unparseable for IPv6).
# Clippy cannot express "this constructor outside that file", so a grep gate is
# the honest tool.
lint-redirect-seam:
    #!/usr/bin/env bash
    set -uo pipefail
    crates="{{server-dir}}/crates"
    owner="types/src/redirect.rs"
    status=0
    if matches=$(grep -rEn --include='*.rs' 'Response::error\("CROSSSLOT' "$crates"); then
        echo "ERROR: inline CROSSSLOT literal — use redirect::crossslot():" >&2
        echo "$matches" >&2
        status=1
    fi
    if matches=$(grep -rEn --include='*.rs' 'Response::error\((format!\()?"(MOVED|ASK) ' "$crates" \
            | grep -v "/$owner:"); then
        echo "ERROR: inline MOVED/ASK redirect — use redirect::moved() / redirect::ask():" >&2
        echo "$matches" >&2
        status=1
    fi
    if [ "$status" -ne 0 ]; then
        echo >&2
        echo "       MOVED/ASK/CROSSSLOT wire formats are owned by" >&2
        echo "       frogdb-types/src/redirect.rs; constructing them elsewhere risks" >&2
        echo "       drift and the IPv6 address-bracketing bug." >&2
        exit 1
    fi
    echo "OK: MOVED/ASK/CROSSSLOT replies come from the redirect seam"

# Run cargo-deny (license/security audit)
deny:
    cargo deny check --config {{server-dir}}/deny.toml

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

# Start server with continuous low-volume traffic for development (debug UI at http://127.0.0.1:9090/debug)
dev workload="mixed" rate="500" *args:
    uv run testing/load/scripts/dev_server.py -w {{workload}} --rate {{rate}} {{args}}

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
    uv run testing/load/scripts/profile_load.py -w {{workload}} -n {{requests}} {{args}}

# Causal-profile FrogDB under load (tokio-coz)
# Usage: just causal-profile [workload] [duration_secs] [--profile release]
causal-profile workload="mixed" duration="90" *args:
    uv run testing/load/scripts/causal_profile.py -w {{workload}} --duration {{duration}} {{args}}

# Analyze a samply profile JSON
# Usage: just analyze-profile <profile-json> [--top 40]
analyze-profile profile *args:
    uv run testing/load/scripts/analyze_profile.py {{profile}} {{args}}

# =============================================================================
# Benchmarking
# =============================================================================

# Run Docker benchmarks against FrogDB, Redis, Valkey, and Dragonfly
# Usage: just benchmark [workload] [requests]
benchmark workload="ycsb-a" requests="100000" *args:
    uv run testing/load/scripts/benchmark.py -w {{workload}} --all --start-docker -n {{requests}} {{args}}

# Stop and remove benchmark Docker containers
benchmark-stop:
    uv run testing/load/scripts/benchmark.py --stop-docker

# Run standalone memtier_benchmark against FrogDB
# Usage: just memtier [workload] [requests]
memtier workload="mixed" requests="10000" *args:
    uv run testing/load/scripts/run_memtier.py -w {{workload}} -n {{requests}} {{args}}

# Run continuous load against FrogDB (runs until Ctrl-C)
# Usage: just load [workload] [duration] [extra-args]
# Duration in seconds, default 0 = unlimited
# Examples:
#   just load                                    # continuous mixed (9:1 read:write)
#   just load read-heavy                         # continuous 19:1 read:write
#   just load write-heavy                        # continuous 1:19 read:write
#   just load mixed 60                           # mixed load for 60 seconds
#   just load mixed 0 --threads 8 --clients 50   # custom memtier args
load workload="mixed" duration="0" *args:
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{workload}}" in
        read-heavy) ratio="19:1" ;;
        write-heavy) ratio="1:19" ;;
        mixed|*) ratio="9:1" ;;
    esac
    time_args=""
    if [ "{{duration}}" != "0" ]; then
        time_args="--test-time {{duration}}"
    else
        time_args="--test-time 999999"
    fi
    echo "Running continuous {{workload}} load (ratio=$ratio)... Ctrl-C to stop"
    memtier_benchmark --server 127.0.0.1 --port 6379 \
        --threads 4 --clients 25 \
        --ratio "$ratio" --key-pattern G:G --data-size 128 \
        $time_args {{args}}

# Quick sanity check with redis-benchmark
# Usage: just redis-bench [workload] [requests]
redis-bench workload="all" requests="100000" *args:
    uv run testing/load/scripts/run_redis_benchmark.py -w {{workload}} -n {{requests}} {{args}}

# Compare FrogDB vs Redis (local instances)
# Usage: just compare-redis [workload] [requests]
compare-redis workload="mixed" requests="10000" *args:
    uv run testing/load/scripts/compare_redis.py -w {{workload}} -n {{requests}} {{args}}

# Full multi-backend comparison with CPU isolation + scaling
# Usage: just compare-all [workload] [requests] [--isolate] [--scaling]
compare-all workload="mixed" requests="10000" *args:
    uv run testing/load/scripts/compare_all.py -w {{workload}} --all --start-docker -n {{requests}} {{args}}

# Cluster-mode benchmark comparison (FrogDB vs Redis/Valkey/Dragonfly clusters)
# Usage: just compare-cluster [workload] [requests]
compare-cluster workload="mixed" requests="10000" *args:
    uv run testing/load/scripts/compare_cluster.py -w {{workload}} -n {{requests}} {{args}}

# Generate Markdown report from benchmark results JSON
# Usage: just benchmark-report <input-json> [--cpus N] [--isolated]
benchmark-report input *args:
    uv run testing/load/scripts/generate_report.py --input {{input}} {{args}}

# Parse memtier_benchmark result files
# Usage: just benchmark-parse <frogdb-json> [--redis <redis-json>] [--json]
benchmark-parse frogdb *args:
    uv run testing/load/scripts/parse_results.py --frogdb {{frogdb}} {{args}}

# =============================================================================
# Fuzz Testing
# =============================================================================

# Run a fuzz target for a given duration (default: 60s)
# Usage: just fuzz resp_parse [duration]
fuzz target duration="60":
    {{dyld-env}} RUSTC_WRAPPER="" LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib {{rocksdb-env}} cargo +nightly fuzz run {{target}} --fuzz-dir testing/fuzz -- -max_total_time={{duration}}

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

# Generate Debian package artifacts from FrogDB config (pass --check to verify)
deb-gen *args:
    {{dyld-env}} {{rocksdb-env}} cargo run -p deb-gen -- -o frogdb-server/ops/deploy/deb {{args}}

# Generate GitHub Actions workflow files (pass --check to verify)
workflow-gen *args:
    uv run --project .github/workflows/workflow_gen python -m workflow_gen {{args}}

# Generate all derived files (Helm chart + dashboard + Debian + workflows)
generate: helm-gen dashboard-gen deb-gen workflow-gen

# Check all derived files are up to date (for CI)
generate-check:
    just helm-gen --check
    just dashboard-gen --check
    just deb-gen --check
    just workflow-gen --check

# =============================================================================
# Documentation Site
# =============================================================================

# Install documentation site dependencies
docs-install:
    cd website && bun install

# Generate config reference data from Rust source code
docs-gen:
    cargo run -p docs-gen

# Verify generated docs data is up to date (for CI)
docs-gen-check:
    cargo run -p docs-gen -- --check

# Generate compatibility exclusions data from regression test metadata
compat-gen:
    uv run website/scripts/compat-gen.py

# Verify generated compatibility data is up to date (for CI)
compat-gen-check:
    uv run website/scripts/compat-gen.py --check

# Run documentation site development server (installs deps if needed)
docs-dev: docs-gen compat-gen
    cd website && [ -d node_modules ] || bun install
    cd website && bun run dev

# Build documentation site for production
docs-build: docs-gen compat-gen
    cd website && bun run build

# Preview production build of documentation site
docs-preview:
    cd website && bun run preview

# Check for broken links in documentation
docs-link-check: docs-build
    cd website && bunx lychee --config ../lychee.toml --root-dir "$(pwd)/dist" dist/

# =============================================================================
# Maintenance
# =============================================================================

# Start the self-hosted GitHub Actions runner (rebuild image if needed)
runner *args:
    cd .github/runner && docker compose up -d --build {{args}}

# Stop the self-hosted GitHub Actions runner
runner-stop:
    cd .github/runner && docker compose down

# Show self-hosted runner logs
runner-logs *args:
    cd .github/runner && docker compose logs {{args}}

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
# Operator
# =============================================================================

# Generate FrogDB CRD YAML
operator-crd:
    {{dyld-env}} {{rocksdb-env}} cargo run --manifest-path frogdb-operator/Cargo.toml -- generate-crd > frogdb-operator/deploy/crd.yaml
    @echo "CRD written to frogdb-operator/deploy/crd.yaml"

# Build the operator (debug)
operator-build:
    {{dyld-env}} {{rocksdb-env}} cargo build --manifest-path frogdb-operator/Cargo.toml

# Run operator tests
operator-test:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run --manifest-path frogdb-operator/Cargo.toml

# =============================================================================
# Toolchain
# =============================================================================

# Verify .mise.toml and rust-toolchain.toml agree on the Rust version
sync-toolchain-check:
    #!/usr/bin/env bash
    set -euo pipefail
    rtc=$(awk -F'"' '/^channel[[:space:]]*=/ {print $2; exit}' rust-toolchain.toml)
    mise=$(awk -F'"' '/^rust[[:space:]]*=/ {print $2; exit}' .mise.toml)
    if [ -z "$rtc" ] || [ -z "$mise" ]; then
        echo "ERROR: could not parse rust version from rust-toolchain.toml ($rtc) or .mise.toml ($mise)" >&2
        exit 1
    fi
    if [ "$rtc" != "$mise" ]; then
        echo "ERROR: rust-toolchain.toml ($rtc) and .mise.toml ($mise) disagree on Rust version" >&2
        echo "       Update whichever is stale so both match." >&2
        exit 1
    fi
    echo "OK: Rust version consistent ($rtc)"

# =============================================================================
# Lint gates
# =============================================================================

# Ban hand-rolled WrongType handling in command code. The typed store accessors
# (StoreTypedExt / StoreTypedFamilyExt: get_list_mut, get_hash_mut, get_bloom,
# get_tdigest, ...) own the WrongType invariant and the COW-avoiding ordering, so
# command impls must not re-derive it. Two forbidden shapes:
#   1. check-then-unwrap: `as_*_mut().unwrap()` / `get_mut(...).unwrap()`
#      (panic-prone — the accessor returns a total `Result`/`Option`).
#   2. hand-rolled chain: `.ok_or(WrongType)` / `.ok_or_else(|| ...WrongType)`
#      (the accessor propagates WrongType via `?`). Note: the cuckoo/timeseries
#      sources used to wrap `.as_*()` and `.ok_or(...)` onto separate lines, but
#      the banned `.ok_or(...WrongType...)` text is itself single-line, so a
#      line-based grep catches every form.
# Clippy's disallowed_methods cannot express "this method followed by unwrap",
# so a grep gate is the honest tool. Scoped to crates/commands so store
# internals stay unconstrained.
lint-no-typed-unwrap:
    #!/usr/bin/env bash
    set -uo pipefail
    status=0
    unwrap_pattern='as_[a-z_]+_mut\(\)[[:space:]]*\.unwrap\(\)|get_mut\([^)]*\)[[:space:]]*\.unwrap\(\)'
    if matches=$(grep -rEn "$unwrap_pattern" {{server-dir}}/crates/commands/src/); then
        echo "ERROR: check-then-unwrap pattern found in command code:" >&2
        echo "$matches" >&2
        echo >&2
        echo "       Use the typed store accessors instead (StoreTypedExt:" >&2
        echo "       get_list_mut / get_hash_mut / get_set_mut / get_zset_mut /" >&2
        echo "       get_string_mut / get_stream_mut, or the generic get_typed_mut)." >&2
        status=1
    fi
    wrongtype_pattern='\.ok_or(_else)?\([^)]*WrongType'
    if matches=$(grep -rEn "$wrongtype_pattern" {{server-dir}}/crates/commands/src/); then
        echo "ERROR: hand-rolled WrongType chain found in command code:" >&2
        echo "$matches" >&2
        echo >&2
        echo "       Use the typed store accessors instead (StoreTypedExt /" >&2
        echo "       StoreTypedFamilyExt: get_<family>[_mut](key)?). They own the" >&2
        echo "       WrongType invariant and propagate it via the \`?\` operator." >&2
        status=1
    fi
    if [ "$status" -ne 0 ]; then
        exit 1
    fi
    echo "OK: no check-then-unwrap or hand-rolled WrongType in crates/commands"

# Keyspace/keyevent notifications must route through the
# KeyspaceNotificationCoordinator, which owns the one emit->subscriber rule:
# broadcast subscribers register on the coordinator shard (shard 0), so an event
# emitted on the key-owner shard must be forwarded there. An emit site that
# reaches past the coordinator to `self.subscriptions.publish` re-opens the
# cross-shard delivery bug (subscribers on shard 0, event on shard N, message
# lost — proposal 22). Only dispatch_pubsub.rs may publish into the local table
# directly: it IS the coordinator shard's delivery arm (PUBLISH + the forwarded
# PublishKeyspace). Clippy cannot express "this field method, outside these
# files," so a grep gate is the honest tool.
lint-keyspace-notify-routing:
    #!/usr/bin/env bash
    set -uo pipefail
    shard_dir="{{server-dir}}/crates/core/src/shard"
    pattern='self\.subscriptions\.publish\('
    if matches=$(grep -rEn --include='*.rs' --exclude='dispatch_pubsub.rs' "$pattern" "$shard_dir"); then
        echo "ERROR: direct keyspace publish bypasses KeyspaceNotificationCoordinator:" >&2
        echo "$matches" >&2
        echo >&2
        echo "       Keyspace/keyevent emit sites must call" >&2
        echo "       self.keyspace_notify.publish(&self.subscriptions, channel, payload)" >&2
        echo "       so cross-shard events reach the shard where subscribers register" >&2
        echo "       (shard 0). Only dispatch_pubsub.rs may publish into the local" >&2
        echo "       table directly (it is the coordinator shard's delivery arm)." >&2
        exit 1
    fi
    echo "OK: keyspace notifications route through the coordinator"

# Keep script sub-command routing behind ScriptCommandGate (scripting/gate.rs).
# The gate is the single owner of key extraction + cross-shard blocking for
# redis.call / redis.pcall, so two shapes are banned in the scripting module:
#   1. block_in_place anywhere but gate.rs — a raw cross-shard block bypasses
#      the gate's explicit-error fallback and can silently write to the wrong
#      shard on a current-thread runtime (the bug the gate fixes).
#   2. extract_keys_from_command in lua_vm.rs — a second key extraction is
#      exactly the cross-slot-vs-cross-shard divergence the gate eliminates by
#      extracting keys once in classify().
# Clippy cannot express "this call outside that file", so a grep gate is the
# honest tool.
lint-script-gate:
    #!/usr/bin/env bash
    set -uo pipefail
    scripting_dir="{{server-dir}}/crates/core/src/scripting"
    status=0
    if matches=$(grep -rEn "block_in_place" "$scripting_dir" | grep -v "/gate\.rs:"); then
        echo "ERROR: block_in_place outside the script command gate:" >&2
        echo "$matches" >&2
        echo >&2
        echo "       Cross-shard script sub-command blocking must go through" >&2
        echo "       ScriptCommandGate::run_remote (scripting/gate.rs), which turns a" >&2
        echo "       current-thread runtime into an explicit error instead of a silent" >&2
        echo "       wrong-shard write." >&2
        status=1
    fi
    if matches=$(grep -rEn "extract_keys_from_command" "$scripting_dir/lua_vm.rs"); then
        echo "ERROR: second key extraction in lua_vm.rs:" >&2
        echo "$matches" >&2
        echo >&2
        echo "       Key extraction for redis.call / redis.pcall routing lives once in" >&2
        echo "       ScriptCommandGate::classify (scripting/gate.rs). Re-extracting keys" >&2
        echo "       here reintroduces the cross-slot vs cross-shard divergence." >&2
        status=1
    fi
    if [ "$status" -ne 0 ]; then
        exit 1
    fi
    echo "OK: script sub-command routing stays behind ScriptCommandGate"

# Pub/sub subscribe/unsubscribe confirmations and the array-null wire shape each
# have exactly one owner (proposal 26):
#   1. Confirmations must be built through frogdb_core::PubSubConfirmation, the
#      single owner of the RESP3-Push-vs-RESP2-Array rule. A hand-rolled
#      confirmation in the pub/sub handlers (a `b"subscribe"`/`b"unsubscribe"`/…
#      label literal) reintroduces the path-dependent shape bug the seam fixed.
#   2. The `*-1\r\n` array-null literal (which redis-protocol cannot produce)
#      belongs only in codec.rs, where the RESP2 codec encodes
#      `Resp2Outbound::NullArray` (proposal 62-A moved it down from the connection
#      layer to sit beside the rest of the RESP2 wire encoding); a second copy
#      risks the two diverging. Clippy cannot express "this literal outside that
#      module", so a grep gate is the honest tool.
lint-pubsub-confirmation-seam:
    #!/usr/bin/env bash
    set -uo pipefail
    status=0
    pubsub_handler="{{server-dir}}/crates/server/src/connection/handlers/pubsub.rs"
    label_pattern='b"(subscribe|unsubscribe|psubscribe|punsubscribe|ssubscribe|sunsubscribe)"'
    if matches=$(grep -nE "$label_pattern" "$pubsub_handler"); then
        echo "ERROR: hand-built pub/sub confirmation in the pub/sub handlers:" >&2
        echo "$matches" >&2
        echo >&2
        echo "       Build confirmations through frogdb_core::PubSubConfirmation" >&2
        echo "       (e.g. PubSubConfirmation::Subscribe { channel, count }" >&2
        echo "       .to_response(self.state.protocol_version)). It is the single" >&2
        echo "       owner of the RESP3 Push vs RESP2 Array confirmation shape." >&2
        status=1
    fi
    null_array_pattern='b"\*-1'
    if matches=$(grep -rEn --include='*.rs' --exclude='codec.rs' "$null_array_pattern" "{{server-dir}}/crates/server/src"); then
        echo "ERROR: array-null (*-1) literal outside codec.rs:" >&2
        echo "$matches" >&2
        echo >&2
        echo "       The RESP2 array-null wire shape lives only in the RESP2 codec" >&2
        echo "       (crates/server/src/connection/codec.rs, Resp2Outbound::NullArray)." >&2
        status=1
    fi
    if [ "$status" -ne 0 ]; then
        exit 1
    fi
    echo "OK: pub/sub confirmations and the array-null shape each have one owner"

# Gate: topology transitions are atomic (todo/proposals/31-atomic-failover-command.md).
# A failover or FAIL-marking must be ONE Raft entry (ClusterCommand::Failover /
# MarkNodeFailed, which bump the epoch inside apply), never a saga of
# RemoveNode/SetRole/AssignSlots followed by a separate IncrementEpoch write.
# A separate `client_write(ClusterCommand::IncrementEpoch)` is the saga's
# signature: if the leader crashes between entries, other nodes observe the new
# topology at a stale epoch (or ownerless slots). Clippy cannot express "these
# commands must not be composed across writes", so a grep gate is the honest tool.
lint-failover-atomicity:
    #!/usr/bin/env bash
    set -uo pipefail
    src="{{server-dir}}/crates/server/src"
    status=0
    if matches=$(grep -rEn --include='*.rs' 'client_write\(ClusterCommand::IncrementEpoch' "$src"); then
        echo "ERROR: standalone IncrementEpoch Raft write (multi-entry topology saga):" >&2
        echo "$matches" >&2
        echo >&2
        echo "       Epoch bumps must ride inside the composite state-machine transition" >&2
        echo "       (ClusterCommand::Failover / MarkNodeFailed). CLUSTER BUMPEPOCH is" >&2
        echo "       unaffected: it flows through convert_raft_cluster_op." >&2
        status=1
    fi
    saga_files="$src/failure_detector.rs $src/connection/handlers/cluster.rs"
    if matches=$(grep -nE 'ClusterCommand::(RemoveNode|AssignSlots|SetRole)' $saga_files); then
        echo "ERROR: failover paths must use the atomic ClusterCommand::Failover, not" >&2
        echo "       hand-rolled RemoveNode/SetRole/AssignSlots sequences:" >&2
        echo "$matches" >&2
        status=1
    fi
    if [ "$status" -ne 0 ]; then
        exit 1
    fi
    echo "OK: topology transitions go through atomic composite commands"

# Gate: metrics are emitted through the typed handles generated by
# define_metrics! (frogdb-types/src/metrics), never by raw string-name
# recorder calls. A raw call re-opens registry drift (unregistered names,
# dead HELP text) and the first-caller-fixes-arity panic class the typed
# chokepoint closed (proposal 35). Allowed: the recorder trait + backend
# implementations (they ARE the seam), the registry/macro, and test dirs.
lint-metrics-chokepoint:
    #!/usr/bin/env bash
    set -uo pipefail
    crates="{{server-dir}}/crates"
    allow=(
        "types/src/traits/metrics.rs"
        "types/src/metrics/"
        "metrics-derive/src/lib.rs"
        "telemetry/src/prometheus_recorder.rs"
        "telemetry/src/otlp.rs"
        # Follow-up (proposal 35): raw sites in files other work owned during
        # the migration. Migrate to typed handles, then remove from this list
        # and from RAW_EMISSION_EXEMPT in telemetry/tests/metrics_usage.rs.
        "vll/src/coordinator.rs"
        "vll/src/traits.rs"
        "server/src/vll_adapter.rs"
    )
    status=0
    while IFS= read -r line; do
        f="${line%%:*}"
        skip=0
        for a in "${allow[@]}"; do
            case "$f" in *"$a"*) skip=1 ;; esac
        done
        [ "$skip" -eq 1 ] && continue
        if [ "$status" -eq 0 ]; then
            echo "ERROR: raw metric emission outside the typed chokepoint:" >&2
        fi
        echo "$line" >&2
        status=1
    done < <(grep -rnE '\.(increment_counter|record_gauge|record_histogram)\(' \
        --include='*.rs' "$crates" | grep -v '/tests/' | grep -vE ':[0-9]+: *//')
    if [ "$status" -ne 0 ]; then
        echo >&2
        echo "       Emit through the typed handle instead:" >&2
        echo "       frogdb_types::metrics::definitions::<Metric>::inc/set/observe(...)" >&2
        echo "       (new metrics are declared in frogdb-types/src/metrics/definitions.rs)" >&2
        exit 1
    fi
    echo "OK: metric emission goes through the typed handles"

# =============================================================================
# Aggregate CI
# =============================================================================

# Fast pre-commit checks (format + lint only)
pre-commit: fmt-check fmt-py-check lint lint-py sync-toolchain-check lint-no-typed-unwrap lint-keyspace-notify-routing lint-script-gate

# Run all checks (CI)
check-all: fmt-check fmt-py-check lint lint-py sync-toolchain-check lint-no-typed-unwrap lint-keyspace-notify-routing lint-script-gate deny test-all generate-check

# Alias: CI
alias ci := check-all
