---
name: check
description: >
  Run build checks, linting, formatting, and tests for the FrogDB project using
  the Justfile. Handles smart scoping (targeting specific crates based on what
  changed), sccache troubleshooting, nextest configuration, and all test types
  (unit, integration, proptest, Shuttle concurrency, Turmoil simulation). Use
  this skill whenever the user asks to check, lint, format, test, verify, or
  validate their code changes — even partial requests like "run tests" or "does
  this compile". Also use when builds fail with sccache errors or test timeouts.
---

# FrogDB Check / Lint / Test

All build, lint, format, and test commands go through `just` (see `Justfile` at repo root).
Never run `cargo` directly — the Justfile sets required environment variables for RocksDB,
libclang, DYLD_LIBRARY_PATH, and sccache.

## Quick Reference

| Task | Command | Scoped variant |
|------|---------|----------------|
| Type-check | `just check` | `just check frogdb-core` |
| Build | `just build` | — |
| Format (Rust) | `just fmt` | `just fmt frogdb-core` |
| Format check | `just fmt-check` | `just fmt-check frogdb-core` |
| Format (Python) | `just fmt-py` | — |
| Clippy | `just lint` | `just lint frogdb-core` |
| Clippy + Ruff | `just lint` + `just lint-py` | — |
| Tests | `just test` | `just test frogdb-server test_name` |
| Concurrency | `just concurrency` | — (Shuttle + Turmoil) |
| Full test suite | `just test-all` | — (test + concurrency) |
| License/security | `just deny` | — |
| Codegen freshness | `just generate-check` | — |
| **Full CI** | `just check-all` | — (everything) |

## Smart Scoping

When the user asks to "check" or "test" without specifying scope, determine what changed and
target accordingly. This is faster and gives more relevant output than running everything.

### Step 1: Identify changed files

```bash
# Unstaged + staged changes
git diff --name-only HEAD

# Or if comparing to main
git diff --name-only main...HEAD
```

### Step 2: Map files to crates

Files under `frogdb-server/crates/<name>/` belong to crate `frogdb-<name>`. Key mappings:

| Path prefix | Crate |
|-------------|-------|
| `frogdb-server/crates/server/` | `frogdb-server` |
| `frogdb-server/crates/core/` | `frogdb-core` |
| `frogdb-server/crates/protocol/` | `frogdb-protocol` |
| `frogdb-server/crates/types/` | `frogdb-types` |
| `frogdb-server/crates/persistence/` | `frogdb-persistence` |
| `frogdb-server/crates/cluster/` | `frogdb-cluster` |
| `frogdb-server/crates/replication/` | `frogdb-replication` |
| `frogdb-server/crates/commands/` | `frogdb-commands` |
| `frogdb-server/crates/acl/` | `frogdb-acl` |
| `frogdb-server/crates/scripting/` | `frogdb-scripting` |
| `frogdb-server/crates/vll/` | `frogdb-vll` |
| `frogdb-server/crates/debug/` | `frogdb-debug` |
| `frogdb-server/crates/telemetry/` | `frogdb-telemetry` |
| `frogdb-server/crates/testing/` | `frogdb-testing` |
| `frogdb-server/crates/test-harness/` | `frogdb-test-harness` |
| `frogdb-server/crates/redis-regression/` | `frogdb-redis-regression` |

### Step 3: Decide scope

- **1-3 crates changed:** Scope check/lint/test to those crates individually
- **Many crates or shared code changed** (types, protocol, Cargo.toml, Justfile): Run workspace-wide
- **Always run `just fmt-check` workspace-wide** — it's fast and catches cross-crate formatting
- **Run `just concurrency`** when `frogdb-core` or `frogdb-server` changed
- **Run `just generate-check`** when ops/, telemetry, config structs, or metrics changed
- **Run `just lint-py` / `just fmt-py-check`** when any `.py` file changed

### Step 4: Run checks

Typical scoped verification for a change to `frogdb-core`:

```bash
just fmt-check                    # always workspace-wide (fast)
just lint frogdb-core             # scoped clippy
just test frogdb-core             # scoped tests
just concurrency                  # core has Shuttle tests
```

Typical full verification (or when in doubt):

```bash
just check-all                    # runs everything: fmt, lint, deny, test-all, generate-check
```

## Test Types

### Unit & Integration Tests (`just test`)

Standard Rust tests run via `cargo nextest`. The test harness creates in-process FrogDB servers
(4 shards by default) using `TestServer::start_standalone_with_config()`.

- **Timeout:** 15s hard kill (5s slow warning × 3)
- **Threads:** 8 concurrent (macOS ephemeral port limit)
- **Pattern matching:** `just test frogdb-server test_publish` — regex, not substring
- **Config:** `.config/nextest.toml`

Important test groups with extended timeouts:
- **Cluster integration** (`integration_cluster::`): 120s hard kill, max 2 threads, 2 retries.
  These are known to be flaky in dev environments (port exhaustion, leader election timeouts).
- **Replication** (`integration_replication::`): 45s hard kill

Use hash tags `{tag}key` when tests need multiple keys in the same shard (4 shards default,
`allow_cross_slot_standalone = false`).

### Shuttle — Deterministic Concurrency (`just concurrency`)

Shuttle replays all possible thread interleavings to find concurrency bugs. It swaps out
`std::sync` primitives (Mutex, RwLock, Arc, atomics) with deterministic versions.

- **Location:** `frogdb-server/crates/core/tests/concurrency.rs`
- **Feature flag:** `shuttle` on `frogdb-core`
- **Runs:** `cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'`
- **Pattern:** `check_random(|| { ... }, iterations)` — runs 1000+ times per test

### Turmoil — Network Simulation (`just concurrency`)

Turmoil simulates network conditions (delays, packet loss, reordering) for distributed systems
testing. It swaps the TCP layer with a simulated network.

- **Location:** `frogdb-server/crates/server/tests/simulation.rs`
- **Feature flag:** `turmoil` on `frogdb-server`
- **Runs:** `cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'`
- **Uses:** `rstest` for parametrized chaos configurations, linearizability checking

### Proptest — Property-Based Testing

Proptest generates random inputs to find edge cases. Regression files track previously-found
failures so they're always re-tested.

- **Locations:** `frogdb-server/crates/core/tests/proptest_*.rs` (4 files),
  `frogdb-server/crates/server/tests/property_tests.rs`,
  `frogdb-server/crates/protocol/tests/proptest_protocol.rs`
- **Regression files:** `proptest-regressions/` directories (committed to git)
- **Runs as part of:** `just test` (no special flags needed)

## sccache

The Justfile auto-detects sccache via `which sccache` and sets `RUSTC_WRAPPER` automatically.

### When sccache causes problems

If you see errors like:
- `"Operation not permitted (os error 1)"`
- `"sccache: error: ..."`
- `"failed to execute rustc"`
- Any unexplained build failure that works on retry

**Fix:** Bypass sccache for that command:

```bash
RUSTC_WRAPPER="" just <recipe>
```

### Root cause

Homebrew-installed sccache (precompiled bottle) can fail on macOS Sequoia 15.5 due to
codesigning/SIP restrictions. Installing from source (`cargo install sccache`) produces a
locally-compiled binary at `~/.cargo/bin/sccache` that works without issues.

### Useful commands

```bash
just sccache-stats    # Cache hit/miss rates
just sccache-zero     # Reset counters (keep cache)
just sccache-clear    # Nuke the cache entirely
```

## When to Defer to Other Skills/Tools

| Situation | What to do |
|-----------|------------|
| Jepsen distributed systems tests | Use the `/jepsen-testing` skill |
| Fuzz testing | `cd fuzz && cargo +nightly fuzz run <target> -- -max_total_time=30` (requires DYLD/ROCKSDB/SNAPPY env vars) |
| Redis compatibility tests | `just redis-compat` (future: separate skill) |
| Browser integration tests | `just test-browser` (requires chromedriver on port 9515) |
| Benchmarks / load testing | `just bench`, `just benchmark`, `just profile-load` |
| Docker builds | `just docker-build-prod`, `just docker-build-debug`, etc. |

## Recommended Verification Workflow

After finishing a task, run checks in this order (fast → slow, cheap → expensive):

1. **`just fmt-check`** — instant, catches formatting issues before anything compiles
2. **`just lint [crate]`** — type-checks + clippy, catches most issues
3. **`just test [crate] [pattern]`** — run relevant tests
4. **`just concurrency`** — if core/server changed (Shuttle + Turmoil)
5. **`just generate-check`** — if ops/telemetry/metrics changed
6. **`just deny`** — if dependencies changed

Or just run `just check-all` to do everything at once.
