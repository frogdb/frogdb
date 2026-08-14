# FrogDB Justfile

# libclang is required by bindgen (used by librocksdb-sys). macOS: brew install llvm
# Linux: apt install libclang-dev (LLVM 18). When LIBCLANG_PATH is set, clang-sys
# searches ONLY that directory, so a wrong default breaks the build — the fallback
# must match the platform's real libclang location.
libclang-default := if os() == "macos" { "/opt/homebrew/opt/llvm/lib" } else { "/usr/lib/llvm-18/lib" }
export LIBCLANG_PATH := env("LIBCLANG_PATH", libclang-default)

# DYLD_LIBRARY_PATH needed at runtime for librocksdb-sys build script to find libclang.dylib
# Note: just's export doesn't propagate DYLD_* vars on macOS (SIP strips them), so this is
# used inline in recipes that need it
dyld-env := "DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib"

# System RocksDB: set FROGDB_SYSTEM_ROCKSDB=1 to link against system-installed RocksDB
# Optionally set FROGDB_LIB_DIR to override the library path (default: /opt/homebrew/lib)
# Defaults on only for macOS (Homebrew rocksdb); Linux distros ship RocksDB versions too old
# for librocksdb-sys, so Linux builds from vendored source unless explicitly overridden.
system-rocksdb-default := if os() == "macos" { "1" } else { "" }
use-system-rocksdb := env("FROGDB_SYSTEM_ROCKSDB", system-rocksdb-default)
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

# Install the shared VS Code config into .vscode/ (git-ignored — see contrib/vscode/README.md)
vscode-setup:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .vscode website/.vscode
    cp contrib/vscode/root/*.json .vscode/
    cp contrib/vscode/website/*.json website/.vscode/
    echo "installed VS Code config into .vscode/ and website/.vscode/"

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
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{crate}}" = "frogctl" ]; then
      echo "frogctl is excluded from the default suite; use: just frogctl-test" >&2
      exit 2
    fi
    {{dyld-env}} {{rocksdb-env}} cargo nextest run {{ if crate != "" { "-p " + crate } else { "--all" } }} {{ if pattern != "" { "-E 'test(/" + pattern + "/)'" } else { "" } }}

# Generate code coverage report (unit tests only)
coverage crate="" pattern="":
    {{dyld-env}} {{rocksdb-env}} cargo llvm-cov nextest {{ if crate != "" { "-p " + crate } else { "--all" } }} {{ if pattern != "" { "-E 'test(/" + pattern + "/)'" } else { "" } }} --html
    @echo "Report: target/llvm-cov/html/index.html"

# Generate lcov coverage data (for CI upload). Pulls frogctl back in — it is excluded
# from the default `just test`/`just check` dev loop, and that exclusion is about
# keeping the dev loop fast, not about hiding code from coverage measurement.
#
# `--output-path` does not create its parent directory, so on a fresh checkout (CI,
# or a clean `target/`) the run aborts with "No such file or directory". Worse, a
# stale lcov.info left in place from an earlier run would then be consumed as if it
# were fresh (that is how the 2026-07-28 near-empty artifact produced a meaningless
# coverage number). Create the directory and delete any stale artifact up front so a
# failed run leaves *no* file rather than a misleading one.
coverage-lcov:
    mkdir -p target/llvm-cov
    rm -f target/llvm-cov/lcov.info
    {{dyld-env}} {{rocksdb-env}} cargo llvm-cov nextest --all --features frogctl/cli-tests --ignore-default-filter --lcov --output-path target/llvm-cov/lcov.info

# Coverage *depth*: per-line exec counts + per-function test diversity
# (see CLAUDE.md "Coverage depth"). Local-only; uses its own target dir.
coverage-depth crate="" pattern="":
    ./scripts/coverage-depth.py run {{ if crate != "" { "--crate " + crate } else { "" } }} {{ if pattern != "" { "--pattern " + pattern } else { "" } }}
    ./scripts/coverage-depth.py report
    @echo "Report: target/llvm-cov/depth/index.html"

# Measure the coverage-depth pipeline on one crate before a full-suite run
coverage-calibrate crate:
    ./scripts/coverage-depth.py calibrate {{crate}}

# Unit tests for the coverage-depth pipeline (monomorphization dedupe, etc.)
test-coverage-depth:
    ./scripts/tests/test_coverage_depth.py

# Unit tests for the continuation-lock gate's Rust scanners (arm/variant parsing)
test-continuation-lock-gate:
    ./scripts/tests/test_continuation_lock_gate.py

# Run concurrency tests (Shuttle + Turmoil + generated workload sweep)
#
# The generated-workload step filters on the whole `concurrency_workload` module, not just its
# `seed_sweep_*` entry points, so `mod regressions`'s pinned reproducers run too — they were
# silently never executed while the filters named the sweeps individually.
concurrency:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/concurrency_workload/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/concurrency_pubsub/)'

# Replay a single concurrency repro file (seed + profile + config)
concurrency-repro FILE:
    {{dyld-env}} {{rocksdb-env}} REPRO_FILE={{FILE}} cargo nextest run -p frogdb-server --features turmoil --run-ignored all -E 'test(/replay_repro/)'

# Run turmoil-featured tests matching PATTERN (default: the generated-workload sweep)
concurrency-turmoil PATTERN='seed_sweep':
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/{{PATTERN}}/)'

# Run the nightly (1000+ seed) generated-workload sweep across all profiles (CI nightly
# tier, not part of `just concurrency`/`just test-all`). SEEDS overrides seeds-per-profile
# (default 250 x 5 profiles = 1250). OPS overrides ops-per-client and matches the harness's
# coded default of 150. (It was held at 75 while the workload runner's final-state readback
# raced long client scripts, reporting phantom "exactly-once delivery" loss above ~90 ops —
# .scratch/concurrency-testing/issues/11 Finding A, fixed by latching
# the readback to client completion; pinned by
# `regressions::regression_drain_capture_race_multiwaiter_ops_110_seed_0`.) clients/shards keep
# the harness defaults but are independently overridable via FROGDB_CONCURRENCY_CLIENTS /
# FROGDB_CONCURRENCY_SHARDS env vars (see frogdb-server/crates/server/tests/concurrency_workload.rs).
# Failing seeds each get a repro file under target/concurrency-repros/, replayable via
# `just concurrency-repro`.
#
# The nightly tier also raises the WGL bounded-search budget from MAX_WGL_STATES to
# MAX_WGL_STATES_NIGHTLY by default (override via FROGDB_CONCURRENCY_MAX_STATES) so state-bound
# downgrades to conservation-only checking are rarer than under the per-PR budget, and reports +
# thresholds the sweep-wide WGL downgrade ratio (FROGDB_WGL_DOWNGRADE_WARN_RATIO /
# FROGDB_WGL_DOWNGRADE_FAIL_RATIO; see `common::sweep_summary`) — a run where too many keys never
# got a real linearizability check now fails loudly instead of reporting a silent clean pass.
concurrency-nightly SEEDS='250' OPS='150':
    {{dyld-env}} {{rocksdb-env}} FROGDB_CONCURRENCY_SEEDS={{SEEDS}} FROGDB_CONCURRENCY_OPS_PER_CLIENT={{OPS}} cargo nextest run -p frogdb-server --features turmoil --run-ignored all -E 'test(/seed_sweep_nightly/)'

# Run the cluster state-machine property harness (frogdb-cluster `properties` module) at a
# raised proptest budget. The default suite runs the same tests at a small case count so the
# dev loop stays sub-second; this is the boosted pass, and the `cluster-nightly` workflow
# calls exactly this recipe so the budget lives in one place (PRD .scratch/cluster-correctness
# §8 D4). CASES sets PROPTEST_CASES; the harness falls back to its own default if it is unset,
# unparseable or zero, so a typo cannot silently reduce the run to nothing. The
# `cluster-proptest` nextest profile lifts the default profile's 120s hard kill for these
# tests only — at this budget the quadratic property runs for minutes on purpose.
cluster-proptest CASES='200000':
    {{dyld-env}} {{rocksdb-env}} PROPTEST_CASES={{CASES}} cargo nextest run --profile cluster-proptest -p frogdb-cluster -E 'test(/properties/)'

# Run the stateright models at their full exploration budget (CI nightly tier, not part of
# `just test`). The default suite carries only the bounded-depth smoke configs, which finish in
# well under 10s; this runs the `#[ignore]`d full-scope checks, which enumerate the whole
# reachable space and are the numbers recorded in each model's file header.
#
# Release profile: the models drive the *production* state machine (frogdb-cluster's
# `apply_command`) once per explored transition, so the run is dominated by debug-build overhead
# otherwise. `debug_assert_clean` stays on either way — cfg(test) covers it — so the invariant
# self-check is not lost by optimizing.
#
# Laptop-runnable by construction (PRD .scratch/cluster-correctness/ §8 D1) — the whole default
# pattern is well under a minute of compute; PATTERN narrows to a single model, e.g.
# `just model-check handoff_model_full_deep`.
model-check PATTERN='(handoff|failover)_model_full':
    {{dyld-env}} {{rocksdb-env}} cargo nextest run --release -p frogdb-cluster --run-ignored all -E 'test(/{{PATTERN}}/)' --no-capture

# Run `frogdb-replication`'s stateright models at their full exploration budget (CI nightly
# tier, not part of `just test`). Sibling of `model-check` above, which stays pointed at
# `frogdb-cluster`: the two crates' models are budgeted, floored and scheduled
# independently, and a single recipe that compiled both would make the cheap one pay for
# the expensive one on every invocation.
#
# The default suite carries only the bounded smoke configs; this runs the `#[ignore]`d
# full scopes recorded in each model's file header (feed_gate ~4M/~2.6M states,
# promotion ~8M/~6.2M states). The `model-check` nextest group pins them one at a time;
# each is a saturating parallel BFS.
#
# Release profile, same reasoning as `model-check`: the models drive the production
# decision functions once per explored transition, so a debug build spends most of the
# run in unoptimized checker plumbing. PATTERN narrows to one model or config, e.g.
# `just replication-model-check feed_gate_model_full_churn`.
replication-model-check PATTERN='(feed_gate|promotion)_model_full':
    {{dyld-env}} {{rocksdb-env}} cargo nextest run --release -p frogdb-replication --run-ignored all -E 'test(/{{PATTERN}}/)' --no-capture

# Run the replication property harnesses at a raised proptest budget: the link harness
# (frogdb-replication `properties`, R1-R5) and the self-fence harness
# (frogdb-replication-runtime `properties`, R6) — replication-correctness issues 04 and 05.
# The default suite runs the same tests at a small case count so the dev loop stays
# sub-second; this is the boosted pass, and the `replication-nightly` workflow calls exactly
# this recipe so the budget lives in one place. CASES sets PROPTEST_CASES; the harnesses fall
# back to their own defaults if it is unset, unparseable or zero, so a typo cannot silently
# reduce the run to nothing. The `replication-proptest` nextest profile lifts the default
# profile's hard kill for these tests only — at this budget R1 runs for minutes on purpose.
#
# Debug profile, unlike the model checks: R1's oracle is `debug_assert_view_clean`, which the
# production seams compile out under `debug_assertions = off`. A release run would exercise
# the actions and check nothing at the seams.
replication-proptest CASES='200000':
    {{dyld-env}} {{rocksdb-env}} PROPTEST_CASES={{CASES}} cargo nextest run --profile replication-proptest -p frogdb-replication -p frogdb-replication-runtime -E 'test(/properties/)'

# Run the seed-driven cluster fault-scheduler sweep (frogdb-server
# `simulation::scheduler`, cluster-correctness issue 09). One u64 seed derives a whole
# turmoil run — fault family, which links are held or slowed and when, which nodes are
# SIGKILLed and restarted, per-node Raft timer skew, and the client workload — so a sweep
# is just a range of seeds. Each seed ends in a quiesce check: the `frogdb-cluster`
# invariant catalog (`check_hard`, via DEBUG CLUSTER CHECK) on every surviving node, plus
# the cross-node checks a single-node catalog cannot express (client-observed epoch
# monotonicity, single-writer-per-slot over the run history).
#
# SEEDS is the whole budget, in one place (PRD .scratch/cluster-correctness §8 D4): the
# `cluster-nightly` workflow calls this recipe rather than duplicating a case count. A
# six-seed smoke sweep runs in the default suite so the scheduler cannot rot, and every
# seed that ever failed is replayed forever from
# `frogdb-server/crates/server/tests/simulation/cluster-regression-seeds.txt`.
#
# Seeds are split across CLUSTER_SEEDS_JOBS worker threads inside the one test (each
# turmoil sim is single-threaded and self-contained); CLUSTER_SEEDS_START shifts the
# range when you want a fresh block rather than a re-run of the same one.
cluster-seeds SEEDS='500':
    {{dyld-env}} {{rocksdb-env}} CLUSTER_SEEDS={{SEEDS}} cargo nextest run --profile cluster-seeds -p frogdb-server --features turmoil --run-ignored all -E 'test(simulation::scheduler::test_cluster_scheduler_seed_sweep)'

# Run the seeded replication DST sweep (`simulation::replication_scheduler`, the
# replication arm of the same scheduler — see `cluster-seeds` above for the shared half).
#
# Each seed derives a whole primary-and-two-replicas run: which fault family, which
# partial-sync boundary the reconnect lands on, which full-sync payload shape the primary
# ships, where the link dies inside a sync, whether the self-fence and
# `min-replicas-to-write` are armed, and the client workload. At quiesce every surviving
# node is asked `DEBUG REPLICATION CHECK` (the invariant catalog, issue 02) and the run
# history is checked for XREPL-1 (no acked write missing from a promoted node), XREPL-2 (a
# replica's applied history is a prefix of the primary's) and XREPL-3 (WAIT never answered
# past connected_slaves — spec GAP-5 at level 4).
#
# SEEDS is the whole budget, in one place, exactly as for `cluster-seeds`: the
# `replication-nightly` workflow calls this recipe rather than duplicating a count. A
# seven-seed smoke sweep — one per fault family — runs in the default suite so the arm
# cannot rot between nightlies.
#
# There is deliberately NO replication regression-seed file yet: PRD
# .scratch/replication-correctness §8 D9 holds it, and every EXPECTED-FAILURE muzzle with
# it, until cluster-correctness issue 23 (same-seed fingerprints diverge under host load)
# closes — a muzzle is a claim about reproducibility, and that claim is not currently
# safe to make. Until then a failing nightly seed is triaged by hand.
#
# Named gaps are a different thing and are allowed: they key on the *signature* of an
# already-filed defect, never on a seed, so they cannot hide a seed that fails some other
# way. A run that reaches one prints "N of M seeds stopped at a known gap — <issue>" on
# stderr and stays green; the count going up is worth a look, a new signature is a finding.
#
# Seeds are split across REPLICATION_SEEDS_JOBS worker threads inside the one test (each
# turmoil sim is single-threaded and self-contained); REPLICATION_SEEDS_START shifts the
# range when you want a fresh block rather than a re-run of the same one. Replay one seed
# with its whole fingerprint via REPLICATION_SEED_TRACE=1.
replication-seeds SEEDS='500':
    {{dyld-env}} {{rocksdb-env}} REPLICATION_SEEDS={{SEEDS}} cargo nextest run --profile replication-seeds -p frogdb-server --features turmoil --run-ignored all -E 'test(simulation::replication_scheduler::test_replication_scheduler_seed_sweep)'

# Run the full test suite (unit + integration + concurrency + simulation)
test-all: test concurrency

# Run tokio-coz causal profiler tests (requires tokio_unstable, set workspace-wide in
# .cargo/config.toml — this recipe no longer exports its own RUSTFLAGS, which used to fork a
# second copy of the build cache and made the `cargo sweep` bracketing necessary)
test-coz:
    cargo test -p tokio-coz

# Run browser integration tests (requires chromedriver running on port 9515)
test-browser:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-browser-tests --features browser-tests

# Run all benchmarks
bench:
    {{dyld-env}} {{rocksdb-env}} cargo bench -p frogdb-benches

# =============================================================================
# Locked core areas (see CLAUDE.md "Locked core areas")
# =============================================================================

# Record an area's warm inner-loop cost (check + test-binary build medians)
loop-cost area:
    RUSTC_WRAPPER="" ./scripts/loop-cost.py {{area}}

# Run one hardening area's crate tests (core command profile).
# Crate lists grow as extraction phases land (frogdb-txn, frogdb-recovery, ...).
core-test area pattern="":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{area}}" in
      txn)         crates="-p frogdb-vll -p frogdb-txn" ;;
      persistence) crates="-p frogdb-persistence -p frogdb-recovery" ;;
      replication) crates="-p frogdb-replication -p frogdb-replication-runtime" ;;
      cluster)     crates="-p frogdb-cluster -p frogdb-cluster-runtime" ;;
      *) echo "unknown area: {{area}} (txn|persistence|replication|cluster)" >&2; exit 2 ;;
    esac
    {{dyld-env}} {{rocksdb-env}} cargo nextest run $crates {{ if pattern != "" { "-E 'test(/" + pattern + "/)'" } else { "" } }}

# Run one hardening area's end-to-end server tests (core command profile)
core-test-e2e area:
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{area}}" in
      txn)         filter="test(integration_transactions::)" ;;
      persistence) filter="test(integration_persistence::)" ;;
      replication) filter="test(integration_replication::)" ;;
      cluster)     filter="binary(/^cluster_/)" ;;
      *) echo "unknown area: {{area}} (txn|persistence|replication|cluster)" >&2; exit 2 ;;
    esac
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server -E "$filter"

# Mutation-test one crate (testbox-class workload; config in .cargo/mutants.toml)
mutants crate *args:
    mkdir -p target/mutants/{{crate}}
    {{dyld-env}} {{rocksdb-env}} cargo mutants -p {{crate}} --output target/mutants/{{crate}} {{args}}

# Mutate only this branch's diff vs origin/main (PR-viable cost)
mutants-diff crate:
    mkdir -p target/mutants/{{crate}}-diff
    git diff $(git merge-base origin/main HEAD) > target/mutants-diff.patch
    {{dyld-env}} {{rocksdb-env}} cargo mutants -p {{crate}} --in-diff target/mutants-diff.patch --output target/mutants/{{crate}}-diff

# Enforce an area's mutation score from a completed run (threshold e.g. 0.90)
mutants-gate crate threshold:
    ./scripts/mutants-gate.py target/mutants/{{crate}}/mutants.out/outcomes.json --min-score {{threshold}}

# Run the Redis compat suite on its own (it is also part of the default `just test`)
regression pattern="":
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-redis-regression {{ if pattern != "" { "-E 'test(/" + pattern + "/)'" } else { "" } }}

# Type-check the compat suite without running it
regression-check:
    {{dyld-env}} {{rocksdb-env}} cargo check -p frogdb-redis-regression --all-targets

# Gate: the specs and the tests that force them must agree, both ways.
# Every `Forced by` test in specs/*.md must
# exist and carry a `// FM-<AREA>-NNN` tag; every tag must name a spec row.
# Builds the listed crates' test binaries (~15-25s warm, no test execution).
# Runs its own fixture test first: a green tree exercises the invariant
# vocabulary check only in the passing direction, so the failing directions
# (dangling / cross-area `INV-*`) are pinned separately, in under a second.
lint-spec: test-spec-lint
    {{dyld-env}} {{rocksdb-env}} RUSTC_WRAPPER="" ./scripts/spec-lint.py

# Unit tests for the spec lint's fixture-pinned checks
test-spec-lint:
    ./scripts/tests/test_spec_lint.py

# Type-check the Quint design models (specs/quint/*.qnt)
#
# A no-op until the first model lands (the cluster phase): the models are the
# design layer of the formal spec, and CI wiring arrives with them rather than
# with this empty directory.
quint-check:
    #!/usr/bin/env bash
    set -uo pipefail
    shopt -s nullglob
    models=(specs/quint/*.qnt)
    if [ ${#models[@]} -eq 0 ]; then
        echo "quint-check: no models under specs/quint/ yet — nothing to type-check"
        exit 0
    fi
    status=0
    for model in "${models[@]}"; do
        echo "quint typecheck $model"
        quint typecheck "$model" || status=1
    done
    exit $status

# Per-model invariant lists for the Quint design models. Quint has no "all
# invariants" declaration to introspect, so these are the single source both
# `quint-run` (sampled smoke) and `quint-verify-*` (exhaustive nightly bound)
# key off of — keep in sync with the `val inv_*` declarations in each .qnt
# file (specs/quint/*.qnt).
quint-admission-invariants := "inv_no_usurper inv_single_routable_group inv_restart_deterministic inv_meet_no_absorption"
quint-migration-failover-invariants := "inv_slot_owner_valid inv_migration_endpoints_valid inv_handoff_owned inv_handoff_seq_never_reused inv_epoch_monotone inv_epoch_never_decreases inv_abort_repatriates inv_repatriating_well_formed inv_last_failover_demoted inv_last_failover_fenced inv_graceful_failover_barriered inv_complete_requires_drained inv_feed_hold_bounded"

# Smoke-test the Quint design models on every CI run (design doc
# .scratch/formal-spec/2026-08-12-formal-state-spec-design.md §3 cadence):
# each model's own named `quint test` suite, plus a small bounded+sampled
# `quint run` that actually checks the model's invariants (not just
# simulates — `quint run` defaults `--invariant` to `"true"`, i.e. no check,
# unless told otherwise). Mirrors `quint-check`'s glob loop. Cheap enough for
# the PR lane, unlike `quint verify` below (Apalache/SMT) — that is the
# nightly tier.
quint-run:
    #!/usr/bin/env bash
    set -uo pipefail
    shopt -s nullglob
    models=(specs/quint/*.qnt)
    if [ ${#models[@]} -eq 0 ]; then
        echo "quint-run: no models under specs/quint/ yet — nothing to run"
        exit 0
    fi
    status=0
    for model in "${models[@]}"; do
        echo "quint test $model"
        quint test "$model" || status=1
        case "$model" in
          */cluster_admission.qnt) invariants="{{quint-admission-invariants}}" ;;
          */cluster_migration_failover.qnt) invariants="{{quint-migration-failover-invariants}}" ;;
          *) invariants="" ;;
        esac
        echo "quint run $model --max-samples=200 --max-steps=20${invariants:+ --invariants $invariants}"
        quint run "$model" --max-samples=200 --max-steps=20 ${invariants:+--invariants $invariants} || status=1
    done
    exit $status

# Bounded *exhaustive* model checking (Apalache) of one Quint design model —
# the nightly tier of formal verification (`quint-run` above is the sampled
# per-PR tier). Unlike sampling, `quint verify` explores every reachable
# state and enabled transition up to MAX_STEPS, not just sampled paths; the
# per-state SMT solve cost grows steeply with depth, so this runs
# minutes-to-tens-of-minutes per invariant rather than the PR lane's sub-10s
# budget — that is why it is nightly, not per-PR.
#
# CARRIED REQUIREMENT (Task 2 review finding N1, binding — see
# .superpowers/sdd/2026-08-13-phase2-cluster-quint-plan/task-2-report.md and
# progress.md): MAX_STEPS must stay >= 6. Depth 3 was proven vacuous for
# inv_repatriating_well_formed and half of inv_abort_repatriates on
# cluster_migration_failover.qnt — both need 4-5 transitions before the
# property is even checkable. Do not shrink this default to make a run
# finish faster; a deep bound that times out is reported as inconclusive
# (below), not silently downgraded.
#
# One invariant per `quint verify` invocation, each under its own TIMEOUT:
# a combined `--invariants` sweep reports one merged verdict, so a single
# SMT-infeasible conjunct would hide every other invariant's result. A
# timed-out invariant is reported as such (inconclusive, not a violation)
# and the loop continues; the recipe still exits nonzero so CI surfaces it.
quint-verify-model model invariants MAX_STEPS='6' TIMEOUT='1200':
    #!/usr/bin/env bash
    set -uo pipefail
    if [ "{{MAX_STEPS}}" -lt 6 ]; then
        echo "quint-verify-model: MAX_STEPS must be >= 6 (Task 2 review finding N1); got {{MAX_STEPS}}" >&2
        exit 1
    fi
    status=0
    for inv in {{invariants}}; do
        echo "=== quint verify {{model}} --invariant=$inv --max-steps={{MAX_STEPS}} (timeout {{TIMEOUT}}s) ==="
        timeout {{TIMEOUT}} quint verify "{{model}}" --invariant="$inv" --max-steps={{MAX_STEPS}}
        rc=$?
        if [ $rc -eq 124 ]; then
            echo "=== $inv: TIMED OUT after {{TIMEOUT}}s at depth {{MAX_STEPS}} (inconclusive, not a violation) ==="
            status=1
        elif [ $rc -ne 0 ]; then
            status=1
        fi
    done
    exit $status

# Nightly Apalache sweep, admission model only (4 invariants — lighter than
# the migration/failover composite below). Split into its own recipe/CI job
# so the two models' timeout budgets don't compete.
quint-verify-admission MAX_STEPS='6' TIMEOUT='1200': (quint-verify-model "specs/quint/cluster_admission.qnt" quint-admission-invariants MAX_STEPS TIMEOUT)

# Nightly Apalache sweep, migration/failover composite model (12 invariants —
# the heavier of the two; see quint-verify-model's docstring for why depth
# 6 can run long here).
quint-verify-migration-failover MAX_STEPS='6' TIMEOUT='1200': (quint-verify-model "specs/quint/cluster_migration_failover.qnt" quint-migration-failover-invariants MAX_STEPS TIMEOUT)

# Both models' nightly Apalache sweep, sequentially. CI itself runs the two
# halves as separate jobs (quint_verify.py) so a hang in one model's sweep
# doesn't eat the other's timeout headroom; this combined target exists for
# a single local invocation.
quint-verify: quint-verify-admission quint-verify-migration-failover

# Report-only: run the quint-connect conformance harness's quarantined
# (#[ignore]d) traces (frogdb-server/crates/cluster/tests/quint_conformance.rs).
# Most are expected to keep failing until issues 15/17/19/20/26 and the
# ghost-field issue land — a failing run here is the *expected* signal, not a
# bug in this recipe; a test flipping to pass here (and nowhere else yet) is
# the "un-ignore me" reminder those issues' acceptance criteria ask for.
quint-conformance-quarantine:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-cluster --run-ignored ignored-only -E 'binary(quint_conformance)'

# Run frogctl's tests (excluded from the default suite during the campaign)
frogctl-test:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogctl --features cli-tests --ignore-default-filter

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
#
# Depends on `lint-gates` rather than re-listing the compile-free gates: the two
# hand-maintained lists had already drifted (`lint-no-typed-unwrap`,
# `lint-keyspace-notify-routing` and `lint-script-gate` ran in `lint-gates` but
# not in `lint`, contradicting agents/seam-lints.md). One list, so `lint` is
# always a superset of `lint-gates`.
lint crate="": lint-gates lint-turmoil-features lint-turmoil lint-spec quint-check
    {{dyld-env}} {{rocksdb-env}} cargo clippy {{ if crate != "" { "-p " + crate } else { "--all-targets" } }} -- -D warnings

# Gate: the compile-free subset of the seam-lint family — every `lint-*` gate
# except `lint-spec` (builds test binaries) and the turmoil lints
# (compile via clippy, or exist only to police the turmoil feature). These are
# grep/regex checks over source text, so the whole set runs in well under a
# second (see agents/seam-lints.md) and is cheap enough to run
# unconditionally on every commit, unlike `lint` (clippy compiles the
# workspace). Wired into lefthook pre-commit with no CLAUDECODE skip.
lint-gates: lint-info-seam lint-redirect-seam lint-pubsub-confirmation-seam lint-failover-atomicity lint-metrics-chokepoint lint-format-float lint-clock-seam lint-durable-ack lint-nested-config lint-error-sanitize lint-no-typed-unwrap lint-keyspace-notify-routing lint-script-gate lint-continuation-lock
    @echo "OK: seam-lint gates passed"

# Gate: turmoil-featured test bodies (frogdb-server/crates/server/tests/simulation.rs)
# are behind #[cfg(feature = "turmoil")], a non-default feature the plain clippy
# pass above never enables — those bodies escape clippy entirely and only surface
# rustc warnings via the nextest build. Lint them explicitly with the feature on.
lint-turmoil:
    {{dyld-env}} {{rocksdb-env}} cargo clippy -p frogdb-server --features turmoil --tests -- -D warnings

# Gate: the turmoil network swap is a cargo feature, so every crate in the
# dependency chain has to forward it. A missing forward compiles the production
# tokio stack into the "simulation" — the sims still pass, they just stop
# simulating. Enforced in the manifests:
#   1. a crate depending on frogdb-net must declare a `turmoil` feature;
#   2. a crate that has a `turmoil` feature must forward `<dep>/turmoil` for
#      every internal dependency that offers one (frogdb-net, frogdb-config, …).
# The compile-time counterpart lives in crates/server/src/net.rs (a type-identity
# assertion against turmoil::net) — this gate catches the wiring, that one
# catches the types.
lint-turmoil-features:
    #!/usr/bin/env bash
    set -uo pipefail
    status=0

    # --others picks up manifests of crates added but not yet committed;
    # --exclude-standard keeps target/ and friends out.
    manifests=$(git ls-files --cached --others --exclude-standard -- '*Cargo.toml')

    # Print a manifest's `turmoil = [...]` feature declaration (empty if absent).
    turmoil_feature() {
        awk '
            /^\[features\]/ { in_features = 1; next }
            /^\[/           { in_features = 0 }
            in_features && /^turmoil[[:space:]]*=/ { found = 1 }
            found { print }
            found && /\]/ { exit }
        ' "$1"
    }

    # Print a manifest's dependency names (normal/dev/build/target, never
    # [workspace.dependencies] — that section is not a dependency edge).
    deps_of() {
        awk '
            /^\[/ { in_deps = ($0 ~ /dependencies\]$/ && $0 !~ /^\[workspace/); next }
            in_deps && /^[A-Za-z0-9_-]+[[:space:]]*[=.]/ {
                name = $0
                sub(/[[:space:]]*[=.].*$/, "", name)
                print name
            }
        ' "$1"
    }

    package_name() { awk -F'"' '/^name[[:space:]]*=/ { print $2; exit }' "$1"; }

    # Internal crates that offer a `turmoil` feature.
    providers=""
    for m in $manifests; do
        [ -n "$(turmoil_feature "$m")" ] || continue
        providers="$providers $(package_name "$m")"
    done

    for m in $manifests; do
        decl=$(turmoil_feature "$m")
        deps=$(deps_of "$m")
        if [ -z "$decl" ]; then
            if echo "$deps" | grep -qx 'frogdb-net'; then
                echo "ERROR: $m depends on frogdb-net but declares no 'turmoil' feature," >&2
                echo "       so the simulation swap can never reach it." >&2
                status=1
            fi
            continue
        fi
        self=$(package_name "$m")
        for p in $providers; do
            [ "$p" = "$self" ] && continue
            echo "$deps" | grep -qx "$p" || continue
            case "$decl" in *"\"$p/turmoil\""*) continue ;; esac
            echo "ERROR: $m: the 'turmoil' feature does not forward \"$p/turmoil\"." >&2
            status=1
        done
    done

    if [ "$status" -ne 0 ]; then
        echo >&2
        echo "       Add the missing forward to the crate's 'turmoil' feature, e.g." >&2
        echo '       turmoil = ["dep:turmoil", "frogdb-net/turmoil", ...]' >&2
        exit 1
    fi
    echo "OK: turmoil feature forwarding is wired through every dependent crate"

# Gate: INFO section content must come from a renderer (crates/server/src/info),
# never a post-hoc string patch. Rejects placeholder-anchor rewrites in the
# shard-local INFO builder and the scatter handlers.
lint-info-seam:
    #!/usr/bin/env bash
    set -euo pipefail
    files=( \
        "{{server-dir}}/crates/server/src/commands/info.rs" \
        "{{server-dir}}/crates/server/src/connection/scatter.rs" \
        "{{server-dir}}/crates/server/src/connection/info_handler.rs" \
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

# Generate all derived files (dashboard + Helm chart + Debian + workflows).
# dashboard-gen must precede helm-gen: the chart's bundled dashboard is a copy of
# frogdb-server/ops/grafana/frogdb-overview.json, so running helm-gen first leaves
# the copy a generation behind whenever the metric set changes.
generate: dashboard-gen helm-gen deb-gen workflow-gen

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

# Generate the website's Specifications section from specs/*.md
spec-gen:
    uv run website/scripts/spec-gen.py

# Verify the generated specification pages are up to date (for CI)
spec-gen-check:
    uv run website/scripts/spec-gen.py --check

# Re-vendor the upstream Redis command list (name/group/since) pinned to
# REDIS_COMPAT_TARGET. Requires network access; not part of docs-build/CI —
# run manually when REDIS_COMPAT_TARGET bumps.
redis-commands-vendor:
    uv run website/scripts/vendor-redis-commands.py

# Generate the command compatibility matrix by joining commands.json, the
# vendored Redis command list, and compat-exclusions.json. Must run after
# docs-gen (commands.json) and compat-gen (compat-exclusions.json); also pulls
# in spec-gen, so this recipe aggregates all three generators. docs-dev/
# docs-build also depend on spec-gen directly (belt-and-suspenders with the
# transitive pull-in above).
matrix-gen: docs-gen compat-gen spec-gen
    uv run website/scripts/matrix-gen.py

# Verify the generated command matrix is up to date (for CI)
matrix-gen-check: docs-gen-check compat-gen-check spec-gen-check
    uv run website/scripts/matrix-gen.py --check

# Run documentation site development server (installs deps if needed)
docs-dev: matrix-gen spec-gen
    cd website && [ -d node_modules ] || bun install
    cd website && bun run dev

# Build documentation site for production
docs-build: matrix-gen spec-gen
    cd website && bun run build

# Preview production build of documentation site
docs-preview:
    cd website && bun run preview

# Check for broken links in documentation
docs-link-check: docs-build
    cd website && bunx lychee --config ../lychee.toml --root-dir "$(pwd)/dist" dist/

# Verify repo code paths referenced in the docs actually exist (no build required)
docs-path-check:
    uv run website/scripts/docs-path-check.py

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
        size=$(du -sm "$dir" | cut -f1); \
        if [ "$size" -gt 100 ]; then \
            echo "Removing stale rocksdb build: $dir (${size}MB)"; \
            rm -rf "$dir"; \
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

# Generate the FrogDB CRD manifest (JSON — `generate-crd` emits JSON, and
# deploy/crd.json is the tracked artifact kustomize/helm reference)
operator-crd:
    {{dyld-env}} {{rocksdb-env}} cargo run --manifest-path frogdb-operator/Cargo.toml -- generate-crd > frogdb-operator/deploy/crd.json
    @echo "CRD written to frogdb-operator/deploy/crd.json"

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

# Verify the .scratch/ issue tracker is internally consistent: legal Status:
# values, Status: agrees with the open/|done/ subdirectory, every feature dir has
# a README.md with a State: line, no duplicate issue numbers.
# See agents/issue-tracker.md.
scratch-check:
    ./scripts/scratch-check.py

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
    pubsub_handler="{{server-dir}}/crates/server/src/connection/pubsub_conn_command.rs"
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

# Gate: topology transitions are atomic.
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
    saga_files="{{server-dir}}/crates/cluster-runtime/src/failure_detector.rs $src/connection/cluster.rs"
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

# Gate: exactly one float renderer in the workspace (issue 55).
#
# `INCRBYFLOAT`/`HINCRBYFLOAT` store the string they render, so the renderer is
# on both the reply path and the store path. When those were two different
# implementations, `SET k 0; INCRBYFLOAT k 0.1` replied `0.1` and stored
# `0.10000000000000001` — and the stored spelling, not the reply, is what the
# WAL persists and what crosses the replication link, so a replica that
# re-derived the value disagreed with the primary. Five copies had drifted.
# The one definition lives in frogdb-protocol, the crate every renderer on that
# path already depends on; everyone else re-exports it.
lint-format-float:
    #!/usr/bin/env bash
    set -uo pipefail
    crates="{{server-dir}}/crates"
    canonical="protocol/src/format.rs"
    hits=$(grep -rn 'fn format_float' --include='*.rs' "$crates" || true)
    extra=$(echo "$hits" | grep -v "$canonical" || true)
    if [ -n "$extra" ]; then
        echo "ERROR: format_float is defined outside frogdb-protocol:" >&2
        echo "$extra" >&2
        echo >&2
        echo "       Re-export the canonical renderer instead:" >&2
        echo "       pub use frogdb_protocol::format_float;" >&2
        exit 1
    fi
    count=$(echo "$hits" | grep -c "$canonical" || true)
    if [ "$count" -ne 1 ]; then
        echo "ERROR: expected exactly 1 format_float definition in $canonical, found $count" >&2
        exit 1
    fi
    echo "OK: one float renderer, in frogdb-protocol"

# Gate: server crates read the clock through the seam (frogdb-types/src/clock.rs),
# not the OS directly (determinism audit R5).
#
# Every deadline the server holds is compared against a `now`, and which clock
# that `now` came from decides whether a key expired, whether a waiter timed
# out, whether a node is FAILed, and what a TTL/TIME/XINFO reply says. Under a
# paused tokio runtime — how every simulated turmoil host runs — the timer's
# clock and the OS clock disagree, so a direct OS read makes its decision on a
# different timeline from the rest of the server and the run stops being
# reproducible. Both seam functions compile to the same reading as the OS clock
# when no paused runtime is present, so a converted site is free in production.
#
# Exemptions live in the script, per file, with a reason and a pinned count.
lint-clock-seam:
    ./scripts/clock-seam.py

# Gate: a raft write acked as durable used sync write options. Single-file pin on
# the openraft storage impl (cluster/src/storage.rs) — the durability ack is a
# callback, not a grep-able return value. See scripts/durable-ack.py.
lint-durable-ack:
    ./scripts/durable-ack.py

# Gate: no figment `.nested()` on a config source (it files a TOML file's tables
# under non-default profiles an extract() never reads). The one live site rides
# the named-gap warn idiom until round-2 issue 49 lands. See scripts/nested-config.py.
lint-nested-config:
    ./scripts/nested-config.py

# Gate: every CRLF-framed error frame (RESP2 Error / RESP3 SimpleError) in the
# encoder is built through frogdb_protocol::sanitize_error_message, so client
# error text cannot inject a second wire frame. RESP3 BlobError is length-framed
# and deliberately exempt. See scripts/error-sanitize.py.
lint-error-sanitize:
    ./scripts/error-sanitize.py

# Gate: every mutating shard-dispatch arm states a continuation-lock disposition.
# The 64 arms of the 11 shard `*Msg` enums are count-pinned per enum; the arms
# that reach store execution are named as GATE (must call
# `can_execute_during_lock`), EXEMPT (reason + a forcing test that must still
# exist), or a tracked named-gap bypass. A new or renamed arm moves the count and
# forces a classification. See scripts/continuation-lock-gate.py.
lint-continuation-lock:
    ./scripts/continuation-lock-gate.py

# =============================================================================
# Build/test execution mode
# =============================================================================

# Show the current mode, or set it: just build-mode testbox
build-mode *mode="":
    ./scripts/build-mode.sh {{mode}}

# =============================================================================
# Blacksmith Testboxes (remote Linux build/test VMs)
# =============================================================================
#
# The tb-* recipes require testbox mode (`just build-mode testbox`). For a
# one-off without switching the worktree, set BUILD_MODE=testbox.

# Warm up a testbox and record its ID for session-end cleanup
tb-warmup workflow="test-unit-tests-testbox.yml" *args="":
    @./scripts/require-testbox-mode.sh
    ./scripts/testbox-warmup.sh {{workflow}} {{args}}

# Run a command on the most recently warmed testbox: just tb-run "just test frogdb-server"
tb-run cmd:
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="$HOME/.local/bin:$PATH"
    ./scripts/require-testbox-mode.sh
    id=$(tail -1 "$(git rev-parse --git-dir)/blacksmith-testboxes" 2>/dev/null || true)
    [ -n "$id" ] || { echo "no testbox recorded; run 'just tb-warmup' first" >&2; exit 1; }
    # The testbox SSH session does not inherit the workflow's PATH; mise-managed
    # tools (just, cargo-nextest) live in the shims dir. CARGO_INCREMENTAL=0
    # matches the hydration build — a mismatch flips rustc flags and recompiles
    # every workspace crate on the first build of each SSH session (and bloats
    # the sticky-disk target/ with incremental artifacts).
    #
    # Keepalive: the run-testbox idle watchdog greps `ss` for the *external* SSH
    # port (a gateway mapping; sshd actually listens on :22), so it never sees
    # the live connection — the ~/.testbox-last-activity marker, touched only at
    # command start/end, is the sole activity signal. Without a mid-run touch,
    # any command longer than the idle timeout kills the box mid-run. The
    # background loop self-terminates when the remote shell exits (kill -0 $$).
    blacksmith testbox run --id "$id" '{ ( while kill -0 $$ 2>/dev/null; do touch ~/.testbox-last-activity; sleep 60; done ) </dev/null >/dev/null 2>&1 & export PATH="$HOME/.local/share/mise/shims:$PATH" CARGO_INCREMENTAL=0; } && '{{quote(cmd)}}

# Show status of the most recently warmed testbox
tb-status *args="":
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="$HOME/.local/bin:$PATH"
    id=$(tail -1 "$(git rev-parse --git-dir)/blacksmith-testboxes" 2>/dev/null || true)
    [ -n "$id" ] || { echo "no testbox recorded; run 'just tb-warmup' first" >&2; exit 1; }
    blacksmith testbox status --id "$id" {{args}}

# Stop all testboxes recorded for this worktree
tb-stop:
    ./scripts/testbox-cleanup.sh

# List active testboxes for the org
tb-list:
    #!/usr/bin/env bash
    export PATH="$HOME/.local/bin:$PATH"
    blacksmith testbox list

# =============================================================================
# Aggregate CI
# =============================================================================

# Fast pre-commit checks (format + lint only)
pre-commit: fmt-check fmt-py-check lint lint-py sync-toolchain-check lint-no-typed-unwrap lint-keyspace-notify-routing lint-script-gate scratch-check

# Run all checks (CI)
check-all: fmt-check fmt-py-check lint lint-py sync-toolchain-check lint-no-typed-unwrap lint-keyspace-notify-routing lint-script-gate scratch-check deny test-all generate-check

# Alias: CI
alias ci := check-all
