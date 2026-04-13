# Redis 8.6.0 Compatibility Coverage Audit

**Date:** 2026-04-13 **Upstream:** [redis/redis@8.6.0](https://github.com/redis/redis/tree/8.6.0)
**Goal:** reach a state where Redis compatibility is verified **entirely by Rust tests in
`redis-regression`** and the `testing/redis-compat/` TCL runner can be deleted.

This audit answers: *what work is left before we can delete `testing/redis-compat/`?*

**Status:** All five phases are complete. The TCL runner was deleted on 2026-04-08 (Phase 5).
Test-level gaps dropped from **~543 → 0** through intentional-exclusion documentation (Phase 1) and
gap-filling (Phase 3). New port files grew from 31 to 49 (Phase 2). The only remaining work is 4
deferred cluster files.

## The end state

- Every Redis 8.6.0 behavior that matters for FrogDB has a corresponding `#[tokio::test]` in
  `frogdb-server/crates/redis-regression/tests/*_tcl.rs`.
- Behaviors that don't matter (AOF, RDB, replication internals, module API, ...) are documented once
  as **out of scope** and never thought about again.
- `testing/redis-compat/`, `skiplist.txt`, `STATUS.md`, and `run_tests.py` are all deleted.

## Audit Tooling

`frogdb-server/crates/redis-regression/tests/audit_tcl.py` parses upstream `.tcl` files, extracts
`test {name} {tags}` blocks, and diffs them against the existing Rust port files using token-based
fuzzy matching (Jaccard + rust-coverage, >=0.5 score, >=2 token intersection). It also parses each
Rust port file's `## Intentional exclusions` section (added in Phase 1) and classifies any upstream
test whose name matches a documented exclusion as `documented` rather than as an unclassified gap.
`frogdb-server/crates/redis-regression/tests/show_missing.py` prints per-file gap details,
filterable by category (`--gaps`, `--documented`, `--tag-excluded`, `--all`). Both scripts live in
the `redis-regression` crate; the script is a *tracker* -- actual test execution happens via `cargo
test -p frogdb-redis-regression`.

**Note on gap counts:** fuzzy matching has ~10-15% false-positive and false-negative rates. Treat
numbers as approximations -- the categories and priorities are the actionable output.


## Part 5 -- Observability gaps identified (info_tcl.rs)

The `info_tcl.rs` port documents 27 upstream tests that exercise INFO metrics FrogDB does not yet
implement. These are categorized as potential observability improvements, not compatibility
blockers:

- **Per-command latency tracking** (6 tests): `latency-tracking` config, per-command p50/p99/p99.9
  percentiles.
- **Error/command stats** (10 tests): per-error-type `errorstat_*` counters,
  `rejected_calls`/`failed_calls` in commandstats, `total_error_replies`.
- **Client stats** (2 tests): `pubsub_clients`, `watching_clients`, `total_watched_keys` in INFO
  clients section.
- **Not applicable** (9 tests): Redis event loop metrics, DEBUG section, dict rehashing internals --
  architecture-specific to Redis's single-threaded model.

---

## How to re-run this audit

```bash
# Download Redis 8.6.0 source (~4 MB, idempotent)
mkdir -p /tmp/claude/redis-tcl && cd /tmp/claude/redis-tcl
curl -sSL https://github.com/redis/redis/archive/refs/tags/8.6.0.tar.gz | tar -xz

# Run the audit script
uv run --script frogdb-server/crates/redis-regression/tests/audit_tcl.py all \
  > /tmp/claude/audit_results.json

# Inspect a specific port's gaps
python3 frogdb-server/crates/redis-regression/tests/show_missing.py zset_tcl.rs --gaps
python3 frogdb-server/crates/redis-regression/tests/show_missing.py zset_tcl.rs --documented
python3 frogdb-server/crates/redis-regression/tests/show_missing.py zset_tcl.rs --tag-excluded
python3 frogdb-server/crates/redis-regression/tests/show_missing.py zset_tcl.rs --all
```

`show_missing.py` accepts a filter mode: `--gaps` (default -- only real unclassified gaps),
`--documented` (entries that matched a `## Intentional exclusions` bullet), `--tag-excluded`
(auto-excluded by upstream tags), or `--all`.

The `audit_tcl.py` script hard-codes `REDIS_ROOT` to `/tmp/claude/redis-tcl/redis-8.6.0` (cache
location); `REGRESSION_ROOT` is derived from `Path(__file__).parent` so the script always finds the
port files alongside itself.

Update this file when new ports land, when an upstream version bump happens, or when the
out-of-scope list changes.
