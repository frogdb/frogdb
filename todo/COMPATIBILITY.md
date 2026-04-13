# Redis 8.6.0 Compatibility Coverage Audit

**Date:** 2026-04-13
**Upstream:** [redis/redis@8.6.0](https://github.com/redis/redis/tree/8.6.0)
**Goal:** reach a state where Redis compatibility is verified **entirely by
Rust tests in `redis-regression`** and the `testing/redis-compat/` TCL runner
can be deleted.

This audit answers: *what work is left before we can delete `testing/redis-compat/`?*

**Status:** All five phases are complete. The TCL runner was deleted on
2026-04-08 (Phase 5). Test-level gaps dropped from **~543 → 0** through
intentional-exclusion documentation (Phase 1) and gap-filling (Phase 3). New
port files grew from 31 to 49 (Phase 2). The only remaining work is 4
deferred cluster files.

## The end state

- Every Redis 8.6.0 behavior that matters for FrogDB has a corresponding
  `#[tokio::test]` in `frogdb-server/crates/redis-regression/tests/*_tcl.rs`.
- Behaviors that don't matter (AOF, RDB, replication internals, module API, ...)
  are documented once as **out of scope** and never thought about again.
- `testing/redis-compat/`, `skiplist.txt`, `STATUS.md`, and `run_tests.py` are
  all deleted.

## Audit Tooling

`frogdb-server/crates/redis-regression/tests/audit_tcl.py` parses upstream
`.tcl` files, extracts `test {name} {tags}` blocks, and diffs them against
the existing Rust port files using token-based fuzzy matching (Jaccard +
rust-coverage, >=0.5 score, >=2 token intersection). It also parses each
Rust port file's `## Intentional exclusions` section (added in Phase 1)
and classifies any upstream test whose name matches a documented exclusion
as `documented` rather than as an unclassified gap.
`frogdb-server/crates/redis-regression/tests/show_missing.py` prints per-file
gap details, filterable by category (`--gaps`, `--documented`,
`--tag-excluded`, `--all`). Both scripts live in the `redis-regression`
crate; the script is a *tracker* -- actual test execution happens via
`cargo test -p frogdb-redis-regression`.

**Note on gap counts:** fuzzy matching has ~10-15% false-positive and
false-negative rates. Treat numbers as approximations -- the categories and
priorities are the actionable output.

---

## Summary

**In scope upstream files:**

| Category | Files | Notes |
|---|---:|---|
| **Ported** (Rust port exists) | 49 | 49 `*_tcl.rs` files, with `list_tcl.rs` bundling 3 upstream files and `acl_tcl.rs` covering both `unit/acl.tcl` and `unit/acl-v2.tcl` |
| **Out of scope forever** | 44 | 28 `integration/` + 7 `unit/` + 9 `unit/cluster/` files. Full list in `frogdb-server/crates/redis-regression/src/lib.rs` crate-level doc-comment. |
| **Need new Rust port** | 0 | 4 deferred (see Part 2). |

**Test-level gap status across all 49 port files:**

| Bucket | Count | Notes |
|---|---:|---|
| Matched to a Rust fn (fuzzy) | ~1,640 | covered by existing `tcl_*` tests (original 1,540 + ~80 gap-fills + ~20 new ports) |
| Documented intentional exclusions | ~530 | machine-readable bullets in each port's `## Intentional exclusions` header section |
| Missing but explained by upstream exclusion tags (`needs:repl`, `aof`, `slow`, ...) | ~230 | auto-classified by upstream `tags { ... }` |
| **Unclassified gaps** | **0** | All gaps resolved |

**Remaining work:**

1. **0 test-level gaps** -- all resolved on 2026-04-13.
2. **0 new port files needed** -- `quit`, `maxmemory`, `info` ported on
   2026-04-13 (4 cluster files deferred).

---

## Part 1 -- Existing ports: real gaps remaining

**No remaining gaps.** All 49 port files have full Rust coverage of in-scope
upstream tests or explicit `## Intentional exclusions` sections documenting
the out-of-scope tests by name.

### Resolved gaps (2026-04-13)

| Port | Resolution |
|---|---|
| `multi_tcl.rs` | `WATCH stale keys should not fail EXEC` — documented as intentional exclusion (needs:debug, requires DEBUG SET-ACTIVE-EXPIRE) |
| `string_tcl.rs` | `LCS indexes` — fixed: tcl_lcs_len expected value was wrong (222→227); added 3 new IDX tests |

### Files with no remaining gaps

All 49 port files have full coverage:

`acl`, `auth`, `bitfield`, `bitops`, `client_eviction`, `cluster_scripting`,
`cluster_sharded_pubsub`, `dump`, `expire`, `functions`, `geo`,
`hash`, `hash_field_expire`, `hotkeys`, `hyperloglog`, `incr`, `info`,
`info_command`, `info_keysizes`, `introspection`, `introspection2`,
`keyspace`, `latency_monitor`, `lazyfree`, `list`, `maxmemory`,
`memefficiency`, `multi`, `networking`, `other`, `pause`, `protocol`,
`pubsub`, `pubsubshard`, `querybuf`, `quit`, `replybufsize`, `scan`,
`scripting`, `set`, `slowlog`, `sort`, `stream`, `stream_cgroups`,
`tracking`, `violations`, `wait`, `zset`.

---

## Part 2 -- New Rust ports needed

All actionable upstream files have been ported. 4 cluster files remain
deferred until their prerequisite features land.

### Deferred

| File | Tests | What it covers | Decision |
|---|---:|---|---|
| `unit/cluster/atomic-slot-migration.tcl` | 81 | Atomic slot migration (Valkey 9-style) | Port when cluster rebalancing lands (see `todo/CLUSTER_REBALANCING.md`) |
| `unit/cluster/slot-stats.tcl` | 50 | Per-slot metrics (`CLUSTER SLOT-STATS`) | Port when per-slot metrics land |
| `unit/cluster/multi-slot-operations.tcl` | 6 | Cross-slot command errors | Port when `CLUSTER ADDSLOTSRANGE`/`DELSLOTSRANGE` land |
| `unit/cluster/misc.tcl` | 3 | Misc cluster commands | Port when `CLUSTER FLUSHSLOTS`/`COUNT-FAILURE-REPORTS` land |

### Recently ported (Phase 2)

18 files were ported as new `*_tcl.rs` files:

**2026-04-08 (15 files):**
`info_command` (3 tests), `replybufsize` (1), `querybuf` (4),
`violations` (7), `client_eviction` (15), `lazyfree` (10),
`networking` (13), `slowlog` (18), `latency_monitor` (16),
`memefficiency` (12), `hotkeys` (43), `info_keysizes` (43),
`other` (41 -- split port of `unit/other.tcl`),
`cluster_scripting` (6), `cluster_sharded_pubsub` (6).

**2026-04-13 (3 files):**
`quit` (3 tests), `maxmemory` (16 tests -- 3 policy-loop groups across 7
standard policies; ~20 exclusions for client eviction, replication, LRM,
DEBUG), `info` (0 tests -- all 27 upstream tests excluded; categorized as
architecture-inapplicable or observability gaps for latency tracking,
error/command stats, and client stats).

---

## Part 3 -- Files documented as out of scope

The authoritative out-of-scope list lives in
`frogdb-server/crates/redis-regression/src/lib.rs` as a crate-level
`//!` doc-comment. It enumerates **44 individual files** (28 `integration/`
+ 7 `unit/` + 9 `unit/cluster/`) grouped by reason, plus **4 whole
directories**, each with a one-line explanation. Browse with
`cargo doc --no-deps -p frogdb-redis-regression` or open the source
directly.

| Source directory | Files OOS | Summary |
|---|---:|---|
| `integration/` | 28 | All integration tests -- AOF, RDB, replication, PSYNC, tool bindings (`redis-cli`, `redis-benchmark`), server lifecycle (`shutdown`, `logging`, `dismiss-mem`) |
| `unit/` | 7 | `aofrw`, `limits`, `obuf-limits`, `oom-score-adj`, `printver`, `shutdown`, `tls` |
| `unit/cluster/` | 9 | `announced-endpoints`, `cli`, `cluster-response-tls`, `failure-marking`, `hostnames`, `human-announced-nodename`, `internal-secret`, `links`, `slot-ownership` |
| Whole dirs | -- | `tests/unit/moduleapi/` (45 files), `tests/sentinel/` (16 files), legacy `tests/cluster/` (28 files), `tests/helpers/`, `tests/support/` |

**Note:** `lib.rs` lists 48 individual files total (44 above + 4 deferred
cluster files tracked in Part 2: `atomic-slot-migration`, `slot-stats`,
`multi-slot-operations`, `misc`). `tls.tcl` is also deferred but counted
in the `unit/` row above.

---

## Part 4 -- Roadmap

Ordered by dependency, not by effort:

**Phase 1 -- Make existing ports self-describing** :white_check_mark: **DONE 2026-04-08**
- For each `*_tcl.rs` file, expanded the file header to list the specific
  *intentional* test-level exclusions with their upstream test names.
- Extended `audit_tcl.py` with `extract_documented_exclusions()` to parse
  `## Intentional exclusions` sections.
- **Result:** 454 tests reclassified from unclassified -> documented;
  headline unclassified count dropped from ~543 to 89.

**Phase 2 -- Port new files** :white_check_mark: **DONE 2026-04-13**
- 18 of ~24 actionable files ported (see Part 2 "Recently ported"); 5
  reclassified as permanent OOS or deferred in `lib.rs`.
- Final 3 files (`quit`, `maxmemory`, `info`) ported on 2026-04-13.
- 4 cluster files deferred until prerequisite features land.

**Phase 3 -- Fill gaps in existing ports** :white_check_mark: **DONE 2026-04-13**
- 89 of 89 gaps resolved: 82 as new test functions, 7 reclassified as
  intentional exclusions (including `WATCH stale keys` as needs:debug,
  `LCS indexes` fixed with correct expected value).

**Phase 4 -- Document out-of-scope** :white_check_mark: **DONE 2026-04-08**
- Added a crate-level `//!` doc-comment to
  `frogdb-server/crates/redis-regression/src/lib.rs` listing every
  out-of-scope upstream file grouped by reason.
- **Result:** 44 individual files enumerated plus 4 whole directories
  (48 total in `lib.rs` including 4 deferred cluster files from Part 2).

**Phase 5 -- Delete the TCL runner** :white_check_mark: **DONE 2026-04-08**
- Deleted `testing/redis-compat/` wholesale.
- Moved `audit_tcl.py` and `show_missing.py` into
  `frogdb-server/crates/redis-regression/tests/`.
- Removed Justfile recipes and documentation references.

---

## Part 5 -- Observability gaps identified (info_tcl.rs)

The `info_tcl.rs` port documents 27 upstream tests that exercise INFO
metrics FrogDB does not yet implement. These are categorized as potential
observability improvements, not compatibility blockers:

- **Per-command latency tracking** (6 tests): `latency-tracking` config,
  per-command p50/p99/p99.9 percentiles.
- **Error/command stats** (10 tests): per-error-type `errorstat_*` counters,
  `rejected_calls`/`failed_calls` in commandstats, `total_error_replies`.
- **Client stats** (2 tests): `pubsub_clients`, `watching_clients`,
  `total_watched_keys` in INFO clients section.
- **Not applicable** (9 tests): Redis event loop metrics, DEBUG section,
  dict rehashing internals -- architecture-specific to Redis's single-threaded
  model.

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

`show_missing.py` accepts a filter mode: `--gaps` (default -- only real
unclassified gaps), `--documented` (entries that matched a `## Intentional
exclusions` bullet), `--tag-excluded` (auto-excluded by upstream tags), or
`--all`.

The `audit_tcl.py` script hard-codes `REDIS_ROOT` to
`/tmp/claude/redis-tcl/redis-8.6.0` (cache location); `REGRESSION_ROOT` is
derived from `Path(__file__).parent` so the script always finds the port
files alongside itself.

Update this file when new ports land, when an upstream version bump happens,
or when the out-of-scope list changes.
