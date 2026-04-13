# Redis 8.6.0 Compatibility Coverage Audit

**Date:** 2026-04-13 (updated from 2026-04-08 snapshot)
**Upstream:** [redis/redis@8.6.0](https://github.com/redis/redis/tree/8.6.0)
**Goal:** reach a state where Redis compatibility is verified **entirely by
Rust tests in `redis-regression`** and the `testing/redis-compat/` TCL runner
can be deleted.

This audit answers: *what work is left before we can delete `testing/redis-compat/`?*

**Status:** All five phases are complete or nearly complete. The TCL runner was
deleted on 2026-04-08 (Phase 5). Test-level gaps dropped from **~543 → 2**
through intentional-exclusion documentation (Phase 1) and gap-filling
(Phase 3). New port files grew from 31 to 46 (Phase 2). The remaining
work is 2 test-level gaps in existing ports and 3 new port files (plus 4
deferred).

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
| **Ported** (Rust port exists) | 46 | 46 `*_tcl.rs` files, with `list_tcl.rs` bundling 3 upstream files and `acl_tcl.rs` covering both `unit/acl.tcl` and `unit/acl-v2.tcl` |
| **Out of scope forever** | 44 | 28 `integration/` + 7 `unit/` + 9 `unit/cluster/` files. Full list in `frogdb-server/crates/redis-regression/src/lib.rs` crate-level doc-comment. |
| **Need new Rust port** | 3 | Plus 4 deferred (see Part 2). |

**Test-level gap status across all 46 port files:**

| Bucket | Count | Notes |
|---|---:|---|
| Matched to a Rust fn (fuzzy) | ~1,620 | covered by existing `tcl_*` tests (original 1,540 + ~80 gap-fills) |
| Documented intentional exclusions | ~460 | machine-readable bullets in each port's `## Intentional exclusions` header section |
| Missing but explained by upstream exclusion tags (`needs:repl`, `aof`, `slow`, ...) | ~230 | auto-classified by upstream `tags { ... }` |
| **Unclassified gaps** | **~2** | Phase 3 work -- see Part 1 |

**Note:** The 15 new port files (Phase 2) have not been re-audited with
`audit_tcl.py` since they were created. Re-running the audit may surface
additional gaps in those files. The counts above are estimates.

**Remaining work:**

1. **2 test-level gaps** across 2 existing ports: `multi_tcl.rs` (1),
   `string_tcl.rs` (1 deferred).
2. **3 new port files needed** for currently-unported in-scope upstream files
   (plus 4 deferred).

---

## Part 1 -- Existing ports: real gaps remaining

Only files with remaining real gaps are listed. 44 of the 46 port files
report zero unclassified gaps.

### Real functional gaps (Phase 3)

| Port | Gaps | Missing coverage |
|---|---:|---|
| `multi_tcl.rs` | 1 | `WATCH stale keys should not fail EXEC` |
| `string_tcl.rs` | 1 | `LCS indexes` -- deferred, FrogDB LCS LEN returns incorrect value |

**Total: 2 real gaps** across 2 port files.

### Files with no remaining gaps

All other 44 port files have full Rust coverage of in-scope upstream tests
or explicit `## Intentional exclusions` sections documenting the out-of-scope
tests by name:

`acl`, `auth`, `bitfield`, `bitops`, `client_eviction`, `cluster_scripting`,
`cluster_sharded_pubsub`, `dump`, `expire`, `functions`, `geo`,
`hash`, `hash_field_expire`, `hotkeys`, `hyperloglog`, `incr`,
`info_command`, `info_keysizes`, `introspection`, `introspection2`,
`keyspace`, `latency_monitor`, `lazyfree`, `list`, `memefficiency`,
`networking`, `other`, `pause`, `protocol`, `pubsub`, `pubsubshard`,
`querybuf`, `replybufsize`, `scan`, `scripting`, `set`, `slowlog`, `sort`,
`stream`, `stream_cgroups`, `tracking`, `violations`, `wait`, `zset`.

---

## Part 2 -- New Rust ports needed

3 upstream files are in scope and have no Rust port yet (down from 30 at the
start of the audit). Plus 4 deferred.

### Quick wins (<=20 tests)

| File | Tests | What it covers |
|---|---:|---|
| `unit/quit.tcl` | 3 | QUIT command |
| `unit/maxmemory.tcl` | 17 | **Eviction policy behavior** -- core FrogDB feature |

### Larger ports

| File | Tests | What it covers | Decision |
|---|---:|---|---|
| `unit/info.tcl` | 27 | INFO section correctness | **Port** |

### Deferred

| File | Tests | What it covers | Decision |
|---|---:|---|---|
| `unit/cluster/atomic-slot-migration.tcl` | 81 | Atomic slot migration (Valkey 9-style) | Port when cluster rebalancing lands (see `todo/CLUSTER_REBALANCING.md`) |
| `unit/cluster/slot-stats.tcl` | 50 | Per-slot metrics (`CLUSTER SLOT-STATS`) | Port when per-slot metrics land |
| `unit/cluster/multi-slot-operations.tcl` | 6 | Cross-slot command errors | Port when `CLUSTER ADDSLOTSRANGE`/`DELSLOTSRANGE` land |
| `unit/cluster/misc.tcl` | 3 | Misc cluster commands | Port when `CLUSTER FLUSHSLOTS`/`COUNT-FAILURE-REPORTS` land |

### Recently ported (Phase 2, completed since 2026-04-08)

15 files were ported as new `*_tcl.rs` files:

**Quick wins (12):** `info_command` (3 tests), `replybufsize` (1),
`querybuf` (4), `violations` (7), `client_eviction` (15), `lazyfree` (10),
`networking` (13), `slowlog` (18), `latency_monitor` (16),
`memefficiency` (12), `hotkeys` (43), `info_keysizes` (43).

**Larger ports (3):** `other` (41 -- split port of `unit/other.tcl`),
`cluster_scripting` (6), `cluster_sharded_pubsub` (6).

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

**Phase 2 -- Port new files** ~85% done
- 15 of ~24 actionable files ported (see Part 2 "Recently ported"); 5
  reclassified as permanent OOS or deferred in `lib.rs`.
- 3 files remain (`quit`, `maxmemory`, `info`), plus 4 deferred
  (`atomic-slot-migration`, `slot-stats`, `multi-slot-operations`, `misc`).
- Key remaining: `maxmemory.tcl` (eviction -- core feature), `info.tcl`.

**Phase 3 -- Fill gaps in existing ports** ~98% done
- 87 of 89 gaps resolved since 2026-04-08: 82 as new test functions,
  5 reclassified as intentional exclusions.
- 2 gaps remain: `WATCH stale keys` (multi_tcl.rs), `LCS indexes`
  (string_tcl.rs, deferred -- known FrogDB bug).

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

## Part 5 -- Remaining gap details

### `multi_tcl.rs` -- 1 gap
- `WATCH stale keys should not fail EXEC`

### `string_tcl.rs` -- 1 gap (deferred)
- `LCS indexes` -- deferred because FrogDB LCS LEN returns incorrect value

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
