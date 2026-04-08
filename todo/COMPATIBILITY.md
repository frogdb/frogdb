# Redis 8.6.0 Compatibility Coverage Audit

**Date:** 2026-04-08
**Upstream:** [redis/redis@8.6.0](https://github.com/redis/redis/tree/8.6.0)
**Goal:** reach a state where Redis compatibility is verified **entirely by
Rust tests in `redis-regression`** and the `testing/redis-compat/` TCL runner
can be deleted.

This audit answers: *what work is left before we can delete `testing/redis-compat/`?*

## The end state

- Every Redis 8.6.0 behavior that matters for FrogDB has a corresponding
  `#[tokio::test]` in `frogdb-server/crates/redis-regression/tests/*_tcl.rs`.
- Behaviors that don't matter (AOF, RDB, replication internals, module API, …)
  are documented once as **out of scope** and never thought about again.
- `testing/redis-compat/`, `skiplist.txt`, `STATUS.md`, and `run_tests.py` are
  all deleted.

## Audit Tooling

`testing/redis-compat/audit/audit_tcl.py` parses upstream `.tcl` files,
extracts `test {name} {tags}` blocks, and diffs them against the 32 existing
Rust port files using token-based fuzzy matching (Jaccard + rust-coverage,
≥0.5 score, ≥2 token intersection). `show_missing.py` prints per-file gap
samples. Both will be moved into the `redis-regression` crate once the TCL
runner is removed.

**Note on gap counts:** fuzzy matching has ~10-15% false-positive and
false-negative rates. Treat numbers as approximations — the categories and
priorities are the actionable output.

---

## Summary

**In scope (100 upstream files):**

| Category | Files | Notes |
|---|---:|---|
| **Ported** (Rust port exists) | 34 | 32 `*_tcl.rs` files, with `list_tcl.rs` bundling 3 upstream files |
| **Out of scope forever** | 36 | AOF, RDB, replication, psync, Sentinel, moduleapi, TLS (deferred), shutdown, OOM, redis-cli/benchmark, printver, `external:skip` files with no functional relevance |
| **Need new Rust port** | 30 | Currently running via the TCL runner; will become invisible when it's removed |

**Ported-file test-level coverage (2,314 upstream tests across 32 ported files):**

| Bucket | Count |
|---|---:|
| Matched to a Rust fn (fuzzy) | ~1,565 |
| Missing but explained by upstream exclusion tags (`needs:repl`, `aof`, `slow`, …) | ~206 |
| **Unclassified gaps** (real test-level omissions, approximate) | **~543** |

**Headline work to kill the TCL runner:**

1. **~543 test-level gaps to fill** across 32 existing ports (concentrated in
   `stream_tcl.rs` (94), `zset_tcl.rs` (71), `list_tcl.rs` (53),
   `stream_cgroups_tcl.rs` (46)).
2. **30 new port files needed** for currently-unported in-scope upstream files
   (of which ~8 are quick wins with <20 tests each, and a few like
   `atomic-slot-migration.tcl` (81 tests) and `slot-stats.tcl` (50 tests)
   are large).
3. **36 out-of-scope files need one-line documentation** in `redis-regression`
   so the decision is visible in code, not in a deleted skiplist.

---

## Part 1 — Existing ports: gaps to fill

Ordered by gap count. Only files with real gaps are listed; 7 port files have
no meaningful gaps (`auth`, `bitfield`, `geo`, `incr`, `pubsubshard`,
`tracking`, `wait` — the last few have many missing tests, all legitimately
`external:skip` or `needs:repl`).

### High priority (real functional gaps, large count)

| Port | Gaps | Key missing coverage |
|---|---:|---|
| `stream_tcl.rs` | 94 | XADD IDMP (Redis 8.x idempotent add), XADD NOMKSTREAM edges, XRANGE with `+`/`-`/`$`, XTRIM MINID + LIMIT, XSETID, XINFO STREAM FULL, XLEN-after-XDEL |
| `zset_tcl.rs` | 71 | **ZUNION / ZINTER / ZDIFF** (non-STORE variants, Redis 6.2+), ZRANGEBYLEX edge cases (non-value min/max, invalid specifiers), ZREMRANGEBYSCORE non-value bounds, ZINTERCARD edges |
| `list_tcl.rs` | 53 | Unblock-fairness tests (pipelining, nested unblock, execution order), CLIENT NO-TOUCH + BRPOP interactions. (Encoding/stress tests are correctly skipped.) |
| `stream_cgroups_tcl.rs` | 46 | XAUTOCLAIM edges, XGROUP creation/deletion variations, XREADGROUP blocking semantics, consumer idle/seen-time tracking, PEL management, RENAME unblocks XREADGROUP |

### Medium priority (fewer gaps, worth porting)

| Port | Gaps | Key missing coverage |
|---|---:|---|
| `hash_tcl.rs` | 26 | HRANDFIELD edge cases (count, RESP3). Most `$encoding`-parameterized tests are intentionally out of scope — document them. |
| `introspection_tcl.rs` | 26 | CLIENT REPLY SKIP/ON/OFF, CLIENT NO-EVICT, CLIENT NO-TOUCH. (MONITOR tests are legitimately out of scope — MONITOR isn't supported.) |
| `scripting_tcl.rs` | 26 | Edge cases that hit strict-key-validation differences. Most should be marked as intentional exclusions in the file header rather than ported. |
| `multi_tcl.rs` | 20 | `WATCH stale keys should not fail EXEC`, `MULTI and script timeout`. (SWAPDB tests are single-DB-OOS.) |
| `dump_tcl.rs` | 19 | **RESTORE with IDLETIME / FREQ / LRU / LFU**, RESTORE absolute-expire, MIGRATE cached connections. Real functional gaps. |
| `introspection2_tcl.rs` | 16 | TOUCH / no-touch / access-time semantics. Relevant for eviction correctness. |
| `set_tcl.rs` | 16 | SINTERCARD illegal arguments, SRANDOM count-against-hash. Encoding tests out of scope. |
| `hash_field_expire_tcl.rs` | 15 | Redis 8.x hash-field TTL edge cases. Worth completing since this is a new feature area. |
| `string_tcl.rs` | 17 | LCS edge cases, CAS/CAD RESP3 variants. (Stress/fuzz and replication tests are OOS.) |
| `pubsub_tcl.rs` | 30 | Mostly **keyspace notifications** (new Rust ports needed — these were only in the TCL skiplist). Real gap. |

### Low priority (minor or mostly noise)

| Port | Gaps | Notes |
|---|---:|---|
| `hyperloglog_tcl.rs` | 11 | All HLL-internal encoding / sparse-to-dense. FrogDB stores HLL as a separate type — mark as intentional exclusions in the file header. |
| `protocol_tcl.rs` | 10 | RESP3 attribute tests (3-4), empty query, desync regression — modest real value. |
| `bitops_tcl.rs` | 8 | All fuzz/stress. Document as intentional exclusions. |
| `acl_tcl.rs` | 7 | ACL LOAD / ACL SAVE file-persistence tests. Depends on whether FrogDB supports ACL file persistence. |
| `expire_tcl.rs` | 6 | All replication-propagation tests that lack the `needs:repl` tag. Mark as intentional. |
| `functions_tcl.rs` | 6 | FUNCTION LOAD error-path edges. Verify against existing exclusion list. |
| `sort_tcl.rs` | 5 | Mostly SORT speed/stress. Document as intentional. |
| `scan_tcl.rs` | 4 | 1 real (`SCAN with expired keys`) + 3 encoding-parameterized. |
| `keyspace_tcl.rs` | 4 | All single-DB or inline-protocol — document as intentional. |
| `incr_tcl.rs` | 3 | "does not use shared objects", "in-place modification" — Redis internals; document. |
| `pause_tcl.rs` | 1 | One rename mismatch (`syntax errors get immediate response`). Verify. |
| `acl_v2_tcl.rs` | 1 | Single ACL load-persistence test. |
| `bitfield_tcl.rs` | 0 | Nothing to do. |
| `geo_tcl.rs` | 2 | Two trivial mismatches. |

**Total test-level gaps across all ports: ~543, of which ~300-350 are real
functional gaps and the rest are fuzzy-match noise plus intentional exclusions
that need explicit header documentation.**

### Fuzzy-match noise to clean up

Before landing Tier-A work, improve the audit script's matching:

1. Strip `$encoding` / `$type` / `- $encoding` suffixes (done).
2. Treat upstream tests run inside `foreach encoding {listpack skiplist} { … }`
   as one logical test, not N.
3. Accept a Rust fn as a match if its tokens are a strict subset of the
   upstream tokens (currently partially done via rust-coverage score).

These fixes will probably drop the 543 gap count by ~30%.

---

## Part 2 — New Rust ports needed (currently TCL-only)

30 upstream files are in scope and currently run via the TCL runner — when the
runner is deleted, coverage vanishes unless they get Rust ports.

### Quick wins (≤20 tests, simple surface area)

| File | Tests | What it covers |
|---|---:|---|
| `unit/quit.tcl` | 3 | QUIT command |
| `unit/info-command.tcl` | 3 | INFO sections correctness |
| `unit/replybufsize.tcl` | 1 | CLIENT REPLY BUFFER SIZE |
| `unit/querybuf.tcl` | 4 | CLIENT LIST qbuf field |
| `unit/violations.tcl` | 7 | security / large-key assertions |
| `unit/client-eviction.tcl` | 15 | client output buffer eviction (relevant to `max_clients` feature) |
| `unit/lazyfree.tcl` | 10 | FLUSHDB ASYNC / UNLINK lazy-free semantics |
| `unit/networking.tcl` | 13 | connection lifecycle, CLIENT KILL, CLIENT UNPAUSE |
| `unit/maxmemory.tcl` | 17 | **eviction policy behavior** — core FrogDB feature |
| `unit/slowlog.tcl` | 18 | SLOWLOG add/get/reset |
| `unit/latency-monitor.tcl` | 16 | LATENCY HISTOGRAM, LATENCY RESET |
| `unit/memefficiency.tcl` | 12 | memory accounting (may need to stub some) |
| `unit/info.tcl` | 27 | INFO section correctness |
| `unit/hotkeys.tcl` | 43 | CLIENT TRACKING hotkey detection (if supported) |
| `unit/info-keysizes.tcl` | 43 | DEBUG-dependent; check whether FrogDB implements DEBUG KEYSIZES |

Subtotal: ~232 tests across 15 files. Most are small and straightforward.

### Larger ports (cluster)

Decision needed per file: does FrogDB cluster mode intend to match Redis at
this command level?

| File | Tests | What it covers | Likely decision |
|---|---:|---|---|
| `unit/other.tcl` | 41 | Grab-bag: RANDOMKEY, BITCOUNT edges, GETRANGE, FLUSHDB edge cases, PIPELINING. Mix of in-scope and OOS. | **Split port** — port the in-scope tests, skip the rest |
| `unit/cluster/atomic-slot-migration.tcl` | 81 | Atomic slot migration (Valkey 9-style). Already on the FrogDB roadmap (`todo/CLUSTER_REBALANCING.md`). | Port when cluster rebalancing lands |
| `unit/cluster/slot-stats.tcl` | 50 | CLUSTER COUNTKEYSINSLOT, GETKEYSINSLOT | Port |
| `unit/cluster/multi-slot-operations.tcl` | 6 | Cross-slot command errors | **Port** — important for error-message parity |
| `unit/cluster/sharded-pubsub.tcl` | 6 | SSUBSCRIBE / SPUBLISH | **Port** — sharded pub/sub is a core cluster feature |
| `unit/cluster/scripting.tcl` | 6 | Cluster-mode script key validation | **Port** |
| `unit/cluster/hostnames.tcl` | 7 | CLUSTER MEET hostname support | Port if supported |
| `unit/cluster/announced-endpoints.tcl` | 3 | CLUSTER MEET with announced endpoints | Port if supported |
| `unit/cluster/failure-marking.tcl` | 2 | Cluster failure detection | Out of scope (gossip internals differ) |
| `unit/cluster/human-announced-nodename.tcl` | 2 | CLUSTER MEET nodename | Out of scope |
| `unit/cluster/internal-secret.tcl` | 3 | Cluster shared secret | Out of scope |
| `unit/cluster/links.tcl` | 6 | CLUSTER LINKS introspection | Out of scope |
| `unit/cluster/misc.tcl` | 3 | Misc cluster | Port if any apply |
| `unit/cluster/slot-ownership.tcl` | 1 | Slot ownership assertion | Out of scope |
| `unit/cluster/cli.tcl` | 14 | Cluster tests that shell out to `redis-cli` | **Out of scope** (requires binary) |

Subtotal: ~231 tests across 15 files — but most should be marked OOS.
Realistically, **6-8 cluster files and ~110 tests** need ports.

---

## Part 3 — Files to document as out of scope

These 36 files will never get Rust ports. To prevent them from being silently
ignored after the TCL runner is removed, create an `OUT_OF_SCOPE.md` file (or
a header doc-comment in `redis-regression/src/lib.rs`) that lists each one
with a one-line reason.

### `integration/` (all 28 files — all out of scope)

All integration tests fall into FrogDB-incompatible categories:

| Category | Files |
|---|---|
| AOF (RocksDB instead) | `aof.tcl`, `aof-multi-part.tcl`, `aof-race.tcl`, `aofrw.tcl` (in `unit/`) |
| RDB (RocksDB snapshots instead) | `rdb.tcl`, `corrupt-dump.tcl`, `corrupt-dump-fuzzer.tcl`, `convert-ziplist-*.tcl`, `convert-zipmap-hash-on-load.tcl` |
| Replication / PSYNC | `replication.tcl`, `replication-2.tcl`, `replication-3.tcl`, `replication-4.tcl`, `replication-buffer.tcl`, `replication-iothreads.tcl`, `replication-psync.tcl`, `replication-rdbchannel.tcl`, `psync2.tcl`, `psync2-master-restart.tcl`, `psync2-pingoff.tcl`, `psync2-reg.tcl`, `block-repl.tcl`, `failover.tcl` |
| Tool integrations | `redis-cli.tcl`, `redis-benchmark.tcl` |
| Server lifecycle | `shutdown.tcl`, `logging.tcl`, `dismiss-mem.tcl` |

### `unit/` (8 files out of scope)

| File | Reason |
|---|---|
| `aofrw.tcl` | AOF rewrite — RocksDB instead |
| `limits.tcl` | Large-memory assertion test |
| `obuf-limits.tcl` | Redis-specific output buffer limits |
| `oom-score-adj.tcl` | Linux `/proc/self/oom_score_adj` tuning |
| `printver.tcl` | 0 tests, just prints Redis version |
| `shutdown.tcl` | Different lifecycle model |
| `tls.tcl` | TLS not yet implemented (tracked in `todo/TLS_PLAN.md`) |

### `unit/cluster/` (TBD — see Part 2)

Final OOS list depends on decisions in Part 2 about which cluster behaviors
FrogDB intends to match at command level.

### Whole directories already out of scope (no action)

- `tests/unit/moduleapi/` (45 files) — FrogDB has no module system
- `tests/sentinel/` (16 files) — Sentinel out of scope
- `tests/cluster/` (legacy runner, 28 files) — superseded by `unit/cluster/`
- `tests/helpers/`, `tests/support/` — TCL helpers, not tests

---

## Part 4 — Roadmap to delete `testing/redis-compat/`

Ordered by dependency, not by effort:

**Phase 1 — Make existing ports self-describing** (small, blocks later phases)
- For each `*_tcl.rs` file, expand the file header to list the specific
  *intentional* test-level exclusions with their upstream test names. Today
  headers list exclusion *categories*; make them cite the actual tests so the
  audit script can classify them as "documented, not missing."
- This alone will drop the ~543 gap count substantially by converting many
  "unclassified" to "intentional."

**Phase 2 — Port new files** (the bulk of the work)
- Work through Part 2: ~15 quick-win files, then ~6-8 cluster files. Most
  quick-wins are <50 lines of Rust each.
- Split `unit/other.tcl` into its in-scope subset and add to existing ports
  where applicable (e.g., FLUSHDB edges → `keyspace_tcl.rs`).

**Phase 3 — Fill gaps in existing ports** (largest, most open-ended)
- Streams first (stream_tcl + stream_cgroups_tcl, ~140 gaps combined).
- Then zset non-STORE variants, list unblock-fairness, hash HRANDFIELD,
  introspection CLIENT flag tests, dump RESTORE parameter variants.

**Phase 4 — Document out-of-scope** (small, parallelizable)
- Create `frogdb-server/crates/redis-regression/src/out_of_scope.md` (or
  equivalent) listing each OOS file with a one-line reason. Migrate the
  rationale from `testing/redis-compat/skiplist.txt`'s comments.

**Phase 5 — Delete the TCL runner**
- Remove `testing/redis-compat/run_tests.py`, `skiplist.txt`, `STATUS.md`,
  `README.md`, `coverage.py`.
- Move `testing/redis-compat/audit/` into `redis-regression/` as a test file.
- Remove the `redis-compat` / `redis-compat-one` / `redis-compat-clean` /
  `redis-compat-coverage` recipes from the Justfile.
- Remove references from `CLAUDE.md` / `AGENTS.md` / README files.
- Delete the `redis-compat` skill at `.claude/skills/redis-compat/` (or
  rewrite it as `redis-regression`).

**Side-finding that becomes moot after Phase 5:**
`run_tests.py:103` uses a regex that won't match upstream 8.6.0's dynamic
`::all_tests` construction. Don't bother fixing it — just delete the file.
(Unless you need it working in the interim to verify Tier-A work; in that
case, a 10-line fix: glob `test_dirs` from the filesystem instead of regexing
the TCL source.)

---

## Part 5 — Per-ported-file gap samples

Appended for reference. Showing the first few unclassified gaps per file so
specific counts can be spot-checked.

### `acl_tcl.rs` — 7 gaps
- `ACL LOAD only disconnects affected clients`
- `ACL LOAD disconnects affected subscriber`
- `ACL load and save`
- `Alice: can execute all command` (ACL LOAD scenario)
- `First server should have role slave after SLAVEOF` — should be `needs:repl`

### `bitops_tcl.rs` — 8 gaps (all fuzz/stress, mark as intentional)
- `BITOP ? fuzzing`, `BITOP NOT fuzzing`
- `BITPOS bit=1/0 fuzzy testing using SETBIT`
- `SETBIT/BITFIELD only increase dirty when the value changed` — DEBUG-dependent

### `dump_tcl.rs` — 19 gaps (real functional gaps)
- `RESTORE can set an expire that overflows a 32 bit integer`
- `RESTORE can set an absolute expire`
- `RESTORE can set LRU`, `RESTORE can set LFU`
- `MIGRATE is caching connections`

### `expire_tcl.rs` — 6 gaps (all replication-propagation)
- `All TTL in commands are propagated as absolute timestamp in replication stream`
- `GETEX propagate as to replica as PERSIST, DEL, or nothing`
- `Redis should not propagate the read command on lazy expire`

### `hash_tcl.rs` — 26 gaps
- `Is the small hash encoded with a listpack?` — encoding, intentional
- `HRANDFIELD - ?`, `HRANDFIELD with RESP3` — **real gap**
- `HGET against the small hash` — encoding-parameterized

### `hash_field_expire_tcl.rs` — 15 gaps
Redis 8.x hash field TTL — worth completing.

### `introspection_tcl.rs` — 26 gaps
- `CLIENT REPLY SKIP: skip the next command reply` — **real gap**
- `CLIENT REPLY ON: unset SKIP flag` — **real gap**
- `MONITOR can log executed commands` — intentional (MONITOR unsupported)
- `MONITOR can log commands issued by the scripting engine` — intentional
- `CLIENT command unhappy path coverage` — partial match, may be real gap

### `introspection2_tcl.rs` — 16 gaps
Mostly access-time / TOUCH / no-touch mode tests. Real gaps.

### `list_tcl.rs` — 53 gaps
- `Unblock fairness is kept while pipelining` — **real gap**
- `Unblock fairness is kept during nested unblock` — **real gap**
- `Command being unblocked cause another command to get unblocked execution order test` — **real gap**
- `Blocking command accounted only once in commandstats` — **real gap**
- `CLIENT NO-TOUCH with BRPOP and RPUSH regression test` — **real gap**
- Many encoding/stress tests — intentional

### `multi_tcl.rs` — 20 gaps
- `WATCH stale keys should not fail EXEC` — **real gap**
- `MULTI and script timeout` — real gap
- SWAPDB-related tests — single-DB, intentional

### `protocol_tcl.rs` — 10 gaps
- `Handle an empty query`
- `Protocol desync regression test`
- `RESP3 attributes`, `RESP3 attributes readraw`, `RESP3 attributes on RESP2`

### `pubsub_tcl.rs` — 30 gaps
- `PubSub messages with CLIENT REPLY OFF` — real
- **Keyspace notifications** (many tests) — **real gap**
- `Pub/Sub PING on RESP?` — RESP3-parameterized

### `scripting_tcl.rs` — 26 gaps
Mostly strict-key-validation behavior differences; mark as intentional.

### `stream_tcl.rs` — 94 gaps
- ~20 XADD IDMP tests (Redis 8.x feature)
- XADD NOMKSTREAM edges
- XRANGE with `+`/`-`/`$` special IDs
- XTRIM MINID + LIMIT
- XSETID cases
- XINFO STREAM FULL

### `stream_cgroups_tcl.rs` — 46 gaps
- XAUTOCLAIM edges
- XGROUP create/delete variations
- XREADGROUP blocking
- Consumer idle / seen-time / active-time
- PEL management
- RENAME unblocks XREADGROUP

### `zset_tcl.rs` — 71 gaps
- **ZUNION / ZINTER / ZDIFF** non-STORE variants (Redis 6.2+)
- ZRANGEBYLEX edge cases
- ZREMRANGEBYSCORE non-value bounds
- ZINTERCARD edges

### Others with ≤17 gaps
Smaller gaps in `string_tcl.rs`, `set_tcl.rs`, `sort_tcl.rs`, `keyspace_tcl.rs`,
`hyperloglog_tcl.rs`, `functions_tcl.rs`, `scan_tcl.rs`, `incr_tcl.rs`,
`acl_v2_tcl.rs`, `pause_tcl.rs`, `geo_tcl.rs`. See per-file details in an
earlier section; most are either mark-as-intentional or 1-2 real edges.

---

## How to re-run this audit

```bash
# Download Redis 8.6.0 source (~4 MB, idempotent)
mkdir -p /tmp/claude/redis-tcl && cd /tmp/claude/redis-tcl
curl -sSL https://github.com/redis/redis/archive/refs/tags/8.6.0.tar.gz | tar -xz

# Run the audit script
cd /Users/nathan/workspace/workspace-4
uv run --script testing/redis-compat/audit/audit_tcl.py all \
  > /tmp/claude/audit_results.json

# Inspect a specific port's gaps
python3 testing/redis-compat/audit/show_missing.py zset_tcl.rs
```

The `audit_tcl.py` script hard-codes `REDIS_ROOT` to
`/tmp/claude/redis-tcl/redis-8.6.0` and `REGRESSION_ROOT` to the in-repo
`redis-regression/tests/` — edit those constants if the source is cached
elsewhere, or if the audit tooling is relocated into `redis-regression/`
after Phase 5.

Update this file when new ports land, when an upstream version bump happens,
or when the out-of-scope list changes.
