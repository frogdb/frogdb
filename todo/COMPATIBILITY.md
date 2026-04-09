# Redis 8.6.0 Compatibility Coverage Audit

**Date:** 2026-04-08 (Phase 1 complete same day)
**Upstream:** [redis/redis@8.6.0](https://github.com/redis/redis/tree/8.6.0)
**Goal:** reach a state where Redis compatibility is verified **entirely by
Rust tests in `redis-regression`** and the `testing/redis-compat/` TCL runner
can be deleted.

This audit answers: *what work is left before we can delete `testing/redis-compat/`?*

**Status:** Phases 1, 4, and 5 are complete. The headline unclassified-gap
count dropped from **~543 → 90** (an 83% reduction) by moving 453 intentional
exclusions into machine-parseable `## Intentional exclusions` sections in
each port file's doc-comment header. The TCL runner has been deleted
(Phase 5, 2026-04-08); the audit script lives at
`frogdb-server/crates/redis-regression/tests/audit_tcl.py` and is a tracker
only — execution happens via `cargo test -p frogdb-redis-regression`.
The remaining 90 gaps are actionable Phase 3 work.

## The end state

- Every Redis 8.6.0 behavior that matters for FrogDB has a corresponding
  `#[tokio::test]` in `frogdb-server/crates/redis-regression/tests/*_tcl.rs`.
- Behaviors that don't matter (AOF, RDB, replication internals, module API, …)
  are documented once as **out of scope** and never thought about again.
- `testing/redis-compat/`, `skiplist.txt`, `STATUS.md`, and `run_tests.py` are
  all deleted.

## Audit Tooling

`frogdb-server/crates/redis-regression/tests/audit_tcl.py` parses upstream
`.tcl` files, extracts `test {name} {tags}` blocks, and diffs them against
the existing Rust port files using token-based fuzzy matching (Jaccard +
rust-coverage, ≥0.5 score, ≥2 token intersection). It also parses each
Rust port file's `## Intentional exclusions` section (added in Phase 1)
and classifies any upstream test whose name matches a documented exclusion
as `documented` rather than as an unclassified gap.
`frogdb-server/crates/redis-regression/tests/show_missing.py` prints per-file
gap details, filterable by category (`--gaps`, `--documented`,
`--tag-excluded`, `--all`). Both scripts moved into the `redis-regression`
crate as part of Phase 5 (2026-04-08); the script is a *tracker* — actual
test execution happens via `cargo test -p frogdb-redis-regression`.

**Note on gap counts:** fuzzy matching has ~10-15% false-positive and
false-negative rates. Treat numbers as approximations — the categories and
priorities are the actionable output.

---

## Summary

**In scope (100 upstream files):**

| Category | Files | Notes |
|---|---:|---|
| **Ported** (Rust port exists) | 34 | 31 `*_tcl.rs` files, with `list_tcl.rs` bundling 3 upstream files and `acl_tcl.rs` covering both `unit/acl.tcl` and `unit/acl-v2.tcl` after the v2 selector removal in commit 8121bfee |
| **Out of scope forever** | 42 | 28 `integration/` + 7 `unit/` + 7 `unit/cluster/` files: AOF, RDB, replication, PSYNC, Sentinel, moduleapi, TLS (deferred), shutdown, OOM, redis-cli/benchmark, printver, cluster gossip internals. Full list in `frogdb-server/crates/redis-regression/src/lib.rs` as a crate-level doc-comment (Phase 4, 2026-04-08). |
| **Need new Rust port** | 30 | Need a fresh `*_tcl.rs` port (Phase 2 work). |

**Ported-file test-level coverage (2,314 upstream tests across 31 port files):**

| Bucket | Count | Notes |
|---|---:|---|
| Matched to a Rust fn (fuzzy) | 1,540 | covered by an existing `tcl_*` test |
| Documented intentional exclusions | **453** | machine-readable bullets in each port's `## Intentional exclusions` header section (Phase 1, 2026-04-08) |
| Missing but explained by upstream exclusion tags (`needs:repl`, `aof`, `slow`, …) | 231 | already auto-classified by upstream `tags { … }` |
| **Unclassified gaps** (real test-level omissions) | **90** | Phase 3 work — ports that need new `tcl_*` functions |

**Headline work to kill the TCL runner:**

1. **89 test-level gaps to fill** across 10 existing ports. Down from 543
   pre-Phase-1. Distribution: `zset_tcl.rs` (22), `list_tcl.rs` (18),
   `hash_field_expire_tcl.rs` (15), `stream_tcl.rs` (13), `stream_cgroups_tcl.rs` (9),
   `introspection_tcl.rs` (5), `dump_tcl.rs` (4), `multi_tcl.rs` (1),
   `pause_tcl.rs` (1), `string_tcl.rs` (1).
2. **30 new port files needed** for currently-unported in-scope upstream files
   (of which ~8 are quick wins with <20 tests each, and a few like
   `atomic-slot-migration.tcl` (81 tests) and `slot-stats.tcl` (50 tests)
   are large).
3. **36 out-of-scope files need one-line documentation** in `redis-regression`
   so the decision is visible in code, not in a deleted skiplist.

---

## Part 1 — Existing ports: real gaps remaining (Phase 3 work)

Numbers reflect **post-Phase-1** state (intentional exclusions removed). Only
files with remaining real gaps are listed; 22 of the 32 port files now report
zero unclassified gaps.

### Real functional gaps to address (Phase 3)

| Port | Gaps | Key missing coverage |
|---|---:|---|
| `zset_tcl.rs` | 22 | **ZUNION / ZINTER / ZDIFF** non-STORE variants (Redis 6.2+), ZRANGEBYLEX edge cases (non-value min/max, invalid specifiers), ZREMRANGEBYSCORE non-value bounds, blocking-edge cases (BZPOPMIN unblock-then-block, BZMPOP edges, MULTI/EXEC pop isolation), zero-timeout blocking |
| `list_tcl.rs` | 18 | Unblock-fairness (pipelining, nested unblock, execution order), CLIENT NO-TOUCH + BRPOP interactions, blocked-client-with-zero-timeout, BLPOP unblock-then-block reprocessing, $pop-on-rename and $pop-on-SORT-STORE |
| `hash_field_expire_tcl.rs` | 15 | Redis 8.x hash-field TTL edge cases (HSETEX/HGETEX argument parsing, field-count validation, error-message consistency, boundary conditions). New feature area. |
| `stream_tcl.rs` | 13 | XADD with LIMIT MAXLEN edge cases, XRANGE iterate-whole-stream, XREVRANGE, XREAD blocking edges (XADD+DEL awake, MULTI XADD), XDEL multi-id, XTRIM with `~`, XSETID arg validation, XGROUP/XINFO HELP coverage |
| `stream_cgroups_tcl.rs` | 9 | XPENDING IDLE filter, RENAME unblocks XREADGROUP (with data and -NOGROUP), Consumer seen-time/active-time tracking, Consumer group lag computation (with XDELs/XTRIM/sanity) |
| `introspection_tcl.rs` | 5 | CLIENT REPLY SKIP/ON/OFF (3 tests), CLIENT command unhappy-path coverage |
| `dump_tcl.rs` | 4 | **RESTORE with IDLETIME / FREQ / LRU / LFU**, RESTORE absolute-expire, RESTORE expire >32-bit overflow |
| `multi_tcl.rs` | 1 | `WATCH stale keys should not fail EXEC` |
| `pause_tcl.rs` | 1 | `Test clients with syntax errors will get responses immediately` (verify — may be a fuzzy-match miss) |
| `string_tcl.rs` | 1 | LCS indexes edge case |

**Total: 89 real gaps** across 10 port files. The 22 other port files
(including 4 of the original 5 high-gap files: `pubsub`, `hash`, `scripting`,
`set`) now have zero unclassified gaps.

### Files with no remaining gaps (post-Phase 1)

`acl`, `acl_v2`, `auth`, `bitfield`, `bitops`, `expire`, `functions`, `geo`,
`hash`, `hyperloglog`, `incr`, `introspection2`, `keyspace`, `protocol`,
`pubsub`, `pubsubshard`, `scan`, `scripting`, `set`, `sort`, `tracking`,
`wait`. All of these either have full Rust coverage of the in-scope upstream
tests or have explicit `## Intentional exclusions` sections documenting the
out-of-scope tests by name.

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

## Part 3 — Files documented as out of scope

As of Phase 4 (2026-04-08) the authoritative out-of-scope list lives in
`frogdb-server/crates/redis-regression/src/lib.rs` as a crate-level
`//!` doc-comment. It enumerates **42 individual files** (28 `integration/`
+ 7 `unit/` + 7 `unit/cluster/`) grouped by reason, plus **4 whole
directories**, each with a one-line explanation. Browse with
`cargo doc --no-deps -p frogdb-redis-regression` or open the source
directly.

This Part retains only the top-level counts; the full file list and
per-file reasons are in the Rust source so that deleting
`testing/redis-compat/` in Phase 5 doesn't drop any context.

| Source directory | Files OOS | Summary |
|---|---:|---|
| `integration/` | 28 | All integration tests — AOF, RDB, replication, PSYNC, tool bindings (`redis-cli`, `redis-benchmark`), server lifecycle (`shutdown`, `logging`, `dismiss-mem`) |
| `unit/` | 7 | `aofrw`, `limits`, `obuf-limits`, `oom-score-adj`, `printver`, `shutdown`, `tls` |
| `unit/cluster/` | 7 | `cli`, `cluster-response-tls`, `failure-marking`, `human-announced-nodename`, `internal-secret`, `links`, `slot-ownership` |
| Whole dirs | — | `tests/unit/moduleapi/` (45 files), `tests/sentinel/` (16 files), legacy `tests/cluster/` (28 files), `tests/helpers/`, `tests/support/` |

The remaining 8 `unit/cluster/` files (`atomic-slot-migration`,
`slot-stats`, `multi-slot-operations`, `sharded-pubsub`, `scripting`,
`hostnames`, `announced-endpoints`, `misc`) are **not** OOS — they are
Phase 2 ports per Part 2.

### Whole directories already out of scope (no action)

- `tests/unit/moduleapi/` (45 files) — FrogDB has no module system
- `tests/sentinel/` (16 files) — Sentinel out of scope
- `tests/cluster/` (legacy runner, 28 files) — superseded by `unit/cluster/`
- `tests/helpers/`, `tests/support/` — TCL helpers, not tests

---

## Part 4 — Roadmap to delete `testing/redis-compat/`

Ordered by dependency, not by effort:

**Phase 1 — Make existing ports self-describing** ✅ **DONE 2026-04-08**
- For each `*_tcl.rs` file, expand the file header to list the specific
  *intentional* test-level exclusions with their upstream test names.
- Extended `audit_tcl.py` with `extract_documented_exclusions()` to parse
  `## Intentional exclusions` sections and classify those tests as
  `documented` rather than `unclassified gap`.
- **Result:** 454 tests reclassified from unclassified → documented;
  headline unclassified count dropped from ~543 to 89 (84% reduction).
  22/32 port files now have zero unclassified gaps. The remaining 89 are
  real Phase 3 work (see Part 1).

**Phase 2 — Port new files** (the bulk of the work)
- Work through Part 2: ~15 quick-win files, then ~6-8 cluster files. Most
  quick-wins are <50 lines of Rust each.
- Split `unit/other.tcl` into its in-scope subset and add to existing ports
  where applicable (e.g., FLUSHDB edges → `keyspace_tcl.rs`).

**Phase 3 — Fill gaps in existing ports** (largest, most open-ended)
- Streams first (stream_tcl + stream_cgroups_tcl, ~140 gaps combined).
- Then zset non-STORE variants, list unblock-fairness, hash HRANDFIELD,
  introspection CLIENT flag tests, dump RESTORE parameter variants.

**Phase 4 — Document out-of-scope** ✅ **DONE 2026-04-08**
- Added a crate-level `//!` doc-comment to
  `frogdb-server/crates/redis-regression/src/lib.rs` listing every
  out-of-scope upstream file grouped by reason.
- **Result:** 42 individual files enumerated (28 `integration/` + 7 `unit/`
  + 7 `unit/cluster/`) plus 4 whole directories. `cargo doc --no-deps
  -p frogdb-redis-regression` renders the section cleanly. COMPATIBILITY.md
  Part 3 rewritten to point at the crate source as the single source of
  truth.
- `testing/redis-compat/skiplist.txt` was the runtime skiplist for
  the TCL runner; Phase 5 deleted it together with the runner.

**Phase 5 — Delete the TCL runner** ✅ **DONE 2026-04-08**
- Deleted `testing/redis-compat/` wholesale: `run_tests.py`, `skiplist.txt`,
  `STATUS.md`, `README.md`, `coverage.py`.
- Moved `audit_tcl.py` and `show_missing.py` from
  `testing/redis-compat/audit/` into
  `frogdb-server/crates/redis-regression/tests/`. `REGRESSION_ROOT` is now
  `Path(__file__).parent`. The scripts are pure trackers — execution moved
  to `cargo test -p frogdb-redis-regression`.
- Removed the `redis-compat` / `redis-compat-one` / `redis-compat-clean` /
  `redis-compat-coverage` recipes from the Justfile. Redis-regression tests
  run via `just test frogdb-redis-regression` (or as part of `just test`).
- Removed references from `website/.../architecture.md`,
  `.claude/skills/check/SKILL.md`, `.claude/skills/doc-sync/{SKILL.md,
  references/doc-map.md}`, and `todo/INDEX.md`.
- Deleted the `.claude/skills/redis-compat/` skill (SKILL.md +
  references/{architecture,regression-tests}.md).

---

## Part 5 — Per-ported-file gap samples (post-Phase-1)

Showing the actual remaining unclassified gaps per file. To regenerate:
`python3 testing/redis-compat/audit/show_missing.py <port>.rs --gaps`.

Files now at zero unclassified gaps are omitted.

### `zset_tcl.rs` — 22 gaps
- `ZUNION with weights - $encoding`
- `ZUNION/ZINTER with AGGREGATE MIN - $encoding`
- `ZUNION/ZINTER with AGGREGATE MAX - $encoding`
- `ZINTER basics - $encoding`
- `ZINTER with weights - $encoding`
- `ZDIFF basics - $encoding`
- `ZDIFF subtracting set from itself - $encoding`
- `ZDIFF algorithm 1 - $encoding`, `ZDIFF algorithm 2 - $encoding`
- `ZRANGEBYSCORE with non-value min or max - $encoding`
- `ZRANGEBYLEX with invalid lex range specifiers - $encoding`
- `ZREMRANGEBYSCORE with non-value min or max - $encoding`
- `$pop, ZADD + DEL should not awake blocked client`
- `$pop, ZADD + DEL + SET should not awake blocked client`
- `BZPOPMIN unblock but the key is expired and then block again - reprocessing command`
- `BZPOPMIN with same key multiple times should work`
- `MULTI/EXEC is isolated from the point of view of $pop`
- `$pop with zero timeout should block indefinitely`
- `BZMPOP with illegal argument`
- `BZMPOP with multiple blocked clients`
- `BZMPOP should not blocks on non key arguments - #10762`
- `zset score double range`

### `list_tcl.rs` — 18 gaps
- `Unblock fairness is kept while pipelining`
- `Unblock fairness is kept during nested unblock`
- `Command being unblocked cause another command to get unblocked execution order test`
- `Blocking command accounted only once in commandstats`
- `Blocking command accounted only once in commandstats after timeout`
- `Blocking timeout following PAUSE should honor the timeout`
- `CLIENT NO-TOUCH with BRPOP and RPUSH regression test`
- `BLMOVE $wherefrom $whereto with zero timeout should block indefinitely`
- `PUSH resulting from BRPOPLPUSH affect WATCH`
- `BLPOP unblock but the key is expired and then block again - reprocessing command`
- `$pop when new key is moved into place`
- `$pop when result key is created by SORT..STORE`
- `$pop: with zero timeout should block indefinitely`
- `$pop: with 0.001 timeout should not block indefinitely`
- `$pop: timeout`
- `$pop: arguments are empty`
- `LRANGE with start > end yields an empty array for backward compatibility`
- `client unblock tests`

### `hash_field_expire_tcl.rs` — 15 gaps (Redis 8.x feature area)
- `HEXPIRE FAMILY - Rigid expiration time positioning ($type)`
- `HEXPIREAT/HPEXPIREAT - Flexible keyword ordering ($type)`
- `HSETEX - Flexible argument parsing and validation ($type)`
- `HGETEX - Flexible argument parsing and validation ($type)`
- `Field count validation - HSETEX ($type)`
- `Field count validation - HGETEX ($type)`
- `Error message consistency and validation ($type)`
- `Numeric field names validation ($type)`
- `Multiple condition flags error handling ($type)`
- `Multiple FIELDS keywords error handling ($type)`
- `Boundary conditions and edge cases ($type)`
- `Field names that look like keywords or numbers ($type)`
- `Parser state consistency ($type)`
- `Stress test - complex scenarios with all features ($type)`
- `Backward compatibility verification ($type)`

### `stream_tcl.rs` — 13 gaps
- `XADD with LIMIT delete entries no more than limit`
- `XRANGE can be used to iterate the whole stream`
- `XREVRANGE returns the reverse of XRANGE`
- `XREAD: XADD + DEL should not awake client`
- `XREAD: XADD + DEL + LPUSH should not awake client`
- `XREAD + multiple XADD inside transaction`
- `XDEL multiply id test`
- `XTRIM with ~ is limited`
- `XSETID cannot run with an offset but without a maximal tombstone`
- `XSETID cannot run with a maximal tombstone but without an offset`
- `XSETID errors on negstive offset`
- `XGROUP HELP should not have unexpected options`
- `XINFO HELP should not have unexpected options`

### `stream_cgroups_tcl.rs` — 9 gaps
- `XPENDING only group`
- `XPENDING with IDLE`
- `RENAME can unblock XREADGROUP with data`
- `RENAME can unblock XREADGROUP with -NOGROUP`
- `Consumer seen-time and active-time`
- `Consumer group read counter and lag sanity`
- `Consumer group lag with XDELs`
- `Consumer Group Lag with XDELs and tombstone after the last_id of consume group`
- `Consumer group lag with XTRIM`

### `introspection_tcl.rs` — 5 gaps
- `CLIENT command unhappy path coverage`
- `CLIENT REPLY SKIP: skip the next command reply`
- `CLIENT REPLY ON: unset SKIP flag`
- (plus 2 more from upstream that the fuzzy matcher couldn't connect to existing fns)

### `dump_tcl.rs` — 4 gaps
- `RESTORE can set an expire that overflows a 32 bit integer`
- `RESTORE can set an absolute expire`
- `RESTORE can set LRU`
- `RESTORE can set LFU`

### `multi_tcl.rs` — 1 gap
- `WATCH stale keys should not fail EXEC`

### `pause_tcl.rs` — 1 gap
- `Test clients with syntax errors will get responses immediately` (verify — may be a fuzzy-match miss)

### `string_tcl.rs` — 1 gap
- `LCS indexes`

---

## How to re-run this audit

```bash
# Download Redis 8.6.0 source (~4 MB, idempotent)
mkdir -p /tmp/claude/redis-tcl && cd /tmp/claude/redis-tcl
curl -sSL https://github.com/redis/redis/archive/refs/tags/8.6.0.tar.gz | tar -xz

# Run the audit script
cd /Users/nathan/workspace/workspace-4
uv run --script frogdb-server/crates/redis-regression/tests/audit_tcl.py all \
  > /tmp/claude/audit_results.json

# Inspect a specific port's gaps
python3 frogdb-server/crates/redis-regression/tests/show_missing.py zset_tcl.rs --gaps
python3 frogdb-server/crates/redis-regression/tests/show_missing.py zset_tcl.rs --documented
python3 frogdb-server/crates/redis-regression/tests/show_missing.py zset_tcl.rs --tag-excluded
python3 frogdb-server/crates/redis-regression/tests/show_missing.py zset_tcl.rs --all
```

`show_missing.py` accepts a filter mode: `--gaps` (default — only real
unclassified gaps), `--documented` (entries that matched a `## Intentional
exclusions` bullet), `--tag-excluded` (auto-excluded by upstream tags), or
`--all`.

The `audit_tcl.py` script hard-codes `REDIS_ROOT` to
`/tmp/claude/redis-tcl/redis-8.6.0` (cache location); `REGRESSION_ROOT` is
derived from `Path(__file__).parent` so the script always finds the port
files alongside itself.

Update this file when new ports land, when an upstream version bump happens,
or when the out-of-scope list changes.
