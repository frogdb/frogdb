# Regression-Test Coverage Audit — bugs fixed during proposals 01–40

**Date:** 2026-07-11. **Scope:** genuine bugs (not arch changes) found+fixed during the
proposal-01–40 refactor effort, per `todo/proposals/INDEX.md`'s bugs section + round entries +
git history. Proposals 42/43/44 excluded (already covered by their own TDD gates). Method: for
each fix, a test counts as COVERED only if it would FAIL were the fix reverted — merely
exercising the code path does not count.

**Result: 55 bugs → 44 COVERED, 5 PARTIAL, 6 MISSING.**

## Round 1 — proposal 01 + original flags

| Bug | Proposal/commit | Fix location | Regression test | Verdict |
|---|---|---|---|---|
| MSETEX/BITOP/XGROUP/XREADGROUP WAL persisted non-key arg or nothing | P01; `23d30ae7`, `06e83d22`, `5e533a08` | `commands/src/string.rs:1388`, `bitmap.rs:217`, `stream/consumer_groups.rs:26`, `stream/read.rs:167` | `register.rs:320` `every_write_command_declares_wal` — spec-level only | **PARTIAL** — no restart test replays these 4; "persisted the wrong arg" variant undetected |
| STORE-destination WAL gap (GEORADIUS/SORT STORE) | P01; `fc0dce48` | `geo.rs:524,670`, `sort.rs` | `integration_persistence.rs:381,447` restart-survival | COVERED |
| ZINCRBY missing waiter wake | P01; `182898b6` | `sorted_set/basic.rs:361` | `register.rs:339` (spec `wakes == Kind(SortedSet)`) | COVERED |
| LREM emitted no keyspace event | P01 | `list.rs` EventSpec | `integration_pubsub.rs:55` | COVERED |
| RPOPLPUSH/LMOVE never woke blocked list waiters | P01 | `list.rs` wakes | `register.rs:339` | COVERED |
| GEORADIUS STORE dest missing from keys() | P01; `fc0dce48` | `geo.rs:510,524` | `geo.rs:1236,1263,1282` units | COVERED |
| GET/HGET miss misclassified (Bulk(None) vs Null) | `23469adc` | `core/src/command.rs` `record_keyspace_lookup` | `integration_metrics.rs:308,347` exact deltas | COVERED |
| INFO hardcoded keyspace_hits/misses:0 | `02de350b` | `server/src/commands/info.rs` | `integration_metrics.rs:641` | COVERED |

## Round 1 — replication/persistence flags

| Bug | Commit | Regression test | Verdict |
|---|---|---|---|
| Replication offset never persisted after startup | `17f01c9d` | `integration_replication.rs:3645,3714,3985` | COVERED |
| Checkpoints recorded offset 0 (stale field) | `64f15bce` | `replica_session.rs:1363`; `integration_replication.rs:3792` | COVERED |
| INFO master_replid zeros (+ replid2 window) | `99c8f91e`, `856107e6`/`2bd83e68` | `integration_replication.rs:2335,3897`; `info/sections.rs:842,864` | COVERED |
| Shard-count mismatch silently dropped recovered data | `95da0256` | `rocks/tests.rs:258,282`; `integration_persistence.rs:307` | COVERED |
| Incomplete staged checkpoint installed → data loss | `3e37ad7b` | `rocks/tests.rs:443` + 7 siblings | COVERED |

## Round 2 flags (proposals 07–13)

| Bug | Commit | Regression test | Verdict |
|---|---|---|---|
| Broadcast gathers no timeout; send failures under-reported | `d5b8de0d` | `scatter/broadcast.rs:901,839` — seam only, predates fix | **PARTIAL** — nothing asserts handlers route through the timed seam |
| SCRIPT LOAD diverged per-shard Lua cache | `d5b8de0d` | `integration_scripting.rs:387` happy path only | **MISSING** — passes with fix reverted |
| In-MULTI ACL denials skipped ACL LOG | `d1d871a6` | `integration_acl.rs:1664` | COVERED |
| NOPERM subcommand space vs pipe | `a1bed2be`+`d1d871a6` | `integration_acl.rs:1709`; `checker.rs:319` | COVERED |
| ACL DRYRUN mis-simulated key access | `4854d00b` | `integration_acl.rs:1743,1777,1811` | COVERED |
| VectorSet bound asymmetry → silent key loss | `048dc792` | `vectorset.rs:1026-1088` | COVERED |
| TimeSeries unknown DuplicatePolicy coerced to Last | `5b37cbbf` | `serialization/timeseries.rs:178` | COVERED |
| READONLY t-digest COW clone per query | `2e7da3a4` | none | **MISSING** — revert compiles, all numeric tests pass |
| Field-emptied key expiry silent + under-counted | `911b2525`+`6017333e` | `event_loop.rs:404,433` | COVERED |
| Active-expiry scan outside time budget | `ffdd156f` | `active_expiry.rs:381,398,267` | **PARTIAL** — nothing pins batch cap/boundedness |
| Blocking timeout wrong RESP2 nil shape | `98309ea3`+`40e88bcb` | `blocking_nil_shape_regression.rs:44-116` raw RESP2 | COVERED |
| Blocking lost-element timeout race (data loss) | `733e4d67` | `blocking.rs:1080,1108,1134` | COVERED |
| Warm-tier toggle broke reopen | `ff24a1a4` | `rocks/tests.rs:220,240` | COVERED |
| `list_cf` failure silently disabled both reopen guards | `ff24a1a4` | none | **MISSING** — needs injectable failure seam |

## Round 3 flags (proposals 14–20)

| Bug | Commit | Regression test | Verdict |
|---|---|---|---|
| Replica/primary offset unit drift (frames vs payload) | `944e8882` | `offset_coordinator.rs:151,126` | COVERED |
| Full-sync handoff dropped writes | `312f57fb` | `replica_session.rs:989`; `integration_replication.rs:4147` | COVERED |
| GETACK built with offset 0 | `944e8882` | `primary/tests.rs:209` | COVERED |
| FT.* OK-on-persist-failure; silent corrupt-index skip | `38258ea5`+`d041912e` | `lifecycle.rs:512,531,552,603` | COVERED |
| SSUBSCRIBE bypassed migration routing | `92bfc083` | `integration_pubsub.rs:1442` MOVED only | **PARTIAL** — no ASKING-serves-local / unassigned→CLUSTERDOWN at SSUBSCRIBE level |
| MOVED IPv6 format drift | `bfb49b6a`/`736a4115` | `redirect.rs:71,87`; `slot_migration/tests.rs:271` | COVERED |
| Vector field-state desync (4 sub-bugs) | `7cf34fd5`+`77349ebf` | `vector.rs:576-811` all four pinned | COVERED |
| Eviction-config validation duplicated / unreachable!() | `ef8a092f`/`70656956`/`ccf01e13` | `runtime_config.rs:2079,2194`; `integration_admin.rs:908` | COVERED |
| `frogdb_eviction_samples_total` missing `policy` label | `01cd1bda` | none | **MISSING** — no test scrapes the labels |

## Round 4 flags (proposals 22–28)

| Bug | Commit | Regression test | Verdict |
|---|---|---|---|
| Cross-shard keyspace notifications lost | `9ea530c0` | `integration_pubsub.rs:206,261,308` | COVERED |
| MOVED IPv6 unbracketed in blocking path | `5072ebd4` | `blocking.rs:1171` | COVERED |
| Lua cross-shard fallthrough wrote to wrong shard | `fd194cf2` | `gate.rs:734` (err + store untouched) | COVERED |
| XAUTOCLAIM pending-count corruption; XCLAIM TIME discarded | `75da9130`+`2aca32a9` | XAUTOCLAIM: `stream_cgroups_tcl.rs:2105`, `stream.rs:1993`. XCLAIM TIME: none | **PARTIAL** — XCLAIM TIME maps to `Instant::now()`, black-box-indistinguishable from discard; needs clock injection |
| Snapshot tmp-dir leak + incomplete-snapshot install | `cc4d865a`+`7462374d` | `snapshot/tests.rs:276,325,246,355` | COVERED |
| INFO persistence WAL placeholder zeros | `d6d8008b` | `sections.rs:740,730` | COVERED |
| RESP3 confirmations Push in EXEC, Array elsewhere | `5f288af2` | `integration_pubsub.rs:1551,1521,1641` | COVERED |

## Round 5 flags (proposals 29–40)

| Bug | Commit | Regression test | Verdict |
|---|---|---|---|
| WAL flush errors swallowed; ack-before-flush (silent data loss) | P29 | `wal/tests.rs:501,525,542,559`; `integration_info.rs:222` | COVERED |
| Stale expiry index deletes persistent key | P30 | `hashmap.rs:1348`; `active_expiry.rs:415` | COVERED |
| `memory_used` drift on in-place growth | P30 (`038974bb`) | `hashmap.rs:1480,1527,1561,1605,1627` | COVERED |
| Failover non-atomic saga | P31 | `state.rs:1505,1540,1565` + `lint-failover-atomicity` gate | COVERED |
| Lua load-time VM sandbox hole | P32 | `loader.rs:620`; `sandbox.rs:986`; `functions_regression.rs:51` | COVERED |
| BCAST tracking prefixes replaced not accumulated | P34 | `integration_client.rs:1452` + units | COVERED |
| Redirect-tracking task leak on re-enable | P34 | none | **MISSING** — nothing exercises `lifecycle.rs:89-92` abort-before-respawn |
| WAIT never solicited GETACK | P39 | `integration_replication.rs:516`; `primary/tests.rs:209` | COVERED |
| VLL abort_remaining dispatch-indices-as-shard-ids lock leak | P37 (`6bf23678`) | `coordinator.rs:656,614` exact-set asserts | COVERED |
| `frogdb_snapshot_epoch` unregistered | P35 | `metrics_usage.rs:79` | COVERED |
| Snapshot num_keys always 0; backup-dir leak | P40 | `rocks/tests.rs:534,567`; `snapshot/tests.rs:104` | COVERED |
| Connection-level execute() stubs fabricated +OK | P40 (`6e22cf06`) | none | **MISSING** — no test invokes the stubs directly to assert the loud error |

## Gap-closing work list (5 PARTIAL + 6 MISSING)

1. MSETEX/BITOP/XGROUP/XREADGROUP: restart-survival test in `integration_persistence.rs`
   replaying all four (mirror the GEORADIUS-STORE test).
2. Broadcast-gather timeout: integration test stalling/dropping a shard, assert PUBSUB
   CHANNELS / SCRIPT LOAD error within deadline (proves handlers use the timed seam).
3. Active-expiry budget: store-level `get_expired_*_limited(now, N)` ≤ N under avalanche +
   `run_cycle` bounded-deletions assert.
4. SSUBSCRIBE routing: ASKING-serves-local + unassigned→CLUSTERDOWN at SSUBSCRIBE level.
5. XCLAIM TIME: requires clock injection into `ClaimOpts::apply` (currently non-observable —
   implementation property, not just test gap).
6. SCRIPT LOAD per-shard cache: multi-shard test w/ one unavailable shard → SCRIPT LOAD errors
   (not SHA); EVALSHA with keys on every shard.
7. t-digest read-path COW: assert repeated TDIGEST.QUANTILE leaves stored value unmutated.
8. `list_cf` failure propagation: injectable failure seam; assert error propagates.
9. Eviction-samples `policy` label: metrics-scrape test asserting the labeled series.
10. Redirect-tracking task leak: enable TRACKING REDIRECT twice, assert first task aborted.
11. Connection-level stubs: unit test calling MULTI/EXEC/DISCARD/WATCH/UNWATCH `execute()`
    directly, assert `Err(CommandError::Internal)`.

## Inventory corrections found

- INDEX.md attributes the MSETEX/BITOP/XGROUP/XREADGROUP WAL fixes to the `fc0dce48` era;
  they actually landed in `23d30ae7`, `06e83d22`, `5e533a08`.
- The XCLAIM TIME "fix" is behaviorally non-observable as implemented (collapses to
  `Instant::now()`); untestability is a property of the implementation.

## Update

- 2026-07-12: all 11 gaps closed by the test wave (see git history: worktree-agent merges on main);
  XCLAIM TIME made observable via ClaimClock seam; consumer-group persistence gap discovered in the
  process (XGROUP/XREADGROUP restart tests #[ignore]d pending a fix).
