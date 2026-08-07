# Sweep: 11 tests that cannot fail — repair the assertions or delete the test

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §4
Score: aggregate of 11 findings
Area: frogdb-server (cluster, vll, protocol) · frogdb-persistence · frogdb-cluster · redis-regression · frogdb-core

## Context

Eleven tests found by the audit are structurally incapable of failing: zero assertions, early `return`s
that count as passes, `eprintln!` where an assert belongs, tautologies, epsilon comparisons that hide
the defect they cover, lower-bound-only assertions, and one test whose key names collide on the same
shard so it never exercises its named path. Each is **worse than no test, because it reads as
coverage**: `trigger_auto_failover` has a named test that never enables the flag, and the flagship
cross-shard atomicity test passes when every reply is an error. Sweep — one checkbox per instance.

## Evidence

- **04/F3** — `integration_cluster.rs:2589 test_replica_receives_writes`: `eprintln!` only, no asserts.
  `:2706 test_promoted_replica_has_all_data` never asserts `found_count`, counting `MOVED` as "found".
- **12/F4** — `server/tests/simulation.rs:1560-1595` computes `has_value =
  response_str.contains("$1\r\n1\r\n")` / `has_nil = contains("$-1\r\n")`, failing only when *both* hold.
  An error sets neither → `assert!(!saw_partial)` passes when every reply is `-ERR`. No assertion any
  poll saw the committed state; no post-run `MGET`.
- **13/F11** — `core/src/persistence/crash_recovery_tests.rs:719` `assert!(result.is_ok() ||
  result.is_err());` is a tautology; `:724 test_incomplete_snapshot_skipped` never invokes the
  coordinator, re-reading the `metadata.json` the test wrote — it tests `serde`, not skip logic.
- **13/F18** — `rocks/tests.rs:1243` asserts `matches!(DBRecoveryMode::PointInTime,
  DBRecoveryMode::PointInTime)` — a literal, not the production options at `rocks/mod.rs:167`. Round-1
  issue 25's type-flip residue and issue 43's generation spread are `eprintln!`'d (nextest discards
  stdout on pass); issue 45's `handle_lastsave` fix has a ±1 s bound looser than the bug.
- **11/F14** — `integration_cluster.rs:10503-10510 test_remove_node_during_active_migration` `return`s
  early — a **pass** — on setup error; `:4763 test_raft_snapshot_during_migration` only `eprintln!`s
  `migration_state_found`/`slot_ownership_clear`; `cluster/src/storage.rs test_storage_committed`
  never reads `committed` back; `network.rs test_all_rpc_variants_roundtrip` asserts the discriminant.
- **07/F15** — sketch assertions lower-bound-only: `cms_topk_regression.rs:100,123,168,212,579` and
  `integration_cms.rs:359` are `>=`; `hyperloglog_regression.rs:12,17,35` assert `>= 3`/`>= 5`/`>= 9`
  — an HLL returning `i64::MAX` passes all three. `tdigest_regression.rs:353,470` use ±5.0 on a
  uniform 1..100; `:606` asserts only `items.len() == 5` for eviction.
- **08/F10** — `protocol/src/response.rs:1039 test_double_to_resp2_string` parses the encoding back to
  `f64` under a `1e-10` epsilon, passing whether the encoder emits `3.14` or `3.14000000000000012`. Same
  at `resp3.rs:385`, `:412`, `property_tests.rs:210`. `response.rs:1470-1540` (exact bytes) is the model.
- **03/F2** — the 15 COPY integration tests never exercise cross-shard COPY: `integration_copy.rs`
  uses `"src"`/`"dst"`, and `crc16_xmodem("src") % 16384 % 4 == 2 == crc16_xmodem("dst") % 16384 % 4`.
  `connection/routing.rs:197 execute_cross_shard_copy`: `untested`, **102 regions**, all four records.
- **04/F2** — `integration_cluster.rs:7773 test_auto_failover_selects_most_caught_up_replica` asserts
  only `info.cluster_state == "ok"`, its comment concedes it cannot measure offsets, and
  `auto_failover` defaults `false` → `failure_detector.rs:330 trigger_auto_failover` (**0/104
  regions**) never executes.
- **15/F13** — `redis-regression/tests/auth_tcl.rs tcl_protected_mode_works_as_expected` has **no
  assertions at all** — start, connect, return, under a 5-point comment of what it "verifies". Sibling
  `..._binary_password_is_wrong` (`:100-144`) re-sets `requirepass` to the *same* value.
- **12** — `core/tests/concurrency.rs:641 test_mset_cross_shard_partial_visibility` asserts partial
  cross-shard visibility is *acceptable*, against a mock cluster, contradicting VLL's contract.

## What to fix

Per instance: give it assertions that fail against a deliberately broken impl, or delete it. Early
`return` → `panic!`; `eprintln!` residue → bounded assertion; epsilon float compare → exact bytes;
lower-bound sketch assertion → two-sided bound; assert values read back from production paths.

## Acceptance criteria

- [ ] **04/F3** — both cluster-replication tests assert what is observable today (role in `CLUSTER
      NODES`, `MOVED` without READONLY) plus a tripwire pin recording that data does *not* arrive,
      citing `.scratch/replication-cluster-rework/wait-cluster-mode.md`. `MOVED` never = success.
- [ ] **12/F4** — `simulation.rs:1560-1595` asserts each of the 100 replies is a well-formed 2-element
      array (failing on any `-ERR`/`BUSY`), some poll observed the committed `[1,1]`, and a final
      `MGET key_a key_b == [1,1]`. Same for the sibling chaos tests.
- [ ] **13/F11** — `:719` asserts a concrete contract; `:724` calls `load_latest_metadata`, asserting `(epoch, None)`.
- [ ] **13/F18** — the WAL-mode pin asserts the value read back from the constructed `Options`
      (deleting the production pin fails the test); both `eprintln!` residues become bounded
      assertions; the LASTSAVE bound is tightened below the magnitude of the bug it fixed.
- [ ] **11/F14** — no early `return` on setup failure in the four named tests;
      `test_remove_node_during_active_migration` asserts slot 950 is owned by exactly one **live** node
      and no migration record references the forgotten node; `test_raft_snapshot_during_migration`
      renamed to what it tests; `test_storage_committed` reads `committed` back;
      `test_all_rpc_variants_roundtrip` asserts payload equality, not the discriminant.
- [ ] **07/F15** — two-sided sketch assertions: HLL within its error bound in *both* directions, CMS
      `query(x) >= true_count(x)` **and** `<= true_count(x) + eps * total`, TopK returns the true
      top-k for a separated distribution, one test inspects a non-nil expel from `topk.rs:153`.
- [ ] **08/F10** — the four epsilon comparisons become exact-byte assertions over a table including
      `-0.0`, `1e-320`, `1e300`. Coordinate with issue 26, same directory.
- [ ] **03/F2** — `integration_copy.rs` gains a cross-shard mirror of each existing case, key names
      *asserted* to differ in shard, covering value bytes, TTL within ±50 ms, `REPLACE`, the
      no-`REPLACE` `Integer(0)` path, missing source, both shard-unavailable arms.
- [ ] **04/F2** — `test_auto_failover_selects_most_caught_up_replica` enables `auto_failover`, or is
      renamed to what it asserts and `trigger_auto_failover` gets real coverage.
- [ ] **15/F13** — `tcl_protected_mode_works_as_expected` gains real assertions or is deleted;
      `tcl_auth_fails_when_binary_password_is_wrong` exercises an actual password *change*.
- [ ] **12** — `concurrency.rs:641 test_mset_cross_shard_partial_visibility` renamed to make clear it
      models a non-VLL path, or deleted.
- [ ] No test in this list passes against a deliberately broken impl of what it names.

## Test boundary

**Unchanged from each test's current level** — assertion strength, not relocation — with three
exceptions: 08/F10's protocol-crate case drops to **1** (pure rendering; `resp3.rs` stays at 4, also
asserting protocol-version dispatch, with exact float bytes); 07/F15 moves to **1** as property tests,
the invariant being statistical; 03/F2's cases stay at **4** — two-phase cross-shard COPY lives in the
routing layer above the shard worker, where `shard_driver` cannot reach.

## Depends on

Nothing for ten of the eleven. **04/F3's full version is blocked** on the unreviewed PRDs in
`.scratch/replication-cluster-rework/` — per `wait-cluster-mode.md` §1–2.2 a cluster primary refuses
the PSYNC its replica is told to open, so no test can assert a cluster replica holds primary data
today; its checkbox is scoped to what is observable now plus a tripwire.

## Re-triage 2026-08-06

**Verdict: partially-fixed**

**1 of 11 instances fixed, 2 partially, 8 reproduce verbatim.** `integration_cluster.rs` was split
into per-concern binaries (`e163ff9a`), so all cluster refs move to
`server/tests/cluster_failover.rs` / `cluster_migration.rs`.

- **04/F3 — FIXED.** `test_replica_receives_writes` (`:2589`) → renamed
  `test_cluster_replica_receives_writes_asserted` (`cluster_failover.rs:1682`) and now asserts
  `connected_slaves:1`, the replicated value under `READONLY`, and `role:slave`.
  `test_promoted_replica_has_all_data` (`:2706` → `cluster_failover.rs:2233`) asserts all 10 keys
  with a `WAIT 1 10000` barrier and an explicit "`MOVED` here is a failure" comment. The blocker in
  "Depends on" is lifted — `c6625ae9` wired cluster replication end to end, so the full version was
  writable, not just the tripwire.
- **04/F2 — PARTIAL.** `test_auto_failover_selects_most_caught_up_replica`
  (`cluster_failover.rs:3591`) no longer asserts `cluster_state == "ok"`; it polls to
  `cluster_state == "fail"` + `cluster_slots_fail > 0` (`:3643-3650`), and `trigger_auto_failover`
  now has real coverage — three unit tests at
  `cluster-runtime/src/failure_detector.rs:1719,1750,1772` (moved from `server/src/failure_detector.rs`).
  Not done: the test is still misnamed for what it asserts (it never enables `auto_failover` and
  never compares replica offsets).
- **13/F18 — PARTIAL.** The two `eprintln!` residues are gone (no `eprintln!` remains anywhere under
  `crates/persistence/src` or `crates/core/src/persistence`). The WAL-mode pin (`rocks/tests.rs:1243`
  → `persistence/src/rocks/tests.rs:1312-1318`) is still the literal tautology
  `matches!(DBRecoveryMode::PointInTime, DBRecoveryMode::PointInTime)` — deleting the production pin
  at `rocks/mod.rs:167` → `:188` still passes it — though it is now documented as a deliberate anchor
  with behavioural proof in the adjacent corruption tests. The LASTSAVE bound is still `±1s`
  (`server/src/connection/persistence_conn_command.rs:526-529`).
- **12/F4 — still valid.** `simulation.rs:1560-1595` → `:1561-1594`, byte-for-byte the same
  `has_value && has_nil` shape and bare `assert!(!saw_partial)`. Two more instances of the same
  pattern now exist: `:1947-1971` and `:1305-1309`.
- **13/F11 — still valid.** `assert!(result.is_ok() || result.is_err())` moved `:719` →
  `core/src/persistence/crash_recovery_tests.rs:729`; `test_incomplete_snapshot_skipped` `:724` →
  `:735`, still reads back the `metadata.json` it wrote and asserts `serde`. Both now carry
  `// FM-PERSISTENCE-017` tags, so the failure-mode spec credits them as forcing tests.
- **11/F14 — still valid, all four.** `test_remove_node_during_active_migration`
  (`cluster_migration.rs:2563`) still `return`s on setup error (`:2617-2624`);
  `test_raft_snapshot_during_migration` (`:1053`) still `eprintln!`s
  `migration_state_found`/`slot_ownership_clear` and its only assert is the disjunction at `:1240`;
  `cluster/src/storage.rs:656 test_storage_committed` still never reads `committed` back;
  `cluster/src/network.rs:891 test_all_rpc_variants_roundtrip` still asserts only the discriminant.
  The last two now carry `// FM-CLUSTER-017` / `// FM-CLUSTER-051` tags.
- **07/F15 — still valid.** `hyperloglog_regression.rs:12,17,35` still `>= 3`/`>= 5`/`>= 9`;
  `cms_topk_regression.rs:100,123,168,212,579` still lower-bound-only.
- **08/F10 — still valid, all four.** `protocol/src/response.rs:1039` → `:1031` (still `1e-10`).
  Correction to the body: `resp3.rs` and `property_tests.rs` are **not** in the protocol crate —
  they are `frogdb-server/crates/server/tests/resp3.rs:385,412` and
  `server/tests/property_tests.rs:275-281` (was cited as `:210`). The exact-byte model is now
  `response.rs:1470-1521`.
- **03/F2 — still valid.** `integration_copy.rs` still uses `"src"`/`"dst"` throughout;
  `connection/routing.rs:197 execute_cross_shard_copy` unchanged.
- **15/F13 — still valid.** `redis-regression/tests/auth_tcl.rs:135
  tcl_protected_mode_works_as_expected` still has zero assertions; `:104` still re-sets
  `requirepass` to the same value.
- **12 — still valid.** `core/tests/concurrency.rs:641 test_mset_cross_shard_partial_visibility`
  unchanged, still documenting partial cross-shard visibility as acceptable against `TestCluster`.
