# Anti-pattern review of the LOCKED specs + 2026-08-13 rulings

Four parallel Opus reviewers audited `specs/{cluster,replication,persistence,txn,vll,blocking}.md`
and the freshly recorded rulings against modern distributed-systems practice
(CockroachDB / etcd / FoundationDB / Kafka / raft literature), per the standing directive to
check every decision for anti-patterns (wall-clock-as-correctness, edge-triggered recovery,
fencing gaps, non-idempotent reconciliation). Full per-area reports sit beside this file.

**Verdict: NOT clean. 5 CRITICAL, 23 HIGH, ~20 ADVISORY.** Several findings amend the
2026-08-13 rulings themselves; those need a follow-up ruling round before implementation
dispatch.

## CRITICAL (5)

| # | Area | Finding |
|---|------|---------|
| P-C1 | persistence | `durability sync` does not gate the client ack under the default `wal-failure-policy = continue`: `Durability::Confirm` on the ack path is selected only by `should_rollback()` (`execution.rs:437`); the default path persists `FireAndForget` (`post_execution.rs:401`). FM-PERSISTENCE-002's forcing tests bypass the shard ack path. **Mechanism confirmed in code by orchestrator.** |
| CL-C1 | cluster | Ruling 17 (log-ordered fence) drops the wall-clock bound without bounding the node-wide replica-feed hold: per-session `VecDeque` and the hold become unbounded if Complete/Abort lags; FM-CLUSTER-097's "a feed cannot wedge" NOT-observable becomes false; `admits_complete_at`'s `lease_expired` term left undefined. Needs an explicit bound (memory/backpressure), while keeping release log-ordered. |
| CL-C2 | cluster | Ruling 15 (prune on failover) leaves keys already `MIGRATE`d+deleted split across two nodes with no repatriation verb — Redis needs `redis-cli --cluster fix` for this state; FrogDB now enters it automatically on every failover mid-rebalance. |
| R-C1 | replication | Ruling 24 option (a) mints a fresh replid only when *no* dataset recovers. A primary restarting *with* a dataset resumes `(id, offset)` below what replicas acked and re-issues those offsets under the same id → `+CONTINUE` over divergent bytes. Must be (a)+(b): shift old id to `replid2` at a frozen boundary and mint fresh on every unclean primary restart (Redis RDB-aux does both). |
| T-C1 | txn | FM-TXN-030 asserts "script batches are slot-validated like any other" while its own bug ref concedes undeclared runtime script writes are unvalidated; FM-TXN-007 concedes ACL/NOREPLICAS/self-fence not applied to script writes inside MULTI. Unrowed ⇒ unforced by the mutation gate. |

## HIGH — amendments to the 2026-08-13 rulings

- **R21 (clamp acks): change clamp → ignore + count.** Clamping writes `min(wire, live)` into
  `acked_offset`, making the primary a writer of its own ack — contradicts
  FM-REPLICATION-039's "wire ACK is the only writer" and manufactures WAIT credit for bytes
  the replica never got (issue-28 class). Disposition (no disconnect) stands.
- **R22 (kick predecessor):** kick classifies as Graceful departure and lands *after* the
  successor's `ClearDeparture` — FM-REPLICATION-062's named NOT-observable; can silently
  un-fence. Add a `Superseded` disconnect outcome recording no departure. Also dedup key
  (peer IP + announced port) is peer-controlled → shared-egress replicas kick-loop; prefer
  node id + cooldown.
- **R24 (identity-in-checkpoint):** offset must commit in the same atomic unit as the write
  it names (manifest-time stamping biases low → non-idempotent replay: INCR/LPUSH/APPEND);
  sequence issue 24 before issue 17 (both relocate `offset_at_save`, neither cites the
  other); FM-REPLICATION-021's "same `master_replid` after reboot" Observable inverts and
  must be rewritten in 24's change set; INV-OFFSET-2 "monotone within a history" needs the
  history key `(replication_id, epoch)` to be evaluable.
- **C19 (global epoch fence):** collides with FM-CLUSTER-013 (every `MarkNodeFailed` bumps
  the epoch) → one failover per reconcile tick under correlated failure; starves under flap.
  Reviewer recommends per-object fence (deposed primary's epoch/role), per CRDB/Kafka/ZK.
  This re-opens the global-vs-per-node choice.
- **C18 (level-triggered reconcile):** no backoff, no in-flight dedup (GAP 2
  `InflightGuard`), no `MAX_ATTEMPTS` fate, no stuck-failover operator signal; combined with
  C19-global it re-proposes a refused failover every tick forever.
- **C20 (demote-don't-remove):** deferring the eviction fence leaves `CLUSTER FORGET` of a
  live node (and permanent `add_learner` failure) reproducing issue 20 exactly on the admin
  path; dead voters accumulate with no `CLUSTER INFO`/metric surface.
- **C14/C16/C25 residues:** AddNode refusal vs FM-CLUSTER-011 "never refuse membership"
  (order-dependent trap if bulk import lands); C16 still lets the source `DELSLOTS` a
  migrating slot — refuse all slot-map mutation on an open migration; C25 should also refuse
  MEET of a node with non-empty Raft state and persist bootstrap intent across restart.

## HIGH — spec gaps independent of the rulings

- **Cluster:** no failover drain/offset-parity barrier on either path (Redis manual FAILOVER
  is lossless by construction; ours loses acked writes on a *planned* failover); self-fence
  quorum derived from TCP probes, not Raft liveness (CheckQuorum-style freshness
  recommended); `CLUSTER FAILOVER FORCE` issued on a primary absorbs an arbitrary peer
  primary (HashMap iteration order) and evicts a healthy node.
- **Persistence:** fsyncgate — FM-007 *requires* un-latching after a later successful
  commit (Postgres PANICs; RocksDB latches read-only); "loss is always a suffix" (FM-003)
  falsified by `continue` (holes, not suffixes — replication consumes this); RocksDB
  multi-CF recovery without `set_atomic_flush` can resurrect deleted keys across hot/warm
  tiers; staged-checkpoint completeness check is `CURRENT.exists()` with no payload
  verification and `BACKUP_RETENTION=1` lets a retried sync destroy the last good copy.
- **VLL:** no-wait refusal + client retry with no priority → mutual-abort livelock
  (wound-wait on txid recommended; the "sorted shards" doc comment is not what provides
  safety — dispatch is concurrent); continuation lock has unbounded hold and no SCRIPT KILL
  row; phase-2/3 partial-failure unwind and gather timeouts are explicitly unrowed (a
  gather timeout currently *resolves an outcome* — wall-clock as correctness); panic
  isolation resumes serving with no statement about partial writes.
- **Blocking:** FM-BLOCKING-004 returns a bare `Null` that rows 002/003 declare
  NOT-observable (three-row contradiction; shard death reported as timeout); zero rows for
  demotion/slot-migration/disconnect-while-parked/WAIT; `ShardWaitQueue` admission limits
  (10k/key, 50k total) forced by no row.
- **Txn:** cross-shard presence probe is TOCTOU vs the shard round-trip — carry a routing
  epoch re-checked at apply (CRDB lease model).
- **Replication:** issue-16 freeze ruling's "reconciler self-heals" has no witness; stranded
  node serves unbounded stale reads with no stale-read gate row.

## Advisories

See per-area reports. Notables: R18's "unfalsifiable by construction" overclaims (closes
well-formedness, not provenance); R23 must write unknown-departure (keeps fencing), never
Graceful; R26 deserves an interim seam lint on the streaming path's gate calls; backup dir
key is a wall-clock second; FM-041 carried an unforced "state written at X" clause — audit
the class.

## What already matches or beats practice

Each report lists these; highlights: FM-CLUSTER-089 no-clock-in-apply (lint-enforced),
fencing token through both snapshot vehicles, ack-on-`landed` (stronger than Redis),
epoch-keyed divergence latch, FM-PERSISTENCE-023 fsync-both-sides-of-rename seam,
FM-BLOCKING-005 ack-based reconciliation, FM-TXN-032 ambiguous-commit honesty, VLL
drain-wait inversion, R20's role/membership separation, R25's etcd bootstrap shape.

## Disposition

Ruling amendments (R21, R22, R24, C15, C17, C18, C19, C20 surfaces) need user decisions
before the affected issues dispatch. Spec-gap findings (persistence P-C1/H*, vll/blocking
rows, failover drain) are spec-first candidates: new FM rows + forcing tests, filed as
issues after triage. Nothing here blocks formal-spec phase 2 scaffolding, but phase-2
models should encode the amended semantics, not the as-filed rulings.

## Amendment rulings (2026-08-13)

All eleven ruling amendments were settled with the user the same day and recorded in their
issue files (`## Amendment (2026-08-13)` sections):

| Ruling | Outcome |
|--------|---------|
| R21 | Reversed: clamp → **ignore + count** (no `acked_offset` write from an over-head ack) |
| R22 | Accepted both: **`Superseded`** outcome (no departure record) + dedup on node id + cooldown |
| R24 | Accepted all four: (a)+(b) replid2-shift + fresh mint on unclean restart; atomic offset pairing; issue 24 before 17; persistence constraints (quiesce-window write, mint-fresh on WAL truncation, intact-recovery definition) |
| C17 | **Byte-cap + disconnect** bounds the feed hold; release stays log-ordered; `lease_expired` term deleted |
| C18 | Hardening package: in-flight guard, capped exponential backoff, never abandons, stuck-failover signal |
| C19 | Reversed: global epoch → **per-object fence** (deposed primary's role-version; FM-CLUSTER-013 interaction decisive) |
| C15 | **Abort = rollback**: target repatriates importing-slot keys before Abort applies clean |
| C20 | Accepted all: live-voter FORGET guard (FORCE escape), eviction fence required, dead-voter observability |
| C14 | FM-CLUSTER-011 amended: refusal only for structural invalidity; bulk-import ordering documented |
| C16 | Tightened: **all** slot-map mutation refused on an open-migration slot; only Complete/Abort mutate |
| C25 | Accepted both: MEET of node with non-empty Raft state refused; bootstrap intent persisted |

The spec-gap findings (P-C1 ack-durability, T-C1 script bypass, failover drain, fsyncgate,
vll/blocking rows, …) remain open for triage into issues; they were not part of this round.

## Spec-gap rulings (2026-08-13, second round)

All remaining spec-gap findings were ruled with the user the same day and filed as issues
(cluster-correctness 26–29, replication-correctness 28–29 + addenda to 16/18/19/23/24/26,
new `spec-gaps` campaign 01–10):

| Finding | Ruling | Filed as |
|---------|--------|----------|
| CL-H2 failover drain | Planned failover lossless (pause→drain→offset parity→swap); honest async-loss row for auto path; quorum-ack write mode filed as design issue | cluster 26 + replication 29 |
| CL-H3 self-fence input | Raft-liveness fence (CheckQuorum shape), not TCP probes; preamble: clock-to-stop fail-closed OK, clock-to-admit anti-pattern | cluster 27 |
| CL-H6 FAILOVER on primary | Refused (Redis parity, replica-only) | cluster 28 |
| CL-A5 row edit-set | Tracked as checklist issue blocked on amended issues | cluster 29 |
| P-C1 sync ack gate | `Confirm` iff sync-mode ∨ rollback; FM-002 rewrite; e2e forcing test | spec-gaps 01 |
| P-H1/H2/A2 WAL failures | Poison latch (restart/operator clear only); prefix-truncate under `continue` restores loss-is-a-suffix; `wal-failure-policy=readonly` fail-stop | spec-gaps 02 |
| P-H3 atomic_flush | Enabled + pinned + crash forcing test | spec-gaps 03 |
| P-H4/A1/A4 checkpoint | MANIFEST-parse verify + payload manifest + trial-open/rollback + monotone backup key + frogctl verify | spec-gaps 04 |
| P-A3/A5 advisories | Confirm→Committed rename, synced_seq binding, recovery counters | spec-gaps 05 |
| T-C1 script bypass | Slot+ACL+admission enforced at shard write seam; seam lint; KNOWN-VIOLATED interim | spec-gaps 06 |
| V-H1..H4 VLL package | Wound-wait lowest-txid; SCRIPT KILL revoke row; ambiguous-not-clock gather outcomes; panic → shard WAL restart | spec-gaps 07 |
| B-H5/H6/H7 blocking | `-ERR shard unavailable` on shard death; demotion/migration/disconnect/WAIT rows; admission-limit row | spec-gaps 08 |
| T-H8/A2 TOCTOU | Routing-epoch carried, refuse at apply (CRDB lease shape); post-pause watch-set re-verdict | spec-gaps 09 |
| T/V advisories | FM-TXN-050 fold, 039→deviations, 019/021 reword, VLL-003 guard promoted, cancellation pinned | spec-gaps 10 |
| R-A4 stale reads | **Implement `replica-serve-stale-data` knob now** (user chose stronger option over doc-only) | replication 28 |
| R-A1/A3/A5/A6/A7, P-R4..R6 | Mechanical addenda appended to owning issues | replication 16/18/19/23/24/26 |
