# Rulings on the independent distsys review

Source: [2026-08-13-independent-distsys-review.md](2026-08-13-independent-distsys-review.md).
User rulings, recorded as issued. Findings not listed here are not yet ruled.

## Global principle (ruled with CRIT-2)

**No wall-clock time in anything state-related.** Wall-clock timestamps must not gate,
order, or admit state-machine transitions anywhere in the system. Time-derived values may
exist as observability data only. This generalizes issue 17's "log-ordered fence, no
wall-clock" ruling from a per-issue decision to a campaign-wide design rule; apply it when
triaging the remaining findings and when writing phase-3 specs/models.

## CRIT-2 — proposer-minted handoff deadlines

Ruling: **logical admission token** (reviewer's suggested resolution (b), option 1).

- The source's drain confirmation carries the `handoff_seq` it drained.
- `CompleteSlotMigration` is admitted iff that seq matches the migration's current seq.
- `prepared_at_ms` / `barrier_ms` / `lease_ms` comparisons are removed from admission
  logic; timestamp fields are demoted to observability-only (may remain in replicated
  payloads for operator visibility, never consulted by `admits_*` / `live_handoff_at`).
- Spec work: amend TR-CLUSTER-013 + FM-CLUSTER-089; add an FM row for the
  stale-seq completion attempt (target proposes completion with an outdated seq → rejected).
- Note: Task 2's quint model already encodes these semantics (`drained` flag,
  seq match, `inv_complete_requires_drained`) — code converges on the model.
- **MAJ-4 folded in** (ruled 2026-08-14): the preamble's directional clock-skew
  argument is backwards for the applying node (forward skew at apply makes
  `!barrier_expired` *refuse more*, not admit more; the apply-side hazard is backward
  skew). CRIT-2's spec amendment must replace that preamble paragraph with the
  role-split statement: proposer-forward-skew was the admission hazard (now moot under
  the logical token); applier skew is irrelevant once the value is replicated data.

## MAJ-4 — preamble's directional clock-skew argument is backwards

Ruling: **fold into CRIT-2's spec work** — no separate issue. CRIT-2's amendment
deletes wall-clock admission reasoning from this area; the requirement that it replace
the preamble with the role-split statement is recorded on the CRIT-2 entry above.

## MAJ-1 — split-brain audit floor collapses to 0

Ruling: **accept, file issue** (reviewer's resolution). Filed as
[replication issue 31](../replication-correctness/issues/open/31-divergence-audit-floor-from-successor-offset.md)
(ready-for-agent); rides the implementation wave. Floor from the `DemotionEvent`'s
successor offset; `unknown-floor` variant when absent — never a silent 0; invert the
zero-floor pinning test.

## MAJ-2 — TR-REPLICATION-022 node-id dedup key does not exist on the wire

Ruling: **fix the protocol** (reviewer's resolution (a), option 1).

- Replica mints and persists a replica run id; transmits it during the replication
  handshake as a `REPLCONF replica-id <id>` capability.
- Session dedup keys on that id, making the AMENDED TR-REPLICATION-022 ruling
  implementable as written; the NAT case (two replicas behind one NAT announcing the
  same `listening-port`) becomes genuinely distinguishable.
- Spec work: amend FM-REPLICATION-049 (announced identity grows the replica id);
  re-key locked INV-SESSION-2 on the new identity — locked-invariant edit goes through
  the normal spec-first flow (failure-mode row → forcing test → change).
- Explicitly ruled: **no backward-compatibility constraint** — pre-alpha software, the
  handshake may require the new capability outright rather than gate on it. Get the
  spec/protocol correct; do not carry a compat shim.

## MAJ-3 — FM-REPLICATION-019 contradicts the State-space table on offset rewind

Ruling: **accept, file issue** — the rewind is the truth (a node must never claim
bytes it did not apply); FM-019's continuity cell is the lie. Amend FM-019, add a
downstream-ahead-of-new-primary FM row (full resync forced) with forcing tests. Filed
as [replication issue 32](../replication-correctness/issues/open/32-fm-019-offset-rewind-truth-reconciliation.md)
(ready-for-agent); rides the implementation wave. Coordinate with issue 24's atomic
replid/offset pairing.

## MAJ-5 — source write pause unfenced after `barrier_ms` deletion

Ruling: **redesign, option B — source-authoritative-until-commit** (filed as
[cluster issue 31](../cluster-correctness/issues/open/31-slot-migration-redesign-source-authoritative-until-commit.md)).

Triage found MAJ-5's premise partially resolved already (feed byte-cap = issue 17
amendment; abort mechanism = issue 15 amendment; FM-CLUSTER-084/085 re-derivation =
issue 29 sweep) but exposed a dangling reference: issue 17's amendment assigns orphaned-
handoff liveness to an "issue 18 reconcile abort" that issue 18 never defines. Rather than
patch (amend 18 + repatriation-when-possible), the user ruled the structural fix: drop the
Redis-style delete-as-you-copy bulk phase; source retains keys and serves all traffic
until `Complete` applies; target catches up via slot-scoped mutation stream; abort =
target-discard, safe at any time including dead target; reconcile orphan-abort on the
FAIL-flag criterion. Supersedes issue 15's repatriation. Design is HITL — brainstorm
before phase-3 models encode migration semantics. Details in issue 31.

## MAJ-6 — `CLUSTER RESET` is replicated and therefore cluster-wide destructive

Ruling: **node-local** (reviewer's first option; destructive scope = invocation
scope, Redis parity). Reset departs and clears only the issuing node; surviving
topology untouched; cluster-wide wipe stays achievable node-by-node. Filed as
[cluster issue 34](../cluster-correctness/issues/open/34-cluster-reset-becomes-node-local.md)
(ready-for-agent); rides the implementation wave. Departure uses issue 20's
demote-don't-remove shape.

## MAJ-7 — node ids: 16 bits/ms, regenerated every boot, upsert-merged collisions

Ruling: **accept, file issue** (reviewer's resolution, plus no-timestamp component per
the no-wall-clock principle and an etcd-style boot guard). Mint once ≥128 random bits,
persist beside `database_id`, read back on boot; `AddNode` rejects conflicting ids;
boot refuses id/data-dir mismatch. Filed as
[cluster issue 35](../cluster-correctness/issues/open/35-node-identity-outlives-the-process.md)
(ready-for-agent); rides the implementation wave. Retires Task 1's node-id-stability
deferred minor; aligns with MAJ-2's replica run id family.

## MAJ-11 — biased select falsifies TR-BLOCKING-007; H5 unsound as ordered

Ruling: **eliminate sender-drops as a signaling mechanism** (reviewer's resolution,
option 1). The deadline fast-path (`shard/blocking.rs:320-322`) sends
`entry.op.timeout_reply()` + increments `BlockedTimeoutTotal` instead of dropping;
`Satisfaction::Retry` and the admission refusal likewise send real replies (H7 already
covers the latter). After that, `Err(_)` at the coordinator uniquely means channel death
and spec-gaps issue 08's H5 (`-ERR shard unavailable`) is sound as written. Amend
TR-BLOCKING-007 to remove the "server's reply normally wins" race claim (it is
deterministically false — `biased;` select, `response_rx` first). Ordering dependency
recorded as an amendment on
[spec-gaps issue 08](../spec-gaps/issues/open/08-blocking-command-rows.md).

## CRIT-1 — restarted source never re-arms its slot write barrier

Ruling: **fix the spec now; code fix with the implementation wave** (clarified by the
user after the initial "fix now"). Filed as
[cluster issue 32](../cluster-correctness/issues/open/32-restarted-source-never-re-arms-its-slot-write-barrier.md)
(ready-for-agent). Spec half landed same day: FM-CLUSTER-104 (gap-cited to issue 32 per
the `MISSING ([gap: …])` mechanism) plus the `PauseState.slots` State-space row restated
to stop laundering "reconstructible in principle" as a property. Code half (reconcile +
snapshot-delta emission + forcing test) rides the ruled-issues implementation wave.
Survives issue 31's redesign (finalization drain keeps the barrier; restart mid-drain
still needs re-arm).

## CRIT-3 — `wal_watermark` can lead the durable point

Ruling: **accept, file issue** (reviewer's resolution). Filed as
[spec-gaps issue 12](../spec-gaps/issues/open/12-wal-watermark-carries-covered-sequence.md)
(ready-for-agent); fix rides the implementation wave. Covered sequence carried through
the write path into `record_wal_watermark(covered_seq)` as a `fetch_max`; amend
FM-PERSISTENCE-035; rewrite the pinning test
(`only_a_synced_rocks_sink_commit_records_the_durable_watermark` asserts the buggy
equality); add the two-shard forcing test.

## CRIT-4 — disconnect-while-parked undetectable; push consumed and delivered to nobody

Ruling: **restructure** (reviewer's "better" option). The blocking wait becomes a
run-loop state instead of an inline await, matching the Redis/Valkey/Dragonfly shape
(blocked client = state on a still-readable connection). Filed bundled with CRIT-5 as
[spec-gaps issue 13](../spec-gaps/issues/open/13-blocking-wait-becomes-a-run-loop-state.md);
rides the implementation wave.

## CRIT-5 — CLIENT KILL cannot terminate a parked client

Ruling: **accept, bundle with CRIT-4's restructure** (one issue, one pass over the
machinery). The restructure keeps `killed()` polled during the wait; CRIT-5-specific
remainder is the CLIENT KILL TR row + forcing test. Same
[spec-gaps issue 13](../spec-gaps/issues/open/13-blocking-wait-becomes-a-run-loop-state.md).

## CRIT-7 — cross-shard SCA admits hold-and-wait cycles

Ruling: **wound-wait on the SCA path** (reviewer's suggested resolution). The user
probed the alternative (Calvin-style atomic globally-ordered multi-shard declaration,
incl. whether ignoring the crates' locked status changes the call — it does not: lock
status is process cost paid equally by both options; the driver is architectural).
Wound-wait is a local lock-table change preserving shared-nothing async scatter; the
sequencer adds an ordering point to the multi-shard hot path. Filed as
[spec-gaps issue 14](../spec-gaps/issues/open/14-sca-wound-wait-restores-acyclicity.md)
(ready-for-agent); rides the implementation wave. Liveness proviso recorded in the
issue: a wounded txn's retry keeps its original txid (age-based priority), else
starvation. Partially moots MAJ-20's timeout-only exit for this cycle class.

## CRIT-6 — `replication_state.json`: shared temp path, two writers, no fsync

Ruling: **accept, file issue** (reviewer's resolution: `stamp_with` reuse + unique temp
+ serialized writers + FM row). Filed as
[replication issue 30](../replication-correctness/issues/open/30-replication-state-file-atomic-durable-single-writer.md)
(ready-for-agent); rides the implementation wave.
