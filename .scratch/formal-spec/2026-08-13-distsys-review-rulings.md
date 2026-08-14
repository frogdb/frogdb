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
