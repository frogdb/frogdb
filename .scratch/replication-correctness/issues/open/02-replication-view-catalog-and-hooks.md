# 02 — `ReplicationView` + invariant catalog + seam hooks

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W1; view/catalog location ruled in §8 D7; vocabulary ruled in §8 D6.

## What to build

`frogdb-server/crates/replication/src/invariants.rs`, plus the `ReplicationView` capture that
feeds it. Because no single struct owns replication state (PRD §1 B4), the catalog cannot be
"pure functions over `&SomeStateInner`" the way `frogdb-cluster`'s is — it is pure functions over
a plain-data projection assembled from the live components at check time. The view shape is
fixed in §2: `state` (replid, secondary window, `offset_at_save`), the `live`/`applied`/`landed`
triple, `apply_gate` (frozen/stint/epoch/diverged), `backlog` (geometry + oldest/newest + entry
and byte totals), `replicas` (id, addr, announced_id, phase, acked, resume_floor, last_ack_age),
`departure`, `feed_gate` hold deadline, `fence`, and `role`. Every field already exists
somewhere; the view is assembly, not new state.

**D7 ruling, restated because it shapes the whole design:** view and catalog live in
`frogdb-replication` (so the mutation gate sees them); the fields owned elsewhere —
`ReplicationQuorumChecker` in `frogdb-replication-runtime` (`quorum.rs:52`), `RoleManager` in the
server crate (`role_manager.rs:119`), `ReplicaFeedGate` owned by `frogdb-core`'s client registry
(`client_registry/mod.rs:546`) — are `Option<T>` filled by the caller. Every invariant declares
which view fields it needs and `check_hard` **skips** entries whose inputs are absent rather than
reporting a false violation. The honest cost, worth writing into the module docs: INV-FENCE-1 and
INV-ROLE-1 are checked less often than the rest. The rejected alternative (a trait implemented by
the server so the view is always complete) puts the catalog behind a dyn boundary the mutation
gate cannot follow.

Vocabulary comes from issue 01's `frogdb-types`: `Tier::{Hard, DocumentedException(Citation)}`
with the citation as a variant field, `Citation` built only through the asserting `const fn`s, so
a citation-less exception in the `static CATALOG` is a build error. Two tiers, no third.

Seed the catalog with all sixteen §3 W1 entries — INV-REPLID-1/2/3, INV-OFFSET-1/2/3/4,
INV-BACKLOG-1/2/3, INV-SESSION-1/2/3, INV-GATE-1, INV-FENCE-1, INV-ROLE-1 — each carrying the
defect class it would have caught, so no entry is decorative.

**Hooks** are `debug_assert_view_clean(&view, "<seam>")` under `#[cfg(any(test, debug_assertions))]`
at the ten seams named in §3 W1:

| seam | file:line |
|---|---|
| `PrimaryReplicationHandler::begin_primary_stint` | `primary/mod.rs:389` |
| `PrimaryReplicationHandler::end_primary_stint` | `primary/mod.rs:448` |
| `ReplicationState::{new_replication_id, shift_replication_id, clear_secondary_window, adopt_replication_history, apply_staged_metadata}` | `state.rs:318/339/354/365/420` |
| `OffsetCoordinator::{advance, settle_at_applied, ingest_replica_ack, seed_replica_position}` | `offset_coordinator.rs:108/160/197/210` |
| `ReplicaOffset::{frame_advance, reset_to}` / `AppliedOffset::{advance_by, freeze, retire_replica_applies}` | `replica/offset.rs:482/514/128/302/215` |
| `ReplicationRingBuffer::{push, arm_start, reset}` | `primary/ring_buffer.rs:169/111/128` |
| `ReplicaSession::set_phase` | `replica_session.rs:591` |
| `ReplicationTrackerImpl::{register_announced_replica, unregister_replica, record_streaming_departure}` | `tracker.rs:183/200/215` |
| `ReplicaFeedGate::publish` | `feed_gate.rs:75` |
| `ReplicaConnection::set_state` | `replica/connection.rs:159` |

Each seam's owning component builds the widest view it can reach; the whole-node view is
assembled only where everything is visible (`begin_primary_stint` and, later, the DEBUG command).
Cluster's transition match was refactored into a private `apply_to` so arms that `return Err(..)`
early could not skip the hook — the replication analogue is that **every hooked seam must have a
single exit**, and straightening the ones that have early returns today is part of this work, not
a side quest.

Expect defects at catalog-build time; the known candidates are already written down: spec GAP-5
is INV-SESSION-2, and the chained-replication non-guarantee is INV-ROLE-1's likely
`DocumentedException`. Per §7, individual defects the catalog finds get filed as their own issues
rather than fixed here.

`frogdb-replication` is LOCKED (gate 0.85): spec-first against
`.scratch/hardening/specs/replication-failure-modes.md`, and `just mutants-diff` before push.

## Acceptance criteria

- [ ] `ReplicationView` + catalog in `frogdb-replication` with all sixteen seed invariants; each
      declares its required view fields and is skipped — not failed — when they are absent
- [ ] `Violation`/`Citation`/`Tier` come from `frogdb-types`; every `DocumentedException` cites an
      FM row or issue, compile-enforced
- [ ] Hooks live at all ten seams above in test/debug builds, each seam single-exit, plus a
      `#[should_panic]` hook-forcing test per seam so a deleted hook goes red
- [ ] One forcing test per HARD invariant, constructing the violating view directly and asserting
      the reported id, so the catalog is not dead code to the mutation gate
- [ ] Full suite green under the hooks (the ~391 in-crate tests plus the 131 in
      `integration_replication.rs`), or every violation triaged into a fix or a cited exception —
      no third bucket (exit criterion 2)
- [ ] `just mutants-diff frogdb-replication` and `just lint-failure-modes` triaged before push

## Blocked by

- Issue 01 (`.scratch/replication-correctness/issues/`) — the `Violation`/`Citation`/`Tier`
  vocabulary has to be in `frogdb-types` before this catalog can use it, since
  `frogdb-replication` must not depend on `frogdb-cluster`.
