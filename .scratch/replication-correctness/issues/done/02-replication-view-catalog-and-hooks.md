# 02 — `ReplicationView` + invariant catalog + seam hooks

Status: done

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

- [x] `ReplicationView` + catalog in `frogdb-replication` with all sixteen seed invariants; each
      declares its required view fields and is skipped — not failed — when they are absent
- [x] `Violation`/`Citation`/`Tier` come from `frogdb-types`; every `DocumentedException` cites an
      FM row or issue, compile-enforced
- [x] Hooks live at all ten seams above in test/debug builds, each seam single-exit, plus a
      `#[should_panic]` hook-forcing test per seam so a deleted hook goes red
- [x] One forcing test per HARD invariant, constructing the violating view directly and asserting
      the reported id, so the catalog is not dead code to the mutation gate
- [x] Full suite green under the hooks (the ~391 in-crate tests plus the 131 in
      `integration_replication.rs`), or every violation triaged into a fix or a cited exception —
      no third bucket (exit criterion 2)
- [x] `just mutants-diff frogdb-replication` and `just lint-failure-modes` triaged before push

## Resolution (2026-08-10)

Two new modules in `frogdb-replication`: `view.rs` (the projection, its three transition
witnesses and the `ViewField` enum) and `invariants.rs` (the sixteen entries, `check_hard`,
`check_all` and the `debug_assert_view_clean` hook). Vocabulary is issue 01's
`frogdb_types::catalog`, imported and re-exported, never redefined.

**Skipping, not failing.** Every entry declares `requires: &[ViewField]`; `check_catalog` skips
entries whose inputs are absent. Each seam builds the widest view it can reach, so a session-only
seam is not read as claiming the offsets are zero. `INV-FENCE-1` and `INV-ROLE-1` are the honest
cost — only a caller holding the quorum checker or the role manager fills their inputs — written
into the `view` module docs rather than left to be rediscovered.

**Two documented exceptions, each citing its ruling** (asserted as a whole list by
`every_documented_exception_names_its_ruling`, so a third one cannot appear unnoticed):

- `INV-ROLE-1` → `.scratch/testing-improvements/issues/done/48-chained-replication-contract.md`
  (the chained-replication non-guarantee, as the issue predicted).
- `INV-OFFSET-2` → issue 16 (below). Not predicted: the catalog's first run found it.

**Nested seams.** The rule the crate already applied to `shift_replication_id` now holds
everywhere: a hooked seam that calls another hooked seam takes an unhooked `*_inner`, so each
hook is uniquely forcible and names the right seam. That took one new split this round —
`OffsetCoordinator::settle_at_applied_inner`, called by `begin_primary_stint`, which is the only
place a whole-node view (and therefore `INV-REPLID-2`) is checkable at all.

**Three seams carry a hook but no forcing test**, documented at the tests rather than left
unexplained: `ReplicationRingBuffer::reset` and `ReplicaFeedGate::publish` leave a state every
entry accepts by construction, and `PartialSyncReplay::handle_partial_sync_request` only ever
sees a grant whose two bounds were proven on the way in. Each note says what future change the
hook would catch.

### Defects the catalog found

Filed, not fixed here (§7), each pinned by a muzzled `#[ignore]` witness so the fix has a test
waiting:

- **Issue 16** (`issues/open/16-…`) — `offset_at_save` can sit above the live head: both
  reconcile paths raise it with a `max` and `reset_to` never lowers it, so a node that follows a
  shorter history keeps the higher save point on disk, re-seeds `live` from it on restart, and
  arms a failover window above data it does not hold. An existing test asserts today's behaviour
  deliberately, which makes this a ruling rather than a bug — hence the exception tier.
  Witness: `save_point_follows_a_backwards_full_resync` (`replica/offset.rs`).
- **Issue 17** (`issues/open/17-…`) — the three replica-side wire paths (`psync`,
  `receive_snapshot`, `receive_checkpoint`) adopt a peer-supplied replication id without
  validating it, while the disk path validates. A malformed id is unmatchable, so it costs
  permanent full resyncs and can make the node unbootable through `validate()`.
  Witness: `a_continue_carrying_a_malformed_id_is_refused` (`replica/connection.rs`).
  Fixture ids that were not 40-hex are now real ones, which is how the defect surfaced.

`INV-OFFSET-4` was narrowed at the code instead of filed: a live head of 0 is a full resync in
flight, where the window deliberately stands over the keyspace this node still holds until the
payload lands (FM-REPLICATION-001). `INV-SESSION-2` (spec GAP-5) stays HARD — nothing in either
suite trips it.

### Evidence

- `just test frogdb-replication`: 462/462, 2 skipped (the two muzzled witnesses).
- `just test frogdb-server`: 2001/2002 with zero invariant violations; the one failure
  (`integration_pubsub::test_ssubscribe_client_receives_sunsubscribe_on_slot_migration`) is
  unrelated to replication and passes in isolation under the hooks.
- `just lint` (workspace clippy, `--all-targets`) and `just lint-failure-modes` green.
- `just mutants-diff frogdb-replication`: 212 mutants over the diff, 4 missed on the first
  pass, all four in the catalog itself. Three were legal boundary states nothing stood on — a
  backlog floor exactly on the oldest entry's end, a feed-gate hold exactly at the barrier
  budget, and `ReplicationView::empty` collapsing into the derived `Default` — and each now has
  a test or, for `empty`, one constructor instead of two. The fourth (`< 0` → `<= 0` on
  `INV-OFFSET-4`'s no-window sentinel) is unobservable: a window frozen at exactly 0 is caught
  by the clause after it, so no test can tell the two apart. Documented at the code. Re-run:
  212 mutants, 160 caught, 51 unviable, 1 missed — that one.

The new `INV-*` ids are deliberately **not** cited in
`.scratch/hardening/specs/replication-failure-modes.md` yet: `lint-failure-modes` resolves
invariant citations against the cluster catalog only, so spec integration waits for issue 14.

## Blocked by

- Issue 01 (`.scratch/replication-correctness/issues/`) — the `Violation`/`Citation`/`Tier`
  vocabulary has to be in `frogdb-types` before this catalog can use it, since
  `frogdb-replication` must not depend on `frogdb-cluster`.
