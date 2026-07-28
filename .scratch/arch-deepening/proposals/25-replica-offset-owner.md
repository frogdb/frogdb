# Proposal 25 — Give the replica-side offset lifecycle an owner (`ReplicaOffset`), symmetric in role to `OffsetCoordinator`

## Summary

The primary side of replication has a single owner of the offset contract —
`OffsetCoordinator` (`replication/src/offset_coordinator.rs`), whose one `advance` gate
mutates the canonical offset atomic and whose `shared_offset()` vends the one handle the
cluster-bus **HealthProbe** reads. The **Replica** side has no such owner. The invariant
"`ReplicationState::replication_offset` and the mirror atomic (`shared_offset`) move
together" is re-implemented by hand at **three** ingest sites — streaming
(`replica/streaming.rs:37-40`), FULLRESYNC (`replica/connection.rs:164-170`), and checkpoint
(`replica/connection.rs:309-316`) — each of which independently writes the offset and then
remembers to `shared.store(...)`. **All three current sites do this correctly; there is no
live bug.** The concern is preventive: the invariant is compiler-invisible, so a *future*
ingest path that forgets the second half would silently desync the HealthProbe — and the
**failure detector** scores replicas for automatic failover off exactly that HealthProbe
value, so a stale mirror would mean a data-loss-suboptimal replica gets promoted, with no
compiler complaint and no unit test to catch the regression (the invariant is exercised today
only through booted `integration_replication` tests). This proposal is failover-safety
*testability* work — making a currently-correct-but-fragile invariant socket-free
unit-testable and single-homed — not a bugfix.

This proposal introduces a `ReplicaOffset` owner in the `replication` crate that holds the
canonical offset home and the optional mirror behind two verbs — `advance(frame)` (streaming)
and `reset_to(id, offset)` (full-sync / checkpoint) — so the offset↔mirror coupling has a
single home, the mirror can never reflect a half-updated sync point, and the whole lifecycle
becomes lockstep unit-testable without a socket.

## Evidence (verified file:line)

Every cited site was opened and confirmed.

| Site | What it does | Verified |
| --- | --- | --- |
| `replication/src/offset_coordinator.rs:48-98` | `OffsetCoordinator` owns the canonical `live: Arc<AtomicU64>`; one `advance(payload)` gate; `frame_advance(frame)` is the shared advance *unit* | Confirmed |
| `replication/src/offset_coordinator.rs:120-122` | `shared_offset()` vends the HealthProbe handle from the owner | Confirmed |
| `replication/src/replica/streaming.rs:37-40` | streaming ingest: `state.increment_offset(OffsetCoordinator::frame_advance(&frame))` → read `state.replication_offset` → `shared.store(offset, Release)` | Confirmed |
| `replication/src/replica/connection.rs:164-170` | FULLRESYNC: write lock, set `replication_id` + `replication_offset = new_offset`, drop, then `shared.store(new_offset, Release)` | Confirmed |
| `replication/src/replica/connection.rs:309-316` | checkpoint: write lock, set `replication_id` + `replication_offset = metadata.replication_offset`, then `shared.store(..., Release)` | Confirmed |
| `replication/src/state.rs:290-294` | `increment_offset(bytes)` — `saturating_add`, the only mutation helper (candidate cited `:292`, the body line) | Confirmed |
| `replication/src/replica/mod.rs:58,124-134,195` | handler holds `shared_offset: Option<Arc<AtomicU64>>`; `set_shared_offset`/`shared_offset()`; cloned into each `ReplicaConnection` in `connect_and_sync` | Confirmed |
| `replication/src/replica/connection.rs:78-89` | `ReplicaConnection` carries `state: Arc<RwLock<ReplicationState>>` and `shared_offset: Option<Arc<AtomicU64>>` as two independent fields | Confirmed |
| `server/src/cluster_bus.rs:162-165` | `HealthProbe` answers with `ctx.replication_offset.load(Acquire)` — the mirror atomic | Confirmed |
| `server/src/failure_detector.rs:329-374` | `trigger_auto_failover` queries each replica's offset via `health_probe()` and scores for least data loss; an unreachable/stale replica is treated as worst | Confirmed |
| `replication/src/apply.rs:101-218` | the frame *consumer* applies commands to shards but **never** touches the offset — the offset advance is decoupled from apply and lives entirely at the three ingest sites | Confirmed |
| `replication/src/replica/tests.rs` (4 tests) | cover connection-state Display, handler creation, and two stop/reconnect-loop cases (`test_connection_state_display`, `test_replica_handler_creation`, `test_stop_terminates_reconnect_loop_without_abort`, `test_stop_before_start_prevents_any_connection_attempt`) — **nothing** asserts the offset↔mirror invariant | Confirmed |

### Evidence discrepancies found (candidate claims corrected)

1. **"owns state *atomic*" is not accurate — the replica has no offset atomic.** The
   canonical offset home on the replica is the plain `u64` field
   `ReplicationState::replication_offset`, guarded by `Arc<RwLock<ReplicationState>>`. The
   only atomic on the replica side is the `shared_offset` **mirror**. This is a genuine
   structural asymmetry with the primary, where the *canonical* home is the atomic
   (`live`) and everything else borrows a clone. So `ReplicaOffset` is symmetric to
   `OffsetCoordinator` in **role** (single owner of the offset lifecycle) but **not** in
   mechanism (canonical `RwLock` field + mirror atomic, vs canonical atomic + borrowed
   clones). The proposal is written to that corrected shape.

2. **`frame_advance` is deliberately shared, not accidentally "primary-only."** The
   candidate frames the replica's use of `OffsetCoordinator::frame_advance` as a smell of
   reaching into a primary type. Verified: `frame_advance` is a `pub` associated fn
   documented (`offset_coordinator.rs:99-109`) precisely so "the coordinator-less replica
   ingest path can call it" and so "the two ends cannot use different units." The seam
   half-exists on purpose. The remaining smell is only that the *unit* is namespaced under
   the primary-named type; this proposal relocates it to a neutral home (below), which is a
   real cleanup but a smaller one than "the replica borrows a primary type by accident."

3. **There is a fourth offset-set site, but it is correctly out of scope.**
   `ReplicationState::apply_staged_metadata` (`state.rs:312-315`) sets `replication_offset`
   during boot recovery, with **no** mirror store — and that is correct, because it runs
   before `shared_offset` is wired (there is no HealthProbe atomic yet at install time).
   `ReplicaOffset` should *not* absorb it; noting it here so the design scope is explicit
   and the "three sites" count is understood as "three *runtime* sites that must mirror."

The core premise holds. **Recommendation: proceed.**

## Proposed design (Rust interface sketch — signatures only)

A new deep module in the `replication` crate. It holds the two Arcs the ingest paths already
share and hides the offset↔mirror coupling behind two mutation verbs plus a read.

```rust
// replication/src/replica/offset.rs  (new)

use crate::frame::ReplicationFrame;
use crate::state::ReplicationState;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::RwLock;

/// Single owner of the *replica-side* offset lifecycle — symmetric in role to
/// the primary's `OffsetCoordinator`, though not in mechanism: here the
/// canonical offset home is `ReplicationState::replication_offset` (a `u64`
/// behind the shared `RwLock`) and `mirror` is the cluster-bus HealthProbe
/// atomic that must move in lockstep with it. Every runtime ingest path routes
/// its offset change through this type so the mirror can never drift from the
/// canonical field.
pub struct ReplicaOffset {
    state: Arc<RwLock<ReplicationState>>,
    mirror: Option<Arc<AtomicU64>>,
}

impl ReplicaOffset {
    /// Adopt the state handle and the optional HealthProbe mirror. The mirror is
    /// `None` outside cluster mode (no HealthProbe atomic exists).
    pub fn new(state: Arc<RwLock<ReplicationState>>, mirror: Option<Arc<AtomicU64>>) -> Self;

    /// Streaming ingest: advance the canonical offset by the frame's stream unit
    /// and publish the new value to the mirror in the same step. Returns the new
    /// offset so the caller can stamp an ACK / trace. Replaces
    /// `streaming.rs:37-40`.
    pub async fn advance(&self, frame: &ReplicationFrame) -> u64;

    /// Full-sync / checkpoint: adopt an authoritative `(replication_id, offset)`
    /// sync point and publish `offset` to the mirror, both under one lock so the
    /// mirror can never reflect a half-updated identity. Replaces the paired
    /// id+offset+mirror writes at `connection.rs:164-170` and `:309-316`.
    pub async fn reset_to(&self, replication_id: String, offset: u64);

    /// The current stream offset (for the spontaneous ACK tick and PSYNC
    /// resume). Reads the canonical field, never the mirror.
    pub async fn current(&self) -> u64;

    /// The mirror handle, for wiring / test assertions — mirrors
    /// `OffsetCoordinator::shared_offset` / the handler's `shared_offset()`.
    pub fn shared_offset(&self) -> Option<Arc<AtomicU64>>;
}
```

**Relocate the shared advance *unit* to a neutral home** so neither owner names the other's
type. Move the byte-count definition out from under the primary-named `OffsetCoordinator`
into an inherent method on the frame itself:

```rust
// replication/src/frame.rs
impl ReplicationFrame {
    /// The replication-offset advance unit: RESP payload bytes only, never the
    /// 20-byte transport header (`FRAME_HEADER_SIZE`: magic 4 + version 1 +
    /// flags 1 + shard 2 + sequence 8 + length 4). The one definition both ends
    /// count by.
    #[inline]
    pub fn stream_advance(&self) -> u64; // == self.payload.len() as u64
}
```

`OffsetCoordinator::advance` (primary) and `ReplicaOffset::advance` (replica) both consume
`ReplicationFrame::stream_advance` (or the primary keeps counting the pre-frame `payload`
via the same one-line unit), so the "two ends use one unit" invariant is preserved with the
unit no longer namespaced under a role. `OffsetCoordinator::frame_advance` becomes a thin
delegator or is deleted — at runtime it has exactly **one** caller (`streaming.rs:37`, the
replica); its other three references are unit tests in `offset_coordinator.rs`
(`:193,219,239`). The primary end does **not** call it — `OffsetCoordinator::advance` counts
`payload` directly (`offset_coordinator.rs:95`). So its live call graph is "the replica end
plus tests," not "the two ends plus tests."

While relocating the unit, **fix the two pre-existing stale "18-byte header" comments** that
this proposal's new doc supersedes: `replica/streaming.rs:33` and `offset_coordinator.rs:36`
both still say "18-byte," a figure stale since `FRAME_VERSION 2` added the 2-byte shard tag
(header is now 20 bytes, `frame.rs:84`). Correcting them in the same change stops a wrong
constant from surviving next to the corrected `stream_advance` doc.

**`ReplicaConnection` collapses its two offset fields into one owner.** The struct
(`connection.rs:78-89`) currently carries `state` and `shared_offset` separately; it instead
carries a single `offset: ReplicaOffset` built by the handler from the state handle + mirror
it already holds (`replica/mod.rs:189-197`). The three call sites become:

- streaming: `let offset = self.offset.advance(&frame).await;`
- FULLRESYNC: `self.offset.reset_to(new_repl_id.clone(), new_offset).await;`
- checkpoint: `self.offset.reset_to(metadata.replication_id.clone(), metadata.replication_offset).await;`

The `state` handle is still needed by `ReplicaConnection` for the non-offset reads/writes
(PSYNC decision at `connection.rs:139-149`, `active_version`, etc.), so it stays; only the
offset mutation and the mirror field move behind `ReplicaOffset`. `ReplicaOffset` and the
connection share the same `Arc<RwLock<ReplicationState>>`, so there is no second source of
truth.

## Migration plan (ordered steps)

1. **Relocate the advance unit first (no behavior change).** Add
   `ReplicationFrame::stream_advance`; point `OffsetCoordinator::advance` /
   `frame_advance` and the existing replica `increment_offset(OffsetCoordinator::frame_advance(&frame))`
   at it. Keep `frame_advance` as a delegator this step to avoid churn. Correct the two stale
   "18-byte header" comments (`streaming.rs:33`, `offset_coordinator.rs:36`) to 20 bytes in
   the same step. Run the existing `offset_coordinator` unit tests (they pin the
   payload-not-header unit at `offset_coordinator.rs:192-240`) green.
2. **Add `ReplicaOffset`** (`replica/offset.rs`) with `advance` / `reset_to` / `current` /
   `shared_offset` and its own unit tests (see Test plan). No call sites use it yet.
3. **Route the three ingest sites through it.** Replace `streaming.rs:37-40` with
   `advance`, and both `connection.rs` blocks with `reset_to`. Build `ReplicaOffset` in
   `ReplicaReplicationHandler::connect_and_sync` (`replica/mod.rs:186-217`) from
   `self.state.clone()` + `self.shared_offset.clone()`; store it on `ReplicaConnection` in
   place of the separate `shared_offset` field.
4. **Delete `OffsetCoordinator::frame_advance`** once no caller remains (compiler-driven);
   or keep it as a one-line delegator if the primary path reads cleaner with it.
5. **Confirm the boot-recovery path is untouched.** `apply_staged_metadata`
   (`state.rs:312-315`) stays as-is: it runs pre-wiring and must not mirror.
6. `just check frogdb-server` / `just test frogdb-server` locally per crate; whole-suite +
   clippy on the Blacksmith testbox.

Each step compiles and is independently testable; the server crate is not touched (the
mirror atomic is still minted in `cluster_init.rs` and handed in via `set_shared_offset`).

## Test plan

The whole point is that the offset↔mirror invariant becomes socket-free unit-testable,
mirroring the primary's existing `offset_coordinator` tests.

- **`advance` mirrors in lockstep.** Build `ReplicaOffset::new(state, Some(mirror))`; call
  `advance(frame_of(payload))`; assert `current().await == payload.len()` **and**
  `mirror.load(Acquire) == current().await`. Repeat to assert cumulative + monotone.
- **`advance` counts payload, not header.** Assert the advance equals `payload.len()` and
  *not* `frame.encoded_size()` — the same guard as
  `offset_coordinator.rs:frame_advance_counts_payload_not_header`, now on the replica owner.
- **`reset_to` sets id + offset + mirror atomically.** After `reset_to(id, 4242)`, assert
  the state's `replication_id == id`, `replication_offset == 4242`, and `mirror == 4242` —
  the FULLRESYNC/checkpoint contract, previously only reachable through a booted replica.
- **`reset_to` can move backward; `advance` cannot underflow.** A fresh full sync legitimately
  resets to a lower offset (unlike the primary's monotone reconcile); pin that `reset_to`
  does not guard monotonicity while `advance` uses `saturating_add`.
- **`None` mirror is a no-op mirror.** With `mirror: None` (non-cluster mode), `advance` /
  `reset_to` still update the canonical field and simply skip the store — matching today's
  `if let Some(ref shared)` guards.
- **Regression guard for the failure detector.** A dedicated test that a sequence of
  `advance` calls leaves `shared_offset()` exactly equal to `current()` — the value
  `HealthProbe` returns and `trigger_auto_failover` scores on. This is the invariant whose
  hand-maintenance the proposal removes.
- Keep the booted `integration_replication` offset tests as end-to-end coverage; they no
  longer carry the *whole* burden of the invariant.

## Risks & alternatives

- **`advance` holds the write lock internally; today the mirror store happens after
  `drop(state)`.** Moving the `mirror.store` inside (or immediately after) the same lock
  acquisition is behavior-preserving and arguably *more* correct (no window where a reader
  sees the field advanced but the mirror stale). The streaming hot path takes the write lock
  once per frame today already (`streaming.rs:32`), so there is no new contention. Verify no
  code depends on the field-updated-before-mirror ordering — none found.
- **`reset_to` merges id + offset under one lock; the sites currently do too.** Both
  `connection.rs:164-170` and `:309-316` already set id and offset under a single write lock
  before mirroring, so folding them into `reset_to(id, offset)` matches the existing locking
  shape — no new lock ordering. If a future sync path needs to set the offset *without*
  touching the id, add a narrower `reset_offset(offset)`; not needed today.
- **`reset_to` pulls `replication_id` into `ReplicaOffset`, creating temporary split
  ownership of the identity field.** The **+CONTINUE partial-sync** path
  (`connection.rs:202-209`) legitimately sets `replication_id` *alone* — no offset change, no
  mirror store — and so cannot route through `reset_to`; it stays hand-written. That means
  `replication_id` is mutated in two places (`ReplicaOffset::reset_to` and the CONTINUE site)
  until **proposal 26** takes ownership of the identity/sync-point lifecycle. This is a
  scope/ordering caveat, not a blocker: the **offset** (this proposal's actual concern) does
  become single-homed at all three runtime mirror sites; only the *identity* field remains
  split, which is exactly the seam 26 closes. An alternative — have `reset_to` take only the
  offset and leave *all* id writes hand-rolled until 26 — was considered but rejected: the
  FULLRESYNC/checkpoint sites set id+offset+mirror as one atomic sync point, and splitting
  that across an owned `reset_to(offset)` plus a hand-written id write would reintroduce the
  very half-updated-sync-point window this proposal removes. So `reset_to` folds id+offset
  now, and 26 unifies the residual CONTINUE-only id write.
- **Asymmetry with `OffsetCoordinator` is real and should be documented, not hidden.** The
  primary's canonical home is an atomic; the replica's is a `RwLock` field with an atomic
  mirror. `ReplicaOffset` deliberately does not pretend otherwise — its doc comment states
  the mechanism difference. Alternative considered and rejected: promote the replica's
  canonical offset to an `AtomicU64` too and make the field a derived view. That is a larger,
  riskier change (the offset is serialized as part of `ReplicationState` for persistence and
  read under the same lock as `replication_id`/`secondary_id` in `window_contains` /
  `save`), and it buys uniformity the failure-detector invariant does not need. Keep the
  `RwLock` field canonical.
- **Scope creep into the boot path.** `apply_staged_metadata` looks like a fourth site but
  must stay out — it runs before the mirror exists. The proposal explicitly excludes it; a
  reviewer tempted to "finish the job" by routing it through `ReplicaOffset` would introduce
  a `None`-mirror call at best and a panic at worst if they assumed a wired mirror.
- **ADR interaction.** None. This is entirely within the data/replication path;
  `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` (the data path never goes through
  the **Raft Metadata Plane**) is untouched — the HealthProbe offset is read by the
  **failure detector** over the cluster bus, not committed through Raft.
- **Crate dependency direction.** `ReplicaOffset` lives in `replication` and depends only on
  same-crate `ReplicationState` / `ReplicationFrame` and `std`/`tokio`. The `server` crate
  still owns and mints the mirror atomic and passes it in via the existing
  `set_shared_offset` seam — no new cross-crate coupling, no reversal.
- **Glossary alignment.** Uses canonical terms: **Primary/Replica**, **Raft Metadata
  Plane**, **Config Epoch**, **Checkpoint** (the on-disk snapshot artifact the checkpoint
  full-sync installs). The mirror is the cluster-bus **HealthProbe** offset; the failover
  scoring is the **failure detector**'s.

## Effort

**M.** One new type (~4 methods) plus tests, a one-line unit relocation into
`ReplicationFrame`, three call-site rewrites, and folding `ReplicaConnection`'s two offset
fields into one owner built in the handler. Confined to the `replication` crate; the `server`
crate's HealthProbe/mirror wiring is unchanged, so no cross-crate signature churn. Not S
because it touches the connection struct, the handler's `connect_and_sync`, and three ingest
paths and adds a type; not L because it is one crate, compiler-guided, with no protocol or
persistence-format change.

## Related

- **Proposal 26** (sibling, same root cause — the replica-side offset/identity lifecycle
  being maintained by hand rather than owned). 25 owns the *offset↔mirror* coupling; 26
  addresses the adjacent identity/sync-point handling. They share the `ReplicaOffset` seam
  and should land in sequence.
- **`OffsetCoordinator`** — the primary-side owner this proposal mirrors in role, introduced
  in the round-7 offset-contract work (proposal 57/58; see the module doc and the
  round-7 follow-up note at `offset_coordinator.rs:136-140`). 25 completes the symmetry by
  giving the replica end the owner the primary end already has.
- **Proposal 06 / 12** (snapshot scheduler/coordinator surface): same architectural move —
  push a hand-maintained invariant down into a single deep owner and make it unit-testable
  without the outer async/socket harness.

## Adversarial review

**Verdict: AMEND** — core premise confirmed against the real source
(`frogdb-server/crates/replication/`); five minor factual/precision fixes, none changing the
design. All resolved:

1. **20-vs-18-byte header constant (minor).** *Resolved.* The design sketch's new
   `stream_advance` doc said "18-byte transport header," which would re-cement a stale
   constant into fresh code. `FRAME_HEADER_SIZE = 20` (`frame.rs:84`: magic 4 + version 1 +
   flags 1 + shard 2 + sequence 8 + length 4; the 2-byte shard tag arrived with
   `FRAME_VERSION 2`). Corrected the new doc to 20 bytes with the field breakdown, and added a
   migration step to fix the two pre-existing stale comments (`streaming.rs:33`,
   `offset_coordinator.rs:36`) in the same change.

2. **tests.rs count 5→4 (minor).** *Resolved.* `replica/tests.rs` has four tests, not five;
   evidence row now lists all four by name. The load-bearing claim (none assert the
   offset↔mirror invariant) was already correct.

3. **Summary framed a hypothetical desync as a present bug (minor).** *Resolved.* Verified all
   three current ingest sites mirror correctly (`streaming.rs:40`, `connection.rs:168-170`,
   `:314-316`) — there is no live bug. Summary now states plainly that all three sites are
   correct today and that this is preventive failover-safety *testability* work, not a bugfix.

4. **"frame_advance's four call sites are the two ends plus tests" imprecise (minor).**
   *Resolved.* The primary calls `advance(&payload)` (`offset_coordinator.rs:95`), not
   `frame_advance`; at runtime `frame_advance` has exactly one caller (`streaming.rs:37`, the
   replica), the other three references being unit tests. Design section now says "the replica
   end plus tests" with the exact line references.

5. **`reset_to` pulls id in, temporarily splitting identity-field ownership (minor).**
   *Resolved.* The +CONTINUE partial-sync path (`connection.rs:202-209`) sets `replication_id`
   alone and cannot route through `reset_to`, so the identity field stays mutated in two places
   until proposal 26. Added an explicit risk bullet: the **offset** does become single-homed
   (the proposal's actual concern holds); only the *identity* field remains split, which is the
   seam 26 closes. Documented why folding id+offset into `reset_to` now (vs deferring all id
   writes) is still correct — an offset-only `reset_to` would reintroduce the half-updated
   sync-point window.

Reviewer notes recorded: no borrow-checker/deadlock issue (`advance(&self)`/`reset_to(&self)`
on a distinct field don't conflict with `&mut self` on the connection; the mirror store is an
atomic inside/after the single write lock the streaming path already takes; HealthProbe reads
only the atomic); crate direction is sound (`ReplicaOffset` lives in `replication`, server
still mints the mirror and passes it via `set_shared_offset`); `apply_staged_metadata`
(`state.rs:312`) correctly excluded as the pre-wiring boot path. Churn (M, one crate,
compiler-guided) justified by the failover-safety invariant becoming socket-free
unit-testable.
