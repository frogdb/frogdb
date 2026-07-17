# Proposal: The Offset Has One Owner

Status: proposed
Date: 2026-07-16

## Problem

Proposal 18 (implemented, `ec025b14`…`944e8882`) unified offset **reads** behind
`OffsetCoordinator`: callers ask `offsets.current()` / `can_serve_partial_sync(..)` instead of
knowing which of three fields is authoritative. That was the right first cut, and it stuck — WAIT,
INFO, PSYNC, and FULLRESYNC now read one accessor. But 18 unified *reads*, not *ownership*. The
offset still has three owners for its three lifecycle events — who holds the atomic, who advances
it, who confirms it — and the coordinator sits *beside* those owners rather than *being* them. It
is a read facade in front of the tracker, not the module that owns the quantity.

The symptom is that the coordinator is half-shallow: its interface is nearly as large as the
tracker calls it forwards. Four of its seven methods are one-line pass-throughs to the tracker;
the atomic it is supposed to own lives on the tracker and is handed out to a *third* holder (the
cluster bus) behind the coordinator's back.

| Symptom | Where (current) |
|---------|-----------------|
| The live atomic is owned by the tracker, not the coordinator; the coordinator borrows the tracker | `tracker.rs:32` (`current_offset: Arc<AtomicU64>`); `offset_coordinator.rs:46` (borrows `tracker`) |
| A *third* holder: the tracker clones the atomic out to the cluster bus for HealthProbe | `tracker.rs:66-68` (`shared_offset`) → `server/replication_init.rs:69` → `server/subsystems.rs:253` |
| 4 of 7 coordinator methods are 1-line delegations to the tracker | `advance_broadcast`→`increment_offset` (`offset_coordinator.rs:68-70`); `current`→`current_offset` (74-76); `record_replica_ack`→`record_ack` (79-81); `min_acked`→`min_acked_offset` (84-86) |
| The advance *unit* is defined once but applied through two different gates | `frame_advance` (`offset_coordinator.rs:60-63`) used by the replica only (`replica/streaming.rs:37`); primary advances by a raw `resp_bytes.len()` (`primary/mod.rs:235-236`, `259`) that bypasses the unit definition |
| `record_ack` is overloaded: it both *seeds* a resumed replica's start position and *ingests* genuine durability ACKs — both notify WAIT waiters | seed `replica_session.rs:647`; genuine ingest `replica_session.rs:669-670`; both → `tracker.rs:240` |

Three consequences, in increasing severity:

1. **The atomic has no single owner, so it leaks.** 18's doc-comment says the coordinator "owns the
   live offset," but the `Arc<AtomicU64>` lives on `ReplicationTrackerImpl` (`tracker.rs:32`) and is
   *cloned out to the cluster bus* by `tracker.shared_offset()` (`tracker.rs:66-68`,
   `replication_init.rs:69`). So the write position has three concurrent holders — the tracker, the
   coordinator (transitively), and the cluster-bus HealthProbe path — and the coordinator is not
   even the one that vends the shared handle. "Single owner" is a comment, not a structure.

2. **The advance unit is shared by convention, not by a gate.** 18 fixed the *bug* (both ends count
   RESP payload bytes, not framing) and put the definition in one function, `frame_advance`
   (`offset_coordinator.rs:60-63`). But only the replica routes through it (`streaming.rs:37`). The
   primary advances by a raw `resp_bytes.len() as u64` handed to `advance_broadcast(payload_len:
   u64)` (`primary/mod.rs:235-236` in `broadcast_tagged`, and again in `request_acks` at
   `primary/mod.rs:259`) — *before* the frame exists, so it cannot call `frame_advance`. Nothing in
   the type system says the `u64` the primary passes is the same quantity `frame_advance` would
   compute from the payload that becomes the frame. The two ends agree because a comment
   (`primary/mod.rs:256-258`) says they must; a divergence is caught only by the offset-parity unit
   test, not by construction.

3. **`record_ack` conflates two different facts.** It is called to *seed* a resumed replica's
   position after backlog replay (`replica_session.rs:647`, "so WAIT waiters and the lag monitor
   start from the resumed position") and to *ingest* a genuine `REPLCONF ACK` confirming durability
   (`replica_session.rs:669-670`). Both land in the same session store (`tracker.rs:240-253`) and
   both fire the WAIT ack-notification channel. The monotonic guard makes it *safe*, but "the
   primary streamed up to here" and "the replica confirmed it has this on disk" are different
   claims, and WAIT — which exists precisely to count durability confirmations — cannot tell them
   apart at the API.

**Deletion test (partial today).** If the coordinator already owned the offset, deleting it would
delete the tracker's `current_offset` field and `shared_offset`/`increment_offset`/`set_offset`
accessors, and the raw `resp_bytes.len()` advance at the primary. It cannot: those still stand on
their own, the coordinator's four delegations reappear one call away, and the pass-through layer
adds a hop without absorbing anything. That is the signature of a facade that unified the interface
but not the module behind it.

This proposal owns the **offset lifecycle**: the atomic's home, the single advance gate, and the
ack-vs-seed split. It deepens 18's seam; it does not re-litigate the read unification (that stays)
or the increment-unit fix (done). Scope note: proposal 57 (parallel) cleans up the session /
streaming code *around* the seed call site; 58 owns the *ack API shape* it calls into. The two are
compatible — 57 = session plumbing, 58 = offset + ack ownership.

## Design

Make `OffsetCoordinator` the module that *is* the offset, not the one that reads it. Three moves:
own the atomic, gate the advance, split the ack.

```rust
pub struct OffsetCoordinator {
    /// Live write position. OWNED here now — the tracker no longer holds it.
    /// The cluster bus receives its HealthProbe handle from `shared_offset()`
    /// below, so this type is the single vendor of the shared atomic.
    live: Arc<AtomicU64>,
    /// Per-replica acked offsets + the WAIT ack-notification channel. Still the
    /// tracker's session registry; the coordinator borrows it for aggregation
    /// and ack ingestion only (that half of 18's seam was already right).
    tracker: Arc<ReplicationTrackerImpl>,
    /// Persisted identity + offset, reconciled at save points.
    state: Arc<RwLock<ReplicationState>>,
}

impl OffsetCoordinator {
    /// The ONE advance gate. Both ends call this with the RESP payload that is
    /// (primary) about to become a frame or (replica) just arrived as one, so
    /// the unit is defined exactly once and applied identically. `frame_advance`
    /// becomes the replica-side spelling of the same computation.
    pub fn advance(&self, payload: &Bytes) -> u64 {
        let n = payload.len() as u64;
        self.live.fetch_add(n, Ordering::Release) + n
    }

    /// Replica-side convenience: advance from a decoded frame's payload. Defined
    /// in terms of `advance` so the two ends cannot use different units.
    pub fn advance_frame(&self, frame: &ReplicationFrame) -> u64 {
        self.advance(&frame.payload)
    }

    pub fn current(&self) -> u64 { self.live.load(Ordering::Acquire) }

    /// The cluster bus's HealthProbe handle now comes from the OWNER, not the
    /// tracker. `tracker.shared_offset()` is deleted; the atomic is vended here.
    pub fn shared_offset(&self) -> Arc<AtomicU64> { self.live.clone() }

    /// Genuine durability ACK from a replica's `REPLCONF ACK`. Notifies WAIT.
    pub fn ingest_replica_ack(&self, replica_id: u64, acked: u64) {
        self.tracker.record_ack(replica_id, acked);
    }

    /// Seed a resumed replica's *stream position* after backlog replay. Same
    /// monotonic session store, but a distinct verb: this is primary-side
    /// bookkeeping, not a durability confirmation. (Today it still notifies WAIT,
    /// matching current behaviour; the split is what lets that ever change.)
    pub fn seed_replica_position(&self, replica_id: u64, position: u64) {
        self.tracker.record_ack(replica_id, position);
    }

    pub fn min_acked(&self) -> Option<u64> { self.tracker.min_acked_offset() }

    pub async fn can_serve_partial_sync(&self, id: &str, offset: u64) -> bool { /* 18, unchanged */ }
    pub async fn reconcile_for_persist(&self) -> ReplicationState { /* 18, unchanged */ }
}
```

The tracker loses `current_offset` ownership: `ReplicationTrackerImpl::new` no longer allocates the
atomic; it receives a handle (or the offset fields — `increment_offset`, `current_offset`,
`set_offset`, `shared_offset` — move out entirely and the tracker keeps only the session registry +
ack channel + lag bookkeeping). `replica_lag` (`tracker.rs:169-175`) reads `self.current_offset()`
today; it takes the coordinator's `current()` instead, or the lag calc moves to the coordinator
where both operands (live, acked) then live together.

The primary's two advance sites route through the gate: `broadcast_tagged`
(`primary/mod.rs:233-236`) and `request_acks` (`primary/mod.rs:259`) call
`self.offsets.advance(&resp_bytes)` and stamp the returned offset into the frame — same values as
today, but the unit is no longer a raw `.len()` a caller could get wrong. The replica's
`streaming.rs:37` calls `advance_frame(&frame)`. `frame_advance` (the free-standing unit) collapses
into `advance_frame`.

### The seed / ingest split at the call sites

`replica_session.rs:647` becomes `handler.offsets.seed_replica_position(self.id, resume_offset)`;
`replica_session.rs:669-670` becomes `read_offsets.ingest_replica_ack(read_replica_id, ack_offset)`.
Both still flow to the same monotonic session store and the same WAIT notification, so **behaviour
is identical today** — but the two facts now have two names, and a future change ("seeding must not
satisfy a WAIT that demands fsync-confirmed durability") is a one-method edit, not a spelunking
expedition through a single overloaded `record_ack`.

## Why this is the right depth

- **Locality.** After this, exactly one type holds the atomic, defines the advance unit, and vends
  the cluster-bus handle. "Who owns the write position" stops being a three-way answer (tracker /
  coordinator / bus) and becomes one. Changing the offset's storage (per-shard vector, a different
  atomic ordering, a durability gate) is a one-module edit; today it is a hunt across `tracker.rs`,
  `primary/mod.rs`, and `replication_init.rs` that silently desyncs if you miss the bus clone.
- **Leverage.** The single `advance` gate makes primary/replica unit agreement a *property of one
  function* rather than of two call sites plus a comment (`primary/mod.rs:256-258`). The ack split
  makes "durability vs bookkeeping" expressible without new machinery. Neither needs a new layer —
  both *remove* a delegation by making the coordinator the thing, not a wrapper of the thing.
- **Deletion test (now passes).** The migration deletes whole artifacts: the tracker's
  `current_offset` field, its `increment_offset` / `set_offset` / `shared_offset` accessors, the
  free `frame_advance`, and the raw `resp_bytes.len()` advance at the primary. The four
  pass-through methods stop being pass-throughs — `advance`, `shared_offset`, and the two ack verbs
  now own state the tracker no longer has, so they cannot be inlined away at the caller. That is
  the signal the module is finally in the right place.
- **Not a new adapter.** This is the same seam 18 introduced, pushed one layer deeper: the
  coordinator absorbs the atomic and the advance gate instead of forwarding to them. The raw
  `tracker.increment_offset` / `tracker.current_offset` / `tracker.shared_offset` reads are
  *removed*, so the seam is a gate, not a wrapper callers can slip past.

## Testing impact

- **Ownership, no live cluster.** With the atomic owned by the coordinator, `advance` /
  `advance_frame` / `current` / `shared_offset` are unit-testable directly (they already are — the
  18 tests in `offset_coordinator.rs:112-236` move over unchanged; add one asserting
  `advance(&payload)` and `advance_frame(&frame_of(payload))` return the same value, pinning the
  single unit).
- **Cluster-bus handle parity.** A test that `coordinator.shared_offset()` and a HealthProbe read
  observe the same value after an `advance` — the regression guard for "the bus reads a stale or
  separate atomic" now that the coordinator, not the tracker, vends the handle.
- **Seed vs ingest.** Two named tests replacing the overloaded `record_ack` coverage: a seeded
  position and a genuine ACK both advance the session store monotonically and both notify a waiting
  WAIT (current behaviour, pinned), so the split is proven behaviour-preserving.
- **WAIT non-regression (proposal 39).** `wait_coordinator.rs`'s suite must stay green unchanged:
  the wait path reads `offsets.current()` and consumes the same ack channel; the ack split keeps
  `ingest_replica_ack` firing that channel, and `seed_replica_position` keeps firing it too.
- **Offset-parity property test (18) still holds.** Driving N commands through the primary gate and
  the replica gate must keep the two offsets equal after every frame — now guaranteed by the shared
  `advance`, not by two `.len()` sites agreeing.

## Risks / open questions

- **The atomic's construction order inverts.** Today `ReplicationTrackerImpl::new` allocates the
  atomic and `OffsetCoordinator::new` borrows the tracker (`primary/mod.rs:117`). After this the
  coordinator (or a small shared owner) allocates the atomic and the tracker receives a handle.
  Construction in `primary/mod.rs:104-133` and `replication_init.rs:45-193` must be re-threaded so
  the bus's `shared_replication_offset` (`replication_init.rs:69`, `subsystems.rs:253`) is sourced
  from the coordinator. Mechanical, but it touches server wiring, not just the replication crate.
- **The replica also holds a `shared_offset` handle.** On a *replica*, `set_shared_offset`
  (`replica/mod.rs:104-105`) points the ingest path's atomic store at the bus handle
  (`streaming.rs:40`, `replica/connection.rs:154,326`). That path has no `OffsetCoordinator` (it is
  primary-only), so the replica keeps its raw `Arc<AtomicU64>` wiring; the "coordinator owns the
  atomic" claim is a *primary-side* invariant. Worth stating explicitly so a future reader does not
  expect a coordinator on the replica.
- **Does seeding belong on the coordinator at all?** The seed (`replica_session.rs:647`) is arguably
  a *session* concern that happens to write the ack store. Putting `seed_replica_position` on the
  coordinator keeps the ack store single-owned, but proposal 57 owns the session cleanup around it —
  coordinate so the verb lands in one place, not both. Recommendation: the coordinator owns the
  *verb* (API shape), 57 owns the *call site* (when/where it fires).
- **`replica_lag` operand move.** If the atomic leaves the tracker, `tracker.replica_lag`
  (`tracker.rs:169-175`) can no longer read `self.current_offset()`. Cleanest is to move the lag
  calc onto the coordinator (both operands live there); the alternative — passing `current` in — is
  the exact shallow-seam tax 18 removed for `can_partial_sync`, so it should be rejected for the
  same reason.
- **Design choice — one gate, not fewer layers.** The pure-delegation methods could be *deleted*
  (let callers hit the tracker), which removes a layer but re-exposes the tracker and forfeits the
  seam. This proposal recommends the opposite: keep the methods but make them the *only* path by
  moving the state they gate under the coordinator, so they stop being delegations. The goal is one
  gate, not a thinner wrapper.
</content>
</invoke>
