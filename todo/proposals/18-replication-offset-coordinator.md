# Proposal: Replication Offset Coordinator

Status: proposed
Date: 2026-06-15

## Problem

The replication offset is a single logical quantity — "how many bytes of the command stream have
been produced/consumed" — but it lives in three loosely-coordinated homes, and every caller must
know which home to read for which question. That knowledge is not enforced by any type; it is
carried in *prose comments* scattered across the replication crate. A seam whose contract is "read
the doc-comment to learn which field is authoritative" is a shallow seam: the interface (what a
caller must know) is as large as the implementation it is supposed to hide.

The three homes, and the implicit rule binding them:

| Offset home | Backing field | Written by | Read by | file:line |
|---|---|---|---|---|
| Live primary write position | `ReplicationTrackerImpl.current_offset: Arc<AtomicU64>` | `broadcast_command` via `tracker.increment_offset` | WAIT, INFO, PSYNC window, FULLRESYNC reply, cluster HealthProbe | `tracker.rs:32,133-139`; written `primary/mod.rs:272`; read `blocking.rs:169`, `info.rs:437`, `primary/mod.rs:172,391` |
| Per-replica acked position | `ReplicaSession.acked_offset: AtomicU64` | `tracker.record_ack` ← replica's `REPLCONF ACK` | WAIT (`count_acked`), `min_acked_offset`, `replica_lag` | `replica_session.rs:171,209-211,252-259`; agg `tracker.rs:143-168` |
| Persisted / reconciled position | `ReplicationState.replication_offset: u64` | `save_state` (reconcile from tracker), load, `apply_staged_metadata`; on a *replica*, advanced live | PSYNC handshake (replica side), `can_partial_sync` window | `state.rs:109`; written `primary/mod.rs:139-151`, `state.rs:310-313`; replica live-advance `replica/streaming.rs:32`; read `connection.rs:127-133`, `state.rs:265-286` |

The implicit rule — **"read the tracker for the live window, never `state.replication_offset`"** — is
spelled out, in English, in at least four places because the type system will not say it:

- `primary/mod.rs:131-138` (`save_state`): "The durable offset is sourced from the tracker … not
  from `self.state`, because the broadcast path increments only the tracker's atomic."
- `primary/mod.rs:166-171` (`handle_psync`): "Source the live stream head from the tracker … not
  `state.replication_offset`, which only holds the offset persisted at the last load/reconcile."
- `primary/mod.rs:237-242` (`current_offset`): "Always report the tracker so this never returns a
  stale offset."
- `state.rs:251-259` (`can_partial_sync`): "`current_offset` … supplied by the caller rather than
  read from `self.replication_offset` … checking against it made every reconnect fall outside the
  window and forced a full resync."

That last comment is a post-mortem of a bug this fragmentation already caused: a caller read the
wrong home and partial sync could never be granted. The fix was to thread `current_offset` in as a
parameter — i.e. to make every caller responsible for fetching the right offset and passing it to
the check. That is the shallow-seam tax: the invariant ("compare against the live position") was
pushed *out* to callers instead of *into* one module.

There is also a redundant fourth writer: `PrimaryReplicationHandler::increment_offset`
(`primary/mod.rs:244-250`) updates *both* `state.replication_offset` and the tracker, but the live
broadcast path (`primary/mod.rs:272`) bypasses it and touches only the tracker. No caller invokes
the async `increment_offset` (verified: the only `.increment_offset(` sites are the tracker's, the
state's, and the replica's). It is dead code whose mere existence advertises that "increment the
offset" has no single owner.

**Deletion test.** If the offset contract were already a deep module, deleting that module would
delete: the four explanatory comments above (they would be unnecessary — the type would enforce the
rule), the dead `increment_offset` method, and the `current_offset` parameter threaded through
`can_partial_sync`. Today none of those can be deleted, because the contract lives in the callers,
not in a module. That is the signal that the seam is in the wrong place.

This proposal owns the **offset contract**: unifying the three homes behind one module
(`OffsetCoordinator`) and fixing the increment-unit correctness bug (below) so that "the offset" has
one meaning and one owner. The **backlog ring buffer + partial-sync REPLAY** mechanism is a
separate concern — see proposal 14, which builds the replay path *on top of* the contract defined
here. 18 makes `can_serve_partial_sync` a correct, single-owner primitive; 14 makes it return
`true`.

## Current state

### The live increment — primary counts RESP payload bytes (`primary/mod.rs:269-285`)

```rust
fn broadcast_command(&self, cmd_name: &str, args: &[Bytes]) -> u64 {
    let resp_bytes = serialize_command_to_resp(cmd_name, args);
    let bytes_len = resp_bytes.len() as u64;          // RESP payload only — no frame header
    let new_offset = self.tracker.increment_offset(bytes_len);
    if let Some(ref rb) = self.ring_buffer {
        rb.push(new_offset, resp_bytes.clone());
    }
    let frame = ReplicationFrame::new(new_offset, resp_bytes); // sequence field carries the offset
    self.broadcast_frame(frame);
    // ...
    new_offset
}
```

The tracker's increment is the only mutation of the live offset on the primary
(`tracker.rs:133-135`):

```rust
pub fn increment_offset(&self, bytes: u64) -> u64 {
    self.current_offset.fetch_add(bytes, Ordering::Release) + bytes
}
```

### The live increment — replica counts payload **plus the frame header** (`replica/streaming.rs:30-36`)

```rust
while let Some(frame) = codec.decode(&mut buf)? {
    let mut state = self.state.write().await;
    state.increment_offset(frame.encoded_size() as u64);  // FRAME_HEADER_SIZE + payload.len()
    let offset = state.replication_offset;
    drop(state);
    if let Some(ref shared) = self.shared_offset { shared.store(offset, Ordering::Release); }
    tracing::trace!(sequence = frame.sequence, offset = offset, "Received replication frame");
    if frame_tx.send(frame).await.is_err() { /* ... */ }
}
```

`encoded_size()` is defined as the header **plus** payload (`frame.rs:210-212`), and the header is a
fixed 18 bytes (`frame.rs:76`):

```rust
pub const FRAME_HEADER_SIZE: usize = 18; // 4 + 1 + 1 + 8 + 4

pub fn encoded_size(&self) -> usize {
    FRAME_HEADER_SIZE + self.payload.len()
}
```

So the primary advances by `payload`, the replica by `payload + 18`. The two ends of the stream do
**not** count the offset in the same unit. (Confirmed; see Correctness flags.)

### The per-replica ack (`replica_session.rs:252-259`)

```rust
pub fn record_ack(&self, sequence: u64) -> bool {
    let now = Instant::now();
    self.inner.write().last_ack_time = now;
    let prev = self.acked_offset.load(Ordering::Acquire);
    if sequence > prev {
        self.acked_offset.store(sequence, Ordering::Release);
        // ...
```

The replica sends its own (header-inflated) `state.replication_offset` in `REPLCONF ACK`
(`replica/streaming.rs:44-47, 53-60`); the primary stores it verbatim and later compares it against
its own (payload-only) live offset.

### The persisted read + the implicit "use the tracker" rule (`primary/mod.rs:172-177`, `state.rs:265-286`)

```rust
// primary/mod.rs — handle_psync
let current_offset = self.tracker.current_offset();   // MUST be the tracker, not state
let state = self.state.read().await;
let offset_in_window = !(replication_id == "?" && offset == -1)
    && offset >= 0
    && state.can_partial_sync(replication_id, offset as u64, current_offset);
```

```rust
// state.rs — the window check, which takes the live offset as a parameter
pub fn can_partial_sync(&self, requested_id: &str, requested_offset: u64, current_offset: u64) -> bool {
    if requested_id == self.replication_id && requested_offset <= current_offset {
        return true;
    }
    // secondary-id failover branch …
    false
}
```

The window check cannot read its own authoritative offset; the caller must fetch it from the right
home and pass it in. That is the contract leaking out of the module.

### Reconciliation (already solved in round 2 — keep it) (`primary/mod.rs:139-151`)

```rust
pub async fn save_state(&self) -> std::io::Result<()> {
    let offset = self.tracker.current_offset();
    let snapshot = {
        let mut state = self.state.write().await;
        if offset > state.replication_offset { state.replication_offset = offset; }
        state.clone()
    };
    snapshot.save(&self.state_path)
}
```

Round-2 work already made the *durability* direction correct: the persisted offset is reconciled
from the live tracker at save points, and the tracker is reseeded from the file on load, so the
reported `master_repl_offset` never rewinds. **That part is done and this proposal preserves it.**
What is *not* done is collapsing the three homes behind one interface and fixing the increment unit.

## Proposed design

Introduce `OffsetCoordinator`: one deep module that owns the live offset, the per-replica acked
offsets (for aggregation), and the reconciliation with the persisted offset. Callers ask it
questions in the vocabulary of *replication*, never in the vocabulary of *which field*.

### The seam

```rust
/// Single owner of the replication offset contract.
///
/// Invariant (the whole reason this type exists): the offset is measured in
/// **RESP command-stream bytes** — the bytes a replica would have to replay to
/// reach this position. Transport framing (the 18-byte `ReplicationFrame`
/// header) is NOT part of the offset. Primary and replica advance by the SAME
/// unit, so an ACK is directly comparable to the live offset.
pub struct OffsetCoordinator {
    /// Live write position. Shared with the cluster bus for HealthProbe.
    live: Arc<AtomicU64>,
    /// Per-replica acked offsets + ACK notifications (the existing session
    /// registry; the coordinator borrows it for aggregation only).
    replicas: Arc<ReplicaRegistry>,
    /// Persisted identity + offset, reconciled at save points.
    state: Arc<RwLock<ReplicationState>>,
}

impl OffsetCoordinator {
    /// Canonical advance for one outbound/inbound frame. ONE definition of the
    /// unit, shared by primary broadcast and replica ingest, so the two ends
    /// can never drift. The header is transport, not stream.
    #[inline]
    pub fn frame_advance(frame: &ReplicationFrame) -> u64 {
        frame.payload.len() as u64
    }

    /// Primary side: record that `payload_len` RESP bytes were broadcast and
    /// return the new live offset. The frame's sequence field is stamped with
    /// this value by the caller, so every frame self-describes its end offset.
    pub fn advance_broadcast(&self, payload_len: u64) -> u64 {
        self.live.fetch_add(payload_len, Ordering::Release) + payload_len
    }

    /// Replica side: record an ingested frame, advancing by the canonical unit.
    pub fn advance_ingest(&self, frame: &ReplicationFrame) -> u64 {
        self.live.fetch_add(Self::frame_advance(frame), Ordering::Release)
            + Self::frame_advance(frame)
    }

    /// The one true live offset. Replaces every `tracker.current_offset()` AND
    /// every `state.replication_offset` read for "where is the stream now".
    pub fn current(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }

    /// Record an ACK from a replica (delegates to the session, notifies WAIT).
    pub fn record_replica_ack(&self, replica_id: u64, acked: u64) {
        self.replicas.record_ack(replica_id, acked);
    }

    /// Minimum acked offset across streaming replicas (for WAIT / safety).
    pub fn min_acked(&self) -> Option<u64> {
        self.replicas.min_acked_offset()
    }

    /// Can a partial resync be *offset-wise* continued for this id/offset?
    /// Reads its OWN live offset — the caller no longer fetches and threads it.
    /// (Replay availability is gated separately; see proposal 14.)
    pub async fn can_serve_partial_sync(&self, requested_id: &str, requested_offset: u64) -> bool {
        let state = self.state.read().await;
        state.window_contains(requested_id, requested_offset, self.current())
    }

    /// Reconcile the persisted offset up to the live offset (monotonic) and
    /// return a state snapshot to save. Absorbs `save_state`'s offset logic.
    pub async fn reconcile_for_persist(&self) -> ReplicationState { /* … */ }
}
```

`ReplicationState.replication_offset` stays as the *persisted* field but is no longer read for "the
live position": `window_contains` is the renamed, offset-parameterized `can_partial_sync`, called
only through the coordinator. The dead `PrimaryReplicationHandler::increment_offset` is deleted.

### Before / after: the PSYNC window check

Before (`primary/mod.rs:172-177`) — caller must know to read the tracker, then hand it to a check
that can't read it itself:

```rust
let current_offset = self.tracker.current_offset();
let state = self.state.read().await;
let offset_in_window = !(replication_id == "?" && offset == -1)
    && offset >= 0
    && state.can_partial_sync(replication_id, offset as u64, current_offset);
```

After — the caller states only the *replica's* request; "which offset is authoritative" is owned
behind the seam:

```rust
let offset_in_window = !(replication_id == "?" && offset == -1)
    && offset >= 0
    && self.offsets.can_serve_partial_sync(replication_id, offset as u64).await;
```

The same collapse applies to WAIT (`blocking.rs:169` → `self.offsets.current()`), INFO
(`info.rs:437`), and the FULLRESYNC reply (`primary/mod.rs:391`): none of them name the tracker or
the state field anymore.

### Why this is the right depth

- **Locality.** The four prose comments enforcing "read the tracker, not state" collapse into one
  invariant on one type. The increment unit is defined once (`frame_advance`) and shared by both
  ends, so "primary and replica agree" stops being a property maintained by two distant call sites
  and becomes a property of a single function. Changing the offset's meaning (e.g. if we ever decide
  framing *should* count) is a one-line edit, not a two-file hunt that silently desyncs if you miss
  one.
- **Leverage.** A ~120-line module deletes the dead `increment_offset`, the threaded
  `current_offset` parameter, the four comments, and — most importantly — the entire class of "which
  offset did this caller read?" bug, of which `can_partial_sync`'s history is one documented
  instance. Every future offset consumer (proposal 14's replay path, multi-shard aggregation) asks
  the coordinator instead of guessing.
- **Deletion test.** The migration is a net deletion at call sites and deletes whole artifacts (the
  dead method, the parameter, the comments). If the new module could not delete those, its shape
  would be wrong.
- **Not a new adapter layer.** This deepens the existing seam — the tracker already owns the live
  atomic and the ack registry; the coordinator just pulls the *persisted* offset and the *increment
  unit* under the same roof and forbids callers from reaching past it. It is not a wrapper callers
  may bypass; the raw `tracker.current_offset()` / `state.replication_offset` reads are removed.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Behavior-preserving **except** the
two increment fixes in Phase 3, which are the point.

1. **Phase 0 — introduce `OffsetCoordinator`** wrapping the existing `live` atomic, session
   registry, and `state`. Add `current`, `min_acked`, `record_replica_ack`, `can_serve_partial_sync`,
   `reconcile_for_persist`, `frame_advance`/`advance_broadcast`/`advance_ingest`. No call sites
   change yet; unit-test the arithmetic and window logic in isolation. `just check frogdb-server`.
2. **Phase 1 — migrate readers.** Repoint WAIT (`blocking.rs:169,174,181,188`), INFO
   (`info.rs:437`), `handle_psync` window (`primary/mod.rs:172-177`), FULLRESYNC reply
   (`primary/mod.rs:391`), and the `current_offset`/`current_offset_sync` accessors
   (`primary/mod.rs:237-265`) to `coordinator.current()` / `can_serve_partial_sync`. Delete the
   `current_offset` parameter from `can_partial_sync` (now `window_contains`, called only by the
   coordinator). Pure read migration; behavior identical.
3. **Phase 2 — migrate writers + delete dead code.** Route `broadcast_command`
   (`primary/mod.rs:272`) through `coordinator.advance_broadcast`, and `save_state`
   (`primary/mod.rs:139-151`) through `reconcile_for_persist`. Delete the orphan async
   `increment_offset` (`primary/mod.rs:244-250`).
4. **Phase 3 — fix the increment unit (the bug).** Change the replica ingest
   (`replica/streaming.rs:32`) from `frame.encoded_size()` to `coordinator.advance_ingest(&frame)`
   (i.e. `frame.payload.len()`), so both ends count RESP payload bytes. Route `REPLCONF GETACK`
   (`primary/mod.rs:226-232`) through the broadcast offset path so it advances the primary offset and
   stamps the frame's sequence field with the real offset instead of `0`. After this phase the
   primary/replica offset-parity property test (below) passes.
5. **Gate.** Add a grep gate to `just lint`: outside `replication/src/state.rs` and the coordinator,
   no `\.replication_offset\b` reads and no `frame\.encoded_size\(\)` in an offset context — the
   offset must come from the coordinator.

## Testing impact

- **Offset arithmetic, no live cluster.** `frame_advance`, `advance_broadcast`, `advance_ingest`,
  `current`, `min_acked`, and `window_contains` are pure/atomic and unit-testable directly — no
  primary+replica socket pair needed. The window cases (matching id in range, out of range,
  secondary-id failover) move out of integration-only coverage into fast unit tests.
- **Primary/replica offset-parity property test (fails today).** Drive N random commands through
  `broadcast_command`, decode each frame through the replica ingest path, and assert the replica's
  offset equals the primary's after every frame. Today this fails by exactly
  `N × FRAME_HEADER_SIZE` (18 bytes/frame); after Phase 3 it holds. This is the regression test that
  pins the unit agreement.
- **WAIT correctness pin.** With the header inflation, a replica that is one small frame *behind*
  can report an offset numerically *ahead* of the primary's target (`18·k ≥ payload`), so
  `count_acked` can satisfy WAIT before the data arrives. Add a test that WAIT does not return until
  the replica has truly ingested the target write; it fails pre-fix.
- **GETACK accounting.** Assert that after a GETACK the primary and replica offsets still match (the
  GETACK either counts on both ends or neither) and that the GETACK frame's sequence field equals
  the live offset, not `0`.
- **Durability unchanged.** The existing `save_state` / reload reconcile tests
  (`primary/tests.rs:161-166`, `state.rs:557-570`) keep passing — round-2 behavior is preserved.

## Risks / open questions

- **The offset is on-the-wire-meaningful.** It appears in `FULLRESYNC <id> <offset>`, `REPLCONF
  ACK <offset>`, and the PSYNC request, and is reported as `master_repl_offset` in INFO. Changing
  the increment unit changes those numbers. The fix is chosen to make FrogDB **self-consistent and
  Redis-compatible**: Redis counts the replication *command stream* (RESP bytes), which has no frame
  header, so payload-only is the Redis-meaning. Any external tooling that reads `master_repl_offset`
  expects RESP-byte semantics, so excluding the header is the compatible choice. The alternative
  (count the header on both ends) would be self-consistent but Redis-incompatible and would bake a
  transport detail into a protocol-visible number — rejected.
- **Cross-version replica skew during rollout.** Because the wire numbers change, a Phase-3 primary
  streaming to a not-yet-upgraded replica (or vice-versa) will disagree on the offset. FrogDB is
  pre-production (no compat guarantee), so the intended mitigation is: upgrade in lockstep, or force
  a full resync across the upgrade boundary (offset mismatch already triggers FULLRESYNC). Worth an
  explicit note in release docs.
- **Multi-shard offset aggregation.** Today the offset is a single global atomic. If replication
  becomes per-shard, "the offset" becomes a vector and `current()` / `min_acked()` need an
  aggregation policy. The coordinator is the right home for that policy, but this proposal keeps the
  single-offset model; the seam just makes the future change a one-module edit.
- **Snapshot-durability coupling.** The FULLRESYNC offset must equal the data in the checkpoint
  (`primary/mod.rs:372-391` captures the live offset *before* cutting the checkpoint). The
  coordinator must expose a `current()` that the checkpoint path samples at cut time; this is the
  same value as today, just sourced through the seam. No change to the offset≤data invariant.
- **`master_replid` is out of scope but adjacent.** The coordinator owns the offset, not the replid;
  the replid stays on `ReplicationState`. But `can_serve_partial_sync` already reads both, so the
  question of whether replid+offset should travel together as an `(id, offset)` position type is
  worth revisiting if the secondary-id failover logic grows.

## Correctness flags

1. **Offset increment unit mismatch — CONFIRMED.** Primary advances the offset by the RESP payload
   length only (`primary/mod.rs:271-272`: `bytes_len = resp_bytes.len()`), while the replica advances
   by the full encoded frame size including the 18-byte header (`replica/streaming.rs:32`:
   `frame.encoded_size()`, where `encoded_size() = FRAME_HEADER_SIZE + payload.len()`,
   `frame.rs:76,210-212`). The two ends count the offset in **different units**, so the replica's
   offset drifts ahead of the primary's by `FRAME_HEADER_SIZE` (18) bytes per frame. This breaks
   `REPLCONF ACK` comparison (replica reports an offset the primary never reaches), corrupts
   `replica_lag` (`tracker.rs:161-168` saturates to 0 because acked > current), and can make WAIT
   return success before the replica has the data; it would also reject a genuinely-caught-up replica
   from the partial-sync window (`requested_offset <= current_offset` is false when the request is
   header-inflated). This is the single most important flag. Fix: replica must advance by
   `frame.payload.len()`, not `encoded_size()`.

2. **`REPLCONF GETACK` frame built with offset `0` — CONFIRMED.** `request_acks`
   (`primary/mod.rs:226-232`) constructs `ReplicationFrame::new(0, payload)`. The frame's first field
   is `sequence`, which on every normal frame carries the post-increment offset
   (`primary/mod.rs:276`: `ReplicationFrame::new(new_offset, resp_bytes)`); the GETACK frame stamps
   `0` instead. Worse, GETACK is sent via `broadcast_frame` (`primary/mod.rs:231`), bypassing
   `broadcast_command`, so it does **not** advance the primary's offset — yet the replica ingests it
   as an ordinary frame and *does* advance its offset by `encoded_size()`. So GETACK both stamps the
   wrong frame offset and adds another primary/replica divergence. Mitigating note: `request_acks`
   currently has **no callers** in the tree (WAIT relies on the replica's 1-second periodic ACK in
   `replica/streaming.rs:43-48`), so the GETACK bug is latent today — but it is a live landmine for
   proposal 14's backlog/replay path, which keys off frame offsets. Fix: route GETACK through the
   offset path so it counts on both ends and the frame carries the real offset.

3. **Dead second writer of the offset — CONFIRMED (code-smell, not a runtime bug).**
   `PrimaryReplicationHandler::increment_offset` (`primary/mod.rs:244-250`) updates both
   `state.replication_offset` and the tracker, but the live broadcast path bypasses it and only the
   tracker is advanced (`primary/mod.rs:272`). It has no callers (verified). Its existence is the
   symptom: "increment the offset" has no single owner, so a second, subtly-different implementation
   was free to appear and rot. Fix: delete it; the coordinator becomes the only writer.
</content>
</invoke>
