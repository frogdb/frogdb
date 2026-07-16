# Proposal: Replica-Session Streaming Loop — Delete the Dead Fan-Out, Split the Overloaded Loop

Status: proposed
Date: 2026-07-16

## Problem

`start_streaming` is the primary's per-replica live-stream pump: subscribe to the WAL broadcast,
replay the backlog handoff tail, then forward every new frame while a read task ingests REPLCONF
ACKs. One ~180-line function
(`replication/src/replica_session.rs:621-800`) carries eight distinct concerns, and one of them —
a whole second `select!` arm plus a handler map on `PrimaryReplicationHandler` — is a fan-out seam
that **nothing in production ever drives**. The dead surface and the overload reinforce each other:
the loop is hard to read partly because a third of it services a channel that is never sent to.

| Symptom | Where |
|---------|-------|
| `connections: Arc<RwLock<HashMap<u64, ReplicaConnectionHandle>>>` field | `replication/src/primary/mod.rs:64` |
| `ReplicaConnectionHandle` (`#[allow(dead_code)]`, all fields `_`-prefixed) | `replication/src/primary/mod.rs:93-99` |
| Map only ever **inserted** (`:656`) and **removed** (`:316`) in production; every read is a test assertion | `replica_session.rs:656,316` vs `1128,1168,1199,1317` |
| `_frame_tx` half of the per-session channel, stored in the handle, **never sent to** | `replica_session.rs:629,653`; `primary/mod.rs:97` |
| The `frame_rx.recv()` `select!` arm (~27 lines) — a second copy of the write+timeout+error branch, purely to drain that dead channel | `replica_session.rs:764-790` |
| Ack seeding smuggled through the ACK-ingestion entry: `tracker.record_ack(self.id, resume_offset)` | `replica_session.rs:647` |
| Backlog-replay dedup, connection bookkeeping, stream split, ACK-parse read task, inline lag-threshold disconnect policy, write timeout, two duplicated write branches — all inline in one loop | `replica_session.rs:621-800` |

The `_frame_tx`/`frame_rx` pair is a **pure pass-through seam with no source**. `frame_tx` is moved
into the `ReplicaConnectionHandle` (`:653`) and parked in the `connections` map; the map is never
read in production; nothing anywhere calls `send` on it. Apply the deletion test: remove the map,
the handle, the channel, and the `frame_rx` `select!` arm, and **no production behavior changes** —
the live stream flows entirely through `wal_rx` (`:698-763`). The `frame_rx` arm exists solely to be
selected-on and never fire. It duplicates the `wal_rx` arm's timeout-wrap, error-log, and break
logic (`:708-726` mirrored at `:768-786`), so the loop maintains two copies of "write a frame to the
replica" that must stay consistent for a code path that cannot execute.

Beyond deletion, even the *live* half of the loop reads at low locality. Reasoning about the
lag-threshold disconnect policy (`:727-751`) means reading past the timeout wrapper, the dedup guard,
and the broadcast-error arms; reasoning about ack seeding means noticing that `:647` routes a
*primary bookkeeping* fact (where this replica resumed) through `record_ack` — the same entry the
read task uses to ingest a replica's *acknowledgement* (`:670`). The monotonic guard in
`ReplicaSession::record_ack` (`replica_session.rs:257-270`) makes the overload safe, but both the
seed and a genuine ACK notify WAIT waiters through `tracker.record_ack`'s broadcast
(`tracker.rs:245-246`) — "the replica told us where it is" and "we told ourselves where it started"
share one channel by accident, not by design.

### Fold-in: the same vestigial-surface theme on `ReplicationBroadcaster`

Two more items on the broadcast trait are the same shape — interface wider than any caller needs:

- **`extract_divergent_writes` sits on the hot-path trait but has one production caller.**
  `replication/src/lib.rs:112-114` declares it on `ReplicationBroadcaster`; the only non-test call
  is the split-brain reconciliation at `server/src/cluster_init.rs:462`. It is meaningful *only* on
  `PrimaryReplicationHandler` (which forwards to the backlog, `primary/mod.rs:307-309`); `Noop`
  (`lib.rs:162`) and the shard test doubles return an empty vec. A backlog-introspection method
  leaking onto the frame-broadcast interface is a leaky seam: shard workers hold a
  `ReplicationBroadcaster` to *emit writes*, not to mine the ring buffer.
- **Untagged `broadcast_command` / `broadcast_transaction` are a parallel definition with no
  production caller.** Production writes go through the shard-tagged variants only
  (`core/src/shard/post_execution.rs:263-285` calls `broadcast_command_on_shard` /
  `broadcast_transaction_on_shard`). The untagged `broadcast_transaction` default
  (`lib.rs:123-129`) hand-rolls MULTI/EXEC framing that must stay byte-for-byte consistent with the
  tagged `broadcast_transaction_on_shard` (`lib.rs:137-143`); two framings, one of them dead outside
  tests. The `_on_shard` methods still default to the untagged ones (`lib.rs:102-104`), so the
  untagged pair cannot simply vanish — but it can stop being the trait's public promise.

## Design

Four deletions and three small extractions. No new module — the work makes `replica_session.rs`
honest about what it does, and trims two trait surfaces to their real callers.

### 1. Delete the dead fan-out seam

```rust
// primary/mod.rs — gone entirely:
//   pub(crate) connections: Arc<RwLock<HashMap<u64, ReplicaConnectionHandle>>>,
//   pub(crate) struct ReplicaConnectionHandle { _replica_id, _address, _frame_tx, _connected_at }
// replica_session.rs — gone:
//   let (frame_tx, mut frame_rx) = mpsc::channel::<ReplicationFrame>(1000);  // :629
//   the ReplicaConnectionHandle build + connections.insert                    // :650-657
//   handler.connections.write().await.remove(&self.id);                       // :316
//   the entire `frame = frame_rx.recv() => { ... }` select arm                // :764-790
```

The `select!` collapses to a single arm over `wal_rx`, which is the only live frame source.

### 2. One `forward_frame` helper — write+timeout+error, defined once

The surviving `wal_rx` arm and the (now-deleted) `frame_rx` arm shared the same write logic. With
the second arm gone there is only one caller, but the timeout-wrap is still worth naming so the
loop body reads as policy, not plumbing:

```rust
/// Write one encoded frame to the replica, honoring the optional write
/// timeout. `Break` means the session must end (timeout or I/O error); the
/// caller stops streaming. The single home for "send a frame to a replica".
enum Forward { Continue, Break }

async fn forward_frame(
    write_half: &mut (impl AsyncWrite + Unpin),
    encoded: &[u8],
    timeout: Option<Duration>,
    replica_id: u64,
) -> Forward {
    match timeout {
        Some(dur) => match tokio::time::timeout(dur, write_half.write_all(encoded)).await {
            Ok(Ok(())) => Forward::Continue,
            Ok(Err(e)) => { tracing::warn!(error = %e, "Error writing to replica"); Forward::Break }
            Err(_) => { tracing::warn!(replica_id, "Write to replica timed out, disconnecting"); Forward::Break }
        },
        None => match write_half.write_all(encoded).await {
            Ok(()) => Forward::Continue,
            Err(e) => { tracing::warn!(error = %e, "Error writing to replica"); Forward::Break }
        },
    }
}
```

### 3. Lift the lag policy into a small named type

The inline threshold check (`:727-751`) becomes a value the loop consults, so "when do we proactively
disconnect a lagging replica" is unit-testable without a live socket:

```rust
/// Proactive lag-disconnect policy for one streaming session. Owns the
/// frame counter and threshold comparison; the streaming loop asks
/// `should_disconnect` every `LAG_CHECK_INTERVAL` frames.
struct LagPolicy {
    threshold_bytes: u64,   // 0 = disabled
    threshold_secs: u64,    // 0 = disabled
    cooldown: Duration,
    frames: u64,
}

impl LagPolicy {
    fn enabled(&self) -> bool { self.threshold_bytes > 0 || self.threshold_secs > 0 }

    /// Returns true when this replica has exceeded a threshold and is not in
    /// cooldown; the caller records the disconnect and breaks.
    fn should_disconnect(&mut self, tracker: &ReplicationTrackerImpl, id: u64) -> bool { /* ... */ }
}
```

### 4. Give ack *seeding* a distinct entry from ack *ingestion*

Replace the overloaded `tracker.record_ack(self.id, resume_offset)` seed (`:647`) with a
`seed_acked_position` entry on the tracker that sets the resume point without pretending a replica
acknowledged it. It can share the same underlying monotonic atomic (`acked_offset`), so no second
source of truth is introduced — but the *name* stops conflating "primary set the initial position"
with "replica reported an offset", and the seeding stops emitting a spurious WAIT-waiter
notification on a position no replica has actually confirmed.

### 5. Trait diet (fold-in)

- Move `extract_divergent_writes` **off** `ReplicationBroadcaster` and onto `PrimaryReplicationHandler`
  (delegating to the backlog it already owns). `cluster_init.rs:462` already holds the concrete
  primary handler on the demotion path, so the call site downcasts nothing new. The `Noop` and shard
  test-double impls lose a method they only ever stubbed.
- Make the **shard-tagged** `broadcast_command_on_shard` / `broadcast_transaction_on_shard` the
  trait's required methods and demote the untagged `broadcast_command` / `broadcast_transaction` to
  a private helper on `PrimaryReplicationHandler` (they remain reachable — the tagged path uses
  `CONTROL_SHARD` framing — but stop being the interface every broadcaster must implement). The
  MULTI/EXEC framing then has exactly one definition.

## Why this is the right depth

- **Deletion test.** The core move is a straight deletion: a field, a struct, a channel, a
  `select!` arm, an insert, and a remove — all provably unreachable in production
  (`connections` is read only by tests; `_frame_tx` is never sent to). Nothing downstream compensates
  for their removal, which is the signature of a pass-through seam with no source. Net negative lines,
  zero behavior change.
- **Locality.** After the split, each concern lives where you would look for it: "how do we write a
  frame" is `forward_frame`, "when do we drop a laggard" is `LagPolicy`, "where did this replica
  resume" is `seed_acked_position`. The streaming loop becomes a readable pump — subscribe, replay,
  forward-or-break — instead of an eight-concern monolith whose largest single block is dead.
- **Leverage.** `forward_frame` and `LagPolicy::should_disconnect` are directly unit-testable; today
  the lag policy can only be exercised through a live `tokio::io::duplex` session. The trait diet
  removes two methods from every non-primary broadcaster, so the frame-emit interface shard workers
  depend on says only "emit writes," not "emit writes and also mine the backlog and also frame
  untagged transactions."
- **Deepens, does not fork.** Offsets still flow through proposal 18's `OffsetCoordinator`; acks
  still ride the tracker's registry and `ack_notify` channel (proposal 39); the backlog stays the
  single home for divergent-write extraction (proposal 14). This proposal removes vestigial surface
  around those seams rather than adding a parallel one. It is the streaming-loop counterpart to
  proposal 40's small-seam deletions.

## Testing impact

- **Existing lifecycle tests keep passing.** The `run_cleans_up_*` tests over `tokio::io::duplex`
  (`replica_session.rs:1135-1318`) assert session teardown via `Phase::Disconnecting`; those hold
  unchanged. The four tests that assert `connections.read().await.get(id).is_none()`
  (`:1128,1168,1199,1317`) lose their subject and are deleted with the map — they only ever proved the
  dead map was maintained.
- **New unit tests, no socket required.** `forward_frame`: clean write returns `Continue`; a write
  that exceeds the timeout returns `Break`; an I/O error returns `Break`. `LagPolicy`: byte threshold
  and time threshold each trigger; cooldown suppresses a re-trigger; disabled policy never fires.
- **Ack-seeding regression.** A test that seeds a resume position and asserts it does **not** emit a
  WAIT-waiter notification (whereas a genuine ACK at a higher offset does) — pinning the split of
  `seed_acked_position` from `record_ack`.
- **Trait diet.** The split-brain reconciliation test that drives `extract_divergent_writes` through
  the demotion path continues to pass against the primary-handler method; the backlog's own
  `extract_divergent_writes` unit tests (`primary/tests.rs:70-119`) are unaffected (they already call
  the ring buffer directly).

## Risks / open questions

- **Was the fan-out channel a planned extension point?** `ReplicaConnectionHandle` and `_frame_tx`
  read like scaffolding for out-of-band per-replica pushes (targeted GETACK, a kill frame). Nothing
  uses it today and GETACK already rides `wal_broadcast` via `request_acks` (`primary/mod.rs:250-263`,
  per proposal 39), so the seam is speculative. If a *targeted* per-replica push ever lands, it should
  be reintroduced as a live seam with a real sender — cheaper than carrying a dead one whose write
  logic silently drifts from the live arm. Deleting now and re-adding later is the DRY-correct trade.
- **Sharing the `acked_offset` atomic between seed and ingest.** `seed_acked_position` reusing the
  monotonic atomic keeps one source of truth, but a future reader might expect "acked" to mean
  "replica-confirmed" exclusively. The method name and a doc-comment carry that distinction; if it
  proves confusing, a separate `resume_offset` field is the fallback (more state, clearer names).
- **`broadcast_command` still reachable.** Demoting it to a private helper (not deleting it) is
  deliberate: the tagged path frames control commands as `CONTROL_SHARD` through the same machinery.
  If a future audit shows even that indirection is unused, it can be inlined — but that is a separate,
  smaller deletion, not a prerequisite here.
- **Lag-check cadence unchanged.** `LagPolicy` preserves the every-`LAG_CHECK_INTERVAL`-frames check
  (`primary/mod.rs:49`); this refactor moves the policy, it does not retune it. Any change to the
  cadence or thresholds is out of scope.
