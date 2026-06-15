# Proposal: Replication Backlog + Partial-Sync Replay

Status: proposed
Date: 2026-06-15

## Problem

FrogDB has every piece of a partial resync (`+CONTINUE`) *except the one that makes it work*. A
`ReplicationRingBuffer` already buffers recent RESP-encoded commands with their offsets
(`primary/ring_buffer.rs:35-76`). The offset-window check is already implemented and unit-tested
(`state.rs:265-286`). The wire protocol already understands `+CONTINUE` on both sides
(`replica_session.rs:352-364`, `replica/connection.rs:190-200`). Yet every replica reconnect still
forces a full database resync, because the *grant decision* is hardcoded to refuse:
`partial_sync_replay_supported()` returns `false` unconditionally (`primary/mod.rs:218-219`).

The pieces don't connect because the **replay path is absent**. The grant decision is

```rust
let can_partial = offset_in_window && self.partial_sync_replay_supported();  // primary/mod.rs:194
```

where the right operand is a constant `false`. So `offset_in_window` — the result of the correct,
tested `can_partial_sync` — is computed and then discarded. Two real subsystems (a ring buffer that
fills on every write, an offset-window primitive that returns the right answer) are wired to a
dead constant.

This is a depth inversion. Granting `+CONTINUE` requires three behaviours that today live nowhere:
(1) decide whether the backlog still *covers* `(req_offset, current_offset]` — not just whether the
offset is below the head, but whether it is above the oldest *retained* entry after eviction; (2)
*extract* that exact range from the backlog; (3) *stream* it to the replica before joining the live
tail. The interface a caller needs ("can I continue this replica, and if so, give me the bytes") is
small. The behaviour behind it (eviction-aware coverage, offset-ordered extraction, the full-sync
handoff replay) is not. That gap — small interface, real behaviour, currently implemented as a
hardcoded `false` — is the missing deep module.

**Deletion test.** Delete `can_partial_sync` today and nothing breaks: its only non-test caller
feeds a result that is immediately `&&`-ed with `false` (`primary/mod.rs:177,194`). Delete
`extract_divergent_writes` and only split-brain reconciliation notices; PSYNC never calls it. Both
are pass-throughs *for the grant path* — they exist but earn nothing there. Conversely, once replay
is wired, the "is `req_offset` still covered, and hand me `(start, end]`" logic is needed in **three**
places (the PSYNC grant, the partial-sync streamer, and — see [Correctness flags](#correctness-flags)
— the full-sync handoff). That recurrence across N callers is the signal a deep module is missing.

Verified evidence (all paths relative to `frogdb-server/crates/replication/src/`):

| Symptom | Where | file:line |
|---------|-------|-----------|
| Grant gated on a hardcoded `false` | `partial_sync_replay_supported` | `primary/mod.rs:218-219` |
| Window check computed, then discarded by the gate | `handle_psync` | `primary/mod.rs:177,194` |
| Offset-window primitive correct but unreachable from grant path | `can_partial_sync` | `state.rs:265-286` |
| Ring buffer populated on every write, never read by PSYNC | `broadcast_command` push | `primary/mod.rs:273-275` |
| Ring buffer's only reader is split-brain, not replay | `extract_divergent_writes` | `primary/mod.rs:294-299`, `ring_buffer.rs:68-75` |
| No lower-bound (eviction) check on the window | `can_partial_sync` upper-bound only | `state.rs:272` |
| `+CONTINUE` path tails the live broadcast with **no** replay | `handle_partial` | `replica_session.rs:352-364` |
| A test pins the limitation in place | `partial_window_match_still_forces_full_resync_without_backlog` | `replica_session.rs:1137-1189` |

The last two rows are the live hazard. `handle_partial` (`replica_session.rs:352-364`) already
exists and already does the wrong thing: it writes `+CONTINUE`, records the replica's ACK at
`req_offset`, and immediately calls `start_streaming`, which subscribes to `wal_broadcast`
(`replica_session.rs:587`) — a `broadcast` tail carrying only *future* frames. The range
`(req_offset, current_offset]` is never sent. It is dead today only because the gate refuses to
route to it; the moment anyone flips the gate without building replay, replicas silently diverge.
See [Correctness flags](#correctness-flags).

## Current state

### The gate: a hardcoded refusal (`primary/mod.rs:209-220`)

```rust
/// Whether the primary can serve a partial resync (`+CONTINUE`).
///
/// Returns `false` unconditionally today: granting a partial resync requires
/// replaying the backlog range between the replica's offset and the live
/// stream head, and no such backlog is wired into the streaming path (see
/// the detailed rationale in [`Self::handle_psync`]). This is the single,
/// explicit gate for partial-sync support so the limitation is greppable and
/// the offset-window check in [`crate::state::ReplicationState::can_partial_sync`]
/// stays a correct, ready-to-use primitive for when replay is implemented.
fn partial_sync_replay_supported(&self) -> bool {
    false
}
```

### The grant decision discards the window check (`primary/mod.rs:174-205`)

```rust
let state = self.state.read().await;
let offset_in_window = !(replication_id == "?" && offset == -1)
    && offset >= 0
    && state.can_partial_sync(replication_id, offset as u64, current_offset);
let current_repl_id = state.replication_id.clone();
drop(state);

// ...long comment explaining why this is false (primary/mod.rs:181-193)...
let can_partial = offset_in_window && self.partial_sync_replay_supported();

let session = self.tracker.register_replica(addr);
let sync_kind = if can_partial {
    SyncKind::Partial { offset: offset as u64 }
} else {
    SyncKind::Full { replication_id: current_repl_id }
};
session.run(stream, sync_kind, self.clone()).await
```

`offset_in_window` is fully computed and then annihilated by `&& false`. The comment at
`primary/mod.rs:181-193` is an accurate description of *why* it's gated — and a precise spec for
what this proposal must build.

### The window primitive — correct, tested, unreachable (`state.rs:261-286`)

```rust
/// ... Note this only validates the *offset window*; the caller is responsible
/// for confirming it can actually deliver the backlog range
/// `(requested_offset, current_offset]` before granting `+CONTINUE`.
pub fn can_partial_sync(
    &self,
    requested_id: &str,
    requested_offset: u64,
    current_offset: u64,
) -> bool {
    // Check primary ID against the live write position.
    if requested_id == self.replication_id && requested_offset <= current_offset {
        return true;
    }
    // Check secondary ID (for failover continuity)
    if let Some(ref secondary) = self.secondary_id
        && requested_id == secondary
        && self.secondary_offset >= 0
        && requested_offset <= self.secondary_offset as u64
    {
        return true;
    }
    false
}
```

This is an **upper-bound-only** test: `requested_offset <= current_offset`. It says nothing about
whether the backlog still *holds* `requested_offset` — and the doc comment (`state.rs:262-264`)
explicitly delegates that to the caller. That caller does not exist yet.

### The backlog — populated every write, read only by split-brain (`ring_buffer.rs:35-76`)

```rust
pub struct ReplicationRingBuffer {
    entries: parking_lot::Mutex<VecDeque<BufferedCommand>>,
    max_entries: usize,
    current_bytes: AtomicUsize,
    max_bytes: usize,
}

impl ReplicationRingBuffer {
    pub fn push(&self, offset: u64, resp_bytes: Bytes) {
        let entry_size = resp_bytes.len();
        let mut entries = self.entries.lock();
        while entries.len() >= self.max_entries
            || (self.current_bytes.load(Ordering::Relaxed) + entry_size > self.max_bytes
                && !entries.is_empty())
        {
            if let Some(evicted) = entries.pop_front() {
                self.current_bytes.fetch_sub(evicted.resp_bytes.len(), Ordering::Relaxed);
            }
        }
        self.current_bytes.fetch_add(entry_size, Ordering::Relaxed);
        entries.push_back(BufferedCommand { offset, resp_bytes });
    }

    pub fn extract_divergent_writes(&self, last_replicated_offset: u64) -> Vec<(u64, Bytes)> {
        let entries = self.entries.lock();
        entries
            .iter()
            .filter(|cmd| cmd.offset > last_replicated_offset)
            .map(|cmd| (cmd.offset, cmd.resp_bytes.clone()))
            .collect()
    }
}
```

`push` is called on every write (`primary/mod.rs:273-275`, inside `broadcast_command`). The stored
`offset` is the cumulative offset **after** the command (`tracker.increment_offset` returns the new
head, `primary/mod.rs:272`), so each entry's offset is the range *end* of that command.
`extract_divergent_writes` already filters `offset > X` — exactly the shape replay needs — but its
only caller is `ReplicationBroadcaster::extract_divergent_writes` (`primary/mod.rs:294-299`), used by
split-brain reconciliation. PSYNC never touches it. The buffer also has **no accessor for its oldest
retained offset**, so no caller can currently ask "did eviction drop `req_offset`?".

### The full-sync fallback PSYNC always takes (`replica_session.rs:366-440`)

When the grant is `SyncKind::Full`, `handle_full` captures the live offset, replies `+FULLRESYNC`,
cuts a RocksDB checkpoint, and streams the entire database:

```rust
let snapshot_offset = handler.tracker.current_offset();              // :391
let response = format!("+FULLRESYNC {} {}\r\n", replication_id, snapshot_offset);
stream.write_all(response.as_bytes()).await?;                         // :393-394
// ... create_checkpoint (spawn_blocking) ...                          // :401
self.stream_checkpoint(&mut stream, &checkpoint_path, &replication_id, snapshot_offset).await?;  // :418
// ...
self.start_streaming(stream, handler).await                           // :439
```

For a reconnecting replica that missed only a handful of writes, this ships the whole dataset (a
multi-file checkpoint, `stream_checkpoint`, `fullsync.rs:129-188`) — the exact cost partial resync
exists to avoid. This is what replay replaces: instead of re-shipping the database, replay ships the
`(req_offset, current_offset]` tail of the backlog and the replica continues.

### Redis `repl-backlog` mapping

| Redis concept | FrogDB equivalent | Gap |
|---------------|-------------------|-----|
| `repl_backlog` circular byte buffer | `ReplicationRingBuffer` (entry-granular `VecDeque<(offset, Bytes)>`) | exists; not read by PSYNC |
| `repl-backlog-size` | `SplitBrainBufferConfig::max_bytes` + `max_entries` (two-dim cap) | exists (`ring_buffer.rs:7-26`) |
| `master_repl_offset` | `tracker.current_offset()` | exists |
| `repl_backlog_off` (oldest valid offset) | *(none — no `oldest_offset()` accessor)* | **missing** |
| `master_replid` | `state.replication_id` | exists (`state.rs:98`) |
| `master_replid2` / `second_repl_offset` | `state.secondary_id` / `secondary_offset` | exists (`state.rs:105,114`), already honored by `can_partial_sync` |
| Window check `offset ∈ [backlog_off, master_repl_offset]` | `can_partial_sync` checks **only** `offset <= current` | **lower bound missing** |
| `+CONTINUE` then send backlog from `offset` | `handle_partial` sends `+CONTINUE` then **nothing** | **replay missing** |
| `repl-backlog-ttl` (drop backlog after last replica leaves) | *(none — buffer is always-on when enabled)* | open question |

Redis grants `+CONTINUE` iff the replid matches (`master_replid` or `master_replid2`) **and**
`offset` lies in `[repl_backlog_off, master_repl_offset]` — both bounds. FrogDB has the upper bound
and the replid match; it is missing the lower bound (`repl_backlog_off`) and the replay itself.

## Proposed design

Introduce one module that owns the backlog's *replication* role end to end: lifecycle (record),
the eviction-aware grant decision (`can_replay`), the range extraction (`extract_backlog`), and the
single entry point PSYNC calls (`handle_partial_sync_request`). The split-brain buffer becomes an
implementation detail behind it (or is shared with it — see Migration), so there is exactly one
place that knows what the backlog contains and what offsets it can still serve.

### New module: `frogdb-server/crates/replication/src/primary/replay.rs`

```rust
/// Owns the replication backlog and answers the only questions PSYNC needs:
/// "can I continue this replica from `req_offset`?" and, if so, "what bytes do
/// I stream before joining the live tail?".
///
/// Deep by construction: the interface is three methods, but behind them sit the
/// eviction-aware coverage check (the lower bound `can_partial_sync` cannot
/// express), offset-ordered extraction, and the offset→data invariant the
/// streamer relies on. Callers never see the `VecDeque`, the byte accounting,
/// or the wrap/eviction rules.
pub struct PartialSyncReplay {
    /// The backlog. Today this is the existing `ReplicationRingBuffer`, extended
    /// with an `oldest_offset()` accessor; see Migration for the sharing question.
    backlog: ReplicationRingBuffer,
    enabled: bool,
}

/// The decision PSYNC acts on. Total: every PSYNC resolves to exactly one arm.
pub enum ReplayDecision {
    /// Window fits AND the backlog still covers `(req_offset, current]`.
    /// Reply `+CONTINUE` and stream `grant.frames`, then join the live tail.
    Continue(ReplayGrant),
    /// Window/backlog/replid insufficient — caller must `+FULLRESYNC`.
    FullResync(FullResyncReason),
}

pub struct ReplayGrant {
    /// RESP-encoded backlog tail `(req_offset, current_offset]`, offset-ordered.
    pub frames: Vec<(u64, Bytes)>,
    /// Offset the replica holds once the tail is applied (== current at grant time).
    pub resume_offset: u64,
}

/// Why a full resync is required — surfaced for logs/metrics so operators can
/// see *why* a replica fell back (Redis tracks `sync_partial_ok`/`sync_partial_err`).
pub enum FullResyncReason {
    InitialSync,     // PSYNC ? -1
    ReplidMismatch,  // matches neither master_replid nor master_replid2
    OffsetAhead,     // req_offset > current_offset (replica ahead — impossible window)
    BacklogEvicted,  // req_offset < oldest retained offset (the lower-bound miss)
    Disabled,        // backlog disabled by config
}

impl PartialSyncReplay {
    /// Record one broadcast command into the backlog. Called by `broadcast_command`.
    pub fn record(&self, offset: u64, resp_bytes: Bytes) {
        if self.enabled {
            self.backlog.push(offset, resp_bytes);
        }
    }

    /// The single entry point. Pure decision over `(state, req_offset, current)`
    /// plus the backlog's current contents; performs no I/O. PSYNC turns the
    /// result into the `+CONTINUE`/`+FULLRESYNC` reply.
    pub fn handle_partial_sync_request(
        &self,
        state: &ReplicationState,
        requested_id: &str,
        req_offset: u64,
        current_offset: u64,
    ) -> ReplayDecision {
        match self.can_replay(state, requested_id, req_offset, current_offset) {
            Err(reason) => ReplayDecision::FullResync(reason),
            Ok(()) => ReplayDecision::Continue(ReplayGrant {
                frames: self.backlog.extract_backlog(req_offset, current_offset),
                resume_offset: current_offset,
            }),
        }
    }

    /// Both bounds. Composes the existing upper-bound window check with the NEW
    /// lower-bound (eviction) check the backlog alone can answer.
    fn can_replay(
        &self,
        state: &ReplicationState,
        requested_id: &str,
        req_offset: u64,
        current_offset: u64,
    ) -> Result<(), FullResyncReason> {
        if !self.enabled {
            return Err(FullResyncReason::Disabled);
        }
        // Upper bound + replid (master_replid / master_replid2) — reuse, don't re-derive.
        if !state.can_partial_sync(requested_id, req_offset, current_offset) {
            return Err(/* OffsetAhead | ReplidMismatch, classified from state */ ..);
        }
        // Lower bound: the check `can_partial_sync` documents but cannot make.
        match self.backlog.oldest_offset() {
            Some(oldest) if req_offset >= oldest => Ok(()),
            _ => Err(FullResyncReason::BacklogEvicted),
        }
    }
}
```

`extract_backlog(start, end)` is the replay sibling of `extract_divergent_writes`: same
`offset > start` filter, but it is only ever reached *after* `can_replay` has confirmed the lower
bound, so the returned range is guaranteed contiguous from `start` — no silent truncation. The
ring buffer gains one accessor:

```rust
// ring_buffer.rs — the missing lower bound (Redis `repl_backlog_off`).
pub fn oldest_offset(&self) -> Option<u64> {
    self.entries.lock().front().map(|c| c.offset)
}
```

### Before / after: the grant decision (`handle_psync`)

Before — window computed, then thrown away (`primary/mod.rs:174-205`):

```rust
let offset_in_window = !(replication_id == "?" && offset == -1)
    && offset >= 0
    && state.can_partial_sync(replication_id, offset as u64, current_offset);
// ...
let can_partial = offset_in_window && self.partial_sync_replay_supported();  // && false
let sync_kind = if can_partial {
    SyncKind::Partial { offset: offset as u64 }
} else {
    SyncKind::Full { replication_id: current_repl_id }
};
```

After — one call, both bounds, the grant carries its bytes:

```rust
let sync_kind = match self.replay.handle_partial_sync_request(
    &state, replication_id, offset.max(0) as u64, current_offset,
) {
    ReplayDecision::Continue(grant) => SyncKind::Partial { grant },
    ReplayDecision::FullResync(reason) => {
        tracing::info!(?reason, "PSYNC -> full resync");   // sync_partial_err visibility
        SyncKind::Full { replication_id: current_repl_id }
    }
};
session.run(stream, sync_kind, self.clone()).await
```

The `PSYNC ? -1` initial-sync case folds into `can_replay` returning `InitialSync`, so the special
case at `primary/mod.rs:175` disappears too. `partial_sync_replay_supported()` is deleted outright.

### Before / after: the partial-sync streamer (`handle_partial`)

Before — writes `+CONTINUE`, then joins the live tail and silently drops `(req_offset, current]`
(`replica_session.rs:352-364`):

```rust
let replication_id = handler.state.read().await.replication_id.clone();
let response = format!("+CONTINUE {}\r\n", replication_id);
stream.write_all(response.as_bytes()).await?;
handler.tracker.record_ack(self.id, offset);
self.start_streaming(stream, handler).await   // <-- live tail only; the gap is lost
```

After — replay the granted tail, *then* join the live tail:

```rust
let replication_id = handler.state.read().await.replication_id.clone();
let response = format!("+CONTINUE {}\r\n", replication_id);
stream.write_all(response.as_bytes()).await?;
// Replay (req_offset, current] from the backlog before the live broadcast,
// closing the gap the live `wal_broadcast` tail cannot fill.
for (_off, frame) in grant.frames {
    stream.write_all(&ReplicationFrame::new(_off, frame).encode()).await?;
}
handler.tracker.record_ack(self.id, grant.resume_offset);
self.start_streaming(stream, handler).await
```

`SyncKind::Partial { offset }` becomes `SyncKind::Partial { grant: ReplayGrant }`
(`replica_session.rs:135`). The replica side already accepts `+CONTINUE` and then reads frames off
the same stream (`replica/connection.rs:190-200` → `stream_replication`,
`replica/streaming.rs:24-50`), so no replica-side protocol change is needed — the replayed frames
arrive on the wire exactly like live frames.

### Ring-buffer sizing / eviction / what gets streamed

- **Sizing.** Reuse the existing two-dimensional cap: `max_entries` and `max_bytes`
  (`ring_buffer.rs:7-26`, wired from `config.replication.split_brain_buffer_size` /
  `split_brain_buffer_max_mb`, `server/.../replication_init.rs:81-86`). For the replication role
  these map to Redis `repl-backlog-size`; a larger backlog widens the offset window over which a
  reconnect can partial-sync instead of full-resync.
- **Eviction.** FIFO by `pop_front` when either cap is exceeded (`ring_buffer.rs:55-63`). Eviction
  raises `oldest_offset()`, which *narrows* the grantable window from the bottom. `can_replay`'s
  lower bound is exactly this: a replica whose `req_offset` fell below the oldest retained entry
  gets `BacklogEvicted` → full resync, never a truncated replay. A single command larger than
  `max_bytes` is retained alone (the `!entries.is_empty()` guard, `ring_buffer.rs:57`), matching
  Redis (the backlog may briefly exceed its nominal size for one oversized write).
- **What gets streamed.** Exactly `extract_backlog(req_offset, current_offset)` — the RESP bytes
  already buffered, re-framed with their offsets and written ahead of the live subscription. No
  re-encoding from the store, no checkpoint, no disk I/O.

### Why this is the right depth

- **Locality.** The coverage rule (both bounds), the extraction, and "stream the tail then join the
  tail" live in `replay.rs` and the two streamers that call it. The knowledge "what offsets can the
  backlog still serve" exists in one place — `oldest_offset()` + `can_replay` — instead of being a
  TODO scattered across a doc comment (`state.rs:262-264`) and a hardcoded `false`.
- **Leverage.** One module flips replication from "always full resync" to "partial resync when the
  window fits", turning a whole-dataset transfer into a tail replay for the common reconnect case —
  and reuses the buffer and window primitive that already exist. The same `extract_backlog` primitive
  also closes the full-sync handoff gap ([Correctness flags](#correctness-flags) F1).
- **Deletion test.** Today `can_partial_sync` and `extract_divergent_writes` fail the deletion test
  *for the grant path*: remove them and the grant path is unchanged (it's `&& false`). After this
  change, delete `PartialSyncReplay` and the both-bounds coverage check + tail extraction + replay
  sequence reappear in `handle_psync`, `handle_partial`, **and** `handle_full` (the F1 fix) — three
  callers, so it earns its keep. The constant `partial_sync_replay_supported` is deleted: its only
  reason to exist was to mark the absence this module fills.
- **No new adapter.** This is not a wrapper around the ring buffer that PSYNC may or may not use; it
  is the buffer's replication role, owned. It reuses `ReplicationRingBuffer`, `can_partial_sync`,
  `ReplicationFrame`, and the existing `+CONTINUE` wire handling unchanged. `record` is the same
  `push` the broadcast path already calls.

## Migration plan

Each phase compiles and keeps `just test frogdb-replication` green. The flip to actual `+CONTINUE`
grants happens only in the final behavioural phase; everything before it is behaviour-preserving.

1. **Phase 0 — backlog accessor + module skeleton.** Add `ReplicationRingBuffer::oldest_offset()`
   and `extract_backlog(start, end)` (`extract_divergent_writes` renamed/aliased — split-brain keeps
   its caller). Add `primary/replay.rs` with `PartialSyncReplay`, `ReplayDecision`, `ReplayGrant`,
   `FullResyncReason`, `can_replay`, and the unit-test matrix below. Construct it in
   `PrimaryReplicationHandler::new` wrapping (or owning) the existing ring buffer. `record` is called
   from `broadcast_command` in place of the direct `rb.push` (`primary/mod.rs:273-275`). No grant
   path changes; the gate still returns `false`. `just check frogdb-replication`.
2. **Phase 1 — build the streamer (still gated).** Change `SyncKind::Partial` to carry
   `ReplayGrant` and rewrite `handle_partial` (`replica_session.rs:352-364`) to write the replay
   frames before `start_streaming`. Because the gate is still `false`, `handle_partial` is still
   unreachable in production — but it is now correct and **directly unit-testable** (feed a grant,
   assert the wire bytes). Behaviour-preserving.
3. **Phase 2 — wire the decision (still gated).** Route `handle_psync` through
   `replay.handle_partial_sync_request`, but keep overriding the result to `FullResync` behind the
   existing gate, so production behaviour is unchanged while the decision logic runs in tests. The
   pinning test `partial_window_match_still_forces_full_resync_without_backlog`
   (`replica_session.rs:1137-1189`) stays green.
4. **Phase 3 — flip the gate (the behavioural change).** Delete `partial_sync_replay_supported`;
   let `ReplayDecision::Continue` actually drive `SyncKind::Partial`. Replace the pinning test with
   its inverse: a matching window **with backlog coverage** now yields `+CONTINUE`; window-miss and
   eviction still yield `+FULLRESYNC`. Update `primary/mod.rs:181-193`'s comment to describe the
   live path. This is the only phase that changes observable behaviour.
5. **Phase 4 — reuse replay for the full-sync handoff (fixes F1).** In `handle_full`, after the
   checkpoint streams and before/at `start_streaming`, replay `(snapshot_offset, current_offset]`
   from the backlog so writes during checkpoint transfer are not lost. Same `extract_backlog`
   primitive; see [Correctness flags](#correctness-flags). Independently shippable after Phase 0.

FrogDB is pre-production — no deprecation shims; `partial_sync_replay_supported` is removed, not
deprecated.

## Testing impact

- **Replay decided without a live primary+replica.** `PartialSyncReplay` is a pure function over a
  `ReplicationState`, a `(req_offset, current)` pair, and a backlog seeded with `record` calls.
  Assert `ReplayDecision` directly — no sockets, no Tokio, no checkpoint:
  - **window-fit** — replid matches, `oldest <= req_offset <= current` → `Continue` with
    `frames == (req_offset, current]` in offset order.
  - **window-miss (ahead)** — `req_offset > current` → `FullResync(OffsetAhead)`.
  - **replid mismatch** — unknown id → `FullResync(ReplidMismatch)`; secondary id within
    `secondary_offset` → `Continue` (exercises the failover branch, `state.rs:277-283`).
  - **eviction (the lower bound)** — push past `max_entries`/`max_bytes`, request an offset below
    the new `oldest_offset()` → `FullResync(BacklogEvicted)`, **not** a truncated `Continue`. This
    is the case the current code cannot even represent.
  - **boundary** — `req_offset == oldest` and `req_offset == current` both grant (empty tail for
    the latter).
- **Streamer tested over an in-memory duplex.** Feed `handle_partial` a `ReplayGrant` and assert the
  bytes on the wire are `+CONTINUE` followed by exactly the replayed frames, then the live tail —
  reusing the duplex pattern already in `replica_session.rs` tests (e.g. `:1149`).
- **Existing ring-buffer tests extended.** `primary/tests.rs:64-122` already covers push/extract and
  both eviction modes; add `oldest_offset()` assertions and an `extract_backlog` contiguity check.
- **Round-trip regression.** A primary+replica integration test: stream N writes, disconnect the
  replica at offset K, write M more, reconnect → assert `+CONTINUE`, the replica converges, and no
  full checkpoint was sent. The mirror test (evict past K → `+FULLRESYNC`) guards the lower bound.

## Risks / open questions

- **Backlog memory cost.** The backlog must be large enough that a typical reconnect window fits, or
  every reconnect still full-resyncs and the buffer is pure overhead. `max_bytes` (default 64 MB,
  `ring_buffer.rs:23`) is the knob; document it as `repl-backlog-size`'s analogue and surface
  `sync_partial_ok`/`sync_partial_err`-style counters (from `FullResyncReason`) so operators can see
  whether the backlog is sized right.
- **Eviction vs slow replicas.** A replica that disconnects and reconnects slowly may find its
  `req_offset` evicted → full resync. This is correct (better a full resync than a silent gap) and
  matches Redis. The lag-disconnect path (`replica_session.rs:660-683`) already drops replicas that
  fall too far behind *while streaming*; the backlog window governs the *reconnect* case. The two
  should be sized consistently (a replica dropped for lag should usually still be inside the backlog
  window when it reconnects).
- **The window invariant `offset <= data`.** `can_replay` must never grant a tail the backlog cannot
  fully cover. The lower-bound check (`req_offset >= oldest_offset()`) is load-bearing: removing it
  reintroduces silent truncation. The unit matrix pins it; consider a debug assertion in
  `extract_backlog` that its first returned offset is `<= req_offset + first_entry_size` (no gap at
  the head).
- **Interaction with the offset coordinator (proposal 18).** This proposal assumes a single,
  consistent offset scale between what the replica sends in `PSYNC <id> <offset>` and what the
  backlog stores. Today they disagree: the primary advances by *payload* bytes
  (`broadcast_command`, `primary/mod.rs:271-272`) while the replica advances by *full frame* size
  (`replica/streaming.rs:32`, `frame.encoded_size()`). That mismatch — and unifying the three-way
  offset tracking — is **proposal 18's** scope, not this one. Replay is correct **given** proposal
  18's offset contract; until then, `req_offset` and the backlog's stored offsets are on different
  scales and `can_replay` would mis-classify. **Proposal 14 depends on proposal 18 landing first**
  (or on both offsets being defined as payload bytes).
- **Multi-shard backlog.** FrogDB has internal shards, but replication is a *single* global stream:
  one `PrimaryReplicationHandler`, one `wal_broadcast`, one `tracker` offset, one ring buffer
  (`server/.../replication_init.rs:71-90`). So one backlog suffices — there is no per-shard offset
  to reconcile here. If replication is ever sharded (per-shard offsets/streams), the backlog would
  shard with it and `can_replay` would need a per-shard window; out of scope today, but the
  single-stream assumption should be stated where the module is constructed.
- **replid continuity across failover (`master_replid2`).** `can_partial_sync` already honours
  `secondary_id`/`secondary_offset` (`state.rs:277-283`, set by `new_replication_id`,
  `state.rs:233-246`), so replay across a failover works *if* the new primary's backlog covers the
  requested offset — which it generally will not, because a freshly promoted replica starts its
  backlog empty. INFO still reports `master_replid2`/`second_repl_offset` as `0`/`-1` (per
  INDEX.md); wiring those to `secondary_id`/`secondary_offset` is a prerequisite for *cross-failover*
  partial sync, but not for the common same-primary reconnect this proposal targets. Track
  separately.
- **`repl-backlog-ttl` has no analogue.** Redis frees the backlog after the last replica has been
  gone for `repl-backlog-ttl`. FrogDB's buffer is always-on when enabled. Low risk (it is bounded),
  but a TTL would reclaim memory on a primary with no replicas. Defer.
- **Sharing vs splitting the buffer with split-brain.** The split-brain reconciler and PSYNC replay
  want the same data (recent commands + offsets). Sharing one `ReplicationRingBuffer` is the DRY
  choice and what Phase 0 assumes. If their sizing requirements diverge (split-brain may want a
  shorter window, replay a longer one), split into two instances behind `PartialSyncReplay`; the
  interface does not change either way.

## Correctness flags

Found while reading; the offset-increment bug (primary by payload, replica by full frame) is
**proposal 18's**, not listed here.

- **F1 — full-sync handoff silently drops writes made during checkpoint transfer (data loss +
  offset divergence).** `handle_full` captures `snapshot_offset` (`replica_session.rs:391`), then
  cuts and streams the entire checkpoint (`stream_checkpoint`, `:418`), and only *after* that calls
  `start_streaming`, which is where the replica's `wal_broadcast` subscription is created
  (`replica_session.rs:587`). `broadcast::Sender::subscribe()` delivers **only frames sent after the
  call** — so every write broadcast during checkpoint creation and the (potentially long) network
  transfer goes to `wal_broadcast` with no subscriber for this replica and is lost for it. The
  replica loads the checkpoint, sets its offset to `snapshot_offset`, then resumes at an arbitrary
  later live frame (incrementing per frame, `replica/streaming.rs:30-38`) — so it both *misses*
  `(snapshot_offset, subscribe_time]` and carries a wrong offset. The design comment's claim that
  "the replica may re-receive a few writes from the live stream after loading"
  (`replica_session.rs:382-388`) assumes the stream *replays* from `snapshot_offset`, but the
  broadcast tail never does. Fix: replay `(snapshot_offset, current_offset]` from the backlog at the
  handoff (Phase 4) — the same primitive this proposal builds. This is the strongest motivation for
  the backlog and is independent of partial sync.

- **F2 — `handle_partial` would tail the live broadcast and drop `(req_offset, current]` (gated, but
  live the instant the gate flips).** `handle_partial` (`replica_session.rs:352-364`) writes
  `+CONTINUE`, records the ACK at `req_offset`, and calls `start_streaming` with **no replay** — the
  exact partial-sync analogue of F1. It is unreachable today only because
  `partial_sync_replay_supported()` is `false` (`primary/mod.rs:218-219`); the known limitation is
  documented in INDEX.md. The hazard is that the gate, the streamer, and the decision are three
  separate sites: flipping the gate (`primary/mod.rs:194`) without rebuilding `handle_partial`
  silently diverges every partial-synced replica. The migration plan forbids this ordering — the
  streamer (Phase 1) and decision (Phase 2) are built and tested *before* the gate flips (Phase 3) —
  and folding the grant, the coverage check, and the replay into one module removes the ability to
  flip one without the others.
