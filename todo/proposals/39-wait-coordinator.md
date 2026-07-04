# Proposal: WAIT Coordinator

Status: implemented
Date: 2026-07-04

## Problem

`WAIT numreplicas timeout` is one decision — *have `numreplicas` replicas acknowledged the stream
up to where it stood when WAIT arrived?* — but before this work it had no module. Its pieces were
smeared across three files, re-encoded through two enums, and one piece (ack solicitation) was
missing entirely:

| Symptom | Where (pre-change) |
|---------|--------------------|
| Parse/validate → `Response::BlockingNeeded { op: BlockingOp::Wait, keys: vec![] }` sentinel | `server/src/commands/replication.rs:401-463` |
| Protocol op re-mapped to an identical core op it never uses | `connection/util.rs:66-74` (`convert_blocking_op` Wait arm) |
| Blocking machinery entered only to be exited: the Wait carve-out | `connection/handlers/blocking.rs:40-46` |
| The actual wait: snapshot + `wait_for_acks` + timeout, inline | `connection/handlers/blocking.rs:158-192` |
| Ack solicitation missing: `request_acks` had **no callers**; the REPLCONF GETACK command arm was a log-only stub | `replication/src/primary/mod.rs:212-224`; `commands/replication.rs:229-235` |
| Replica ignored GETACK (consumer skips all REPLCONF), ACKing only on a 1 s interval | `server/src/replication/executor.rs:154-157`; `replication/src/replica/streaming.rs:14,48-53` |
| ROLE independently re-projects streaming replicas + acked offsets — the same set WAIT counts | `commands/replication.rs:509-535` vs `tracker.rs:153-159` |

Three consequences, in increasing severity:

1. **A sentinel path that is all interface and no module.** WAIT rode the blocking-command channel
   (`BlockingNeeded` → `handle_internal_action` → `handle_blocking_wait`) only to be special-cased
   *back out* before any of that machinery ran. Both `BlockingOp` enums carried a `Wait` variant
   whose only purpose was to survive one hop and be pattern-matched away; `keys: vec![]` existed to
   satisfy a field WAIT has no use for. Proposal 12 explicitly flagged this as a procrustean fit
   ("do not force WAIT through it"). The deletion test failed in the telling direction: deleting
   the `Wait` variants deletes *only* plumbing — no behavior.

2. **WAIT latency was floored at the replica's spontaneous ACK cadence.** Redis's `waitCommand`
   calls `replicationRequestAckFromSlaves()` the moment a WAIT blocks, so a `REPLCONF GETACK *`
   goes out and replicas answer immediately. FrogDB's `request_acks` existed (proposal 18 made it
   advance + stamp + record correctly, commit 944e8882) but **nothing called it**, and even if
   called, the replica's frame consumer skipped REPLCONF frames without answering. Every WAIT
   therefore waited for the 1-second ack tick — a ~500 ms average, 1 s worst-case latency tax on
   every quorum check, invisible in tests that pass generous timeouts.

3. **Redis semantic drift.** `timeout 0` returned immediately (Redis: block until quorum);
   `numreplicas 0` returned a hard-coded `0` (Redis: the actual acked count); WAIT on a replica
   succeeded vacuously (Redis: an error, before argument parsing); WAIT inside MULTI returned nil
   (Redis: the acked count, never blocking); CLIENT UNBLOCK could not release a blocked WAIT
   (Redis: it can — and with timeout-0-blocks-forever it is the escape hatch).

## Design

One new module owns the replication side of the decision; the connection handler keeps only what
is genuinely *connection*.

### The seam — `replication/src/wait_coordinator.rs`

```rust
/// The ack-solicitation edge, as a seam. PrimaryReplicationHandler is the
/// production adapter; tests supply a mock that records invocations.
/// (Mirrors proposal 12's UnblockSignal precedent.)
pub trait AckSolicitor: Sync {
    fn solicit_acks(&self) -> impl Future<Output = ()> + Send;
}

/// How a WAIT ended. Both arms carry the acked count, because the reply is
/// the count regardless.
pub enum WaitVerdict { Reached(u32), TimedOut(u32) }

pub struct WaitCoordinator {
    offsets: Arc<OffsetCoordinator>,     // proposal 18's seam: the ONLY offset source
    tracker: Arc<ReplicationTrackerImpl>, // ack registry + notification channel
}

impl WaitCoordinator {
    /// Snapshot the target: the live offset at WAIT time (via OffsetCoordinator).
    pub fn target_offset(&self) -> u64;
    /// Quorum count over the shared streaming-ack projection.
    pub fn count_acked(&self, target: u64) -> u32;
    /// immediate check → solicit once → quorum-or-deadline → count.
    /// `deadline = None` is Redis `timeout 0`: no deadline.
    pub async fn wait_for_replicas(
        &self, target: u64, num_replicas: u32,
        deadline: Option<Instant>, solicitor: &impl AckSolicitor,
    ) -> WaitVerdict;
}
```

`wait_for_replicas` is a line-for-line mirror of Redis `waitCommand`:

1. `count_acked(target) >= num_replicas` → return the count without blocking (Redis's
   `replicationCountAcksByOffset` fast path; covers `numreplicas 0`, returning the *actual* count).
2. Otherwise solicit one GETACK round — but only when at least one streaming replica is attached.
   GETACK is part of the command stream (it advances the offset, per proposal 18), and Redis
   likewise skips the stream write when nobody would consume it; without the guard, a WAIT-heavy
   client on a replica-less primary would grow `master_repl_offset` with no writes.
3. Block on the tracker's ack-notification channel until the quorum is reached or the deadline
   elapses; return the count either way.

There is deliberately **no periodic re-solicit** during long waits, matching Redis (which requests
acks once per blocking WAIT): replicas now answer GETACK immediately, and their spontaneous
1-second ACK cadence bounds the staleness of any replica that attaches mid-wait.

The `PrimaryReplicationHandler` constructs and stores the coordinator (it already owns both
dependencies) and is itself the production `AckSolicitor` (`solicit_acks` → `request_acks`), so the
handler side reads `handler.wait_coordinator().wait_for_replicas(.., handler)`.

### The replica answers GETACK — `replica/streaming.rs`

The streaming loop now recognizes the solicitation frame (`is_getack_frame`, a structural
case-insensitive match — full RESP decode is unnecessary on the per-frame hot path) and answers
with an immediate `REPLCONF ACK <offset>`, where the offset already includes the GETACK frame's own
bytes — exactly the Redis replica behavior. The frame still flows to the consumer (which skips
REPLCONF as before); only the *answer* is new.

### The handler keeps the connection concerns — `handlers/blocking.rs`

```rust
pub(crate) async fn handle_wait_command(&mut self, args: &[Bytes]) -> Response {
    if self.is_replica.load(..) { return Response::error(WAIT_ON_REPLICA_ERR); } // before parsing, as Redis
    let (num_replicas, timeout_ms) = parse_wait_args(args)?;                     // shared with the shard path
    let Some(primary) = self.cluster.primary_replication_handler.clone() else {
        return Response::Integer(0);                                            // standalone (divergence, below)
    };
    let wait = primary.wait_coordinator();
    let target = wait.target_offset();
    if wait.count_acked(target) >= num_replicas { return count; }               // fast path, no bookkeeping
    // mark blocked in the registry (so CLIENT UNBLOCK can target it), then race:
    tokio::select! { biased;
        verdict = wait.wait_for_replicas(target, num_replicas, deadline, &*primary) => count,
        mode = self.client_handle.unblocked() => /* count, or -UNBLOCKED on ERROR mode */,
    }
    // clear blocked state, reset unblock
}
```

The coordinator holds the **single timeout authority** (its internal `timeout_at`); the handler's
race has exactly two arms — the verdict and CLIENT UNBLOCK — so the dual-deadline hazard proposal
12 documented for the shard path cannot arise here.

### Routing: the sentinel path is gone

`WAIT` is intercepted by name in `route_and_execute_with_transaction`, in the same dispatch slot
(and with the same shape) as PSYNC's existing special case: after arity validation, pause, and —
critically — after MULTI queueing. Deleted outright:

- `BlockingOp::Wait` from **both** enums (protocol and core), the `convert_blocking_op` Wait arm,
  the `handle_blocking_wait` carve-out, and the scripting binding's Wait match arm.
- `WaitCommand::execute` no longer emits `BlockingNeeded`. It keeps `Standard` strategy and becomes
  the **deny-blocking path**: queued WAITs execute on the shard at EXEC time, where `execute`
  returns `count_acked(current_offset)` immediately — Redis's `CLIENT_DENY_BLOCKING` branch,
  replacing the previous nil. (`ExecutionStrategy::ConnectionLevel(Replication)` was considered and
  rejected: EXEC's deferred-command partition would route WAIT to the transaction handler's
  fallback arm and reply `+OK`.)

Argument validation lives once in `parse_wait_args`, shared by the blocking and deny-blocking
paths, so they cannot diverge.

### The shared projection — WAIT and ROLE read one accessor

`ReplicationTrackerImpl::get_streaming_replicas()` is now documented as THE acked-offset
projection, and `count_acked` / `min_acked_offset` are **derived from it** instead of re-filtering
the session map with their own copies of the streaming predicate. ROLE already listed
`get_streaming_replicas()`; the replicas ROLE lists and the replicas WAIT counts are now the same
set by construction, not by parallel filters that happen to agree.

## Redis semantics: mirrored and diverged

Mirrored (from `waitCommand`, `replication.c`):

- Replica rejection **before** argument parsing, with Redis's exact error text.
- Fast path returns the actual acked count (can exceed `numreplicas`; `numreplicas 0` no longer
  hard-codes `0`).
- One `REPLCONF GETACK *` solicitation when a WAIT blocks; replicas answer immediately.
- `timeout 0` blocks until the quorum is reached; CLIENT UNBLOCK releases it (TIMEOUT mode → the
  current count, ERROR mode → `-UNBLOCKED`).
- Timeout returns the count acked by then; the reply is always an integer.
- WAIT inside MULTI/EXEC never blocks and returns the count (deny-blocking).
- Timeout parse: non-negative, `now + timeout` overflow-checked (Redis `mstime() + timeout`).

Documented divergences (deliberate):

- **Target offset is the global live offset, not `c->woff`.** Redis snapshots the offset after the
  *client's own* last write, so unrelated writers don't delay a WAIT. FrogDB does not track a
  per-connection write offset; the global offset is strictly conservative (never reports success
  before the client's writes are acknowledged) at the cost of waiting on other clients' later
  writes. Revisit if per-connection `woff` tracking ever lands.
- **Standalone returns 0 immediately, even with `timeout 0`.** With no primary replication handler
  there is no PSYNC surface — no replica can *ever* attach, so the quorum is unreachable by
  construction and idling out the timeout (or blocking forever) would communicate nothing. Redis
  cannot make this shortcut because any Redis can acquire replicas at runtime. A FrogDB *primary*
  with zero replicas behaves like Redis: it blocks (replicas may attach mid-wait).
- **No `last_numreplicas` cache.** Redis caches the last computed count to short-circuit repeated
  scans when many clients WAIT concurrently. FrogDB's `count_acked` is a scan over the (small)
  streaming-session map per ack notification; not worth the shared mutable state until replica
  counts or WAIT concurrency demand it.

## Why this is the right depth

- **Locality.** The whole WAIT decision — snapshot, fast path, solicitation policy, quorum wait,
  timeout→count — is one module with one entry point. The previous change-surface (command parse
  site, two enum variants, a converter arm, a carve-out, an inline wait) collapses to
  `wait_coordinator.rs` plus a thin connection wrapper. The solicitation *policy* (when to GETACK)
  lives beside the quorum logic it serves, not in a handler that would have to know GETACK advances
  the offset.
- **Leverage.** `AckSolicitor` + the tracker's constructibility make every Redis semantic a fast
  unit test (8 in the coordinator, 3 for the wire match, 1 end-to-end pin on the primary) — none of
  which existed before because the logic was welded to the connection. WAITAOF, when it comes,
  slots in as a second verdict source behind the same seam instead of a second sentinel path.
- **Deletion test.** The migration deleted whole artifacts: two enum variants, a converter arm, the
  carve-out, the `BlockingNeeded` construction, and the scripting special case — net plumbing
  removal, which is the signature of replacing an interface-only path with a module.
- **Deepens, does not fork.** Offsets come only through proposal 18's `OffsetCoordinator`; acks
  ride the tracker's existing registry/notification channel; the blocked-client registry
  bookkeeping and `biased` race shape follow proposal 12's coordinator conventions. No parallel
  offset home, no second ack channel, no second blocking framework.

## Testing impact

- **Coordinator unit tests** (`wait_coordinator.rs`): satisfied-quorum never solicits;
  `numreplicas 0` returns the real count; a blocking wait with a streaming replica solicits
  *exactly once* then reaches quorum on ack; timeout returns the count at the target; a
  replica-less wait never solicits (offset stays put); acks below the target don't count;
  `deadline = None` blocks until quorum.
- **GETACK wire + end-to-end pins**: `is_getack_frame` accepts the exact production wire form
  (case-insensitively) and rejects `REPLCONF ACK`/other commands; a primary-level test asserts a
  blocking WAIT broadcasts a GETACK frame stamped with the advanced live offset (regression for
  "WAIT never solicits").
- **Integration** (`integration_replication.rs`): 8 write+WAIT rounds against a live replica must
  finish far under the 1-second-cadence floor (regression: solicitation actually reduces latency
  end-to-end); `WAIT 1 0` resolves promptly with a live replica; WAIT on a replica errors; CLIENT
  UNBLOCK releases a deadline-less WAIT in both TIMEOUT and ERROR modes; WAIT inside MULTI returns
  the count immediately. `test_wait_no_replicas` updated for the `timeout 0` semantic change
  (now uses a real timeout).
- **Existing suites**: `wait_tcl.rs` argument-validation and standalone tests pass unchanged
  (validation moved, not altered); the blocking-command suites are untouched by the enum deletion
  (no other variant changed).

## Risks / open questions

- **`timeout 0` now blocks indefinitely on a replica-less primary.** This is the Redis behavior,
  and CLIENT UNBLOCK / CLIENT KILL are the operator escape hatches — but any internal tooling that
  issued `WAIT n 0` expecting an instant probe must use `timeout > 0` (one integration test needed
  exactly this fix). Grep found no other `WAIT * 0` callers.
- **GETACK for large replica fleets.** One solicitation per blocking WAIT is Redis-equivalent, but
  Redis coalesces multiple WAITs arriving in the same event-loop iteration into one GETACK
  (`beforeSleep` flag). FrogDB sends one per blocking WAIT invocation. Under heavy concurrent WAIT
  load this is more stream traffic (each GETACK is ~40 bytes and advances the offset). If it shows
  up, the fix is a debounce inside `request_acks` — one place.
- **CLIENT UNBLOCK TIMEOUT mode replies with the count, not a nil** — correct for WAIT (its
  "timeout reply" *is* the count), pinned by test; noted because every other blocking command maps
  TIMEOUT mode to a nil shape via `BlockingOp::timeout_reply`, and WAIT's absence from that enum is
  now load-bearing.
- **Per-client `woff`.** The conservative global-offset divergence is the one most likely to be
  revisited (a busy primary makes every WAIT wait for *everyone's* writes). Tracking a
  per-connection last-write offset would land in the post-execution pipeline and thread through to
  `handle_wait_command`; the coordinator API already takes `target` as a parameter, so it would be
  a call-site change only.
