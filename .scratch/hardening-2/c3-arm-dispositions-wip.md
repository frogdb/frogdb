# C3 arm dispositions — preliminary investigation (decision 4)

Status: WIP, reading-only findings. Forcing tests NOT yet written. Dispositions below are
**proposed**, pending the tests decision-4 requires before the C3 lint lands.

Ruling (PRD §8.4): investigate-and-propose. Write forcing tests for both arms, record proposed
dispositions with evidence, present for approval before the C3 lint lands. C2/C5/C6/C7 are not
blocked on this.

## The gate

`ShardWorker::can_execute_during_lock(conn_id)` (`core/src/shard/worker.rs:854-862`):

```rust
pub(crate) fn can_execute_during_lock(&self, conn_id: u64) -> Result<(), Response> {
    if let Some(owner) = self.vll.continuation_lock_owner()
        && owner != conn_id
    {
        return Err(Response::error("ERR shard busy with continuation lock"));
    }
    Ok(())
}
```

Rejects work from any connection that is not the continuation-lock owner, while a lock is held.
`execute_scatter_part` does NOT call it internally — every caller must gate first (the
`ScatterRequest` arm at `dispatch_core.rs:49` does; issue 50's `ExecTransaction` and issue 05's
`FunctionCall` do not — those are the two real defects, fixed in wave 2).

## Arm 1 — `VllMsg::VllExecute` → `handle_vll_execute` (`vll.rs:40-55`)

```rust
let result = self.execute_scatter_part(&op.keys, &op.operation, 0).await;  // conn_id hardcoded 0
```

Established by reading:
- The op is produced by `self.vll.dequeue_for_execution(txid)` — it is only dequeued once its VLL
  locks are held (two-phase). VLL lock acquisition and the continuation lock are the **same
  subsystem** (`self.vll`): `continuation_lock_owner()`, `request_continuation_lock()` and the
  op queue all live on the one `vll` object.
- `handle_vll_lock_request` enqueues via `enqueue_lock_request`; the continuation-lock handler
  (`vll.rs:70-78`) parks and is granted *from the drain point*, i.e. the continuation lock is
  itself ordered through the same queue that gates `VllExecute`.

**Proposed disposition: EXEMPT** — the VLL two-phase protocol is the isolation mechanism for a
dequeued op; `conn_id = 0` is a drain-path sentinel, not a real connection, and re-checking the
continuation lock here would be redundant with the queue ordering that granted the op in the first
place.

**Open question the forcing test MUST answer** (do not lock this in without it): does
`dequeue_for_execution` / the VLL queue actually serialize a held continuation lock against other
txns' `VllExecute` drains? If a cross-shard script holds the continuation lock and a *different*
connection's VLL op can still dequeue and mutate `op.keys`, then this arm is issue-50's bug class,
NOT exempt. The test: cross-shard script takes the continuation lock; a second connection's VLL
scatter op targets a held key; assert it does not execute until the lock releases. Needs reading
the `vll` coordinator's `dequeue_for_execution` ordering vs `continuation_lock_owner` — not yet
done.

## Arm 2 — `CoreMsg::GetVersion` (`dispatch_core.rs:56-93`)

Not read-only. Calls `purge_if_expired(key)` (`:76`, physically removes already-expired keys) and
`apply_lazy_purge_effects_no_version_bump()` (`:86`, fires tracking invalidation, search-index
deletion, `expired` keyspace notification, XREADGROUP drain). No gate call.

Argument for EXEMPT: it only removes keys already past their TTL — keys the continuation-lock
owner would itself observe as expired — and it withholds the shard-version bump (WATCH F3
semantics: an already-stale watch is a nonexistent watch). No *semantic* write.

Argument for GATE: it fires client-visible effects (notifications, invalidations, index mutations)
mid-lock, and it physically mutates the store while another connection believes it holds the shard
exclusively.

**Proposed disposition: EXEMPT with a documented reason**, on the grounds that lazy expiry of
already-dead keys is not a write the lock protects against (Redis fires `expired` on any
triggering command regardless), and the no-bump rule keeps it invisible to the lock owner's WATCH
set. **But** the forcing test must confirm the fired effects do not violate the lock owner's
isolation — specifically that the lock owner does not observe a key it holds being purged out from
under a live (unexpired) view. If a not-yet-expired key can be purged here, that flips to GATE.

## Next steps (for the C3 work agent)

1. Read `frogdb-server/crates/core/src/vll/*` — `dequeue_for_execution`, queue ordering vs
   `continuation_lock_owner`, to settle arm 1's open question.
2. Write the two forcing tests above (compile-gated; LOCAL mode, `just test frogdb-core <pattern>`).
3. Record final dispositions in `GATED_ARMS` / `EXEMPT_ARMS{reason}` classification and present
   for user approval BEFORE landing the C3 lint (`no direct can_execute_during_lock outside the
   `execute_gated` wrapper` per issue 05's preferred fix).
