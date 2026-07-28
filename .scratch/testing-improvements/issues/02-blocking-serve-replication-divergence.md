# Served blocking pops (BLPOP/BRPOP/BLMOVE/...) never replicate — replica silently diverges

Status: done
Type: bug
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 3/3 (score 9)
Area: replication

## Context

When a blocking command (e.g. `BLPOP`) is served by a subsequent write from another client (e.g.
`LPUSH`), the store mutation for the *served pop* happens inside `blocking.rs` with no replication
broadcast at all (`core/src/shard/blocking.rs:291-320`). In the write-effect ordering,
`WaiterSatisfaction` is index 4 of `WRITE_EFFECT_ORDER` while `ReplicationBroadcast` is index 8
(`core/src/shard/post_execution.rs:238-248`) — but the only broadcast that actually fires is for the
*waking* write (the `LPUSH`), not for the pop that consumed it. There is no broadcast call anywhere
in `blocking.rs` or `dispatch_blocking.rs`.

Concretely: replica + a client blocked on `BLPOP k 0`; another client does `LPUSH k v`. On the
primary, the blocking client immediately consumes `v` and the key ends up empty. The broadcast that
reaches the replica is just `LPUSH k v` — the replica never learns the value was popped, so it
retains `v` in the list indefinitely. This is the same root cause as issue 01 (verbatim
propagation, no rewrite/serve-aware broadcast) applied to the blocking-serve path specifically, and
is a distinct, confirmed live bug: `grep BLPOP|BRPOP|BLMOVE` in `integration_replication.rs`
returns zero matches — there is no blocking replication test of any kind. Redis propagates the
serving pop (rewritten, deterministically) rather than relying on the replica to independently
satisfy its own waiters.

## What to build

- Fix: when a blocking waiter is served, broadcast the equivalent pop mutation (e.g. `LPOP k` /
  `RPOP k`) alongside or instead of relying on the replica's own blocking machinery to reproduce the
  same outcome from the waking write.
- Test: replica + client blocked on `BLPOP k 0`; `LPUSH k v` from another client; `WAIT`; assert
  `LRANGE k 0 -1` is empty on the replica (matching the primary).
- Repeat for `BRPOPLPUSH`/`BLMOVE` (both source removal and destination-side replication must
  match), `BZPOPMIN`, and `XREAD BLOCK` (consumer position/ack state must match).

## Acceptance criteria

- [ ] Served blocking pops broadcast their mutation to replicas so primary and replica state match
      after `WAIT`, for `BLPOP`/`BRPOP`, `BLMOVE`/`BRPOPLPUSH` (source + destination), `BZPOPMIN`/
      `BZPOPMAX`, and `XREAD BLOCK`
- [ ] Integration test per blocking command family asserting replica state equals primary post-serve
- [ ] No regression to blocking wake-order/exactly-once semantics on the primary (existing
      `concurrency_workload.rs` suites stay green)

## Blocked by

None - can start immediately. Shares root cause and likely fix shape with issue 01
(`01-spop-replication-divergence.md`) — consider designing the rewrite/broadcast mechanism jointly.

## References

- `core/src/shard/blocking.rs:291-320` — served-pop mutation with no broadcast
- `core/src/shard/post_execution.rs:238-248` — WRITE_EFFECT_ORDER (WaiterSatisfaction idx4,
  ReplicationBroadcast idx8)
- `server/tests/integration_replication.rs` — zero BLPOP/BRPOP/BLMOVE tests (grep confirmed)
- `server/tests/slowlog_tcl.rs:16` — corroborating doc comment ("blocked BLPOP->LPOP" not implemented)
- Source: `.scratch/testing-improvements/audit/E-replication.md` gap #2, `.scratch/testing-improvements/audit/verdicts-E.md` #2

## Resolution

Fixed by synthesizing the served pop as a deterministic command and broadcasting it **after** the
waking write, in the same effect batch, reusing the issue-01 `SynthesizedCommand` /
`replication_forms` seam.

### Mechanism

The store mutation for a served waiter runs at `WaiterSatisfaction` (WRITE_EFFECT_ORDER idx 4); the
broadcast runs later at `ReplicationBroadcast` (idx 8). To carry the synthesized pop across the two
effect-loop iterations, the satisfaction driver records it in a new worker field and the broadcast
arm drains it:

- `core/src/shard/blocking.rs` — `Satisfaction::Done` gained a `propagate: Option<SynthesizedCommand>`
  field. Each served op sets it to its deterministic equivalent:
  - `BLPOP` -> `LPOP <key>`, `BRPOP` -> `RPOP <key>`
  - `BLMOVE`/`BRPOPLPUSH` -> `LMOVE <src> <dst> <srcdir> <dstdir>` (covers **both** keys atomically)
  - `BLMPOP` -> `LPOP`/`RPOP <key> <actual-count>` (uses the real popped count, not the request)
  - `BZPOPMIN` -> `ZPOPMIN <key>`, `BZPOPMAX` -> `ZPOPMAX <key>`
  - `BZMPOP` -> `ZPOPMIN`/`ZPOPMAX <key> <actual-count>`
  The BLMOVE wake-cascade pushes the parent's `LMOVE` before recursing, so replicas apply moves in
  the primary's order.
- `core/src/shard/worker.rs` — new `pending_serve_propagations: Vec<SynthesizedCommand>` accumulator
  (constructed in `builder.rs`).
- `core/src/shard/post_execution.rs` — the `ReplicationBroadcast` arm `mem::take`s the buffer
  (unconditionally, so it never leaks) and, when the broadcast is not gated off, ships each pop
  **after** the waking write's frame(s), inheriting the same replica-loop-guard
  (`REPLICA_INTERNAL_CONN_ID`) and `replicate`/`is_active` gate.

Because a replica is read-only (BLPOP is a WRITE, rejected there), it has no blocked poppers of its
own: re-executing the waking push and then the synthesized pop converges it exactly with the primary.

### Known gap (deferred)

`XREAD`/`XREADGROUP BLOCK` serve replication is **not** covered (`propagate: None`, documented in
`blocking.rs`). Stream consumer-group serve state (PEL/last-delivered) is a verbatim-propagating
write needing a larger, separate mechanism (Redis uses XCLAIM-style propagation) — left to a
dedicated task. This narrows acceptance-criterion coverage for `XREAD BLOCK` only.

### Tests

- Unit (store-level + driver-level), `core/src/shard/blocking.rs`: per-op `propagate` synthesis
  incl. actual-count partial drains, direction args, cascade ordering, and no-propagation for
  doomed waiters and stream reads.
- Unit (pipeline), `core/src/shard/post_execution.rs`: `served_blpop_broadcasts_push_then_pop`
  (order pinned via `RecordingBroadcaster`) and `served_pop_respects_replica_loop_guard_and_drains`.
- Integration (primary+replica), `server/tests/integration_replication.rs`: `test_served_blpop_*`,
  `test_served_bzpopmin_*`, `test_served_blmove_replicates_both_keys`, `test_served_blmpop_*`, and
  `test_served_blpop_multi_waiter_replicates` — each in-memory and with-persistence.
- Property (turmoil), `server/tests/simulation.rs`:
  `test_blocking_serve_replication_convergence_random_workload` — concurrent BLPOP/BRPOP/BLMOVE
  poppers racing a random push workload across seeds; quiescent per-list primary==replica equality.
