# Architecture Proposals

Deepening refactors surfaced by an architecture review on 2026-06-12. Each proposal turns a shallow
module into a deep one: more behaviour behind a smaller interface, with locality (bugs and change
concentrate in one place) and leverage (callers and tests get more per unit of interface) as the
payoff. All evidence verified against the code at time of writing.

Ordered by leverage:

1. [01-declarative-command-spec.md](01-declarative-command-spec.md) — Replace opt-in `Command`
   trait methods (`keys`, `keyspace_event_type`, `wakes_waiters`) with a declarative spec the
   dispatcher derives behavior from. 255 hand-rolled `keys()` impls; missing-impl bugs found:
   LREM keyspace event, RPOPLPUSH/LMOVE waiter wake, GEORADIUS STORE key extraction.
2. [02-typed-store-access.md](02-typed-store-access.md) — Typed store accessors
   (`get_list_mut(key) -> Result<Option<&mut _>, WrongTypeError>`) so the WrongType invariant
   lives in one module. Kills 26 panic-prone `.unwrap().as_*_mut().unwrap()` chains and 205
   hand-rolled WrongType checks.
3. [03-unified-post-execution-pipeline.md](03-unified-post-execution-pipeline.md) — Collapse the
   four near-identical post-execution functions in `core/src/shard/pipeline.rs` into one module
   owning write-effect ordering. Drift already present: transaction path skips keyspace metrics
   and keysizes flush; scatter BCAST invalidation omitted.
4. [04-connection-state-encapsulation.md](04-connection-state-encapsulation.md) — Make
   `ConnectionState` own its transitions as methods; ~80 pub-field reach-ins across 10 files,
   asking flag set in `dispatch.rs` but cleared in `guards.rs`, MULTI five-field reset ritual
   duplicated 4×.
5. [05-single-routing-decision.md](05-single-routing-decision.md) — Delete the dead
   `CommandRouter::route`/`op_to_handler` twin (zero production callers) and move the live
   `refine_handler` mapping into the routing module as a pure, table-testable function.
6. [06-recovery-orchestrator.md](06-recovery-orchestrator.md) — One recovery module with a single
   seam (`recover(inputs) -> RecoveredState`) over six ordered phases, replacing startup logic
   smeared across four sites in three crates.

## Correctness flags found during the review

Bugs adjacent to (but separable from) the proposals:

- **Replication offset never persisted after startup** — `replication/src/tracker.rs:129-140`
  updates only an `AtomicU64`; `ReplicationState::save()` has no callers post-init. Crash rewinds
  the offset to the boot value. Staged `replication_metadata.json` (written by
  `replica/connection.rs:302`) has no reader anywhere.
- **Shard-count mismatch silently drops recovered data** — `server/src/server/shards.rs:60` uses
  `unwrap_or_default()` on the recovered-store iterator (see proposal 06).
- **Missing command behaviors** — LREM emits no keyspace event; RPOPLPUSH/LMOVE never wake blocked
  list waiters; GEORADIUS STORE destination key not extracted (see proposal 01).
- **Post-execution drift** — transaction path skips keyspace metrics and keysizes flush; scatter
  BCAST tracking invalidation omitted (see proposal 03).
- **Checkpoint staging untested** — `persistence/src/rocks/checkpoint.rs` (renames a live db
  directory) has zero tests (see proposal 06).
- **STORE-destination WAL gap** — WAL persistence is driven by `wal_strategy().actions()`
  (arg-index based), so GEORADIUS STORE and SORT…STORE destinations are never WAL-persisted even
  with `keys()` fixed. Needs the dynamic-destination escape hatch from proposal 01.
- **Cross-shard keyspace notifications lost** — SUBSCRIBE registers on shard 0 (broadcast
  coordinator) but keyspace events emit on the key-owner shard; in multi-shard mode a keyevent for
  a key not on shard 0 never reaches the subscriber.
