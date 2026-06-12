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

- **Replication offset never persisted after startup** — ~~tracker only updates an `AtomicU64`;
  staged `replication_metadata.json` has no reader~~ Fixed in `17f01c9d` (primary saves at
  shutdown + pre-snapshot hook; replica reconciles from staged metadata; corrupt/missing →
  full resync).
- **Partial resync never granted; checkpoints record offset 0** — ~~stale
  `state.replication_offset` read by FULLRESYNC/`can_partial_sync`~~ Fixed in `64f15bce`
  (offset captured from tracker before checkpoint cut; offset ≤ data invariant).
- **No replication backlog — partial resync structurally ungrantable** — `+CONTINUE` would tail
  the live broadcast only, silently dropping `(requested, current]`. Gated explicitly behind
  `partial_sync_replay_supported()` (false). Implementing a backlog ring buffer (Redis
  `repl-backlog`) is the unlock; the split-brain `ReplicationRingBuffer` is a separate mechanism.
- **INFO replication `master_replid` reports zeros** — built from `ctx.node_id` (`{:040x}`)
  instead of the real `ReplicationState::replication_id` used by PSYNC/FULLRESYNC
  (`server/src/commands/info.rs:458`); non-cluster primaries report all-zeros.
- **Shard-count mismatch silently drops recovered data** — ~~`server/src/server/shards.rs:60`
  uses `unwrap_or_default()`~~ Fixed in `95da0256` (hard startup error at `RocksStore::open`,
  persisted count derived from `shard_*` column families).
- **Warm-tier toggle breaks reopen** — data dir created with warm tier enabled fails to reopen
  with it disabled (`tiered_warm_*` CFs not reopened → RocksDB "column families not opened").
  Same must-open-all-CFs constraint as the shard-count case; needs a decision on whether config
  toggling is supported.
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
- **Keyspace hit/miss misclassification** — ~~`track_keyspace_metrics` classifies via
  `Response::Null`, but GET/HGET misses return `Response::Bulk(None)`~~ Fixed in `23469adc`
  (lookup-level classification via `CommandContext::record_keyspace_lookup`).
- **INFO stats hardcodes `keyspace_hits:0` / `keyspace_misses:0`** — ~~never wired to the real
  counters~~ Fixed in `02de350b` (`handle_info` patches from the Prometheus registry).
- **CONFIG RESETSTAT doesn't reset keyspace_hits/misses** — Redis zeroes them; FrogDB's are
  Prometheus monotonic counters (reset would break `rate()`/`increase()` semantics). Needs a
  design decision (baseline-offset vs registry recreation) — deliberate divergence for now.
- **Keyspace-stats command coverage gap** — only GET/HGET/LINDEX/GETDEL/GETEX/MGET report
  lookups; Redis counts most read commands (LRANGE, SMEMBERS, ZRANGE, …). Enhancement, not a bug.
