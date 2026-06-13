# Architecture Proposals

Deepening refactors surfaced by an architecture review on 2026-06-12. Each proposal turns a shallow
module into a deep one: more behaviour behind a smaller interface, with locality (bugs and change
concentrate in one place) and leverage (callers and tests get more per unit of interface) as the
payoff. All evidence verified against the code at time of writing.

Ordered by leverage:

1. [01-declarative-command-spec.md](01-declarative-command-spec.md) — **Implemented**
   (`8d573510`…`182898b6`, 13 commits): 377/377 commands declare a `CommandSpec`; dispatcher
   derives keys/events/waking/WAL; legacy opt-in methods deleted; registry-wide exhaustiveness
   tests. Found+fixed along the way: MSETEX/BITOP/XGROUP/XREADGROUP WAL bugs, ZINCRBY wake,
   STORE-destination WAL gap (`WalStrategy::Dynamic`).
2. [02-typed-store-access.md](02-typed-store-access.md) — **Implemented** (`e792ed42`,
   `a1a702ca`, `760944d1`, `b8db0087`): `StoreTypedExt` owns the WrongType invariant; unwrap
   chains in commands now 0, enforced by the `lint-no-typed-unwrap` gate (Justfile + lefthook);
   triplicated `get_or_create` + five utils wrappers deleted. Follow-up candidate: extend
   `typed_family_accessors!` to the probabilistic/extension families (tdigest, cuckoo, bloom,
   topk, cms, HLL, timeseries, vectorset, JSON) to retire ~128 remaining
   `as_X().ok_or(WrongType)` read-path sites.
3. [03-unified-post-execution-pipeline.md](03-unified-post-execution-pipeline.md) —
   **Implemented** (`0db27d82`, `bfe61d2e`): `shard/post_execution.rs` owns the canonical
   9-step `WRITE_EFFECT_ORDER`; `WalPhase`/`EffectScope` are data; `pipeline.rs` deleted; each
   effect step invoked from exactly one site; order-invariant regression tests added. (Drift was
   pre-fixed in `6e483280`/`23469adc`.)
4. [04-connection-state-encapsulation.md](04-connection-state-encapsulation.md) —
   **Implemented** (`694ab867`…`470004e7`, 5 commits): `ConnectionState` owns all transitions as
   methods; six state-machine fields now private; external field reach-ins 0 (40+31+6+3+3+3 → 0,
   enforced by the compiler); MULTI five-field reset and the subscribe-limit blocks collapsed to
   one method each; ASKING/LocalServe nuance preserved exactly; 12 socket-free state-machine
   tests. Phase 6 (tracking/auth/blocked fields) intentionally deferred.
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
- **INFO replication `master_replid` reports zeros** — ~~built from `ctx.node_id` instead of the
  real replication id~~ Fixed in `99c8f91e` (`handle_info` patches from the shared
  `ReplicationState`). `master_replid2`/`second_repl_offset` remain `0`/`-1` — no
  failover-continuity (replid2) concept yet; needed only when partial resync across failover
  becomes possible (see backlog flag above).
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
- **Checkpoint staging untested** — ~~zero tests~~ Fixed in `165fc950` (8 tests: happy path,
  crash windows, idempotency, partial states). Testing found a real bug, fixed in `3e37ad7b`:
  an incomplete staged dir (no `CURRENT`) was installed anyway — live DB moved aside, fresh
  empty DB created in its place. Install now validates before touching the live dir.
- **STORE-destination WAL gap** — ~~arg-index WAL actions miss GEORADIUS/SORT STORE
  destinations~~ Fixed in `fc0dce48` (`WalStrategy::Dynamic` resolves from the command's own key
  extraction; restart-survival tests). Same-class WAL bugs also fixed: MSETEX, BITOP, XGROUP,
  XREADGROUP persisted a non-key arg or nothing. ZINCRBY waiter wake fixed in `182898b6`.
- **Missing command behaviors — class closed** — proposal 01 implemented (13 commits,
  `8d573510`…`182898b6`): 377/377 commands declare a `CommandSpec`; registry-wide exhaustiveness
  tests make silent event/WAL/wake omissions unrepresentable.
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
