# Spec: architecture/vll.md
Status: update
Audiences: A2, A3, A5

Goal: A reader walks away understanding FrogDB's VLL (Very Lightweight Locking)
mechanism and *why* it was chosen for multi-shard atomicity: instead of global
mutexes, 2PC, or optimistic concurrency, each operation declares the keys it will
touch as intents in a per-shard intent table, a global monotonically increasing
transaction id gives a total order that makes deadlock impossible (locks are
acquired in sorted shard order), and Selective Contention Analysis (SCA) lets
non-conflicting operations execute out of order — only operations whose intents
actually conflict (an exclusive lock overlapping another lock on the same key)
wait. The reader understands that VLL provides *execution* atomicity (no
interleaving, globally ordered) but not *failure* atomicity (no rollback; partial
writes persist on mid-execution failure), matching Redis/Dragonfly semantics, and
how continuation locks give Lua/MULTI whole-shard exclusive access when key
access can't be declared upfront.

Not in scope: The scatter-gather sequence and channel plumbing (request-flows.md
§3, concurrency.md); the general shard model (concurrency.md). Link outward.

Sources of truth (author must read and reconcile every claim):
- `frogdb-server/crates/vll/src/lib.rs` — module overview; confirms the design
  names, including **SCA (Selective Contention Analysis)**.
- `frogdb-server/crates/vll/src/intent_table.rs` — `IntentTable`, `Intent`,
  `LockMode` (Shared/Exclusive); the header explicitly ties the intent table to
  SCA.
- `frogdb-server/crates/vll/src/lock_state.rs` — conflict detection / retry.
- `frogdb-server/crates/vll/src/queue.rs` — per-shard ordered transaction queue
  (verify the real structure vs the page's illustrative `ShardTransactionQueue`).
- `frogdb-server/crates/vll/src/coordinator.rs` — `VllCoordinator`,
  `ScatterRequest`/`ScatterOutcome`/`ScatterError`, `ContinuationGuard` /
  `ContinuationError` (continuation locks), sink/metrics abstraction.
- `frogdb-server/crates/vll/src/types.rs` — `ShardReadyResult`, `ExecuteSignal`,
  txid type, lock/result types.
- `frogdb-server/crates/core/src/shard/message.rs` — the VLL `ShardMessage`
  variants (`VllLockRequest`, `VllExecute`, `VllAbort`, `VllContinuationLock`)
  and their real field lists.
- `frogdb-server/crates/config/src/` — real defaults for
  `scatter-gather-timeout-ms`, `vll-max-queue-depth`, and the rejection metric
  name (`frogdb_vll_queue_rejections_total`).

Existing content: `website/src/content/docs/architecture/vll.md`. Good design
narrative, but it never names Selective Contention Analysis (the crate's central
concept) and its code samples are illustrative approximations.

Structure (keep existing H2s, plus SCA):
- ## Overview — intent-based locking, out-of-order execution, txid ordering,
  shard locks only for conflicts. **Introduce the term SCA here** and state that
  the intent table exists to support it.
- ## Design Goals — concurrency for non-conflicting ops, atomic multi-key
  semantics, deadlock-free ordering, low single-shard latency.
- ## VLL Use Cases — the operation table (multi-key, MULTI/EXEC, Lua, multi-key
  blocking, set/zset/list cross-key ops); standalone-cross-slot vs cluster
  `-CROSSSLOT`. Verify `allow-cross-slot-standalone` config name against source.
- ## How VLL Works — Intent Table (`IntentTable`, `Intent`, `LockMode`);
  Transaction Flow (acquire txid → register intents in sorted order → wait ready
  → execute → release); Deadlock Prevention (sorted order + total txid order);
  **Selective Contention Analysis** (new/expanded subsection): describe how
  conflicts are detected from intents so non-conflicting operations skip the
  queue, sourced from `intent_table.rs` / `lock_state.rs`.
- ## Atomicity Semantics — execution vs failure atomicity table; the "no
  rollback, partial state persists" behavior. Keep.
- ## Why VLL vs Traditional Approaches — mutex/2PC/OCC drawbacks. Keep.
- ## VLL Queue Configuration — `scatter-gather-timeout-ms`, `vll-max-queue-depth`,
  overflow error + metric. Verify names/defaults.
- ## Continuation Locks (Lua/MULTI) — whole-shard exclusive access;
  `VllContinuationLock` / `ContinuationGuard`. Verify field list.

Generated data: None. Config defaults and the metric name should track the
config/telemetry crates (future S3/S6 tie-in), not be independently hardcoded.

Drift guards:
- **Name SCA (Selective Contention Analysis).** The crate is explicitly built
  around SCA (`vll/src/lib.rs`, `intent_table.rs`), yet the page never uses the
  term. Add it and explain it; this is the mechanism behind the page's existing
  "out-of-order execution" prose. Source the explanation from the crate, not
  from the paper alone.
- **Illustrative code must match real types.** `IntentTable`, `Intent`,
  `LockMode`, and the per-shard queue in the page are approximations. Reconcile
  field names and the queue structure with `intent_table.rs` / `queue.rs` /
  `coordinator.rs`; label anything simplified as illustrative and point to the
  crate.
- **ShardMessage VLL variants.** `VllLockRequest`, `VllExecute`, `VllAbort`,
  `VllContinuationLock` and their fields must match `core/src/shard/message.rs`
  (e.g. confirm whether `VllLockRequest` carries `modes: Vec<LockMode>` and
  `execute_rx`, and `VllExecute` carries a `VllCommand`).
- **Config names/defaults + metric.** Verify `scatter-gather-timeout-ms` (5000),
  `vll-max-queue-depth` (10000), `allow-cross-slot-standalone`, and
  `frogdb_vll_queue_rejections_total` against the config/telemetry crates.
- **Atomicity claims.** The execution-vs-failure atomicity distinction and the
  "no rollback / partial state" rows must match the real coordinator behavior on
  timeout and mid-execution failure; verify against `coordinator.rs`.
- **No unbacked numbers.** No latency/throughput figures without a benchmark
  citation (PLAN §6).
- **S7 code-path check.** Any `crates/...` path named must exist; include this
  page in the S7 scan.
