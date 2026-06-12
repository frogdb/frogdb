# Proposal: Unified Post-Execution Pipeline

Status: proposed
Date: 2026-06-12

## Problem

`crates/core/src/shard/pipeline.rs` contains four near-identical post-execution functions:

| Function | Line | Role |
|---|---|---|
| `run_post_execution` | `pipeline.rs:20` | single command, pipeline persists WAL |
| `run_post_execution_after_wal` | `pipeline.rs:96` | single command, WAL already persisted (rollback mode) |
| `run_transaction_post_execution` | `pipeline.rs:231` | MULTI/EXEC batch, pipeline persists WAL |
| `run_transaction_post_execution_after_wal` | `pipeline.rs:312` | MULTI/EXEC batch, WAL already persisted |

Each one re-states the same sequence of write effects: version increment, client-tracking
invalidation, keyspace event emission, dirty counting, waiter satisfaction, WAL persistence, search
index hook, replication broadcast. **The effect *ordering* is the real invariant** — and it lives in
four copies. Nothing in the type system or module structure says "these four must agree"; agreement
is maintained by hand, comment-numbered steps (`// 2.5.`, `// 4.5.`) and all.

Why ordering is the dangerous part:

- **Waiter satisfied before WAL persisted.** A blocked `BLPOP` client observes a push that the WAL
  never recorded. If one clone reorders steps 4 and 5, a crash window appears where a client acted
  on data that does not survive restart. Today the only thing preventing this is that four separate
  functions happen to list the steps in the same order.
- **Stale tracking invalidation.** If invalidation runs after replication broadcast in one clone but
  before it in another, RESP3 clients caching keys see invalidation messages race differently on the
  rollback path vs the default path — a heisenbug keyed on whether `wal_rollback` is enabled.
- **Replica sees a write the WAL rejected.** The whole point of the `_after_wal` split is that
  rollback mode must not broadcast or satisfy waiters until the WAL confirms. That contract is
  enforced by *which clone the caller picks* (`execution.rs:160-210`), not by the pipeline itself.

The clones have **already drifted** — there are divergences that look accidental, not designed
(detailed in the table below):

- Keyspace hit/miss metrics (`TRACKS_KEYSPACE`) run only in the single-command clones. Commands
  inside MULTI/EXEC never count toward `frogdb_keyspace_hits_total`/`misses_total`. Redis does count
  them.
- `store.flush_keysizes_refreshes()` after waiter satisfaction (`pipeline.rs:75`, `pipeline.rs:150`)
  exists only in the single-command clones. Waiter satisfaction inside a transaction mutates via
  `get_mut` the same way, but the histogram flush is missing from both transaction clones.
- Version increment is conditional on `dirty_delta >= 0` in the single-command clones
  (`pipeline.rs:44`, `pipeline.rs:119`, with a comment explaining the Redis WATCH no-op rule) but
  unconditional in both transaction clones (`pipeline.rs:242`, `pipeline.rs:323`).
- The FLUSHDB/FLUSHALL → `flush_all_tracking()` special case exists only in the transaction clones
  (`pipeline.rs:248-252`, `pipeline.rs:327-331`).

Related smear: client-tracking logic has no single home. The data structures live in
`crates/core/src/tracking.rs`, the shard-facing wrapper in `crates/core/src/shard/types.rs:140-178`
(`ShardTracking`), read-side recording inline in `crates/core/src/shard/execution.rs:109-114`, and
write-side invalidation in all four pipeline clones. The scatter paths hand-roll it a fifth way —
and have drifted too: `scatter_mset` (`execution.rs:590-593`) and `scatter_del`
(`execution.rs:637-640`) call `tracking.invalidate_keys()` but skip
`broadcast_table.invalidate_matching()`, so BCAST-mode clients never hear about cross-shard
MSET/DEL writes.

In module-design terms, `pipeline.rs` exports four **shallow** entry points whose names encode
caller state (`_after_wal`, `_transaction_`) instead of one **deep** module that owns the invariant.
The interface forces every caller to know the internal step list well enough to pick the right
clone, and every change to the effect order costs four edits (poor **locality**, no **leverage**).

## Current state

Effect steps × the four clones. Line numbers are `crates/core/src/shard/pipeline.rs`.

| # | Effect step | `run_post_execution` (20) | `_after_wal` (96) | `run_transaction_…` (231) | `…_after_wal` (312) |
|---|---|---|---|---|---|
| 1 | Keyspace hit/miss metrics | yes (31-33) | yes (107-109) | **missing** | **missing** |
| 2 | Version increment | if `dirty_delta >= 0` (44-46) | if `dirty_delta >= 0` (119-121) | unconditional, once per txn (242) | unconditional, once per txn (323) |
| 3 | Tracking invalidation | per-command keys (49-63) | per-command keys (124-138) | batched keys + FLUSH special case (247-271) | batched keys + FLUSH special case (326-350) |
| 4 | Keyspace notifications | single (66) | single (141) | loop (274-276) | loop (353-355) |
| 5 | Dirty counter | per-command delta (69) | per-command delta (144) | accumulated total (279) | accumulated total (358) |
| 6 | Waiter satisfaction | single (72) | single (147) | loop (282-284) | loop (361-363) |
| 7 | Keysizes histogram flush | yes (75) | yes (150) | **missing** | **missing** |
| 8 | WAL persistence | `persist_by_strategy` (78) | **skipped** (152) | loop `persist_by_strategy` (287-289) | **skipped** (365) |
| 9 | Search index hook | single (81-83) | single (155-157) | loop (292-296) | loop (368-372) |
| 10 | Replication broadcast | `broadcast_command` (86-89) | `broadcast_command` (160-163) | `broadcast_transaction` (299-306) | `broadcast_transaction` (375-382) |

Three axes of variation hide in those columns: **WAL phase** (persist here vs already persisted),
**scope** (one command vs a batch), and a residue of **accidental drift** (rows 1, 2, 7). Only the
first two are real; the third is the cost of maintaining four copies.

The duplication in the flesh — the tracking-invalidation block, verbatim in clones 1 and 2
(`pipeline.rs:49-63` and `pipeline.rs:124-138`):

```rust
// 2.5. Client tracking: invalidate written keys
if self.tracking.has_tracking_clients() || !self.tracking.broadcast_table.is_empty() {
    let keys = handler.keys(args);
    if !keys.is_empty() {
        if self.tracking.has_tracking_clients() {
            self.tracking.invalidate_keys(&keys, conn_id);
        }
        if !self.tracking.broadcast_table.is_empty() {
            self.tracking.broadcast_table.invalidate_matching(
                &keys,
                conn_id,
                &self.tracking.invalidation_registry,
            );
        }
    }
}
```

…and a third near-copy with the FLUSH special case bolted on, verbatim in clones 3 and 4
(`pipeline.rs:247-271` and `pipeline.rs:326-350`, abridged):

```rust
if self.tracking.has_tracking_clients() || !self.tracking.broadcast_table.is_empty() {
    let has_flush = write_infos
        .iter()
        .any(|&(handler, _)| matches!(handler.name(), "FLUSHDB" | "FLUSHALL"));
    if has_flush && self.tracking.has_tracking_clients() {
        self.tracking.flush_all_tracking();
    } else {
        let mut all_keys: Vec<&[u8]> = Vec::new();
        for &(handler, args) in write_infos {
            all_keys.extend(handler.keys(args));
        }
        // ... identical invalidate_keys + invalidate_matching block as above ...
    }
}
```

The `_after_wal` variants differ from their siblings by exactly one line — a comment
(`pipeline.rs:152`, `pipeline.rs:365`):

```rust
// 5. WAL persistence — SKIPPED (already done by persist_and_confirm)
```

A whole duplicated function exists to delete one step.

Call sites (`crates/core/src/shard/execution.rs`):

| Line | Caller context | Clone invoked |
|---|---|---|
| 166 | single write, rollback mode, after `persist_and_confirm` succeeded | `run_post_execution_after_wal` |
| 192 | single write, default path | `run_post_execution` |
| 207 | **read** command — abuses the write pipeline just for step 1 (keyspace metrics) | `run_post_execution` |
| 320 | transaction, rollback mode, after `persist_transaction_to_wal` succeeded | `run_transaction_post_execution_after_wal` |
| 323 | transaction, default path | `run_transaction_post_execution` |

## Proposed design

One **module**, `crates/core/src/shard/post_execution.rs`, that owns the canonical effect order.
Its **interface**: feed it a write summary (the writes, the accumulated dirty delta, the
originating connection) plus the two real axes of variation as data — WAL phase and scope. The four
whole-function clones become two enum parameters of a single path.

```rust
// crates/core/src/shard/post_execution.rs

/// What a (possibly batched) execution wrote. The input to the effect pipeline.
pub(crate) struct WriteSummary<'a> {
    /// Write commands in execution order: (handler, args). One entry for a plain
    /// command; all write commands of the transaction for MULTI/EXEC.
    pub writes: &'a [(&'a dyn Command, &'a [Bytes])],
    /// Accumulated dirty delta. Negative means "no data actually changed"
    /// (e.g. DEL on an expired key) — see the WATCH no-op rule.
    pub dirty_delta: i64,
    pub conn_id: u64,
}

/// Whether the pipeline persists the WAL or the caller already did (rollback mode,
/// via persist_and_confirm / persist_transaction_to_wal).
pub(crate) enum WalPhase {
    Persist,
    AlreadyPersisted,
}

/// Batching scope: per-command effects vs atomic transaction effects.
pub(crate) enum EffectScope {
    Command,
    Transaction,
}

impl ShardWorker {
    /// THE canonical write-effect order. The only statement of the invariant.
    ///
    /// 1. Version increment        (skipped when dirty_delta < 0 — Redis WATCH no-op rule)
    /// 2. Tracking invalidation    (key-based, or flush-all for FLUSHDB/FLUSHALL)
    /// 3. Keyspace notifications   (per write, in execution order)
    /// 4. Dirty counter            (accumulated)
    /// 5. Waiter satisfaction      (per write; may mutate store via get_mut)
    /// 6. Keysizes histogram flush (after waiter mutations)
    /// 7. WAL persistence          (skipped when WalPhase::AlreadyPersisted)
    /// 8. Search index hook        (per write)
    /// 9. Replication broadcast    (per command, or MULTI/EXEC-framed for Transaction)
    pub(crate) async fn run_write_effects(
        &mut self,
        summary: WriteSummary<'_>,
        wal: WalPhase,
        scope: EffectScope,
    ) {
        if summary.writes.is_empty() {
            return;
        }
        if summary.dirty_delta >= 0 {
            self.increment_version();                                    // 1
        }
        self.invalidate_written_keys(summary.writes, summary.conn_id);   // 2
        for &(handler, args) in summary.writes {
            self.emit_keyspace_notifications_for_command(handler, args); // 3
        }
        self.update_dirty_counter(summary.dirty_delta);                  // 4
        for &(handler, args) in summary.writes {
            self.satisfy_waiters_for_command(handler, args);             // 5
        }
        self.store.flush_keysizes_refreshes();                           // 6
        if matches!(wal, WalPhase::Persist) {
            for &(handler, args) in summary.writes {
                self.persist_by_strategy(handler, args).await;           // 7
            }
        }
        if !self.search.indexes.is_empty() {
            for &(handler, args) in summary.writes {
                self.update_search_indexes(handler.name(), args);        // 8
            }
        }
        if summary.conn_id != REPLICA_INTERNAL_CONN_ID
            && self.replication_broadcaster.is_active()
        {
            match scope {                                                // 9
                EffectScope::Command => {
                    let (handler, args) = summary.writes[0];
                    self.replication_broadcaster.broadcast_command(handler.name(), args);
                }
                EffectScope::Transaction => {
                    let commands: Vec<(&str, &[Bytes])> = summary
                        .writes
                        .iter()
                        .map(|&(h, a)| (h.name(), a))
                        .collect();
                    self.replication_broadcaster.broadcast_transaction(&commands);
                }
            }
        }
    }
}
```

Two companion moves:

- **Keyspace hit/miss metrics leave the pipeline.** Step 1 of the old clones is the only effect
  that applies to reads, which is why `execution.rs:207` calls the *write* pipeline for read
  commands. It becomes a one-liner in `execute_command` keyed on `TRACKS_KEYSPACE`, run for every
  command regardless of write status — which also fixes the missing-in-transactions drift.
- **Tracking gets one write-side seam.** `invalidate_written_keys` (a method on `ShardWorker`, or
  better on `ShardTracking` in `shard/types.rs`) absorbs the duplicated invalidation block including
  the FLUSH special case. The scatter paths (`scatter_mset`, `scatter_del`, `scatter_flushdb`) call
  the same method, fixing their missing `broadcast_table` invalidation as a side effect. Read-side
  recording (`execution.rs:109-114`) already goes through `ShardTracking::record_read`; after this,
  every tracking touchpoint outside `tracking.rs` is a `ShardTracking` method call (good
  **locality**).

The four call sites collapse — the rollback-mode choice becomes a value, not a function name:

```rust
// execution.rs — single command (replaces lines 160-210)
let wal = if rollback_mode {
    self.persist_and_confirm(write_meta.handler.as_ref(), &command.args).await?; // rollback on Err
    WalPhase::AlreadyPersisted
} else {
    WalPhase::Persist
};
self.run_write_effects(
    WriteSummary {
        writes: &[(write_meta.handler.as_ref(), command.args.as_slice())],
        dirty_delta: write_meta.dirty_delta,
        conn_id,
    },
    wal,
    EffectScope::Command,
)
.await;

// execution.rs — transaction (replaces lines 294-325)
let wal = if rollback_mode {
    self.persist_transaction_to_wal(&write_infos).await?; // rollback path on Err, as today
    WalPhase::AlreadyPersisted
} else {
    WalPhase::Persist
};
self.run_write_effects(
    WriteSummary { writes: &write_infos, dirty_delta: total_dirty, conn_id },
    wal,
    EffectScope::Transaction,
)
.await;
```

This is a **deep** module: a small interface (one function, two enums, one summary struct) hiding
the entire ordering contract. It passes the **deletion test**: today, deleting one clone silently
deletes behavior on one of four paths and nothing notices until a Jepsen run; after, deleting or
reordering a step is one diff in one function that every path inherits. That is the **leverage** —
a future effect (e.g. AOF-style fsync policy, slot-migration journaling) is added once, in order,
for all four paths. And it is the **locality** — to answer "what happens after a write commits?"
you read one function, not four-plus-scatter.

## Migration plan

Behavior-characterization first, then unify, then delete. No big-bang rewrite.

1. **Characterize the divergences** (no code change). For each row of the table that differs,
   decide intentional vs accidental, and record the verdict in the new module's docs:
   - Single-txn version increment, MULTI/EXEC replication framing, batched invalidation: clearly
     intentional (atomicity toward replicas and WATCH) — these stay as `EffectScope` behavior.
   - Keyspace metrics missing in transactions: accidental. Redis counts hits/misses inside
     MULTI/EXEC. Fix by hoisting metrics out of the pipeline (see design).
   - Keysizes flush missing in transactions: accidental. Waiter satisfaction mutates the store the
     same way in both scopes.
   - Unconditional version increment in transactions: needs a decision — should an all-no-op
     transaction (every write returning `dirty_delta < 0`) dirty WATCH state? Check Redis; if the
     `dirty_delta >= 0` rule applies, `total_dirty` aggregation must distinguish "all no-ops" from
     "mixed". Pin the chosen semantics with a test before unifying.
   - FLUSH special case only in transaction clones: intentional but accidental-looking —
     single-command FLUSHDB routes through `scatter_flushdb` (`execution.rs:645-667`) which calls
     `flush_all_tracking()` itself. Folding the special case into `invalidate_written_keys` makes
     it uniformly correct rather than path-dependent.
2. **Pin current behavior with tests** for the intentional divergences (txn version increment count,
   MULTI/EXEC replication framing, rollback-mode "no effects on WAL failure").
3. **Introduce `post_execution.rs`** with `run_write_effects` + `invalidate_written_keys`. Port the
   default single-command call site (`execution.rs:192`) first; run the full suite.
4. **Port the remaining three call sites** (`execution.rs:166`, `320`, `323`) and the read-metrics
   hoist (`execution.rs:201-210`).
5. **Delete all four clones** from `pipeline.rs`. The private helpers (`track_keyspace_metrics`,
   `update_dirty_counter`, `satisfy_waiters_for_command`, `persist_by_strategy`) move into the new
   module. `pipeline.rs` itself should pass the deletion test: nothing left, file removed.
6. **Converge the scatter paths** onto `invalidate_written_keys`, fixing the `broadcast_table`
   omission in `scatter_mset`/`scatter_del` (flag in release notes: BCAST clients start receiving
   invalidations for cross-shard MSET/DEL that they previously, incorrectly, did not).

## Testing impact

The pipeline becomes testable **as a module** instead of only end-to-end:

- **Effect-order regression test.** Behind `#[cfg(test)]` (or a `test-util` feature), record each
  step into an effect log (`Vec<EffectKind>` on `ShardWorker` or via a test observer). Feed a
  `WriteSummary` for a stub write command, assert the log equals the canonical order, for all four
  `(WalPhase, EffectScope)` combinations. This turns the ordering invariant from a comment into a
  failing test.
- **Targeted ordering properties.** Assert waiter satisfaction never precedes WAL persistence on
  the `WalPhase::Persist` path; assert no broadcast/waiter/tracking effect fires when the
  rollback-mode caller's WAL write fails (effects gated behind `persist_and_confirm` success).
- **Divergence-fix tests** (new behavior): `INFO`-visible keyspace hits/misses increment inside
  MULTI/EXEC; keysizes histogram reflects waiter-driven mutations inside a transaction; BCAST
  tracking invalidation fires for scatter MSET/DEL.
- **Existing coverage carries over.** Integration tests for keyspace notifications, client
  tracking, blocking commands, MULTI/EXEC replication, and the Jepsen suites exercise all four
  paths; they are the safety net for steps 3-5 of the migration.

## Risks / open questions

- **Async boundary.** `persist_by_strategy` is the only `.await` in the pipeline
  (`pipeline.rs:385-396`). The unified function keeps WAL as the only suspension point; effects
  before it run synchronously on the shard task. Care: do not "optimize" the WAL loop into
  concurrent futures — per-command WAL order is part of the replay contract.
- **Intentional vs accidental divergence calls.** The migration plan's step 1 verdicts (especially
  the transaction version-increment condition) need confirmation against Redis/Valkey behavior
  before unification; pinning the wrong semantics would launder a bug into the canonical order.
- **Transaction batching semantics.** Single version increment and `broadcast_transaction` framing
  are scope-level, not per-write — the design keeps them under `EffectScope`, but any future
  per-write effect must decide which scope behavior it wants. The enum makes that decision explicit
  at review time.
- **Rollback seam still leaks one step.** `WalPhase::AlreadyPersisted` relies on the caller having
  run `persist_and_confirm`/`persist_transaction_to_wal` first (`shard/persistence.rs:89,112`). The
  ordering responsibility for "WAL before all other effects" remains split across the seam in
  rollback mode. Acceptable for now (the rollback decision needs the snapshot and response, which
  live in `execution.rs`), but a follow-up could fold confirm-or-rollback into the module as a
  third `WalPhase` variant carrying the snapshot.
- **`writes[0]` for `EffectScope::Command`.** The summary-of-one representation makes the
  single-command path index into a slice. A `debug_assert!(writes.len() == 1)` under
  `EffectScope::Command` keeps the interface honest without splitting the type back into two.
- **Waiter-driven mutations.** `satisfy_waiters_for_command` can mutate the store via `get_mut`
  (noted at `pipeline.rs:71`). Those mutations are currently *not* re-fed through the pipeline (no
  WAL entry, no notification). Unification does not change this, but the new module is the right
  place to document — and eventually fix — that gap.
