# 12 — Re-WATCHing an already-watched key resets its version snapshot (WATCH false-negative)

Status: ready-for-agent
Type: bug
Origin: post-harness-fix re-verification of issue 11 Finding B (2026-08-02), local `seed_sweep_nightly`
runs at `ops_per_client = 60`, `seeds = 20`.

## What happened

`TransactionState::watch_key` (`frogdb-server/crates/txn/src/state.rs:244`) records a watch as a
`HashMap` **insert**:

```rust
pub fn watch_key(&mut self, key: Bytes, shard_id: usize, version: u64, live_at_watch: bool) {
    self.watches.insert(key, (shard_id, version, live_at_watch));
}
```

`handle_watch` (`frogdb-server/crates/server/src/connection/transaction_conn_command.rs:335`)
fetches a fresh `GetVersion` snapshot from the shard and calls it unconditionally for every
argument, with no "already watched" guard. So a second `WATCH k` on a connection that is already
watching `k` **overwrites the earlier version snapshot with the current one**, erasing every
modification that happened between the two WATCHes. The following EXEC compares against the *later*
snapshot and commits.

Redis does the opposite — `watchForKey()` (`multi.c`) is a no-op when the key is already in
`c->watched_keys`:

```c
/* Check if we are already watching for this key */
listRewind(c->watched_keys,&li);
while((ln = listNext(&li))) {
    wk = listNodeValue(ln);
    if (wk->db == c->db && equalStringObjects(key,wk->key))
        return; /* Key already watched */
}
```

and `CLIENT_DIRTY_CAS` — once set by an interfering write — is cleared only by
EXEC/DISCARD/UNWATCH/RESET, never by another WATCH. Valkey and DragonflyDB match Redis here.

This is a **different mechanism** from the already-fixed, pinned
`regression_crossshard_watch_false_negative_seed_8` (cross-shard watch-set fold): it reproduces on a
single shard with a single watched key.

## Violation signature

Verbatim, `FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20 just concurrency-nightly`
(local, 2026-08-02, harness with the Finding-A drain fix in place):

```
seed 3 (TxHeavy) FAILED: ["WATCH false-negative: watch false-negative: exec op 24459 committed
  though op 24406 wrote watched key [123, 116, 49, 125, 107, 118, 49] after watch op 24403"]
seed 16 (TxHeavy) FAILED: ["WATCH false-negative: watch false-negative: exec op 30468 committed
  though op 30464 wrote watched key [123, 116, 48, 125, 107, 118, 48] after watch op 30459"]
```

(`[123,116,49,125,107,118,49]` = `{t1}kv1`, `[123,116,48,125,107,118,48]` = `{t0}kv0`.)

Seed 3 / TxHeavy / OPS=60 reproduced the violation on **5 of 5** repeated runs, so this class is
robust despite the harness's run-to-run nondeterminism (issue 14).

## Trace (seed 3, TxHeavy, OPS=60, 4 clients, 2 shards)

Op ids drift between runs (issue 14); this is one run's history, client `c0` on key `{t1}kv1`:

```
op 485 c0 [ 63,  64] watch ["{t1}kv1"]                       -> "OK"     <- first watch
op 488 c1 [ 69,  82] exec  ["1","del","1","{t1}kv1"]         -> "1"      <- other client, real DEL
op 514 c0 [121, 130] del   ["{t1}kv1"]                       -> "1"
op 535 c0 [163, 164] watch ["{t1}kv1"]                       -> "OK"     <- RE-WATCH, snapshot reset
op 541 c0 [175, 190] exec  ["3","incr",…,"get",…,"del","1","{t1}kv1"] -> "1|1|1"  <- COMMITTED
```

`c0` issues no DISCARD/UNWATCH/RESET and no EXEC between op 485 and op 541 (the workload never emits
UNWATCH or RESET), so under Redis semantics the DEL at op 488 leaves `c0` dirty and op 541 must
return nil. FrogDB committed it. The flagged writer (op 488) is a DEL that returned `1` — a genuine
modification, not the no-op-DEL checker artifact filed as issue 13.

## Suggested fix

Guard the insert so the *first* snapshot wins:

```rust
self.watches.entry(key).or_insert((shard_id, version, live_at_watch));
```

Check the same shape elsewhere in the watch path (the `live_at_watch` flag must also keep the
*first* observation, otherwise a live-then-expired watch can be laundered into "already stale" by a
re-WATCH — the same laundering in a different coat).

## Acceptance criteria

- [ ] A re-`WATCH` of an already-watched key does not reset that key's version/liveness snapshot.
- [ ] Deterministic regression test (worker- or turmoil-level, **not** a pinned nightly seed — see
      issue 14): client A `WATCH k`; client B writes `k`; client A `WATCH k` again; `MULTI`/write/
      `EXEC` must return nil.
- [ ] Existing WATCH pins stay green: `regression_crossshard_watch_false_negative_seed_8`,
      `regression_watch_second_watcher_aborts_realpath`,
      `watch_lazy_expiry_false_negative_realpath`,
      `regression_watch_read_lazy_purge_aborts_realpath`.
- [ ] TCL/redis-regression WATCH coverage unaffected.

## References

- `frogdb-server/crates/txn/src/state.rs` — `watch_key`, `take`.
- `frogdb-server/crates/server/src/connection/transaction_conn_command.rs` — `handle_watch`.
- `frogdb-server/crates/testing/src/conservation.rs` — `check_watch_no_false_negative`,
  `writer_between` (the checker that caught it; it anchors on the **earliest** watch of a key, which
  is exactly Redis' rule).
- Issue 11 (`.scratch/concurrency-testing/issues/`) — Finding B, where this was first seen.
- Issue 13 — the co-occurring checker false positive (no-op DEL), which is *not* this bug.
- Issue 14 — harness run-to-run nondeterminism, why this must not be pinned by seed.
