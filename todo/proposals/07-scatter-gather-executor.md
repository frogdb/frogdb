# Proposal: Scatter/Gather Executor

Status: proposed
Date: 2026-06-13

## Problem

The server crate crosses the "fan out to every internal shard, await, time out, merge" seam by
hand in every broadcast command. The mechanical choreography — enumerate `shard_senders`, open a
`oneshot` per shard, send (and decide what to do if the send fails), collect receivers, await each
under a timeout (and decide what to do on drop/timeout), then fold the replies — is re-typed in
~15 handlers across six files. Each handler then hand-codes its own merge (sort-by-key, sum
integers, dedup channels, sum per-channel counts, merge partial aggregates). The fan-out plumbing
and the merge policy are interleaved inline, so the interface a new broadcast command must learn is
the entire implementation: there is no seam, only a copy-paste template.

There *is* already a real `ScatterGatherExecutor` + `ScatterGatherStrategy` trait in
`frogdb-server/crates/server/src/scatter/` — but it solves a different problem (keyed, VLL-locked,
partitioned multi-key commands) and is wired only for MGET/MSET/DEL/EXISTS/TOUCH/UNLINK. The
broadcast commands bypass it entirely. Worse, broadcast strategies were *started* inside that
module and then abandoned: `KeysStrategy`, `DbSizeStrategy`, `FlushDbStrategy`
(`scatter/strategies.rs:352,304,421`) and an entire parallel `ScatterHandler` with
`merge_keys_results`/`merge_dbsize_results`/`merge_flush_results`/`merge_scan_results`/
`merge_randomkey_results` (`scatter/../handlers/scatter.rs:714-815`) are **dead** — no caller
reaches them (verified: zero references outside their own modules). So the merge for KEYS, DBSIZE
and FLUSHDB exists in *triplicate*: once live and inline in the handler, twice dead and quietly
drifting.

Deletion test: delete the inline fan-out/timeout/error-mapping from `handle_dbsize` and the same
seven lines reappear in `handle_keys`, `handle_flushdb`, `handle_ts_queryindex`,
`broadcast_and_check_shard0`, the five `PUBSUB` introspection handlers, `SCRIPT FLUSH`, … — it
earns its keep across N callers. Delete the merge `SumIntegers` step and it reappears as
`merge_sum_integers` (`strategies.rs:153`), as the inline DBSIZE sum (`scatter.rs:251-259`), and as
`PUBSUB NUMPAT`'s `total += count` (`pubsub.rs:526-531`). That recurrence is the signal a deep
module is missing.

Verified evidence (all paths relative to `frogdb-server/crates/server/src/`):

| Symptom | Count | Where |
|---------|-------|-------|
| Hand-rolled per-shard fan-out loops (`self.core.shard_senders.iter()` / `.enumerate()`) | 46 sites | `connection/handlers/scatter.rs` ×5, `pubsub.rs` ×5, `scripting/script.rs` ×4, `timeseries_scatter.rs` ×3, `slowlog.rs`/`latency.rs`/`debug.rs`/`client.rs` ×3 each, `search/*` ×11, others |
| `handles.push((shard_id, rx))` + await-with-timeout + inline merge | ~15 broadcast handlers | `scatter.rs:202,247,293,404`; `timeseries_scatter.rs:33,80,131`; `search/{query,helpers,index_mgmt,tagvals,synonyms,spellcheck,hybrid,es,create}.rs`; `pubsub.rs:450,486,522,556,592` |
| Duplicated `SumIntegers` merge | 3 live + 1 dead | `scatter.rs:251-259` (DBSIZE), `pubsub.rs:526-531` (NUMPAT), `pubsub.rs:489-497` (NUMSUB per-channel) vs dead `strategies.rs:153-166` |
| Duplicated `SortedUnion` merge (collect keys + `sort()`) | 3 live + 2 dead | `scatter.rs:206-228` (KEYS), `timeseries_scatter.rs:36-56` (TS.QUERYINDEX) vs dead `strategies.rs:402-407` + dead `ScatterHandler::merge_keys_results` (`scatter.rs:767`) |
| Dead scatter/merge code (never called) | ~180 lines | `ScatterHandler` + 5 `merge_*` + `ScanResult` (`scatter.rs:714-844`); `KeysStrategy`/`DbSizeStrategy`/`FlushDbStrategy` (`strategies.rs:304-462`) |
| Gather loops with **no** timeout (hang risk) | 8 | `pubsub.rs:456,492,528,562,598`; `scripting/script.rs:50,76,119` |

The last row is not just duplication — it is a divergence the missing seam allowed. The `scatter.rs`
and `timeseries_scatter.rs` handlers wrap every receiver in
`tokio::time::timeout(self.scatter_gather_timeout, rx)`; the `pubsub.rs` and `script.rs` handlers
were written from the same mental template but dropped the timeout, so a single stalled shard hangs
those commands forever (see [Correctness flags](#correctness-flags)). One shared runner makes the
timeout impossible to forget.

## Current state

### Hand-rolled fan-out + sum merge (DBSIZE, `scatter.rs:232-273`)

```rust
pub(crate) async fn handle_dbsize(&self) -> Response {
    // Scatter to all shards
    let mut handles = Vec::with_capacity(self.num_shards);
    for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::DbSize,
            conn_id: self.state.id,
            response_tx,
        };
        if sender.send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }
        handles.push((shard_id, response_rx));
    }

    // Sum counts
    let mut total: i64 = 0;
    for (shard_id, rx) in handles {
        match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
            Ok(Ok(partial)) => {
                for (_, response) in partial.results {
                    if let Response::Integer(count) = response {
                        total += count;
                    }
                }
            }
            Ok(Err(_)) => {
                warn!(shard_id, "Shard dropped DBSIZE request");
                return Response::error("ERR shard dropped request");
            }
            Err(_) => {
                warn!(shard_id, "DBSIZE timeout");
                return Response::error("ERR dbsize timeout");
            }
        }
    }

    Response::Integer(total)
}
```

Of these 42 lines, exactly two are DBSIZE-specific: `operation: ScatterOp::DbSize` and the
`total += count` fold. Everything else is the fan-out/timeout/error template.

### The identical template, different merge (KEYS, `scatter.rs:179-228`)

```rust
let mut handles = Vec::with_capacity(self.num_shards);
for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
    let (response_tx, response_rx) = oneshot::channel();
    let msg = ShardMessage::ScatterRequest {
        request_id: next_txid(),
        keys: vec![],
        operation: ScatterOp::Keys { pattern: pattern.clone() },
        conn_id: self.state.id,
        response_tx,
    };
    if sender.send(msg).await.is_err() {
        return Response::error("ERR shard unavailable");
    }
    handles.push((shard_id, response_rx));
}

// Gather all keys
let mut all_keys = Vec::new();
for (shard_id, rx) in handles {
    match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
        Ok(Ok(partial)) => {
            for (key, _) in partial.results { all_keys.push(key); }
        }
        Ok(Err(_)) => { warn!(shard_id, "Shard dropped KEYS request");
                        return Response::error("ERR shard dropped request"); }
        Err(_)     => { warn!(shard_id, "KEYS timeout");
                        return Response::error("ERR keys timeout"); }
    }
}
all_keys.sort();
Response::Array(all_keys.into_iter().map(Response::bulk).collect())
```

`TS.QUERYINDEX` (`timeseries_scatter.rs:13-57`) is the same sorted-union body again, only the
`ScatterOp` variant and the warn strings differ.

### The same fan-out, but the timeout was forgotten (PUBSUB NUMPAT, `pubsub.rs:511-534`)

```rust
let mut handles = Vec::with_capacity(self.num_shards);
for sender in self.core.shard_senders.iter() {
    let (response_tx, response_rx) = oneshot::channel();
    let _ = sender
        .send(ShardMessage::PubSubIntrospection {
            request: IntrospectionRequest::NumPat,
            response_tx,
        })
        .await;
    handles.push(response_rx);
}

// Sum all pattern counts
let mut total = 0usize;
for rx in handles {
    if let Ok(IntrospectionResponse::NumPat(count)) = rx.await {   // <-- no timeout
        total += count;
    }
}
Response::Integer(total as i64)
```

Same shape as DBSIZE — but the send error is swallowed (`let _ =`) and the gather is a bare
`rx.await` with no `tokio::time::timeout`. A shard that never replies hangs the connection. The
merge is `SumIntegers` yet again, this time spelled `total += count`.

### A `ScatterGatherStrategy` seam already exists — but only for the keyed/locked path

`scatter/mod.rs:44-74` defines the trait, and `scatter/executor.rs:71-139` drives it through the
VLL coordinator (5-phase lock/execute/gather):

```rust
pub trait ScatterGatherStrategy: Send + Sync {
    fn name(&self) -> &'static str;
    fn lock_mode(&self) -> LockMode;
    fn partition(&self, args: &[Bytes], num_shards: usize) -> PartitionResult;
    fn merge(
        &self,
        key_order: &[(usize, Bytes)],
        shard_results: &HashMap<usize, HashMap<Bytes, Response>>,
    ) -> Response;
    fn scatter_op(&self) -> ScatterOp;
}
```

This is genuinely deep for what it covers, but the interface is welded to *keyed* commands:
`partition` distributes keys to shards by hash slot, `lock_mode` acquires VLL locks, and `merge`
takes a `key_order` for result reconstruction. Broadcast commands have no keys, need no locks, and
have no key order — so they cannot use it without faking all three (`DbSizeStrategy::partition`
literally invents a `b"__dbsize__"` dummy key per shard, `strategies.rs:317-333`). And in fact they
don't: `routing.rs:132-148` only feeds MGET/MSET/DEL/EXISTS/TOUCH/UNLINK into `strategy_for_op`, so
`KeysStrategy`/`DbSizeStrategy`/`FlushDbStrategy` are constructed by nothing. They — and the older
`ScatterHandler` with its `merge_*` helpers and `ScanResult` (`scatter.rs:714-844`) — are two
abandoned attempts at exactly the seam this proposal finishes. They are dead, untested against the
live path, and silently diverging from it.

## Proposed design

Add a second, shallow-locking-free path that owns the broadcast mechanics once, with a
`MergeStrategy` seam. Keep the VLL `ScatterGatherExecutor` for keyed/locked commands; unify the two
merge vocabularies so a fold like "sum integers" is written once and shared by DEL and DBSIZE.

The key observation: every broadcast handler differs in exactly two things —
1. **the message it builds per shard** (`ScatterRequest{op}`, `PubSubIntrospection{req}`,
   `ScriptFlush`, …) and its reply type, and
2. **how replies fold** into a `Response`.

Everything else (enumerate shards, one `oneshot` per shard, send + handle send failure, collect,
await all under one timeout + handle drop/timeout) is invariant. That invariant is the deep module.

### New module: `frogdb-server/crates/server/src/scatter/broadcast.rs`

```rust
/// How a broadcast command folds per-shard replies into one client `Response`.
///
/// This is the entire interface a broadcast command must implement: the fan-out,
/// the shared timeout, the send-failure / dropped-shard / timeout error mapping all
/// live in `ScatterGather::run` behind it.
pub trait MergeStrategy {
    /// What one shard returns (e.g. `PartialResult`, `IntrospectionResponse`, `Vec<bool>`).
    type Reply: Send + 'static;

    /// Static command label for tracing / metrics and error strings.
    fn name(&self) -> &'static str;

    /// Fold one shard's reply into the running accumulator.
    fn absorb(&mut self, shard_id: usize, reply: Self::Reply);

    /// Produce the final client response once every surviving shard has been folded.
    fn finish(self: Box<Self>) -> Response;

    /// What to do when a shard send fails / drops / times out.
    /// `FailFast` (default) returns an error reply; `BestEffort` skips that shard.
    fn on_missing(&self) -> PartialPolicy { PartialPolicy::FailFast }
}

pub enum PartialPolicy { FailFast, BestEffort }

/// Lock-free broadcast coordinator. Holds the shard senders + the one timeout policy.
pub struct ScatterGather<'a> {
    senders: &'a [ShardSender],
    timeout: Duration,
    conn_id: u64,
}

impl<'a> ScatterGather<'a> {
    /// Fan `make_msg(shard_id, tx)` out to every shard, await all replies under a single
    /// shared deadline, fold them with `merge`, and map send-failure / drop / timeout to the
    /// canonical error replies — once, for every broadcast command.
    pub async fn run<M, F>(&self, mut merge: Box<M>, make_msg: F) -> Response
    where
        M: MergeStrategy + ?Sized,
        F: Fn(usize, oneshot::Sender<M::Reply>) -> ShardMessage,
    {
        let mut rxs = Vec::with_capacity(self.senders.len());
        for (shard_id, sender) in self.senders.iter().enumerate() {
            let (tx, rx) = oneshot::channel();
            if sender.send(make_msg(shard_id, tx)).await.is_err() {
                if matches!(merge.on_missing(), PartialPolicy::FailFast) {
                    return Response::error("ERR shard unavailable");
                }
                continue;
            }
            rxs.push((shard_id, rx));
        }

        // One deadline for the whole gather, not one-per-receiver.
        let deadline = tokio::time::sleep(self.timeout);
        tokio::pin!(deadline);
        for (shard_id, rx) in rxs {
            tokio::select! {
                reply = rx => match reply {
                    Ok(reply) => merge.absorb(shard_id, reply),
                    Err(_) => match merge.on_missing() {
                        PartialPolicy::FailFast => {
                            warn!(conn_id = self.conn_id, shard_id, cmd = merge.name(),
                                  "shard dropped request");
                            return Response::error("ERR shard dropped request");
                        }
                        PartialPolicy::BestEffort => continue,
                    },
                },
                _ = &mut deadline => {
                    warn!(conn_id = self.conn_id, shard_id, cmd = merge.name(), "scatter timeout");
                    return Response::error("ERR timeout");
                }
            }
        }
        merge.finish()
    }
}
```

Ready-made strategies, reused across families (these replace the inline folds and the dead
`strategies.rs`/`ScatterHandler` copies):

| Strategy | Merge | Replaces |
|----------|-------|----------|
| `SumIntegers` | sum every `Response::Integer` in every reply | DBSIZE, PUBSUB NUMPAT, NUMSUB (per channel), DEL/EXISTS via VLL |
| `SortedUnion` | collect keys, `sort()` | KEYS, TS.QUERYINDEX |
| `SortedByKey` | collect `(key, resp)`, sort by key, emit `resp` | TS.MGET, TS.MRANGE |
| `DedupSorted` | `HashSet` then `sort()` | PUBSUB CHANNELS / SHARDCHANNELS |
| `CountByKey` | sum counts per key, emit interleaved | PUBSUB NUMSUB / SHARDNUMSUB |
| `ShardZeroReply` | return shard 0's response, error-check all | `broadcast_and_check_shard0`, `…_return_shard0_response` |
| `AllOk` | OK if every shard ok | FLUSHDB, SCRIPT FLUSH |

A `ConnectionHandler::scatter_gather(&self) -> ScatterGather<'_>` accessor binds
`self.core.shard_senders`, `self.scatter_gather_timeout`, `self.state.id` so call sites stay terse.

### Before / after: DBSIZE

Before — `scatter.rs:232-273`, 42 lines (shown in full above).

After — the command states only its two distinguishing facts (the op and the fold):

```rust
pub(crate) async fn handle_dbsize(&self) -> Response {
    self.scatter_gather()
        .run(Box::new(SumIntegers::default()), |_shard, response_tx| {
            ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::DbSize,
                conn_id: self.state.id,
                response_tx,
            }
        })
        .await
}
```

KEYS becomes the same call with `SortedUnion::default()` and `ScatterOp::Keys`; PUBSUB NUMPAT
becomes `SumIntegers` over `IntrospectionResponse` and — critically — *inherits the timeout it is
missing today* for free, because the timeout lives in `run`, not in the handler.

### Why this is the right depth

- **Locality.** The timeout policy, the single-deadline gather, the `"ERR shard unavailable"` /
  `"ERR shard dropped request"` text, and the send-failure decision live in `ScatterGather::run`
  and nowhere else. The fact that `pubsub.rs` and `script.rs` *forgot the timeout* is precisely the
  class of bug a single owner eliminates — there is no longer a place to forget it.
- **Leverage.** ~15 broadcast handlers and 46 fan-out loops collapse to a strategy pick plus a
  message closure. A new broadcast command (e.g. a future `MEMORY USAGE`-style aggregate) is one
  `MergeStrategy` impl, not another copy of the template.
- **Deletion test.** Deleting `ScatterGather` makes the await/timeout/error-mapping reappear in
  every broadcast handler — it earns its keep. Conversely, landing it lets us *delete* the dead
  `ScatterHandler` + five `merge_*` + `ScanResult` (`scatter.rs:714-844`) and the dead
  `KeysStrategy`/`DbSizeStrategy`/`FlushDbStrategy` (`strategies.rs:304-462`): they were this seam,
  half-built, and the migration finishes and removes them.
- **No new adapter.** This is not a wrapper over `ShardSender`; it is the same `oneshot` +
  `ShardMessage` mechanics every handler already uses, owned once. It reuses `ScatterOp`,
  `PartialResult`, `IntrospectionResponse` unchanged. The closure `make_msg` keeps message
  construction at the call site, so no command loses control over what it sends.
- **One merge taxonomy.** `SumIntegers` is shared by DBSIZE *and* the VLL `DelStrategy`/
  `ExistsStrategy` (today `merge_sum_integers`, `strategies.rs:153`, is a fourth copy). Unifying the
  merge vocabulary across the locked and lock-free paths is the cleanup that removes the triplication
  the deletion test exposed.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green.

1. **Phase 0 — add the interface.** New `scatter/broadcast.rs`: `MergeStrategy`, `PartialPolicy`,
   `ScatterGather`, and the ready-made strategies (`SumIntegers`, `SortedUnion`, `SortedByKey`,
   `DedupSorted`, `CountByKey`, `ShardZeroReply`, `AllOk`) with the unit-test matrix below. Add
   `ConnectionHandler::scatter_gather()`. No call sites change. `just check frogdb-server`.
2. **Phase 1 — simple full broadcasts.** Migrate `handle_dbsize`, `handle_keys`, `handle_flushdb`
   (`scatter.rs`), `handle_ts_queryindex`/`handle_ts_mget`/`handle_ts_mrange`
   (`timeseries_scatter.rs`), and `broadcast_and_check_shard0`/`broadcast_and_return_shard0_response`
   (`search/helpers.rs`). Then delete the now-redundant dead code: `ScatterHandler` + `merge_*` +
   `ScanResult` (`scatter.rs:714-844`) and `KeysStrategy`/`DbSizeStrategy`/`FlushDbStrategy`
   (`strategies.rs:304-462`, and their arms in `strategy_for_op`). FrogDB is pre-production — no shims.
3. **Phase 2 — pubsub + scripting (also a bug fix).** Migrate the five `PUBSUB` introspection
   handlers (`pubsub.rs:431-600+`) to `DedupSorted` / `CountByKey` / `SumIntegers`, and
   `handle_script_load`/`handle_script_exists`/`handle_script_flush` (`script.rs:34-124`) to
   `ShardZeroReply` / a boolean-OR strategy / `AllOk`. This deletes the bare `rx.await` gathers and
   fixes [Correctness flags](#correctness-flags) F1 and F2 as a side effect.
4. **Phase 3 — search fan-outs.** `search/{query,aggregate,hybrid,es,create,index_mgmt,tagvals,
   synonyms,spellcheck}.rs` keep their bespoke merges (FT.SEARCH overfetch + sort, FT.AGGREGATE
   partial-aggregate reduction) but express them as `MergeStrategy` impls and route the fan-out
   through `ScatterGather::run` with their per-command `effective_timeout`.
5. **Phase 4 — multi-phase commands.** SCAN (`scatter.rs:21-176`, sequential cursor walk, not a
   plain fan-out) gets a dedicated `paginate` entry point; RANDOMKEY (`scatter.rs:276-378`,
   two-round count-then-fetch) gets a `select_then_fetch` entry point. These reuse `ScatterGather`'s
   per-shard send/timeout helper but keep their control flow.
6. **Phase 5 — unify with VLL.** Re-express `ScatterGatherStrategy::merge`
   (`scatter/mod.rs:66`) in terms of the same `SumIntegers`/`SortedUnion` folds so the merge
   taxonomy is single-sourced across the locked and lock-free paths.

## Testing impact

- **Merge strategies become pure, socket-free unit tests.** A `MergeStrategy` is folded over a
  `Vec<(shard_id, Reply)>` and asserted on the resulting `Response` — no Tokio runtime, no real
  internal shards, no live socket. Today the *live* folds (the inline DBSIZE sum, the KEYS
  `sort()`, the NUMSUB per-channel sum) are reachable only end-to-end through a real connection and
  real shard workers. (The irony the seam removes: the *dead* copies in `strategies.rs` already
  have unit tests — `test_del_merge_sums`, `test_dbsize_goes_to_all_shards` — while the live code
  has none.)
- **Timeout / dropped-shard behavior tested once.** `ScatterGather::run` is tested with mock
  senders: a shard whose `oneshot::Sender` is dropped → `"ERR shard dropped request"` under
  `FailFast`, skipped under `BestEffort`; a shard that never replies → `"ERR timeout"` within the
  deadline (with `tokio::time::pause()`); a closed sender → `"ERR shard unavailable"`. These
  properties are currently untestable in isolation and, in `pubsub.rs`/`script.rs`, currently wrong.
- **Single-deadline semantics pinned.** A test asserts total gather time is bounded by one
  `scatter_gather_timeout`, not `num_shards × timeout` (the per-receiver sequential awaits today
  can, worst case, wait longer than intended).
- **Existing suites are the behavioral net.** The Redis-compat integration tests for KEYS, DBSIZE,
  SCAN, FLUSHDB, TS.*, FT.*, PUBSUB and SCRIPT are the end-to-end check that no reply changed during
  migration.

## Risks / open questions

- **Timeout semantics change.** Today each handler awaits receivers sequentially, each with a fresh
  `scatter_gather_timeout`; the runner uses one shared deadline. This is stricter (and more
  correct) but is a behavior change for the rare slow-shard case — call it out and keep the same
  `scatter_gather_timeout` source so the common path is identical.
- **Partial-shard policy must be chosen explicitly.** Behavior is inconsistent today: `scatter.rs`
  fails fast on any dropped shard; `pubsub.rs` silently drops failures and under-reports;
  FT.AGGREGATE returns the first `__ft_error__`. `PartialPolicy` forces each strategy to *declare*
  fail-fast vs best-effort instead of leaving it to whoever copied the template. Picking the wrong
  default per command (e.g. making PUBSUB fail-fast when it was best-effort) would be a visible
  change — must be decided per command, matching today's intended (not accidental) behavior.
- **Generics vs object safety.** `make_msg`'s reply type and the message variant differ per command
  (`PartialResult` vs `IntrospectionResponse` vs `Vec<bool>` vs `String`). `run` is generic over
  `M: MergeStrategy` + closure `F`, monomorphized per call site — not `dyn` — which is fine but
  means the strategies aren't a single `dyn` list. Acceptable; the seam is the trait, not a registry.
- **SCAN cursor encoding stays bespoke.** SCAN is a sequential cursor walk across shards
  (`scatter.rs:106-167`) with a 16-bit-shard / 48-bit-position encoding, not a fan-out; it only
  borrows the per-shard send/timeout helper. Folding it fully into `MergeStrategy` is not a goal —
  forcing it would make the interface leaky.
- **RESP3 push is out of scope.** This seam covers PUBSUB *introspection* (CHANNELS/NUMSUB/NUMPAT),
  which return ordinary arrays. The message-*publish* fan-out (push frames to subscribers) is a
  different path and must not be conflated with it.
- **VLL unification ordering.** Phase 5 (sharing folds with the locked executor) is optional polish;
  Phases 1–4 stand alone. If `ScatterGatherStrategy::merge`'s `HashMap<usize, HashMap<Bytes,
  Response>>` shape resists the simpler `MergeStrategy` reply model, leave the two merge taxonomies
  separate rather than contorting the broadcast seam to match the keyed one.

## Correctness flags

Found while reading; not introduced by this proposal. Each is a direct consequence of the missing
seam (the fan-out template was copied without its timeout / error handling).

- **F1 — PUBSUB introspection has no gather timeout (connection hang).**
  `pubsub.rs:456,492,528,562,598` gather with a bare `rx.await` (e.g. `pubsub.rs:511-534`,
  `handle_pubsub_numpat`) instead of `tokio::time::timeout(self.scatter_gather_timeout, rx)` as
  every `scatter.rs` handler does. A shard worker that stalls or never replies hangs `PUBSUB
  CHANNELS / NUMSUB / NUMPAT / SHARDCHANNELS / SHARDNUMSUB` indefinitely. Send failures are also
  swallowed (`let _ = sender.send(...)`, `pubsub.rs:442,478,516,548,584`), so a dropped shard
  silently under-reports counts/channels instead of erroring.

- **F2 — SCRIPT broadcast has no timeout, and SCRIPT LOAD can diverge across shards.**
  `script.rs:50,76,119` gather with bare `rx.await` (no timeout). `handle_script_load`
  (`script.rs:34-54`) ignores every shard's send result (`let _ = sender.send(...)`) and then
  awaits only shard 0 (`handles.into_iter().next().unwrap().await`), returning that SHA while other
  shards may never have received the script — leaving the Lua script cache silently inconsistent
  across internal shards on a dropped/slow shard. `handle_script_flush` (`script.rs:110-123`) and
  `handle_script_exists` (`script.rs:57-90`) likewise swallow send errors and can hang.

- **F3 — dead, drifting duplicate merge code (not a live bug, but a latent one).**
  `ScatterHandler` + `merge_scan_results`/`merge_keys_results`/`merge_dbsize_results`/
  `merge_randomkey_results`/`merge_flush_results` + `ScanResult` (`scatter.rs:714-844`) and
  `KeysStrategy`/`DbSizeStrategy`/`FlushDbStrategy` (`strategies.rs:304-462`) have zero callers
  (verified). They are tested copies of the merge logic that diverge from the live inline versions —
  a maintenance trap where a fix to the live path won't touch the tested-but-dead path. Should be
  deleted in Phase 1.
