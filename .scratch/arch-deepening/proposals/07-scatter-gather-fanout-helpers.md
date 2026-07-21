# Proposal 07 — Grow the broadcast seam to cover every fan-out shape (not just fold-into-Response)

## Summary (2-3 sentences)

The `ScatterGather` broadcast seam (`scatter/broadcast.rs`) owns fan-out, one shared deadline, and
the drop/timeout→canonical-error mapping — but only for commands that fold shard replies into a
client `Response`. Every fan-out that wants a typed `Vec<T>`, a first-success walk, or a
fire-and-forget broadcast hand-rolls its own `for sender in … { send; await }` loop, and 23 such
loops now exist across six files — inconsistently timed, and one (FUNCTION KILL) with **no timeout
at all**, so it hangs forever on a wedged ShardWorker. This proposal adds three sibling helpers
(`gather_all`, `find_first`, `broadcast_all`) to the same seam so the fan-out/timeout policy lives
in one Module for all shapes, and flags the FUNCTION KILL hang as a live defect the refactor fixes
structurally.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/server/src/scatter/broadcast.rs` | 923 | The seam: `ScatterGather::run` (L92), `query_one` (L157), `MergeStrategy` trait (L45), ready-made merges + tests. Owns the *fold-into-Response* fan-out only |
| `frogdb-server/crates/server/src/connection/scatter.rs` | — | `ConnectionHandler::scatter_gather()` builder (L25) + `scatter_gather_with_timeout` (L35); 3 `run`/`query_one` consumers |
| `frogdb-server/crates/server/src/connection/debug_handler.rs` | 420 | 8 hand-rolled loops: `gather_vll` L74, `gather_lock_table` L116, `gather_wait_queue` L146, `memory_check` L176, `expiry_index_check` L209 (per-shard 5s timeout); `set_active_expire` L365, `keysizes_snapshot` L384, `allocsize_in_slot` L403 (no timeout) |
| `frogdb-server/crates/server/src/connection/observability_conn_command.rs` | 1378 | 7 hand-rolled loops, **all timeout-less**: `gather_memory_stats` L55, `gather_latency_latest` L74, `gather_latency_history` L105, `slowlog_get` L215, `slowlog_len` L255, `slowlog_reset` L272, `latency_reset` L816 |
| `frogdb-server/crates/server/src/connection/conn_command.rs` | 725 | `config_resetstat` L333 (CONFIG RESETSTAT), timeout-less broadcast-and-discard |
| `frogdb-server/crates/server/src/connection/scripting/script.rs` | 314 | `handle_script_kill` L104: first-success walk, **retrofitted** with `scatter_gather_timeout` (L115). SCRIPT LOAD/EXISTS/FLUSH (L45/63/91) correctly use `.run()` |
| `frogdb-server/crates/server/src/connection/scripting/function.rs` | 529 | `handle_function_kill` L495: gather-then-scan, **NO timeout** (L508 bare `response_rx.await`) — the live hang bug |
| `frogdb-server/crates/server/src/connection/lifecycle.rs` | 721 | 4 fire-and-forget broadcasts: tracking register/unregister L132/143/165, `ConnectionClosed` L198 |
| `frogdb-server/crates/server/src/connection/connection_state_conn_command.rs` | 488 | RESET's `ConnectionClosed` fan-out L138 — same shape as `lifecycle.rs:198` |

## Problem (concrete verified evidence)

**The seam's own docstring claims a monopoly it does not have.** `broadcast.rs:14-18`:

```rust
// Everything else — enumerate shards, one `oneshot` per shard, send and handle
// send failure, collect, await all under one timeout, map drop/timeout to the
// canonical error replies — lives in [`ScatterGather::run`] and nowhere else.
// That is the seam the missing-timeout bugs in `pubsub.rs`/`script.rs` came
// from: there is no longer a place to forget the timeout.
```

"and nowhere else" is false. `run` only serves fan-outs that end in a `Response` (folded by a
`MergeStrategy`). Verified count: **26 production `.run()` call sites** (timeseries ×3, scatter ×3,
pubsub ×5, scripting ×3, search ×12) plus 3 `query_one` — the seam is heavily used *for that
shape*. But three other shapes have no helper, so **23 hand-rolled fan-out loops** live outside it,
each re-deriving "enumerate shards, one oneshot per shard, send, handle send failure, collect":

Shape (a) — **typed `Vec<T>` gather** (send → await → collect, sequential, best-effort). Five in
`debug_handler.rs` carry a per-shard 5s timeout; the rest carry none:

```rust
// debug_handler.rs:116  gather_lock_table — one of four ~28-line near-clones
for shard_id in 0..self.core.shard_senders.len() {
    let (response_tx, response_rx) = oneshot::channel();
    if self.core.shard_senders[shard_id]
        .send(frogdb_core::shard::ShardMessage::GetLockTableInfo { response_tx })
        .await
        .is_err()
    {
        tracing::warn!(shard_id, "Failed to send GetLockTableInfo message");
        continue;
    }
    match tokio::time::timeout(timeout, response_rx).await {
        Ok(Ok(info)) => results.push(info),
        Ok(Err(_)) => tracing::warn!(shard_id, "Channel closed while waiting for lock-table info"),
        Err(_) => tracing::warn!(shard_id, "Timeout waiting for lock-table info"),
    }
}
```

```rust
// observability_conn_command.rs:55  gather_memory_stats — NO timeout
for sender in shard_senders.iter() {
    let (response_tx, response_rx) = oneshot::channel();
    if sender.send(ShardMessage::MemoryStats { response_tx }).await.is_ok()
        && let Ok(shard_stats) = response_rx.await   // <-- unbounded await
    {
        stats.push(shard_stats);
    }
}
```

Sites, verified: `debug_handler.rs` 74, 116, 146, 176, 209 (5s per-shard timeout), 365, 384, 403
(no timeout); `observability_conn_command.rs` 55, 74, 105, 215, 255, 272, 816 (all timeout-less);
`conn_command.rs` 333 (timeout-less).

Shape (b) — **first-success / find-first walk**. Two handlers, both sending
`ShardMessage::ScriptKill` (verified: `function.rs:502`, `script.rs:108`), diverge on timeout:

```rust
// script.rs:104  SCRIPT KILL — bounded, after a round-2 retrofit (comment L99-103)
match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
    Ok(Ok(Ok(()))) => return Response::ok(),
    Ok(Ok(Err(e))) if e.contains("NOTBUSY") => continue,
    Ok(Ok(Err(e))) => return Response::error(e),
    Ok(Err(_)) | Err(_) => continue,   // dropped or timed out: next shard
}
```

```rust
// function.rs:508  FUNCTION KILL — UNBOUNDED
if let Ok(result) = response_rx.await {   // <-- no timeout: hangs forever
    responses.push(result);
}
```

Both are `impl ConnectionHandler` (verified: `function.rs:13`, `script.rs:11`), so FUNCTION KILL
has `self.scatter_gather_timeout` and `self.scatter_gather()` in scope exactly as SCRIPT KILL does —
it simply doesn't use them. **A ShardWorker wedged mid-script makes FUNCTION KILL block the whole
ConnectionHandler indefinitely, while SCRIPT KILL against the identical wedge returns `ERR timeout`
and moves on.** This is the third instance of the bug class the seam docstring says it eliminated.

Shape (c) — **fire-and-forget broadcast** (send to every shard, ignore replies):

```rust
// lifecycle.rs:198  and, structurally, connection_state_conn_command.rs:138
for sender in self.core.shard_senders.iter() {
    let _ = sender.send(ShardMessage::ConnectionClosed { conn_id: self.state.id }).await;
}
```

Sites: `lifecycle.rs` 132, 143, 165, 198; `connection_state_conn_command.rs` 138.

## Why it is shallow/fragmented (architecture vocabulary)

**The seam is deep for one shape and absent for three.** `ScatterGather::run` is a genuinely deep
Interface: a one-line call at the site hides shard enumeration, per-shard `oneshot` wiring, send-
failure handling, a single shared deadline, and canonical drop/timeout error mapping. That Leverage
is real — but it is only reachable by callers whose Implementation is a `MergeStrategy` producing a
`Response`. A gather that needs `Vec<LockTableInfo>`, a walk that needs `Option<KillResult>`, or a
broadcast that needs nothing back cannot pass through the seam, so it reimplements the fan-out. The
Module's Interface is narrower than its subject matter: it models *fan-out-then-fold-to-Response*,
not *fan-out*.

**The duplicated loop is the fragmentation.** The exact body the docstring enumerates —
"enumerate shards, one oneshot per shard, send and handle send failure, collect" — is copy-pasted
23 times. `debug_handler.rs` alone contains four ~28-line near-identical methods (`gather_lock_table`,
`gather_wait_queue`, `memory_check`, `expiry_index_check`) differing only in the `ShardMessage`
variant and the log string. Locality is inverted: the knowledge "how FrogDB fans a message across
every ShardWorker and survives a slow/dead shard" is smeared across six files instead of sitting
behind one Interface.

**Deletion test.** Delete `ScatterGather::run` and 26 call sites break — it is load-bearing. Delete
any one hand-rolled loop and only that command breaks, because none of them share code — the mark
of copy-paste, not of a seam. Conversely, the timeout policy fails the *consistency* half of the
test: the seam guarantees one shared deadline, but the 23 hand-rolled loops variously use a per-shard
5s timeout (worst case N×5s wall-clock, not one deadline), or no timeout at all. The policy the
docstring claims is centralized is in fact forked 23 ways.

**The divergence proves the hazard is live, not theoretical.** SCRIPT KILL and FUNCTION KILL are the
same walk over the same `ShardMessage::ScriptKill`. One was retrofitted with a timeout (an explicit
`round-2 flag F2` fix, per the comment at `script.rs:99-103`); its twin one file over was not. When
the same fact is maintained by hand in two Adapters, they drift — and here the drift is a
hang-forever bug shipped in a sibling of a function someone already fixed.

**Contrast with the deep side.** The `MergeStrategy` path shows the target shape: SCRIPT
LOAD/EXISTS/FLUSH (`script.rs:45/63/91`) each route through `.run()` and inherit the timeout for
free — there is literally no per-shard await to forget. `pubsub.rs`/`script.rs`'s past missing-timeout
bugs were fixed *by moving them onto the seam*. The three fan-out shapes above are simply the parts
of the seam that were never built.

## Proposed change (plain English)

Grow `ScatterGather` from one fan-out method to four, so every fan-out shape passes through the same
Module and inherits the one-deadline / send-failure / drop-timeout policy. The `make_msg(shard_id, tx)`
closure convention and the shared-deadline mechanics already in `run` (L118-147) are reused verbatim.

1. **`gather_all<R>(make_msg) -> Vec<R>`** — concurrent fan-out, one shared deadline, best-effort
   typed collect. Replaces every shape-(a) loop. Callers get a `Vec<R>` and do their own
   merge/sort/sum at the call site (as they do today), but lose the fan-out boilerplate and gain the
   single deadline. The per-shard 5s timeout in `debug_handler` becomes one gather-wide deadline
   (bounded regardless of shard count); the timeout-less observability gathers become bounded for the
   first time.
2. **`find_first<R>(make_msg, predicate) -> Option<R>`** — short-circuit walk with the timeout baked
   in. `predicate` decides "is this the answer?" (SCRIPT/FUNCTION KILL: "did this shard kill a
   running script?"). Returns on the first shard that satisfies it, skipping send-failure / drop /
   timeout shards. Both kill handlers collapse onto this — and FUNCTION KILL's missing timeout
   becomes structurally impossible, not a thing to remember.
3. **`broadcast_all(make_msg)`** — fire-and-forget to every shard, no replies awaited. Replaces every
   shape-(c) loop (tracking register/unregister, the two `ConnectionClosed` fan-outs). One method,
   one place that decides whether send failures are logged.

Route all 23 hand-rolled loops through these. Update the seam docstring's "and nowhere else" claim so
it is true again — after this, there is genuinely no per-shard await left to hand-roll.

**FUNCTION KILL — hotfix vs. structural fix.** The refactor fixes the hang by construction, but it
touches ~7 files and should not gate a one-line correctness fix. Recommend a **targeted hotfix
first**: wrap `function.rs:508`'s `response_rx.await` in
`tokio::time::timeout(self.scatter_gather_timeout, …)`, mirroring `script.rs:115` (both are
`impl ConnectionHandler`, so no plumbing is needed). Land that ahead of — or independently of — this
proposal; the `find_first` migration then subsumes it.

## Before / After

### Before — three shapes, hand-rolled (real current code)

```rust
// (a) debug_handler.rs:116 — 28 lines, one of four near-identical clones
fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<LockTableInfo>> {
    Box::pin(async move {
        let mut results = Vec::new();
        let timeout = std::time::Duration::from_secs(5);
        for shard_id in 0..self.core.shard_senders.len() {
            let (response_tx, response_rx) = oneshot::channel();
            if self.core.shard_senders[shard_id]
                .send(ShardMessage::GetLockTableInfo { response_tx }).await.is_err()
            { tracing::warn!(shard_id, "Failed to send GetLockTableInfo message"); continue; }
            match tokio::time::timeout(timeout, response_rx).await {
                Ok(Ok(info)) => results.push(info),
                Ok(Err(_)) => tracing::warn!(shard_id, "Channel closed …"),
                Err(_) => tracing::warn!(shard_id, "Timeout …"),
            }
        }
        results
    })
}
```

```rust
// (b) function.rs:495 — FUNCTION KILL, unbounded await, gather-then-scan
async fn handle_function_kill(&self) -> Response {
    let mut responses = Vec::with_capacity(self.num_shards);
    for sender in self.core.shard_senders.iter() {
        let (response_tx, response_rx) = oneshot::channel();
        if sender.send(ShardMessage::ScriptKill { response_tx }).await.is_err() { continue; }
        if let Ok(result) = response_rx.await { responses.push(result); }   // hangs on a wedged shard
    }
    for response in responses {
        match response {
            Ok(()) => return Response::ok(),
            Err(e) if e.contains("UNKILLABLE") =>
                return Response::error("UNKILLABLE The busy script was not running in read-only mode."),
            Err(_) => {}
        }
    }
    Response::error("NOTBUSY No scripts in execution right now.")
}
```

```rust
// (c) lifecycle.rs:198 — fire-and-forget
for sender in self.core.shard_senders.iter() {
    let _ = sender.send(ShardMessage::ConnectionClosed { conn_id: self.state.id }).await;
}
```

### After — all three route through the seam (illustrative sketch)

```rust
// (a) one shared deadline, no boilerplate
fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<LockTableInfo>> {
    Box::pin(self.scatter_gather().gather_all(|_shard, tx|
        ShardMessage::GetLockTableInfo { response_tx: tx }))
}
```

```rust
// (b) FUNCTION KILL — timeout baked in, can't be forgotten
async fn handle_function_kill(&self) -> Response {
    match self.scatter_gather()
        .find_first(
            |_shard, tx| ShardMessage::ScriptKill { response_tx: tx },
            |r| !matches!(r, Err(e) if e.contains("NOTBUSY")),  // first shard that killed / erred hard
        ).await
    {
        Some(Ok(())) => Response::ok(),
        Some(Err(e)) if e.contains("UNKILLABLE") =>
            Response::error("UNKILLABLE The busy script was not running in read-only mode."),
        _ => Response::error("NOTBUSY No scripts in execution right now."),
    }
}
// SCRIPT KILL collapses onto the identical call — the two can no longer drift.
```

```rust
// (c) one method decides send-failure logging for every broadcast
self.scatter_gather().broadcast_all(|_shard, _| ShardMessage::ConnectionClosed { conn_id: self.state.id }).await;
```

## Testability improvement

**Today the timeout guarantee is per-call-site and untestable in bulk.** The seam has a real timeout
unit test — `broadcast_timeout_routing_tests` (`script.rs:163`) wires a handler to two mock shards,
stalls shard 1 forever, and asserts SCRIPT LOAD / PUBSUB CHANNELS surface `ERR timeout`. But that
test only covers commands *on the seam*. There is no equivalent for the 23 hand-rolled loops, which
is exactly why FUNCTION KILL shipped with an unbounded await: reverting SCRIPT KILL's timeout would
fail that mock-shard test, but the identical omission in FUNCTION KILL trips no test at all, because
no test drives a stalled shard through a hand-rolled gather.

**After the change:**

1. **One stalled-shard test covers every fan-out.** Point the existing mock-shard harness
   (`script.rs:183` `MockShards`) at `gather_all` / `find_first` / `broadcast_all` and assert each
   respects the deadline. Every command that routes through them inherits the guarantee — a new DEBUG
   gather or a new KILL-style walk cannot reintroduce the unbounded-await bug, because it has no await
   to leave unbounded.
2. **FUNCTION KILL gets a regression test that is real today.** A test that stalls one shard and
   asserts FUNCTION KILL returns within the timeout **fails against current `main`** (the handler
   hangs) and passes after either the hotfix or the `find_first` migration — a genuine
   red→green, not a tautology.
3. **The four `debug_handler` clones stop being four test surfaces.** Their partition/merge logic is
   trivial (`push`/`sum`/`extend`); once the fan-out is behind `gather_all`, the only thing worth
   testing per command is the message variant, and the timeout behavior is tested once.

## Risks / open questions

- **`find_first` predicate ergonomics.** SCRIPT/FUNCTION KILL's reply is a nested
  `Result<Result<(), String>, _>`; the predicate + post-match must preserve the exact `UNKILLABLE` /
  `NOTBUSY` / hard-error precedence. The sketch folds the classification into the predicate; an
  alternative is `find_first` returning the first non-skipped reply and the caller classifying. Pick
  whichever keeps the three-way KILL semantics verbatim — this is the one arm where a subtle change
  alters observable errors.
- **Per-shard 5s vs. one shared deadline (behavior change).** The five `debug_handler` gathers use a
  *per-shard* 5s timeout (worst case ~N×5s); `gather_all` imposes one deadline for the whole gather.
  This is a strict improvement (bounded wall-clock) but *is* a behavior change under many shards.
  Confirm no DEBUG test asserts the old cumulative timing; choose the gather-wide deadline
  deliberately (likely `scatter_gather_timeout`, not 5s) and note it.
- **Concurrency vs. current sequential order.** Every hand-rolled loop today is sequential
  (send-await-send-await); `gather_all` sending all then awaiting all changes ordering. Merges that
  sort afterward (`gather_latency_history` sorts by timestamp; `slowlog_get` sorts by id) are
  order-independent, but audit each for hidden order dependence before switching to concurrent
  dispatch. A sequential `gather_all` variant is a fallback if any caller depends on order.
- **`broadcast_all` send-failure policy.** The shape-(c) loops today silently `let _ =` send
  failures. `broadcast_all` should preserve that (best-effort, no error surfaced) — tracking
  register/`ConnectionClosed` must not start returning errors. One decision, made once, but it must
  match today's silence.
- **Scope boundary vs. Proposal 01.** This proposal is strictly the *broadcast/fan-out helper
  shapes*. It does **not** touch the keyed VLL-locked `ScatterGatherExecutor` path or the declarative
  `MergeStrategy`-enum / routing seam — that is Proposal 01's subject. The name collision it notes
  (`core::command::MergeStrategy` enum vs. `broadcast.rs:45` `MergeStrategy` trait) is unaffected
  here; these helpers sit beside `run`, reusing the same trait-free plumbing.

## Effort estimate

**M.** The three helpers are ~60 lines total, all reusing the shard-enumeration and shared-deadline
mechanics already in `ScatterGather::run` (L98-147). The migration is mechanical but broad: 23 call
sites across six files, most a pure 1:1 substitution (the four `debug_handler` clones and the five
observability gathers are nearly identical and can be swept together). It is not S because the two
KILL handlers need care to preserve the three-way error precedence, and because the per-shard→shared
deadline and sequential→concurrent changes each need a per-caller audit. It stays out of L because no
`ShardMessage` variants, no shard-side (`execution.rs`) code, and no wire formats change — the blast
radius is confined to the ConnectionHandler fan-out call sites. **Recommend landing the one-line
FUNCTION KILL timeout hotfix first** (`function.rs:508`), independently, since it is a live
hang-forever defect and the refactor should not gate the fix.
