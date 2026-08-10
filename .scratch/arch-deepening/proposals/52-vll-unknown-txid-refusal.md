# Proposal 52 — `VllExecute` for an unheld transaction becomes a typed refusal, not a silent empty success

*Round 38. Candidate TX14 of the txn+vll+scripting lane. **Spec-first**: this proposal designs a new
`FM-VLL-006` row, the forcing tests that make it fail first, and the fix — in that order.*

## Summary

`handle_vll_execute` answers a `VllExecute` whose txid this shard is not holding by sending
`PartialResult::default()` — an **empty successful reply** (`core/src/shard/vll.rs:45-48`). The
coordinator folds that into the gather as data, so MGET reports the shard's keys as missing,
DEL/EXISTS/TOUCH/UNLINK add `0` to the sum, and MSET answers `+OK`: a truncated result is
indistinguishable from a real one at the client. The arm should say what actually happened —
a fatal, data-free `PartialResult::ShardError` plus a counter and an `error!` — and the scatter
executor should honor it the way the broadcast gather already honors the identical reply shape
(`scatter/broadcast.rs:68-79`), which today it does not (`scatter/executor.rs:127-129`).

## Files involved (verified paths + current line counts)

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/core/src/shard/vll.rs` | 290 | **Owner.** `handle_vll_execute` (L40–75); the silent-miss arm at **L45–48**; the panic-isolation comment + guard (L50–58); `release_after_execution` (L72). Tests module L102–290: the FM-VLL-003 test (L146–190) and the C3 EXEMPT evidence test (L192–289), which calls `handle_vll_execute` on a never-queued txid at **L250**. |
| `frogdb-server/crates/core/src/shard/dispatch_vll.rs` | 39 | The only production caller: `VllMsg::VllExecute { txid, response_tx } => handle_vll_execute(..)` (L18–20). |
| `frogdb-server/crates/core/src/shard/types.rs` | 1498 | `PartialResult` (L757–800), incl. the `ShardError` variant + its "a `ShardError` is fatal: the coordinator must surface it" doc (L790–799); `impl Default for PartialResult` (L812–820) whose own doc says *"the live call site is the VLL dequeue-miss empty reply (`vll.rs`)"*; `as_shard_error` (L843–848); `into_keyed_results` (L854–859). |
| `frogdb-server/crates/core/src/shard/dispatch_core.rs` | 535 | `scatter_error_reply` (L193–225) — the existing error-shaping helper; unusable on the miss path because it needs the op and keys, which a miss does not have. |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | `recover_from_panic` (L875–905): the metric+log+`Response` pattern this proposal copies (`ShardPanicsIsolated::inc` at L882–886). |
| `frogdb-server/crates/core/src/shard/panic_guard.rs` | 564 | `RecordingRecorder` (L294–315) and FM-VLL-005's forcing test (tag L483, fn L495–563) — the working template for a `frogdb-core` forcing test that asserts a reply shape **and** a counter. |
| `frogdb-server/crates/vll/src/shard.rs` | 1159 | `enqueue_lock_request` (L137–183, `ShardBusy`/`QueueFull` refusals at L145–151 / L158–164); `dequeue_for_execution` (L226–238) — returns `Option<DequeuedOp<O>>`; `abort` (L264–279), whose *own* unknown-txid miss is legitimate (see Problem §3). **Not edited by the recommended shape.** |
| `frogdb-server/crates/vll/src/coordinator.rs` | 886 | `scatter` (L208–310): phase 2 failure aborts all participants (L250–265), phase 3 dispatches execute (L268–290), phase 4 gathers replies (L292–306) and then records `status="success"` (L308). |
| `frogdb-server/crates/server/src/vll_adapter.rs` | 173 | `send_execute` (L117–131) — the only production sender of `VllMsg::VllExecute`. |
| `frogdb-server/crates/server/src/scatter/executor.rs` | 194 | **Second half of the fix.** The reply fold at **L125–129** calls `into_keyed_results()` unconditionally, which returns an empty vec for `ShardError` — so a fatal reply is dropped here. `scatter_error_to_response` (L141–193) is sibling 46's territory. |
| `frogdb-server/crates/server/src/scatter/broadcast.rs` | 1275 | `FatalReply` trait (L68–73) + `impl FatalReply for PartialResult` (L75–79) — the seam that already says "a fatal per-shard reply aborts the gather instead of folding into a truncated success (issue #15 item 4)". The VLL executor is the one gather that does not cross it. |
| `frogdb-server/crates/server/src/scatter/strategies.rs` | 393 | The merges that swallow the empty reply: `MGetStrategy::merge` (L55–72, `unwrap_or(Response::null())` at L68) and `merge_sum_integers` (L153–166). |
| `frogdb-server/crates/types/src/metrics/definitions.rs` | 545 | Where the new counter is declared (`ShardPanicsIsolated` at L147–152 is the shape to copy). |
| `scripts/continuation-lock-gate.py` | 458 | `EXEMPT["VllMsg::VllExecute"]` (L110–118) — the C3 exemption whose *reason string* is literally *"`dequeue_for_execution` returns nothing"*, pinned to the forcing test `vll_execute_cannot_mutate_a_held_key_under_a_foreign_continuation_lock` in `vll.rs`. |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | LOCKED spec. This proposal **appends** `FM-VLL-006` after L109 and edits no existing line (see Risks for the one optional exception). |

## Problem (concrete verified evidence)

### 1. One reply shape, two incompatible meanings

`core/src/shard/vll.rs:45-48`:

```rust
let Some(op) = self.vll.dequeue_for_execution(txid) else {
    let _ = response_tx.send(PartialResult::default());
    return;
};
```

`PartialResult::default()` is `PartialResult::Keyed(Vec::new())` (`types.rs:812-819`). That is the
*same* value a genuinely-executed MGET of zero keys would produce. The interface of
`handle_vll_execute` promises "one `PartialResult` per `VllExecute`", and the caller has no way to
learn which of the two things it got:

| Command | What the coordinator receives from the missing shard | What the client sees |
| --- | --- | --- |
| MGET | `Keyed([])` → empty map for that shard | `nil` for every key on that shard (`strategies.rs:68`) — indistinguishable from "absent" |
| DEL / EXISTS / TOUCH / UNLINK | `Keyed([])` → no integers to fold | `+0` contribution (`strategies.rs:153-166`) — a deletion that did not happen reads as "key was not there" |
| MSET | `Keyed([])` → merge ignores replies entirely | `+OK` (`strategies.rs:132-139`) — a write that never landed reads as committed |

The coordinator is not at fault: phase 4 (`coordinator.rs:292-306`) accepts any `S::Response` as
success, and `record_outcome(.., "success", ..)` fires at L308. The shard told it everything was
fine.

**This is exactly the bug class the `ShardError` variant was introduced for.** Its own doc
(`types.rs:790-799`) says: *"A `ShardError` is fatal: the coordinator must surface it (fail the whole
command) rather than fold it into a truncated success"*, and `FatalReply`
(`broadcast.rs:58-79`) enforces that centrally for every broadcast merge *"present and future"*.
The VLL scatter path predates the rule and never joined it.

### 2. The scatter executor cannot surface a fatal reply even if one is sent

`scatter/executor.rs:125-129`:

```rust
for (shard_id, partial) in outcome.responses {
    shard_results.insert(shard_id, partial.into_keyed_results().into_iter().collect());
}
```

`into_keyed_results()` returns `Vec::new()` for every non-`Keyed` variant (`types.rs:854-859`), so a
`ShardError` reply is silently converted into "this shard returned nothing". Verified: the VLL
executor never calls `as_shard_error()`/`fatal_error()`, while `ScatterGather::run` does
(`broadcast.rs:60-79`). **This is not a live bug today** — the only producer of `ShardError` is
`scatter_error_reply`'s keyless branch (`dispatch_core.rs:220`), and every VLL scatter op is keyed
(MGET/MSET/DEL/EXISTS/TOUCH/UNLINK are the entire strategy set, `strategies.rs`), so `keys` is never
empty on that path. It is, however, the reason a fix that only changes the shard would be invisible.

### 3. `execute` and `abort` are not symmetric, and only one of them may miss silently

`abort(txid)`'s unknown-txid early return (`shard.rs:264-270`) is **correct and load-bearing**: when a
shard refuses a lock request with `ShardBusy` (FM-VLL-001) the coordinator still aborts every
participant by real shard id (`coordinator.rs:250-254`), so the refusing shard legitimately receives
an abort for a txid it never queued. Abort is idempotent by design.

`execute` has no such legitimate miss. Verified caller census: `VllMsg::VllExecute` is constructed in
exactly two places — `server/src/vll_adapter.rs:123` (production) and
`shard-harness/src/sink.rs:65` (the harness sink) — both driven by `VllCoordinator::scatter` phase 3,
which sends execute **only** after every participant answered `Ready` (L246-266), **once** per shard
(L278-290), and never to a shard it has aborted (L268-274, and `abort_shards(&shard_ids[idx..])` at
L285 covers exactly the not-yet-executed suffix). So today a miss means one of: a duplicated execute,
an execute for an aborted txid, a txid mismatch between coordinator and shard, or a message
reordering. **Every one of those is a protocol violation, and the shard's answer to all of them is
"success, no data."**

That is why this is `LATENT` rather than `LIVE`: the arm is unreachable in production today. Its cost
is that it converts *any* future defect that reaches it into silent data loss, and that it removes
the witness that would have caught the defect. The shard event loop is sequential
(`event_loop.rs:35-175`: one `tokio::select!` in a loop, `dispatch_message` awaited inline at L157),
and `handle_vll_execute` dequeues → awaits → releases inside a single dispatch, so there is no
in-flight window that makes the miss benign.

### 4. The C3 exemption's reason rests on this arm — untested

`scripts/continuation-lock-gate.py:110-118` pins `VllMsg::VllExecute` as EXEMPT from
`can_execute_during_lock` with the reason *"…so `dequeue_for_execution` returns nothing and the
hardcoded `conn_id = 0` drain path can never mutate under a foreign lock"*. Its named forcing test
(`vll.rs:211`) drives that path at L250 and asserts only `exec_rx.await.is_ok()` — i.e. *"a value
arrived"*. A mutant that changed what the arm sends would survive it. The safety argument depends on
this arm; the assertion does not describe it.

## Proposed change

**The refusal is host-side, and it is one arm, one reply, one counter.**

### Shape A — recommended: `frogdb-core` + `frogdb-server` only, no `frogdb-vll` edit

1. **`core/src/shard/vll.rs:45-48`** — replace the empty-success arm with a typed refusal:

```rust
let Some(op) = self.vll.dequeue_for_execution(txid) else {
    // Nothing to dequeue means the coordinator and this shard disagree about
    // who holds `txid`. There is no benign cause (unlike `abort`, which the
    // coordinator legitimately sends to shards that refused their lock
    // request), and an empty *successful* reply would fold into the gather as
    // data — a truncated MGET, a DEL that counted 0, an MSET that reports OK.
    // Answer with the fatal, data-free reply shape the merges already treat as
    // an abort, and count it: a non-zero counter is always a bug.
    VllUnheldExecute::inc(self.observability.metrics(), &self.shard_id().to_string());
    tracing::error!(shard_id = self.shard_id(), txid,
        "VllExecute for a transaction this shard is not holding; the scatter was \
         failed rather than answered with partial data — this is always a bug");
    let _ = response_tx.send(PartialResult::shard_error(
        Response::error(UNHELD_EXECUTE_ERROR),           // "ERR VLL execute for an unheld transaction"
    ));
    return;
};
```

   Nothing else in the arm changes: no lock is touched, `release_after_execution` is not called
   (nothing was acquired), and the state machine is left byte-identical.

2. **`types/src/metrics/definitions.rs`** — one typed counter next to `ShardPanicsIsolated`
   (satisfies `lint-metrics-chokepoint`, which forbids raw string-named emission):

```rust
/// Total `VllExecute` messages for a transaction the shard was not holding.
/// Always a coordinator/shard protocol disagreement; a non-zero value is a bug.
counter VllUnheldExecute("frogdb_vll_unheld_execute_total") {
    labels: [shard: &str],
}
```

3. **`server/src/scatter/executor.rs:125-129`** — cross the seam that already exists. `FatalReply`
   is `pub` inside the private `broadcast` module, so the sibling module can use it directly; no
   new interface is introduced:

```rust
use super::broadcast::FatalReply;
…
let mut shard_results = HashMap::with_capacity(outcome.responses.len());
for (shard_id, partial) in outcome.responses {
    if let Some(err) = partial.fatal_error() {
        warn!(conn_id = self.conn_id, txid, shard_id, "fatal shard reply aborted the scatter");
        return err;
    }
    shard_results.insert(shard_id, partial.into_keyed_results().into_iter().collect());
}
```

   Pull the loop out as `fn fold_shard_replies(..) -> Result<HashMap<..>, Response>` so it is unit
   testable without a live shard (see the forcing-test sketch).

4. **`impl Default for PartialResult` (`types.rs:812-820`) loses its only call site** — verified: a
   repo-wide search for `PartialResult::default()` returns exactly one hit, `vll.rs:46`. **Delete
   the impl.** This is the deletion test paying out in the honest direction: the module existed only
   to make "empty success" cheap to say, and it was said in exactly the one place where it was wrong.

**Depth reading.** `handle_vll_execute` is a deep module with one shallow spot: its interface
promises "exactly one `PartialResult` per message" but leaves the *meaning* of one of the two replies
unstated, so every caller — coordinator, executor, merge strategy — has to guess. Making the refusal
a distinct, fatal reply shape moves that fact from prose into the value, and the leverage lands at
the `FatalReply` seam, where six merges (and every future one) already know what to do with it.

### Shape B — deeper, but it collides with siblings: type the outcome in `frogdb-vll`

Change `dequeue_for_execution` to return a named outcome instead of `Option`:

```rust
pub enum DequeueOutcome<O> { Ready(DequeuedOp<O>), UnheldTxid }
```

This would put the classification inside the locked crate and give `FM-VLL-006` weight on the
`frogdb-vll` 0.90 mutation gate. It is **not recommended for this round**: it rewrites
`shard.rs:226-238` — the exact method sibling **45** rewrites (to insert into its `executing` map)
and inside which sibling **51** removes the `self.tx_queue.as_mut()?` unwrap — plus eight in-crate
test call sites (`shard.rs:545, 590, 665, 708, 779, 835, 887, 903`). And the extra information it
buys is nil today: without 45's `executing: HashMap<u64, Vec<Bytes>>`, the state machine cannot
distinguish "never enqueued" from "already dequeued and still executing" either. **Hand the typed
outcome to 45 as a follow-up** (45 is already rewriting that method and already needs a `frogdb-vll`
re-gate); Shape A does not block it and does not pre-empt it.

### The new spec row — `FM-VLL-006`

Appended after L109 (the end of the file), immediately following FM-VLL-005, in the execution-side
half of the spec that begins at the `---` on L97. Verbatim:

```markdown
## FM-VLL-006 — `VllExecute` arrives for a transaction the shard is not holding

| Field | Value |
|---|---|
| Trigger | A `VllMsg::VllExecute` reaches a shard that has no queue entry for its `txid`: the op was never enqueued here (its lock request was refused by [FM-VLL-001](#fm-vll-001--sca-request-refused-while-a-continuation-lock-is-held)/[FM-VLL-004](#fm-vll-004--sca-request-refused-while-a-continuation-request-is-parked) or with `QueueFull`), it was already aborted, or an earlier `VllExecute` for the same txid already dequeued it. `dequeue_for_execution` returns nothing. There is no benign cause: `VllCoordinator::scatter` sends execute once per participant, only after every participant answered `Ready`, and never to a shard it has aborted — so a miss is always a coordinator/shard protocol disagreement. |
| Observable | The op's coordinator receives a fatal, data-free `PartialResult::ShardError` for this shard — the same reply shape the keyless-scatter rejection uses, so the gather recognizes it — carrying `ERR VLL execute for an unheld transaction`, and the scatter fails with that error instead of merging. `frogdb_vll_unheld_execute_total{shard}` increments and one `error!` names the shard and the txid. The shard answers the next message normally. |
| NOT observable | An empty *successful* reply folding into the merge as data — MGET would report this shard's keys as `nil`, DEL/EXISTS/TOUCH/UNLINK would add `0` to the sum and MSET would answer `+OK`, and no client could tell a truncated result from a real one; the response channel being dropped instead of answered (that is `ScatterError::ResultChannelClosed`, a different mode with a different cause); any lock, intent or outstanding-op count created or released by the miss — nothing was dequeued, so `release_after_execution` must not run and `is_drained()` must be unchanged; the shard stalling, or the reply carrying another txid's data. |
| Invariant | The arm is total and side-effect-free: every path out of `handle_vll_execute` (`frogdb-server/crates/core/src/shard/vll.rs`) sends exactly one `PartialResult`, and the miss path calls neither `execute_scatter_part` nor `release_after_execution`, leaving the `VllShardState` byte-identical to what it was before the message arrived. The refusal survives the gather because the scatter executor checks `FatalReply::fatal_error()` before folding, the same rule `ScatterGather::run` applies to every broadcast merge. `abort` deliberately keeps its silent unknown-txid no-op: the coordinator legitimately aborts participants that refused their lock request, so a miss there is expected. |
| Outcome variant | n/a — no lock was acquired or released; the failure is reported on the result channel (`PartialResult::ShardError`), not on `ShardReadyResult`. |
| Forced by | `vll_execute_for_an_unheld_transaction_is_refused_not_silently_empty`, `a_fatal_shard_reply_aborts_the_scatter_instead_of_folding` |
| Bug refs | none |
```

Row schema verified against `scripts/failure-modes.py:114-122` (all seven `REQUIRED_FIELDS` present)
and the numbering is sequential, so the script's per-area gap warning stays quiet.

### Forcing-test sketch (red first, then green)

**Test 1 — `frogdb-core`, in `core/src/shard/vll.rs`'s tests module** (next to the code it pins).
`test_worker()` (L119-135) currently hardcodes `NoopMetricsRecorder`; add a
`test_worker_with(recorder)` variant and keep `test_worker()` delegating to it, mirroring
`panic_guard.rs`'s `worker_with` + `RecordingRecorder` (L294-315).

```rust
// FM-VLL-006
#[tokio::test]
async fn vll_execute_for_an_unheld_transaction_is_refused_not_silently_empty() {
    let recorder = Arc::new(RecordingRecorder::default());
    let mut worker = test_worker_with(recorder.clone());

    // No lock request was ever enqueued for 4242 on this shard.
    let (tx, rx) = oneshot::channel();
    worker.handle_vll_execute(4242, tx).await;

    match rx.await.expect("every path must answer exactly once") {
        PartialResult::ShardError(err) => assert_eq!(
            error_text(&err), "ERR VLL execute for an unheld transaction"),
        other => panic!("an unheld execute must not answer with data: {other:?}"),
    }
    assert_eq!(recorder.counter_value(UNHELD_METRIC), Some(1));

    // Nothing was acquired, so nothing was released: the shard is untouched and
    // a real op on any key is still grantable and still executes.
    let (ready_tx, ready_rx) = oneshot::channel();
    worker.handle_vll_lock_request(1, vec![key.clone()], LockMode::Write,
        ScatterOp::MSet { pairs: vec![(key.clone(), Bytes::from_static(b"v"))] }, ready_tx).await;
    assert!(matches!(ready_rx.await, Ok(ShardReadyResult::Ready)));
    let (exec_tx, exec_rx) = oneshot::channel();
    worker.handle_vll_execute(1, exec_tx).await;
    assert!(matches!(exec_rx.await.expect("vll reply"), PartialResult::Keyed(_)));
    assert!(worker.store.contains(key.as_ref()));
    assert_eq!(recorder.counter_value(UNHELD_METRIC), Some(1),
        "a healthy op must not add to the unheld counter");
}
```

**RED today** on the first `match`: the arm sends `Keyed([])`, so the test panics with
*"an unheld execute must not answer with data: Keyed([])"*; the counter assertion fails too
(`None`, and the metric does not exist).

**Test 2 — `frogdb-server`, in `scatter/executor.rs`'s tests module** (new module; the file has no
tests today), over the extracted fold:

```rust
// FM-VLL-006
#[test]
fn a_fatal_shard_reply_aborts_the_scatter_instead_of_folding() {
    let responses = vec![
        (0, PartialResult::keyed(vec![(Bytes::from_static(b"a"), Response::bulk(..))])),
        (1, PartialResult::shard_error(
                Response::error("ERR VLL execute for an unheld transaction"))),
    ];
    match fold_shard_replies(responses) {
        Err(err) => assert_eq!(error_text(&err), "ERR VLL execute for an unheld transaction"),
        Ok(folded) => panic!("a fatal reply must not fold into a result: {folded:?}"),
    }
}
```

**RED today**: `into_keyed_results()` turns the `ShardError` into an empty map, the fold succeeds,
and an MGET over those two shards answers `[a, nil]`.

**Both tests live in crates `scripts/failure-modes.py` lists** (`NEXTEST_CRATES`, L64-77: `frogdb-core`
and `frogdb-server` are both there), so `lint-failure-modes` resolves the `Forced by` names in both
directions. Note the constraint that shapes this choice: **`frogdb-shard-harness` is not in that
list**, so the harness — which can drive a real `ShardWorker` through a real coordinator and is the
most natural place for an end-to-end version — cannot be named in a `Forced by` cell. A harness
scenario is still worth adding; it just cannot be the row's witness.

## Testability improvement

- **A silent path becomes an asserted one.** There is currently no test anywhere that asserts what a
  dequeue-miss replies. The nearest thing, the C3 evidence test (`vll.rs:250-251`), asserts only that
  *some* value arrived. Every mutant of the miss arm survives today; after the change the reply
  shape, the error text and the counter are all pinned.
- **The C3 EXEMPT pin gets real evidence.** `scripts/continuation-lock-gate.py:110-118` justifies the
  `VllMsg::VllExecute` exemption with *"`dequeue_for_execution` returns nothing"*. Tightening
  `vll_execute_cannot_mutate_a_held_key_under_a_foreign_continuation_lock` (L250-255) from
  `assert!(exec_rx.await.is_ok())` to *"the refused foreign op's execute is answered with the unheld
  refusal, and the store is untouched"* converts a negative assertion (nothing happened) into a
  positive one (this specific thing happened) — the difference between a test a mutant can walk past
  and one it cannot.
- **The deletion test resolves cleanly.** `impl Default for PartialResult` disappears with its only
  caller. After that, no code anywhere can produce "empty success" by writing `..Default::default()`
  — the shape must be spelled out, which is the point.
- **The `FatalReply` invariant stops having an exception.** Its doc claims a new merge *"cannot
  silently reintroduce the swallow bug (issue #15 item 4)"*. That is true for broadcast merges and
  false for the VLL executor. After this change the claim is true as written, and test 2 is the pin.

## Risks / scope boundaries vs sibling proposals

**Ordering (all four round-38 VLL proposals, verified against their current on-disk text):**

| Proposal | Overlap with 52 | Required order |
| --- | --- | --- |
| **51** txn-slot-vll-state-small | **None** under Shape A — 52 touches no file in `frogdb-vll`. 51 keeps `dequeue_for_execution`'s signature and semantics identical (it only removes the `Option` unwrap at L227). 51 owns `vll-failure-modes.md:46`; 52 appends after L109. 51's own sibling table already records "None" from its side. | **51 first** (it is the round's designated first lander), then 52 — but they are commutative. |
| **45** vll-key-ownership-diagnostics | **CONFLICT (same function).** 45 variant (a) edits `core/src/shard/vll.rs:72` (drop the `&op.keys` argument); 52 rewrites L45–48 of the same function. A few lines apart, disjoint hunks. 45 variant (b) restructures `handle_vll_execute` around `run_dequeued`, which **swallows the `else` arm entirely** — under (b) the two must be one change. 45's table records this and names variant (a) the safe default; accepted and restated from this side. Spec: 45 owns L20–23, :69, :105–106; 52 owns only the appended row. | **52 before 45** under variant (a). Under variant (b): merge them, or defer (b) to a follow-up issue (45's own recommendation). |
| **46** vll-acquire-error-unify | **Adjacent, disjoint.** Both edit `scatter/executor.rs`: 46 owns `scatter_error_to_response` (L141–193), 52 owns the reply fold (L125–129). No shared line. 46 unifies the **acquisition** error surface (`ScatterError`/`ContinuationError` → `VllAcquireError::to_response`); `FM-VLL-006` reports on the **result** channel, so no unification is implied and no coordinator error enum changes. Spec: 46 owns the preamble (L14–18, L26–30) and FM-VLL-001–004's `Observable`/`Outcome variant` fields; 52's row is append-only after L109. | Either order. If 46 lands first, re-check that `ERR VLL execute for an unheld transaction` still reads consistently with whatever prefix convention 46 settles on. |

**Locked-area landing steps:**

1. **Spec-first, in order**: append the `FM-VLL-006` row → write both forcing tests (they fail) →
   land the arm + counter + executor fold (they pass).
2. `just lint-failure-modes` after the spec edit; it is part of `just lint` and checks both
   directions, so the `// FM-VLL-006` tags and the `Forced by` names must be added together.
3. **No `frogdb-vll` re-gate is owed under Shape A** — no line of `frogdb-vll` changes. This is worth
   stating explicitly because the row lives in the VLL spec: the spec's scope section
   (`vll-failure-modes.md:19-23`) already extends to `core/src/shard/vll.rs`, and FM-VLL-005 is the
   precedent for a VLL row whose only forcing test lives in `frogdb-core`
   (`panic_guard.rs:483`). The flip side, stated plainly: **`FM-VLL-006` contributes nothing to the
   `frogdb-vll` 0.90 gate**, because `cargo mutants -p frogdb-vll` runs only that package's tests.
   Shape B is what would change that, and it belongs with 45.
4. **Optional preamble edit, deliberately declined.** The scope carve-out at L19–23 names the
   dequeue/release *pairing* specifically. `FM-VLL-006` is the other branch of the same call in the
   same file, so it could justify widening L23 to *"…as is the answer the host owes when there is
   nothing to dequeue (FM-VLL-006)"*. **That line belongs to 45's block** (45 rewrites L20–23
   wholesale). Recommendation: skip the edit — the row establishes itself, exactly as FM-VLL-005
   does — and if a reviewer wants it, sequence it after 45.
5. **Do not rename `vll_execute_cannot_mutate_a_held_key_under_a_foreign_continuation_lock`.**
   `scripts/continuation-lock-gate.py:116` pins the name and the file (`vll.rs`); `just lint-gates`
   runs on every commit. Tightening its assertions and prose is fine; its EXEMPT reason string
   (L112–115) should also gain "…and the drain path refuses the miss rather than answering empty",
   which is a `scripts/` edit in the same commit.

**Other risks:**

- **A new error reaches clients on a path that previously "succeeded."** That is the change, and it
  is only reachable through a protocol violation (Problem §3), so no legitimate workload regresses.
  The one deliberate caller of the miss path is the C3 test.
- **New metric ⇒ generated-artifact refresh.** `definitions.rs` feeds `website/src/data/metrics.json`
  via `frogdb-server/ops/docs-gen`; run `just docs-gen` (verified recipe, `Justfile:812`) and let
  `docs-gen-check` gate it in CI. A Grafana panel is optional — the dashboards
  (`ops/grafana/frogdb-overview.json`, and the helm copy) are generated, so any panel goes in the
  generator, never the JSON.
- **Known inaccuracy left in place, named rather than hidden.** With the executor returning an error
  after phase 4 completed, `VllCoordinator::scatter` has already recorded
  `frogdb_scatter_gather_total{status="success"}` (`coordinator.rs:308`). Fixing it means teaching
  the coordinator about fatal replies — `frogdb-vll`, locked, and squarely inside sibling 46's
  error-surface rewrite. Flag it in the PR and file it against 46 or as a follow-up; do not smuggle a
  coordinator change into this one.
- **Explicitly out of scope: `TransactionQueue::enqueue` overwrites a duplicate txid.**
  `queue.rs:102-108` is `pending.insert(op.txid, op)`, which silently replaces an existing op and
  drops its `ready_tx`. Adjacent hygiene, unreachable today (txids come from a process-wide
  `AtomicU64`, `server::next_txid`), and it lives in `frogdb-vll` where 45/51 are working. Not rowed
  here.
- **Harness coverage is a bonus, not the witness.** `frogdb-shard-harness` (`sink.rs:65` already
  sends `VllExecute`) can drive the end-to-end version through a real `ShardWorker`; verified that no
  existing scenario sends an execute for an aborted or never-queued txid, so nothing there breaks.
  It cannot be named in `Forced by` (see above).

## Effort estimate

**S.** One `else` arm (~10 lines), one metric definition, one extracted fold plus a `fatal_error()`
check in the executor, one `Default` impl deleted, two new forcing tests, one existing test tightened
and one gate reason string extended, one appended spec row, one `just docs-gen`. No `frogdb-vll` line
changes and therefore no mutation re-gate; `just lint-failure-modes` and `just lint-gates` are the
gates that matter.

### Independently-landable prerequisite

**The executor's `FatalReply` check (step 3) can land on its own, ahead of everything else, with no
spec row.** It is a pure no-op today — verified that no VLL scatter op is keyless, so
`scatter_error_reply` never produces `ShardError` on that path — and it touches neither a locked
crate nor the spec. Landing it first buys two things: the `FatalReply` invariant becomes true as its
doc already claims, and the shard-side arm in step 1 then lands as a self-contained change whose
effect is immediately visible end to end. It edits `scatter/executor.rs:125-129` only, so it does not
collide with 46 (which owns L141–193 of the same file) — sequence the two rather than landing them
simultaneously.
