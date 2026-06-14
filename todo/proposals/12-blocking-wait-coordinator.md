# Proposal: Blocking Wait Coordinator

Status: proposed
Date: 2026-06-13

## Problem

"A client blocks on a key until a value arrives, a deadline elapses, or it is
externally unblocked" is one load-bearing concept in FrogDB — it underpins
BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP, XREAD/XREADGROUP BLOCK, and
WAIT, and is the natural home for future WAIT/replication-ACK and WAITAOF waits.
Yet that concept has no module and no interface. It is smeared across three
layers that talk to each other through an *implicit* contract:

1. **Server handler** (`frogdb-server/crates/server/src/connection/handlers/blocking.rs:33-177`)
   owns wait setup, the `select!` over response/unblock/deadline, blocked-state
   bookkeeping, and the unregister-on-cleanup. The state machine of "what ended
   this wait" is a private `WaitOutcome` enum (`blocking.rs:17-26`) buried in the
   handler, and the deadline future, the CLIENT UNBLOCK future, and the response
   channel are wired together inline.
2. **Connection state** (`frogdb-server/crates/server/src/connection/state.rs:129-136`)
   holds `BlockedState { shard_id, keys }`, mutated by hand from the handler
   (`blocking.rs:98-104`, cleared at `:138-141`), with no transition method —
   unlike every other field on `ConnectionState`, which was recently
   encapsulated behind named transitions (transactions, pub/sub, reply mode).
3. **Internal shard** (`frogdb-server/crates/core/src/shard/blocking.rs:220-662`)
   owns *satisfaction*: three near-parallel `try_satisfy_*_waiters` methods plus
   a recursive BLMOVE fan-out (`try_satisfy_list_waiters_with_depth`,
   `:234-412`) with a depth-16 cap (`MAX_BLMOVE_FANOUT_DEPTH`, `:21`). These reach
   directly into `self.wait_queue`, `self.store`, stream consumer-group state,
   and the metrics recorder through an interface that exists only as a habit.

This is the classic shallow-module-with-no-seam diagnosis. The *interface* of the
blocking concept — "register a wait", "what ended it", "did the key become
satisfiable", "produce the reply" — is never named; it is reconstructed inline at
every layer. The two real decisions (server: *which event won the race*; core:
*is the key satisfiable and what is the reply*) are interleaved with I/O,
bookkeeping, and metrics, so neither can be exercised on its own.

**Deletion test.** Try to delete the blocking-wait abstraction and replace it
with a smaller one. You cannot, because there is nothing to delete — there is no
type, no trait, no module that *is* the abstraction. `WaitOutcome` is the closest
thing to an interface, and it is a private enum with no methods. A concept this
central having no deletable surface is the tell that the seam is missing, not
merely badly drawn.

Verified evidence:

| Symptom | Where |
|---------|-------|
| Wait setup + `select!` + cleanup in one async fn (~145 lines) | `connection/handlers/blocking.rs:33-177` |
| `WaitOutcome` is a private handler-local enum, no methods | `connection/handlers/blocking.rs:17-26` |
| `BlockedState` mutated by hand (no transition method) | `connection/state.rs:129-136`; set `blocking.rs:98-104`, cleared `:138-141` |
| Three parallel satisfaction methods reaching into store + wait_queue | `core/src/shard/blocking.rs:223,417,547` |
| Recursive BLMOVE fan-out + depth cap inline in the list satisfier | `core/src/shard/blocking.rs:234-412`, cap at `:21` |
| Two independent timeout authorities (server `select!` + shard 100 ms tick) | `connection/handlers/blocking.rs:133` and `core/src/shard/event_loop.rs:23,73` |
| `ClientHandle::clear_unblock` is a no-op with a misleading comment | `core/src/client_registry/mod.rs:339-341` |

**Untestable without a live socket.** Because the race-selection logic is welded
to `self.core.shard_senders`, `self.admin.client_registry`, and
`self.client_handle`, the only way to test "timeout beats response" or "CLIENT
UNBLOCK beats both" today is to stand up a connection over a real channel and a
running shard worker. The satisfaction logic is likewise reachable only through a
`ShardWorker` with a populated store. Both behaviors are integration-only.

## Current state

### Server-side: setup + race `select!` + cleanup (`connection/handlers/blocking.rs:69-177`)

The whole lifecycle is one inline block. Note the hand-rolled deadline future,
the `biased` race, the two outcome conversions to `Response`, and the
unregister-on-cleanup duplicated across two match arms:

```rust
// Defensively clear any stale CLIENT UNBLOCK signal from a previous
// blocking command so the new wait starts fresh.
self.admin.client_registry.reset_unblock(self.state.id);

// Create response channel
let (response_tx, response_rx) = oneshot::channel();

// Send BlockWait message to shard
let sender = match self.core.shard_senders.get(target_shard) {
    Some(s) => s,
    None => return Response::error("ERR Internal error: invalid shard"),
};

if sender
    .send(ShardMessage::BlockWait {
        conn_id: self.state.id,
        keys: keys.clone(),
        op,
        response_tx,
        deadline,
        protocol_version: self.state.protocol_version,
    })
    .await
    .is_err()
{
    return Response::error("ERR Internal error: shard unreachable");
}

// Update blocked state
self.state.blocked = Some(BlockedState {
    shard_id: target_shard,
    keys: keys.clone(),
});
self.admin
    .client_registry
    .update_blocked_state(self.state.id, true);

// Wait for either a shard response, a CLIENT UNBLOCK signal, or the
// timeout deadline.
let outcome: WaitOutcome = {
    let timeout_fut = async {
        match deadline {
            Some(d) => tokio::time::sleep_until(d.into()).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(timeout_fut);

    tokio::select! {
        biased;
        recv = response_rx => match recv {
            Ok(resp) => WaitOutcome::Response(resp),
            Err(_) => WaitOutcome::Response(Response::Null),
        },
        mode = self.client_handle.unblocked() => match mode {
            Some(m) => WaitOutcome::Unblocked(m),
            None => WaitOutcome::Response(Response::Null),
        },
        _ = &mut timeout_fut => WaitOutcome::Timeout,
    }
};

// Clear blocked state.
self.state.blocked = None;
self.admin
    .client_registry
    .update_blocked_state(self.state.id, false);
self.admin.client_registry.reset_unblock(self.state.id);

match outcome {
    WaitOutcome::Response(resp) => resp,
    WaitOutcome::Timeout => {
        if let Some(sender) = self.core.shard_senders.get(target_shard) {
            let _ = sender
                .send(ShardMessage::UnregisterWait { conn_id: self.state.id })
                .await;
        }
        Response::Null
    }
    WaitOutcome::Unblocked(mode) => {
        if let Some(sender) = self.core.shard_senders.get(target_shard) {
            let _ = sender
                .send(ShardMessage::UnregisterWait { conn_id: self.state.id })
                .await;
        }
        match mode {
            UnblockMode::Timeout => Response::Null,
            UnblockMode::Error => {
                Response::error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
            }
        }
    }
}
```

Three observations the design must address:

- The `Response::Null` returned on every timeout/unblock path
  (`blocking.rs:125,130,157,170`) is **op-blind**: the outcome has thrown away
  which op was waiting, so it cannot choose the right RESP nil shape. (See
  *Correctness flags* — BLPOP's timeout reply is wire-wrong in RESP2.)
- The `UnregisterWait` send is duplicated across the Timeout and Unblocked arms,
  and the Response arm relies (correctly, but only by luck of the channel being
  dropped) on the shard already having removed the entry.
- The server-side `timeout_fut` is a *second* timeout authority competing with
  the shard's own expiry tick (below) — a race, not a redundancy.

### Internal shard: recursive satisfaction + depth cap (`core/src/shard/blocking.rs:223-412`)

The list satisfier is the deepest of the three. It reaches into `wait_queue`,
`store`, and (transitively) the metrics recorder, and recurses into itself for
BLMOVE destinations with a hand-carried depth counter:

```rust
pub fn try_satisfy_list_waiters(&mut self, key: &Bytes) {
    self.try_satisfy_list_waiters_with_depth(key, 0);
}

fn try_satisfy_list_waiters_with_depth(&mut self, key: &Bytes, depth: usize) {
    if depth >= MAX_BLMOVE_FANOUT_DEPTH {
        tracing::warn!(/* ...depth cap hit... */);
        return;
    }

    while self.wait_queue.has_waiters_for_kind(key, WaiterKind::List) {
        let has_data = self.list_is_non_empty(key);
        if !has_data {
            break;
        }

        let entry = match self
            .wait_queue
            .pop_oldest_waiter_of_kind(key, WaiterKind::List)
        {
            Some(e) => e,
            None => break,
        };

        let mut recurse_dest: Option<Bytes> = None;

        let response = match &entry.op {
            BlockingOp::BLPop => {
                if let Some(value) = self
                    .store
                    .get_mut(key)
                    .and_then(|v| v.as_list_mut())
                    .and_then(|l| l.pop_front())
                {
                    self.cleanup_empty_list(key);
                    self.increment_version();
                    Response::Array(vec![Response::bulk(key.clone()), Response::bulk(value)])
                } else {
                    continue; // List became empty, try next waiter
                }
            }
            BlockingOp::BLMove { dest, src_dir, dest_dir } => {
                // ...wrong-type check on dest, pop from src, push to dest...
                recurse_dest = Some(dest.clone());
                Response::bulk(value)
            }
            // BRPop, BLMPop arms elided...
            _ => { debug_assert!(false, "..."); continue; }
        };

        self.complete_blocked_waiter(entry, response);

        if let Some(dest) = recurse_dest {
            self.try_satisfy_list_waiters_with_depth(&dest, depth + 1);
        }
    }
}
```

`try_satisfy_zset_waiters` (`:417-542`) and `try_satisfy_stream_waiters`
(`:547-662`) repeat the same skeleton — `while has_waiters_for_kind { check
non-empty; pop oldest; match op; complete }` — diverging only in the op match and
in the stream variant's extra deleted/wrong-type/NOGROUP draining
(`:554-577`, `drain_stream_waiters_*` at `:694-720`). The shared control flow
(loop, pop, complete, metrics) is copied three times; only the per-op body and
the cascade differ. The implicit interface each method assumes — *given a key
and a popped `WaitEntry`, is the key satisfiable, and what reply does this op
produce, and where (if anywhere) does the wake cascade* — is exactly the seam
that is missing.

The single satisfaction trigger that fans into these three is already a clean
seam by comparison (`core/src/shard/post_execution.rs:286-311`): a write
command declares `WaiterWake::{None,Kind,All}` and `satisfy_waiters` dispatches
by `WaiterKind`. The *dispatch* is declarative; the *satisfaction* underneath it
is not.

### State: hand-mutated `BlockedState` (`connection/state.rs:129-136`)

```rust
/// Blocked state for connections waiting on blocking commands.
#[derive(Debug, Clone)]
pub struct BlockedState {
    /// Shard ID where the wait is registered.
    pub shard_id: usize,
    /// Keys the client is waiting on.
    pub keys: Vec<Bytes>,
}
```

`ConnectionState` recently moved transactions, pub/sub, reply mode, and cluster
flags behind named transition methods with private fields (see proposal
04). `blocked` is the lone holdout: a `pub` field poked directly from the handler
(`self.state.blocked = Some(...)` / `= None`), with no `begin_block` /
`end_block` transition and no test coverage in `state.rs`'s unit suite.

## Proposed design

Two seams, each owning one of the two real decisions.

### Server side — `BlockingWaitCoordinator` (owns the race, makes `WaitOutcome` public)

A small module `connection/handlers/blocking/coordinator.rs` whose single job is
the event race. It does **no** registration, bookkeeping, or unregistration —
those stay with the handler, which owns the shard senders and registry. The
coordinator is a pure async decision over three inputs, which makes it
unit-testable with an in-memory `oneshot` and a mock unblock source.

```rust
/// Outcome of a blocking wait. Public so it can be asserted in unit tests and
/// converted to a reply with op-aware nil shaping.
#[derive(Debug)]
pub enum WaitOutcome {
    /// The shard delivered a reply (or the channel closed).
    Response(Response),
    /// The deadline elapsed.
    Timeout,
    /// CLIENT UNBLOCK signalled this connection.
    Unblocked(UnblockMode),
}

/// The CLIENT UNBLOCK edge, as a seam. `ClientHandle` is the production adapter;
/// tests supply a mock that fires on command. This is the adapter that decouples
/// the race from the registry/watch-channel machinery.
pub trait UnblockSignal {
    /// Resolves when CLIENT UNBLOCK targets this connection.
    async fn unblocked(&mut self) -> Option<UnblockMode>;
}

impl UnblockSignal for ClientHandle {
    async fn unblocked(&mut self) -> Option<UnblockMode> {
        ClientHandle::unblocked(self).await
    }
}

pub struct BlockingWaitCoordinator;

impl BlockingWaitCoordinator {
    /// Race the shard response, CLIENT UNBLOCK, and the deadline. Pure: the
    /// caller owns register/cleanup. `deadline = None` blocks forever.
    pub async fn wait_for_response(
        response_rx: oneshot::Receiver<Response>,
        deadline: Option<Instant>,
        unblock: &mut impl UnblockSignal,
    ) -> WaitOutcome {
        let timeout_fut = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d.into()).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);

        tokio::select! {
            biased;
            recv = response_rx => match recv {
                Ok(resp) => WaitOutcome::Response(resp),
                Err(_) => WaitOutcome::Response(Response::Null),
            },
            mode = unblock.unblocked() => match mode {
                Some(m) => WaitOutcome::Unblocked(m),
                None => WaitOutcome::Response(Response::Null),
            },
            _ = &mut timeout_fut => WaitOutcome::Timeout,
        }
    }
}

impl WaitOutcome {
    /// Convert to the client reply, choosing the nil shape from the op so an
    /// array-returning op (BLPOP, BZPOPMIN, XREAD, ...) times out with a null
    /// *array* and a single-value op (BLMOVE, BRPOPLPUSH) with a null *bulk*.
    /// This is the one place the wrong-nil-shape bug is fixed.
    pub fn into_response(self, op: &BlockingOp) -> Response {
        match self {
            WaitOutcome::Response(resp) => resp,
            WaitOutcome::Timeout => op.timeout_reply(),
            WaitOutcome::Unblocked(UnblockMode::Timeout) => op.timeout_reply(),
            WaitOutcome::Unblocked(UnblockMode::Error) => {
                Response::error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
            }
        }
    }
}
```

`BlockingOp::timeout_reply()` lives next to `BlockingOp` in core and returns
`Response::NullArray` for the multi-bulk ops and `Response::Null` for
BLMOVE/BRPOPLPUSH — the single audited site for "what does a timed-out *X* look
like on the wire". The shard's coarse timeout (`check_waiter_timeouts`) calls the
same helper, so both timeout authorities agree.

A thin `BlockedState` transition pair on `ConnectionState` removes the last
hand-mutated field (matching proposal 04):

```rust
impl ConnectionState {
    /// Enter the blocked state for a wait registered on `shard_id`.
    pub fn begin_block(&mut self, shard_id: usize, keys: Vec<Bytes>) { /* ... */ }
    /// Leave the blocked state. Returns the prior `BlockedState`, if any.
    pub fn end_block(&mut self) -> Option<BlockedState> { /* ... */ }
}
```

### Core side — `WaiterSatisfaction` strategy seam

The recursive driver, the depth cap, the loop, `complete_blocked_waiter`, and the
metrics move into **one** generic method. The per-op "is this satisfiable / what
is the reply / where does the wake cascade" logic becomes a strategy with an
*explicit* interface — store and key in, reply (and optional cascade target) out:

```rust
/// What a satisfaction attempt produced for one popped waiter.
pub enum Satisfaction {
    /// Reply produced; `cascade` is a follow-up key whose waiters must also be
    /// woken (BLMOVE/BRPOPLPUSH destination), or `None`.
    Done { reply: Response, cascade: Option<Bytes> },
    /// The key is no longer satisfiable for this waiter (lost a race to an
    /// earlier waiter); requeue/skip and re-loop.
    Retry,
    /// A terminal reply that consumed nothing (WRONGTYPE, NOGROUP); deliver and
    /// drop the waiter without touching the value.
    Reject(Response),
}

/// Strategy for one waiter kind. The store is the only collaborator it sees —
/// the wait_queue, recursion, metrics, and depth cap are the driver's job.
pub trait WaiterSatisfaction {
    /// Which waiter kind this strategy drives.
    fn kind(&self) -> WaiterKind;
    /// Does `key` currently hold data a waiter of this kind could consume?
    fn is_satisfiable(&mut self, store: &mut dyn Store, key: &Bytes) -> bool;
    /// Execute `entry.op` against the store for `key`.
    fn satisfy(&mut self, store: &mut dyn Store, key: &Bytes, entry: &WaitEntry) -> Satisfaction;
}
```

The driver owns everything that is the *same* across the three families today:

```rust
impl ShardWorker {
    fn drive_satisfaction(&mut self, strat: &mut dyn WaiterSatisfaction, key: &Bytes, depth: usize) {
        if depth >= MAX_BLMOVE_FANOUT_DEPTH {
            tracing::warn!(/* depth cap */);
            return;
        }
        let kind = strat.kind();
        while self.wait_queue.has_waiters_for_kind(key, kind) {
            if !strat.is_satisfiable(&mut self.store, key) {
                break;
            }
            let Some(entry) = self.wait_queue.pop_oldest_waiter_of_kind(key, kind) else { break };
            match strat.satisfy(&mut self.store, key, &entry) {
                Satisfaction::Retry => continue,
                Satisfaction::Reject(reply) => self.complete_blocked_waiter(entry, reply),
                Satisfaction::Done { reply, cascade } => {
                    self.increment_version();
                    self.complete_blocked_waiter(entry, reply);
                    if let Some(dest) = cascade {
                        self.drive_satisfaction(strat, &dest, depth + 1);
                    }
                }
            }
        }
    }
}
```

`ListSatisfaction`, `ZsetSatisfaction`, and `StreamSatisfaction` carry only the
op match. The depth cap, the FIFO loop, the wake-cascade recursion, and the
metrics now exist exactly once. `satisfy_waiters` (`post_execution.rs:302-311`)
keeps its declarative `WaiterKind` dispatch but constructs the strategy instead
of calling three bespoke methods.

### Before / after: the BLPOP handler

Before, `handle_blocking_wait` is the ~145-line block in *Current state*. After,
it is a register → coordinate → cleanup skeleton, with the race and the nil
shaping behind the seam:

```rust
pub(crate) async fn handle_blocking_wait(
    &mut self,
    keys: Vec<Bytes>,
    timeout: f64,
    proto_op: frogdb_protocol::BlockingOp,
) -> Response {
    if let frogdb_protocol::BlockingOp::Wait { num_replicas, timeout_ms } = proto_op {
        return self.handle_wait_command(num_replicas, timeout_ms).await;
    }
    let op = convert_blocking_op(proto_op);
    if keys.is_empty() {
        return Response::error("ERR No keys provided for blocking command");
    }
    let target_shard = shard_for_key(&keys[0], self.num_shards);
    let deadline = (timeout > 0.0).then(|| Instant::now() + Duration::from_secs_f64(timeout));

    // Register (sends BlockWait, sets blocked state, resets stale unblock).
    let response_rx = match self.register_wait(target_shard, &keys, op.clone(), deadline).await {
        Ok(rx) => rx,
        Err(resp) => return resp,
    };

    let outcome = BlockingWaitCoordinator::wait_for_response(
        response_rx, deadline, &mut self.client_handle,
    ).await;

    // Cleanup (clears blocked state, resets unblock, unregisters iff still registered).
    self.cleanup_wait(target_shard, &outcome).await;

    outcome.into_response(&op)
}
```

`register_wait` and `cleanup_wait` are private handler methods (they still own
`shard_senders` and `client_registry`); the *decision* is the coordinator's, the
*reply shaping* is `WaitOutcome::into_response`'s, and the *satisfaction* is the
strategy's. The handler is now a sequence of named steps with no `select!`,
no inline deadline future, and no duplicated unregister.

### Why this is the right depth

- **Locality.** The race-selection logic lives in `BlockingWaitCoordinator` and
  nowhere else; the satisfaction control flow (loop + depth cap + cascade +
  metrics) lives in `drive_satisfaction` and nowhere else. The nil-shape decision
  lives in `BlockingOp::timeout_reply`. A change to any one of these is a
  one-place edit, not a sweep across handler + state + three core methods.
- **Leverage.** Three ~95-line satisfaction methods collapse to one driver plus
  three op-only strategies; the handler sheds ~145 lines to a skeleton; and a
  whole class of wire-shape bug is closed because the op survives to the reply.
  Every future blocking primitive (WAIT/WAITAOF, replication-ACK, a future
  cross-shard wait) reuses the coordinator instead of re-deriving the `select!`.
- **Deletion test.** Unlike today, there is now something to delete: the private
  `WaitOutcome` enum and inline `select!` are replaced by a named, public,
  method-bearing type; the three `try_satisfy_*` bodies are replaced by strategy
  impls. If the migration could not delete the inline `select!` and at least two
  of the three duplicated loops, the seam would be the wrong shape.
- **Not an adapter for its own sake.** `UnblockSignal` is the only new trait, and
  it exists to cut the one dependency (the registry watch channel) that blocks
  unit testing — it is a seam, not a wrapper layer the handler may or may not
  use. `WaiterSatisfaction` deepens the existing `WaiterKind` dispatch that
  `post_execution.rs` already proved out, rather than introducing a parallel one.

## Migration plan

Behavior-preserving except where a *Correctness flag* is explicitly fixed; each
phase compiles and tests green on its own.

1. **Phase 0 — public outcome + nil helper (no behavior change yet except the
   nil fix).** Move `WaitOutcome` to a `coordinator` module, make it `pub`, add
   `WaitOutcome::into_response(op)` and `BlockingOp::timeout_reply()`. Point both
   the handler's timeout/unblock arms (`blocking.rs:148-176`) and the shard's
   `check_waiter_timeouts` (`core/src/shard/blocking.rs:149`) at the helper. This
   single phase fixes the RESP nil-shape flag (below) and adds the per-op
   `timeout_reply` unit tests. `just test frogdb-server`, `just test frogdb-core`.
2. **Phase 1 — extract `BlockingWaitCoordinator::wait_for_response` + `UnblockSignal`.**
   Replace the inline `select!` with the coordinator call; add the `UnblockSignal`
   trait and impl for `ClientHandle`. Add `begin_block`/`end_block` on
   `ConnectionState` and route `register_wait`/`cleanup_wait` through them. Lower
   risk: server-only, no core changes. Add the mock-channel selection unit tests.
3. **Phase 2 — collapse satisfaction into `drive_satisfaction` + strategies.**
   Introduce `WaiterSatisfaction`/`Satisfaction` and the generic driver; port
   `List` first (it has the cascade and the depth cap), then `SortedSet`, then
   `Stream` (carrying its deleted/wrong-type/NOGROUP draining into the strategy).
   Delete the three `try_satisfy_*_with_depth` bodies as each port lands. The
   Redis-compat integration suite is the behavioral net.
4. **Phase 3 — resolve the dual-timeout authority (see flags).** Decide whether
   the server-side precise `timeout_fut` or the shard 100 ms tick is canonical,
   and make the non-canonical one stop tearing down state independently. This is
   the only phase that changes timing behavior and ships behind its own tests.
5. **Phase 4 — tidy.** Remove the dead `ClientHandle::clear_unblock`
   (`client_registry/mod.rs:339-341`) or give it the body its comment promises.

## Testing impact

- **`WaitOutcome` selection becomes a unit test.** With `UnblockSignal` as a
  seam and `oneshot` as the response channel, the three races are pure-async
  tests: pre-resolve `response_rx` → `Response`; fire the mock unblock →
  `Unblocked`; pass `deadline = Some(now)` with the channel idle → `Timeout`; and
  the `biased` ordering (response wins a simultaneous response/timeout) is now
  directly assertable. Today every one of these needs a live socket and a running
  shard.
- **Per-op nil shape pinned.** `BlockingOp::timeout_reply()` gets a table test:
  BLPOP/BRPOP/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/XREAD → `NullArray`;
  BLMOVE/BRPOPLPUSH → `Null`. This is the regression lock for the flag below.
- **BLMOVE fan-out depth cap testable in isolation.** `drive_satisfaction` takes
  a `&mut dyn WaiterSatisfaction` and a store, so a test can build a 20-deep
  BLMOVE chain over an in-memory store and assert the cascade stops at
  `MAX_BLMOVE_FANOUT_DEPTH` and that the element lands where the cap is hit —
  today this is reachable only through a full shard worker.
- **Strategy `satisfy` is store-only.** Each `WaiterSatisfaction::satisfy` can be
  tested against a bare `Store` with a hand-built `WaitEntry`, covering
  empty-after-pop `Retry`, WRONGTYPE/NOGROUP `Reject`, and `Done { cascade }`
  without channels.
- **Existing suites unchanged.** Phases 0–2 are behavior-preserving (aside from
  the nil fix); the Redis-compat and Jepsen blocking suites are the end-to-end
  guard that no reply changed.

## Risks / open questions

- **Cleanup-on-cancel must always fire `UnregisterWait`.** Today the Timeout and
  Unblocked arms send it and the Response arm relies on the shard having already
  removed the entry (channel dropped ⇒ entry gone). `cleanup_wait` must preserve
  exactly this: unregister on Timeout/Unblocked, *not* on Response (a redundant
  unregister is harmless but wasteful). If `handle_blocking_wait` ever grows an
  early return between register and coordinate, the registration leaks — the
  register/cleanup pairing should be structured so cleanup runs on every path
  (e.g. the coordinator call sandwiched by a guard that unregisters on drop).
- **Two timeout authorities (race).** The server `select!` deadline
  (`blocking.rs:133`, precise) and the shard `check_waiter_timeouts` tick
  (`core/src/shard/blocking.rs:134`, every 100 ms via `event_loop.rs:23`) both
  end a wait. They do not coordinate, which is the root of the lost-element flag
  below. Phase 4 must pick one canonical authority. Option A: keep the precise
  server timeout but make `cleanup_wait` send `UnregisterWait` *before* yielding
  so a concurrent push cannot satisfy a doomed waiter. Option B: make the server
  timeout advisory (it returns to the client) but let only the shard pop entries,
  accepting 100 ms-granular shard-side cleanup.
- **Recursive fan-out depth cap.** The cap (`MAX_BLMOVE_FANOUT_DEPTH = 16`)
  bounds one write's cascade, not a multi-write chain (each external write starts
  at depth 0). In a BLMOVE *cycle* the same element ping-pongs until the cap or
  until both waiters are exhausted; the driver must keep the cap check at entry so
  the bound is structural, not incidental. The degraded case (chain > 16) leaves
  the element correctly placed but the next waiter unwoken until the next write —
  acceptable, but should be a documented property with a test.
- **Timeout vs wake ordering.** With `biased`, a simultaneous response and
  deadline favors the response (no spurious timeout). Preserve `biased` in the
  coordinator; a test must pin it, because flipping the arm order silently
  reintroduces the bug.
- **Who owns the wait queue.** Satisfaction stays inside `ShardWorker` (the queue
  is `self.wait_queue`); `WaiterSatisfaction` deliberately sees only the store,
  so the driver remains the sole mutator of the queue. Do not let a strategy take
  `&mut ShardWaitQueue`, or the seam dissolves.
- **CLIENT UNBLOCK depends on the BLOCKED flag being set.**
  `update_blocked_state(true)` runs *after* `BlockWait` is sent
  (`blocking.rs:82-104`), and `ClientRegistry::unblock` no-ops unless the BLOCKED
  flag is set (`client_registry/mod.rs:518`). A CLIENT UNBLOCK arriving in that
  window returns 0 even though the client is effectively blocked. Pre-existing
  and narrow; `register_wait` should set the flag as part of the same step to
  shrink the window.
- **Cross-shard waits are out of scope.** All keys of a blocking command are
  validated onto one shard (`requires_same_slot`), so the coordinator targets a
  single `response_rx`. A future multi-shard wait would need a different
  registration shape; the coordinator's `wait_for_response` is agnostic to how
  the `oneshot` is fed, so it should still apply.
- **Reusability vs over-abstraction.** WAIT already bypasses this path entirely
  (`handle_wait_command`, `blocking.rs:183-217`, uses the replication tracker).
  The coordinator is worth it for the BLPOP-family + XREAD, which genuinely share
  the register/race/cleanup shape; do not force WAIT/WAITAOF through it unless
  they end up needing the same three-way race, or the seam becomes a procrustean
  bed.

## Correctness flags

- **`connection/handlers/blocking.rs:157,170` (and `:125,:130`) + `core/src/shard/blocking.rs:149`:**
  blocking timeouts emit `Response::Null` (`$-1`, null bulk) for *every* op. For
  the array-returning ops (BLPOP/BRPOP/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/XREAD)
  Redis returns a null **array** (`*-1`) in RESP2 — their success replies are
  `Response::Array(...)` (e.g. `core/src/shard/blocking.rs:280`,
  `commands/src/blocking.rs:79`), and the project already has `Response::NullArray`
  for exactly this (`protocol/src/response.rs:629-630`). The timeout path discards
  the op, so it cannot pick the shape. Wire-wrong for RESP2 clients;
  RESP3 is unaffected (both nils serialize to `_`). BLMOVE/BRPOPLPUSH correctly
  want `$-1`. Fix: `BlockingOp::timeout_reply()` (Phase 0).
- **`core/src/shard/blocking.rs:665-672` raced against `connection/handlers/blocking.rs:133-156`:**
  lost-element timeout race. The server-side `timeout_fut` fires precisely at the
  deadline and tears the wait down via `UnregisterWait`, independently of the
  shard's authoritative expiry. If a push reaches the shard and is processed
  *before* the `UnregisterWait` message, `try_satisfy_list_waiters` pops the list
  element and `complete_blocked_waiter` does `let _ = entry.response_tx.send(...)`
  into the now-abandoned oneshot (the server already returned a timeout nil). The
  element is removed from the store and delivered to no client. `biased` does not
  help — it orders only the local task, not cross-task shard message processing.
  Narrow (requires a push within the deadline→unregister window) but a genuine
  data-loss race; addressed by resolving the dual-timeout authority (Phase 4 /
  risks above).
