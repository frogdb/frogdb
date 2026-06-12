# Proposal: ConnectionState Encapsulation

Status: proposed
Date: 2026-06-12

## Problem

`ConnectionState` (`frogdb-server/crates/server/src/connection/state.rs:263-319`) is a struct of
public fields — `transaction`, `pubsub`, `tracking`, `auth`, `reply_mode`, `skip_next_reply`,
`asking`, `readonly`, `blocked` — that every handler module pokes directly. It is a shallow
module: the interface is the entire implementation. Grep evidence:

| Field group              | Reach-ins | Files                                                          |
|--------------------------|-----------|----------------------------------------------------------------|
| `state.pubsub.*`         | 41        | pubsub.rs (30), guards.rs, dispatch.rs, lifecycle.rs, auth.rs, debug.rs |
| `state.transaction.*`    | 31        | handlers/transaction.rs (29), dispatch.rs (2)                  |
| `state.asking`           | 6         | dispatch.rs (set), guards.rs (read + 4 clears)                 |
| `state.readonly`         | 3         | dispatch.rs (2 sets), guards.rs (read)                         |
| `state.reply_mode` / `state.skip_next_reply` | 5 | handlers/client.rs (sets), connection.rs (consume) |

That is ~80 direct field accesses spread over 10 files. The protocol state machine — what Redis
calls the client struct's mode flags — has no single owner. Every invariant is enforced at call
sites:

- **MULTI queue discipline**: "queue exists ⇒ in transaction" is re-derived by
  `queue.is_some()` checks in `dispatch.rs:558` and `handlers/transaction.rs:30,355,387`; the
  five-field reset ritual is duplicated between `handle_multi` (`transaction.rs:35-39`),
  `clear_transaction_state` (`transaction.rs:588-592`), `handle_discard` (`transaction.rs:380`),
  and `handle_reset` (`auth.rs:640`).
- **Pub/sub mode filtering**: subscription limit checks and 80%-warning latch bookkeeping
  (`warned_sub_80` etc.) are copy-pasted three times in `handlers/pubsub.rs` (channels:39-54,
  patterns:155-170, sharded:300-315), and `handle_reset` clears the three sets field-by-field
  (`auth.rs:603-605`).
- **ASKING reset-after-command** — the emblematic case. The flag is **set** in
  `dispatch.rs:462` and **cleared** in a *different module*, `guards.rs:282,286,290,318` — four
  separate clear sites inside match arms of `validate_cluster_slots`. A one-shot flag whose
  set and reset live across a module seam, with the "cleared after use" doc comment sitting on
  the field (`state.rs:308`) and the enforcement nowhere near it. Miss one match arm (the
  `LocalServe` arm already doesn't clear it) and the flag silently leaks into the next command.

The seam leaks: to touch any connection-mode feature you must know the internal shape of the
state *and* the transition rules, then replicate them correctly at your call site. The
`#[allow(dead_code)]` on the struct (`state.rs:264`) is a symptom — the compiler can't even tell
which parts of the interface are real.

Locality is the cost: understanding "when does a connection leave pub/sub mode" requires reading
six files. Leverage is the missed opportunity: a fix to the transaction-reset ritual must be
applied in four places instead of one.

## Current state

Real reach-in mutations as they exist today.

**ASKING set** — `connection/dispatch.rs:461-464`:

```rust
"ASKING" => {
    self.state.asking = true;
    Some(vec![Response::ok()])
}
```

**ASKING reset** — `connection/guards.rs:279-290` (plus two more clears at 290 and 318):

```rust
match coordinator.route(first_slot, &cmd_name, self.state.asking, node_id) {
    RouteDecision::LocalServe => None,
    RouteDecision::LocalServeMigrating => {
        self.state.asking = false;
        None
    }
    RouteDecision::AcceptImporting => {
        self.state.asking = false;
        None
    }
    // ... Moved and Unassigned arms each repeat `self.state.asking = false;`
```

**MULTI five-field ritual** — `connection/handlers/transaction.rs:29-42`:

```rust
pub(crate) fn handle_multi(&mut self) -> Response {
    if self.state.transaction.queue.is_some() {
        return Response::error("ERR MULTI calls can not be nested");
    }

    debug!(conn_id = self.state.id, "Transaction started");
    self.state.transaction.queue = Some(Vec::new());
    self.state.transaction.target = TransactionTarget::None;
    self.state.transaction.exec_abort = false;
    self.state.transaction.queued_errors.clear();
    self.state.transaction.start_time = Some(std::time::Instant::now());

    Response::ok()
}
```

…and its near-twin — `connection/handlers/transaction.rs:586-594`:

```rust
pub(crate) fn clear_transaction_state(&mut self) {
    self.state.transaction.queue = None;
    self.state.transaction.target = TransactionTarget::None;
    self.state.transaction.cluster_first_slot = None;
    self.state.transaction.exec_abort = false;
    self.state.transaction.queued_errors.clear();
    // Note: watches are cleared separately, not here (they're consumed by EXEC)
}
```

**SUBSCRIBE limit + warning bookkeeping** — `connection/handlers/pubsub.rs:38-61` (repeated for
patterns at 155-176 and sharded channels at 300-321):

```rust
// Check subscription limits
let new_count = self.state.pubsub.subscriptions.len() + args.len();
if new_count > MAX_SUBSCRIPTIONS_PER_CONNECTION {
    return vec![Response::error("ERR max subscriptions reached")];
}

// 80% warning threshold
let threshold_80 = MAX_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5;
if new_count >= threshold_80 && !self.state.pubsub.warned_sub_80 {
    self.state.pubsub.warned_sub_80 = true;
    // ... tracing::warn! ...
}
// ...
self.state.pubsub.subscriptions.insert(channel.clone());
```

**Reply-mode set and consume split across files** — set in `connection/handlers/client.rs:922-935`,
consumed in `connection.rs:482-498`:

```rust
// client.rs — handle_client_reply
b"SKIP" => {
    self.state.skip_next_reply = true;
    Response::ok()
}

// connection.rs — response buffering
match self.state.reply_mode {
    ReplyMode::On => {
        if self.state.skip_next_reply {
            self.state.skip_next_reply = false;
            return FrameAction::SkipResponse;
        }
        // ...
```

## Proposed design

Make `ConnectionState` a deep module: a small interface of named transitions hiding the field
soup. Callers ask ("should this reply be sent?", "queue this command"), never poke. This is also
the project's standing convention: define methods on types instead of reaching into internals.

Sketch of the method surface (all in `state.rs`; impl details — substructs, latch booleans,
counters — become private):

```rust
impl ConnectionState {
    // ---- Pub/sub subscriptions ----------------------------------------
    /// Add a subscription. Enforces the per-connection limit and the
    /// one-shot 80% warning latch internally.
    pub fn subscribe(&mut self, kind: SubKind, name: Bytes) -> SubscribeOutcome;
    /// Remove one subscription; returns remaining (channels + patterns) count.
    pub fn unsubscribe(&mut self, kind: SubKind, name: &Bytes) -> usize;
    /// Snapshot of current subscriptions of a kind (owned, for shard fan-out).
    pub fn subscriptions(&self, kind: SubKind) -> Vec<Bytes>;
    /// Drop all subscriptions; returns whether the connection was in pub/sub
    /// mode (caller decides whether to notify shards).
    pub fn exit_pubsub(&mut self) -> bool;
    pub fn in_pubsub_mode(&self) -> bool;           // delegates to PubSubState
    pub fn subscription_counts(&self) -> SubCounts; // read-only, for CLIENT INFO / DEBUG

    // ---- Transactions ---------------------------------------------------
    /// MULTI. Errors if already in a transaction.
    pub fn begin_transaction(&mut self) -> Result<(), TxnError>;
    pub fn in_transaction(&self) -> bool;
    /// Queue a command and fold its shard into the transaction target.
    pub fn queue_command(&mut self, cmd: Arc<ParsedCommand>, shard: usize);
    /// Mark the transaction poisoned (bad command during queuing).
    pub fn abort_transaction(&mut self, error: String);
    /// EXEC: take the queue + watches atomically, leaving a clean state.
    /// None = EXEC without MULTI. Summary carries queue, watches, target,
    /// abort flag, and elapsed time for metrics.
    pub fn take_transaction(&mut self) -> Option<TxnSummary>;
    /// DISCARD: drop everything including watches; None = DISCARD without MULTI.
    pub fn discard_transaction(&mut self) -> Option<TxnMetrics>;
    pub fn watch_key(&mut self, key: Bytes, shard: usize, version: u64);
    pub fn unwatch_all(&mut self);

    // ---- Cluster flags: ASKING / READONLY -------------------------------
    pub fn set_asking(&mut self);
    /// One-shot read-and-clear. The flag cannot survive past the command
    /// that consumed it — the reset-after-command invariant lives HERE.
    pub fn take_asking(&mut self) -> bool;
    pub fn set_readonly(&mut self, readonly: bool);
    pub fn is_readonly(&self) -> bool;

    // ---- Reply control ----------------------------------------------------
    pub fn reply_on(&mut self);
    pub fn reply_off(&mut self);
    pub fn reply_skip_next(&mut self);
    /// Decide the fate of the next reply, consuming the skip latch.
    pub fn consume_reply_disposition(&mut self) -> ReplyDisposition; // Send | Suppress

    // ---- RESET --------------------------------------------------------------
    /// State half of the RESET command: clears pubsub, transaction, tracking,
    /// protocol version, name. Returns what was active so the handler can do
    /// the I/O half (shard notifications, channel teardown).
    pub fn reset(&mut self) -> ResetEffects;
}
```

Identity/bookkeeping fields (`id`, `addr`, `created_at`, `local_stats`, …) are not state-machine
fields; they can stay public or grow trivial getters later — out of scope here.

### Before/after call sites

**ASKING** — the set and the consume become two halves of one named interface; the four clear
sites collapse to zero:

```rust
// BEFORE — dispatch.rs:462 sets; guards.rs:282,286,290,318 each clear
"ASKING" => {
    self.state.asking = true;
    Some(vec![Response::ok()])
}
match coordinator.route(first_slot, &cmd_name, self.state.asking, node_id) {
    RouteDecision::LocalServeMigrating => {
        self.state.asking = false;
        None
    }
    // ...three more arms repeating the clear...

// AFTER
"ASKING" => {
    self.state.set_asking();
    Some(vec![Response::ok()])
}
let asking = self.state.take_asking(); // read-and-clear, once
match coordinator.route(first_slot, &cmd_name, asking, node_id) {
    RouteDecision::LocalServeMigrating => None,
    // ...arms carry routing logic only...
```

**MULTI** — `handlers/transaction.rs:29-42` shrinks from a five-field ritual to a transition:

```rust
// AFTER
pub(crate) fn handle_multi(&mut self) -> Response {
    match self.state.begin_transaction() {
        Ok(()) => Response::ok(),
        Err(TxnError::Nested) => Response::error("ERR MULTI calls can not be nested"),
    }
}
```

**SUBSCRIBE** — `handlers/pubsub.rs:38-61`; the limit/latch logic written three times becomes
one method, and the handler keeps only protocol formatting and shard fan-out:

```rust
// AFTER
for channel in args {
    let outcome = match self.state.subscribe(SubKind::Channel, channel.clone()) {
        SubscribeOutcome::LimitReached => {
            return vec![Response::error("ERR max subscriptions reached")];
        }
        outcome => outcome,
    };
    if outcome.crossed_80_pct {
        tracing::warn!(conn_id = self.state.id(), /* ... */);
    }
    // shard fan-out + confirmation response using outcome.sub_count ...
}
```

## Migration plan

Each phase compiles and passes tests independently; one PR per phase keeps review scoped.

1. **Add methods, keep fields public.** Land the full method surface in `state.rs` with unit
   tests. No call-site changes; zero behavioral risk.
2. **Migrate pub/sub call sites** — `handlers/pubsub.rs` (30 reach-ins), plus the pubsub touches
   in `guards.rs:211`, `dispatch.rs:476,534`, `lifecycle.rs:56,181-182`, `auth.rs:598-605`,
   `debug.rs:323-324`.
3. **Migrate transaction call sites** — `handlers/transaction.rs` (29), `dispatch.rs:558-572`.
   `clear_transaction_state` and the MULTI/DISCARD/RESET rituals all collapse into the state
   methods.
4. **Migrate asking/readonly/reply** — `dispatch.rs:461-471`, `guards.rs:279-318`,
   `handlers/client.rs:922-935`, `connection.rs:482-498`.
5. **Flip fields to private** (or `pub(crate)`-getter-only) and delete `#[allow(dead_code)]`.
   The compiler finds every straggler — this is the leverage of doing encapsulation last instead
   of first: rustc enforces the seam permanently, not a review checklist.
6. Optional follow-up: same treatment for `tracking` and `auth` (lower traffic, `auth.rs` and
   `client.rs` only).

## Testing impact

Today the protocol state machine is only exercised through full connection handlers — sockets,
shard channels, registry. After this change, `ConnectionState` is a plain struct with methods:
unit-testable in `state.rs` with no tokio runtime, no shards, no sockets.

Invariant tests that become one-liners:

- `take_asking` returns true exactly once after `set_asking`; a second call returns false
  (asking cleared after one command — currently untestable without standing up cluster routing).
- `begin_transaction` twice yields `TxnError::Nested`; `take_transaction` without `begin` yields
  `None`; after `abort_transaction`, `take_transaction` reports the poisoned flag and queued
  errors.
- `subscribe` to the limit returns `LimitReached`; the 80% latch fires exactly once and re-arms
  after dropping below threshold (currently three copy-pasted blocks, none unit-tested).
- `consume_reply_disposition` suppresses exactly one reply after `reply_skip_next`, and every
  reply after `reply_off`.
- `reset` leaves a state indistinguishable from `ConnectionState::new` for the fields RESET
  covers (a property test against a fresh instance).

Existing integration tests (`just test frogdb-server`) stay as the wiring check; behavior is
unchanged except where noted under Risks.

## Risks / open questions

- **Borrow conflicts.** Handlers interleave state access with `self.core.shard_senders` sends
  across `.await` points. Methods that return borrows into the state (e.g., an iterator over
  subscriptions) would conflict; the interface above returns owned snapshots (`Vec<Bytes>`,
  `TxnSummary`) — the same pattern handlers already use (`pubsub.rs:99` clones the set before
  fanning out). Rule for the new interface: transitions return owned data or decisions, never
  references held across awaits.
- **`take_asking` is a small behavior change.** Today the `LocalServe` arm (`guards.rs:280`)
  does *not* clear the flag, so ASKING can survive a command that routed normally; and in
  non-cluster mode the flag is never cleared at all. `take_asking` clears unconditionally, which
  matches Redis (`resetClient` drops `CLIENT_ASKING` after every command except ASKING/MULTI).
  Verify against cluster ASK redirect tests; the new semantics are arguably a bug fix.
- **Where do limits live?** `MAX_SUBSCRIPTIONS_PER_CONNECTION` currently lives in the handler
  module. Moving it into `state.rs` keeps `subscribe` self-contained; passing it as a parameter
  keeps state config-free. Lean toward moving it — the limit is part of the transition rule.
- **Legit external reads stay as getters.** `debug.rs:323-324` (DEBUG counters),
  `lifecycle.rs:181-182` (cleanup fan-out), CLIENT INFO. Read-only getters
  (`subscription_counts`, `in_transaction`, `is_readonly`) are fine and don't weaken the seam —
  only mutation paths must go through transitions.
- **Scope of `blocked` and `tracking`.** `handlers/blocking.rs:98,138` sets `state.blocked`
  directly and `TrackingState` is overwritten wholesale in two places (`client.rs:776`,
  `auth.rs:619`). Include them in phase 6, or leave public and accept a weaker seam? Proposed:
  phase 6, same pattern.
- **Deletion test.** After migration, removing a connection-mode feature (say, CLIENT REPLY)
  should mean deleting its methods from `state.rs` plus one handler — the compiler lists the
  rest. Today the same removal requires hunting consume sites in `connection.rs` by hand. If a
  migrated subsystem still fails this test, the interface is in the wrong place; treat that as a
  review gate for each phase PR.
