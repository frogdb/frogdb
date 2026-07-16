# Proposal: Pre-Dispatch Stage Pipeline

Status: implemented (2026-07-16)
Date: 2026-07-16

## Problem

"Before a command executes, run it through an ordered gauntlet of guards and interceptions" is a
load-bearing concept in FrogDB — it decides authentication, replica-readonly rejection, RESET,
pub/sub framing, MULTI queuing, arity, pause, cluster-slot routing, and a dozen special-case
commands — but it has no module and no interface. It exists only as ~230 lines of control flow plus
prose comments inside one async method, `route_and_execute_with_transaction`
(`frogdb-server/crates/server/src/connection/dispatch.rs:227-457`). The ordering is real Redis
semantics (AUTH must precede the NOAUTH check; MULTI queuing must precede pause; arity must precede
pause) yet the *only* thing that pins it is the top-to-bottom order of `if` blocks and the comments
that explain them (`dispatch.rs:232-238, 284-298, 355-376`). Connection-mode state
(`in_pubsub_mode`, `in_transaction`, `take_asking`, `should_track_read`, `pending_no_touch`) is read
and mutated throughout, interleaved with the interception logic.

| Symptom | Where |
|---------|-------|
| ~15 ordered interceptions inline in one ~230-line async fn | `connection/dispatch.rs:227-457` |
| AUTH/HELLO intercepted *before* the auth pre-check (ordering is prose) | `dispatch.rs:239-251`, comment `:232-238` |
| Pre-check gauntlet (auth, replica-readonly, quorum-fence, admin-port, ACL, pub/sub-mode) | `guards.rs:107-170`, called at `dispatch.rs:258` |
| RESET intercept | `dispatch.rs:269-271` |
| Pub/sub-mode PING bespoke framing | `dispatch.rs:278-282` |
| Transaction-control (MULTI/EXEC/DISCARD/WATCH/UNWATCH) | `dispatch.rs:290-292` |
| MULTI queuing (+ inline cluster-slot pre-validation) | `dispatch.rs:299-311` |
| Arity check, ordered *before* pause deliberately | `dispatch.rs:316-325`, comment `:313-315` |
| Pause wait | `dispatch.rs:329` |
| Connection-state cmds (ASKING/READONLY/READWRITE) via mutable seam | `dispatch.rs:337-342` |
| Registry-union connection-command dispatch (CONFIG/CLIENT/…) | `dispatch.rs:351-353` |
| PSYNC intercept | `dispatch.rs:357-366` |
| WAIT intercept (into the `WaitCoordinator`, proposal 39) | `dispatch.rs:374-376` |
| Server-wide ops (SCAN/KEYS/FLUSHDB/…) | `dispatch.rs:384-392` |
| CLUSTER GETKEYSINSLOT/COUNTKEYSINSLOT slot routing | `dispatch.rs:399-409` |
| Cluster-slot validation (MOVED/ASK/CROSSSLOT; consumes `take_asking`) | `dispatch.rs:412-414`, `guards.rs:192-229` |
| Terminal `route_and_execute` (shard routing) | `dispatch.rs:437-441` |

The concept fails the deletion test in the diagnostic direction: there is nothing to delete. No
type, no trait, no `PRE_DISPATCH_ORDER` names the gauntlet — the abstraction is reconstructed inline
every time the method is read. A reviewer wanting to answer "what runs before MULTI queuing, and
why" must read the method top-to-bottom and trust the comments; a reviewer wanting to reorder two
guards has no compiler check that the reorder is safe.

**Untestable without a socket.** `ConnectionHandler` owns `Framed<ConnectionStream>`
(`connection.rs:88-89`), so exercising any pre-check requires a real loopback TCP
listener+connect — `make_test_handler` (`guards.rs:411-476`) binds `127.0.0.1:0`, accepts, and
connects (`:419-427`) purely to construct a handler. `run_pre_checks` (`guards.rs:107`) touches
nothing on the socket, yet cannot be built without one. Every guard decision is therefore
integration-only, gated behind tokio + a TCP pair.

We already have two precedents for the fix inside this same codebase:

- The **`ConnCtx` seam** (`frogdb-server/crates/core/src/conn_command.rs:578`) lets connection
  commands run socketless: a struct of borrowed dependency views, no `Framed`, unit-testable.
- The shard-side **`WRITE_EFFECT_ORDER`** (`core/src/shard/post_execution.rs:156`, proposal 03)
  turned a load-bearing ordering into a `const [WriteEffectKind; 9]` array with an order-pinning
  test (`post_execution.rs:500-517`: each effect appears exactly once, length pinned). The
  gauntlet is the *connection-side* analogue of that ordering, still stuck at the inline-control-flow
  stage.

## Design

Model the pre-dispatch gauntlet as an **ordered, static list of typed stages**. Ordering becomes a
declared `const` array (pinnable, reviewable, deletion-testable); each stage's guard logic operates
on a **state view that does not own the socket** (unit-testable); the stages that terminate into
command execution stay thin adapters over the existing executors (locality: we do not fork the
dispatch machinery).

This is exactly the shape of Redis's `processCommand` (`server.c`): a single ~200-line function that
runs a fixed sequence — `lookupCommand` (unknown/arity), authentication + `ACLCheckAllPerm`, cluster
redirection, `maxmemory`/OOM, disk-error rejection, replica read-only rejection, pub/sub-context
filtering, then `queueMultiCommand` for MULTI, then `call()`. Redis encodes that gauntlet as
sequential C control flow with explanatory comments — the same shape FrogDB has now. The proposal is
not to change the order (it mirrors Redis) but to make the order a *data structure* instead of the
layout of an `if`-ladder.

### The ordering, as data

```rust
/// The pre-dispatch gauntlet, in execution order. Reordering a guard means
/// editing THIS array (and the pinning test notices). Mirrors WRITE_EFFECT_ORDER.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchStage {
    PreAuthIntercept,       // AUTH/HELLO, before the NOAUTH check       (dispatch.rs:239)
    PreChecks,              // auth/replica/quorum/admin/ACL/pubsub-gate (guards.rs:107)
    ResetIntercept,         // RESET, never queued, never paused         (dispatch.rs:269)
    PubSubPing,             // ["pong", msg] framing in pub/sub mode      (dispatch.rs:278)
    TransactionControl,     // MULTI/EXEC/DISCARD/WATCH/UNWATCH           (dispatch.rs:290)
    TransactionQueue,       // queue if in MULTI (+ slot pre-validate)    (dispatch.rs:299)
    Arity,                  // wrong-arg-count error, before pause        (dispatch.rs:316)
    PauseGate,              // CLIENT PAUSE wait, after queuing           (dispatch.rs:329)
    ConnectionStateCommand, // ASKING/READONLY/READWRITE (mutable seam)   (dispatch.rs:337)
    ConnectionCommand,      // CONFIG/CLIENT/INFO/… registry union        (dispatch.rs:351)
    PsyncIntercept,         // PSYNC handoff signal                       (dispatch.rs:357)
    WaitIntercept,          // WAIT → WaitCoordinator (proposal 39)       (dispatch.rs:374)
    ServerWide,             // SCAN/KEYS/FLUSHDB/MIGRATE/…                 (dispatch.rs:384)
    ClusterSlotSubcommand,  // CLUSTER GET/COUNTKEYSINSLOT slot routing   (dispatch.rs:399)
    ClusterSlotValidation,  // MOVED/ASK/CROSSSLOT (consumes take_asking) (dispatch.rs:412)
    MigratingTryAgain,      // TRYAGAIN during multi-key slot migration   (dispatch.rs:417)
    Execute,                // terminal: bookkeeping + route_and_execute  (dispatch.rs:422)
}

pub(crate) const PRE_DISPATCH_ORDER: [DispatchStage; 17] = [ /* the variants, in order */ ];
```

### Short-circuit flow

```rust
/// A stage either lets the command proceed or ends dispatch with a reply.
/// `Continue` is the hot-path common case and allocates nothing.
pub(crate) enum StageOutcome {
    Continue,
    ShortCircuit(Vec<Response>), // one-or-many, exactly today's `Vec<Response>` return
}
```

The driver is one async loop — a `match` over a `Copy` enum, **no trait objects, no boxed futures**:

```rust
for stage in PRE_DISPATCH_ORDER {
    match self.run_stage(stage, cmd, cmd_name).await {
        StageOutcome::Continue => {}
        StageOutcome::ShortCircuit(responses) => return responses,
    }
}
unreachable!("PRE_DISPATCH_ORDER ends in Execute, which never returns Continue")
```

`run_stage` is a single `match stage { … }` whose arms are the *existing* method bodies, moved
verbatim. Because it is a static match over a `Copy` enum with directly-inlined `.await`s (not a
`Vec<Box<dyn Stage>>` of `async fn`s), the compiler monomorphizes and inlines it: this is the
hottest path in the server, and the design must not add per-command allocation or dyn-dispatch cost.
`Continue` carries no payload; the only allocation is the `ShortCircuit` `Vec`, which is precisely
what each `return vec![…]` builds today. (Discussed under Risks: a `Vec<Box<dyn Stage>>` was
rejected for exactly this reason — it would box a future per stage per command.)

### The socketless state view

Guard stages take a borrowed view that excludes `framed` — the `ConnCtx` move, applied to dispatch:

```rust
/// Everything the gauntlet's *guard* stages read or mutate — connection-mode
/// state, the registry, cluster/admin dep handles, per-command scratch flags —
/// and nothing on the socket. Constructible in a unit test with no TCP pair
/// (contrast make_test_handler, guards.rs:411). The `PreChecks`, `Arity`,
/// `PubSubPing`, `TransactionQueue`, `ClusterSlotValidation`, and `MigratingTryAgain`
/// stages become pure functions over this.
pub(crate) struct PreDispatchView<'a> {
    pub state: &'a mut ConnectionState,   // in_pubsub_mode/in_transaction/take_asking/should_track_read
    pub registry: &'a CommandRegistry,    // arity, flags, execution_strategy, keys
    pub cluster: &'a ClusterDeps,         // slot_migration, node_id, quorum_checker, primary handle
    pub client_registry: &'a ClientRegistry,
    pub is_replica: &'a AtomicBool,
    pub is_admin: bool,
    pub admin_enabled: bool,
    pub pending_track_reads: &'a mut bool,
    pub pending_no_touch: &'a mut bool,
    // ...the exact fields the migrated guard bodies name, and no more
}
```

The migration moves the guard methods (`run_pre_checks`, `validate_cluster_slots`, the arity block,
the pub/sub-PING framing, the transaction-queue decision, `check_migrating_multikey`) to take
`&mut PreDispatchView` instead of `&mut self`. `should_track_read` is already `&mut self`
(`state.rs:349`), so the view carries `&mut ConnectionState` — the guards mutate mode state but never
the socket, which is the whole point.

Two stage flavors, honestly distinguished — this is where the design earns its keep without
over-reaching:

- **Guard stages** (`PreChecks`, `Arity`, `PubSubPing`, `TransactionQueue`, `ClusterSlotValidation`,
  `MigratingTryAgain`, plus the `PauseGate` *predicate*) are pure decisions over `PreDispatchView`:
  socketless, independently unit-testable.
- **Dispatch stages** (`PreAuthIntercept`, `ResetIntercept`, `TransactionControl`,
  `ConnectionStateCommand`, `ConnectionCommand`, `PsyncIntercept`, `WaitIntercept`, `ServerWide`,
  `ClusterSlotSubcommand`, `Execute`) terminate into a command executor that legitimately needs the
  full handler (pub/sub responses queue into `framed`; PSYNC sets `pending_psync_handoff`; WAIT
  drives the `WaitCoordinator`). Their arms stay thin adapters that call the *unchanged* executors
  (`execute_reset`, `dispatch_connection_command`, `handle_wait_command`, …). We move the *ordering
  and the guard predicates* behind a seam; we do **not** fork the executors.

### What is out of scope (the post-execution tail)

The three steps *after* the `Execute` terminal — `migrating_ask_for_nil` (`dispatch.rs:446`),
`handle_internal_action` (`dispatch.rs:451`), and `record_error_response` (`dispatch.rs:454`) — are
post-dispatch, not pre-dispatch, and the shard-side effects already have their home in proposal 03's
post-execution pipeline. This proposal stops at command entry, symmetric with `WRITE_EFFECT_ORDER`
stopping at write effects.

## Why this is the right depth

- **Locality.** The whole "what runs before execution, and in what order" question collapses to one
  `const PRE_DISPATCH_ORDER` array plus one `run_stage` match — read the array, know the gauntlet.
  Today that answer is distributed across a 230-line method, `guards.rs`, and the prose comments that
  hold the ordering rationale. The guard *predicates* move next to each other, no longer interleaved
  with the executors they guard.
- **Leverage.** `PreDispatchView` makes every guard a fast unit test with no socket and no tokio TCP
  pair — the tests `make_test_handler` exists to enable become plain struct tests. `PRE_DISPATCH_ORDER`
  makes the ordering itself testable (pinning test, below): the AUTH-before-NOAUTH and
  MULTI-before-pause invariants that are prose today become assertions. A new interception is a new
  enum variant + array slot + match arm; the compiler's exhaustiveness check forces all three.
- **Deletion test.** Removing a gauntlet step means deleting its `DispatchStage` variant, its
  `PRE_DISPATCH_ORDER` entry, and its `run_stage` arm — and rustc lists any straggler. Today the same
  removal is an inline surgery with no compiler backstop, exactly the drift `refine_handler` /
  `op_to_handler` suffered in proposal 05.
- **Deepens, does not fork.** The socketless view is proposal 04's `ConnectionState`-encapsulation
  direction and proposal's `ConnCtx` seam, applied one layer up. WAIT stays behind proposal 39's
  `WaitCoordinator` (the `WaitIntercept` arm is unchanged); connection-state commands stay behind the
  mutable `conn_ctx_authmut` seam; the ordering-as-data move is proposal 03's `WRITE_EFFECT_ORDER`
  pattern. No second dispatch path, no parallel state home.

## Testing impact

- **Order-pinning test** (pattern: `post_execution.rs:500-517`): assert every `DispatchStage` variant
  appears in `PRE_DISPATCH_ORDER` exactly once, `len == 17`, and — the load-bearing relative
  orderings as explicit assertions — `PreAuthIntercept` precedes `PreChecks` (pre-auth), `Arity`
  precedes `PauseGate` (syntax errors bypass pause), `TransactionQueue` precedes `PauseGate` (queued
  MULTI commands do not block), and `ClusterSlotValidation` follows `TransactionQueue` (queued
  commands slot-validate at queue time, `dispatch.rs:301-309`).
- **Guard-stage unit tests over `PreDispatchView`** (no socket, no tokio): the self-fence /
  replica-readonly / NOAUTH / admin-port / ACL / pub/sub-gate matrix that today needs
  `make_test_handler` and a loopback pair; arity rejection; pub/sub-mode PING framing; `take_asking`
  consumed exactly once by `ClusterSlotValidation`; MULTI queuing aborts on a cross-slot queued command.
- **Existing integration suites** (`just test frogdb-server`) stay as the wiring check: the executor
  arms are moved verbatim, so end-to-end behavior is unchanged. `make_test_handler` can shrink (or its
  socket become optional) once the guards no longer route through the full handler.

## Risks / open questions

- **Hot path — no regression budget.** This is the busiest method in the server. The static-array +
  inlined-`match` design is deliberate: `Continue` is payload-free, the driver monomorphizes, and no
  future is boxed. A `Vec<Box<dyn Stage>>` / `async-trait` formulation is **rejected** — it would heap
  a stage list and box a future per stage per command. Benchmark the migrated dispatch against
  baseline (`just bench`, or a `criterion` micro-bench over `route_and_execute_with_transaction` on a
  no-op GET) and gate the merge on parity.
- **Borrow discipline across `.await`.** Dispatch stages await executors that reborrow `self`; the
  guard `PreDispatchView` must be dropped before an executor arm runs (or the two must borrow disjoint
  fields). This is the same constraint proposal 04 documents (transitions return owned data, never
  references held across awaits) — the view holds `&mut` borrows only for the synchronous guard body,
  not across the executor `.await`.
- **The guard/dispatch split is a judgment call.** Ten of seventeen stages terminate into an executor
  and cannot be socketless without also extracting the executors — a much larger change. Drawing the
  line at "extract the ordering and the guards, leave the executors" keeps the proposal small enough
  to land; a later change that makes an executor socketless can join the guard flavor without touching
  `PRE_DISPATCH_ORDER`.
- **`Execute` as a non-returning terminal.** The driver loop is total only because `Execute` is last
  and never returns `Continue`; the pinning test asserts `PRE_DISPATCH_ORDER.last() == Execute`. A
  `(stages, terminal)` pair would enforce it in the type system — heavier, deferred.
- **Interaction with proposal 05.** `route_and_execute`'s internal `connection_level_handler_for`
  routing (proposal 05's subject) sits *inside* the `ConnectionCommand`/`Execute` arms; the
  `ConnectionCommand` stage delegates to the unchanged `dispatch_connection_command`, so the
  single-routing-decision seam stays the one authority — the two proposals compose, not overlap.

## Implementation notes (2026-07-16)

Landed as designed. Delta from the sketch, and the judgment calls:

- **The array and driver.** `DispatchStage`, `PRE_DISPATCH_ORDER: [DispatchStage; 17]`, and
  `StageOutcome` live in `connection/dispatch.rs`. `route_and_execute_with_transaction` is now the
  proposal's exact `for stage in PRE_DISPATCH_ORDER { match run_stage(...).await { … } }` loop
  ending in `unreachable!`. `run_stage` is one `match stage { … }` whose arms are the former inline
  bodies, moved verbatim. Stage order matches the proposal enum 1:1 (PreAuthIntercept=0 …
  Execute=16) and preserves the old top-to-bottom `if`-ladder order exactly.
- **The socketless view.** `PreDispatchView<'a>` lives in `guards.rs` (the guard half of the
  gauntlet). Guard predicates moved onto it: `run_pre_checks`, `is_auth_exempt`,
  `is_allowed_in_pubsub_mode`, `permission_guard`, `validate_cluster_slots`, `is_cluster_exempt`,
  `check_migrating_multikey`, `pubsub_mode_ping`, `arity_check`, `try_queue_in_transaction`, and
  `queue_command` (moved out of `handlers/transaction.rs` — its only caller was dispatch). The view
  holds `state: &mut ConnectionState` + borrowed dep handles and **no socket**. `ConnectionHandler::
  pre_dispatch_view(&mut self)` builds it from disjoint fields; each guard arm binds the owned
  result to a `let` (so the view temporary drops before `record_error_response`/executors reborrow
  `self`), which is the borrow discipline the Risks section flagged.
- **Shared `permission_guard`.** Extracted `build_permission_guard(&AclManager, &ConnectionState)`
  in `permission_guard.rs`; both `ConnectionHandler::permission_guard` (key checks on the routing
  path) and `PreDispatchView::permission_guard` delegate to it — identical user/client-info binding,
  no drift.
- **Two stages kept as thin handler closures, deliberately** (the "do not force it" clause):
  `PauseGate` calls `self.wait_if_paused(...).await` — the pause *wait loop* mutates the client
  registry's paused flag and sleeps, so only its predicate (`should_pause_command`) is guard-shaped;
  moving the loop would drag `client_registry` mutation into the view for no test win. The `Execute`
  terminal and the other eight dispatch stages (`PreAuthIntercept`, `ResetIntercept`,
  `TransactionControl`, `ConnectionStateCommand`, `ConnectionCommand`, `PsyncIntercept`,
  `WaitIntercept`, `ServerWide`, `ClusterSlotSubcommand`) terminate into executors that need the
  full handler and stay adapters over the unchanged code.
- **Post-execution tail stays on the handler** (out of scope, as stated): `migrating_ask_for_nil`,
  `handle_internal_action`, `record_error_response` run inside the `Execute` arm.
- **Tests.** Order-pinning (`every_stage_appears_exactly_once`: each variant once, `len == 17`) and
  the load-bearing relative orderings (`load_bearing_ordering_invariants`) in `dispatch.rs`, modeled
  on `WRITE_EFFECT_ORDER`'s tests. The four `run_pre_checks` self-fence tests migrated off
  `make_test_handler`/loopback TCP to a socketless `ViewFixture` in `guards.rs`, plus five new guard
  unit tests (replica-readonly, admin-port NOADMIN, arity, pub/sub-PING RESP2 framing, NOAUTH).
  `make_test_handler` was fully removed (no remaining callers).
- **Verification.** `just check frogdb-server` clean; `just lint frogdb-server` clean;
  dispatch tests 3/3, guards 9/9, integration_transactions 25/25 (the MULTI-queue ordering risk),
  integration_pubsub 94/94. The criterion micro-bench in Risks was not added — the design is a
  monomorphized `match` over a `Copy` enum with no boxing/allocation, so it lowers to the same shape
  as the old `if`-ladder; a bench harness for `route_and_execute_with_transaction` did not exist to
  extend and building one was out of scope for this change.
