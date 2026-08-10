# Proposal 62 — the handoff finalizer moves down to the crates that own the handoff

Round 38 · lane: replication+cluster · effort **M** · LOCKED area (cluster, mutation gate 0.80) ·
**not spec-first** (no behavior change proposed; the one behavior question found is filed as a
follow-up, not folded in)

Covers exploration-lane candidate **RC11** ("Handoff finalizer in server crate away from barrier —
`SlotMigrationCoordinator::complete/poll` (`server/slot_migration/mod.rs:208-337`) vs
cluster-runtime `handoff_barrier.rs` model"). Verified against `08c143d6`.

## Summary

The two-phase slot handoff has two halves. The **source-side** half —
"a prepare means arm a barrier, drain the shard, confirm" — lives in
`frogdb-cluster-runtime::handoff_barrier` as a pure planner (`plan_handoff_action`) plus a thin
async runner (`run_slot_handoff_barrier`) with the propose leg injected as a closure. It has six
in-crate unit tests, four FM-CLUSTER-090 and two FM-CLUSTER-091, and the lane's own
counter-example list names it as *already deep*.

The **finalizer** half — "propose prepare, poll replicated state for my own attempt, poll for the
drain, then propose complete or abort" — lives 130 lines up in `frogdb-server`
(`slot_migration/mod.rs:208-337`) as one 46-line `async fn` plus three private poll helpers and a
`commit` that manufactures a `ClusterWriter` per proposal. It is the **implementation** of a
protocol whose every collaborator (`ClusterCommand`, `SlotHandoff`, `ClusterWriter`,
`HANDOFF_*`, `handoff_now_ms`) lives one or two crates below it, and it has **no interface**: no
type names the finalizer's states, no function is callable without a live three-node Raft cluster.

The measurable consequence is a census, not an opinion:

* **`complete`, `await_prepared_seq`, `await_drained` and `poll_handoff` have zero direct tests.**
  Verified by grepping every reference in the tree (only `connection/cluster.rs:149` calls
  `complete`; `slot_migration/tests.rs` is 676 lines of routing tests and touches none of them).
* Their only forcing test is `a_source_that_cannot_drain_aborts_the_finalization`
  (`server/tests/cluster_handoff_barrier.rs:288-389`) — a three-node live cluster that kills a
  node — plus the `finalize_handoff` helper (`:208-229`) four other integration tests call for
  their happy path.
* Their only *specification* is a **hand transcription** in a third crate: the stateright model at
  `cluster/src/model/mod.rs:36-45` says out loud that its `Coord` enum (`:242-254`) is "a direct
  transcription of the *control flow* of `SlotMigrationCoordinator::complete` (server crate)". The
  model's header sets the discipline for the state machine — *"the model never re-implements the
  transition function"* (`:15-23`) — and then exempts the coordinator from it.

**This proposal claims no live bug.** Two latent findings, one stale-doc defect and one
seam-coverage gap are recorded below with the reason each is not reachable as a defect today.
Two independently-landable hotfixes (both doc/seam, no behavior) are proposed at the end.

**The move's whole cost is the mutation gate, and it must be paid up front.** `frogdb-server` has
no gate; `frogdb-cluster-runtime` has one at 0.80 and `cargo mutants -p` runs only that package's
tests. Moving this code moves it *into* scoring range with no in-crate test to score it. §"Spec and
gate impact" is the load-bearing section of this proposal.

## Files involved (verified at `08c143d6`)

All paths under `frogdb-server/crates/` unless noted.

| file | lines | role |
|---|---:|---|
| `server/src/slot_migration/mod.rs` | 338 | **the code that moves.** module doc `:1-32`; `use frogdb_cluster_runtime::handoff_now_ms` `:52`; struct `:69-73`; `new` `:77-87`; `spawn_event_dispatcher` `:91-107`; `spawn_handoff_barrier` `:118-156` (injected `confirm` closure `:134-152`); `begin` `:179-186`; **`complete` `:208-253`** (doc `:188-207`); **`await_prepared_seq` `:257-262`**; **`await_drained` `:265-269`**; **`poll_handoff` `:278-297`** (doc `:271-277`); `cancel` `:300-303`; **`commit` `:309-337`** (writer construction `:313-317`, retryable-prefix fork `:324-329`); inline TRYAGAIN literals `:229-231`, `:241-243` |
| `cluster-runtime/src/handoff_barrier.rs` | 465 | **the model to mirror, and a lane counter-example — not to be restructured.** `handoff_now_ms` `:55-60`; `to_ms` `:64-68`; `HandoffAction` `:71-91`; `plan_handoff_action` `:97-120`; `drain_shard` `:129-169`; `run_slot_handoff_barrier` `:177-219` (injected `confirm` `:183-187`); tests `:221-465` with FM tags at `:305`, `:341`, `:363`, `:394`, `:419`, `:442`, `:454` |
| `cluster-runtime/src/lib.rs` | 49 | crate charter (`:1-26`) and the `pub use` surface (`:28-49`) — gains one module and one export |
| `cluster-runtime/src/migration_events.rs` | 251 | the second precedent for the `plan_*` + `run_*` pair (`plan_migration_notice` / `run_slot_migration_event_dispatcher`, exported `:44-46`) |
| `cluster/src/model/mod.rs` | 812 | **the hand transcription.** discipline statement `:15-23`; the transcription's own confession `:36-45`; `Coord` `:242-254`; action enabling `:516-547`; `observed_prepare` `:427-434`; `observed_drained` `:437-443`; `Action::CoordGivesUp` arm `:607-622`; `Action::Coord` arm `:623-650` |
| `cluster/src/types.rs` | 1313 | `ClusterError::HandoffNotReady` `:629-637`; `is_retryable` `:640-644`; `HANDOFF_BARRIER_MS = 100` `:647-659`; `HANDOFF_DRAIN_WAIT_MS = 50` `:661-665`; `HANDOFF_POLL_INTERVAL_MS = 2` `:667-671`; `HANDOFF_DRAIN_TIMEOUT_MS = 2000` `:673-678`; `HANDOFF_LEASE_MS = 10000` `:680-685`; `SlotHandoff` `:691-…`; FM-CLUSTER-091 forcing test `only_a_not_ready_handoff_is_retryable` `:921-945` |
| `cluster/src/writer.rs` | 660 | the propose **seam** the finalizer already uses, already generic over fakes: `Proposed` `:55`, `ProposeError` `:88`, `RaftProposer` `:104-111`, `LeaderForwarder` `:129-138`, `ClusterWriter` `:157-161`, `new` `:165-171`, `propose` `:182-203` |
| `server/tests/cluster_handoff_barrier.rs` | 723 | the only end-to-end forcing tests. `finalize_handoff` `:208-229`; `a_write_parked_by_the_barrier_wakes_up_redirected` `:231-286`; **`a_source_that_cannot_drain_aborts_the_finalization` `:288-389`** (TRYAGAIN prefix assertion `:334-343`); `:391-458` (093/083); `:460-517` (094); `:519-590` (096); `:614-723` (097) |
| `server/src/connection/cluster.rs` | 229 | the finalizer's sole caller: `handle_slot_migration` `:133-151` (`complete` at `:149`); `redirect_to_response` `:21-26` — `pub(crate)`, so it is a move blocker |
| `types/src/redirect.rs` | 159 | the redirect **seam**. `TRYAGAIN_MSG` `:22`; `tryagain()` `:50-52`; **`tryagain_slot_handoff(slot)` `:65-67`** — the seam already owns a handoff-specific TRYAGAIN; tests `:132-158` |
| `server/src/slot_migration/slot_fence.rs` | 346 | the seam's one handoff consumer today (`redirect::tryagain_slot_handoff(fence.slot)` `:159`), FM-CLUSTER-095, ten in-crate tags `:215-330` |
| `Justfile` | — | `lint-gates` `:329`; **`lint-redirect-seam` `:442-473`** — greps `CROSSSLOT`, `MOVED`, `ASK`; **not `TRYAGAIN`** |
| `.cargo/mutants.toml` | — | `exclude_globs = ["**/tests/**", …]` — the integration forcing test is not even a mutation *target*, let alone a killer |
| `.scratch/hardening/specs/cluster-failure-modes.md` | 1573 | status/gate `:1-7`; **stale scope paragraph `:53-56`**; FM-CLUSTER-084 `:1196-1206`, -085 `:1208-1219`, -086 `:1221-1232`, -087 `:1234-1245`, -088 `:1247-1258`, **-089 `:1262-1272`**, **-090 `:1274-1285`**, **-091 `:1287-1298`**, -092 `:1300-…`, -095 `:1335`, -096 `:1353`, -097 `:1374` |
| `adr/0004-replication-runtime-seams.md` | 83 | the governing precedent for this exact move and its exact cost (`:73-83`) |

## Problem

### 1. The finalizer is an implementation with no interface

`complete` (`:208-253`) is one `async fn` that performs, in sequence: mint a timestamp
(`handoff_now_ms`, `:209`), propose `PrepareSlotHandoff` (`:210-219`), branch on
`Response::Error` (`:220-222`), poll local replicated state for *its own* attempt by proposer
timestamp (`:227`), poll again for `drained` (`:234`), on failure propose `AbortSlotHandoff`
best-effort and render a TRYAGAIN string (`:236-244`), otherwise propose `CompleteSlotMigration`
with a second freshly-minted timestamp (`:246-252`). Underneath it, `poll_handoff` (`:278-297`)
owns the deadline, the sleep cadence, and the reach into `cluster_state.get_slot_migration(slot)
.handoff`.

Nothing in that sequence is named. There is no `FinalizeState`, no `FinalizeStep`, no
`FinalizeOutcome` — the states exist only as program counter positions inside an `async fn`, and
the outcomes exist only as three `Response` values built at three different lines. The **only**
way to observe any of it is to stand up a Raft cluster and read a wire reply.

Compare the sibling half, one crate down, for the *same* protocol:

| | source side (`handoff_barrier.rs`) | finalizer side (`slot_migration/mod.rs`) |
|---|---|---|
| decision | `plan_handoff_action(event, self_id, num_shards) -> HandoffAction` — pure, `:97-120` | inlined in `complete`, `:208-253` |
| named states/outcomes | `HandoffAction::{Arm, Release, Ignore}` `:71-91` | none |
| driver | `run_slot_handoff_barrier(...)` `:177-219` | `complete` + `poll_handoff`, `:208-297` |
| propose leg | injected closure `confirm: C` `:183-187` — "a parameter rather than a `ClusterWriter` so this loop can be tested without a live Raft instance" (`:173-176`) | `self.commit(...)`, which builds a real `ClusterWriter` at `:313-317` |
| in-crate tests | 6, tagged FM-CLUSTER-089/090/091, `:305-464` | 0 |
| clock | via `handoff_now_ms`, in-crate | via `handoff_now_ms`, **imported across the crate boundary** at `:52` |

The last row is the tell. The finalizer already reaches *down* into `frogdb-cluster-runtime` for
its clock, and *down* into `frogdb-cluster` for `ClusterCommand`, `ClusterWriter`, `ClusterState`,
`SlotHandoff`, `ClusterError::is_retryable` and all five `HANDOFF_*` constants. The **locality**
is inverted: every collaborator is below, and only the `Response` rendering belongs where the code
sits.

### 2. The specification of this code is a transcription in a crate that cannot import it

`cluster/src/model/mod.rs:15-23` states the discipline the model holds itself to:

> The model never re-implements the state machine. Every step loads a node's `ClusterSnapshot`
> back into a real `ClusterState` … calls the production `ClusterState::apply_command` … So the
> thing being checked is `commands.rs` itself.

and then `:36-45` records the exception:

> The coordinator is a direct transcription of the *control flow* of
> `SlotMigrationCoordinator::complete` (server crate) — prepare, poll for *our own* prepare by
> proposer timestamp, poll for drained, complete; abort-and-TRYAGAIN if the drain never lands;
> TRYAGAIN with **no** abort if the prepare never becomes visible.

The transcription is `Coord` (`:242-254`), `observed_prepare` (`:427-434`), `observed_drained`
(`:437-443`) and the two `next_state` arms (`:607-650`). **I verified all five arms against
production and they agree today** — prepare-refused → `GaveUp` matches `:220-222`; give-up from
`AwaitPrepared` proposes no abort, matching `:227-232`; give-up from `AwaitDrained` proposes
`Abort` first, matching `:236-244`; `Complete` carries a second, later timestamp, matching `:250`.
This is a **divergence risk that has not diverged**, not a defect.

It is worth naming as a risk anyway because the sibling proposal 58 found the analogous failover
transcription (`cluster/src/model/failover/replay.rs:51-66`) *had* diverged from its production
planner. Same mechanism, same crate, one instance already realized.

**An honest constraint that shapes the design.** The obvious remedy — "let the model consume the
production planner" — is only available if the planner lands in `frogdb-cluster`. The crate graph
is `frogdb-cluster ← frogdb-core ← frogdb-cluster-runtime` (verified:
`cluster/Cargo.toml` depends on protocol/openraft/rocksdb/tokio/serde and **not** on
`frogdb-core` or `frogdb-cluster-runtime`; `core/src/lib.rs:10` re-exports `frogdb_cluster as
cluster`; `cluster-runtime/Cargo.toml` depends on `frogdb-core`). A planner placed in
`frogdb-cluster-runtime` **cannot** be imported by the model, because that edge is a cycle. A
naive "move it next to `handoff_barrier.rs`" therefore buys the mutation gate but not the model.

A second constraint pushes the other way: `frogdb-cluster` has **no** dependency on
`frogdb-types`, so `frogdb_core::clock` — and therefore `handoff_now_ms` — is unavailable there.
That is *why* `handoff_now_ms` sits in `cluster-runtime` (`handoff_barrier.rs:55-60`) despite
being a `frogdb-cluster` concept. The clock cannot go down; the pure decision can.

### 3. `poll_handoff`'s budget is spent twice, and the constant's doc explains it once

`HANDOFF_DRAIN_WAIT_MS = 50` carries this justification (`cluster/src/types.rs:661-665`):

> Default budget the finalizer waits for the drain confirmation before it aborts, in milliseconds.
> Deliberately smaller than `HANDOFF_BARRIER_MS` so the abort proposal and a successful `Complete`
> both still land inside the barrier window.

`HANDOFF_BARRIER_MS = 100` (`:659`), and FM-CLUSTER-084's Invariant (`:1204`) repeats the claim:
*"The finalizer's own wait budget (`HANDOFF_DRAIN_WAIT_MS`) is deliberately shorter than the
barrier window so both a successful `Complete` and an abort still land inside it."*

`poll_handoff` (`:283`) sets `deadline = now + HANDOFF_DRAIN_WAIT_MS` **per call**, and `complete`
calls it twice in series — `await_prepared_seq` (`:227`) then `await_drained` (`:234`). Worst case
the finalizer spends `50 + 50 = 100 ms` *after* `prepared_at_ms` was minted at `:209`, plus the
prepare's own Raft round trip, before it proposes anything. The barrier window closes at
`prepared_at_ms + 100`.

**This is not a safety defect and is not offered as one.** `SlotHandoff::admits_complete_at`
refuses a late `Complete` (FM-CLUSTER-084) and the source's local pause outlives the replicated
window by construction (`handoff_barrier.rs:18-26`), so the worst outcome is a `HandoffNotReady`
→ `TRYAGAIN` → operator retry: wasted work, not a stranded write. Reachability is real but
narrow — it needs local apply lag to eat most of the first budget, which is exactly the case the
poll exists for.

What is architecturally wrong is that **nobody owns the arithmetic**. `HANDOFF_DRAIN_WAIT_MS` is a
single named constant used as two independent deadlines, and the relation "the finalizer's *total*
budget must fit inside `barrier_ms`" is stated in two doc comments and enforced nowhere. The model
does not catch it either: `Action::CoordGivesUp` is enabled unconditionally in both waiting states
(`model/mod.rs:535`, `:541`), so give-up is over-approximated as nondeterminism and the timing
relation between the two budgets and the barrier window is never evaluated. **Latent, unmodelled,
non-safety** — and it is exactly the kind of fact a named `FinalizeBudget` would make assertable.
Filed as a follow-up (spec-first) rather than folded in.

### 4. The finalizer's TRYAGAIN strings are outside the seam, and the gate cannot see them

Four TRYAGAIN wire families exist. Two are owned by the redirect seam
(`types/src/redirect.rs`): `tryagain()` `:50-52` (keys straddle an open slot) and
`tryagain_slot_handoff(slot)` `:65-67` (a handoff was prepared under an already-validated
command, FM-CLUSTER-095, consumed at `slot_fence.rs:159`). The seam even carries a test asserting
the two share a prefix and differ in body (`:151-158`).

The other two are built inline in the finalizer:

* `:229-231` — `"TRYAGAIN slot {} handoff not ready: prepare did not become visible"`
* `:241-243` — `"TRYAGAIN slot {} handoff not ready: source did not drain in {}ms"`

plus a third, composed at `:324-329`, where `commit` picks the `TRYAGAIN`/`ERR` prefix from
`ClusterError::is_retryable` and concatenates the error's `Display`
(`"slot {0} handoff not ready: {1}"`, `types.rs:636`). All three imitate the same grammar as
`HandoffNotReady`'s `Display`, by hand, in a crate that does not own it.

`just lint-redirect-seam` (`Justfile:442-473`) is the gate that would normally forbid this. **It
does not cover TRYAGAIN.** Verified: it greps exactly `Response::error\("CROSSSLOT` and
`Response::error\((format!\()?"(MOVED|ASK) `, and its own doc-comment `:442-448` names only
MOVED/ASK/CROSSSLOT. So the finalizer's strings are not a lint violation today; they are a gap in
the lint.

**No drift exists today** — all four forms start `TRYAGAIN ` (pinned by
`the_two_tryagain_forms_share_a_prefix_and_differ_in_body`, `redirect.rs:151-158`, and by
`a_source_that_cannot_drain_aborts_the_finalization`'s `starts_with("TRYAGAIN ")` assertion,
`cluster_handoff_barrier.rs:337-340`). The **bodies** are asserted nowhere: a tree-wide grep for
`"did not drain in"` and `"prepare did not become visible"` finds only the two definitions and one
prose mention in a closed issue. That is precisely the condition under which a refactor silently
changes a client-visible string, which is why they are carried verbatim below.

### Why this is shallow, in the round's vocabulary

* **Depth.** `plan_handoff_action` is 24 lines and stands in front of "which node am I, is this
  event mine, which shard owns this slot, what does zero shards mean". `complete` is 46 lines and
  stands in front of nothing — it *is* the procedure. A caller cannot ask it a question; it can
  only run it against a cluster.
* **Seam.** The `confirm: C` closure in `run_slot_handoff_barrier` (`:183-187`) exists with an
  explicit rationale: *"a parameter rather than a `ClusterWriter` so this loop can be tested
  without a live Raft instance"*. The finalizer, which proposes **four** commands to the barrier's
  one, has no such seam — it manufactures a `ClusterWriter` per proposal (`:313-317`), even though
  `ClusterWriter` is itself already generic over `RaftProposer` and `LeaderForwarder` fakes
  (`writer.rs:104-148`, `:157-161`). The fakeable seam exists one crate down and the caller
  reaches past it.
* **Locality.** A change to the finalization protocol — a third phase, a different poll source, a
  budget that accounts for both waits — today touches `slot_migration/mod.rs` (server),
  `handoff_barrier.rs` (cluster-runtime), `commands.rs`/`types.rs` (cluster) **and**
  `model/mod.rs`'s transcription, and only the first three are found by grep. After the move the
  two client-side halves sit in one crate and the model consumes the planner instead of copying
  it.
* **Deletion test.** Delete a `HandoffFinalizer` and: the four handoff states go back to being
  program-counter positions; the two poll helpers reappear as private methods on a server type;
  the double-budget arithmetic goes back to being unnamed; the model's `Coord` transcription
  becomes load-bearing again; and the only test of the whole procedure is a three-node cluster
  that kills a node. That is leverage — a five-argument call standing in front of state naming,
  attempt identification by proposer timestamp, the two poll predicates, the abort-vs-no-abort
  fork, deadline arithmetic and outcome classification.
* **The shallow version to reject.** "Move `complete` verbatim into `cluster-runtime` and keep it
  an `async fn`." It relocates 130 lines, pays the full 0.80 gate cost, and buys nothing: still no
  named states, still untestable without Raft, still hand-transcribed by a model that cannot
  import it. If the pure decision is not extracted, do not do the move at all.

## Proposed change

**Three layers, split by what each crate is allowed to know.** The shape is a deliberate mirror of
`handoff_barrier.rs` and `migration_events.rs` — the two modules that already establish
`plan_* : pure` + `run_* : async, injected propose` in this area.

### Layer 1 — `frogdb-cluster/src/handoff_finalizer.rs` (new, pure, no clock, no async)

```rust
/// Where a finalization is, between two observations of replicated state.
pub enum FinalizeState {
    /// `PrepareSlotHandoff` proposed at this proposer-minted timestamp; the
    /// attempt is identified by it, because a forwarded proposal never sees a
    /// response and the `seq` is assigned during apply.
    AwaitPrepared { prepared_at_ms: u64 },
    /// Our attempt is visible; waiting for the source's drain confirmation.
    AwaitDrained { seq: u64 },
}

/// What the finalizer should do next, given what it just read.
pub enum FinalizeStep {
    /// Nothing observable yet — poll again if the budget allows.
    Wait,
    /// Advance, and propose this entry.
    Propose(ClusterCommand),
    /// Advance state without proposing (prepare became visible).
    Advance(FinalizeState),
}

/// Why a finalization ended without moving ownership.
pub enum FinalizeGiveUp { PrepareInvisible, Undrained }

pub fn prepare_command(slot, source, target, now_ms) -> ClusterCommand;
pub fn observe(state: &FinalizeState, handoff: Option<&SlotHandoff>) -> FinalizeStep;
pub fn give_up_command(state: &FinalizeState) -> Option<ClusterCommand>;  // Abort iff AwaitDrained
pub fn complete_command(slot, source, target, now_ms) -> ClusterCommand;
```

Every function is total, synchronous and clock-free — `now_ms` arrives as **data**, exactly as it
does in the Raft entries themselves (FM-CLUSTER-089's whole point). This is what lets the module
live in `frogdb-cluster` despite that crate having no clock seam, and it is what lets the model
import it.

`observe` is the two poll predicates, named:
`AwaitPrepared` matches on `h.prepared_at_ms == prepared_at_ms`; `AwaitDrained` on
`h.seq == seq && h.drained`. Byte-for-byte the predicates at `mod.rs:258-260` and `:266`.

`give_up_command` is the abort-vs-no-abort fork, named. Today that fork is the difference between
returning at `:228` and returning at `:240`, and it is the single most subtle rule in the
procedure — an abort from `AwaitPrepared` would name a `seq` the finalizer never learned.

**The model rewires to consume it.** `observed_prepare` (`:427-434`) and `observed_drained`
(`:437-443`) are deleted; `Coord`'s two waiting variants wrap `FinalizeState`; the `Action::Coord`
and `Action::CoordGivesUp` arms (`:607-650`) call `observe` and `give_up_command`. The model keeps
exactly what it should keep — the *scheduling* nondeterminism (when a coordinator steps, when it
gives up, when a drain lands) — and stops owning the *decision*. The header's exemption at `:36-45`
shrinks to "the model supplies the budget's nondeterminism; the decision is production code",
which is the discipline `:15-23` already claims for the transition function.

### Layer 2 — `frogdb-cluster-runtime/src/handoff_finalizer.rs` (new, async driver)

```rust
/// The wire-independent result of one finalization attempt.
pub enum FinalizeOutcome {
    Completed,
    Refused(ClusterResponse),         // the state machine said no; caller renders prefix
    GaveUp(FinalizeGiveUp),           // -> TRYAGAIN, body chosen by the caller
    Redirect(LeaderRedirect),
    RaftError(String),
}

/// Drive one two-phase finalization to a terminal outcome.
///
/// `propose` is a parameter rather than a `ClusterWriter` for the same reason
/// `run_slot_handoff_barrier`'s `confirm` is: so this loop is testable without a
/// live Raft instance.
pub async fn finalize_slot_handoff<P, F>(
    cluster_state: &ClusterState,
    slot: u16, source: NodeId, target: NodeId,
    budget: FinalizeBudget,          // { drain_wait_ms, poll_interval_ms }
    propose: P,
) -> FinalizeOutcome
where P: Fn(ClusterCommand) -> F, F: Future<Output = Result<Proposed, ProposeError>>;
```

This layer owns exactly three things and no others: the **clock** (`handoff_now_ms`, already here
at `handoff_barrier.rs:55-60` — the cross-crate import at `slot_migration/mod.rs:52` disappears),
the **sleep cadence and deadline** (`poll_handoff`'s `:283-296` loop, once, with the budget named
rather than re-derived per call), and the **propose leg** as an injected closure.

`FinalizeBudget` is the smallest honest home for finding 3: one struct with two fields and a
constructor from the `HANDOFF_*` defaults, which is where a future `debug_assert!(2 *
drain_wait_ms <= barrier_ms)` — or a corrected single shared deadline — will go. **This proposal
adds the type and no assertion**: adding one is a behavior change and therefore spec-first.

### Layer 3 — `frogdb-server/src/slot_migration/mod.rs` (what stays)

`complete` becomes rendering, and nothing else:

```rust
pub async fn complete(&self, slot: u16, source: NodeId, target: NodeId) -> Response {
    let writer = self.writer();                       // one per finalization, not per proposal
    let outcome = frogdb_cluster_runtime::finalize_slot_handoff(
        &self.cluster_state, slot, source, target, FinalizeBudget::default(),
        |cmd| writer.propose(cmd),
    ).await;
    match outcome {
        FinalizeOutcome::Completed          => Response::ok(),
        FinalizeOutcome::Refused(resp)      => self.render_state_machine_error(resp),  // :320-330 verbatim
        FinalizeOutcome::GaveUp(why)        => redirect::tryagain_handoff_not_ready(slot, why),
        FinalizeOutcome::Redirect(r)        => redirect_to_response(r),                // :334
        FinalizeOutcome::RaftError(e)       => Response::error(format!("ERR Raft error: {}", e)),
    }
}
```

Wire strings stay in the server-and-below **adapter** layer, never in the runtime. `begin`,
`cancel`, `commit`, `route`, `snapshot`, `spawn_event_dispatcher` and `spawn_handoff_barrier` are
untouched.

### The TRYAGAIN strings — verbatim, and moved to the seam

The two bodies move to `frogdb-types/src/redirect.rs` beside `tryagain_slot_handoff`, **character
for character**:

```rust
pub fn tryagain_handoff_not_ready(slot: u16, why: FinalizeGiveUp) -> Response {   // or (&str)
    match why {
        PrepareInvisible => Response::error(format!(
            "TRYAGAIN slot {} handoff not ready: prepare did not become visible", slot)),
        Undrained { wait_ms } => Response::error(format!(
            "TRYAGAIN slot {} handoff not ready: source did not drain in {}ms", slot, wait_ms)),
    }
}
```

Acceptance: a byte-literal test in `redirect.rs` pinning both strings, landing **before** the
extraction (see effort table step 1). Note the two grammars in the tree today are *"TRYAGAIN slot
N handoff not ready: …"* (finalizer, lowercase `slot`) and *"TRYAGAIN Slot N finalization in
progress"* (`tryagain_slot_handoff`, capitalised). **Do not harmonise them.** Both are
client-visible; only the `TRYAGAIN ` prefix is pinned by a test, and unifying the bodies is a
behavior change dressed as tidying.

`FinalizeGiveUp` carrying `wait_ms` matters: the `{}ms` in the second body is
`HANDOFF_DRAIN_WAIT_MS` today (`:243`), which after the move is `budget.drain_wait_ms`. If the
renderer re-reads the constant instead, a non-default budget would print a lie.

## Testability improvement

**Today the finalizer's test surface is a three-node cluster; after the move it is four function
calls.** Concretely, each of these is impossible today and a unit test after:

1. **The abort-vs-no-abort fork, directly.** `give_up_command(&AwaitPrepared{..}) == None` and
   `give_up_command(&AwaitDrained{seq}) == Some(AbortSlotHandoff{slot, seq})`. This is
   FM-CLUSTER-091's and FM-CLUSTER-087's shared hinge and nothing asserts it in isolation;
   `a_source_that_cannot_drain_aborts_the_finalization` reaches it only by killing a node
   (`cluster_handoff_barrier.rs:327`).
2. **Attempt identification by proposer timestamp.** `observe(&AwaitPrepared{at}, Some(&handoff))`
   returns `Wait` when another finalizer's prepare (different `prepared_at_ms`) is what is visible.
   This is the "our own attempt" rule the doc at `:224-226` explains at length and no test names.
   It is FM-CLUSTER-086's finalizer-side counterpart.
3. **Stale-drain rejection at the poll.** `observe(&AwaitDrained{seq: 7}, Some(&handoff{seq: 8,
   drained: true}))` is `Wait`, not `Propose(Complete)` — the finalizer's half of
   FM-CLUSTER-086's "a stale drain acknowledgement must not vouch for a fresh attempt".
4. **Budget exhaustion with a paused clock.** `frogdb-cluster-runtime`'s dev-dependencies already
   carry `tokio` with `test-util` (`cluster-runtime/Cargo.toml`), which is what
   `a_shard_that_never_answers_is_never_confirmed` (`handoff_barrier.rs:420`) uses. The finalizer's
   timeout path gets the same treatment: advance the clock past `drain_wait_ms`, assert exactly one
   `AbortSlotHandoff` reached the fake propose sink, assert `GaveUp(Undrained)`. **In-crate,
   sub-millisecond, and it forces FM-CLUSTER-091's finalizer half — which nothing in a mutated
   crate forces today.** (`frogdb-cluster` has only `macros`/`rt`, no `test-util`, which is a
   further reason the driver belongs in cluster-runtime.)
5. **Poll count.** With `poll_interval_ms` injected, "the common case resolves in the first poll or
   two" (`types.rs:667-671`) becomes an assertion instead of a comment.
6. **The model stops being a second implementation.** After layer 1, a change to `observe` changes
   the model's behavior with no edit in `model/mod.rs` — the property the model header already
   claims for `apply_command` (`:15-23`), extended to the control flow it currently copies.

## Spec and gate impact of the move (the load-bearing section)

### Which rows cover the finalizer today, and where their forcing tests live

Verified by grepping every `// FM-CLUSTER-08[4-9]` and `// FM-CLUSTER-09[0-7]` tag in
`frogdb-server/crates`:

| row | what it says about the finalizer | forcing tests | crate they live in |
|---|---|---|---|
| **091** | Invariant explicitly names `server/src/slot_migration/mod.rs`: *"`SlotMigrationCoordinator::complete` polls its own replicated state … on timeout it aborts and renders `ClusterError::is_retryable` as `TRYAGAIN` rather than `ERR`"* | `a_shard_that_never_answers_is_never_confirmed` `handoff_barrier.rs:419`; `a_missing_shard_fails_the_drain` `:442`; `only_a_not_ready_handoff_is_retryable` `types.rs:921`; **`a_source_that_cannot_drain_aborts_the_finalization` `cluster_handoff_barrier.rs:288`** | cluster-runtime ×2, cluster ×1, **frogdb-server integration ×1** |
| **089** | Invariant names `cluster-runtime/src/handoff_barrier.rs` for `handoff_now_ms`; the two *mint sites* are `slot_migration/mod.rs:209` and `:250` | `handoff_deadlines_are_pure_functions_of_replicated_data` `commands.rs:1799`; `handoff_now_ms_reads_the_clock_seam` `handoff_barrier.rs:454` | cluster, cluster-runtime |
| **084/085/086/087/088** | the *state machine's* half of the same protocol | 16 tags in `commands.rs:1400-1799` + `model/tests.rs:32-52` | cluster |
| **090** | the source side | 4 tags `handoff_barrier.rs:305-394` | cluster-runtime |
| **092/093/094/096/097** | what a parked command observes across a finalization — all call `finalize_handoff` (`:208-229`), i.e. `complete`'s happy path, incidentally | tags at `cluster_handoff_barrier.rs:231,391,460,519,614` | **frogdb-server integration** |

**The finding: exactly one FM-tagged test exercises the finalizer's control flow, and it is a
three-node integration test in `frogdb-server`.** `frogdb-server` has no mutation gate, and
`.cargo/mutants.toml`'s `exclude_globs = ["**/tests/**"]` means that file is not even a mutation
*target*. So the 130 lines under discussion are, today, **scored by nothing**.

### What the move does to the 0.80 gate

The baseline (spec header `:1-7`, retrospective `.scratch/hardening/retrospective-2026-08-05.md:13`):
`frogdb-cluster` 99.6% on 496 mutants, `frogdb-cluster-runtime` 99.0% on 224, gate 0.80, four
documented equivalents.

Moving code from an ungated crate into a 99.0%-scoring gated one is **strictly a risk**, and the
mechanism is the one ADR-0004 already recorded at `:73-83`: *"`cargo mutants -p X` builds and runs
only that package's tests … raising the real score means moving forcing tests down into the
crates, not tuning the gate."* That ADR measured `frogdb-replication-runtime` at **50.0%** for
exactly this reason — every seam whose only caller was a live two-node test survived.

Arithmetic, to size it. `cluster-runtime` at 99.0% of 224 means ~2 surviving mutants. The moved
code is ~130 lines of branchy async: conservatively 25-40 mutants (three `match`/`if` forks, two
predicates, a deadline comparison, a loop). If they land with only the current tests to kill them,
**every one survives** — the crate drops to roughly `222/(224+35) ≈ 86%`. Still above 0.80, but it
converts a 19-point margin into a 6-point one and buries the four documented equivalents in noise.
Doing this to two crates at once (with sibling 58 also adding targets to `cluster-runtime`) is how
a gate stops being a signal.

**Therefore, non-negotiable acceptance criteria:**

1. **In-crate forcing tests land in the same commit as the code they force, in the crate that will
   be mutated.** Layer-1 tests in `frogdb-cluster`, layer-2 tests in `frogdb-cluster-runtime`.
   `cargo mutants -p` runs only the package's own tests — a test written in `frogdb-server`
   contributes zero to either score.
2. **`a_source_that_cannot_drain_aborts_the_finalization` stays exactly where it is, unedited.**
   It is the only end-to-end proof that a killed source produces `TRYAGAIN` with the migration
   record intact and `SETSLOT STABLE` still working. The new unit tests are *additional* forcing
   tests for FM-CLUSTER-091, never a replacement. Same for the five integration tests that call
   `finalize_handoff`.
3. **`just mutants-diff frogdb-cluster` and `… frogdb-cluster-runtime` before pushing; then full
   `just mutants` + `just mutants-gate <crate> 0.80` for both.** A full run, not just the diff: the
   move relocates mutation targets across a crate boundary and the diff view would score the new
   modules against nothing. Testbox-class workload.
4. **Spec edits are citation-only, and the lint will not check them.**
   `scripts/failure-modes.py` parses only backticked names in the `Forced by` cell and matches them
   against `cargo nextest list` over `NEXTEST_CRATES` (`:64-77`), which already includes
   `frogdb-cluster`, `frogdb-cluster-runtime` and `frogdb-server`. It is **name-keyed and
   path-agnostic**: moving a tagged test between those crates keeps `just lint-failure-modes` green
   with no spec edit at all. Invariant prose is never parsed. So these are human-review items:
   * FM-CLUSTER-091's Invariant (`:1294`) names `server/src/slot_migration/mod.rs` and
     `SlotMigrationCoordinator::complete`; both citations must follow the code. **Meaning
     unchanged** — the polling-own-state rationale and the `TRYAGAIN`-not-`ERR` rule are exactly
     preserved.
   * FM-CLUSTER-089's Invariant (`:1270`) already cites `cluster-runtime/src/handoff_barrier.rs`;
     the mint sites move into `handoff_finalizer.rs` in the same crate, so the citation gains a
     file.
   * Both rows' `Forced by` cells gain the new in-crate test names. Adding a `// FM-CLUSTER-NNN`
     tag *without* adding the name to the row **fails** `lint-failure-modes` (it enforces both
     directions), so this edit is mechanical and enforced.
5. **No behavior change in the extraction commits.** The two budget calls stay two budget calls
   (finding 3 is a follow-up); the two TRYAGAIN bodies stay byte-identical; `complete`'s four
   proposals stay four proposals in the same order with the same payloads. Anything else is
   spec-first: row → failing test → fix.
6. **The model rewire is its own commit**, so a `model-check` regression bisects cleanly. The three
   model configs (`handoff_model_smoke` / `_full_cross_slot` / `_full_deep`, `model/tests.rs:32-52`,
   forcing FM-CLUSTER-084/085/086/088/100) must pass with **identical state and depth counts** to
   the table at `model/mod.rs:66-71` — 31 324/31, 1 156/14, 1 306 692/30, 12 186 542/54. A changed
   count means the rewire changed the transcription's semantics, and review stops there.

## Risks / scope boundaries vs sibling proposals

### vs proposal 60 (MigrationTable, RC8) — the sharpest edge, and it fixes the order

60 owns the **state machine's** slot-handoff lifecycle: the eight `apply_command` arms in
`cluster/src/commands.rs:48-783`, the record types at `cluster/src/types.rs:712-739` and `:820-894`,
and a new `cluster/src/migrations.rs` with `drop_handoff` as the sole removal path. 62 owns the
**client** of that state machine. Same protocol, opposite sides of the Raft log.

| unit | owner |
|---|---|
| `apply_command`'s handoff arms, `release_events`, `admits_complete_at`, `SlotMigration`/`SlotHandoff` shape | **60** |
| new `cluster/src/migrations.rs` | **60** |
| `complete`/`await_*`/`poll_handoff` (`slot_migration/mod.rs:208-297`) | **62** |
| new `cluster/src/handoff_finalizer.rs`, new `cluster-runtime/src/handoff_finalizer.rs` | **62** |
| `model/mod.rs`'s `Coord` + `observed_*` (the coordinator transcription, `:242-254`, `:427-443`, `:607-650`) | **62** |
| `model/mod.rs`'s `apply_next`/`commit` (the state-machine driver, `:415-422`) | **neither** — both go through production `apply_command` unchanged |
| `handoff_barrier.rs` | **neither** — lane counter-example, not to be restructured |

Three conflict edges:

1. **Textual, trivial.** Both add a `pub mod` line to `cluster/src/lib.rs`. Five-line region;
   whichever lands second rebases.
2. **Real, and it dictates order — 62 first.** 62's `observe` takes `Option<&SlotHandoff>` and
   reads `prepared_at_ms`, `seq`, `drained`. If 60 lands first and relocates `SlotHandoff` into
   `migrations.rs` behind a `MigrationTable` accessor, 62's imports and possibly its signature move
   with it. The lane's own order already says 60 is **"L, solo, land last"**; this proposal
   confirms that and asks for the same. If 60 does land first, 62's `observe` takes whatever
   accessor `MigrationTable` exposes — a signature change, not a design change.
3. **Shared re-gate.** Both re-gate `frogdb-cluster` at 0.80. Land them as a chain and run the full
   gate once at the end of the chain, not twice. **Do not run them concurrently**: two changes
   restructuring one locked crate's handoff code at the same time is the shared-tree trap.

They do **not** overlap in spec rows in any load-bearing way: 60 re-cites FM-CLUSTER-084/087's
Invariants (which name `commands.rs`), 62 re-cites FM-CLUSTER-089/091's (which name
`slot_migration/mod.rs` and `handoff_barrier.rs`). Both touch `.scratch/hardening/specs/
cluster-failure-modes.md`; different rows, different cells. Coordinate the file, not the content.

### vs proposal 58 (auto-failover, RC6) — same crate, same gate, one shared open question

58 changes `cluster-runtime/src/failure_detector.rs` (2381 lines) and `cluster/src/network.rs`;
62 adds `cluster-runtime/src/handoff_finalizer.rs`. **No file overlap.** Two real edges:

1. **Both add mutation targets to `frogdb-cluster-runtime` and both must clear 0.80.** Per the
   arithmetic above, a crate at 99.0% of 224 has ~2 survivors of margin in absolute terms; two
   proposals adding branchy code simultaneously can eat it. Land them in a chain, gate once at the
   end, and if both are in flight the second one runs `mutants-diff` against the first's merge
   base, not `origin/main`.
2. **They face the identical crate-cycle constraint, and should answer it the same way.** 58
   proposes extracting `plan_auto_failover` so *"the model consume[s] production code"* — but 58's
   detector lives in `frogdb-cluster-runtime` and its model lives in `cluster/src/model/failover/`,
   and `frogdb-cluster` cannot depend on `frogdb-cluster-runtime` (verified above). A planner
   extracted into `cluster-runtime` therefore **cannot** be consumed by the failover model, for the
   same reason 62 puts layer 1 in `frogdb-cluster`. This is flagged, not litigated: it is 58's
   ruling to make. If both proposals land pure planners in `frogdb-cluster`, they are two new sibling
   modules in one crate, which is fine.
3. 58 cites `slot_migration/mod.rs:309-337` (`commit`) as census site 8 — *"the only consumer of
   `is_retryable`"* — explicitly **not touched** by 58. 62 does not touch `commit` either: the
   `is_retryable` fork at `:324-329` moves nowhere and keeps rendering
   `FinalizeOutcome::Refused`. No edge.

### vs proposal 59 (ClusterEventRouter, RC7)

59 restructures `cluster/src/state.rs:879-1005` — the `apply()` event fan-out that feeds the
`SlotHandoffEvent` channel `run_slot_handoff_barrier` consumes. 62 deliberately does **not** consume
events: it polls replicated state, for the reason documented at `slot_migration/mod.rs:271-277`
(*"`CLUSTER SETSLOT` may be issued to any node, and a Raft entry applies on all of them"*). No file
overlap, no semantic overlap. `spawn_event_dispatcher` (`:91-107`) and `spawn_handoff_barrier`
(`:118-156`) are the router's consumer-side spawns and 62 leaves both untouched.

### vs proposals 53/54/55/56/57/61 — disjoint

53/54/55/56 are `frogdb-replication` (fullsync emitter, replica connection wiring, full-sync
landing, PSYNC parse); 61 is `replication/src/primary/mod.rs`; 57 is `cluster/src/network.rs`'s
`RaftNetwork` error mapping. None touches `slot_migration/`, `handoff_barrier.rs`, or the handoff
model. Only 57 shares a crate with 62's layer 1 (`frogdb-cluster`) — different file, shared 0.80
re-gate, same chain-and-gate-once advice as 58.

### Risk — the extraction's entire claim is "nothing changed", and only the pins prove it

There is no new behavioral acceptance test for the move itself; it borrows the six FM-tagged
handoff-barrier tests, the four cluster-side handoff tests, the three model configs and the six
integration tests. That is why step 1 of the effort table lands the TRYAGAIN byte-literal pins
**before** the extraction, against today's code, so the extraction commit shows them passing
unedited. A pin written alongside the refactor pins whatever the refactor produced.

### Risk — three crates for one procedure is one crate too many, if layer 1 is skipped

If layer 1 is dropped and only the driver moves, the result is worse than today: the code is in a
gated crate the model cannot import, the transcription survives, and the gate cost is paid anyway.
**Layer 1 and layer 2 land together or not at all.** Layer 3 (rendering) is what stays behind and
is not optional.

### Risk — `FinalizeOutcome::Refused(ClusterResponse)` leaks a cluster type through the runtime

It does, and deliberately: the alternative is rendering `ClusterError` in `cluster-runtime`, which
would put a wire string in a crate whose charter (`cluster-runtime/src/lib.rs:1-26`) says it owns
transport and decisions, not replies. `ClusterResponse` is a `frogdb-cluster` type both crates
already depend on. Recorded so a reviewer does not read it as an oversight.

## Effort

**M**, in five commits with one shared re-gate per crate. The mutation gate, not the code, is the
long pole.

| step | scope | size |
|---|---|---|
| **1 — pins first** | Byte-literal tests for both finalizer TRYAGAIN bodies, asserted against today's `complete` via the seam functions they will become. Land in `types/src/redirect.rs` beside `tryagain_slot_handoff_names_the_slot` (`:141-146`). No production change. | **S** — ~25 test lines |
| **2 — layer 1** | `cluster/src/handoff_finalizer.rs`: `FinalizeState`, `FinalizeStep`, `FinalizeGiveUp`, `prepare_command`/`observe`/`give_up_command`/`complete_command`, plus in-crate unit tests for testability items 1-3, tagged FM-CLUSTER-086/091. `cluster/src/lib.rs` gains one `pub mod`. | **M** — ~130 new (half tests) |
| **3 — model rewire** | `model/mod.rs`: delete `observed_prepare` (`:427-434`) and `observed_drained` (`:437-443`); `Coord`'s waiting variants wrap `FinalizeState`; `:607-650` call the planner; header `:36-45` amended to say the model supplies scheduling nondeterminism, not the decision. Own commit; the three model configs must report identical state/depth counts (`:66-71`). | **S/M** — net negative |
| **4 — layer 2 + layer 3** | `cluster-runtime/src/handoff_finalizer.rs` (`FinalizeBudget`, `FinalizeOutcome`, `finalize_slot_handoff` with injected `propose`), paused-clock tests for testability items 4-5 tagged FM-CLUSTER-091; `cluster-runtime/src/lib.rs` exports; `slot_migration/mod.rs` `:208-297` deleted, `complete` becomes the five-arm render; the two TRYAGAIN bodies move to `redirect.rs`; `handoff_now_ms` import at `:52` drops. Spec citation edits for FM-CLUSTER-089/091 in the same commit. | **M** — ~200 new (half tests), ~110 deleted |
| **re-gate** | `just mutants-diff` then full `just mutants` + `just mutants-gate … 0.80` for **both** `frogdb-cluster` and `frogdb-cluster-runtime`. Testbox-class. | — |
| *follow-up (separate issue, spec-first — not this proposal)* | The double budget of finding 3: either one shared deadline across both polls, or `debug_assert!(2 * drain_wait_ms <= barrier_ms)`, plus a model action that spends the budget instead of giving up nondeterministically. Changes when a finalization aborts, so: FM-CLUSTER-084 row edit → failing test → fix. | **S/M** |

### Independently landable ahead of the refactor

Both are doc/lint only, neither touches behavior, and both are worth landing whether or not this
proposal is approved.

**Hotfix A — 19 broken links in the LOCKED cluster spec.** Every one of FM-CLUSTER-084 through
-097 cites `[rework issue 02](../../replication-cluster-rework/issues/open/02-migration-finalization-pause-barrier.md)`
in its `Bug refs` cell. The file is at `.scratch/replication-cluster-rework/issues/**done**/02-migration-finalization-pause-barrier.md`
— `open/` contains only `03-lua-internal-write-validation.md`. 19 occurrences in
`cluster-failure-modes.md`, plus one each in
`.scratch/replication-cluster-rework/migration-pause-barrier-brief-2026-08-04.md` and
`finalization-window-measurement-2026-08-05.md`. `scripts/scratch-check.py` lints `Status:`,
directory agreement and duplicate numbers — it does **not** check links, which is why this went
unnoticed. One `sed`. **S.**

**Hotfix B — the spec's scope paragraph contradicts its own rows.**
`cluster-failure-modes.md:53-56` reads: *"Out of scope, deliberately: the rest of the pause-barrier
design — the Raft `PrepareSlotHandoff` op, the drain round trip, the handoff lease, and the fencing
token are phase 2 of rework issue 02 and get their rows when they land."* All four landed:
`PrepareSlotHandoff` is FM-CLUSTER-084/086, the drain round trip is -090/-091, the lease is -085,
the fencing token is -095. The Scope bullet list (`:14-52`) has no bullet for -084..-097 either.
Replace the paragraph with a scope bullet naming the two-phase handoff and its files. **S.**

**Optional, adjacent, and best landed with step 4 rather than ahead of it:** extend
`lint-redirect-seam` (`Justfile:442-473`) to grep inline `Response::error\((format!\()?"TRYAGAIN `
outside `types/src/redirect.rs`, closing the gap that let finding 4 exist. It is only honest after
step 4 removes the two violations it would fire on, and it will still not catch the composed form
at `slot_migration/mod.rs:324-329` — a grep gate sees literals, not concatenation. Say so in the
recipe's doc comment rather than implying coverage it does not have.
