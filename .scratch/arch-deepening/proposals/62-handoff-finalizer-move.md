# Proposal 62 — the handoff finalizer moves down to the crates that own the handoff

Round 38 · lane: replication+cluster · effort **M** · LOCKED area (cluster, mutation gate 0.80) ·
**not spec-first** (no behavior change proposed; the two behavior questions found are filed as
follow-ups, not folded in)

Covers exploration-lane candidate **RC11** ("Handoff finalizer in server crate away from barrier —
`SlotMigrationCoordinator::complete/poll` (`server/slot_migration/mod.rs:208-337`) vs
cluster-runtime `handoff_barrier.rs` model"). Verified against `08c143d6`. Sibling proposals 58, 59
and 60 are cited from their **current working-tree drafts**, which are ahead of their last commit
(`ade5ab80`); every cross-proposal line number below is as-of that working tree and is marked where
it matters.

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

The measurable consequence is a census, not an opinion, and one grep is the whole thesis:

```
$ grep -rn 'await_prepared_seq|await_drained|poll_handoff' --include='*.rs' --include='*.md' .
  | grep -v slot_migration/mod.rs
cluster/src/model/mod.rs:425:/// `await_prepared_seq`: our own attempt, identified by the timestamp we
cluster/src/model/mod.rs:426:/// minted. No lease filter — `poll_handoff` reads `handoff` directly.
cluster/src/model/mod.rs:436:/// `await_drained`.
cluster/src/model/mod.rs:610:                    // `await_prepared_seq` returned None: TRYAGAIN with *no*
cluster/src/model/mod.rs:613:                    // `await_drained` returned false: best-effort abort, then
```

**Five hits, all of them comments, all of them inside the hand transcription in a third crate.**
Nothing calls these functions; nothing tests them; the only thing in the tree that *mentions* them
is a model that copied their control flow by hand and cannot import them.

* **`complete`, `await_prepared_seq`, `await_drained` and `poll_handoff` have zero direct tests.**
  Only `connection/cluster.rs:149` calls `complete`; `slot_migration/tests.rs` is 676 lines of
  routing tests and touches none of them.
* Their only forcing test is `a_source_that_cannot_drain_aborts_the_finalization`
  (`server/tests/cluster_handoff_barrier.rs:288-389`, tag `:288`, fn `:299`) — a three-node live
  cluster that kills a node — plus the `finalize_handoff` helper (`:208-229`) four other
  integration tests call for their happy path.
* Their only *specification* is that **hand transcription**: the stateright model at
  `cluster/src/model/mod.rs:36-45` says out loud that its `Coord` enum (`:241-254`) is "a direct
  transcription of the *control flow* of `SlotMigrationCoordinator::complete` (server crate)". The
  model's header sets the discipline for the state machine — *"the model never re-implements the
  transition function"* (`:15-23`) — and then exempts the coordinator from it.

**This proposal claims no live bug.** Two latent findings, one stale-doc defect and one
seam-coverage gap are recorded below with the reason each is not reachable as a defect today.
Two independently-landable hotfixes (both doc/seam, no behavior) are proposed at the end.

**The move's whole cost is the mutation gate, and it must be paid up front.** `frogdb-server` has
no gate; `frogdb-cluster` and `frogdb-cluster-runtime` both have one at 0.80 and `cargo mutants -p`
runs only that package's tests. Moving this code moves it *into* scoring range with no in-crate
test to score it. §"Spec and gate impact" is the load-bearing section of this proposal.

## Files involved (verified at `08c143d6`)

All paths under `frogdb-server/crates/` unless noted.

| file | lines | role |
|---|---:|---|
| `server/src/slot_migration/mod.rs` | 338 | **the code that moves.** module doc `:1-32`; `use frogdb_cluster_runtime::handoff_now_ms` `:52`; the `frogdb_core` re-export block `:55-59` (all five `HANDOFF_*`, `ClusterResponse`, `ClusterState`, `NodeId`); struct `:69-73`; `new` `:77-87`; `spawn_event_dispatcher` `:91-107`; `spawn_handoff_barrier` `:118-156` (injected `confirm` closure `:134-152`); `begin` `:179-186`; **`complete` `:208-253`** (doc `:188-207`); **`await_prepared_seq` `:257-262`**; **`await_drained` `:265-269`**; **`poll_handoff` `:278-297`** (doc `:271-277`); `cancel` `:300-303`; **`commit` `:309-337`** (writer construction `:313-317`, retryable-prefix fork `:324-329`, `Forwarded` → `ok` `:333`, redirect `:334`, Raft error `:335`); inline TRYAGAIN literals `:228-231`, `:240-243` |
| `cluster-runtime/src/handoff_barrier.rs` | 465 | **the model to mirror, and a lane counter-example — not to be restructured.** `handoff_now_ms` `:55-60`; `to_ms` `:64-68`; `HandoffAction` `:72-91`; `plan_handoff_action` `:97-120`; `drain_shard` `:129-169`; `run_slot_handoff_barrier` `:177-219` (injected `confirm` `:183-187`); tests `:221-465` with FM tags at `:305`, `:341`, `:363`, `:394`, `:419`, `:442`, `:454` |
| `cluster-runtime/src/lib.rs` | 49 | crate charter (`:1-26`) and the `pub use` surface (`:35-49`) — gains one module and one export |
| `cluster-runtime/src/migration_events.rs` | 251 | the second precedent for the `plan_*` + `run_*` pair (`plan_migration_notice` `:42`, `run_slot_migration_event_dispatcher` `:91`, exported `lib.rs:43-45`) |
| `cluster/src/model/mod.rs` | 812 | **the hand transcription.** discipline statement `:15-23`; the transcription's own confession `:36-45`; exploration-budget table `:66-71`; `Entry::command` `:175-200` (expands via `scope.barrier_ms` `:183` / `scope.lease_ms` `:184`); `Coord` `:241-254`; action enabling `:516-575` (the two unconditional `CoordGivesUp` pushes at `:539`, `:545`); `observed_prepare` `:425-434`; `observed_drained` `:436-443`; `next_state` `:577`; `Action::CoordGivesUp` arm `:607-622`; `Action::Coord` arm `:623-650` |
| `cluster/src/types.rs` | 1313 | `ClusterError` `:580-638` (`HandoffNotReady` `:637`); `is_retryable` `:642-644`; `HANDOFF_BARRIER_MS = 100` `:659`; `HANDOFF_DRAIN_WAIT_MS = 50` `:665`; `HANDOFF_POLL_INTERVAL_MS = 2` `:671`; `HANDOFF_DRAIN_TIMEOUT_MS = 2000` `:678`; `HANDOFF_LEASE_MS = 10000` `:685`; `SlotHandoff` `:692-710` + impl `:712-739` (`admits_complete_at` `:736`); `SlotMigration` `:767-777` + impl `:779-798`; `ClusterSnapshot` `:802-829` + impl `:831-905`; FM-CLUSTER-091 forcing test `only_a_not_ready_handoff_is_retryable` tag `:921`, fn `:923` |
| `cluster/src/lib.rs` | — | module region `:45-62` (alphabetical, one line per module; `mod model` is `#[cfg(test)]`, `:49-50`) — gains one `pub mod` |
| `cluster/src/writer.rs` | 660 | the propose **seam** the finalizer already uses, already generic over fakes: `LeaderRedirect` `:40`, `Proposed` `:55`, `ProposeError` `:88`, `RaftProposer` `:105`, `LeaderForwarder` `:129`, `ClusterWriter` `:157-161`, `new` `:166`, `propose` `:182-203` |
| `server/tests/cluster_handoff_barrier.rs` | 723 | the only end-to-end forcing tests. `finalize_handoff` `:208-229`; FM-CLUSTER-092 `:231`, fn `:240`; **FM-CLUSTER-091 + -087 `:288`, `a_source_that_cannot_drain_aborts_the_finalization` `:299-389`** (node kill `:327`, TRYAGAIN prefix assertion `:337-340`); 093/083 `:391`; 094 `:460`; 096 `:519`; 097 `:614` |
| `server/src/connection/cluster.rs` | 229 | the finalizer's sole caller: `handle_slot_migration` `:133-151` (`complete` at `:149`); `redirect_to_response` `:21-26` (doc `:17-20`) — `pub(crate)`, so it is a move blocker |
| `server/src/server/cluster_init.rs` | 1938 | **not edited.** The single `SlotMigrationCoordinator::new` construction site, `:735` |
| `types/src/redirect.rs` | 159 | the redirect **seam**. `TRYAGAIN_MSG` `:22`; `tryagain()` `:50-52`; **`tryagain_slot_handoff(slot)` `:65-67`** — the seam already owns a handoff-specific TRYAGAIN; tests `:88-158` |
| `types/Cargo.toml` | — | the foundation crate's dependency list — `frogdb-protocol` and leaf libraries only, **no `frogdb-cluster`**. Load-bearing for the seam signature ruled below |
| `server/src/slot_migration/slot_fence.rs` | 346 | the seam's one handoff consumer today (`redirect::tryagain_slot_handoff(fence.slot)` `:159`), FM-CLUSTER-095, ten in-crate tags `:215-330` |
| `Justfile` | — | `lint-gates` `:329`; **`lint-redirect-seam` `:442-473`** (doc `:442-448`, recipe `:449-473`) — greps `CROSSSLOT` `:455`, `MOVED`/`ASK` `:460`; **not `TRYAGAIN`** |
| `.cargo/mutants.toml` | — | `exclude_globs = ["**/tests/**", …]` — the integration forcing test is not even a mutation *target*, let alone a killer |
| `.scratch/hardening/specs/cluster-failure-modes.md` | 1573 | status/gate `:1-7`; **stale scope paragraph `:53-56`**; FM-CLUSTER-084 `:1199-1211` (Invariant `:1206`, `Forced by` `:1209`), -085 `:1212-1223`, -086 `:1224-1236`, -087 `:1237-1248`, -088 `:1249-1261`, **-089 `:1262-1273`** (Invariant `:1269`, `Forced by` `:1271`), **-090 `:1274-1286`**, **-091 `:1287-1298`** (Invariant `:1294`, `Forced by` `:1296`), -092 `:1299-…` |
| `scripts/failure-modes.py` | — | `NEXTEST_CRATES` `:64-77`; `parse_forced_by` `:206-232` (backticked names only); the test→spec direction `:494-502` |
| `adr/0004-replication-runtime-seams.md` | 83 | the governing precedent for this exact move and its exact cost (`:64-70`, measured consequence `:71-83`) |

## Problem

### 1. The finalizer is an implementation with no interface

`complete` (`:208-253`) is one `async fn` that performs, in sequence: mint a timestamp
(`handoff_now_ms`, `:209`), propose `PrepareSlotHandoff` (`:210-219`), branch on
`Response::Error` (`:220-222`), poll local replicated state for *its own* attempt by proposer
timestamp (`:227`), poll again for `drained` (`:234`), on failure propose `AbortSlotHandoff`
best-effort and render a TRYAGAIN string (`:236-243`), otherwise propose `CompleteSlotMigration`
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
| named states/outcomes | `HandoffAction::{Arm, Release, Ignore}` `:72-91` | none |
| driver | `run_slot_handoff_barrier(...)` `:177-219` | `complete` + `poll_handoff`, `:208-297` |
| propose leg | injected closure `confirm: C` `:183-187` — "a parameter rather than a `ClusterWriter` so this loop can be tested without a live Raft instance" (`:173-176`) | `self.commit(...)`, which builds a real `ClusterWriter` at `:313-317` |
| in-crate tests | 6, tagged FM-CLUSTER-089/090/091, `:305-464` | 0 |
| clock | via `handoff_now_ms`, in-crate | via `handoff_now_ms`, **imported across the crate boundary** at `:52` |

The last row is the tell. The finalizer already reaches *down* into `frogdb-cluster-runtime` for
its clock, and *down* into `frogdb-cluster` (mostly through `frogdb_core`'s re-export, `:55-59`,
and once directly at `:280`) for `ClusterCommand`, `ClusterWriter`, `ClusterState`, `SlotHandoff`,
`ClusterError::is_retryable` and all five `HANDOFF_*` constants. The **locality** is inverted:
every collaborator is below, and only the `Response` rendering belongs where the code sits.

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

The transcription is `Coord` (`:241-254`), `observed_prepare` (`:425-434`), `observed_drained`
(`:436-443`) and the two `next_state` arms (`:607-650`). **I verified all five modelled arms
against production and they agree today** — prepare-refused → `GaveUp` matches `:220-222`; give-up
from `AwaitPrepared` proposes no abort, matching `:227-232`; give-up from `AwaitDrained` proposes
`Abort` first, matching `:236-243`; `AwaitPrepared` → `AwaitDrained` on a matching
`prepared_at_ms`, matching `:257-262`; `Complete` carries a second, later timestamp, matching
`:250`. This is a **divergence risk that has not diverged**, not a defect.

**Naming the sixth arm, which the model does not have.** Production's prepare has *two* refusal
paths, not one. A leader-committed refusal surfaces as `Response::Error` and is caught at `:220`.
A **forwarded** proposal returns `Ok(Proposed::Forwarded) => Response::ok()` (`:333`) whether or
not the state machine later refuses it, so a refused forwarded prepare walks
`AwaitPrepared` → budget lapses → `GaveUp` with no abort. The model has no `Forwarded` notion and
jumps straight from `Idle` to `GaveUp` (`:634-644`). The two paths reach the **same terminal
state** with the same log effects (nothing proposed after the prepare), so this is a deliberate
over-approximation of the transition and not a divergence — but it is the one arm the "all five
verified" claim does not cover, and it is named here so a future reader does not have to
rediscover it. Layer 1 does not change it: `observe` sees only replicated state, which is exactly
what production sees.

It is worth naming the divergence risk anyway because the sibling proposal 58 found the analogous
failover transcription (`cluster/src/model/failover/replay.rs:51-66`) *had* drifted from its
production planner. Same mechanism, same crate, one instance already realized.

**An honest constraint that shapes the design.** The obvious remedy — "let the model consume the
production planner" — is only available if the planner lands in `frogdb-cluster`. The crate graph
is `frogdb-cluster ← frogdb-core ← frogdb-cluster-runtime` (verified: `cluster/Cargo.toml` depends
on `frogdb-protocol`/openraft/rocksdb/tokio/serde/postcard/semver/thiserror and **not** on
`frogdb-core`, `frogdb-types` or `frogdb-cluster-runtime`; `core/src/lib.rs:10` re-exports
`frogdb_cluster as cluster`; `cluster-runtime/Cargo.toml` depends on `frogdb-core`). A planner
placed in `frogdb-cluster-runtime` **cannot** be imported by the model, because that edge is a
cycle. Proposal 58 verified the same fact independently and states it in the same terms
(58 working draft `:340-342`). A naive "move it next to `handoff_barrier.rs`" therefore buys the
mutation gate but not the model.

A second constraint pushes the other way: `frogdb-cluster` has **no** dependency on
`frogdb-types`, so `frogdb_core::clock` — and therefore `handoff_now_ms` — is unavailable there.
That is *why* `handoff_now_ms` sits in `cluster-runtime` (`handoff_barrier.rs:55-60`) despite
being a `frogdb-cluster` concept. The clock cannot go down; the pure decision can.

A third, decisive for the paused-clock test in §Testability: the two crates' **dev**-dependencies
are asymmetric. `cluster/Cargo.toml` carries `tokio = { features = ["macros", "rt"] }`;
`cluster-runtime/Cargo.toml` carries `["macros", "rt", "test-util"]`. Only the latter can run
`#[tokio::test(start_paused = true)]`, which is what
`a_shard_that_never_answers_is_never_confirmed` (`handoff_barrier.rs:419`) already uses. The
budget-exhaustion test is therefore only writable where the driver goes.

### 3. `poll_handoff`'s budget is spent twice, and two docs assert the opposite

`HANDOFF_DRAIN_WAIT_MS = 50` carries this justification (`cluster/src/types.rs:661-665`):

> Default budget the finalizer waits for the drain confirmation before it aborts, in milliseconds.
> Deliberately smaller than `HANDOFF_BARRIER_MS` so the abort proposal and a successful `Complete`
> both still land inside the barrier window.

`HANDOFF_BARRIER_MS = 100` (`:659`), and FM-CLUSTER-084's Invariant (`:1206`) repeats the claim:
*"The finalizer's own wait budget (`HANDOFF_DRAIN_WAIT_MS`) is deliberately shorter than the
barrier window so both a successful `Complete` and an abort still land inside it."*

`poll_handoff` (`:283`) sets `deadline = now + HANDOFF_DRAIN_WAIT_MS` **per call**, and `complete`
calls it twice in series — `await_prepared_seq` (`:227`) then `await_drained` (`:234`). Worst case
the finalizer spends `50 + 50 = 100 ms` *after* `prepared_at_ms` was minted at `:209`, plus the
prepare's own Raft round trip, before it proposes anything. The barrier window closes at
`prepared_at_ms + 100`. **The clause "so both a successful `Complete` and an abort still land
inside it" is therefore false as written** — it is true of one budget, and the finalizer spends
two.

**This is not a safety defect and is not offered as one.** `SlotHandoff::admits_complete_at`
(`types.rs:736`) refuses a late `Complete` (FM-CLUSTER-084) and the source's local fence is armed
at *apply* time and so outlives the replicated window by construction
(`handoff_barrier.rs:18-26`; the model records the same offset at `mod.rs:205-207`), so the worst
outcome is a `HandoffNotReady` → `TRYAGAIN` → operator retry: wasted work, not a stranded write.
Reachability is real but narrow — it needs local apply lag to eat most of the first budget, which
is exactly the case the poll exists for.

What is architecturally wrong is that **nobody owns the arithmetic**. `HANDOFF_DRAIN_WAIT_MS` is a
single named constant used as two independent deadlines, and the relation "the finalizer's *total*
budget must fit inside `barrier_ms`" is stated in two doc comments and enforced nowhere. The model
does not catch it either: `Action::CoordGivesUp` is enabled unconditionally in both waiting states
(`model/mod.rs:539`, `:545`), so give-up is over-approximated as nondeterminism and the timing
relation between the two budgets and the barrier window is never evaluated. **Latent, unmodelled,
non-safety** — and it is exactly the kind of fact a named `FinalizeBudget` would make assertable.
Filed as a follow-up (spec-first) rather than folded in; the FM-CLUSTER-084 Invariant clause it
disproves gets a `Bug refs` pointer in this proposal's own step 5 (doc-only, see
[Spec and gate impact](#4-spec-edits-are-citation-only-and-the-lint-will-not-check-them)).

### 4. The finalizer's TRYAGAIN strings are outside the seam, and the gate cannot see them

Four TRYAGAIN wire families exist. Two are owned by the redirect seam
(`types/src/redirect.rs`): `tryagain()` `:50-52` (keys straddle an open slot) and
`tryagain_slot_handoff(slot)` `:65-67` (a handoff was prepared under an already-validated
command, FM-CLUSTER-095, consumed at `slot_fence.rs:159`). The seam even carries a test asserting
the two share a prefix and differ in body (`:151-158`).

The other two are built inline in the finalizer:

* `:228-231` — `"TRYAGAIN slot {} handoff not ready: prepare did not become visible"`
* `:240-243` — `"TRYAGAIN slot {} handoff not ready: source did not drain in {}ms"`

plus a third, composed at `:324-329`, where `commit` picks the `TRYAGAIN`/`ERR` prefix from
`ClusterError::is_retryable` and concatenates the error's `Display`
(`"slot {0} handoff not ready: {1}"`, `types.rs:637`). All three imitate the same grammar as
`HandoffNotReady`'s `Display`, by hand, in a crate that does not own it.

`just lint-redirect-seam` (`Justfile:442-473`) is the gate that would normally forbid this. **It
does not cover TRYAGAIN.** Verified: it greps exactly `Response::error\("CROSSSLOT` (`:455`) and
`Response::error\((format!\()?"(MOVED|ASK) ` (`:460`), and its own doc-comment `:442-448` names
only MOVED/ASK/CROSSSLOT. So the finalizer's strings are not a lint violation today; they are a
gap in the lint.

**No drift exists today** — all four forms start `TRYAGAIN ` (pinned by
`the_two_tryagain_forms_share_a_prefix_and_differ_in_body`, `redirect.rs:151-158`, and by
`a_source_that_cannot_drain_aborts_the_finalization`'s `starts_with("TRYAGAIN ")` assertion,
`cluster_handoff_barrier.rs:337-340`). The **bodies** are asserted nowhere, and the grep is
stronger than "no test": a tree-wide search for `"did not drain in"` and
`"prepare did not become visible"` returns **exactly two hits — the two definitions themselves**
(`slot_migration/mod.rs:229`, `:241`). Not one test, not one spec cell, not one doc reference.
That is precisely the condition under which a refactor silently changes a client-visible string,
which is why the pins land **first** and the bodies are carried verbatim below.

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
  (`writer.rs:105`, `:129`, `:157-161`). The fakeable seam exists one crate down and the caller
  reaches past it.
* **Locality.** A change to the finalization protocol — a third phase, a different poll source, a
  budget that accounts for both waits — today touches `slot_migration/mod.rs` (server),
  `handoff_barrier.rs` (cluster-runtime), `commands.rs`/`types.rs` (cluster) **and**
  `model/mod.rs`'s transcription, and only the first three are found by grep. After the move the
  two client-side halves sit in one crate and the model consumes the predicates instead of
  copying them.
* **Deletion test.** Delete a handoff finalizer module and: the two handoff wait-states go back to
  being program-counter positions; the two poll helpers reappear as private methods on a server
  type; the double-budget arithmetic goes back to being unnamed; the model's `Coord` transcription
  becomes load-bearing again; and the only test of the whole procedure is a three-node cluster
  that kills a node. That is leverage — a small pure API standing in front of state naming,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinalizeState {
    /// `PrepareSlotHandoff` proposed at this proposer-minted timestamp; the
    /// attempt is identified by it, because a forwarded proposal never sees a
    /// response and the `seq` is assigned during apply.
    AwaitPrepared { prepared_at_ms: u64 },
    /// Our attempt is visible; waiting for the source's drain confirmation.
    AwaitDrained { seq: u64 },
}

/// What the finalizer should do next, given what it just read. No variant
/// carries a `ClusterCommand`: building one needs the slot, the two node ids
/// and — for `CompleteSlotMigration` — a *fresh* clock read, none of which this
/// layer may see. The driver turns `Ready` into the command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinalizeStep {
    /// Nothing observable yet — poll again if the budget allows.
    Wait,
    /// Advance to this state without proposing (our prepare became visible).
    Advance(FinalizeState),
    /// The drain is confirmed for our attempt: propose the completion.
    Ready,
}

/// Why a finalization ended without moving ownership. `wait_ms` is the budget
/// that actually lapsed, so the renderer cannot print a constant the run did
/// not use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinalizeGiveUp {
    PrepareInvisible,
    Undrained { wait_ms: u64 },
}

pub fn prepare_command(
    slot: u16, source_node: NodeId, target_node: NodeId,
    barrier_ms: u64, lease_ms: u64, proposed_at_ms: u64,
) -> ClusterCommand;

pub fn complete_command(
    slot: u16, source_node: NodeId, target_node: NodeId, proposed_at_ms: u64,
) -> ClusterCommand;

/// The two poll predicates, as one total function of state and observation.
pub fn observe(state: &FinalizeState, handoff: Option<&SlotHandoff>) -> FinalizeStep;

/// The abort-vs-no-abort fork. `Some(AbortSlotHandoff { slot, seq })` only from
/// `AwaitDrained`, because only there has a `seq` been learned.
pub fn give_up_command(slot: u16, state: &FinalizeState) -> Option<ClusterCommand>;
```

**Signature notes, ruled rather than sketched.**

* `FinalizeStep::Ready` is a marker, **not** `Propose(ClusterCommand)`. A `Propose` variant cannot
  be built here: `CompleteSlotMigration` needs `source_node`, `target_node` and a
  `proposed_at_ms` that is a *fresh* clock read (production mints it at `:250`, after the drain is
  seen), and a clock read is exactly what layer 1 forbids itself. The driver owns the
  translation: `Ready => propose(complete_command(slot, source, target, handoff_now_ms()))`.
* `give_up_command` takes `slot` explicitly, because `FinalizeState` does not carry it and
  `AbortSlotHandoff { slot, seq }` (`:238`) needs both.
* `prepare_command` takes `barrier_ms` and `lease_ms` as parameters, matching production, which
  passes `HANDOFF_BARRIER_MS`/`HANDOFF_LEASE_MS` explicitly at `:215-216`. This is not
  generalisation for its own sake: the model's `Entry::command` (`:175-200`) fills the same two
  fields from `scope.barrier_ms`/`scope.lease_ms` (`:183-184`) with values of 1 and 2 ms against
  production's 100 and 10 000, and a constructor that hard-coded the constants would be unusable
  by the model — which is half of why layer 1 exists.
* `observe` is a **pure predicate pair**, byte-for-byte the two conditions at `mod.rs:259`
  (`h.prepared_at_ms == prepared_at_ms`, yielding `Advance(AwaitDrained { seq: h.seq })`) and
  `:266` (`h.seq == seq && h.drained`, yielding `Ready`). Everything else is `Wait`, including
  `handoff: None`.
* **Visibility: `pub mod handoff_finalizer`, not `pub(crate)`.** `pub(crate)` would suffice for
  the model (`mod model` is `#[cfg(test)]`, in-crate, `lib.rs:49-50`) but layer 2 lives in
  `frogdb-cluster-runtime`, a different crate, and calls `observe`/`give_up_command`/both command
  constructors. Cross-crate use requires `pub`. Recorded so a reviewer does not read the widened
  surface as carelessness.

**What the model actually gains — the reduced, honest claim.** `observed_prepare` (`:425-434`) and
`observed_drained` (`:436-443`) are deleted and replaced by `observe`; the `Action::CoordGivesUp`
arm's abort-vs-no-abort fork (`:607-622`) is replaced by `give_up_command`. That is **two
predicates and one fork** — the parts §2 calls the subtle ones. It is *not* the whole coordinator:

* `Coord::Idle`'s arm (`:625-644`) **stays a transcription**. It models a prepare being refused by
  inspecting whether the leader minted a `seq`, which is a model-side observation of the log, not
  a production code path (production reads a `Response`, and cannot even see the refusal when the
  proposal was forwarded — the sixth arm above).
* `Entry::command` (`:175-200`) keeps building the entries, now by calling `prepare_command` /
  `complete_command` with `scope.barrier_ms`/`scope.lease_ms`, so the model's scaled budgets keep
  working.
* The **scheduling nondeterminism** — when a coordinator steps, when it gives up, when a drain
  lands, when a leader changes — stays the model's, and must.

So the exemption at `:36-45` shrinks rather than disappears: from "the coordinator is a direct
transcription of the control flow" to "the coordinator's *scheduling* is modelled; its two poll
predicates and its give-up fork are production code; its prepare-refusal is a deliberate
over-approximation". That is a smaller claim than "the model stops being a second implementation",
and it is the true one.

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

/// The finalizer's two deadlines and its poll cadence, in one place.
pub struct FinalizeBudget { pub drain_wait_ms: u64, pub poll_interval_ms: u64 }

/// Drive one two-phase finalization to a terminal outcome.
///
/// `propose` is a parameter rather than a `ClusterWriter` for the same reason
/// `run_slot_handoff_barrier`'s `confirm` is: so this loop is testable without a
/// live Raft instance.
pub async fn finalize_slot_handoff<P, F>(
    cluster_state: &ClusterState,
    slot: u16, source_node: NodeId, target_node: NodeId,
    budget: FinalizeBudget,
    propose: P,
) -> FinalizeOutcome
where P: Fn(ClusterCommand) -> F, F: Future<Output = Result<Proposed, ProposeError>>;
```

This layer owns exactly four things and no others: the **clock** (`handoff_now_ms`, already here
at `handoff_barrier.rs:55-60` — the cross-crate import at `slot_migration/mod.rs:52` disappears),
the **sleep cadence and deadline** (`poll_handoff`'s `:283-296` loop, once, with the budget named
rather than re-derived per call), the **propose leg** as an injected closure, and the
`FinalizeStep::Ready` → `complete_command(…, handoff_now_ms())` translation layer 1 cannot do.

`FinalizeBudget` is the smallest honest home for finding 3: one struct with two fields and a
constructor from the `HANDOFF_*` defaults, which is where a future `debug_assert!(2 *
drain_wait_ms <= barrier_ms)` — or a corrected single shared deadline — will go. **This proposal
adds the type and no assertion**: adding one is a behavior change and therefore spec-first. To
keep the extraction behaviour-identical, the driver arms the deadline **twice**, once per wait,
exactly as `poll_handoff` does today.

### Layer 3 — `frogdb-server/src/slot_migration/mod.rs` (what stays)

`complete` becomes rendering, and nothing else:

```rust
pub async fn complete(&self, slot: u16, source_node: NodeId, target_node: NodeId) -> Response {
    let writer = self.writer();                       // one per finalization, not per proposal
    let budget = FinalizeBudget::default();
    let outcome = frogdb_cluster_runtime::finalize_slot_handoff(
        &self.cluster_state, slot, source_node, target_node, budget,
        |cmd| writer.propose(cmd),
    ).await;
    match outcome {
        FinalizeOutcome::Completed     => Response::ok(),
        FinalizeOutcome::Refused(resp) => render_state_machine_error(resp),   // extracted from `commit`
        FinalizeOutcome::GaveUp(FinalizeGiveUp::PrepareInvisible) =>
            redirect::tryagain_handoff_prepare_invisible(slot),
        FinalizeOutcome::GaveUp(FinalizeGiveUp::Undrained { wait_ms }) =>
            redirect::tryagain_handoff_undrained(slot, wait_ms),
        FinalizeOutcome::Redirect(r)   => redirect_to_response(r),            // :334
        FinalizeOutcome::RaftError(e)  => Response::error(format!("ERR Raft error: {}", e)),
    }
}
```

Wire strings stay in the server-and-below **adapter** layer, never in the runtime.

**`commit` is edited — trivially, and the earlier draft's "untouched" claim was wrong.** The
`is_retryable` → `TRYAGAIN`/`ERR` fork at `:320-330` is *extracted* out of `commit` into a free
`render_state_machine_error(ClusterResponse) -> Response`, which both `commit` and the new
`complete` call. The extracted body is bit-identical and `commit`'s remaining structure — writer
construction, the four `match` arms, `Forwarded` → `ok` — is unchanged. It is a one-hunk edit, and
it is stated here because proposal 58 cites `commit` as census site 8 (see
[vs 58](#vs-proposal-58-auto-failover-rc6--same-crates-same-gate)). `begin`, `cancel`, `route`,
`snapshot`, `spawn_event_dispatcher` and `spawn_handoff_barrier` are untouched, and
`SlotMigrationCoordinator::new`'s signature is unchanged — the coordinator keeps `raft` and
`network_factory` (`:70-72`) because `commit` still needs them.

### The TRYAGAIN strings — verbatim, and moved to the seam

The two bodies move to `frogdb-types/src/redirect.rs` beside `tryagain_slot_handoff`, **character
for character**, as **two flat constructors that name no cluster type**:

```rust
/// `TRYAGAIN slot <slot> handoff not ready: prepare did not become visible` —
/// the finalizer's own `PrepareSlotHandoff` never appeared in replicated state
/// within its budget, so it gave up *without* proposing an abort (it never
/// learned an attempt `seq` to abort).
pub fn tryagain_handoff_prepare_invisible(slot: u16) -> Response {
    Response::error(format!(
        "TRYAGAIN slot {} handoff not ready: prepare did not become visible", slot))
}

/// `TRYAGAIN slot <slot> handoff not ready: source did not drain in <wait_ms>ms`
/// — the source never confirmed the drain, so the finalizer aborted its attempt
/// and answered retryably (FM-CLUSTER-091).
pub fn tryagain_handoff_undrained(slot: u16, wait_ms: u64) -> Response {
    Response::error(format!(
        "TRYAGAIN slot {} handoff not ready: source did not drain in {}ms", slot, wait_ms))
}
```

**This signature is ruled, not offered.** The tempting form —
`tryagain_handoff_not_ready(slot: u16, why: FinalizeGiveUp)` — would put a `frogdb-cluster` type in
`frogdb-types`' public signature, which requires `frogdb-types` to depend on `frogdb-cluster`.
Verified: `types/Cargo.toml` depends on `frogdb-protocol` and leaf libraries only, and
`cluster/Cargo.toml` does not depend on `frogdb-types`, so that edge is **not a cycle — it
compiles**, which is the worse outcome: it silently drags `openraft`, `rocksdb`, `postcard` and
`semver` into the crate every other crate in the tree sits on top of. Two flat `u16`/`u64`
constructors keep the foundation crate a foundation. The `match` on `FinalizeGiveUp` lives in
**layer 3**, the only layer that depends on both crates. The `(&str)` hedge in the earlier draft
is deleted.

`wait_ms` travelling as data (not re-read from `HANDOFF_DRAIN_WAIT_MS` in the renderer) is what
keeps the `{}ms` honest under a non-default `FinalizeBudget`; it originates as
`budget.drain_wait_ms` and is carried through `FinalizeGiveUp::Undrained`.

Acceptance: a byte-literal test in `redirect.rs` pinning both strings, landing **before** the
extraction (see effort table step 1). Note the two grammars in the tree today are *"TRYAGAIN slot
N handoff not ready: …"* (finalizer, lowercase `slot`) and *"TRYAGAIN Slot N finalization in
progress"* (`tryagain_slot_handoff`, capitalised). **Do not harmonise them.** Both are
client-visible; only the `TRYAGAIN ` prefix is pinned by a test, and unifying the bodies is a
behavior change dressed as tidying.

## Testability improvement

**Today the finalizer's test surface is a three-node cluster; after the move it is four function
calls.** Concretely, each of these is impossible today and a unit test after:

1. **The abort-vs-no-abort fork, directly.** `give_up_command(slot, &AwaitPrepared{..}) == None`
   and `give_up_command(slot, &AwaitDrained{seq}) == Some(AbortSlotHandoff{slot, seq})`. This is
   FM-CLUSTER-091's and FM-CLUSTER-087's shared hinge (both tags sit on the same test,
   `cluster_handoff_barrier.rs:288`) and nothing asserts it in isolation; that test reaches it only
   by killing a node (`:327`).
2. **Attempt identification by proposer timestamp.** `observe(&AwaitPrepared{at}, Some(&handoff))`
   returns `Wait` when another finalizer's prepare (different `prepared_at_ms`) is what is visible.
   This is the "our own attempt" rule the doc at `:224-226` explains at length and no test names.
   It is FM-CLUSTER-086's finalizer-side counterpart.
3. **Stale-drain rejection at the poll.** `observe(&AwaitDrained{seq: 7}, Some(&handoff{seq: 8,
   drained: true}))` is `Wait`, not `Ready` — the finalizer's half of FM-CLUSTER-086's "a stale
   drain acknowledgement must not vouch for a fresh attempt". Likewise
   `observe(&AwaitDrained{..}, None)` is `Wait`.
4. **Budget exhaustion with a paused clock.** `frogdb-cluster-runtime`'s dev-dependencies carry
   `tokio` with `test-util` (`cluster-runtime/Cargo.toml`), which is what
   `a_shard_that_never_answers_is_never_confirmed` (`handoff_barrier.rs:419`) uses. The finalizer's
   timeout path gets the same treatment: advance the clock past `drain_wait_ms`, assert exactly one
   `AbortSlotHandoff` reached the fake propose sink, assert `GaveUp(Undrained { wait_ms })`.
   **In-crate, sub-millisecond, and it forces FM-CLUSTER-091's finalizer half — which nothing in a
   mutated crate forces today.** (`frogdb-cluster`'s dev-`tokio` has only `macros`/`rt`, no
   `test-util`, which is a further reason the driver belongs in cluster-runtime.)
5. **Poll count.** With `poll_interval_ms` injected, "the common case resolves in the first poll or
   two" (`types.rs:667-671`) becomes an assertion instead of a comment.
6. **The model stops copying two predicates and a fork.** After layer 1, a change to `observe` or
   `give_up_command` changes the model's behavior with no edit in `model/mod.rs` — the property the
   model header already claims for `apply_command` (`:15-23`), extended to the three pieces of
   control flow it currently copies. The scheduling nondeterminism and the `Idle` arm stay the
   model's, as §"Layer 1" states.

## Spec and gate impact of the move (the load-bearing section)

### Which rows cover the finalizer today, and where their forcing tests live

Verified by grepping every `// FM-CLUSTER-08[4-9]` and `// FM-CLUSTER-09[0-7]` tag in
`frogdb-server/crates`:

| row | what it says about the finalizer | forcing tests | crate they live in |
|---|---|---|---|
| **091** | Invariant (`:1294`) explicitly names `server/src/slot_migration/mod.rs`: *"`SlotMigrationCoordinator::complete` polls its own replicated state … on timeout it aborts and renders `ClusterError::is_retryable` as `TRYAGAIN` rather than `ERR`"* | `a_shard_that_never_answers_is_never_confirmed` `handoff_barrier.rs:419`; `a_missing_shard_fails_the_drain` `:442`; `only_a_not_ready_handoff_is_retryable` `types.rs:921`; **`a_source_that_cannot_drain_aborts_the_finalization` `cluster_handoff_barrier.rs:288`** | cluster-runtime ×2, cluster ×1, **frogdb-server integration ×1** |
| **089** | Invariant (`:1269`) names `cluster-runtime/src/handoff_barrier.rs` for `handoff_now_ms`; the two *mint sites* are `slot_migration/mod.rs:209` and `:250` | `handoff_deadlines_are_pure_functions_of_replicated_data` `commands.rs`; `handoff_now_ms_reads_the_clock_seam` `handoff_barrier.rs:454` | cluster, cluster-runtime |
| **084/085/086/087/088** | the *state machine's* half of the same protocol | 15 tags across `commands.rs` + 6 in `invariants.rs` + `model/mod.rs` + `model/tests.rs:32,42,52` | cluster |
| **090** | the source side | 4 tags `handoff_barrier.rs:305-394` | cluster-runtime |
| **092/093/094/096/097** | what a parked command observes across a finalization — all call `finalize_handoff` (`:208-229`), i.e. `complete`'s happy path, incidentally | tags at `cluster_handoff_barrier.rs:231,391,460,519,614` | **frogdb-server integration** |

**The finding: exactly one FM-tagged test exercises the finalizer's control flow, and it is a
three-node integration test in `frogdb-server`.** `frogdb-server` has no mutation gate, and
`.cargo/mutants.toml`'s `exclude_globs = ["**/tests/**"]` means that file is not even a mutation
*target*. So the 130 lines under discussion are, today, **scored by nothing**.

### What the move does to the two 0.80 gates

The baseline (spec header `:1-7`, retrospective `.scratch/hardening/retrospective-2026-08-05.md:13`):
`frogdb-cluster` 99.6% on 496 mutants, `frogdb-cluster-runtime` 99.0% on 224, gate 0.80, four
documented equivalents.

Moving code from an ungated crate into two 99%-scoring gated ones is **strictly a risk**, and the
mechanism is the one ADR-0004 already recorded at `:64-70`: *"`cargo mutants -p frogdb-replication`
builds and runs only that package's tests. The forcing tests for most of these behaviors live in
`frogdb-server/crates/server/tests/integration_replication.rs`, which never runs against a mutant
… raising the real score means moving forcing tests down into the crates, not tuning the gate."*
That ADR measured `frogdb-replication-runtime` at **50.0%** (`:71-73`) for exactly this reason —
every seam whose only caller was a live two-node test survived.

Arithmetic, to size both sides. This is a worst case in which the new in-crate tests kill nothing;
it is not a prediction, it is the ceiling on the damage.

* **`frogdb-cluster-runtime` — the tight one.** 99.0% of 224 ≈ 222 caught, 2 missed. Layer 2 is
  ~130 lines of branchy async: conservatively 25-40 mutants (an outcome `match`, the
  `Ready`/`Advance`/`Wait` fork, a deadline comparison, a loop, the clock read). All surviving →
  `222/(224+35) ≈ 86%`. Still above 0.80, but it converts a 19-point margin into a 6-point one
  and buries the four documented equivalents in noise.
* **`frogdb-cluster` — the roomier one.** 99.6% of 496 ≈ 494 caught, 2 missed. Layer 1 is ~65
  production lines of total, pure functions: conservatively 15-25 mutants (two struct-literal
  constructors, the two `observe` predicates, the give-up fork). All surviving →
  `494/(496+20) ≈ 96%`. A 19.6-point margin becomes ~15.7. Much more forgiving, because the
  denominator is more than double — but pure functions are also the easiest mutants in the tree to
  kill, so the realistic outcome here is "no measurable movement".

The asymmetry is the argument for the split, not against it: the layer that is hard to score is
small and async, and the layer that is easy to score is where the decisions are.

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
3. **Byte-identical client-visible error text**, in both directions. The two finalizer TRYAGAIN
   bodies (pinned by step 1) *and* `ClusterError::HandoffNotReady`'s payload text
   (`types.rs:637`), which the composed form at `:324-329` concatenates onto a `TRYAGAIN `/`ERR `
   prefix. This is proposal 60's criterion 3 restated in 62's own criteria at 60's explicit
   request (60 working draft `:554-556`): 60 *moves* the five `HandoffNotReady` construction
   sites, 62 *renders* them, and 62's wire output is only unchanged if 60's text is.
4. **Spec edits are citation-only, and the lint will not check them.**
   `scripts/failure-modes.py` parses only backticked names in the `Forced by` cell
   (`parse_forced_by` `:206-232`) and matches them against `cargo nextest list` over
   `NEXTEST_CRATES` (`:64-77`), which already includes `frogdb-cluster`, `frogdb-cluster-runtime`
   and `frogdb-server`. It is **name-keyed and path-agnostic**: moving a tagged test between those
   crates keeps `just lint-failure-modes` green with no spec edit at all. Invariant prose is never
   parsed. So these are human-review items:
   * FM-CLUSTER-091's Invariant (`:1294`) names `server/src/slot_migration/mod.rs` and
     `SlotMigrationCoordinator::complete`; both citations must follow the code. **Meaning
     unchanged** — the polling-own-state rationale and the `TRYAGAIN`-not-`ERR` rule are exactly
     preserved.
   * FM-CLUSTER-089's Invariant (`:1269`) already cites `cluster-runtime/src/handoff_barrier.rs`;
     the mint sites move into `handoff_finalizer.rs` in the same crate, so the citation gains a
     file.
   * **FM-CLUSTER-084's `Bug refs` (`:1210`) gains a pointer to the finding-3 follow-up issue**,
     because that row's Invariant (`:1206`) asserts the double-budget claim finding 3 disproves.
     `Bug refs` is not parsed by `failure-modes.py` at all, so this is a doc-only edit with zero
     lint interaction and it does not make 62 spec-first. The Invariant sentence itself is **not**
     edited here — correcting it is the follow-up's job, spec-first.
   * Both 089's and 091's `Forced by` cells (`:1271`, `:1296`) gain the new in-crate test names.
     Adding a `// FM-CLUSTER-NNN` tag *without* adding the name to the row **fails**
     `lint-failure-modes` (the test→spec direction, `:494-502`), so this edit is mechanical and
     enforced.
5. **No behavior change in the extraction commits.** The two budget calls stay two budget calls
   (finding 3 is a follow-up); the two TRYAGAIN bodies stay byte-identical; `complete`'s four
   proposals stay four proposals in the same order with the same payloads; the extracted
   `render_state_machine_error` is bit-identical to `:320-330`. Anything else is spec-first:
   row → failing test → fix.
6. **The model rewire is its own commit**, so a `model-check` regression bisects cleanly. The three
   model configs (`handoff_model_smoke` / `_full_cross_slot` / `_full_deep`, tags at
   `model/tests.rs:32`, `:42`, `:52`, forcing FM-CLUSTER-084/085/086/088/100) must pass with
   **identical state and depth counts** to the table at `model/mod.rs:66-71` — smoke 31 324/31,
   cross-slot 1 306 692/30, deep 12 186 542/54 (the fourth row, `unbounded_lag_scope` 1 156/14, is
   driven by a separate test in the same file and is bound by the same rule). A changed count
   means the rewire changed the transcription's semantics, and review stops there.

## Risks / scope boundaries vs sibling proposals

> Sibling line numbers below are from each proposal's **current working-tree draft**, which is
> ahead of `ade5ab80`. Where a sibling has revised a position since the lane brief, the substance
> is quoted rather than relied on by number.

### vs proposal 60 (MigrationTable, RC8) — same protocol, opposite sides of the Raft log

60 owns the **state machine's** slot-handoff lifecycle: nine `apply_command` arms in
`cluster/src/commands.rs` (`RemoveNode` `:215-236`, `Failover` `:393-499`, `BeginSlotMigration`
`:527-563`, `PrepareSlotHandoff` `:565-625`, `ConfirmSlotHandoffDrained` `:627-648`,
`AbortSlotHandoff` `:650-676`, `CompleteSlotMigration` `:678-762`, `CancelSlotMigration`
`:764-772`, `ResetCluster` `:816-862`), plus a new `cluster/src/migrations.rs` holding a
`MigrationTable` **borrowed façade** over `ClusterStateInner`'s `migrations` and `handoff_seq`
fields. 62 owns the **client** of that state machine.

*Three descriptions in this proposal's earlier draft were wrong and are corrected here at 60's
request:* it said "eight arms in `commands.rs:48-783`" (there are nine, and that span omits
`ResetCluster`, which is where the generation is rewound); it attributed `types.rs:712-739` to 60
(that is `impl SlotHandoff`, which 60's file table explicitly marks **"DTO, deliberately
unchanged"**) and cited a range `:820-894` that appears nowhere in 60 (the real `ClusterSnapshot`
is `:802-829` + impl `:831-905`, and 60 touches only its two migration fields read-only); and it
invented a method **`drop_handoff`** as 60's "sole removal path". No such method is proposed
anywhere in 60 — its removal surface is five methods (`abort`, `cancel`, `complete`,
`prune_naming`, `reset`), all routed through the already-private free function `release_events`,
and 60 has since withdrawn the claim that making that function private is itself the enforcement
mechanism.

| unit | owner |
|---|---|
| `apply_command`'s nine arms, `release_events`, `admits_complete_at`, `MigrationTable` | **60** |
| new `cluster/src/migrations.rs` | **60** |
| `complete`/`await_*`/`poll_handoff` (`slot_migration/mod.rs:208-297`), `commit`'s error render | **62** |
| new `cluster/src/handoff_finalizer.rs`, new `cluster-runtime/src/handoff_finalizer.rs` | **62** |
| `model/mod.rs`'s `observed_*` and the `CoordGivesUp` fork (`:425-443`, `:607-622`) | **62** |
| `SlotHandoff` / `SlotMigration` and their impls (`types.rs:692-798`) | **neither** — 60 freezes them as DTOs; 62 only reads them |
| `ClusterError::HandoffNotReady` payload text (`types.rs:637`, five construction sites) | **shared, frozen by both** (60's criterion 3 = 62's criterion 3) |
| `model/mod.rs`'s `apply_next`/`commit` (`:415-422`) | **neither** — both go through production `apply_command` unchanged |
| `handoff_barrier.rs` | **neither** — lane counter-example, not to be restructured |

**Conflict edges — one withdrawn, two standing.**

1. **Textual, trivial.** Both add one line to `cluster/src/lib.rs`'s module region (`:45-62`, one
   alphabetical line per module): 60 a private `mod migrations;`, 62 a `pub mod
   handoff_finalizer;`. Whichever lands second rebases. This is a **two-way** region, not
   three-way: proposal 58's planner move, which would have added a third line, has been **ruled
   out by 58 itself** (see below), so 58 adds no module here.
2. **WITHDRAWN — the `SlotHandoff` relocation edge.** The earlier draft argued 62 must land first
   because 60 might relocate `SlotHandoff` behind a `MigrationTable` accessor, moving 62's
   `observe` signature with it. **That is false and 62 withdraws it.** 60 does not relocate the
   record types: `MigrationTable` borrows `&mut BTreeMap<u16, SlotMigration>` and `&mut u64`, is
   serde-invisible by construction (the fields keep their position and names because
   `ClusterStateInner` is the Raft snapshot wire format, `encoding_golden.rs:1-29`), and 60's
   own "what deliberately does not change" list freezes `types.rs:692-798`. `use
   crate::types::SlotHandoff` resolves identically before and after. 60 flagged this for 62 to
   withdraw; withdrawn.
3. **Shared re-gate, and it still dictates the order — but for 60's reason, not 62's.** Both
   re-gate `frogdb-cluster` at 0.80. Land them as a chain and run the full gate once at the end,
   not twice. **Do not run them concurrently**: two changes restructuring one LOCKED crate's
   handoff code at the same time is the shared-tree trap. The order that falls out is **62 before
   60**, taken from 60's own "solo-last" verdict, which confirms *"last" as a gate-ordering
   constraint* — 60 is the change whose value the 0.80 gate measures, so it should be the final
   shape any surviving mutant is attributed to. 62 asks for nothing more than that.

*A note on the reviewer's suggested substitute rationale.* An earlier review round proposed
re-grounding "62 first" on 60's review-legibility argument — that landing 60 after 62 lets a
reviewer attribute any client-visible string change to the right proposal. **60's current draft
explicitly withdraws that argument** (`:679-688`): 60 and 62 share no file, so 60's diff shows no
TRYAGAIN rendering change in either order, and criterion 3 is checkable by inspecting 60's diff
against `types.rs:629-645` and the five construction sites with or without 62 in flight. 60 names
the surviving reason as plain shared-tree concurrency. 62 adopts the surviving reason and does not
resurrect the withdrawn one.

They do **not** overlap in spec rows in any load-bearing way: 60 re-cites FM-CLUSTER-084/086/087's
Invariants (which name `commands.rs`), 62 re-cites FM-CLUSTER-089/091's (which name
`slot_migration/mod.rs` and `handoff_barrier.rs`) and appends to 084's `Bug refs`. Both touch
`.scratch/hardening/specs/cluster-failure-modes.md`; different cells. Coordinate the file, not the
content — and note 60's own serialization point is with **59**, over FM-CLUSTER-087's `Forced by`
cell, which 62 does not touch.

### vs proposal 58 (auto-failover, RC6) — same crates, same gate

58 changes `cluster-runtime/src/failure_detector.rs` (2381 lines, restructured **in place**) and
`cluster/src/network.rs`; 62 adds `cluster/src/handoff_finalizer.rs` and
`cluster-runtime/src/handoff_finalizer.rs`. **No file overlap.** Three edges:

1. **The crate-cycle question is settled, and 58 settled it the other way.** The earlier draft of
   62 flagged, as an open question for 58 to rule, that a planner extracted into `cluster-runtime`
   cannot be consumed by the failover model in `cluster/src/model/failover/`. **58 has now ruled**
   — *"Crate placement — recommendation: keep both planners in `frogdb-cluster-runtime`"* (58
   working draft `:326`), explicitly withdrawing its earlier draft's `frogdb-cluster`
   recommendation, on three grounds: the model-fidelity payoff is small (only `replay.rs:60`
   re-spells the decision, into the same answer for its own scope), the move does not compile as a
   mechanical range move (`test_replica_priority_store_changes_failover_target` constructs a
   `ClusterRuntimeFlags`, a `cluster-runtime` type, and cannot follow), and it re-prices a
   single-crate job as a two-crate testbox-class one. 58 instead records the equivalence in
   `replay.rs`'s comments at zero gate cost, and keeps the crate move as a costed follow-up for
   when a second `frogdb-cluster` consumer appears.

   **62's ruling is the opposite and the difference is principled, not a disagreement.** 62's
   layer 1 *has* the second consumer already — layer 2 in `cluster-runtime` and the model in
   `frogdb-cluster` both call it, which is exactly the trigger 58 names. And 62's payoff is not a
   comment: `observe` and `give_up_command` replace working transcribed code, not a coincidence.
   The two proposals agree on the underlying fact and cite it identically (58 `:340-342`: *"there
   is no dependency edge from `frogdb-cluster` to `frogdb-cluster-runtime` … the edge runs the
   other way"*); they differ only on whether this particular extraction earns the crossing. **58
   does not add a module to `cluster/src/lib.rs`**, which is why edge 1 vs 60 is two-way.
2. **Both re-gate `frogdb-cluster-runtime` at 0.80.** 58's part (b) restructures a 152-line body
   *in place* — it reshapes that crate's mutants rather than importing new ones, and its tests stay
   put — while 62 imports ~130 new lines with new tests. Different mechanisms, one gate. Per the
   arithmetic above, a crate at 99.0% of 224 has ~2 survivors of margin in absolute terms, so two
   proposals moving that crate's mutant population at once can eat it. Land them in a chain, gate
   once at the end, and if both are in flight the second runs `mutants-diff` against the first's
   merge base, not `origin/main`.
3. **`commit` is census site 8 in 58's inventory** — *"the only consumer of `is_retryable`"* —
   explicitly **not touched** by 58. 62 *does* touch it, minimally: the `is_retryable` fork at
   `:320-330` is extracted verbatim into `render_state_machine_error` so both `commit` and the new
   `complete` share it. The fork itself is bit-identical and `is_retryable` gains no new consumer
   beyond that one function, so 58's census entry stays true by count; but "62 does not touch
   `commit`" was wrong and is corrected. If 58 lands first, this is a one-hunk rebase.

### vs proposal 59 (ClusterEventRouter, RC7)

59 restructures `cluster/src/state.rs`'s `apply()` event fan-out — **`:942-1024`** inside `apply`
`:914-1040` (this proposal's earlier draft repeated the lane's stale `:879-1005`; 59 asks all three
proposals to agree on the current number, and 62 now does) — the fan-out that feeds the
`SlotHandoffEvent` channel `run_slot_handoff_barrier` consumes. 62 deliberately does **not** consume
events: it polls replicated state, for the reason documented at `slot_migration/mod.rs:271-277`
(*"`CLUSTER SETSLOT` may be issued to any node, and a Raft entry applies on all of them"*). No file
overlap, no semantic overlap. `spawn_event_dispatcher` (`:91-107`) and `spawn_handoff_barrier`
(`:118-156`) are the router's consumer-side spawns and 62 leaves both untouched.

**Answering the `cluster_init.rs` edge in one sentence:** 62 edits `cluster_init.rs` **zero times**
— the file's only relevant line is the sole `SlotMigrationCoordinator::new` construction at `:735`,
and `new`'s signature is unchanged because the coordinator still holds `raft` and
`network_factory` for `commit`. (59's *current* draft has already withdrawn the claimed edge on its
own, at `:58` and `:478`; this sentence is recorded so the question does not get re-asked.)

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
would put a client-visible wire string in the runtime crate, one layer below the only crate that
owns wire rendering for this path. `ClusterResponse` is a `frogdb-cluster` type both crates already
depend on, so the leak costs nothing at the dependency graph and keeps every `TRYAGAIN`/`ERR`
literal in `frogdb-server` + `frogdb-types`. Recorded so a reviewer does not read it as an
oversight. (An earlier draft justified this by citing `cluster-runtime/src/lib.rs:1-26` as a
charter that says the crate owns "decisions, not replies" — the charter says no such thing; it
describes the six components and the `frogdb-net` boundary. The design point stands, the citation
is deleted.)

## Effort

**M**, in **five commits** plus one gate run per crate (the re-gate is a gate run, not a commit).
The mutation gate, not the code, is the long pole.

| step | scope | size |
|---|---|---|
| **1 — pins first** | Byte-literal tests for both finalizer TRYAGAIN bodies, asserted against today's `complete` via the seam functions they will become. Land in `types/src/redirect.rs` beside `tryagain_slot_handoff_names_the_slot` (`:141-146`). No production change. **Also in this step, outside the tree:** file the finding-3 follow-up issue, so step 5's `Bug refs` edit has something to point at. | **S** — ~25 test lines |
| **2 — layer 1** | `cluster/src/handoff_finalizer.rs`: `FinalizeState`, `FinalizeStep`, `FinalizeGiveUp`, `prepare_command`/`complete_command`/`observe`/`give_up_command`, plus in-crate unit tests for testability items 1-3, tagged FM-CLUSTER-086/091. `cluster/src/lib.rs` gains one `pub mod` line in `:45-62`. | **M** — ~130 new (half tests) |
| **3 — model rewire** | `model/mod.rs`: delete `observed_prepare` (`:425-434`) and `observed_drained` (`:436-443`) in favour of `observe` at `:539`/`:545`/`:646-649`; the `CoordGivesUp` fork (`:607-622`) calls `give_up_command`; `Entry::command` (`:175-200`) calls the two command constructors with `scope.barrier_ms`/`scope.lease_ms`; header `:36-45` amended to the reduced claim (scheduling is modelled; the two predicates and the fork are production; the `Idle` prepare-refusal stays an over-approximation, and the forwarded-refusal arm is named). Own commit; the three configs must report identical state/depth counts (`:66-71`). | **S/M** — net negative |
| **4 — layer 2** | `cluster-runtime/src/handoff_finalizer.rs`: `FinalizeBudget`, `FinalizeOutcome`, `finalize_slot_handoff` with injected `propose` and the `Ready` → `complete_command(…, handoff_now_ms())` translation; paused-clock tests for testability items 4-5 tagged FM-CLUSTER-091; `cluster-runtime/src/lib.rs` gains the module and the export. `frogdb-server` still calls its own `complete`, unchanged — this commit compiles and passes with the new module unused by production. | **M** — ~120 new (half tests) |
| **5 — layer 3 + seam + spec** | `slot_migration/mod.rs` `:208-297` deleted; `complete` becomes the six-arm render; `render_state_machine_error` extracted from `commit` `:320-330`; `handoff_now_ms` import at `:52` drops; the two TRYAGAIN bodies land in `redirect.rs` as the two flat constructors and the step-1 pins are re-pointed at them. Spec citation edits for FM-CLUSTER-089/091 (`:1269`, `:1271`, `:1294`, `:1296`) plus the FM-CLUSTER-084 `Bug refs` pointer (`:1210`). | **M** — ~80 new, ~110 deleted |
| **re-gate** *(not a commit)* | `just mutants-diff` then full `just mutants` + `just mutants-gate … 0.80` for **both** `frogdb-cluster` and `frogdb-cluster-runtime`. A full run, not just the diff: the move relocates mutation targets across crate boundaries and the diff view would score the new modules against nothing. **Testbox-class workload** — and since the session default is local mode, the execution mode must be settled with the user *before* this step, not during it (`just build-mode`). | — |
| *follow-up (separate issue, spec-first — not this proposal)* | The double budget of finding 3: either one shared deadline across both polls, or `debug_assert!(2 * drain_wait_ms <= barrier_ms)`, plus a model action that spends the budget instead of giving up nondeterministically. Changes when a finalization aborts, so: FM-CLUSTER-084 Invariant edit (`:1206`) → failing test → fix. Filed in step 1; pointed at from `:1210` in step 5. | **S/M** |

### Independently landable ahead of the refactor

Both are doc/lint only, neither touches behavior, and both are worth landing whether or not this
proposal is approved. Neither is part of steps 1-5.

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

**Optional, adjacent, and best landed with step 5 rather than ahead of it:** extend
`lint-redirect-seam` (`Justfile:442-473`) to grep inline `Response::error\((format!\()?"TRYAGAIN `
outside `types/src/redirect.rs`, closing the gap that let finding 4 exist. It is only honest after
step 5 removes the two violations it would fire on, and it will still not catch the composed form
at `slot_migration/mod.rs:324-329` — a grep gate sees literals, not concatenation. Say so in the
recipe's doc comment rather than implying coverage it does not have.
