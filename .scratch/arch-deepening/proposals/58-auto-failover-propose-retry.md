# Proposal 58 — `trigger_auto_failover` decomposition, and one propose-retry policy

Round 38 · lane: replication+cluster · effort **M** · LOCKED area (cluster, mutation gate 0.80) ·
**SPEC-FIRST** for part (a)

## Summary

`FailureDetector::trigger_auto_failover` (`cluster-runtime/src/failure_detector.rs:594-745`) is a
152-line `async` **implementation** with no **interface** in front of it: it reads a snapshot,
decides whether a failover is warranted, opens sockets to probe every candidate, scores them,
proposes through Raft, runs its own retry loop, and dispatches the voter-set side effect — all in
one body, reachable only by `.await`. Around it, the tree contains **ten sites that propose a
`ClusterCommand` and decide what to do when the proposal does not land, under six mutually
inconsistent terminality policies** (census below). Two of the six live in the same 350-line
region of `cluster/src/network.rs` as the third.

This proposal is two separable pieces:

* **(b) Decomposition — interface-only, no behaviour change, no spec row.** Split the procedure
  into a *pure planner* (`plan_auto_failover(view, failed) -> AutoFailoverPlan`), an *I/O probe
  step*, and a *commit step*. The planner is the same decision the failover model already
  hand-transcribes in `cluster/src/model/failover/replay.rs:55-66` — and transcribes
  **incorrectly** (see Problem §4). Extracting it lets the model consume production code, which
  is the discipline the model's own header sets for the state machine
  (`model/failover/mod.rs:15-23`) and does not yet hold for the detector's control flow.
* **(a) Propose-retry unification — SPEC-FIRST.** One `ProposeRetry` policy **adapter** that
  names *attempts*, *backoff*, and *terminality*, replacing three hand-rolled loops and making
  the sixth policy (auto-failover's) inspectable. This one changes behaviour if the auto-failover
  site's `ForwardToLeader` handling is corrected, so it goes failure-mode row → failing test →
  fix, not code-first.

**A LIVE defect is present and it is already filed.** The auto-failover proposal path is the one
propose site that neither forwards to the leader nor is re-driven by a level-triggered loop, so a
deposed-but-still-`Leader`-believing detector burns all three attempts on `ForwardToLeader` — each
of which cannot commit by construction — and then discards the failover permanently. That is
exposure #4 of
[`.scratch/cluster-correctness/issues/open/18-a-missed-failover-is-never-retried.md`](../../cluster-correctness/issues/open/18-a-missed-failover-is-never-retried.md)
(`Status: needs-triage`, unruled). It is **not** offered as a hotfix here: the current behaviour is
pinned by a test (`auto_failover_retries_the_failover_write_with_backoff`,
`failure_detector.rs:2083-2103`), issue 18 lists three candidate rulings that are not this
proposal's to make, and closing it flips two characterization tests plus a `sometimes` model
property. Two genuinely independent hotfixes — a *false* faithfulness claim in the model's header
and a silent-outage log line — are written up at the end.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/cluster-runtime/src/failure_detector.rs` | 2381 | **the change.** `DetectorRaft` seam `:47-59`, prod impl `:61-76`; `raft_write_timeout` `:431-434`; `reconcile_topology` `:474-497`; `spawn_reconcile` `:501-521`; `mark_node_failed` `:538-567`; `mark_node_recovered` `:570-588`; **`trigger_auto_failover` `:594-745`**; `effective_priority` `:763-769`; `offset_of` `:773-779`; `compute_replica_score` `:792-797`; `select_failover_target` `:806-823`; task loop `:849-902`; tests `:1911-2103` |
| `frogdb-server/crates/cluster/src/network.rs` | 1877 | **shared file with proposal 57, different half.** `voter_retry_delay` `:669-671`, `const MAX_ATTEMPTS = 5` `:676`, `voter_change` `:709-749`, `spawn_voter_change` `:756-761`, `spawn_add_raft_voter` `:777-828`, `plan_voter_removal` `:852-863`, `spawn_remove_raft_voter` `:878-926`, `ForwardedWrite` receiver `:947-972` |
| `frogdb-server/crates/cluster/src/writer.rs` | ~500 | the existing propose **seam** the detector does *not* use. `Proposed` `:54-64`, `ProposeError` `:86-99`, `RaftProposer` `:105-123`, `LeaderForwarder` `:129-149`, `ClusterWriter::propose` `:182-205` |
| `frogdb-server/crates/cluster/src/types.rs` | — | `ClusterError::is_retryable` `:642-644` — the *typed* terminality classifier, used at exactly one of the ten sites |
| `frogdb-server/crates/cluster/src/model/failover/mod.rs` | — | model header `:15-23` (the "transition function is production code" discipline), `:34-45` (the detector control-flow transcription), **`:47-52` (the false claim, hotfix 1)**, `:54-82` (the two pinned exposures), `Det` `:224-234` |
| `frogdb-server/crates/cluster/src/model/failover/replay.rs` | — | `Node::plan_failover` `:51-66` — the hand-transcribed planner that diverges from production `:60`; `a_missed_failover_leaves_the_slot_on_a_failed_primary` `:96-160` |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | — | census sites 6 and 7: self-registration retry `:556-601`, bootstrap slot-assignment retry `:606-658` — **not touched**, cited as evidence |
| `frogdb-server/crates/server/src/slot_migration/mod.rs` | — | census site 8: `commit` `:309-337`, the only consumer of `is_retryable` `:324` — **not touched**, cited as evidence; proposal 11's file |
| `.scratch/hardening/specs/cluster-failure-modes.md` | 1500+ | FM-CLUSTER-013 `:274`, -039 `:634`, -040 `:646`, -042 `:672`, -047 `:733`, -048 `:745`, -050 `:769`, -053 `:813`, -056 `:849`, -057 `:861`, -058 `:873`, **-101 `:1454`** |
| `.scratch/cluster-correctness/issues/open/18-…md` | 105 | the LIVE defect, filed, `needs-triage` |
| `.scratch/cluster-correctness/issues/open/20-…md` | — | adjacent open issue on the same `force: true` → `VoterChange::Remove` edge; conflict edge below |

Verified against the current worktree (`117d5acc`, on top of `main` `08c143d6`); every citation was
read, not inferred. **The candidate brief's citations were stale on both counts.** Its line ranges
(`failure_detector.rs:517-659, 461-511`; `network.rs:687-741`) do not correspond to the named code
in the current tree — the correct ranges are in the table above, and `network.rs:687-741` lands in
the middle of `voter_change`, not a retry loop. Its governing-row claim (FM-CLUSTER-009/010/011) is
also **wrong**: those three rows are the version-gate and config-epoch rows (spec `:217`, `:231`,
`:248`) and have nothing to do with failover or proposal retry. The rows that actually govern this
code are enumerated under *Spec impact*.

## Problem

### 1. The procedure has no interface

`trigger_auto_failover` `:594-745` is one body performing five distinct jobs:

| step | lines | kind | reachable how |
|---|---|---|---|
| eligibility (is the failed node a primary? does it have replicas?) | `:598-612` | pure read of `snapshot()` | only via `.await` on the whole procedure |
| probe (`health_probe` RPC per candidate, `connect_timeout_ms`) | `:615-635` | network I/O | needs a live `ClusterNetworkFactory` |
| score + select | `:637-654` | pure, already extracted (`select_failover_target` `:806`) | directly testable ✅ |
| propose + retry | `:681-739` | Raft I/O + policy | needs a `DetectorRaft` fake |
| voter-set side effect | `:722-724` | Raft I/O | needs a `DetectorRaft` fake |

Three of the four early returns (`:600`, `:611`, `:652`) and the retry-exhaustion return (`:744`)
are the *decisions* this function exists to make, and none of them is observable except as "the
fake Raft recorded no write". The current tests pay for that: `build()` (`:1611-1641`),
`network_reporting` (`:1656`), `probe_factory` (`:1666`), `serve_health_probes` (`:1681`) and
`settle`/`eventually` (`:1704-1720`) are ~110 lines of fixture standing between a test and a
four-way branch. `auto_failover_ignores_a_failed_replica` (`:2058-2079`) spins up a duplex-stream
probe server to assert `f.raft.writes().is_empty()` — an assertion about a pure snapshot read.

The score/select step is the counter-example that proves the point: it was already pulled out as
free functions with an injected `priority_of` closure (`:806-823`), and it is the one step covered
by six cheap, direct unit tests (`:1335-1481`) and three spec rows (FM-CLUSTER-056/057/058).

### 2. Ten propose sites, six terminality policies

Verified census of every site that submits a `ClusterCommand` and decides what "it did not land"
means:

| # | site | attempts | backoff | `ForwardToLeader` | state-machine `Error` | re-driven later? |
|---|---|---:|---|---|---|---|
| 1 | `mark_node_failed` `fd:538-567` | 1 | — | plain error `:563` | warn + return `:549-553` | ✅ level-triggered `:486-496` |
| 2 | `mark_node_recovered` `fd:570-588` | 1 | — | plain error `:584` | not distinguished `:581` | ✅ level-triggered |
| 3 | **`trigger_auto_failover` `fd:687-739`** | **3** | **flat 500 ms `:737`** | **plain error, retried against the same non-leader `:732`** | **terminal `:704-715`** | ❌ **never** |
| 4 | `spawn_add_raft_voter` `net:777-828` | 5 | linear `500·attempt` `:669-671` | n/a (membership API) | n/a | ❌ (idempotence precheck `:784-789` instead) |
| 5 | `spawn_remove_raft_voter` `net:878-926` | 5 | linear `:909` | n/a | n/a | ❌ (classify precheck `:884-887`) |
| 6 | self-registration `init:570-600` | 30 | flat 500 ms `:589` | **forwarded** by `ClusterWriter` | ignored with the redirect `:587` | ❌ |
| 7 | bootstrap slot assign `init:623-655` | 30 | flat 500 ms `:645` | **forwarded** | ignored `:643` | ❌ |
| 8 | `SlotMigrationCoordinator::commit` `sm:309-337` | 0 | — | **forwarded** | `is_retryable()` → `TRYAGAIN` else `ERR` `:324` | client's job |
| 9 | `ClusterWriter::propose` `wr:182-205` | 0 | — | **forwarded once**, else `Redirect` | returned as `Committed(Error)` | caller's job |
| 10 | `ForwardedWrite` receiver `net:947-972` | 0 | — | n/a (this *is* the leader) | flattened to a string `:958-963` | remote caller's job |

Six distinct policies for one question. Two constants named `MAX_ATTEMPTS` hold different values
in the two crates (`fd:687` = 3, `net:676` = 5). Three different backoff shapes. The one *typed*
answer to "is this rejection retryable" — `ClusterError::is_retryable` (`types.rs:642-644`) — is
consulted at exactly one of the ten sites.

### 3. The disagreement that is LIVE

Sites 6-9 route through `ClusterWriter::propose`, whose contract (FM-CLUSTER-048, spec `:745`) is
that `ForwardToLeader` is **not a failure**: the write is forwarded over the cluster bus and lands
on the leader. Site 3 does not use `ClusterWriter` at all. Its `DetectorRaft` seam is declared as
`pub trait DetectorRaft: RaftProposer` (`fd:47`), so `client_write` is the *raw* proposer
(`writer.rs:114-123` → `openraft::Raft::client_write`), with no forwarder behind it.

The consequence, traced:

1. The detector's task gates reconciliation on `if detector.is_leader()` (`fd:871`), which reads
   `server_state()` (`fd:69-71`). A deposed leader keeps answering `Leader` until it hears
   otherwise — the model's own header states this at `model/failover/mod.rs:47-50`.
2. In that window `mark_node_failed` proposes, gets `ForwardToLeader`, warns, returns (`fd:563`);
   the level-triggered loop retries it next tick, so **nothing is lost** — the flag will land once
   a real leader's detector runs.
3. But if the `MarkNodeFailed` *did* commit (this node was still leader) and leadership moves
   during the probe round (`fd:615-635` — one `connect_timeout_ms` per candidate, serially),
   then `trigger_auto_failover`'s three attempts (`fd:688`) all hit `ForwardToLeader`. Each is
   `Ok(Err(e))` → the `:732` arm → warn, sleep 500 ms, re-propose **to the same node, which is
   still not the leader**. All three fail by construction.
4. `:740-744` logs `"Auto-failover failed after 3 attempts"` and returns. The FAIL flag is already
   in the replicated topology, so on every subsequent tick `reconcile_topology`'s
   `LocalVerdict::Failed if !marked_failed` guard (`fd:488`) is false and the `_ => {}` arm
   (`fd:494`) runs. Nothing re-attempts the promotion. **Consequence: the failed primary's slots
   stay owned by a node the cluster has flagged FAIL — a `CLUSTERDOWN`/black-hole for that slot
   range until an operator issues `CLUSTER FAILOVER TAKEOVER`.** Not a double failover; a stuck
   one.

This is pinned as current behaviour by `auto_failover_retries_the_failover_write_with_backoff`
(`fd:2083-2103`), which drives exactly this case — `f.raft.set_outcome(Err(forward_to_leader_err()))`
(`:2092`) — and asserts three attempts and 1 000 ms elapsed. The test documents the retry schedule;
it does not question whether re-proposing to a node that just told you it is not the leader can
ever succeed.

**Already filed.** Issue 18 §"What is wrong" lists this as the fourth of four early-return paths
("the `Failover` proposal fails `MAX_ATTEMPTS = 3` times") and states the class: "Every one of
those early returns discards the failover permanently." Its three candidate rulings (level-trigger
the failover pass / re-arm on exhaustion only / require the proposer to be caught up) are
un-adjudicated. **This proposal does not adjudicate them.** It makes the terminality decision an
inspectable, named value so that whichever ruling lands has one place to land in.

The second-order disagreement, latent: site 3 classifies *every* `ClusterResponse::Error` as
terminal (`fd:704-715`, comment: "retrying the same command cannot succeed"). That is true today
because `is_retryable()` matches only `HandoffNotReady` (`types.rs:643`), which a `Failover` can
never produce. It is true by coincidence, restated in prose 400 lines away from the function that
owns the classification. Site 1 does the opposite — it returns on a rejection (`fd:549-553`) and
the level-triggered loop then re-proposes the identical command every tick forever, which is
precisely the "retry that must fail identically" FM-CLUSTER-047 (`:741`) names as a hazard. Benign
only because `MarkNodeFailed`'s sole rejection is `NodeNotFound` (FM-CLUSTER-013, `:279`) for a
node that by construction cannot be in `get_all_nodes()` (`fd:479`). Neither argument is written
down at either site.

### 4. The model transcribes the detector, and transcribes it wrong

`model/failover/mod.rs:15-23` sets the discipline explicitly: *"the model never re-implements the
state machine … an edit to an arm changes the model with no edit here."* `:34-42` then concedes
that the detector is exempt: *"The detector is a direct transcription of the **control flow** of
`reconcile_topology` → `mark_node_failed` → `trigger_auto_failover`."*

That transcription has drifted. `replay.rs:55-66`:

```rust
fn plan_failover(&self, failed: NodeId) -> Option<ClusterCommand> {
    let view = self.view();
    if !view.nodes.get(&failed)?.is_primary() { return None; }
    let successor = view.get_replicas(failed).first()?.id;   // :60
    Some(ClusterCommand::Failover { old_primary_id: failed, new_primary_id: successor, force: true })
}
```

Line `:60` takes the **first** replica. Production takes the **scored** replica
(`fd:643-654` → `select_failover_target`), which filters priority 0 (FM-CLUSTER-057) and orders by
`(priority, lag, node_id)` (FM-CLUSTER-056/058). The model's planner would promote a candidate
production refuses. It does not change the *stranded* witness (any candidate strands equally), so
nothing is currently wrong in the checked scopes — but it is exactly the drift the "production
code" discipline exists to prevent, and it is invisible because the two live in different crates
with no shared symbol.

## Proposed change

### (b) Decomposition — interface-only

Three named steps replacing one body. The **module** stays `failure_detector.rs`; the **interface**
is a plan enum whose variants *are* the four decisions.

```rust
/// What an automatic failover for `failed` should do, decided from one
/// snapshot with no I/O. Every early return in the old procedure is a
/// variant here, so "the detector declined" is a value a test can read
/// rather than the absence of a Raft write.
pub enum AutoFailoverPlan {
    /// Not a primary in this view, or absent from it. FM-CLUSTER-101's
    /// "a rejected failover proposes nothing" starts here.
    NotAPrimary,
    /// A primary with no replicas: nothing to promote.
    NoCandidates,
    /// Every candidate is effective-priority 0 (FM-CLUSTER-057).
    AllNeverPromote,
    /// Promote `successor`, chosen by FM-CLUSTER-056/058's scoring.
    Promote { successor: NodeId, score: u64 },
}

/// Pure. `offsets` is the probe result; `priority_of` resolves the live
/// `cluster-replica-priority` for this node (FM-CLUSTER-058).
pub fn plan_auto_failover(
    view: &ClusterSnapshot,
    failed: NodeId,
    offsets: &[(NodeId, u64)],
    priority_of: &dyn Fn(&NodeInfo) -> u32,
) -> AutoFailoverPlan;
```

`trigger_auto_failover` becomes the composition, unchanged in behaviour:

```rust
async fn trigger_auto_failover(&self, failed_node_id: NodeId) {
    let view = self.cluster_state.snapshot();
    let candidates = match failover_candidates(&view, failed_node_id) { ... };   // eligibility
    let offsets = self.probe_offsets(&candidates).await;                         // I/O, unchanged
    let successor = match plan_auto_failover(&view, failed_node_id, &offsets, &|n| self.effective_priority(n)) {
        AutoFailoverPlan::Promote { successor, .. } => successor,
        declined => { log_declined(failed_node_id, declined); return; }
    };
    self.commit_failover(failed_node_id, successor).await;                       // propose + retry
}
```

Every log line, every early return, and the probe's serial ordering are preserved verbatim.
`probe_offsets` keeps the "unreachable scores 0 (worst)" rule (`fd:626-633`) that FM-CLUSTER-056
pins; `commit_failover` keeps the retry loop and the `voter_change` dispatch (`fd:722-724`)
byte-for-byte until part (a) lands.

**Crate placement.** `plan_auto_failover` should live in **`frogdb-cluster`**, not
`frogdb-cluster-runtime`. The dependency runs one way — `cluster-runtime` reaches `frogdb-cluster`
through `frogdb_core::cluster` (`core/src/lib.rs:9-10`, `fd:30-33`) — so a planner in
`cluster-runtime` is unreachable from `model/failover/replay.rs`, and the transcription of §4 stays
a transcription. Both crates carry the same 0.80 gate and both are in the failure-mode lint's crate
list (`scripts/failure-modes.py:64-77`), so the move is affordable. It carries a real cost, stated
plainly: **`cargo mutants -p <crate>` runs only that package's own tests**, so the forcing tests for
`plan_auto_failover`, `compute_replica_score` and `select_failover_target` must move to
`frogdb-cluster` with the code, or those rows stop contributing to *either* crate's score. That is a
mechanical move of `fd:1314-1481` (nine tests, no fixture — they take `&[NodeInfo]` and a closure),
plus retagging FM-CLUSTER-056/057/058's `Forced by` entries, which the lint verifies. **Alternative
if that cost is judged too high:** keep the planner in `cluster-runtime` and leave the model's
transcription in place, accepting §4. The decomposition's other benefits survive; only the
model-fidelity gain is lost. Recommend the move.

### (a) Propose-retry unification — SPEC-FIRST

One **adapter** naming the three axes the six policies disagree on:

```rust
/// How a proposer answers "it did not land". The three axes the ten propose
/// sites currently answer six different ways, written once.
pub struct ProposeRetry {
    pub max_attempts: u32,
    /// `None` at the last attempt. `voter_retry_delay` is this, generalised.
    pub backoff: fn(attempt: u32, max_attempts: u32) -> Option<Duration>,
}

/// Why an attempt did not land, classified once.
pub enum ProposeOutcome {
    Landed(ClusterResponse),
    /// The state machine refused. Retryable iff `ClusterError::is_retryable`.
    Refused(ClusterError),
    /// This node is not the leader. Retrying *here* cannot help; forwarding can.
    NotLeader(LeaderRedirect),
    /// Transport/timeout. Retrying here is exactly what helps.
    Transient(String),
}
```

Sites 3, 4 and 5 become `ProposeRetry { max_attempts, backoff }` values plus their existing
idempotence prechecks. `voter_retry_delay` (`net:669-671`) is already this function with the
signature spelled out — it is the precedent, and it is already forced by name
(`the_voter_retry_schedule_backs_off_and_then_stops`, spec `:783`).

The **behaviour change**, and the whole reason this half is spec-first: once `NotLeader` is a
distinguishable outcome, the auto-failover site can stop burning attempts on it. What it should do
instead is issue 18's ruling to make. The spec row must state the chosen answer; the failing test
comes before the fix.

### Why this is depth, not a wrapper

**Deletion test, part (b).** Delete `plan_auto_failover` and the four-way eligibility decision must
be re-derived inline from a `ClusterSnapshot`, with each early return observable only as the
absence of a Raft write, and the model must keep its own second copy. That is the code today, and
§4 is the drift it has already produced. Passes.

**Deletion test, part (a).** Delete `ProposeRetry` and each site re-spells attempts, backoff and
terminality by hand. That is the code today, and the census is the drift it has already produced.
Passes.

**Leverage:** part (b) — one production caller plus the model plus every future promotion policy
(issue 18's ruling 1 adds a *second* production caller, a level-triggered sweep, which needs
exactly this planner). Part (a) — three loops today, ten propose sites as the vocabulary spreads.
**Locality:** the terminality decision, and the mutants that model it, concentrate in one classifier
instead of being spread across two crates and three files.

## Testability improvement

| what | today | after |
|---|---|---|
| "a failed replica is not a failover" | `auto_failover_ignores_a_failed_replica` `fd:2058-2079` — full fixture, duplex probe server, asserts `writes().is_empty()` | `assert!(matches!(plan_auto_failover(&view, 3, &[], &p), NotAPrimary))` — no fixture, no runtime |
| "all-priority-0 declines" | `test_select_failover_target_all_never_promote` `fd:1460` covers the *selector*; the procedure's `:647-653` arm has no direct test | `AllNeverPromote` variant, directly asserted |
| "no replicas declines" | untested at the procedure level | `NoCandidates` variant, directly asserted |
| retry exhaustion is permanent | `auto_failover_retries_the_failover_write_with_backoff` `fd:2083` asserts *3 attempts / 1 000 ms*, not the consequence | `ProposeRetry` schedule checkable without a Raft, exactly as `voter_retry_delay` already is (`net:663-671`, "checkable without a live Raft and without sleeping") |
| model fidelity | `replay.rs:55-66` re-implements the planner and has drifted (`:60`) | `replay.rs` calls `plan_auto_failover`; a production edit changes the model with no edit there |

Mutation effects, stated honestly for a 0.80-gated pair of crates:

* Part (b) is a **pure move plus a wrap**: the mutant population moves between the two named
  functions but does not shrink much (the eligibility `match` arms become enum constructions,
  which cargo-mutants still mutates). Score movement is expected to be *upward* because four
  previously-unforced early returns gain direct killers.
* Part (b) with the crate move **shifts mutants across the gate boundary**: `frogdb-cluster` gains
  the planner and its tests, `frogdb-cluster-runtime` loses both. Run the **full**
  `just mutants <crate>` + `just mutants-gate <crate> 0.80` on *both* crates, not `mutants-diff` —
  a ratio whose numerator and denominator both move is not readable from a diff run.
* Part (a) removes duplicated loop bodies, so `frogdb-cluster`'s population shrinks. Same rule:
  full run, both crates.

## Spec impact

Rows read verbatim and classified. **The candidate brief's FM-CLUSTER-009/010/011 are not among
them** — those are the version-gate (`:217`) and epoch (`:231`, `:248`) rows.

| row | line | governs | part (b) — no behaviour change | part (a) — spec-first |
|---|---:|---|---|---|
| **FM-CLUSTER-101** | `:1454` | the voter-set effect of a committed command | **Invariant prose edit (lint-invisible).** The Invariant names *"`trigger_auto_failover` in `cluster-runtime/src/failure_detector.rs`, which reaches Raft through the `DetectorRaft` seam"* as one of three commit sites. After (b) the site is `commit_failover`. Citation-only; **flag for human review** — the lint cannot see it | further prose edit if `NotLeader` changes when the voter change fires. `Forced by` names `auto_failover_removes_the_failed_primary_from_the_voter_set` `fd:1947` and `a_rejected_auto_failover_leaves_the_voter_set_alone` `fd:1970`, both of which call `trigger_auto_failover` directly and must keep compiling |
| FM-CLUSTER-056 | `:849` | the score formula | **`Forced by` edit (lint-visible) only if the tests move crate.** Names `test_compute_replica_score_*` + `auto_failover_promotes_the_replica_with_the_freshest_offset` `fd:1913` | — |
| FM-CLUSTER-057 | `:861` | priority 0 never promoted; *"selection returns `None` and the failover is abandoned with a warning"* | same; the "abandoned" wording stays true — `AllNeverPromote` logs the same warning | if issue 18's ruling 1 lands, "abandoned" becomes "abandoned this pass" — **prose edit, flag for human** |
| FM-CLUSTER-058 | `:873` | deterministic + live-tunable selection | same | — |
| FM-CLUSTER-047 | `:733` | *"a committed proposal may still carry a state-machine rejection"*; NOT-observable: a rejection reported as transport, which *"would invite a retry that must fail identically"* | untouched — this is the row `ProposeOutcome::Refused` encodes | **the row to extend.** Its Invariant is stated only about `ClusterWriter`; part (a) makes the classification universal. Additive Invariant prose |
| FM-CLUSTER-048 | `:745` | forward-to-leader is not a failure | untouched | **the row the LIVE defect contradicts** — its guarantee holds for `ClusterWriter` callers and not for the detector. Any fix belongs here or in a new row |
| FM-CLUSTER-050 | `:769` | non-`ForwardToLeader` Raft errors reported as themselves | untouched | Invariant already says the match is *"on the specific `ClientWriteError` variant … rather than being classified by string matching"* — `ProposeOutcome` is that rule generalised, so this row is the precedent, not an obstacle |
| FM-CLUSTER-013 / -014 | `:274` / `:286` | `MarkNodeFailed`/`MarkNodeRecovered` semantics | untouched (sites 1-2 not restructured by (b)) | touched only if site 1's classification changes |
| FM-CLUSTER-039 / -040 / -042 | `:634` / `:646` / `:672` | the `Failover` state-machine arm | untouched — this proposal never edits `commands.rs` | untouched |
| FM-CLUSTER-053 | `:813` | *"the verdict is level-triggered and a latch never decays"*; Invariant: the transition-triggered version *"drops that write and never retries it"* | untouched | **the row whose argument the LIVE defect re-opens one level up.** The detector's *verdict* is level-triggered; the *failover* hanging off it is not. Whatever row issue 18 produces should cross-reference this one |

**A new row will be needed for issue 18's ruling.** Requirement, stated so the implementer does not
trip on it: it must name **in-crate** forcing tests. If the ruling is "level-trigger the failover
pass" the fix lives in `reconcile_topology` (`frogdb-cluster-runtime`) and the forcing tests must be
that crate's own `#[cfg(test)]` tests — a witness driven only from `frogdb-server`'s integration
suite or from `frogdb-cluster`'s model contributes **nothing** to `frogdb-cluster-runtime`'s 0.80
score. The existing detector fixture (`fd:1611-1641`) already supports it.

**Two characterization tests flip when issue 18 closes** (issue 18's own acceptance criteria):
`model::failover::tests::a_slot_strands_on_a_primary_the_cluster_has_failed` and
`model::failover::replay::a_missed_failover_leaves_the_slot_on_a_failed_primary` (`replay.rs:96`),
plus the `sometimes` property `a_slot_strands_on_a_failed_primary` (`mod.rs:70-73`) → `always`.
Part (b) alone flips none of them.

## Risks / scope boundaries vs siblings

**vs proposal 57 (RaftNetwork send error mapping) — same file, disjoint halves, explicit edge.**
57 owns `cluster/src/network.rs:461-657`: `send_rpc_pooled` `:461`, `send_rpc_oneshot` `:507`,
`try_send_on_framed` `:528`, and the `impl RaftNetwork for ClusterNetwork` block `:573-657` with
its three copies of the `RPCError::Network(NetworkError::new(&Unreachable::new(…)))` mapping.
58 owns `:659-926` — everything below the `// Server-side helpers (used by cluster_bus)` banner
(`:659-661`): `voter_retry_delay`, `MAX_ATTEMPTS`, `voter_change`, `spawn_voter_change`,
`spawn_add_raft_voter`, `plan_voter_removal`, `spawn_remove_raft_voter`. **No shared symbol.** The
banner at `:659` is the boundary; neither proposal crosses it. Ordering: independent, but both land
in `frogdb-cluster`, so (i) whichever lands second rebases line citations in this file, and (ii)
run the 0.80 gate **once, after both**, not once each. If 57 grows to touch `ClusterError` →
`RPCError` classification it becomes an *input* to 58's `ProposeOutcome::Transient` arm — in that
case land 57 first.

**vs proposals 59-62 (not on disk).** 58 claims, precisely: `failure_detector.rs:594-745` plus the
`plan_auto_failover`/`compute_replica_score`/`select_failover_target` group (`:773-823`) and its
tests (`:1314-1481`, `:1911-2103`); `network.rs:659-926`; the `ProposeRetry`/`ProposeOutcome`
vocabulary wherever it is sited. 58 does **not** claim: `HealthTable` (`:225-380`),
`FailureDetectorConfig` + clamping (`:78-220`, FM-CLUSTER-102), `reconcile_topology`/`spawn_reconcile`
(`:474-521`), `spawn_failure_detector_task` (`:849-902`), `writer.rs`, `commands.rs`, `state.rs`, or
`cluster/src/model/**` beyond the two cited comment/transcription sites.

**vs issue 18 (open, `needs-triage`) — must not pre-empt.** Part (b) is deliberately ruling-neutral:
it makes all four decline paths named values without deciding whether any of them re-arms. Land (b)
first *whatever* the ruling turns out to be — ruling 1 (level-trigger the sweep) needs
`plan_auto_failover` as a second caller, ruling 2 (re-arm on exhaustion) needs `ProposeRetry`'s
exhaustion outcome, ruling 3 (wait for own applied index) needs the eligibility step separated from
the probe. All three are cheaper after (b).

**vs issue 19 (`a_promotion_moves_nothing`, `mod.rs:74-82`) — adjacent, not touched.** That exposure
is in the `force: true` waiver inside `commands.rs`, not in the detector. `plan_auto_failover`
reproduces the current selection exactly and does not add the successor-health gate the model
records as having been tried and rejected (`mod.rs:60-63`: three fencing attempts "each moved the
counterexample rather than removing it").

**vs issue 20 (open, `needs-triage`) — direct conflict edge.** Issue 20 cites
`failure_detector.rs:681` (the `Failover { force: true }` construction, inside part (b)'s
`commit_failover`) and `network.rs:719-725` (`voter_change`'s `Failover` arm, inside 58's claimed
region). Its subject is what the *evicted* node does afterwards — a channel this proposal neither
opens nor closes. **Do not "fix" the eviction while restructuring**: FM-CLUSTER-040/-041 and
FM-CLUSTER-101 all pin the current remove-vs-demote split deliberately. Preserve `voter_change`'s
`Failover` arm byte-for-byte; if issue 20's ruling lands first, 58 rebases onto it.

**Landed decisions this proposal must not contradict.** The testing-gap audit's *dead auto-failover
(edge-triggered detector)* finding is fixed and is now FM-CLUSTER-053's Invariant plus the module
header (`fd:13-22`): reconciliation is level-triggered *by design*, and reverting to a
transition-triggered write is prohibited. Part (b) does not touch `reconcile_topology`. The
`replication-cluster-rework` rulings that touch this area (05 CLUSTER admin-gating, 12 replica-feed
policy) are in the command and barrier paths, not the detector.

**Residual risk.** Part (b): low-to-moderate — mechanical, but it is a 152-line body in a
0.80-gated crate and the crate move touches three spec rows' `Forced by` lists. Part (a): moderate
and gated on a ruling that does not exist yet; do not start it as a refactor.

## Effort

**M**, split so the halves land separately.

* **(b) decomposition — S/M.** One plan enum, one pure planner, two private helpers, one caller
  rewritten, ~9 tests moved crate with retagging, three `Forced by` cells updated, one Invariant
  citation updated (FM-CLUSTER-101). No behaviour change, no wire change, no config change. Full
  mutants + gate on both crates.
* **(a) unification — M, blocked.** Needs issue 18 triaged and a failure-mode row written first.
  Sequence: ruling → FM row → failing test in `frogdb-cluster-runtime` → `ProposeRetry`/
  `ProposeOutcome` → migrate sites 3/4/5 → flip the two characterization tests and the `sometimes`
  property → full gate on both crates.

## Independently-landable hotfixes

Both are documentation/observability only: zero behaviour change, zero spec edit, no mutant impact,
landable before either half.

### Hotfix 1 — the failover model states something about production that is not true

`cluster/src/model/failover/mod.rs:47-52` reads:

> Detectors are *not* gated on being the real leader. `reconcile_topology` is called behind
> `if detector.is_leader()`, and `is_leader()` reads openraft's `server_state()`, which a deposed
> leader keeps answering `Leader` to for as long as it takes to hear otherwise — **while its
> `client_write` still commits, by forwarding.**

The bolded clause is false for this path. The detector's `client_write` is
`RaftProposer for Arc<ClusterRaft>` (`writer.rs:114-123`), which calls `openraft::Raft::client_write`
directly and returns `ForwardToLeader` as an error. Forwarding lives in `ClusterWriter::propose`
(`writer.rs:186-199`), which the detector does not use (`DetectorRaft: RaftProposer`, `fd:47`). The
crate's own test helper proves it: `auto_failover_retries_the_failover_write_with_backoff`
(`fd:2083-2103`) feeds `forward_to_leader_err()` and observes three failed attempts and no forward.

Why it matters beyond tidiness: the sentence is the *justification* for letting two would-be leaders
both append to the model's log. That over-approximation is sound for the `always` safety properties
and worth keeping — but it is an over-approximation, not a description, and the real production
behaviour is worse in the liveness direction (the proposal is silently lost, which is §3 above and
issue 18's fourth path). Stating it as fact invites a future editor to "fix" the model toward a
fiction, or to conclude the detector is covered against deposition when it is not.

**Exact edit** — replace the trailing clause, keep the paragraph:

> … keeps answering `Leader` to for as long as it takes to hear otherwise. `scope.detectors` is
> exactly the set of nodes in that window at once, which is the "two would-be leaders" the model
> exists to permute. Their proposals are modelled as *committing*, which is a deliberate
> over-approximation: in production the detector proposes through the raw `RaftProposer`
> (`DetectorRaft: RaftProposer`, `cluster-runtime/src/failure_detector.rs:47`) with no forwarder
> behind it, so a deposed leader's `client_write` returns `ForwardToLeader` and the write is *lost*
> rather than committed elsewhere — which is issue 18's fourth path, not a second proposer. The
> over-approximation is kept because it is conservative for the safety properties and because it is
> what puts two appenders on the board.

Comment-only, `frogdb-cluster`, no test touched, `lint-failure-modes` unaffected.

### Hotfix 2 — the outage that produces no greppable statement of itself

`fd:740-744` logs, at `error`:

```
"Auto-failover failed after {MAX_ATTEMPTS} attempts"
```

with `failed_primary` and `new_primary` fields. It does not say that the failover has been
abandoned permanently and that a slot range is now stranded on a FAIL-flagged node — which is the
actual operational state, and the one thing an operator needs to know to reach for
`CLUSTER FAILOVER TAKEOVER`. Every sibling terminal log in this family does state its consequence:
`net:821-822` ("node is in cluster state but NOT a Raft voter"), `net:918-919` ("node is gone from
cluster state but is STILL a Raft voter").

**Exact edit**, matching the siblings' shape:

```rust
tracing::error!(
    failed_primary = failed_node_id,
    new_primary = new_primary.id,
    "Auto-failover failed after {MAX_ATTEMPTS} attempts; the failover is abandoned and will \
     not be retried — slots stay on the FAIL-flagged primary until an operator issues \
     CLUSTER FAILOVER TAKEOVER (see .scratch/cluster-correctness/issues/open/18)"
);
```

One log string in `frogdb-cluster-runtime`. No metric is added: the metrics-emission seam gate
(`just lint-gates`) governs that surface, and a counter here is a real observability change that
belongs with issue 18's ruling, not ahead of it.
