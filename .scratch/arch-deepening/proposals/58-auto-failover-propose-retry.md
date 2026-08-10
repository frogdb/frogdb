# Proposal 58 — `trigger_auto_failover` decomposition, and one propose-retry policy

Round 38 · lane: replication+cluster · effort **M** · LOCKED area (cluster, mutation gate 0.80) ·
**SPEC-FIRST** for part (a)

## Summary

`FailureDetector::trigger_auto_failover` (`cluster-runtime/src/failure_detector.rs:594-745`) is a
152-line `async` **implementation** with no **interface** in front of it: it reads a snapshot,
decides whether a failover is warranted, opens sockets to probe every candidate, scores them,
proposes through Raft, runs its own retry loop, and dispatches the voter-set side effect — all in
one body, reachable only by `.await`. Around it, the tree contains **twelve live sites that propose
a `ClusterCommand` and decide what to do when the proposal does not land, under eight mutually
inconsistent terminality policies** — plus a thirteenth, dead site that proposes with no policy at
all (census below). Two of the eight live in the same 350-line region of `cluster/src/network.rs`
as the third.

This proposal is two separable pieces:

* **(b) Decomposition — interface-only, no behaviour change, no spec row.** Split the procedure
  into a *pure eligibility step*, an *I/O probe step*, a *pure selection step*, and a *commit
  step*, so each of the four pre-proposal decisions is a value a test can read rather than the
  absence of a Raft write.
* **(a) Propose-retry unification — SPEC-FIRST.** One `ProposeRetry` policy **adapter** that
  names *attempts*, *backoff*, and *terminality*, replacing three hand-rolled loops and making
  the auto-failover site's policy inspectable. This one changes behaviour if the auto-failover
  site's `ForwardToLeader` handling is corrected, so it goes failure-mode row → failing test →
  fix, not code-first.

**A LIVE defect is present and it is already filed.** The auto-failover proposal path is the one
*live* propose site that neither forwards to the leader nor is re-driven by a level-triggered loop,
so a deposed-but-still-`Leader`-believing detector burns all three attempts on `ForwardToLeader` —
none of which can commit unless this node is re-elected inside the ~1 s the retry window spans —
and then discards the failover permanently. That is exposure #4 of
[`.scratch/cluster-correctness/issues/open/18-a-missed-failover-is-never-retried.md`](../../cluster-correctness/issues/open/18-a-missed-failover-is-never-retried.md)
(`Status: needs-triage`, unruled). It is **not** offered as a hotfix here: the current behaviour is
pinned by a test (`auto_failover_retries_the_failover_write_with_backoff`,
`failure_detector.rs:2083-2103`), issue 18 lists three candidate rulings that are not this
proposal's to make, and closing it flips two characterization tests plus a `sometimes` model
property. Four genuinely independent hotfixes — a *false* faithfulness claim in the model's header,
a silent-outage log line, a dead `ClusterMsg` variant, and a doc citation into a retired directory
— are written up at the end.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/cluster-runtime/src/failure_detector.rs` | 2381 | **the change.** `DetectorRaft` seam `:47-59`, prod impl `:61-76`; `raft_write_timeout` `:431-434`; `reconcile_topology` `:474-497`; `spawn_reconcile` `:501-521`; `mark_node_failed` `:538-567`; `mark_node_recovered` `:570-588`; **`trigger_auto_failover` `:594-745`**; `effective_priority` `:763-769`; `offset_of` `:773-779`; `compute_replica_score` `:792-797` (doc `:781-791`, **hotfix 4 at `:783`**); `select_failover_target` `:806-823`; task loop `:849-902`; pure selection tests `:1337-1466` (helpers `:1314`, `:1327`); detector-fixture tests `:1611-2103` |
| `frogdb-server/crates/cluster/src/network.rs` | 1877 | **shared file with proposal 57, different half.** `voter_retry_delay` `:669-671`, `const MAX_ATTEMPTS = 5` `:676`, `voter_change` `:709-749`, `spawn_voter_change` `:756-761`, `spawn_add_raft_voter` `:777-828`, `plan_voter_removal` `:852-863`, `spawn_remove_raft_voter` `:878-926`. Part (a) also edits two tests in place: `the_voter_retry_schedule_backs_off_and_then_stops` `:1450-1462`, `adding_a_voter_runs_for_a_stranger_and_skips_an_existing_member` `:1470-…` |
| `frogdb-server/crates/cluster/src/writer.rs` | ~500 | the existing propose **seam** the detector does *not* use, and where part (a)'s vocabulary must be sited so both crates can reach it. `Proposed` `:55-64`, `ProposeError` `:88-99`, `RaftProposer` `:105-112` + blanket impl `:114-123`, `LeaderForwarder` `:129-149`, `ClusterWriter::propose` `:182-205`, `propose_reset` `:219-…` |
| `frogdb-server/crates/cluster/src/types.rs` | — | `ClusterError::is_retryable` `:642-644` — the *typed* terminality classifier, consulted at exactly one of the thirteen sites; `ClusterResponse::as_error` `:463-468` (the typed accessor that makes `ProposeOutcome::Refused(ClusterError)` constructible today, with no wire change); `ClusterSnapshot::nodes: BTreeMap` `:804`; `get_replicas` `:882-887`; `test_missing_replica_priority_defaults_to_the_neutral_value` `:953` (an FM-CLUSTER-057 forcing test that already lives in this crate) |
| `frogdb-server/crates/server/src/connection/cluster.rs` | — | **census sites 11 and 12** — `handle_raft_command`'s leader-local admin arm `:83-126` (the first of FM-CLUSTER-101's three commit sites, `:83` + `:105-107`), `handle_reset_command` `:161-186` (propose `:175`, arms `:176-185`). Cited as evidence; **not touched** |
| `frogdb-server/crates/core/src/shard/dispatch_cluster.rs` | 36 | **census site 13, DEAD.** `ClusterMsg::RaftCommand` arm `:12-21` — a raw `raft.client_write(cmd)` with no writer, no forward, no retry. Nothing constructs the message. **Hotfix 3 deletes it** |
| `frogdb-server/crates/core/src/shard/message.rs` | — | `ClusterMsg::RaftCommand` definition `:785-790` with its false doc `:781-784`; `probe_type_str` arm `:1154`. Hotfix 3 |
| `frogdb-server/crates/cluster/src/model/failover/mod.rs` | — | model header `:15-23` (the "transition function is production code" discipline, which is about `apply_command`), `:34-45` (the detector control-flow transcription), **`:47-52` (the paragraph carrying the false clause at `:50-51`, hotfix 1)**, `:54-82` (the two pinned exposures; the `a_slot_strands_on_a_failed_primary` bullet is `:68-73`), `Det` `:222-234`, `Action::Select`/`Abandon` enumeration `:575-588`, `sometimes` property definition `:752`, exploration-budget table `:100-110` (`smoke_scope` row `:105`) |
| `frogdb-server/crates/cluster/src/model/failover/replay.rs` | — | `Node::plan_failover` `:51-66` — the hand-transcribed planner whose `:60` differs *textually* from production (see Problem §4); `a_missed_failover_leaves_the_slot_on_a_failed_primary` `:96-161` |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | — | census sites 6 and 7: self-registration retry `:556-601` (loop `:571-599`), bootstrap slot-assignment retry `:606-658` (loop `:623-655`) — **not touched**, cited as evidence |
| `frogdb-server/crates/server/src/slot_migration/mod.rs` | — | census site 8: `commit` `:309-337`, the only consumer of `is_retryable` `:324` — **not touched**, cited as evidence; proposal 11's file |
| `.scratch/hardening/specs/cluster-failure-modes.md` | 1500+ | FM-CLUSTER-013 `:274`, -014 `:286`, -039 `:634`, -040 `:646`, -042 `:672`, -047 `:733`, -048 `:745`, -050 `:769`, **-051 `:787`**, -053 `:813`, -056 `:849`, -057 `:861`, -058 `:873`, **-101 `:1454`** |
| `.cargo/mutants.toml`, `Justfile` | — | `timeout_multiplier = 4.0`, `minimum_test_timeout = 60.0` (the effort argument); `lint-metrics-chokepoint` `Justfile:1198` (hotfix 2's metrics note); `scripts/failure-modes.py:64-77` (both crates are in the lint's crate list) |
| `.scratch/cluster-correctness/issues/open/18-…md` | 105 | the LIVE defect, filed, `needs-triage`. Cites `MAX_ATTEMPTS = 3` at `:29` and the "`MAX_ATTEMPTS`-exhausted path" at `:83` |
| `.scratch/cluster-correctness/issues/open/20-…md` | — | adjacent open issue on the same `force: true` → `VoterChange::Remove` edge; cites `failure_detector.rs:681` verbatim at `:18`. Conflict edge below |

Verified against the current worktree (on top of `main` `08c143d6`); every citation was read, not
inferred. **The candidate brief's citations were stale on both counts.** Its line ranges
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

Three of the four early returns (`:600`, `:611`, `:652`) and the retry-exhaustion fall-through
(`:740-744` logs and the body ends at `:745`; there is no `return`) are the *decisions* this
function exists to make, and none of them is observable except as "the fake Raft recorded no
write". The current tests pay for that: `build()` (`:1611-1641`), `network_reporting` (`:1656`),
`probe_factory` (`:1666`), `serve_health_probes` (`:1681`) and `settle`/`eventually`
(`:1704-1720`) are ~110 lines of fixture standing between a test and a four-way branch.
`auto_failover_ignores_a_failed_replica` (`:2058-2079`) spins up a duplex-stream probe server to
assert `f.raft.writes().is_empty()` — an assertion about a pure snapshot read.

The score/select step is the counter-example that proves the point: it was already pulled out as
free functions with an injected `priority_of` closure (`:806-823`), and it is the one step covered
by **eight** cheap, direct unit tests (`:1337-1466`, over the helpers at `:1314`/`:1327`) and three
spec rows (FM-CLUSTER-056/057/058).

### 2. Twelve live propose sites, eight terminality policies (plus one dead site)

Verified census of every site that submits a `ClusterCommand` and decides what "it did not land"
means. The **policy** column is the grouping key `(attempts + backoff, ForwardToLeader treatment,
state-machine-`Error` treatment, re-driven?)`; sites sharing a letter answer all four the same way.

| # | site | attempts | backoff | `ForwardToLeader` | state-machine `Error` | re-driven later? | policy |
|---|---|---:|---|---|---|---|---|
| 1 | `mark_node_failed` `fd:538-567` | 1 | — | plain error `:563` | warn + return `:549-553` | ✅ level-triggered `:486-496` | **P1** |
| 2 | `mark_node_recovered` `fd:570-588` | 1 | — | plain error `:584` | not distinguished `:581` | ✅ level-triggered | **P1** |
| 3 | **`trigger_auto_failover` `fd:687-739`** | **3** | **flat 500 ms `:737`** | **plain error, retried against the same non-leader `:732`** | **terminal `:704-715`** | ❌ **never** | **P2** |
| 4 | `spawn_add_raft_voter` `net:777-828` | 5 | linear `500·attempt` `:669-671` | n/a (membership API) | n/a | ❌ (idempotence precheck `:784-789`) | **P3** |
| 5 | `spawn_remove_raft_voter` `net:878-926` | 5 | linear `:909` | n/a | n/a | ❌ (classify precheck `:884-887`) | **P3** |
| 6 | self-registration `init:571-599` | 30 | flat 500 ms `:589` | **forwarded** by `ClusterWriter` | ignored with the redirect `:587` | ❌ | **P4** |
| 7 | bootstrap slot assign `init:623-655` | 30 | flat 500 ms `:645` | **forwarded** | ignored `:643` | ❌ | **P4** |
| 8 | `SlotMigrationCoordinator::commit` `sm:309-337` | 0 | — | **forwarded** | `is_retryable()` → `TRYAGAIN` else `ERR` `:324` | client's job | **P5** |
| 9 | `ClusterWriter::propose` `wr:182-205` | 0 | — | **forwarded once**, else `Redirect` | returned as `Committed(Error)` | caller's job | **P6** |
| 10 | `ForwardedWrite` receiver `net:947-972` | 0 | — | n/a (this *is* the leader) | flattened to a string `:958-963` | remote caller's job | **P7** |
| 11 | `handle_raft_command` leader-local arm `conn:83-126` | 0 | — | **forwarded** | `ERR {msg}` unconditionally `:94` | client's job | **P8** |
| 12 | `handle_reset_command` `conn:175-185` | 0 | — | **forwarded** | `ERR {e}` `:182` | client's job | **P8** |
| — | ~~`ClusterMsg::RaftCommand` arm `dispatch_cluster.rs:12-21`~~ | 1 | — | **flattened to `String`** `:17` | flattened to `String` | ❌ | **DEAD** — nothing constructs the message (hotfix 3) |

Eight distinct policies for one question, across twelve live sites. Two constants named
`MAX_ATTEMPTS` hold different values in the two crates (`fd:687` = 3, `net:676` = 5). Three
different backoff shapes. The one *typed* answer to "is this rejection retryable" —
`ClusterError::is_retryable` (`types.rs:642-644`) — is consulted at exactly one of the twelve.

Two sites the earlier draft of this census missed are worth calling out on their own:

* **Sites 11/12 are FM-CLUSTER-101's first commit site.** `handle_raft_command` derives
  `voter_change(&cmd)` *before* proposing (`conn:83`) and dispatches it only on
  `Proposed::Committed` (`:105-107`), with a deliberate side-effect fork against
  `Proposed::Forwarded` (`:111-123`, transport bookkeeping only). Any terminality vocabulary that
  cannot express "committed, and I am the node that owes the side effect" is not a vocabulary this
  tree can adopt. The FM-CLUSTER-101 Invariant names this arm first, `network.rs`'s
  `ForwardedWrite` receiver second, and `trigger_auto_failover` third (spec `:1461`).
* **Site 13 is a raw `client_write` with no policy at all.** `dispatch_cluster.rs:12-21` calls
  `raft.client_write(cmd)` directly, flattens *every* error — including `ForwardToLeader` — into a
  `String` over a `oneshot`, and is neither forwarded nor re-driven. It is unreachable today (see
  hotfix 3), which is the only reason §3's headline is true as written; a single future
  constructor would make it a second stuck-not-split path with no test and no spec row.

### 3. The disagreement that is LIVE

Sites 6-9 and 11-12 route through `ClusterWriter::propose`, whose contract (FM-CLUSTER-048, spec
`:745`) is that `ForwardToLeader` is **not a failure**: the write is forwarded over the cluster bus
and lands on the leader. Site 3 does not use `ClusterWriter` at all. Its `DetectorRaft` seam is
declared as `pub trait DetectorRaft: RaftProposer + 'static` (`fd:47`), so `client_write` is the
*raw* proposer (`writer.rs:114-123` → `openraft::Raft::client_write`), with no forwarder behind it.

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
   still not the leader**. None of the three can commit unless this node is re-elected inside the
   ~1 s the three attempts and two 500 ms sleeps span — which is possible but is not a policy, and
   is not what the retry was written for.
4. `:740-744` logs `"Auto-failover failed after 3 attempts"` and the body ends at `:745`. The FAIL
   flag is already in the replicated topology, so on every subsequent tick `reconcile_topology`'s
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
("the `Failover` proposal fails `MAX_ATTEMPTS = 3` times", `:29`) and states the class: "Every one
of those early returns discards the failover permanently." Its three candidate rulings
(level-trigger the failover pass / re-arm on exhaustion only / require the proposer to be caught
up) are un-adjudicated. **This proposal does not adjudicate them.** It makes the terminality
decision an inspectable, named value so that whichever ruling lands has one place to land in.

Two second-order disagreements, both latent, both benign only by coincidence:

* Site 3 classifies *every* `ClusterResponse::Error` as terminal (`fd:704-715`, comment: "retrying
  the same command cannot succeed"). That is true today because `is_retryable()` matches only
  `HandoffNotReady` (`types.rs:643`), which a `Failover` can never produce. It is true by
  coincidence, restated in prose 400 lines away from the function that owns the classification.
  Site 1 does the opposite — it returns on a rejection (`fd:549-553`) and the level-triggered loop
  then re-proposes the identical command every tick forever. That is adjacent to the hazard
  FM-CLUSTER-047 names at spec `:739`: the row's NOT-observable is a rejection *misreported as
  transport*, "which would invite a retry that must fail identically". Site 1 does not misreport
  anything; it just retries a rejection anyway, which is the same waste arrived at by a different
  route. Benign only because `MarkNodeFailed`'s sole rejection is `NodeNotFound` (FM-CLUSTER-013,
  spec `:279`) for a node that by construction cannot be in `get_all_nodes()` (`fd:479`).
* Sites 8 and 11 render the *same* `ClusterError` two different ways: site 8 maps `is_retryable()`
  to a `TRYAGAIN` prefix (`sm:324`), site 11 always says `ERR` (`conn:94`). Unobservable today only
  because `raft_op_to_command` (`connection/util.rs:185-244`) emits no handoff command, so no site-11
  rejection can be `HandoffNotReady`. Nobody wrote that down either.

Neither argument is recorded at any of the sites.

### 4. The model's transcription of the planner has drifted textually — and only textually

`model/failover/mod.rs:15-23` sets the discipline explicitly: *"the model never re-implements the
state machine … an edit to an arm changes the model with no edit here."* Read precisely, that
discipline is about **`apply_command`**, and it is honoured everywhere it claims to apply. `:34-45`
then states that the *detector* is a transcription of control flow, not of code.

Where does the transcription actually live, and does it drift?

* **The `stateright` model proper does not transcribe the selection at all.** `mod.rs:575-588`
  enumerates `Action::Select(d, r)` for **every** replica of the failed node, plus
  `Action::Abandon(d)`. That is a deliberate over-approximation: every candidate production could
  pick, and several it never would. Adopting `plan_auto_failover` there would *shrink* the explored
  state space, which is the wrong direction for a model whose job is to permute what the callers
  can do. **The model proper should keep enumerating.**
* **The drift is confined to `replay.rs:60`**, in the deterministic replay:

  ```rust
  fn plan_failover(&self, failed: NodeId) -> Option<ClusterCommand> {   // :55
      let view = self.view();
      if !view.nodes.get(&failed)?.is_primary() { return None; }
      let successor = view.get_replicas(failed).first()?.id;            // :60
      Some(ClusterCommand::Failover { old_primary_id: failed, new_primary_id: successor, force: true })
  }
  ```

  Line `:60` takes the **first** replica; production takes the **scored** replica (`fd:643-654` →
  `select_failover_target`), which filters priority 0 (FM-CLUSTER-057) and orders by `(priority,
  lag, node_id)` (FM-CLUSTER-056/058).
* **And even there the two agree, structurally, for this replay's configuration.**
  `ClusterSnapshot::nodes` is a `BTreeMap<NodeId, NodeInfo>` (`types.rs:804`) and `get_replicas`
  iterates `self.nodes.values()` (`types.rs:882-887`), so `.first()` is the **lowest node id** —
  which is exactly production's tiebreak (`fd:821`, `score_a.cmp(&score_b).then_with(|| a.id.cmp(&b.id))`)
  whenever priorities and offsets are uniform. In `stranded_scope` they are: the replay builds its
  nodes from one base snapshot and never probes an offset. So the drift is a *textual* one that
  today produces the identical answer.

The honest statement is therefore narrower than "the model transcribes the detector and transcribes
it wrong": one deterministic replay re-spells a decision production owns, in a form that happens to
coincide, with nothing in the tree recording *why* it coincides. That is worth fixing — but it is a
comment-and-equivalence problem, not a state-space-fidelity problem, and it cannot on its own carry
a cross-crate move. See *Crate placement* below, where the recommendation flips because of it.

## Proposed change

### (b) Decomposition — interface-only

Four named steps replacing one body. The **module** stays `failure_detector.rs`; the **interface**
is a plan enum whose variants *are* the four pre-proposal decisions. **Each decision is made
exactly once**: eligibility belongs to the pre-probe step and to nothing else, promotability
belongs to the post-probe step and to nothing else. (An earlier sketch had both functions owning
`NotAPrimary`/`NoCandidates`, which would have re-created inside one refactor the two-copy drift
§2 and §4 condemn — and would have made the headline testability assertion exercise a path
production never takes.)

```rust
/// What an automatic failover for `failed` should do, decided with no I/O.
/// Every early return the old procedure took *before the Raft proposal* is a
/// variant here, so "the detector declined" is a value a test can read rather
/// than the absence of a Raft write. The two exits *after* the proposal — the
/// state-machine rejection (`fd:704-715`) and the success path (`fd:716-730`) —
/// are `commit_failover`'s outcomes, not this enum's; naming them is part (a)'s
/// job (`ProposeOutcome`).
pub enum AutoFailoverPlan {
    /// Not a primary in this view, or absent from it. Produced only by
    /// `plan_failover_probe`.
    NotAPrimary,
    /// A primary with no replicas: nothing to promote. Produced only by
    /// `plan_failover_probe`.
    NoCandidates,
    /// Every candidate is effective-priority 0 (FM-CLUSTER-057). Produced only
    /// by `plan_auto_failover`.
    AllNeverPromote,
    /// Promote `successor`, chosen by FM-CLUSTER-056/058's scoring. Produced
    /// only by `plan_auto_failover`.
    Promote { successor: NodeId, score: u64 },
}

/// Pure. Eligibility, decided once: either these are the nodes to probe, or the
/// failover is declined before a single socket is opened.
pub fn plan_failover_probe<'a>(
    view: &'a ClusterSnapshot,
    failed: NodeId,
) -> Result<Vec<&'a NodeInfo>, AutoFailoverPlan>;

/// Pure. Promotability, decided once, over the candidates the probe returned.
/// `offsets` is the probe result; `priority_of` resolves the live
/// `cluster-replica-priority` for this node (FM-CLUSTER-058). Takes no
/// `ClusterSnapshot` — the view was already consumed by the step above.
pub fn plan_auto_failover(
    candidates: &[&NodeInfo],
    offsets: &[(NodeId, u64)],
    priority_of: &dyn Fn(&NodeInfo) -> u32,
) -> AutoFailoverPlan;
```

`trigger_auto_failover` becomes the composition, unchanged in behaviour:

```rust
async fn trigger_auto_failover(&self, failed_node_id: NodeId) {
    let view = self.cluster_state.snapshot();
    let candidates = match plan_failover_probe(&view, failed_node_id) {
        Ok(c) => c,
        Err(declined) => return log_declined(failed_node_id, declined),
    };
    let offsets = self.probe_offsets(&candidates).await;               // I/O, unchanged
    let successor = match plan_auto_failover(&candidates, &offsets, &|n| self.effective_priority(n)) {
        AutoFailoverPlan::Promote { successor, .. } => successor,
        declined => return log_declined(failed_node_id, declined),
    };
    self.commit_failover(failed_node_id, successor).await;            // propose + retry
}
```

Every log line, every early return, and the probe's serial ordering are preserved verbatim.
`probe_offsets` keeps the "unreachable scores 0 (worst)" rule (`fd:626-633`) that FM-CLUSTER-056
pins; `commit_failover` keeps the retry loop and the `voter_change` dispatch (`fd:722-724`)
byte-for-byte until part (a) lands.

**Crate placement — recommendation: keep both planners in `frogdb-cluster-runtime`.** The earlier
draft recommended moving them to `frogdb-cluster` so `model/failover/replay.rs` could consume them.
Three things kill that recommendation:

1. **The model-fidelity payoff is small and one level below where it was claimed** (§4). The model
   proper deliberately over-approximates and must keep doing so; only `replay.rs:60` re-spells the
   decision, and it re-spells it into the same answer for its own configuration.
2. **The move does not compile as a mechanical range move.** `fd:1314-1481` is not a movable block:
   * `make_node` (`fd:1314-1325`) is shared with health-table/quorum tests that **stay** (`fd:1223`,
     `:1259-1261`, `:1297`, `:1300`), so it must be **duplicated**, not moved.
   * `fd:1474-1481` is the `use` block for the detector-fixture tests that stay — the range's tail
     is not test code at all.
   * `test_replica_priority_store_changes_failover_target` (`fd:1412-1455`, FM-CLUSTER-058)
     **cannot move**: it constructs `ClusterRuntimeFlags::new(true, true, 100)` (`fd:1420`), a
     `frogdb-cluster-runtime` type, and there is no dependency edge from `frogdb-cluster` to
     `frogdb-cluster-runtime` (`cluster/Cargo.toml` has no such dep; the edge runs the other way,
     `cluster-runtime → frogdb-core → frogdb-cluster`). Rewriting it with a plain closure would
     delete exactly the live-flag coupling FM-CLUSTER-058's Observable and Invariant force
     (spec `:878`, `:880`: *"the priority read goes through `ClusterRuntimeFlags`"*).

   So the movable set is **7 tests** (`fd:1337`, `:1345`, `:1356`, `:1371`, `:1383`, `:1395`,
   `:1460`) plus the `score_of` helper (`:1327`) plus a *duplicated* `make_node` — not the nine a
   range move suggests. FM-CLUSTER-058 would end with forcing tests in **both** crates
   (`fd:1412` and `fd:2143` stay; `fd:1371` goes), and so would FM-CLUSTER-056 (`fd:1913` stays).
   FM-CLUSTER-057 would end entirely in `frogdb-cluster` (`fd:1337`, `fd:1460` go, `types.rs:953`
   is already there) — which is coherent, but it is a third distinct outcome from one move.
3. **The move re-prices part (b) from a single-crate job into a two-crate, testbox-class one** (see
   *Effort*), because `cargo mutants -p <crate>` runs only that package's own tests and a ratio
   whose numerator and denominator both move is not readable from a diff run.

**Instead, part (b) records the equivalence where the drift is.** Add to `replay.rs:55-66` the two
sentences §4 establishes: `ClusterSnapshot::nodes` is a `BTreeMap`, so `.first()` is the lowest
node id, which is production's documented tiebreak (`fd:821`) and therefore the same answer
whenever the scope leaves priorities and offsets uniform — as `stranded_scope` does. That converts
a silent coincidence into a stated, checkable one at zero gate cost. **The crate move stays on the
table as a costed follow-up**, to be taken when a *second* `frogdb-cluster` consumer appears — note
that issue 18's ruling 1 is not one: its level-triggered sweep lives in `reconcile_topology`, which
is in `frogdb-cluster-runtime` and therefore argues for keeping the planner exactly where it is.

### (a) Propose-retry unification — SPEC-FIRST

One **adapter** naming the three axes the eight policies disagree on. It must be sited in
`frogdb-cluster` (`writer.rs`, beside `Proposed`/`ProposeError`) so both crates can reach it —
`cluster-runtime` gets there through `frogdb_core::cluster` (`core/src/lib.rs:9-10`, `fd:30-33`).

```rust
/// How a proposer answers "it did not land". The three axes the twelve live
/// propose sites currently answer eight different ways, written once.
pub struct ProposeRetry {
    pub max_attempts: u32,
    /// `None` at the last attempt. `voter_retry_delay` is this, generalised.
    pub backoff: fn(attempt: u32, max_attempts: u32) -> Option<Duration>,
}

/// Why an attempt did not land, classified once.
pub enum ProposeOutcome {
    Landed(ClusterResponse),
    /// The state machine refused. Retryable iff `ClusterError::is_retryable`.
    /// Constructible today with no wire change: `ClusterResponse::as_error`
    /// (`types.rs:463-468`) already hands back the typed `&ClusterError`.
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
(`the_voter_retry_schedule_backs_off_and_then_stops`, FM-CLUSTER-051's `Forced by`, spec `:796`).

Sites 8, 11 and 12 are **not** migration targets: they run zero attempts, so they have no retry
policy to unify. What they do have is the `TRYAGAIN`-vs-`ERR` rendering split §3 records, which is
`ProposeOutcome::Refused`'s business. That is named here as a follow-up axis and **not claimed** —
it is a client-visible error string, so it needs its own ruling.

The **behaviour change**, and the whole reason this half is spec-first: once `NotLeader` is a
distinguishable outcome, the auto-failover site can stop burning attempts on it. What it should do
instead is issue 18's ruling to make. The spec row must state the chosen answer; the failing test
comes before the fix.

### Why this is depth, not a wrapper

**Deletion test, part (b).** Delete `plan_failover_probe`/`plan_auto_failover` and the four-way
decision must be re-derived inline from a `ClusterSnapshot`, with each early return observable only
as the absence of a Raft write. That is the code today. Passes.

**Deletion test, part (a).** Delete `ProposeRetry` and each site re-spells attempts, backoff and
terminality by hand. That is the code today, and the census is the drift it has already produced.
Passes.

**Leverage:** part (b) — one production caller today plus every future promotion policy (issue 18's
ruling 1 adds a *second* production caller, a level-triggered sweep in the same crate, which needs
exactly `plan_failover_probe`). Part (a) — three retry loops today, twelve propose sites as the
vocabulary spreads. **Locality:** the terminality decision, and the mutants that model it,
concentrate in one classifier instead of being spread across three crates and five files.

## Testability improvement

| what | today | after |
|---|---|---|
| "a failed replica is not a failover" | `auto_failover_ignores_a_failed_replica` `fd:2058-2079` — full fixture, duplex probe server, asserts `writes().is_empty()` | `assert!(matches!(plan_failover_probe(&view, 3), Err(NotAPrimary)))` — no fixture, no runtime, and it is the *same* function production calls first |
| "no replicas declines" | untested at the procedure level | `plan_failover_probe(&view, p)` → `Err(NoCandidates)`, directly asserted |
| "all-priority-0 declines" | `test_select_failover_target_all_never_promote` `fd:1460` covers the *selector*; the procedure's `:647-653` arm has no direct test | `plan_auto_failover(&candidates, &offsets, &p)` → `AllNeverPromote`, directly asserted |
| "the scored candidate wins" | `auto_failover_promotes_the_replica_with_the_freshest_offset` `fd:1913` — full fixture | `plan_auto_failover(…)` → `Promote { successor, score }`, with `score` assertable (it is not, today, at any level above `compute_replica_score`) |
| retry exhaustion is permanent | `auto_failover_retries_the_failover_write_with_backoff` `fd:2083` asserts *3 attempts / 1 000 ms*, not the consequence | `ProposeRetry` schedule checkable without a Raft, exactly as `voter_retry_delay` already is (`net:663-671`, "checkable without a live Raft and without sleeping") |
| replay ↔ production agreement | `replay.rs:60` re-spells the selection and nothing says why the two agree | the equivalence is stated at `replay.rs:55-66` (BTreeMap ⇒ lowest id ⇒ `fd:821`'s tiebreak, given a uniform scope), so a scope change that breaks it is a comment a reader can check |

Mutation effects, stated honestly for a 0.80-gated pair of crates:

* Part (b) is a **split plus a wrap inside one crate**: the mutant population moves between the
  named functions but does not shrink much (the eligibility `match` arms become enum
  constructions, which cargo-mutants still mutates). Score movement is expected to be *upward*
  because four previously-unforced pre-proposal decisions gain direct killers. Only
  `frogdb-cluster-runtime`'s population moves, so `just mutants frogdb-cluster-runtime` +
  `just mutants-gate frogdb-cluster-runtime 0.80` is the whole obligation.
* Part (a) removes duplicated loop bodies and adds two types to `writer.rs`, so **both** crates'
  populations move. Full `just mutants <crate>` + `just mutants-gate <crate> 0.80` on both, not
  `mutants-diff` — a ratio whose numerator and denominator both move is not readable from a diff
  run. Price that decision before starting: see *Effort*.
* **If the crate move is ever taken as the follow-up**, it shifts mutants across the gate boundary
  and inherits part (a)'s two-crate obligation. That cost is a reason to take the move only when a
  second `frogdb-cluster` consumer justifies it, not as a side effect of (b).

## Spec impact

Rows read verbatim and classified. **The candidate brief's FM-CLUSTER-009/010/011 are not among
them** — those are the version-gate (`:217`) and epoch (`:231`, `:248`) rows.

| row | line | governs | part (b) — no behaviour change | part (a) — spec-first |
|---|---:|---|---|---|
| **FM-CLUSTER-101** | `:1454` | the voter-set effect of a committed command | **Invariant prose edit (lint-invisible).** The Invariant (`:1461`) names *"`trigger_auto_failover` in `cluster-runtime/src/failure_detector.rs`, which reaches Raft through the `DetectorRaft` seam"* as the third of three commit sites. After (b) the site is `commit_failover`. Citation-only; **flag for human review** — the lint sees `Forced by`, not Invariant prose | further prose edit if `NotLeader` changes when the voter change fires. `Forced by` names `auto_failover_removes_the_failed_primary_from_the_voter_set` `fd:1947` and `a_rejected_auto_failover_leaves_the_voter_set_alone` `fd:1970`, both of which call `trigger_auto_failover` directly and must keep compiling |
| FM-CLUSTER-056 | `:849` | the score formula | untouched (no crate move ⇒ no `Forced by` edit). Its five forcing tests keep their crate | — |
| FM-CLUSTER-057 | `:861` | priority 0 never promoted; *"selection returns `None` and the failover is abandoned with a warning"* | untouched; the "abandoned" wording stays true — `AllNeverPromote` logs the same warning | if issue 18's ruling 1 lands, "abandoned" becomes "abandoned this pass" — **prose edit, flag for human** |
| FM-CLUSTER-058 | `:873` | deterministic + live-tunable selection; Invariant `:880` pins the read through `ClusterRuntimeFlags` | untouched. This is the row the crate move would have split, which is a third of the reason it is deferred | — |
| **FM-CLUSTER-051** | `:787` | the bus envelope split — and, through its `Forced by` at `:796`, the **voter retry schedule and the add-voter idempotence precheck** | untouched | **the row governing census sites 4 and 5.** `the_voter_retry_schedule_backs_off_and_then_stops` (`net:1450`) and `adding_a_voter_runs_for_a_stranger_and_skips_an_existing_member` (`net:1470`) are both named here; part (a) rewrites both against `ProposeRetry`. Whether the schedule keeps living under an envelope-shaped row, or moves to a new retry row, is a call the implementer must make **before** editing — a test that silently stops forcing this row fails `lint-failure-modes` |
| FM-CLUSTER-047 | `:733` | *"a committed proposal may still carry a state-machine rejection"*; NOT-observable (`:739`): a rejection reported as *transport*, which *"would invite a retry that must fail identically"* | untouched — this is the row `ProposeOutcome::Refused` encodes | **the row to extend.** Its Invariant is stated only about `ClusterWriter`; part (a) makes the classification universal. Additive Invariant prose |
| FM-CLUSTER-048 | `:745` | forward-to-leader is not a failure | untouched | **the row the LIVE defect contradicts** — its guarantee holds for `ClusterWriter` callers and not for the detector. Any fix belongs here or in a new row |
| FM-CLUSTER-050 | `:769` | non-`ForwardToLeader` Raft errors reported as themselves | untouched | Invariant (`:776`) already says the match is *"on the specific `ClientWriteError` variant … rather than being classified by string matching"* — `ProposeOutcome` is that rule generalised, so this row is the precedent, not an obstacle |
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
`model::failover::replay::a_missed_failover_leaves_the_slot_on_a_failed_primary`
(`replay.rs:96-161`), plus the `sometimes` property `a_slot_strands_on_a_failed_primary`
(declared `mod.rs:752`, documented `mod.rs:68-73`) → `always`. Part (b) alone flips none of them.

## Risks / scope boundaries vs siblings

**vs proposal 57 (RaftNetwork send error mapping) — same file; production halves disjoint, test
module shared.**

*Production.* 57 owns `cluster/src/network.rs:461-657`: `send_rpc_pooled` `:461`, `send_rpc_oneshot`
`:507`, `try_send_on_framed` `:528`, and the `impl RaftNetwork for ClusterNetwork` block `:573-657`
with its three copies of the `RPCError::Network(NetworkError::new(&Unreachable::new(…)))` mapping.
**58's editable production region is `:659-926`** — everything below the
`// Server-side helpers (used by cluster_bus)` banner (`:659-661`) down to the end of
`spawn_remove_raft_voter`: `voter_retry_delay`, `MAX_ATTEMPTS`, `voter_change`, `spawn_voter_change`,
`spawn_add_raft_voter`, `plan_voter_removal`, `spawn_remove_raft_voter`. **No shared symbol.** The
banner at `:659` is the boundary; neither proposal crosses it.

*Reconciliation of the one inconsistency in earlier drafts.* The `ForwardedWrite` receiver
(`:947-972`, census site 10) is **read-only evidence for 58, not an edit target** — 58's Files table
cites it and 58's boundary excludes it, and both statements now agree. It is likewise outside 57's
`:461-657`. 57's summary of 58's footprint as `:669-972` was generous; the editable region is
`:659-926`. Neither proposal edits `:947-972`; if part (a)'s `ProposeOutcome` is ever pushed through
that receiver, it is a separate, wire-visible change (the RPC contract is `Result<(), String>` by
deliberate scope-out, `net:959-963`) and needs its own ruling.

*Tests.* `network.rs` has exactly one `#[cfg(test)] mod tests` (`#[cfg(test)]` `:1024`, `mod tests`
`:1025-1877`) and both proposals land in it, so state the split by **test name**, not by line:

| proposal | tests in `network.rs`'s module |
|---|---|
| 57 | new `impl RaftNetwork` error-mapping tests (the block has none today); reads `test_network_factory_node_registration` `:1031` as a fixture precedent |
| 58 (a) | edits in place: `the_voter_retry_schedule_backs_off_and_then_stops` `:1450-1462`, `adding_a_voter_runs_for_a_stranger_and_skips_an_existing_member` `:1470-…` |

Disjoint by name. **Landing order: 57 first, then 58.** 57 only *appends* tests; 58 (a) *edits two
existing ones in place*, so a 57-after-58 order makes 57 rebase around modified hunks instead of
appending to a stable tail. Then (i) 58 rebases line citations in this file, and (ii) run the 0.80
gate **once, after both**, not once each. If 57 grows to touch `ClusterError` → `RPCError`
classification it becomes an *input* to 58's `ProposeOutcome::Transient` arm — that reinforces the
same order.

**vs proposals 59-62 (not on disk).** 58 claims, precisely: `failure_detector.rs:594-745` plus the
`compute_replica_score`/`select_failover_target` group (`:773-823`) and its tests (`:1337-1466`,
`:1911-2103`); `network.rs:659-926` and the two FM-CLUSTER-051 tests by name; the
`ProposeRetry`/`ProposeOutcome` vocabulary in `writer.rs`; and hotfixes 1-4's exact sites. 58 does
**not** claim: `HealthTable` (`:225-380`), `FailureDetectorConfig` + clamping (`:78-220`,
FM-CLUSTER-102), `reconcile_topology`/`spawn_reconcile` (`:474-521`),
`spawn_failure_detector_task` (`:849-902`), `ClusterWriter::propose`'s body, `commands.rs`,
`state.rs`, `network.rs:947-972`, `connection/cluster.rs`, or `cluster/src/model/**` beyond the
cited comment/transcription sites.

**vs issue 18 (open, `needs-triage`) — must not pre-empt.** Part (b) is deliberately ruling-neutral:
it makes all four pre-proposal decline paths named values without deciding whether any of them
re-arms. Land (b) first *whatever* the ruling turns out to be — ruling 1 (level-trigger the sweep)
needs `plan_failover_probe` as a second caller, ruling 2 (re-arm on exhaustion) needs
`ProposeRetry`'s exhaustion outcome, ruling 3 (wait for own applied index) needs the eligibility
step separated from the probe. All three are cheaper after (b).

**vs issue 19 (`a_promotion_moves_nothing`, `mod.rs:74-82`) — adjacent, not touched.** That exposure
is in the `force: true` waiver inside `commands.rs`, not in the detector. `plan_auto_failover`
reproduces the current selection exactly and does not add the successor-health gate the model
records as having been tried and rejected (`mod.rs:60-63`: three fencing attempts "each moved the
counterexample rather than removing it").

**vs issue 20 (open, `needs-triage`) — direct conflict edge.** Issue 20 cites
`failure_detector.rs:681` verbatim (`issue 20:18` — the `Failover { force: true }` construction,
which part (b) moves into `commit_failover`) and `network.rs:719-725` (`voter_change`'s `Failover`
arm, inside 58's claimed region). Its subject is what the *evicted* node does afterwards — a channel
this proposal neither opens nor closes. **Do not "fix" the eviction while restructuring**:
FM-CLUSTER-040/-041 and FM-CLUSTER-101 all pin the current remove-vs-demote split deliberately.
Preserve `voter_change`'s `Failover` arm byte-for-byte; if issue 20's ruling lands first, 58 rebases
onto it.

**Landed decisions this proposal must not contradict.** The testing-gap audit's *dead auto-failover
(edge-triggered detector)* finding is fixed and is now FM-CLUSTER-053's Invariant plus the module
header (`fd:13-22`): reconciliation is level-triggered *by design*, and reverting to a
transition-triggered write is prohibited. Part (b) does not touch `reconcile_topology`. The
`replication-cluster-rework` rulings that touch this area (05 CLUSTER admin-gating, 12 replica-feed
policy) are in the command and barrier paths, not the detector.

**Residual risk.** Part (b): low-to-moderate — mechanical, single-crate, but it is a 152-line body
in a 0.80-gated crate and it invalidates two open issues' code citations. Part (a): moderate,
two-crate, gated on a ruling that does not exist yet; do not start it as a refactor.

## Effort

**M**, split so the halves land separately. Both halves' mutation obligations are priced here
because the earlier draft under-priced them.

* **(b) decomposition — S/M, local mode.** One plan enum, two pure planners, two private helpers,
  one caller rewritten, one comment added to `replay.rs:55-66`, one Invariant citation updated
  (FM-CLUSTER-101 `:1461`). No test moves crate, no `Forced by` cell changes, no behaviour change,
  no wire change, no config change.
  Checklist the implementer must not drop:
  * **Update issues 18 and 20's code citations.** Issue 20 `:18` names `failure_detector.rs:681`,
    which becomes a line inside `commit_failover`; issue 18 `:29`/`:83` name `MAX_ATTEMPTS = 3`,
    which moves with it. Both are `needs-triage` and will be read by whoever rules on them.
  * Mutation obligation: **full `just mutants frogdb-cluster-runtime` + `just mutants-gate
    frogdb-cluster-runtime 0.80`**, one crate. That crate is 4 830 source lines and its default
    suite carries no `stateright` run, so this is a laptop-scale job. **This is only true because
    the crate move was dropped** — see the next bullet for what it would have cost.
* **The deferred crate move — M/L, testbox mode if ever taken.** It drags `frogdb-cluster` into the
  mutation obligation, and `frogdb-cluster` is 18 700 source lines whose *default* test suite runs
  the failover model's `smoke_scope` at **4.5 s per run in debug** (`model/failover/mod.rs:105`),
  re-executed once per surviving-candidate mutant under `timeout_multiplier = 4.0` and
  `minimum_test_timeout = 60.0` (`.cargo/mutants.toml`). That is a multi-hour, testbox-class run,
  and the gate must be read *after* the move, not from a diff. Do not take this on as a rider to
  (b).
* **(a) unification — M, blocked, testbox mode.** Needs issue 18 triaged and a failure-mode row
  written first. Sequence: ruling → FM row (and the FM-CLUSTER-051 `Forced by` decision) → failing
  test in `frogdb-cluster-runtime` → `ProposeRetry`/`ProposeOutcome` in `writer.rs` → migrate sites
  3/4/5 → flip the two characterization tests and the `sometimes` property → full mutants + gate on
  **both** crates, which inherits the multi-hour `frogdb-cluster` run priced above.

  **(a) is deliberately not split into "vocabulary + sites 4/5" and "site 3 behaviour".** The split
  is tempting — sites 4 and 5 have no `ForwardToLeader` axis at all, so migrating them is
  mechanical and ruling-neutral. It is rejected because **the adapter's shape is chosen by the
  ruling**: whether `ProposeOutcome` needs `NotLeader` distinct from `Transient`, and whether
  `ProposeRetry` must return an exhaustion *value* the caller can re-arm on (ruling 2) or may keep
  returning `()` (rulings 1 and 3), are both decided by issue 18. Landing sites 4/5 against a
  vocabulary that the ruling may re-cut buys line-count now and pays for a second migration later,
  in a 0.80-gated crate whose full mutation run is the expensive part. One migration, once.

## Independently-landable hotfixes

All four are documentation/observability/dead-code only: zero behaviour change, zero spec edit, no
mutant-score movement, landable before either half and in any order.

### Hotfix 1 — the failover model states something about production that is not true

`cluster/src/model/failover/mod.rs:47-52` reads:

> Detectors are *not* gated on being the real leader. `reconcile_topology` is called behind
> `if detector.is_leader()`, and `is_leader()` reads openraft's `server_state()`, which a deposed
> leader keeps answering `Leader` to for as long as it takes to hear otherwise — **while its
> `client_write` still commits, by forwarding.**

The bolded clause (`:50-51`) is false for this path. The detector's `client_write` is
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
`CLUSTER FAILOVER TAKEOVER`. Every sibling terminal log in this family does state its consequence,
and stops there: `net:821-822` ("node is in cluster state but NOT a Raft voter"), `net:918-919`
("node is gone from cluster state but is STILL a Raft voter"). Neither points at a tracker file.

**Exact edit**, matching the siblings' shape:

```rust
tracing::error!(
    failed_primary = failed_node_id,
    new_primary = new_primary.id,
    "Auto-failover failed after {MAX_ATTEMPTS} attempts; the failover is abandoned and will \
     not be retried — slots stay on the FAIL-flagged primary until an operator issues \
     CLUSTER FAILOVER TAKEOVER"
);
```

No issue path in the string: `.scratch/cluster-correctness/issues/open/18` moves to `done/` the day
it is triaged, and `done/` is pruned to cited issues, so an operator-facing log line would rot into
a dangling reference. State the consequence and stop, exactly as the siblings do.

One log string in `frogdb-cluster-runtime`. **No metric is added, and not because a lint forbids
it** — `lint-metrics-chokepoint` (`Justfile:1198`) governs *how* a metric is emitted (typed handles
from `define_metrics!`, never raw string-name recorder calls), not whether a new one may exist. The
reason is the observability contract: a stranded-failover counter is a value operators would alert
on, so its name, its cardinality and the moment it increments are part of the answer to issue 18,
and adding it ahead of the ruling would pin one of the three candidate rulings by the back door.
It belongs with the ruling. This hotfix is also test-neutral: no existing test asserts on this
log line.

### Hotfix 3 — a dead `ClusterMsg` variant carrying a false doc comment and a raw `client_write`

`ClusterMsg::RaftCommand` (`core/src/shard/message.rs:785-790`) has **no constructor anywhere in
the tree**. A repo-wide grep for the identifier returns exactly four hits, none of which builds one:

| site | what it is |
|---|---|
| `core/src/shard/message.rs:785` | the variant definition (doc `:781-784`) |
| `core/src/shard/message.rs:1154` | its `probe_type_str` arm — a USDT probe name for a probe that never fires |
| `core/src/shard/dispatch_cluster.rs:5` | a doc comment listing it |
| `core/src/shard/dispatch_cluster.rs:12-21` | the consumer arm |

Its doc (`:781-784`) says *"Used by cluster commands (CLUSTER MEET, CLUSTER FORGET, etc.) that need
to call async Raft operations from synchronous command handlers."* That is false: those commands go
through `ClusterWriter` in `connection/cluster.rs:88-126` (census sites 11/12), which is precisely
the change that orphaned this path.

The consumer is worse than dead weight — it is a *pattern*. `dispatch_cluster.rs:12-21` calls
`raft.client_write(cmd)` raw and does `.map_err(|e| e.to_string())`, flattening `ForwardToLeader`
into a `String` on a `oneshot`. It is a thirteenth propose site with no forward, no retry, no
re-drive and no classification — a ready-made second instance of §3's stuck-not-split defect, one
constructor away from being live, and today invisible to every spec row and every test.

**Edit:** delete the variant with its doc (`message.rs:781-790`), its `probe_type_str` arm
(`:1154`), the
consumer arm (`dispatch_cluster.rs:12-21`), and the stale mention in `dispatch_cluster.rs:5`. Same
class as round-10 proposal 37's deletions. Compile-checked by construction: nothing constructs it,
so nothing can fail to match it.

### Hotfix 4 — a doc citation into a directory that no longer exists

`compute_replica_score`'s doc (`fd:783`) reads:

```
/// Formula from the spec (docs/spec/CLUSTER.md lines 1733-1755):
```

`docs/` was retired in `f34d476d` (*"docs: move docs/adr -> adr/, docs/agents -> agents/, retire
root docs/"*). There is no `docs/` directory in the tree and no `CLUSTER.md` anywhere in the
repository. This is the **only** remaining `docs/spec` reference in any Rust source.

The formula's live home is FM-CLUSTER-056 (`.scratch/hardening/specs/cluster-failure-modes.md:849`),
whose Observable states it verbatim and whose Invariant already says *"`compute_replica_score` is a
pure function of three numbers, so the weighting is auditable in one place"*.

**Edit:** replace the citation with `FM-CLUSTER-056`
(`.scratch/hardening/specs/cluster-failure-modes.md`). Doc-only, same class as hotfix 1; keeps the
"single source of truth, linked not copied" rule the repo's own guidelines set.
