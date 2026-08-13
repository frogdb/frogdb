# Adversarial design review — `specs/cluster.md` + the 2026-08-13 rulings

Scope reviewed: all 103 FM rows of `specs/cluster.md` (incl. the GAPS and Redis-deviation
tables), and the `## Ruling (2026-08-13)` sections of open issues 14, 15, 16, 17, 18, 19, 20,
25 (each read whole). Issue 24 read for context on the clock seam. Read-only; no files edited,
no builds run. Two code spot-checks were made to avoid manufacturing findings:
`frogdb-server/crates/server/src/commands/cluster/admin.rs` (the `CLUSTER FAILOVER` handler) and
`frogdb-server/crates/server/src/slot_migration/mod.rs` (`cancel`).

**Verdict: 13 findings — 2 CRITICAL, 6 HIGH, 5 ADVISORY.** None of the rulings is unsound in
its own frame; the failures are at the seams *between* rulings, and between a ruling and a row
it silently invalidates.

---

## CRITICAL

### C1 — Ruling 17 removes the wall-clock bound the replica-feed hold is built on (FM-CLUSTER-097, 084, 085, 079/082)

**Where.** Issue 17 ruling: "the `barrier_ms` wall-clock admission window on Complete is
DROPPED"; "release always flows through the log"; "Wall clock is not a correctness input
anywhere in the handoff protocol." The fence now arms at `PrepareSlotHandoff` and is released
**only** by applying `Complete`/`Abort` for that `handoff_seq`.

**The contradiction.** FM-CLUSTER-097 states, as a `NOT observable`:

> Nor a feed that can wedge: the hold carries the barrier's own deadline, so a finalizer that
> dies mid-handoff cannot leave the feed held.

and its `Invariant` derives the hold from exactly the deadline the ruling deletes:

> `PauseState::feed_hold_until` derives the hold — the latest deadline across armed slot pauses
> … The gate stores an `Instant`, not a latch: `hold_deadline` answers `None` once
> `clock::now()` passes it, so normal completion, abort, and a lapsed lease all release without
> anyone clearing anything.

Remove the deadline and every one of those clauses is false. Concretely, a source that applies
`Prepare` and is then partitioned from the leader (the *same* apply-lag condition issue 17 was
filed about) holds:

1. a **node-wide** replica-feed hold — FM-CLUSTER-097 is explicit that the hold is node-wide
   and not per-shard, so *all* replication out of that node stops, not just the barriered slot;
2. an **unbounded per-session `VecDeque`** of buffered frames ("buffers frames in a per-session
   `VecDeque` while held, draining in offset order on release"), sized today only by the ≤100 ms
   window;
3. its own slot's writes, parked with no deadline.

(3) is the intended, correct fail-closed trade. (1) and (2) are not: they convert a bounded
anomaly-hiding device into an unbounded node-wide replication stall plus an OOM vector, reached
by an ordinary follower stall. FM-CLUSTER-085's title ("so a dead finalizer cannot wedge a
slot") and its lease clauses (`a_second_prepare_waits_for_the_lease_but_not_forever`,
`complete_is_refused_once_the_lease_expired`) and FM-CLUSTER-084's
`admits_complete_at = drained && !barrier_expired && !lease_expired` are all left dangling —
the ruling never says what replaces `lease_expired`. Note also that the issue text's own
justification for option 1 ("bounded by the lease") is void once the ruling drops the lease.

**Practice.** etcd/raft, CRDB and Kafka all separate the two: the *safety* fence is
log/epoch-ordered and unbounded in time (correct), while the *resource* it holds is bounded
independently — Kafka drops a slow follower out of the ISR rather than buffering for it
forever; CRDB's Raft log queues have a max size and fall back to a snapshot.

**Recommendation.** Split the two axes explicitly in the amended rows:
- write fence + `Complete`/`Abort` admission: log-ordered, no clock (as ruled);
- replica-feed hold: keep a deadline (it is an anomaly-hiding device, not a single-writer
  mechanism — FM-CLUSTER-097 says so itself), and bound the per-session buffer, degrading to
  "disconnect the replica, force a resync" rather than growing;
- rewrite FM-CLUSTER-084/085 to state the new resolution rule (`exactly one of Complete/Abort
  per `seq``, second refused) and say plainly that a partitioned source parks its own slot
  until it applies the resolution, and that the *only* liveness path is the leader's
  level-triggered `Abort` from issue 18's pass.

### C2 — Ruling 15 makes migration cancellation automatic, and nothing repatriates keys `MIGRATE` already moved

**Where.** Issue 15 ruling: "Any failover — graceful or forced — cancels open migrations naming
the demoted node on either leg… The orchestrator restarts migration under the new topology."

**The hole.** FM-CLUSTER-028 states the source deletes each key as it hands it over:

> `MIGRATE` deletes each key as it hands it over, so "we own the slot" and "we hold the key" are
> different questions for the whole duration of the window.

No FM row states what happens to those already-moved keys when the migration record is deleted.
FM-CLUSTER-035 (`SETSLOT … STABLE`) is an unconditional `migrations.remove(&slot)`; the
coordinator's `cancel` (`slot_migration/mod.rs:299-303`) just proposes `CancelSlotMigration`.
After a cancel, the slot's owner is missing every key already shipped, and the ex-target holds
them but no longer answers for the slot (no migration record ⇒ no `ASK`, and `ASKING` opens
nothing). Those are acknowledged writes that are now unreachable — the precise shape
FM-CLUSTER-037/092 call "acknowledged-then-orphaned" and that the whole barrier family exists
to prevent.

This is Redis parity in the narrow sense (an aborted `redis-cli --cluster reshard` needs
`--cluster fix`), but Redis never triggers it *automatically*. Ruling 15 does: every failover
during a rebalance now silently splits a slot's keyspace, with no operator in the loop and no
`fix` verb in FrogDB.

**Why the ruling's own alternative was better here.** Issue 15's option 2 (retarget
`source_node` to the successor) does not strand anything: the successor inherited the
post-delete keyspace via PSYNC, the target keeps its imported keys and its importing role, and
the migration can still complete. The one objection recorded against option 2 — "has to decide
what a prepared handoff's drain state means when the source changed under it" — is largely
dissolved by ruling 17: the handoff is now resolved by exactly one `Complete`/`Abort` keyed to
`handoff_seq`, so a retarget can `Abort` the in-flight attempt (emitting the release) and leave
the migration record retargeted for a fresh `Prepare`.

**Recommendation.** Either (a) retarget on the source leg and prune only on the target leg
(where the target is gone and its partial import is unreachable anyway), or (b) keep prune and
add: a spec row for "a cancelled migration may leave a slot split", a loud replicated event +
metric, and a repair verb. Either way the row is owed for `SETSLOT … STABLE` as well — the
exposure predates ruling 15; the ruling only makes it automatic.

---

## HIGH

### H1 — Ruling 19's **global** epoch fence contradicts the spec's own per-object fencing argument, and collides with FM-CLUSTER-013

**Where.** Issue 19 ruling: "epoch-fenced proposals, GLOBAL epoch … apply refuses the command
if the current epoch has advanced."

FM-CLUSTER-095 already litigated exactly this question and ruled the other way:

> Nor the refusal firing on a handoff prepared for a *different* slot: the token carries one
> slot's generation, not a node-wide epoch, so unrelated cluster traffic … cannot cause
> spurious refusals.

The failover fence is now the node-wide-epoch design that row rejects. The interference is not
hypothetical: FM-CLUSTER-013 bumps `config_epoch` on **every** `MarkNodeFailed`, and
FM-CLUSTER-040/041 bump it on every failover. So in a correlated failure — a rack or AZ loss,
the case automatic failover exists for — the leader's reconcile pass marks k primaries (k
bumps) and each accepted failover bumps again, invalidating every proposal scored earlier in the
same pass. Best case that serializes to one failover per tick; under a flapping link
(`MarkNodeFailed`/`MarkNodeRecovered` churn, which FM-CLUSTER-052's hysteresis reduces but does
not eliminate) it can starve indefinitely, with the level-triggered pass supplying an endless
supply of refused proposals and no signal that anything is wrong.

**Practice.** Every mature system fences per-object: CRDB range-lease sequence numbers, Kafka
per-partition leader epochs, ZooKeeper per-znode versions, etcd per-key mod-revision CAS.
Global monotonic counters as CAS tokens are a known false-sharing anti-pattern.

**Recommendation.** Fence on the object the command is about: `(old_primary_id, that node's
`config_epoch`, its role)` — that is exactly what makes the stale proposal in issue 19's
evidence stale, and it is immune to unrelated marks. Keep the belt ("refuse when the command
would move nothing"), which already closes issue 19's actual defect on its own; the fence is
the generalization, and it should be the per-object one the spec already prefers. If global is
kept for simplicity, the amended row must state the starvation mode and the pass must carry a
progress guarantee (at most one epoch-moving entry in flight per tick, and a stuck-failover
signal).

### H2 — Failover has no barrier, no drain and no offset-parity wait: the two-writers/lost-write window the migration path spent 14 rows closing is wide open on the role-change path

**Where.** FM-CLUSTER-041 (graceful failover) and the operator handler
(`commands/cluster/admin.rs:293-…`). The handler validates roles and proposes `Failover`
immediately: no client pause on the old primary, no wait for the successor's replication offset
to reach the primary's head, no fence.

Consequences, on both paths:
- The old primary keeps admitting writes for its slots until *it* applies the demotion, while
  the successor starts serving as soon as *it* applies the promotion. Two writers, bounded only
  by per-node apply lag — structurally identical to issue 17's stale-source finding, which the
  team ruled worth fixing for migration.
- Writes acked by the old primary above the successor's replication offset are discarded when
  the demoted node resyncs (`specs/replication.md` FM rows for demotion/adopt-history). Silent
  loss of acknowledged writes on an operator-initiated, no-failure-present command.

**Practice.** Redis's manual `CLUSTER FAILOVER` is lossless by construction: the master pauses
clients, the replica waits until its offset matches, and only then does the swap happen
(`CLUSTER FAILOVER FORCE`/`TAKEOVER` are the documented lossy escapes). Raft leadership
transfer (and CRDB's lease transfer) likewise waits for the target to catch up before handing
over. FrogDB's "graceful" failover is a demote, not a handover.

The automatic path being lossy is defensible (async PSYNC, Redis parity) — but the spec never
says so, and ruling 20 now routes the automatic path through the same `force: false` shape,
which makes the two indistinguishable in the rows.

**Recommendation.** (i) Add an FM row stating plainly that automatic failover may lose
acknowledged writes not yet shipped, with `WAIT` as the operator's tool — it is the honest
Redis-parity claim. (ii) For the planned path, reuse the machinery that already exists: arm the
slot pauses on the old primary (FM-CLUSTER-079), drain (FM-CLUSTER-090/091), require offset
parity, then propose the swap; the `SlotFence` token (FM-CLUSTER-095) already carries `owner`,
so the execute-seam half is free.

### H3 — Write admission is gated on TCP-probe-derived local quorum, not on Raft liveness or view freshness

**Where.** FM-CLUSTER-055 (quorum counted over locally-probed peers), FM-CLUSTER-059
(self-fence), GAP 4 ("liveness is a bare TCP connect, so a wedged-but-listening node reads as
healthy").

Issue 20's evidence is the proof: node 0 is partitioned from the Raft leader, still reaches one
peer, counts 2-of-3, does **not** fence, and serves slots that have already been failed over.
Ruling 20 fixes the *post-heal* half (Raft becomes the reconciliation channel). The
during-partition window is untouched, and it is the window in which two nodes serve one slot.

The general defect: the quantity being measured (can I open a TCP socket to some peers?) is not
the quantity that matters (is my view of who owns this slot still current?). A wedged peer, a
peer that answers `HealthProbe` unconditionally (GAP 4 says it does — "without consulting
quorum, fence, or loading state"), or a peer on the same losing side of the partition all count
as evidence of health.

**Practice.** etcd/raft use CheckQuorum + PreVote so a leader that cannot hear a majority steps
down; CRDB refuses to serve a range without a valid lease; Kafka's leader stops accepting
writes when it cannot maintain the ISR. All three make "am I still authoritative?" a function
of consensus-layer liveness, not of point-to-point reachability.

**Recommendation.** Derive the self-fence from the Raft layer: fence when this node has not
applied an entry from, or heard an `AppendEntries` from, a leader within an election timeout —
and state the principle in the spec so it does not read as a violation of ruling 17: **using a
clock to *stop* serving is fail-closed and safe; using one to *admit* is the rejected
anti-pattern.** That distinction reconciles H3, H4 and ruling 17 and is worth one paragraph in
the spec preamble, because without it a future reader will "fix" H3 by deleting the timeout.

### H4 — Ruling 20 defers the eviction fence, leaving the identical two-writers shape on the administrative path

**Where.** Issue 20 ruling: "Option 2 (local eviction-fence signal) is a candidate follow-up
issue for administrative eviction, not part of this fix."

`CLUSTER FORGET` of a *live* node reproduces issue 20 exactly: the node is removed from the
topology and (FM-CLUSTER-101) from the Raft voter set, so it applies nothing further, keeps its
pre-`FORGET` slot map, and keeps answering `GET`/`SET` for slots the survivors have since
reassigned. FM-CLUSTER-002 leaves those slots unassigned, so the window opens the moment an
operator re-`ADDSLOTS` them — the documented recovery procedure. The same shape appears when
`add_learner` fails permanently (issue 25's log: "node is in cluster state but NOT a Raft
voter").

**Recommendation.** File the follow-up now and treat it as part of this campaign's exit, not as
a maybe: a node that (a) finds itself absent from the membership of the last entry it applied,
or (b) has applied nothing for more than N election timeouts, fences its keyed traffic
(`CLUSTERDOWN`). This is the same fail-closed-use-of-time principle as H3, and it is the only
finding class the campaign has closed for migration but not for membership.

### H5 — Ruling 18's level-triggered pass has no backoff, no in-flight dedup, and no stated interaction with `MAX_ATTEMPTS`

**Where.** Issue 18 ruling: "`reconcile_topology` gains a pass that runs failover selection for
EVERY failed primary still owning slots that has an eligible replica, every tick."

Level-triggering is the right call (see "matches practice" below). What the ruling omits:
- **Backoff.** A failover that cannot succeed — H1's epoch refusal, a successor that keeps
  failing, a `Failover` the state machine refuses on any other ground — is re-proposed every
  `check_interval_ms` (default 1000 ms) per failed primary, forever. Each attempt is a Raft
  round trip.
- **Dedup.** The spec already has the right primitive and flags it as untested: GAP 2,
  `InflightGuard`'s "at-most-one-write-per-peer property". The new pass should reuse it, or two
  ticks can have two failovers in flight for one primary (each of which, under H1, invalidates
  the other).
- **The old give-up.** `trigger_auto_failover`'s `MAX_ATTEMPTS = 3` permanent give-up (spec GAP
  3: "the highest-consequence untested path in the area") is the mechanism ruling 18 exists to
  replace. The ruling never says it is deleted; leaving both gives a retry loop and a
  permanent-abandon flag fighting each other.
- **Observability.** A cluster whose failover has been refused 4000 times looks identical, from
  outside, to a converged one.

**Recommendation.** Per-`(node_id)` exponential backoff with a cap, `InflightGuard` reuse,
delete `MAX_ATTEMPTS`, and a `cluster_failover_blocked{node,reason}` counter + a warn-once log.

### H6 — `CLUSTER FAILOVER FORCE` sent to a *primary* absorbs an arbitrarily chosen peer primary and evicts it

**Where.** `commands/cluster/admin.rs:284-308` (reached only via the `FORCE`/`TAKEOVER` arm on a
primary); FM-CLUSTER-040 acknowledges "the primary-absorbs-primary case" as an observable but
never says how the victim is chosen.

The victim is selected by `snapshot.nodes.iter().find(…)` — a `HashMap` iteration — preferring
a `fail`-flagged primary and otherwise **any other primary**, and the resulting proposal is
`force: true`, i.e. the victim is removed from the topology (FM-CLUSTER-002-shaped pruning) and
from the Raft voter set (FM-CLUSTER-101). So a single mistyped/misrouted command takes a
*healthy* primary's slots, moves them without their data (issue 20's evidence: "ownership moved
without the data — a promotion carries only what PSYNC already shipped"), and shrinks quorum —
against a target that is not deterministic run to run.

Redis refuses this outright: `CLUSTER FAILOVER` is replica-only, and a master answers an error.
A Redis-compatible operator script that sends `FAILOVER FORCE` to the wrong node gets an error
from Redis and data destruction from FrogDB.

**Recommendation.** Refuse `CLUSTER FAILOVER` on a primary (Redis parity), or require an
explicit victim argument. If the absorb mode is deliberately kept, it needs its own FM row
naming the selection rule; "any other primary, HashMap order" is not a rule.

---

## ADVISORY

### A1 — Ruling 14 makes `AddNode` refusable, against FM-CLUSTER-011's stated principle

FM-CLUSTER-011: "Nor a rejection: `AddNode` never refuses on epoch grounds, because refusing
membership over a numbering conflict partitions the cluster instead of healing it."
FM-CLUSTER-001: "The command always answers `ClusterResponse::Ok`." Ruling 14 introduces a
refusal path for a new node whose `primary_id` dangles or names a replica.

On today's paths this is unreachable — FM-CLUSTER-001 establishes that a self-registering node
can only ever claim `Primary`, and re-registration keeps recorded role/parent — so it is a
fail-closed guard, not a live hazard. Two notes anyway: (a) any future bulk-topology import,
restore-from-backup or operator-supplied roster makes admission *order-dependent*, a classic
reconfiguration trap (register the replica before its primary and the node never joins, with a
one-shot `MEET` that nothing retries); (b) **repair-on-admit** (accept the node, clear the
illegal parent, log + event) retires INV-REF-3B just as completely as refusal does, and is
consistent with FM-CLUSTER-011's own reasoning. `SetRole` refusing is unambiguously right —
that is an explicit operator intent with a caller who can see the error.

### A2 — Ruling 25 should also require an empty Raft state to *join*, and persist the intent

Adopting etcd's `initial-cluster-state: new|existing` split is exactly right. Two gaps the
ruling leaves:
- A node that legitimately bootstrapped its own single-node cluster (has a term, a vote and
  committed entries) can still be `MEET`'d into another cluster later, and one side's log is
  silently discarded. etcd's protection is that a joining member must start with an empty data
  directory, and the join is rejected otherwise. Recommend: `MEET` refuses a node whose Raft
  state is non-empty and whose membership does not already contain the meeting cluster —
  operator remediation is `CLUSTER RESET HARD`.
- The bootstrap-vs-join intent must be persisted (or derived from on-disk Raft state, as the
  existing `already_initialized` check does) so that a restart of a bootstrap node cannot
  re-bootstrap over a cluster it has since joined.
The proposed cross-node checker invariant ("a node answering as leader must be the agreed
leader or unreachable") is the right generalization and is cheap; it would also have caught H4.

### A3 — Ruling 16 leaves "migrating but unassigned" reachable

Source-only assignment closes the third-party hole precisely. But the source may still
`DELSLOTS` its own slot (the ruling applies the *same* check to `RemoveSlots`, i.e. the source
is permitted), so the migrating-but-unassigned state stays reachable and the slot answers
`CLUSTERDOWN` until someone re-assigns or `STABLE`s it. The structurally simpler rule — refuse
**any** slot-map mutation on a slot with an open migration, making `SETSLOT … STABLE` the one
way out — is closer to "the migration record is the authority" (FM-CLUSTER-033) and removes a
state rather than legalizing it. Worth a sentence in the amended row either way.

### A4 — Failover no longer shrinks membership: dead voters now accumulate invisibly

Ruling 20's accepted cost ("a dead node lingers as a voter until the operator removes it") is
the right trade, but it needs an operator-facing surface or quorum erodes silently: after two
automatic failovers a 5-voter cluster may have 2 dead voters and no indication in `CLUSTER
INFO`. Recommend a rendered count/metric of "voters flagged `fail`", and a documented
decommission procedure in the website docs (the `CLUSTER FORGET`-then-shutdown order matters,
per H4).

### A5 — Ruling 20 invalidates FM-CLUSTER-039's stated rationale for the `force` waiver

FM-CLUSTER-039: "A *force* failover of an already-removed node is deliberately allowed, because
that is precisely the situation an automatic failover fires in." With the automatic path
proposing `force: false`, that sentence is no longer true, and the automatic path now inherits
`test_failover_graceful_requires_old_node`'s `NodeNotFound(old)` refusal — e.g. a failover
scored against a primary an operator `FORGET`s in the same window is refused where it used to
land. Ruling 18's level-triggered pass makes that self-correcting (the next tick re-scores and
finds nothing owed), which is the right answer — but it must be *written*, or the next reader
"restores" the force waiver. Related amendments the rulings imply and should be tracked as one
edit set: FM-CLUSTER-036 (title says "the node it removes"), FM-CLUSTER-040/041 (which shape
the automatic path takes), FM-CLUSTER-101 (its trigger list names `Failover { force: true }`
"decided by the failure detector"), FM-CLUSTER-013/014 (epoch churn under H1), FM-CLUSTER-084/
085/095/097 (C1), FM-CLUSTER-001/005 (ruling 14), FM-CLUSTER-003/004 (ruling 16).

---

## Where the spec already matches or exceeds best practice

Substantive, not a rubber stamp — these are the places an adversarial read found nothing to
attack:

1. **FM-CLUSTER-089 — no clock read during `apply`.** Every deadline is proposer-minted data
   carried in the log entry, enforced by a lint (`just lint-clock-seam`). This is the
   deterministic-state-machine discipline FDB, etcd and CRDB all hold, and the row states the
   divergence argument (replay yesterday's log, compute today's deadline) correctly. Ruling 17
   sharpens it further by removing the last clock input from the handoff's *admission*.
2. **FM-CLUSTER-086 + FM-CLUSTER-100 — the replicated `handoff_seq` fencing token.** Minted
   from replicated state, echoed by every follow-up message, filtered on match, and — the part
   most implementations get wrong — carried through *both* snapshot restore vehicles so it can
   never be re-minted. That is a textbook fencing token (Kafka leader epoch / ZK zxid class),
   and FM-CLUSTER-100's reasoning about why `max(seq)` over surviving records is *not* a valid
   re-derivation is exactly right.
3. **FM-CLUSTER-053 — level-triggered verdicts, latch never decays.** The row states the
   precise reason (a new leader must converge on a peer whose threshold was crossed while it
   was a follower — "the transition-triggered version drops that write and never retries it").
   Ruling 18 extends the same discipline to the failover trigger, which is the correct fix and
   the one this review would have recommended unprompted.
4. **FM-CLUSTER-055 — unprobed peers count as unreachable.** Fail-closed quorum arithmetic; a
   freshly booted node has no quorum until its probes land. (H3 is about the *input*, not this
   arithmetic, which is right.)
5. **FM-CLUSTER-040/041/039 — the composite failover is one log entry, validate-all-then-apply.**
   No sequence of entries a crash can land between; the same two-phase shape recurs in
   FM-CLUSTER-003/004/033. This is the CRDB/KRaft atomic-reconfiguration pattern and it is
   applied consistently.
6. **Ruling 20 — failover changes roles, never membership.** This is the single best decision
   in the set. Automatic, detector-driven membership changes are a well-known distributed-systems
   anti-pattern (etcd, CRDB and KRaft all require explicit member add/remove precisely because a
   failure detector's opinion must never shrink a quorum), and using Raft itself as the
   reconciliation channel for the returning node is the structurally correct analogue of Redis's
   `clusterUpdateSlotsConfigWith`.
7. **Ruling 25 — explicit bootstrap-vs-join.** Adopting etcd's `initial-cluster-state` shape is
   the standard answer to the "everyone self-elects a singleton" hazard, and the accompanying
   error-shape fix (never render a learner-promotion conflict as a client `-REDIRECT`) is the
   right layering: a redirect must name an authority, not echo an internal forwarding hint.
8. **FM-CLUSTER-098/099/103 — the storage layer below Raft.** Synced vote writes classified at a
   single chokepoint (with the *reason* — a lost vote is a split-brain precondition, not a
   performance detail), a shared log cache so a long-lived reader cannot serve truncated
   entries, and an inclusive `truncate` contract validated against openraft's own conformance
   suite. Finding a one-index log-divergence bug by *building the layer that could see it* is
   the strongest evidence in the whole document.
9. **FM-CLUSTER-062/063/064 — allow-list admin gating.** Fail-closed on the bare container
   command and on unknown subcommands, with a registry-coherence test that makes "gated by two
   mechanisms that disagree" unrepresentable.
10. **The invariant catalog + property harness + stateright models.** Checking the HARD tier at
    every state-producing seam (including the *rejected* apply path), cross-referencing rows to
    catalog entries in both directions with a lint, and then model-checking with
    characterization properties that go red when a defect is fixed, is a stronger verification
    posture than most production databases carry. Issues 17, 18 and 19 were all *found* this
    way, which is the system working.
11. **FM-CLUSTER-095's honesty about what a fence buys** ("A fenced command may still have
    *applied* locally — the fence refuses the acknowledgement, not the work… the ordinary
    at-least-once contract"). Precise, and the right claim.
