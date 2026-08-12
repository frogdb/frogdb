# Replication correctness — invariant-driven validation

Status: **RULED** — all §8 decisions ruled 2026-08-10
Author: 2026-08-10
Related: [cluster correctness](../cluster-correctness/PRD.md) (the pattern this ports; exit
criterion 8 met 5/5), [replication failure-mode spec](../hardening/specs/replication-failure-modes.md)
(LOCKED, FM-REPLICATION-001..064), [hardening campaign 2](../hardening-2/PRD.md) (detection-first,
running), [replication-cluster rework](../replication-cluster-rework/PRD.md) (promotion replid +
WAIT cluster mode, shipped), [ADR 0004](../../adr/0004-replication-runtime-seam-boundary.md)

## 1. Why

The cluster campaign made a falsifiable claim — "the machine now catches what a manual audit
caught" — and then tested it by reverting five known fixes one at a time. It came back 3/5, the two
misses became [issue 21](../cluster-correctness/issues/done/21-no-layer-sees-the-raft-log-store.md)
and [issue 22](../cluster-correctness/issues/done/22-no-layer-generates-runtime-config-values.md),
and closing them made it 5/5. Along the way the layers themselves found eight defects nobody had
read out of the code, plus FM-CLUSTER-103 (`truncate` exclusive where openraft contracts it
inclusive) on the first run of a new layer. The pattern paid for itself. The user's ruling at exit:
**it ports per area, against each area's own state shape, with no shared framework.** Replication is
next.

Replication looks, on paper, like the *most* validated area in the tree: a LOCKED spec with 64 rows,
a 98.7% mutation score on `frogdb-replication` over 1180 mutants, 100% of viable on
`frogdb-replication-runtime`, 391 in-crate tests and 131 in `integration_replication.rs`. Three
things say that number is not what it looks like:

1. **The score is a floor by construction, and the ADR says so.** ADR 0004 records the extraction
   baseline: `cargo mutants -p frogdb-replication` scored 74.7% on viable mutants "because the
   forcing tests for most of these behaviors live in
   `frogdb-server/crates/server/tests/integration_replication.rs`, which never runs against a
   mutant"; `frogdb-replication-runtime` scored **50.0%**, with `apply_single`, `apply_transaction`,
   `apply_group`, `export_live_dataset`, `install` and `read_snapshot` surviving as whole functions.
   The gate was reached by moving tests down, but the shape of the problem — behavior specified in
   one crate and forced from another — has not gone away.
2. **The area's recent defect history is four real bugs, all found by humans.** WAL not drained
   before the full-resync checkpoint was cut, so acked writes were absent from the payload
   (`ebdf7d9e`, 2026-07-30). Promotion minted no replid and adopted no secondary window, so a
   promoted node answered every downstream `PSYNC` with an error (`f6484219`, 2026-07-28). `WAIT`
   counted a replica from its *seeded resume position* instead of a wire ACK (`90fefaf7`,
   2026-08-05). The replica feed ran while a slot-handoff barrier was armed (`8d55cc4f`,
   2026-08-08). Every one was a code read or a rework PRD, not the machinery.
3. **The spec's own GAPS section still has two open holes** — GAP-5 (nothing asserts `WAIT` can
   never exceed `connected_slaves` across a reconnect, `tracker.rs:146-153`) and GAP-6 (`-UNBLOCKED`
   on demotion racing `CLIENT UNBLOCK`) — both of which are quantified statements no point witness
   can make.

The three structural blind spots the cluster PRD named transfer verbatim, and replication adds a
fourth that is the whole cost story of this port:

- **B1 — point witnesses do not quantify.** An FM row pins one scenario; a mutation score is a floor
  on code that exists. Nothing in the tree states "`landed <= applied <= live`, always" or "a
  promotion preserves the inherited history as `secondary_id`" *for all sequences*.
- **B2 — no harness explores interleavings.** There is no turmoil sweep over a replication topology
  at all. The seeded cluster scheduler has a `Family::ReplicaPartition`
  (`frogdb-server/crates/server/tests/simulation/scheduler.rs:105-127`), but it attaches one PSYNC
  replica to a *Raft* cluster; primary/replica-only schedules, promotion mid-stream and full-sync
  interruption are unexplored. Shuttle covers `frogdb-core` only.
- **B3 — internal state is invisible to the black-box suites.** There is no `DEBUG REPLICATION
  CHECK`. The nearest thing in the area is `ReplicationState::validate`
  (`frogdb-server/crates/replication/src/state.rs:295`), which checks the *shape* of two hex strings
  and nothing else. A half-cleared failover window, a backlog floor above its own oldest entry, or
  two sessions counted for one replica is not client-visible until it detonates.
- **B4 — there is no state machine to hang any of this on.** Cluster had `ClusterStateInner` and one
  `apply_command`; the entire campaign hung off that. Replication state is a federation of shared
  cells mutated in place by async tasks: `Arc<RwLock<ReplicationState>>` held by six components
  (`identity.rs:30`), one live-offset `Arc<AtomicU64>` shared four ways, `AppliedOffset`'s two
  atomics plus a gate mutex plus an epoch and a divergence counter (`replica/offset.rs:49`), the
  tracker's two `RwLock<HashMap>`s and an `AtomicU8` departure code (`tracker.rs:64`), and a ring
  buffer whose entries are behind a `Mutex<VecDeque>` while its floor is a separate `AtomicI64`
  (`primary/ring_buffer.rs:81`). There is no command enum, no transition function, and no plain-data
  state to hand a model checker. **W1's first job is to create one.**

**Thesis, unchanged from the cluster campaign: define replication correctness once, as executable
invariants over one plain-data projection, and check that single definition under *generated*
permutations and faults at every fidelity level.** What is new here is that the projection has to be
built before it can be checked, and that construction is itself the architectural payoff — it is the
missing seam between the protocol's decisions and its I/O.

## 2. Shape of the system

One projection, one catalog, five consumers:

| layer | explores | cost/run | repro | new machinery |
|---|---|---|---|---|
| L1 seam self-check | every existing test's scenarios | ~free | exact | `ReplicationView` + catalog + hooks |
| L2 proptest permutations | link-action sequences × save/restore points | seconds | shrunk sequence | generator + properties |
| L3 stateright model check | promotion/feed-gate interleavings, small scope | minutes | exact trace | 2 models + a decision/IO split |
| L4 seeded turmoil schedules | link/crash/timing schedules on a replication topology | minutes–hours | seed | replication arm on the existing scheduler |
| L5 Jepsen + live check | real processes, real faults | heavy | partial | `DEBUG REPLICATION CHECK` + checker wiring |

**`ReplicationView` is the load-bearing new type.** Because no single struct owns replication state,
the catalog cannot be "pure functions over `&SomeStateInner`" the way `frogdb-cluster`'s is. It is
instead pure functions over a plain-data projection captured from the live components at check time:

```
ReplicationView {
    state:     ReplicationState,          // replid, secondary window, offset_at_save (clone)
    live:      u64, applied: u64, landed: u64,
    apply_gate: { frozen: bool, stint: u64, epoch: u64, diverged: u64 },
    backlog:   BacklogGeometry + oldest/newest offsets + entry/byte totals,
    replicas:  Vec<{ id, addr, announced_id, phase, acked, resume_floor, last_ack_age }>,
    departure: Option<ReplicaDeparture>,
    feed_gate: Option<Instant>,           // hold deadline
    fence:     { self_fence_enabled, armed, freshness_window },
    role:      Primary | Replica(addr),
}
```

Every field already exists somewhere; the view is assembly, not new state. It is what the catalog
checks, what `DEBUG REPLICATION CHECK` renders, what the proptest asserts on after every action, and
what the stateright models use as their model state. See §8 D7 for where it lives, given that three
of its inputs are outside `frogdb-replication`.

A violation report is identical in shape at every layer: stable invariant id + detail string —
`Violation { id: &'static str, detail: String }`, the same type the cluster catalog defines at
`frogdb-server/crates/cluster/src/invariants.rs:80` (§8 D6 on sharing vs duplicating it).

## 3. Workstreams

### W1 — Invariant catalog + self-checking projection

`frogdb-server/crates/replication/src/invariants.rs`, and the `ReplicationView` capture that feeds
it. Port the cluster catalog's exact vocabulary and its compile-time discipline verbatim:
`Tier::{Hard, DocumentedException(Citation)}` with the citation as a **variant field**, `Citation`
constructed only through `const fn failure_mode(..)` / `issue(..)` which `assert!` non-empty — so
inside a `static CATALOG`, a citation-less exception is a build error rather than a review comment.
Two tiers, no third.

Seed catalog — each entry names the defect class it would have caught, so no entry is decorative:

| ID | claim | would have caught |
|---|---|---|
| INV-REPLID-1 | `secondary_id.is_some()` iff `secondary_offset >= 0` | a half-cleared failover window (`clear_secondary_window` vs `shift_replication_id` divergence) |
| INV-REPLID-2 | after a promotion the previous `replication_id` is the `secondary_id` and `secondary_offset` is the offset it was frozen at | **revert (b)** — `f6484219`, promotion minting nothing and adopting no window |
| INV-REPLID-3 | `replication_id != secondary_id`; both 40-hex per `is_valid_replication_id` | id reuse across a promotion, which makes `window_contains` answer for two histories |
| INV-OFFSET-1 | `landed <= applied <= live` | `ReplicaApplyStint::{claim,land}` / `freeze` ordering regressions (`replica/offset.rs:350/382/302`) |
| INV-OFFSET-2 | `offset_at_save <= live` | a persisted offset ahead of what was received — resurrection after restart |
| INV-OFFSET-3 | every registered replica's `acked` and `resume_floor` are `<= live`, and `acked` never precedes a wire ACK for a session below `Streaming` | **revert (c)** — `90fefaf7`, a seeded resume position counted as an ack |
| INV-OFFSET-4 | `secondary_offset <= live` | a frozen window above the history it describes |
| INV-BACKLOG-1 | `start_offset <= oldest buffered offset`, and buffered offsets are contiguous | the backlog-eviction hole class (`86a016fd`); FM-REPLICATION-016 |
| INV-BACKLOG-2 | a granted `+CONTINUE`'s `replay_from` is `>= start_offset` and `<= current` | FM-REPLICATION-014 — a `+CONTINUE` served over a hole |
| INV-BACKLOG-3 | buffered entries and bytes are within both configured caps | FM-REPLICATION-016/047 |
| INV-SESSION-1 | a session's `Phase` only moves forward in the declared order; `Disconnecting` is terminal | `replica_session.rs:46` asserts this in prose ("moves monotonically forward") and nothing checks it |
| INV-SESSION-2 | at most one session per announced replica identity is in `Streaming` | **spec GAP-5** — `WAIT` double-counting a reconnect while the old session lingers |
| INV-SESSION-3 | a `last_streaming_departure` exists only if some session reached `Streaming` | FM-REPLICATION-062 — the self-fence disarmed by a session that never armed it |
| INV-GATE-1 | a held feed gate's deadline is in the future and within the configured barrier budget | **revert (d)** — `8d55cc4f` / FM-CLUSTER-097; a hold that outlives its deadline wedges the feed |
| INV-FENCE-1 | the quorum checker is armed only after a session reached `Streaming`, and disarmed only by a `Graceful` departure | FM-REPLICATION-041/062 |
| INV-ROLE-1 | a node with an upstream link registers no downstream streaming session unless chaining is enabled | two primaries feeding one replica; the chained-replication non-guarantee (testing-improvements 48) |

**Hook sites.** Cluster had one (`apply_command`, plus two restore vehicles). Replication has a
handful, and naming them precisely is most of W1's design:

| seam | file:line | why |
|---|---|---|
| `PrimaryReplicationHandler::begin_primary_stint` | `primary/mod.rs:389` | **the highest-value single hook** — synchronous, mints the replid, persists, resets and re-arms the backlog under one write lock, rolls back on IO error. Exactly where revert (b) lived |
| `PrimaryReplicationHandler::end_primary_stint` | `primary/mod.rs:448` | fences WAIT, resets backlog, retires applies, disconnects replicas |
| `ReplicationState::{new_replication_id, shift_replication_id, clear_secondary_window, adopt_replication_history, apply_staged_metadata}` | `state.rs:318/339/354/365/420` | the five writers of the identity/window pair |
| `OffsetCoordinator::{advance, settle_at_applied, ingest_replica_ack, seed_replica_position}` | `offset_coordinator.rs:108/160/197/210` | the primary's single advance gate and the two ack ingest paths |
| `ReplicaOffset::{frame_advance, reset_to}` / `AppliedOffset::{advance_by, freeze, retire_replica_applies}` | `replica/offset.rs:482/514/128/302/215` | the replica's received/applied/landed triple |
| `ReplicationRingBuffer::{push, arm_start, reset}` | `primary/ring_buffer.rs:169/111/128` | append + eviction + floor arm — the only writers of the two-cell ring |
| `ReplicaSession::commit_phase` | `replica_session.rs` | the sole `Phase` writer, now fed by `session_machine::step` |
| `ReplicationTrackerImpl::{register_announced_replica, unregister_replica, record_streaming_departure}` | `tracker.rs:183/200/215` | registry add/remove and the departure latch |
| `ReplicaFeedGate::publish` | `feed_gate.rs:75` | the only writer; called from `core/src/client_registry/mod.rs:979` |
| `ReplicaConnection::set_state` | `replica/connection.rs:159` | the only `ConnectionState` writer, publishes `link_up` in the same step |

The hook is `debug_assert_view_clean(&view, "<seam>")` under `#[cfg(any(test, debug_assertions))]`,
where the seam's owning component builds the widest view it can reach — a component that cannot see
the tracker checks the invariants that do not need it, and the whole-node view is assembled only at
the two places that hold everything (`begin_primary_stint` and the `DEBUG` command). This *partial
view* design is the honest consequence of B4 and should be stated in the catalog: every invariant
declares which view fields it needs, and `check_hard(view)` skips the ones whose inputs are absent
rather than reporting a false violation.

Cluster's transition match was refactored into a private `apply_to` so that arms which `return
Err(..)` early cannot skip the hook. The replication analogue is that every hooked seam must have a
single exit — several of the ones above have early returns today, and straightening them is part of
the work, not a side quest.

Retroactive upgrade: the hooks turn the 391 in-crate `frogdb-replication` tests and the 131
`integration_replication.rs` tests into invariant tests at zero authoring cost (the cluster
equivalent was ~430). Each HARD invariant additionally lands with a forcing test that constructs the
violating view directly and asserts the catalog reports it — otherwise the catalog is dead code to
the mutation gate — plus a `#[should_panic]` hook-forcing test per seam so a deleted hook goes red.

Expect defects at catalog-build time: cluster found two ([issue
14](../cluster-correctness/issues/open/14-role-transitions-admit-malformed-parents.md), [issue
15](../cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md)),
one of which had to become the catalog's single `DocumentedException` because a runtime test was
building the malformed state on purpose. Replication's known candidates are already written down:
GAP-5 is INV-SESSION-2, and the chained-replication non-guarantee is INV-ROLE-1's likely exception.

### W2 — Property-based permutation harness (proptest)

`proptest` is **already a dev-dependency** of `frogdb-replication` and is used exactly once, for a
checkpoint-header round trip at `fullsync.rs:1128`. No new dependency; `frogdb-replication-runtime`
has no `[dev-dependencies]` section at all and gains one (§8 D7 touches the same boundary).

Hard rule inherited from the cluster campaign: **generation goes through the real code — no shadow
model.** Since replication has no command enum, the alphabet is the seam operations themselves:

`arb_link_sequence(len)` over `LinkAction` — `Write(bytes)`, `Ack{replica, offset}`,
`Attach{replica, announcement}`, `Psync{id, offset}`, `Detach{replica, departure}`, `Promote`,
`Demote(addr)`, `HoldFeed(ms)`, `ReleaseFeed`, `SaveState`, `RestoreState`, `StageMetadata`,
`EvictBacklog`, `Freeze`, `AdmitDivergence`. Stateful generation tracking live replicas, phases and
the backlog window, biased ~80/20 toward in-context-valid actions, with garbage retained
deliberately: a *rejected* action must also preserve every invariant, and the rejection path is
exactly where validate-then-mutate bugs live.

Properties:

- **R1 — invariants always hold**: apply the sequence, assert `check_hard(view)` clean after every
  action, including rejected ones.
- **R2 — save/restore is lossless at any point, through both vehicles**: apply a prefix, round-trip
  the state through `ReplicationState::save` / `load_or_create` *and* through the staged path
  (`read_staged_replication_metadata` → `apply_staged_metadata`, `state.rs:52/420`), apply the
  suffix, compare against the uninterrupted run. The two vehicles can disagree — that is exactly the
  class cluster's P2 caught in FM-CLUSTER-100 — and here it is also the FM-PERSISTENCE-039 window.
- **R3 — the PSYNC decision is total**: `PartialSyncReplay::handle_partial_sync_request`
  (`primary/replay.rs:350`) never panics, yields exactly one `ReplayDecision`, and every granted
  `+CONTINUE` names a range that is contiguous and inside the ring. FM-REPLICATION-013/014/015
  stated once for all inputs instead of three times for three scenarios.
- **R4 — determinism and purity**: the same action sequence against two fresh nodes yields identical
  views. Doubles as a wall-clock guard on the decision path — the exact class of cluster [issue
  23](../cluster-correctness/issues/open/23-scheduler-fingerprint-is-load-dependent.md), caught here
  cheaply instead of in a turmoil fingerprint.
- **R5 — offset conservation**: `live` minus the seed equals the sum of bytes admitted by `advance`;
  `landed <= applied <= live` survives arbitrary interleavings of `claim` / `land` / `freeze` /
  `admit_divergence` / `retire_replica_applies`.
- **R6 — fence admission is total** (the `frogdb-replication-runtime` half, and the cheapest way to
  move that crate's score): `ReplicationQuorumChecker` (`quorum.rs:52`) is all-synchronous over
  atomics, so generate `(config, replica set, departure code)` triples and assert `has_quorum` and
  `write_fence_reason` are total, agree with each other, and never arm from a state no session ever
  streamed. This is the direct analogue of cluster issue 22's config-admission properties C1/C2,
  which is what turned that campaign's second retro-validation miss into a catch.

Plus one non-property deliverable: **frozen encoding fixtures** for `ReplicationFrame` and
`FullSyncMetadata` — golden bytes checked in, round-trip asserted. `version_compat.rs` gates
*majors* at the handshake; nothing pins the byte layout *within* a major, so a silent field
reordering is a rolling-upgrade wire break that the current tests cannot see.

Muzzle pattern, ported verbatim as three parts (`frogdb-cluster/src/properties.rs:738/1361/1449`):
`known_defect(view, action) -> Option<&'static str>` filtering only shapes under an open ticket, one
`pinned_issue_NN_*` `#[should_panic]` witness per entry that goes red the day the fix lands, and a
single `the_muzzle_only_covers_the_pinned_shapes` test enumerating the near-misses so the muzzle
cannot quietly widen.

Budget: default case counts in the normal suite; `just replication-proptest CASES='200000'` for the
boosted pass, mirroring `cluster-proptest` (Justfile:174), called from the nightly.

### W3 — Model checking the protocols (stateright)

`stateright = "0.31"` is already a workspace dependency and `frogdb-cluster` already uses it; adding
it to `frogdb-replication` is one dev-dep line. The cost is not the dependency.

**Precondition, and the main risk of this campaign: the state machine needs a seam extracted from
the async runtime.** The cluster models work because of the snapshot-reload trick — every transition
loads a `ClusterSnapshot` back into a real `ClusterState`, calls production `apply_command`, and
keeps the resulting snapshot, so "the thing being checked is `commands.rs` itself"
(`frogdb-cluster/src/model/mod.rs:15-25`). Replication has the snapshot half (that is
`ReplicationView` from W1) and not the transition half: the decisions are spread across async
methods that own sockets, shard senders, timers and files —
`replica_session::run` (`replica_session.rs:642`), `ReplicaConnection::psync`
(`replica/connection.rs:224`), `consume_frames` (`apply.rs:361`).

W3's first deliverable is therefore a **decision/IO split**: extract the decision half of each
modelled transition into a pure `fn(&ReplicationView, Action) -> Outcome` and leave the async method
as the I/O half that calls it. This is not a novel shape for the area — it is the shape
`PartialSyncReplay::handle_partial_sync_request` (`replay.rs:350`) already has: `&ReplicationState`
plus a request plus the current offset in, a `ReplayDecision` out, no I/O. ADR 0004 already
established the analogous boundary one level up ("`frogdb-replication` owns the protocol and owns no
shards and no store, so every place the protocol has to touch this node's data is an injected
seam"); this pushes the same discipline down from *data* seams to *decision* seams. `begin_primary_stint`
is already synchronous and already has a transactional character, so it is the first candidate;
`AppliedOffset::{freeze, claim, land, admit_divergence}` are synchronous too and only entangled with
their atomics.

Models:

- **Model 1 — promotion racing an active stream.** Primary P feeding replica R, with frames in
  flight, an ack lag, and a promotion of R landing at every point in the interleaving; then the
  ex-primary demoted toward R and re-`PSYNC`ing. Safety: a granted `+CONTINUE` is never served over
  a hole in the promoted node's history; no two live replids claim the same offset range; a
  `+FULLRESYNC` adopts neither half of the granted pair before the payload installs
  (FM-REPLICATION-001's ordering clause). Liveness: every reconnect eventually resolves to exactly
  one arm. Scope variant with two would-be primaries covers the **two primaries feeding one
  replica** hunt — a surviving replica must not accept a stream from a stale primary after
  reattaching to the promoted one.
- **Model 2 — feed gate vs promotion.** A deadline-carrying feed hold (`ReplicaFeedGate::publish`)
  races a barrier release, a barrier re-arm, and a role change. Safety: the feed is never held past
  its deadline; a release belonging to an ended barrier never opens a feed a later barrier holds;
  a role change never leaves a hold with no owner. Liveness: every hold is eventually released.
  This models `8d55cc4f` / FM-CLUSTER-097 directly.

Both carry the cluster campaign's hard-won budgeting lesson: **breadth and depth do not compose.**
Two slots *with* retries never terminated inside 25 s / 7 GB, and the failover model's second
takeover was SIGKILLed by the OS memory manager after four minutes. Plan two exhaustive configs run
side by side per model, not one product. Port the same guardrails: `MIN_*_STATES` floors in the test
file so a scope edit cannot quietly shrink the space (`model/tests.rs:12-14`), `sometimes` vacuity
witnesses so a green run is not green because nothing happened, the `#[ignore]` split (bounded-depth
smoke <10 s per commit; full configs nightly), and the `[test-groups.model-check] max-threads = 1`
pin in `.config/nextest.toml:131-150` because the deep scopes hold multiple GB resident. Recipe:
widen `model-check`'s default `PATTERN` (Justfile:190) to include the two new models, or add a
sibling recipe (§8 D8).

Dropped without ceremony if the state space defeats the small-scope hypothesis — and, separately,
**stopped and re-decided if the seam extraction turns out to require restructuring
`replica_session.rs` (4574 LoC) wholesale** (§8 D2 sets the ceiling in advance so that call is not
made under sunk-cost pressure).

### W4 — Seeded fault schedules (deterministic simulation)

Extend the existing scheduler; do not build a second one.
`frogdb-server/crates/server/tests/simulation/scheduler.rs` already has `Schedule::from_seed` (:300)
deriving a whole run from one `u64` — family first, then fault kinds, endpoints, arm/heal instants,
link latency, per-node timer skew and the client workload — plus `RunOutcome::fingerprint`, a
self-expiring regression file (`simulation/cluster-regression-seeds.txt`, `EXPECTED-FAILURE:<issue>`
markers that fail the replay test the day the fix lands), and `just cluster-seeds SEEDS='500'`.

What is reusable is the derivation half (`from_seed`, `derive_faults`, `prune_concurrent_crashes`,
the fingerprint, the regression-file machinery). What is Raft-specific is `spawn_scheduled_hosts`
(:1059), `parse_cluster_nodes` (:1492) and `check_cross_node` (:725). W4 adds a **replication
topology arm** — one primary plus N replicas, no Raft — and four families beside the reusable
`Healthy` / `CrashRestart` / `Mixed`:

- **`LinkDrop`** — the primary↔replica edge held and healed at deliberately chosen partial-sync
  boundaries: inside the backlog window (expect `+CONTINUE`), just outside it (expect
  `+FULLRESYNC`), and straddling an eviction.
- **`PromotionMidStream`** — `REPLICAOF NO ONE` on a replica with frames in flight, then the
  ex-primary demoted toward it and re-syncing. The level-4 witness for revert (b).
- **`SlowReplica`** — one edge slowed past the lag-disconnect and `min-replicas-max-lag` thresholds
  so the self-fence and `min-replicas-to-write` arm and disarm under backpressure.
- **`FullSyncInterrupt`** — the link dies mid-payload, in both payload shapes (staged checkpoint and
  live dataset), including after the trailer but before the install.

Quiesce checking, three layers as in cluster: the catalog on every surviving node via W5's
`DEBUG REPLICATION CHECK`; the existing client-visible assertions (no acked-write loss, convergence);
and cross-node checks a single-node view cannot express — `XREPL-1` no acked write is absent from
the promoted node's keyspace, `XREPL-2` a replica's applied history is a prefix of the primary's,
`XREPL-3` `WAIT`'s answer never exceeded `connected_slaves` at any sample (which closes spec GAP-5
at level 4, where it belongs).

Recipe `just replication-seeds SEEDS='500'`, budget in the recipe default so CI does not duplicate
it, laptop-runnable as a plain `just` invocation; nightly via a generated
`replication_nightly.py` alongside `cluster_nightly.py` in
`.github/workflows/workflow_gen/src/workflow_gen/workflows/`.

**Dependency: cluster [issue
23](../cluster-correctness/issues/open/23-scheduler-fingerprint-is-load-dependent.md).** The
same-seed fingerprint diverged on a clean tree under host load, which means the determinism claim the
muzzle list rests on is not yet proven. A replication regression-seed list must not land before 23
closes, or a nightly failure may not reproduce locally. It is being fixed now; W4's sequencing
assumes that.

**Durability faults ride campaign 2's crash harness, not turmoil** — turmoil has no disk model.
Replica-crash-mid-sync and lose-unsynced-writes-on-kill go through `CrashTestHarness`
(`frogdb-server/crates/core/src/persistence/test_harness.rs`, made to actually discard unsynced
writes in `7b7efcf8`). This PRD consumes that machinery; it does not duplicate it, and it does not
own it.

### W5 — Live invariant surface + Jepsen integration

- **`DEBUG REPLICATION CHECK`** — mirror of `DEBUG CLUSTER CHECK`
  (`frogdb-server/crates/server/src/connection/debug_conn_command.rs:226`, `:302`,
  `format_cluster_check_response` at `:819`; seam `DebugProvider::cluster_check` in
  `frogdb-core/src/conn_command.rs:603`). RESP array of `{id, detail}` maps, empty array = clean
  (not a sentinel string — Jepsen's "clean" test reads emptiness), ADMIN-flag gated like every
  sibling DEBUG subcommand, **always compiled** because Jepsen runs release binaries. One deliberate
  difference from the cluster command: it answers in *every* mode. `DEBUG CLUSTER CHECK` errors in
  standalone because an empty array would misread as "clean"; a node with no replication link still
  has a replid, an offset triple and a persisted state that can be wrong, so there is no
  not-applicable case to distinguish.
- **Jepsen**: `testing/jepsen/frogdb/src/jepsen/frogdb/invariant.clj` already implements the whole
  quiesce+final pattern as a nemesis wrapper — `wrap-nemesis` intercepts one `:f` and delegates
  everything else, so it composes with any nemesis package with zero per-workload changes, and
  `core.clj:268/276/295/307` wires it for `cluster-mode?`. W5 parameterizes the command and the
  result key and extends the wiring to `Topology.REPLICATION`, which covers `replication`, `lag`,
  `zombie`, `split-brain`, `register-partition`, `replication-chaos`, `replication-failover`,
  `replication-failover-chain` and `replication-clock-skew` (`testing/jepsen/run.py:256-380`) in one
  change. Carry over the counting bug that validation found there: every check op appears twice in
  the history (dispatch + completion), so the checker must filter on `(map? (:value op))`, not on
  `:f` alone.

### W6 — Spec and gate integration

- **Cross-reference**: each catalog invariant cites the FM rows it generalizes; rows whose invariant
  is now universally checked note the id. Close the spec's two live GAPs while in the file — GAP-5
  becomes INV-SESSION-2 plus `XREPL-3`, and GAP-6 (`-UNBLOCKED` vs `CLIENT UNBLOCK` racing a
  demotion, `connection/blocking.rs:285-305`) is a point test no layer here reaches, so write it.
- **The dangling-INV lint is only half generic, and this is where that gets fixed.**
  `scripts/failure-modes.py` already globs *every* `*-failure-modes.md` for `INV-*` citations
  (`INV_REF_RE` at :107, `check_invariant_vocabulary` at :306) — but the vocabulary it checks against
  is one hard-coded file, `INVARIANTS_RS = REPO / "frogdb-server/crates/cluster/src/invariants.rs"`
  (:52), loaded once (`load_catalog_ids`, :274). As it stands, an `INV-REPLID-2` cited in
  `replication-failure-modes.md` is flagged as dangling. W6 turns `INVARIANTS_RS` into a per-area
  catalog map so each spec is checked against **its own** area's catalog — a replication row citing
  `INV-HANDOFF-1` should be an error, not a pass. Small change, same script, and it is the piece that
  makes the third and fourth ports free.
- **Mutation re-baseline**: `just mutants frogdb-replication` + `just mutants-gate
  frogdb-replication 0.85`, same for `frogdb-replication-runtime`, on current code. Record not just
  the score but the **in-crate share of forcing tests**, which is the number ADR 0004 says the score
  is really measuring. W2's R6 and the runtime crate's first `[dev-dependencies]` are the targeted
  fix for the 50%-era survivors (`apply_single`, `apply_transaction`, `apply_group`,
  `export_live_dataset`, `install`, `read_snapshot`).
- Run `just mutants-diff` on both crates before every push that touches them, per the locked-area
  push discipline.

## 4. Relationship to other campaigns

**Cluster correctness** — this is its exit follow-on, executing its §8 D5 ruling that the pattern
ports per area "against its own state shape rather than through a shared framework". Reused,
concretely: the seeded scheduler's derivation half and its regression/muzzle machinery (W4), the
`invariant.clj` nemesis-wrapper design (W5), the three-part `known_defect` muzzle convention (W2),
the `Violation`/`Citation`/`Tier` vocabulary and the compile-time citation trick (W1), and the
retro-validation gate design from [issue
13](../cluster-correctness/issues/done/13-retro-validation-gate.md) (§6). Not reused: any shared
abstraction over the two areas — explicitly ruled out. Its open defect rulings 14–20 are a parallel
track and not a dependency; [issue
23](../cluster-correctness/issues/open/23-scheduler-fingerprint-is-load-dependent.md) is the one
exception and blocks W4's regression list.

**Hardening campaign 2** — its W2 crash harness is the **durability-fault layer, and it belongs to
the persistence follow-on, not this campaign.** Replication's only claim on it is consumption: W4's
replica-crash-mid-sync schedules need process kills that lose unsynced writes, and turmoil cannot
provide them. Campaign 2's seam gates stay repo-wide and stay campaign 2's: note that
`just lint-durable-ack` is the layer that caught FM-CLUSTER-098 in the cluster retro-validation, and
it is the analogous catcher for any replication ack-durability defect — a reminder that not every
catch has to come from this PRD's five layers, and the gate only counts once its allowlist is empty.

**replication-cluster rework** — shipped; two of the five retro-validation reverts came out of it,
and its `promotion-replid-psync.md` is the design record behind INV-REPLID-2.

**Persistence correctness** — the next port after this one (§7). It will want the per-area catalog
map W6 builds, so W6 is deliberately generalized rather than special-cased.

## 5. Sequencing

1. **W1** — `ReplicationView` + catalog + hooks + forcing tests. Unlocks everything: the projection
   is a precondition for W2's assertions, W3's model state, W4's quiesce check and W5's command. It
   also immediately upgrades ~520 existing tests.
2. **W5's `DEBUG REPLICATION CHECK`** — small once the view exists, and W4 consumes it. Land it
   inside W1's tail or W4's first PR, not as a separate late workstream.
3. **W2** — highest defect-catching density per line. R2 and R3 retro-cover two revert classes by
   construction; R6 is the cheapest available fix for the runtime crate's mutation floor.
4. **W6** — mutation re-baseline and the lint generalization; cheap once W1/W2 are in-crate, and
   re-baselines the gate honestly rather than inheriting a number from before FM-REPLICATION-054..064.
5. **W4** — seeded replication schedules, plus the Jepsen half of W5. Gated on cluster issue 23 for
   the regression list (the sweep itself can land earlier).
6. **W3** — last, because it carries the decision/IO refactor and the most novelty. Scoped to the two
   named models; dropped, or its refactor halted, per §8 D2.

Retro-validation runs incrementally, not only at the end: after each workstream, re-run the revert
set and record which layers newly catch which defect. That is how the cluster campaign discovered
that two of its five were structurally out of reach in time to build layers for them.

## 6. Exit criteria

1. Catalog exists in `frogdb-replication`; every HARD invariant has a forcing test that fails when
   its check is deleted; every `DocumentedException` cites an FM row or issue (compile-enforced);
   every invariant declares the view fields it requires.
2. Hooks active at all seams named in §3 W1 in test/debug builds; full suite green under them, or
   each violation triaged into a fix or a cited exception — no third bucket.
3. R1–R6 and the frozen frame fixtures land in-crate; `just replication-proptest` exists and the
   boosted pass is wired to the nightly.
4. Mutation gates re-run on current code for both crates and passing at 0.85, with the in-crate share
   of forcing tests recorded alongside the score.
5. `just replication-seeds` exists, laptop-runnable, budget in one place; nightly wired; a
   regression-seed file exists with self-expiring muzzles and replays in the default suite (this
   criterion is blocked on cluster issue 23).
6. `DEBUG REPLICATION CHECK` shipped and documented; the Jepsen invariant wrapper sweeps it at
   quiesce and final on every `Topology.REPLICATION` workload, proven by one seeded-violation run and
   one clean run.
7. Stateright: both models shipped with recorded state-space sizes, `MIN_*_STATES` floors, safety +
   liveness + vacuity properties — **or** a written decision recording why not, naming the budget
   that was tried and how far the seam extraction got.
8. **Retro-validation gate**: each defect below is reverted one at a time in a throwaway working tree
   (never committed), the layers are run, and the verdict is recorded in a §6.1 table with the same
   columns the cluster campaign used. **At least one *non-forcing* layer must catch each.** The fix's
   own FM forcing tests are excluded from the verdict — counting them makes the gate vacuous, and
   "the forcing test was the only thing that failed" is precisely the finding the gate exists to
   surface. A miss is filed as an issue and the gap closed with a new layer before exit; cluster
   issues 21 and 22 are the precedent for what "closed" means.

Revert set (commits verified against `git log` in this worktree):

| # | defect | fix commit | date | status | expected catcher |
|---|---|---|---|---|---|
| a | the full-resync checkpoint was cut before the shard WALs drained, so acked writes were missing from the payload | `ebdf7d9e` | 2026-07-30 | **verified** | W4 `FullSyncInterrupt`/`LinkDrop` acked-write-loss check; possibly campaign 2's crash harness |
| b | promotion minted no replid and adopted no secondary window; a promoted node served no `PSYNC` | `f6484219` | 2026-07-28 | **verified** | W1 INV-REPLID-2 via the `begin_primary_stint` hook; W3 model 1; W4 `PromotionMidStream` |
| c | `WAIT` counted a replica from its seeded resume position instead of a wire ACK | `90fefaf7` | 2026-08-05 | **verified** | W1 INV-OFFSET-3; W2 R1; W4 `XREPL-3` |
| d | the replica feed ran while a slot-handoff barrier was armed | `8d55cc4f` | 2026-08-08 | **verified** | W1 INV-GATE-1; W3 model 2 |
| e | one FM-REPLICATION forcing-test pick from the LOCKED spec, chosen at gate time rather than named here, so the layers cannot be built toward it | n/a | — | selection method, not a commit | any |

Candidates held in reserve if §8 D4 raises N above 5: `86a016fd` (frame-size wedge, backlog-eviction
hole, early replid adoption — three defects in one commit, revertable individually), `85fc3095` (the
five link-machinery bugs: solicited ACK, frame epochs, ack-on-apply, backlog TTL, divergence latch),
`4ced6229` (checkpoint path traversal, unbounded replicated `MULTI`, a lag window that round-trips to
"off"), `1ea25181` (a persistence-disabled primary shipping no dataset — FM-REPLICATION-001's
originating bug).

### 6.1 Retro-validation results (2026-08-11)

Run under [issue 15](issues/open/15-retro-validation-gate.md). Each defect was reinstated as a
minimal inverse patch at the current decision seam — never a literal `git revert`, because the four
named commits long predate the restructures around them — in a throwaway tree, never committed. The
layer stack was then run and the failures attributed. **The fix's own tests are excluded from every
verdict**: both the FM rows' named forcing tests and any regression test that shipped in the fix
commit itself (`git log -S` on each failing test name settles which). A defect is CAUGHT only if
something else went red.

| defect | revert | L1 catalog + `DEBUG REPLICATION CHECK` | L2 R1–R6 | L3 stateright | L4 seeded schedules | L6 integration | verdict |
|---|---|---|---|---|---|---|---|
| a — checkpoint cut before the shard WALs drained | `view()` stops reporting `pre_checkpoint_drain`, so `DrainBeforeCheckpoint` never runs | green | green | n/a | green at 7 seeds **and at 500** (238 s) | only the fix's own tests | **MISS** → [issue 25](issues/done/25-no-layer-sees-what-a-full-resync-payload-contains.md), closed below |
| b — promotion minted no replid, adopted no window | `plan_primary_stint` ignores `minted_id` and shifts nothing | red (`INV-REPLID-2` via the promotion hook) | red — R1, R2, R3, R4 | red — promotion model smoke, `a_failed_promotion_strands_the_node` | red — smoke sweep aborts | red — 28 tests, incl. PSYNC2 failover and identity-across-restart | **CAUGHT**, by every layer |
| c — `WAIT` counted a seeded resume position | `acked_offset()` folds in `resume_offset()` again | green | red — R1, R2, R3, R4 | green | green | green | **CAUGHT**, by W2 alone |
| d-i — feed ran during an armed barrier (derivation) | `decide_feed_hold_until` returns `None` | n/a | green | red — feed-gate model smoke + all three replay cases | green | green | **CAUGHT**, by W3 alone |
| d-ii — same defect at the consumer | the session stops awaiting `feed_gate.released()` / `is_held()` | green | green | green | green | green | **MISS** → [issue 26](issues/open/26-the-feed-gate-model-proves-a-transcription-not-the-session.md) |
| e — FM-REPLICATION-063, drawn at gate time | `NetByteCounters::record_{output,input}` become no-ops | green | green | n/a | green | green | **MISS** → [issue 27](issues/done/27-nothing-but-its-own-tests-watches-the-replication-byte-counters.md), closed below |

`n/a` above always means *structurally* out of reach, never "not bothered": `ReplicationView`
carries offsets, ids, phases and gate state, so a defect about payload bytes (a), byte tallies (e)
or a pure derivation the catalog never projects (d-i) cannot be stated by that layer at all.

**The (e) draw.** The fifth sample is drawn, not chosen, so it cannot be a defect the layers were
built around. The draw was fixed before any candidate was read: branch point `2a81867e`, low six
bits (`0x7e` = 62), 0-based into FM-REPLICATION-001..064 → **FM-REPLICATION-063**. It landed on an
observability row, which is exactly the kind of member a hand-picked fifth would have skipped.

**First-pass tally: 3 of 5 defects caught (b, c, d).** §8 D4's escalation to N = 8 is **not taken**,
and the reason is the result rather than the cost: the trigger was "≥ 4/5 caught cheaply", meaning
the layer stack is strong enough that a wider sample is the informative next move. At 3/5 the
informative move is the opposite one — the three misses already name three distinct structural
blind spots (payload contents, model-versus-tree drift, observability), and eight more reverts
would keep re-deriving the same three. The reserve list (`86a016fd`, `85fc3095`, `4ced6229`,
`1ea25181`) stays as written for a re-run after issue 26's layer lands.

**Two cross-cutting conclusions.**

*What the layers see is what `ReplicationView` carries.* Every miss is a fact outside the
projection — what a payload contains, how many bytes crossed a socket. The projection is the
campaign's central asset and its central limit, and the only cure is a layer that reads the real
node rather than its view. That is why both closing layers are turmoil scenarios rather than new
invariants: the sweep is the one place the campaign already talks to real servers.

*A model proves what it transcribes.* (d-i) and (d-ii) are the same defect at two placements, and
the stateright model catches the one whose decision function it calls while missing the one whose
control flow it copies. Whenever a model re-implements a loop rather than driving it, the copy can
go stale silently — recorded as [issue 26](issues/open/26-the-feed-gate-model-proves-a-transcription-not-the-session.md),
which blocks issue 15's own exit.

**Layer built to close the misses.** `simulation::full_sync_payload::a_full_resync_carries_writes_still_sitting_in_the_batch_window`
(`frogdb-server/crates/server/tests/simulation/full_sync_payload.rs`): two real servers under
turmoil, persistence on, and a batch-flush window wider than the whole simulation, so every write
the primary acks is still un-flushed when the replica attaches for a `PSYNC ? -1`. The drain ahead
of the cut is then the only thing that can put those writes in the payload. This required one
harness knob — `ReplicationNodeParams::batch_timeout_ms`, defaulted to the shipped 10 ms so the
existing sweep is unchanged (re-verified 32/32). The same run asserts the transfer moved both
replication byte counters, which closes (e). Both claims are proven non-vacuous by re-applying their
inverse patches: revert (a) turns the readback red, revert (e) turns the counter assertion red.

## 7. Out of scope

- **The persistence-correctness campaign** — the next port, and the owner of campaign 2's crash
  harness as a validation layer. This PRD consumes that harness for one fault family and specifies
  nothing about it.
- **Cluster defect rulings 14–20** — a parallel track on the cluster campaign. Cluster issue 23 is a
  *dependency* of W4, not work owned here.
- **Data-plane value verification beyond what the Jepsen replication workloads already check** — elle
  and Knossos stay exactly as they are; W5 adds an invariant sweep beside them, not a new consistency
  model.
- **A shared cross-area validation framework** — explicitly ruled out. Each area ports the pattern
  against its own state shape. The only thing that generalizes is the lint's catalog map (W6), and
  that is a dict, not a framework.
- **Fixing individual defects the layers find at build time** — they get filed and ride their own
  issues, as cluster's 14/15/17/18/19 did. Muzzled in W2, pinned by a `#[should_panic]` witness,
  never silently skipped.
- **Performance of the replication path** — catalog checks are `cfg(any(test, debug_assertions))`
  plus one always-compiled DEBUG command that costs nothing until it is called.

## 8. Decisions (all RULED 2026-08-10)

**D1 — inherit the cluster campaign's rulings unless overridden.** Cluster's §8 ruled: adopt proptest
and stateright with real budgets nightly and bounded smoke per commit; fix behavior rather than
register exceptions when a catalog rejects a reachable state; ship the DEBUG check always-compiled
behind the existing ADMIN/DEBUG surface; keep the heavy-sweep budget in one Justfile variable and
laptop-runnable; put the catalog in the crate the mutation gate sees.
*Recommendation:* **inherit all five as written**, with the concrete numbers proposed in §3 —
`just replication-seeds SEEDS='500'`, `just replication-proptest CASES='200000'` (both matching their
cluster twins), per-commit model smoke under 10 s, nightly full model configs pinned one-at-a-time.
Ruling on this single item settles the four inherited questions so §8 only carries what is new.
*Ruled 2026-08-10:* **inherit all five as written**, with the concrete numbers above.

**D2 — how invasive a refactor of the replication runtime is acceptable to make the state machine
model-checkable (W3).** The models need a pure decision function; today the decisions live inside
`replica_session::run` (`replica_session.rs:642`, in a 4574-line file), `ReplicaConnection::psync`
(`replica/connection.rs:224`) and `consume_frames` (`apply.rs:361`). Three tiers, cheapest first:
(i) **extract only what the two models need** — the promotion transition around `begin_primary_stint`
and the feed-gate transition — leaving the session loop untouched; (ii) **also split the PSYNC arm
selection** out of `ReplicaConnection::psync` into a pure function beside
`handle_partial_sync_request`, which is a genuine symmetry win (the primary side already has it, the
replica side does not); (iii) **restructure `replica_session.rs` into an explicit phase state machine**
with a pure `step(view, event) -> (phase, effects)` and the async loop as an interpreter.
*Recommendation:* **authorize (i) + (ii); require an explicit re-decision before (iii).** (ii) is the
one piece that is good architecture independent of stateright — it deletes the asymmetry ADR 0004
implicitly created — and it is bounded to one function. (iii) is the interpreter pattern the whole
area would eventually benefit from, but doing it under a validation campaign's schedule is how
locked-area refactors go wrong; if W3 shows it is required, that is a finding worth its own PRD.
*Ruled 2026-08-10:* **full restructure (iii) authorized** — `replica_session.rs` becomes an explicit
phase state machine with a pure `step(view, event) -> (phase, effects)` and the async loop as an
interpreter. Execution discipline for a locked-crate restructure of this size: land (i) and (ii)
first as stepping stones, then (iii) as its own issue chain; every step spec-first against
`replication-failure-modes.md` (rows may move file:line citations but not meaning); the full
mutation gate (0.85) re-runs after (iii), not just `mutants-diff`, because the restructure moves
most forcing-test targets.

**D3 — are WAIT and durability semantics in scope, or deferred?** `WAIT` is a replication surface
(FM-REPLICATION-037..040, `wait_coordinator.rs`) and one revert candidate is a `WAIT` bug — but the
*meaning* of a `WAIT` ack in cluster mode is entangled with the txn barrier work (hardening campaign 2's
acked-after-commit acceptance, the `ReplicaFeedGate` node-wide deadline-hold, rework issue 12) and with
the persistence campaign's durability definitions.
*Recommendation:* **in scope as a mechanism, out of scope as a semantic.** Model and check what `WAIT`
*does* — INV-OFFSET-3, `XREPL-3`, R1, revert (c) — and treat the cross-area question "what should a
counted ack guarantee about durability" as owned by the persistence campaign. If W2 or W4 surfaces a
case where the mechanism is right and the semantic is wrong, file it against that campaign rather than
re-litigating it here.
*Ruled 2026-08-10:* **mechanism in scope; durability semantics deferred to the persistence
campaign**, which owns "what a counted ack guarantees about durability."

**D4 — retro-validation revert-set size.** Cluster used N=5 (the five audit defects, which were simply
all of them). Four replication reverts are verified above; the fifth is a spec forcing-test pick, and
§6 lists four more commits in reserve.
*Recommendation:* **N=5 as listed — four verified commits plus one spec pick chosen at gate time.**
The spec pick matters more than a fifth known commit: the four are all defects the campaign is
explicitly designing layers for, which biases the gate toward what it already expects to catch. A row
drawn from the 64 LOCKED rows at gate time is the only member of the set the layers were not built
toward. If the first pass comes back ≥4/5 cheaply, raise N to 8 using the reserve list before
declaring exit rather than after.
*Ruled 2026-08-10:* **N=5 — four verified commits plus one spec pick at gate time; escalate to 8
from the reserve list before declaring exit if the first pass is ≥4/5 cheaply.**

**D5 — does the proptest generate over single-primary-single-replica only, or include chained and
multi-replica topologies from day one?** Chained replication is a *documented non-guarantee* today
(testing-improvements issue 48 — chaining is deliberately rejected), and the cluster campaign's
equivalent decision (INV-REF-3B, chained replicas) became its only `DocumentedException` and its
[issue 14](../cluster-correctness/issues/open/14-role-transitions-admit-malformed-parents.md).
Meanwhile `replication-failover-chain` is a live Jepsen workload, so chains are exercised in practice.
*Recommendation:* **multi-replica from day one; chains behind a generator flag, off by default.**
Multi-replica is where INV-SESSION-2, INV-OFFSET-3 and the whole `WAIT`-counting class live, and a
single-replica generator cannot express any of them — that is most of the value. Chains are a
non-guarantee whose *shape* nonetheless ships (the failover-chain workload), so generating them by
default would produce violations that are neither defects nor exceptions. The flag makes it a
one-line experiment when someone wants to know what chaining actually does, and gives INV-ROLE-1 a
place to be tested.
*Ruled 2026-08-10:* **multi-replica (bounded ≤3) from day one; chained topologies behind a
generator flag, off by default.**

**D6 — where do `Violation`, `Citation` and `Tier` live?** They are defined in
`frogdb-cluster/src/invariants.rs:80-148` and re-exported through `frogdb-core`
(`frogdb-core/src/lib.rs:63`), which is how `DEBUG CLUSTER CHECK` and the Jepsen path see them.
`frogdb-replication` does **not** depend on `frogdb-cluster` and must not start.
*Recommendation:* **move the three types to `frogdb-types`** (which both crates already depend on) and
re-export from both catalogs, keeping `frogdb_core::Violation` working so nothing downstream moves.
The alternative — a duplicate `Violation` in `frogdb-replication` — is three lines of struct and
avoids touching a locked crate, but it means the Jepsen checker, the DEBUG formatter and the scheduler's
violation rendering all need two code paths for one concept. This is not the shared *framework* D5-of-cluster
ruled out; it is one 4-field struct and a `const fn` assert, and keeping it single is what makes the
per-area catalogs report in one voice.
*Ruled 2026-08-10:* **move to `frogdb-types`**, re-export from both catalogs, keep
`frogdb_core::Violation` working.

**D7 — where does `ReplicationView` live, given that three of its inputs are outside
`frogdb-replication`?** The catalog belongs in `frogdb-replication` (mutation gate, per cluster's
ruling), but `ReplicationQuorumChecker` is in `frogdb-replication-runtime` (`quorum.rs:52`),
`RoleManager` is in the server crate (`role_manager.rs:119`), and `ReplicaFeedGate` — though defined in
`frogdb-replication` — is *owned* by `frogdb-core`'s client registry (`client_registry/mod.rs:546`).
*Recommendation:* **`ReplicationView` and the catalog live in `frogdb-replication`; the fields owned
elsewhere are `Option<T>` filled by the caller.** An invariant declares its required fields and is
skipped when they are absent, so `frogdb-replication`'s own tests check what they can see, the runtime
crate's tests fill the fence fields, and only `DEBUG REPLICATION CHECK` and the W4 quiesce path
assemble a complete view. The rejected alternative — a `ReplicationInvariants` trait implemented by the
server so the view is always complete — puts the catalog behind a dyn boundary the mutation gate cannot
follow, which is the exact ceremony finding campaign 2 recorded. The cost of the recommendation is
honest and worth stating: INV-FENCE-1 and INV-ROLE-1 will be checked less often than the rest.
*Ruled 2026-08-10:* **as recommended** — view + catalog in `frogdb-replication`, foreign fields
`Option<T>` filled by the caller, invariants declare required fields.

**D8 — extend `simulation/scheduler.rs` and `model-check` in place, or fork siblings?** The scheduler
is 2500+ lines with Raft topology assumptions threaded through `spawn_scheduled_hosts` and
`check_cross_node`; `model-check`'s `PATTERN` default is `(handoff|failover)_model_full`.
*Recommendation:* **extract the topology-agnostic derivation into a shared `simulation/schedule.rs`
and put the replication arm in `simulation/replication_scheduler.rs`; add a sibling
`just replication-model-check` rather than widening `model-check`'s pattern.** The seed derivation,
fingerprint and regression-file machinery genuinely want to be shared (that is not a cross-area
framework — it is one test module in one crate); the host spawning and cross-node checks genuinely
differ. Separate recipes keep the two nightly budgets independently tunable and let a cluster model
regression be triaged without a replication rebuild — the same reason `cluster-nightly` and
`cluster-model-nightly` are two workflows.
*Ruled 2026-08-10:* **extract shared `simulation/schedule.rs`** (seed→schedule derivation with
per-topology `Family`/budget/op-vocabulary supplied by each arm, fingerprint + regression/muzzle
machinery, `parse_check_entry`/`hard_violations` over `frogdb_types::Violation`, generic fault
application — ≈1100 of 2650 lines); replication arm in `simulation/replication_scheduler.rs`; the
~50-line `run_seed` driver shape is duplicated per arm, not genericized; separate
`just replication-seeds` / `replication-model-check` recipes.

**D9 — does W4's replication sweep block on cluster issue 23, or land muzzle-free ahead of it?**
Issue 23 (same-seed fingerprint diverges under host load) is `needs-triage` with nothing started.
*Recommendation:* **land the sweep early, hold the regression file.** A sweep that runs seeds and
fails loudly is useful immediately and will itself help localize 23 (a second topology that does *not*
reproduce the divergence is evidence about where the real-time leak is). What must wait is committing
`EXPECTED-FAILURE` muzzles, because a muzzle is a claim about reproducibility. Concretely: exit
criterion 5 stays blocked on 23; the rest of W4 does not.
*Ruled 2026-08-10:* **land the sweep early, hold the regression file** — exit criterion 5 alone
blocks on cluster issue 23.
