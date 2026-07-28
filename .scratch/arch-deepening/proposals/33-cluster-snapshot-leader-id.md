# Proposal 33 — Delete `ClusterSnapshot.leader_id`; read the leader from Raft metrics at the debug seam

## Summary

`ClusterSnapshot` carries a `leader_id: Option<NodeId>` field that is **permanently `None`**. It is
written in exactly one place — the `ClusterState::snapshot()` DTO builder (`state.rs:98`), hard-coded
to `None` with the comment `// Will be set by caller` — and **no caller ever sets it** (verified by
grep). Its sole production reader, the debug UI provider (`debug_providers.rs:107`), copies the
`None` straight through to the debug web UI, which therefore **always renders the cluster leader as
"Unknown"** (`handlers.rs:590-593`), even though the real leader is trivially available elsewhere via
`raft.metrics().borrow().current_leader` (`commands/cluster/mod.rs:225`).

The field is architecturally un-fillable at its current location: the leader is **Raft node runtime
state**, and `ClusterState::snapshot()` is a method on the applied **state machine** — the state
machine lives *inside* the Raft node and cannot reach back up to it. This proposal **deletes the
field** and sources the leader from Raft metrics at the one seam that displays it (the
`ServerDebugProvider`, which already has an `Arc<ClusterRaft>` available at its construction site).
The result: the misleading always-`None` field disappears, the debug UI shows the real leader, and a
provider-level test pins "reported leader == Raft metrics."

## Problem

`ClusterSnapshot` (`types.rs:478-495`) is the in-memory read DTO that `ClusterState::snapshot()`
produces for every read-only consumer of cluster topology (CLUSTER INFO, slot routing, the failure
detector, the debug UI, etc. — 27 non-test call sites). One of its fields, `leader_id`, describes a fact the
DTO builder cannot know:

- The leader is a property of the **Raft Metadata Plane** node (`ClusterRaft = Raft<TypeConfig>`),
  exposed through `Raft::metrics().current_leader`.
- `snapshot()` reads only `self.inner` — the applied `ClusterStateInner` (nodes, slot_assignment,
  Config Epoch, migrations, membership, active_version). `ClusterStateInner` has **no leader field**
  (`state.rs:29-48`), because the leader is not replicated metadata; it is local Raft runtime state.
- So `snapshot()` sets `leader_id: None` and defers to "the caller." That contract was never
  honoured: **zero callers patch the field**. The value is a constant `None` dressed up as data.

Downstream, this is not merely dead surface — it produces **misleading observability**, which the
project's accuracy rule explicitly forbids:

- `ServerDebugProvider::cluster_overview()` copies `snapshot.leader_id` into the debug crate's
  `ClusterOverviewSnapshot.leader_id` (`debug_providers.rs:107`).
- The debug web UI renders it as `overview.leader_id.map(|id| format!("Node {id}")).
  unwrap_or_else(|| "Unknown")` (`handlers.rs:590-593`). Because the source is always `None`, the
  **Cluster Overview panel reports the leader as "Unknown" on every node, in every cluster, always** —
  including on the leader itself, mid-healthy-cluster.

Meanwhile the real leader is one `borrow()` away and is already read for CLUSTER INFO health
(`commands/cluster/mod.rs:221-225`, `metrics.current_leader`) and by the test harness
(`cluster_harness.rs:882`, `raft.metrics().borrow().current_leader`). The debug seam simply reads the
wrong source.

This is a **shallow, mis-located field**. The DTO's Interface advertises a fact its Implementation
cannot supply; the "set by caller" comment pushes the real work onto consumers, none of whom do it —
the classic failure mode of a field whose owner is not where the data lives. Deleting it and reading
the leader at the seam that has the Raft handle restores **locality**: the leader is read from the
one object that owns it, at the one place that displays it.

## Evidence (verified file:line)

All paths under `frogdb-server/crates/`. Every claim below was opened and confirmed.

**The field and its sole writer:**

- `cluster/src/types.rs:491` — `pub leader_id: Option<NodeId>,` on `ClusterSnapshot`
  (`types.rs:479-495`), doc-commented "The Raft leader node ID (if known)."
- `cluster/src/state.rs:93-100` — the **only** construction of the field, in the **only** builder of a
  populated `ClusterSnapshot` (`ClusterState::snapshot()`):
  ```rust
  ClusterSnapshot {
      nodes: inner.nodes.clone(),
      slot_assignment: inner.slot_assignment.clone(),
      config_epoch: inner.config_epoch,
      migrations: inner.migrations.clone(),
      leader_id: None, // Will be set by caller
      active_version: inner.active_version.clone(),
  }
  ```
- **No caller sets it.** A workspace grep for `leader_id` on `ClusterSnapshot`/`ClusterOverviewSnapshot`
  shows exactly one write (`state.rs:98`, = `None`) and exactly one read
  (`debug_providers.rs:107`). The many other `leader_id` hits are an **unrelated** field on the
  `ForwardToLeader` redirect type (`connection/cluster.rs:101,124,137`, `slot_migration/mod.rs:140,149,160`,
  `cluster_init.rs:397,466`) and openraft's `Vote::leader_id()` (`storage.rs:432`) — different types,
  not this field.

**The consumer chain (always-None → "Unknown"):**

- `server/src/debug_providers.rs:107` — `leader_id: snapshot.leader_id,` copies the `None` into the
  debug DTO.
- `debug/src/web_ui/state.rs:235` — `pub leader_id: Option<u64>` on `ClusterOverviewSnapshot`
  (this is the debug crate's own DTO; server implements the provider and fills it).
- `debug/src/web_ui/handlers.rs:590-593` — renders `"Node {id}"` or `"Unknown"`; with the source
  fixed at `None`, always `"Unknown"`.

**The real leader source, already used elsewhere:**

- `server/src/commands/cluster/mod.rs:221-225` — CLUSTER INFO reads `metrics.current_leader` from
  `raft.metrics().borrow()` for health classification. (Candidate cited line 224; the field read is
  at 225 — off-by-one, corrected here.)
- `test-harness/src/cluster_harness.rs:882` — `raft.metrics().borrow().current_leader`.

**The Raft handle is available at the fix site:**

- `server/src/server/mod.rs:153` — the `Server` holds `raft: Option<Arc<ClusterRaft>>`.
- `server/src/server/subsystems.rs:116-123` — `ServerDebugProvider::new(...)` is constructed from
  `self`, where `self.raft` is in scope. The provider (`debug_providers.rs:23-33`) does **not**
  currently receive it — it takes `client_registry`, `cluster_state`, `self_node_id`,
  `replication_tracker`, `mode`, `role_manager`. Adding the leader source is a one-argument change at
  one construction site.

**Deletion is format-safe (enrichment beyond the candidate):**

- The persisted / `InstallSnapshot` wire form of the Raft Metadata Plane is `ClusterStateInner`, not
  `ClusterSnapshot`: `build_snapshot` and `get_current_snapshot` serialize `&*inner`
  (`state.rs:450, 477`), and `install_snapshot` deserializes `ClusterStateInner`
  (`state.rs:421`). `ClusterStateInner` has **no `leader_id`** (`state.rs:29-48`).
- `ClusterSnapshot`'s own `Serialize`/`Deserialize` derives and the `from_snapshot` constructor are
  effectively vestigial: `from_snapshot` has **zero callers** (`grep` finds only a doc reference at
  `state.rs:85`), and no code serializes a `ClusterSnapshot` to JSON/bytes. So removing `leader_id`
  from the DTO cannot affect any persisted state, Raft snapshot, or wire message.

**No test pins the current behavior.** No test asserts `snapshot.leader_id == None` or reads it. The
`leader_id` locals throughout `integration_cluster.rs` / `integration_pubsub.rs` come from
`harness.get_leader()` (Raft-metrics-backed), unrelated to the DTO field.

## Proposed design

Delete the field; give the debug provider a leader source read from Raft metrics. Signatures/types
only.

### 1. `cluster/src/types.rs` — remove the un-fillable field

```rust
pub struct ClusterSnapshot {
    pub nodes: BTreeMap<NodeId, NodeInfo>,
    pub slot_assignment: BTreeMap<u16, NodeId>,
    pub config_epoch: ConfigEpoch,
    pub migrations: BTreeMap<u16, SlotMigration>,
    // leader_id removed — the leader is Raft runtime state, not replicated metadata,
    // and this DTO's builder cannot supply it.
    #[serde(default)]
    pub active_version: Option<String>,
}
```

`ClusterState::snapshot()` (`state.rs:93-100`) drops the `leader_id: None` line. `Default` and the
serde derives continue to work unchanged.

### 2. `server/src/debug_providers.rs` — inject a leader source at the seam that owns the Raft handle

Give `ServerDebugProvider` a leader reader and fill the debug DTO from it. Keep the debug crate's
`ClusterOverviewSnapshot.leader_id` field — the debug UI *should* display a leader; only its **data
source** changes.

```rust
pub struct ServerDebugProvider {
    client_registry: Arc<ClientRegistry>,
    cluster_state: Option<Arc<ClusterState>>,
    self_node_id: Option<u64>,
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    mode: LiveMode,
    role_manager: RoleManagerHandle,
    raft: Option<Arc<ClusterRaft>>,   // new: the live leader source
}

impl ServerDebugProvider {
    pub fn new(
        client_registry: Arc<ClientRegistry>,
        cluster_state: Option<Arc<ClusterState>>,
        self_node_id: Option<u64>,
        replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
        mode: LiveMode,
        role_manager: RoleManagerHandle,
        raft: Option<Arc<ClusterRaft>>,   // new
    ) -> Self { /* ... */ }

    /// The current Raft leader, read live from metrics. `None` when standalone or
    /// no leader is currently known (election in progress) — the honest value.
    fn current_leader(&self) -> Option<u64> {
        self.raft.as_ref()?.metrics().borrow().current_leader
    }
}
```

In `cluster_overview()` (`debug_providers.rs:100-110`), replace `leader_id: snapshot.leader_id,`
with `leader_id: self.current_leader(),`.

The seam is honest by construction: `None` now means "no leader known right now" (standalone, or a
live election), not "this field is never populated." The debug UI's existing `"Unknown"` fallback
(`handlers.rs:593`) becomes *correct* rather than *permanent*.

### Why read-at-seam, not a `snapshot_with_leader(raft)` constructor

The candidate floated a `ClusterState::snapshot_with_leader(raft)` alternative. Rejected: it would
force the leader read into the `cluster` crate's DTO builder, which 27 read-only call sites use
(routing, failure detector, CLUSTER INFO, admin handlers), **none of which want the leader**. It also
threads the Raft node handle back *into* a method on the state machine the Raft node owns — an
inverted, awkward coupling. Reading at the single display seam keeps **locality** tight: the one
consumer that shows a leader reads it from the one object that owns it. Deep module, thin interface.

## Migration plan (ordered steps)

1. **Add the leader source to `ServerDebugProvider`** — new `raft: Option<Arc<ClusterRaft>>` field +
   constructor param + `current_leader()` helper; fill `cluster_overview()` from it. Import
   `frogdb_cluster::ClusterRaft` (server already depends on `cluster`; no new crate edge).
2. **Wire the construction site** — pass `self.raft.clone()` at `subsystems.rs:116-123`.
3. **Delete `ClusterSnapshot.leader_id`** — remove the field (`types.rs:491`) and the
   `leader_id: None` line in `snapshot()` (`state.rs:98`). The compiler flags `debug_providers.rs:107`
   as the only broken read; step 1 has already replaced it, so this compiles clean.
4. **Add the provider test** (below).
5. `just check frogdb-cluster && just check frogdb-server`, then `just lint` — the change is
   compiler-driven; the only edited read site is the one fixed in step 1.

Ordering note: doing step 1 before step 3 keeps the tree green at every commit (the field still
exists while the new source is added, then is removed once nothing reads it).

## Test plan

- **New unit test in `debug_providers.rs`** (alongside the existing
  `replication_view_tracks_live_role_and_primary_target` at `debug_providers.rs:208`):
  `cluster_overview_reports_raft_leader` — build a `ServerDebugProvider` with a cluster state and a
  Raft handle whose metrics report a known `current_leader`, assert
  `provider.cluster_overview().unwrap().leader_id == raft.metrics().borrow().current_leader`. This is
  the "reported leader == Raft metrics" pin the candidate calls for, and it would fail today (always
  `None` regardless of the real leader). *Feasibility note:* constructing a real `ClusterRaft` in a
  unit test is heavyweight; if a bare `Raft<TypeConfig>` is impractical to stand up synchronously,
  refactor the leader read behind a tiny `LeaderReader` trait (`fn current_leader(&self) -> Option<u64>`)
  with the `Arc<ClusterRaft>` as the production impl and a stub in the test — this also sharpens the
  seam. Decide during implementation based on how cheaply a test `ClusterRaft` can be built (the
  cluster test-harness already spins up Raft nodes).
- **Standalone path**: `current_leader()` returns `None` when `raft` is `None`; assert the standalone
  `cluster_overview()` still returns `None` for `leader_id` (and the UI's "Unknown" branch, unchanged).
- **Integration (optional, cheap add)**: in an existing multi-node cluster integration test, after a
  leader is elected, assert the debug provider / Cluster Overview reports a `Some(leader)` matching
  `harness.get_leader()`. Guards against regressions where the seam is re-pointed at stale data.
- **Existing suite**: `ClusterSnapshot` construction/consumer tests compile unchanged (no test reads
  the deleted field); `just test frogdb-cluster` and `just test frogdb-server` stay green.

## Risks & alternatives

- **`ClusterRaft` in a unit test may be heavy.** The mitigation is the `LeaderReader` trait seam
  above — it decouples the provider from the concrete Raft type and makes the test a one-liner. Low
  risk; the trait is a net locality win regardless.
- **Metrics freshness.** `Raft::metrics()` is a `watch` channel updated by the Raft runtime;
  `.borrow().current_leader` is the same value CLUSTER INFO already trusts (`mod.rs:225`). No new
  staleness class is introduced — the debug UI simply joins the source of truth the rest of the
  server already uses.
- **Debug crate DTO field stays.** `ClusterOverviewSnapshot.leader_id` (`debug/web_ui/state.rs:235`)
  is intentionally kept — the debug UI's job is to *display* a leader. Only the always-`None` source
  in `cluster` is removed. No debug-crate signature or template change.
- **Alternative — `snapshot_with_leader(raft)` constructor:** rejected (see "Why read-at-seam");
  poor locality across 27 `snapshot()` call sites and an inverted state-machine→Raft coupling.
- **Alternative — keep the field, populate it at the debug seam before reading:** strictly worse than
  deletion — it preserves a struct field that all but the one debug caller ignore and that can drift back to
  `None`, re-opening the exact "set by caller, never set" trap. Deleting removes the trap by
  construction (deletion test: nothing production-observable is lost, one bug is fixed).
- **ADR-0001 (embedded Raft for cluster metadata) untouched.** The leader is Raft runtime state, not
  replicated metadata; sourcing it from `Raft::metrics()` at a read seam is fully consistent with
  "consensus applies to metadata only, the data path never goes through Raft." No metadata-plane
  behavior changes. Config Epoch, slot ownership, and role changes are unaffected.

## Effort

**S.** Three files: delete one field + its `None` initializer (`cluster`), add one field + param +
a 1-line metrics read to the provider and fill it (`server/debug_providers.rs`), pass one argument at
the construction site (`server/subsystems.rs`), plus one focused provider test. Compiler-driven — the
only broken read site is the one the change fixes. Bumps to S-M only if the `LeaderReader` trait seam
is added for testability (still a single small trait in the server crate). No async, persistence,
wire-format, or cross-crate signature churn; the persisted Raft snapshot (`ClusterStateInner`) is
untouched.

## Related

None.

## Adversarial review

**Verdict: CONFIRMED.** The reviewer opened every cited `file:line`, independently traced the runtime
path, and confirmed all core claims: the permanently-`None` premise (no caller ever mutates
`leader_id`; `from_snapshot` discards it and has zero callers), the misleading-observability bug
(`debug_providers.rs:107` → `handlers.rs:590-593` always renders "Unknown"), fix feasibility (the
`Arc<ClusterRaft>` handle is already in scope at the construction site; `raft.metrics().borrow().current_leader`
is the established CLUSTER INFO pattern), and format-safety (nothing serializes `ClusterSnapshot`; the
persisted/`InstallSnapshot` form is `ClusterStateInner`, which has no `leader_id`). The deletion test
passes and effort S is accurate. The reviewer concluded "proceed."

Issues raised and resolution:

- **Minor — call-site count imprecise (`~20` vs actual 27).** *Resolved.* Re-counted with
  `grep -rn '\.snapshot()'` over `frogdb-server/crates` excluding tests and the
  `build/get/install_snapshot` methods → 27 non-test call sites. Updated all four occurrences in the
  Problem section and the two "Why read-at-seam" alternatives to state "27 non-test call sites"
  (and reworded the "19 of 20" line to "all but the one debug caller"). The reviewer noted this was
  not load-bearing to the verdict — the argument (most callers ignore the leader) is unaffected — but
  the number is now precise.

No critical or major issues were raised; no design changes were required.
