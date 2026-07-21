# Proposal 10 — Let `apply_command` own event derivation, so `apply` only forwards

## Summary (2-3 sentences)

`RaftStateMachine::apply` (`cluster/src/state.rs`) re-pattern-matches three `ClusterCommand`
variants that `apply_command` (`cluster/src/commands.rs`) already owns, purely to derive the
`DemotionEvent` / `SlotMigrationCompleteEvent` each mutation should emit — three near-duplicate
capture-before-apply blocks with inconsistent success-gating. One of them, the `SetRole`
self-demotion emission, fires *before* `apply_command` runs with **no success gate at all**, so a
rejected metadata mutation still emits a demotion into the data-path role machinery. This proposal
moves the variant→event mapping into `apply_command` (which returns the events it produced),
uniformly success-gates it by construction, shrinks `apply` to "call + filter-for-self + forward",
and fixes the emit-on-failure bug structurally.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/cluster/src/state.rs` | 1918 | `RaftStateMachine::apply` (L285-388) with the three emit blocks; `ClusterStateMachine` holds `self_node_id` + the two `mpsc` senders (L200-208); `DemotionEvent` (L180-189), `SlotMigrationCompleteEvent` (L191-197) |
| `frogdb-server/crates/cluster/src/commands.rs` | 407 | `apply_command` (L9-406) — the mutation owner; `SetRole` arm L105-134, `Failover` arm L142-226, `CompleteSlotMigration` arm L297-322 |
| `frogdb-server/crates/cluster/src/types.rs` | — | `ClusterResponse { Ok, Value, Error }` (L361-368); `ClusterError` (L405) |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | 874 | Wires the channels: `enable_demotion_detection(node_id)` (L140), `enable_migration_complete_notification()` (L143); `DemotionConsumer` (L693-734) + its spawn loop (L519-532); `SplitBrainLogger::log` (L616-684) |
| `frogdb-server/crates/server/src/slot_migration/events.rs` | 48 | `run_event_dispatcher` — consumes `SlotMigrationCompleteEvent`, fans out `ShardMessage::SlotMigrated` (L18-47) |
| `frogdb-server/crates/server/src/role_manager.rs` | — | `RoleManagerHandle::request_demote` → `RoleManager::demote` (L155-172): flips `is_replica`, fences read-only, starts replicating from the new primary |
| `frogdb-server/crates/core/src/command.rs` | — | `RoleController` trait (L253-261) — the seam `DemotionConsumer` drives |
| `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` | 16 | Settled: embedded Raft owns metadata; the data path never goes through Raft. This proposal does not touch that decision. |

## Problem (concrete verified evidence)

`apply` and `apply_command` both know the mapping "which `ClusterCommand` variant produces which
event." `apply_command` owns the *mutation*; `apply` re-derives the *event* by matching the same
variants a second time. There are three of these blocks, and they do not agree on when to emit.

**Block 1 — `SetRole` self-demotion, emitted BEFORE apply, ungated (`state.rs:299-315`):**

```rust
// Check for self-demotion before applying
if let Some(self_id) = self.self_node_id
    && let ClusterCommand::SetRole {
        node_id,
        role: NodeRole::Replica,
        primary_id,
    } = &cmd
    && *node_id == self_id
    && let Some(ref tx) = self.demotion_tx
{
    let epoch = self.state.config_epoch();
    let _ = tx.send(DemotionEvent {
        demoted_node_id: self_id,
        new_primary_id: *primary_id,
        epoch,
    });
}
```

**Block 2 — `Failover` self-demotion, captured before, emitted AFTER apply, success-gated
(`state.rs:317-331` capture, `state.rs:354-364` emit):**

```rust
// Emit demotion event for a successful graceful failover of self
if let Some((demoted_id, new_primary_id)) = failover_demotion
    && !matches!(result, ClusterResponse::Error(_))          // <-- success gate
    && let Some(ref tx) = self.demotion_tx
{
    let _ = tx.send(DemotionEvent { /* … */ });
}
```

**Block 3 — `CompleteSlotMigration`, captured before, emitted AFTER apply, success-gated
(`state.rs:333-347` capture, `state.rs:366-372` emit):**

```rust
// Emit migration complete event on success
if let Some(event) = migration_event
    && !matches!(result, ClusterResponse::Error(_))          // <-- success gate
    && let Some(ref tx) = self.migration_complete_tx
{
    let _ = tx.send(event);
}
```

Between blocks 1 and 2/3 sits the actual mutation (`state.rs:349-352`):

```rust
let result = self.state.apply_command(cmd).unwrap_or_else(|e| {
    tracing::warn!(error = %e, "Failed to apply cluster command");
    ClusterResponse::Error(e.to_string())
});
```

**The emit-on-failure bug is real — confirmed.** `apply_command`'s `SetRole` arm rejects malformed
self-demotions:

- node absent → `Err(ClusterError::NodeNotFound(node_id))` (`commands.rs:111-113`)
- `primary_id` given but absent → `Err(ClusterError::NodeNotFound(pid))` (`commands.rs:118-120`)
- replica role with `primary_id: None` → `Err(ClusterError::InvalidOperation(...))`
  (`commands.rs:122-125`)

Because block 1 sends the `DemotionEvent` *before* `apply_command` is even called, **a rejected
`SetRole` still emits a demotion.** State unchanged, event fired. The success gate that blocks 2 and
3 use — `!matches!(result, ClusterResponse::Error(_))` — is sound (verified: `apply_command`
returns `Result<ClusterResponse, ClusterError>` and never returns `Ok(ClusterResponse::Error)`; the
only source of `ClusterResponse::Error` is the `unwrap_or_else` converting an `Err` at
`state.rs:351`). Block 1 simply has no equivalent.

**Blast radius, traced concretely.** The `demotion_tx` receiver feeds `DemotionConsumer::handle`
(`cluster_init.rs:704-733`), spawned at `cluster_init.rs:519-532`. On every event it does two
things:

1. **Unconditionally** runs `SplitBrainLogger::log` if split-brain logging is enabled
   (`cluster_init.rs:705-708`, before any validation). `log` (`cluster_init.rs:616-684`) emits a
   `"Split-brain demotion detected"` warning, extracts divergent writes, may **write a split-brain
   log file to disk**, and bumps `SplitBrainEventsTotal` / `SplitBrainOpsDiscardedTotal` /
   `SplitBrainRecoveryPending` telemetry. For a *rejected* `SetRole`, all of this is spurious
   split-brain evidence for a demotion that never happened in the Raft Metadata Plane.
2. If `new_primary_id` is `Some` and that node exists in cluster state, calls
   `role_controller.request_demote(addr)` → `RoleManager::demote` (`role_manager.rs:155-172`),
   which **fences the node read-only and starts replicating from that primary** — a real Role
   Demotion on the data path.

The consumer re-validates `new_primary_id` against cluster state, which cushions two of the three
failure modes (`InvalidOperation` gives `new_primary_id: None` → skipped; `NodeNotFound(pid)` for a
missing primary → "cannot reconfigure" no-op). But the `NodeNotFound(self)` case with a **valid**
`primary_id` — self not yet in the nodes map (a bootstrap/join window) while the target primary
exists — passes the re-validation and drives a genuine spurious data-path demotion: the node flips
to read-only replica while the metadata plane still records it as primary. Even in the cushioned
cases, hazard #1 (spurious split-brain telemetry + on-disk log) fires regardless. This directly
violates the project's observability-accuracy rule: misleading split-brain data is not acceptable.

**No test covers the buggy path.** `test_demotion_detection_not_fired_for_failed_failover`
(`state.rs:1836`) and `_for_force_failover` (`state.rs:1859`) prove blocks 2/3 are gated — but
there is **no** `SetRole`-failure analogue asserting block 1 stays silent, because block 1 is the
one that does not gate.

## Why it is shallow/fragmented (architecture vocabulary)

**The variant→event mapping is duplicated across the Interface seam.** `apply_command` is the
Module that owns cluster mutation; its Interface is `ClusterCommand → Result<ClusterResponse>`. But
the fact "a `SetRole{Replica}` / graceful `Failover` is a demotion, a `CompleteSlotMigration` is a
migration-complete" is knowledge that lives *twice*: once implicitly in `apply_command`'s arms, and
again explicitly in `apply`'s three pattern-matches. `apply` re-opens `ClusterCommand` to
reconstruct what `apply_command` already knew it was doing. That is poor **Locality**: the mutation
and the event it logically produces are derived in two different files, kept consistent by hand.

**The Seam leaks the success contract.** A well-formed seam would let the caller forward a result
without re-deriving it. Instead each of `apply`'s three blocks must independently re-encode the
success rule (`!matches!(result, Error)`), and one of the three encodes it *wrong* (fires before
the result even exists). The rule "events are consequences of successful mutations" is an invariant
that belongs *inside* the mutation Module; here it is re-implemented by the caller three times, so
it can be — and is — implemented inconsistently. This is the signature of a **shallow** module: the
interface (`Result<ClusterResponse>`) is too thin to carry the module's real output (the events),
forcing the caller to reach back in and reconstruct it.

**`apply` is doing two jobs.** Its genuine responsibility is Raft plumbing: iterate entries, handle
`Blank`/`Membership`, thread `last_applied_log`, filter demotions for *self* (the one thing it
legitimately owns, since `self_node_id` lives on the `ClusterStateMachine`, not on `ClusterState`).
The event-derivation is smuggled in alongside, tripling the body of the `Normal` arm and burying
the self-filter — the actual state-machine concern — inside boilerplate that re-does
`apply_command`'s work.

**Deletion test.** Delete `apply_command`'s knowledge and nothing about events breaks, because
`apply` re-derives them independently — the two are not linked. Conversely, add a new
demotion-producing command (say a future `TransferPrimary`) and `apply_command` will mutate
correctly while `apply` silently emits *no* event, with no compiler complaint — exactly the failure
class Proposal 01 identifies for scatter routing. The event mapping has no single owner the compiler
can hold to completeness.

## Proposed change (plain English)

Give `apply_command` a return type rich enough to carry the events its mutation produced, so the
variant→event mapping lives next to the mutation and is success-gated by construction (events are
only returned on the `Ok` path). `apply` stops re-matching `ClusterCommand` and becomes call +
self-filter + forward.

1. **Introduce a `ClusterEvent` enum** in `cluster` (flat, describes *what happened*, node-agnostic):

   ```rust
   pub enum ClusterEvent {
       /// A node was demoted from primary to replica.
       NodeDemoted { demoted_node_id: NodeId, new_primary_id: Option<NodeId>, epoch: u64 },
       /// A slot migration completed (relevant on all nodes).
       SlotMigrationCompleted { slot: u16, source_node: NodeId, target_node: NodeId },
   }
   ```

2. **`apply_command` returns the events it produced on success:**
   `Result<(ClusterResponse, Vec<ClusterEvent>), ClusterError>` (or an `Applied { response, events }`
   struct with `.events()`). The `SetRole{Replica}` arm pushes `NodeDemoted`, the graceful
   `Failover` arm pushes `NodeDemoted` for the old primary, `CompleteSlotMigration` pushes
   `SlotMigrationCompleted`. Because these are pushed only where the arm reaches its `Ok`, **emit-on-
   failure is now structurally impossible** — an `Err` return carries no events at all.

3. **`apply` shrinks to forwarding with the self-filter it legitimately owns:**

   ```rust
   let (response, events) = self.state.apply_command(cmd).unwrap_or_else(|e| {
       tracing::warn!(error = %e, "Failed to apply cluster command");
       (ClusterResponse::Error(e.to_string()), Vec::new())
   });
   for event in events {
       match event {
           // Demotion events are only relevant when *this* node is the one demoted.
           ClusterEvent::NodeDemoted { demoted_node_id, new_primary_id, epoch }
               if Some(demoted_node_id) == self.self_node_id =>
           {
               if let Some(tx) = &self.demotion_tx {
                   let _ = tx.send(DemotionEvent { demoted_node_id, new_primary_id, epoch });
               }
           }
           // Migration-complete fires on ALL nodes (no self-filter).
           ClusterEvent::SlotMigrationCompleted { slot, source_node, target_node } => {
               if let Some(tx) = &self.migration_complete_tx {
                   let _ = tx.send(SlotMigrationCompleteEvent { slot, source_node, target_node });
               }
           }
           _ => {}
       }
   }
   results.push(response);
   ```

   `self_node_id` stays on the `ClusterStateMachine` (verified: it is *not* on `ClusterState`, and
   `apply_command` has no notion of "self") — so the self-vs-other filter stays exactly where the
   identity lives, which is correct. Only the *derivation* of events moves; the *routing* of them
   (which channel, self-filter) stays in the state machine.

4. **`force`-failover and `Failover`-of-not-self** produce no `NodeDemoted` (matching today:
   block 2 requires `force: false` and `old_primary_id == self_id`) — the "not self" part is now the
   `apply` filter, and the "not force / demotes rather than removes" part is naturally expressed by
   *which* `Failover` code path reaches the push. `apply_local` (`state.rs:147`, the bootstrap seam)
   simply ignores the returned events (bootstrap is local and pre-consumer), a one-line `.map(|(r, _)| r)`.

Net: the event mapping gains a single owner (`apply_command`), the success invariant is enforced by
control flow rather than re-checked three times, and `apply`'s `Normal` arm collapses from ~75 lines
to a call plus a short forward loop.

## Before / After

### Before — `apply` re-matches variants; one block fires pre-apply, ungated (`state.rs:298-374`)

```rust
EntryPayload::Normal(cmd) => {
    // Check for self-demotion before applying
    if let Some(self_id) = self.self_node_id
        && let ClusterCommand::SetRole { node_id, role: NodeRole::Replica, primary_id } = &cmd
        && *node_id == self_id
        && let Some(ref tx) = self.demotion_tx
    {
        // BUG: sent before apply, no success gate — a rejected SetRole still fires this.
        let _ = tx.send(DemotionEvent { demoted_node_id: self_id, new_primary_id: *primary_id, epoch });
    }

    let failover_demotion = /* capture SetRole/Failover-of-self before cmd is moved */;
    let migration_event    = /* capture CompleteSlotMigration before cmd is moved */;

    let result = self.state.apply_command(cmd).unwrap_or_else(|e| ClusterResponse::Error(e.to_string()));

    if let Some(..) = failover_demotion
        && !matches!(result, ClusterResponse::Error(_)) { /* emit demotion */ }
    if let Some(event) = migration_event
        && !matches!(result, ClusterResponse::Error(_)) { /* emit migration */ }

    results.push(result);
}
```

Three variant matches, three copies of the success rule, one of them wrong. Adding a future
demotion-producing command and forgetting to add a fourth block here compiles fine and silently
emits nothing.

### After — `apply_command` returns events; `apply` forwards with the self-filter

```rust
EntryPayload::Normal(cmd) => {
    let (response, events) = self.state.apply_command(cmd)
        .unwrap_or_else(|e| { tracing::warn!(error = %e, "Failed to apply cluster command");
                              (ClusterResponse::Error(e.to_string()), Vec::new()) });
    for event in events { /* self-filter demotions; forward to the right channel */ }
    results.push(response);
}
```

`apply` no longer names `ClusterCommand::SetRole` / `Failover` / `CompleteSlotMigration` at all. The
"is this a demotion?" decision lives in the arm that performs the demotion, and an `Err` arm carries
no events, so emit-on-failure cannot recur.

## Testability improvement

**Today, event emission is only testable through the Raft `Entry` API.** Every existing event test —
`test_demotion_detection_fires_for_self` (`state.rs:705`),
`test_demotion_detection_ignores_other_nodes` (L741), `test_migration_complete_event_fires` (L775),
`test_demotion_detection_fires_for_graceful_failover_of_self` (L1812),
`_not_fired_for_failed_failover` (L1836), `_not_fired_for_force_failover` (L1859) — must hand-build
`openraft::Entry::<TypeConfig> { log_id: LogId { leader_id: CommittedLeaderId::new(..), index }, payload:
EntryPayload::Normal(..) }` and drive `sm.apply(vec![entry]).await`, because `apply_command` returns
no events and `apply` is the only place they exist. There is no way to unit-test "this mutation
should produce this event" without constructing Raft log-entry types and an async state machine.

**After the change, event derivation is a plain synchronous unit test on `apply_command`:**

```rust
#[test]
fn set_role_self_demotion_emits_no_event_on_error() {
    let state = ClusterState::new();               // node 1 never added
    let (resp, events) = match state.apply_command(ClusterCommand::SetRole {
        node_id: 1, role: NodeRole::Replica, primary_id: Some(2),
    }) { Err(_) => return_assert_empty(), Ok(v) => v };
    // ...or simply:
    assert!(state.apply_command(ClusterCommand::SetRole { .. }).is_err());
}

#[test]
fn complete_migration_emits_event_on_success() {
    let (_, events) = /* build state, */ state.apply_command(complete).unwrap();
    assert_eq!(events, vec![ClusterEvent::SlotMigrationCompleted { slot: 42, .. }]);
}
```

No `Entry`, no `LogId`, no `CommittedLeaderId`, no `tokio`. The **currently missing** test — "a
rejected `SetRole` self-demotion emits no event" — becomes trivially expressible and would have
caught the live bug. The `Entry`-based `apply` tests remain, but only to cover the two things `apply`
still owns: the self-filter and the channel wiring, not the variant→event mapping.

## Risks / open questions

- **Ship the one-line hotfix first.** The structural fix is M-sized; the live bug is closed by a
  ~10-line targeted patch: move block 1's `SetRole` emission to *after* `apply_command`, gated on
  `!matches!(result, ClusterResponse::Error(_))` exactly like blocks 2/3 (capture `(self_id,
  *primary_id)` before `cmd` is moved, emit after). This is worth landing ahead of the refactor
  because the bug is a real split-brain-telemetry / spurious-demotion hazard, and the hotfix is
  low-risk and independently testable. The refactor then subsumes it.
- **`ClusterEvent` vs the existing `DemotionEvent` / `SlotMigrationCompleteEvent`.** The two public
  channel types (`state.rs:180-197`, re-exported through `core`) stay as the *channel* payloads;
  `ClusterEvent` is the internal derivation type `apply` translates from. Alternatively, emit the
  public types directly from `apply_command` — but that couples the mutation module to `self`-carrying
  `DemotionEvent` (which has no notion of "was this self?"), so a node-agnostic internal enum keeps
  the self-filter cleanly in `apply`. Decide during implementation; the node-agnostic enum is the
  cleaner seam.
- **`Vec<ClusterEvent>` allocation per apply.** Every `apply_command` call now returns a `Vec`, almost
  always empty. Use `SmallVec`/`ArrayVec` or return `Option<ClusterEvent>` if a single event always
  suffices (verified: no arm produces more than one today). Minor.
- **`apply_local` (`state.rs:147`).** The bootstrap seam must adapt to the new signature by dropping
  the events (`.map(|(r, _)| r)`); bootstrap runs before any consumer is wired, so this is correct,
  not a lost event. Covered by `test_apply_local_shares_validated_path`.
- **Behavioral parity of the self-filter.** Today block 1 filters on `*node_id == self_id` and block 2
  on `*old_primary_id == self_id`; both collapse into the single `Some(demoted_node_id) ==
  self.self_node_id` guard in the forward loop. The graceful-vs-force and self-vs-other distinctions
  must map exactly onto "which arm pushes `NodeDemoted`" + "the `apply` self-filter" — the existing
  `_not_fired_for_force_failover` / `_ignores_other_nodes` tests pin this and must stay green.
- **ADR-0001 untouched.** This is purely an internal refactor of how the state machine *derives and
  forwards* metadata events; the Raft-owns-metadata / data-path-never-through-Raft decision is
  unchanged.

## Effort estimate

**M.** Add `ClusterEvent` (~2 variants), change `apply_command`'s return type and push events in three
arms, rewrite `apply`'s `Normal` arm into the forward loop, adapt `apply_local`, and port the six
existing event tests (they can keep using `apply`, plus add the new sync `apply_command` tests). It is
confined to the `cluster` crate — `cluster_init.rs`, `slot_migration/events.rs`, and the `RoleController`
seam are untouched because the *channel* payload types don't change. Not S because the return-type
change ripples through every `apply_command` arm and call site and the self-filter parity needs care;
not L because it is one crate with no cross-crate signature churn and the compiler drives the migration.
The one-line hotfix (Risks) is S and can land immediately.
