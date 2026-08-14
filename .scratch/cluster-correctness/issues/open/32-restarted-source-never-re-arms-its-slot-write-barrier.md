# 32: Restarted source never re-arms its slot write barrier

Status: ready-for-agent

## Origin

Distsys-review CRIT-1 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled fix-now by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

The slot write barrier — the safety argument that "the source has quiesced" before
ownership moves — is armed and released exclusively from the live `SlotHandoffEvent`
stream (`cluster-runtime/src/handoff_barrier.rs:177`, `run_slot_handoff_barrier` =
`while let Some(event) = handoff_rx.recv().await`). Nothing reconstructs it from the
replicated `migrations` map:

- `restore_from_snapshot` (`cluster/src/state.rs:257-268`) assigns and emits nothing.
- `install_snapshot` (`state.rs:1065-1102`) reconciles the *role* view
  (`emit_self_role_change`) but has no handoff analogue.
- Boot ordering guarantees the miss: `cluster_init.rs` attaches the snapshot store
  (~:215) before `enable_slot_handoff_notification()` (~:240), so the boot-time restore
  always predates the channel.

Lost-write window: source A arms its barrier on `PrepareSlotHandoff`, crashes, restarts
(or is snapshot-caught-up). Replicated state still names A the migrating owner, so A
serves the slot — barrier unarmed — and admits writes the cluster believes cannot exist.
The target completes on A's earlier drain confirmation; A's post-restart writes are
silently lost. FM-CLUSTER-095's `SlotFence` does not close it: stamp and verdict read
the same unchanged `handoff_seq`, so a post-restart write validates consistently.

Precedent in-repo: the role view had the identical bug; issue 37 (arch-deepening) added
`reconcile_self_role` — "a role folded into a snapshot produced no log entry to replay".
The barrier is the same lesson, unapplied. (CRDB rebuilds leaseholder/latch state from
replicated range state on every restart for exactly this reason.)

## What to build (spec-first)

1. New FM row (spec-first: row → forcing test → fix): "the source restarts (or
   snapshot-installs) with a handoff prepared → its barrier is armed before the node
   admits any client write to that slot."
2. `reconcile_slot_handoff(...)` beside `reconcile_self_role` in `cluster_init.rs`,
   driven from the restored `migrations` map, emitting the `Prepared` events the barrier
   task would have seen live.
3. `install_snapshot` emits `Prepared`/`Released` on any handoff-state delta, the same
   way it emits role changes.
4. Forcing test: restart a source mid-handoff, assert a write to the migrating slot is
   refused before any client write is admitted.
5. Restate the `PauseState.slots` State-space row in `specs/cluster.md` — it currently
   launders an unimplemented "reconstructible in principle" as a property.

## Acceptance criteria

- [ ] FM row added and cited; `just lint-spec` green
- [ ] Reconcile path covers both boot restore and live `install_snapshot`
- [ ] Forcing test fails on the pre-fix tree, passes post-fix
- [ ] `just mutants-diff` on touched locked crates (cluster, cluster-runtime) triaged
- [ ] State-space row restated

## Cross-references

- Survives [issue 31](31-slot-migration-redesign-source-authoritative-until-commit.md)'s
  migration redesign unchanged: 31 keeps the Prepare→drain→Complete finalization, and a
  restart mid-drain still requires re-arming.

## Blocked by

None — can start immediately.
