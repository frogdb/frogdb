# 18 — Promotion builds no primary-side replication seams

Status: done (full fix landed same round, 2026-07-28, branch workspace-3)

Resolution — the full fix (design below) landed, not the interim mitigation: tracker,
`PrimaryReplicationHandler`, and `ReplicationQuorumChecker` are now constructed for **every
role at boot** and published into the write gate; behavior is gated on the single
process-wide `is_replica_flag` (sole writer `RoleManager`). A `RoleGatedBroadcaster` no-ops
while replica; the presence-as-role-proxy gates (PSYNC dispatch, INFO, both
replication-state save paths) became explicit role gates; demotion un-latches self-fence
arming (a re-promoted node must not inherit a fence earned by sessions no longer its own).
Feasible because construction has no role-dependent inputs — only behavior does.
Tests: `test_promoted_replica_enforces_write_gates_set_before_promotion` (boot replica →
SET self-fence + thresholds → `REPLICAOF NO ONE` → enforcement verified),
`test_promoted_node_via_replicaof_no_one_serves_downstream_psync`,
`test_promoted_replica_serves_all_writes_after_promotion`,
`demote_resets_replication_self_fence_arming`.

Filed 2026-07-28 from the config-mutability round's adversarial review (finding M5);
original problem statement and design kept below as the record.

## Problem

The primary-side replication machinery — replica tracker, `PrimaryReplicationHandler`, and
`ReplicationQuorumChecker` — is constructed only when the node boots as a primary
(`server/replication_init.rs:60-107,215-238`, gated on `config.replication.is_primary()`).
Runtime promotion (`REPLICAOF NO ONE` → `role_manager.rs:163-175` `promote()`) drops streams,
stops the boot replica handler, clears `primary_target`, flips `is_replica = false` — and
builds **none** of the primary-side seams. Nothing is published into the server's
OnceLock handles.

Consequences on a replica-booted node after promotion:

- `guards.rs:290-299` sees `quorum_checker == None` → **writes are accepted with zero
  replicas, forever; self-fence never engages**, regardless of
  `self-fence-on-replica-loss`.
- The config-mutability round promoted `self-fence-on-replica-loss`,
  `replica-freshness-timeout-ms`, and `replication-lag-threshold-bytes`/`-secs` to CONFIG
  SET. The SET arms (`runtime_config.rs:2888-2932` region) store into the ConfigManager's
  authority atomics and push into the OnceLock **only if present** — so on such a node,
  CONFIG SET returns OK and CONFIG GET echoes the value **with nothing behind it**: a
  safety knob that reports success while doing nothing.
- The doc comment at `runtime_config.rs:782-800` claims the SET "takes effect if the node
  is later re-initialised as a primary" — true only across a restart (config re-read), not
  across promotion.

The construction gap is pre-existing (sole production construction site is
`replication_init.rs:80`; other `ReplicationQuorumChecker` constructions are tests). The
mutability round made it materially worse by adding the lying CONFIG surface.

## Fix design (from the review)

Mirror boot wiring at promotion:

1. In `promote()`, construct tracker + `PrimaryReplicationHandler` + quorum checker exactly
   as `replication_init.rs` does at boot, seeded from the ConfigManager's live authority
   atomics (so any SETs issued while still a replica take effect on the promoted primary).
2. Convert the relevant OnceLock publications to swappable handles
   (`ArcSwapOption`/`RwLock<Option<Arc<_>>>`), updating all readers, so promotion (and
   repeated promote/demote cycles) can re-publish.
3. Install the checker into the write-gate path `guards.rs` reads.
4. Demotion: decide and document what `guards.rs` should see on a replica (the replica
   write-gate path does not consult the quorum checker today).
5. Integration test: boot replica → `CONFIG SET self-fence-on-replica-loss yes` (+ lag
   thresholds) → `REPLICAOF NO ONE` → verify the promoted node enforces them (quorum fence
   rejects writes with no replicas).

Interim mitigation if the full fix is deferred: correct the doc comment, `warn!` from
`promote()` listing unwired params, and make the four SET arms **error** when no live seam
is published instead of silently accepting.

## Relates to

- Issue 14 (propagation-truth gate: a param may be mutable only if SET changes runtime
  behavior — these four violate it on promoted primaries).
- Round-7 follow-ups item 1 (WAIT wake-up on resume-seed) touched the same tracker
  lifecycle.
