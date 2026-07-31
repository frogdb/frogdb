# 07 — Decouple cluster Role Demotion from `split_brain_log_enabled`; wire HealthProbe

Status: ready-for-human

## What to build

Decision made (option A, 2026-07-20): **logging configuration must not affect cluster behavior
at all.** The Raft Metadata Plane `DemotionEvent` consumer — which since round-8 performs a real
data-path Role Demotion via `request_demote`, not just split-brain logging — currently lives
behind the `split_brain_log_enabled` gate in `cluster_init.rs`. Disabling a log line silently
disables automatic demotion during failover.

End-to-end:
1. The demotion-event consumer always runs when `cluster.enabled`; `split_brain_log_enabled`
   controls only whether the split-brain log line is emitted. Kill-switch remains
   `cluster.enabled` itself — no new knob.
2. Runtime-started replica streams (RealReplicaStreamer) wire the cluster-bus `shared_offset`
   HealthProbe, so the failure detector sees a runtime-demoted Replica's replication offset the
   same as a boot-configured one.

## Acceptance criteria

- [x] With `split_brain_log_enabled = false` and cluster mode on, a DemotionEvent still demotes the node (test)
- [x] With it true, the log line appears; demotion behavior identical either way
- [x] Runtime demote registers the `shared_offset` HealthProbe; failure detector observes offsets from a runtime-demoted Replica (test at the probe seam)
- [x] Config docs for `split_brain_log_enabled` describe it as log-only

## Blocked by

None - can start immediately

## Source

Round-8 P05 agent report; gating decision recorded above (was the only HITL item — now resolved).

## Comments

### 2026-07-20 — Implemented (option A)

Logging config no longer affects cluster behavior.

**Part 1 — demotion decoupled from the log gate (`cluster_init.rs`):**
- `enable_demotion_detection` is now called unconditionally in cluster mode
  (was gated on `split_brain_log_enabled`). The demotion-event consumer always
  spawns when `cluster.enabled`.
- Extracted the consumer body into a testable `DemotionConsumer::handle`. The
  split-brain logging (log line + divergent-write capture + telemetry) is now an
  `Option<SplitBrainLogger>` on the consumer, `Some` iff `split_brain_log_enabled`.
  The `request_demote` data-path reconfiguration runs regardless. **The log gate
  and the demotion are now separate branches: disabling the log cannot disable
  failover demotion.** Kill-switch remains `cluster.enabled`; no new knob.

**Part 2 — runtime replica HealthProbe wiring (`role_manager.rs`, `cluster_init.rs`,
`server/mod.rs`, `replication/replica/mod.rs`):**
- `init_cluster` now guarantees the cluster-bus HealthProbe offset atomic exists
  in cluster mode (reuses the boot atomic from `init_replication`, else mints one)
  and returns it via `ClusterInitResult`, so a node that booted primary/standalone
  still has a shared offset. `server/mod.rs` reads the effective atomic back off
  the cluster result and hands it to the cluster bus.
- `RealReplicaStreamer` now holds that `shared_offset` and, in the new non-spawning
  `build_handler` seam, calls `handler.set_shared_offset(...)` — so every
  runtime-demoted replica publishes its applied offset into the same atomic the
  failure detector's HealthProbe reads, exactly like a boot-configured replica.
- Added `ReplicaReplicationHandler::shared_offset()` accessor (mirrors the primary
  handler's) for the seam assertion.

**Config docs:** `split_brain_log_enabled` doc-comment now states it is log-only and
does not affect cluster behavior; kill-switch is `cluster.enabled`.

**Where things live now:** the demotion-event consumer (`DemotionConsumer`) runs
unconditionally under `if config.cluster.enabled`; the split-brain log is a gated
`Option<SplitBrainLogger>` branch inside `DemotionConsumer::handle`.

**Tests (all pass, `just test frogdb-server`):**
- `cluster_init::tests::demotion_fires_when_split_brain_log_disabled` — logger
  `None`, demotion still issued.
- `cluster_init::tests::demotion_identical_whether_or_not_log_enabled` — real
  `SplitBrainLogger` present vs absent, identical demotion.
- `cluster_init::tests::no_demotion_when_new_primary_absent_from_topology` — edge.
- `role_manager::tests::runtime_stream_wires_shared_offset_to_healthprobe_atomic` —
  probe seam: `build_handler` wires the SAME atomic the cluster bus reads
  (`Arc::ptr_eq`), and a stored offset is visible through it.
- `role_manager::tests::runtime_stream_without_cluster_leaves_offset_unwired`.
- Existing 8 `role_manager` tests still green; full `just check` (all-targets) +
  clippy clean.

Commits: `0659e9a7` (impl), `64b19187` (tests + config doc).
