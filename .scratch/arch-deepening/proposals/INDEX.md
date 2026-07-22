# Architecture-deepening proposals index

## Round 8 (01–06) — landed 2026-07-20
Scatter routing spec, WAL-lag aggregate, ConnMutation, HashMapStore lifecycle, RoleManager,
SnapshotScheduler. See ledger memory for detail.

## Round 9 (07–13) — landed 2026-07-21
Scatter fan-out helpers, ACL single algorithm, search doc-write API, cluster apply events,
ShardMessage sub-enums, snapshot surface trim, config param registry.

## Round 10 (14–37) — landed 2026-07-22, branch arch-deepening/impl
- 14 internal-removal-write-effects — expiry+eviction through WRITE_EFFECT_ORDER; eviction now WAL-durable + replicated (live bug)
- 15 reindex-command-spec-fact — ReindexSpec on CommandSpec; hotfix landed first for 7 stale hash commands (live bug)
- 16 partial-result-typed-replies — PartialResult enum; sentinel keys deleted
- 17 pubsub-registered-ack — barrier ack replaces fabricated counts
- 18 watch-key-granularity — slot-granular WATCH versions (over-abort fixed; upstream live_at_watch had fixed under-abort)
- 19 dead-persistence-config-knobs — compression honored via preset table; failure_policy field deleted (live bug)
- 20 rocksstore-surface-trim — dead methods deleted, batch primitives pub(crate), test-support feature
- 21 noop-coordinator-scheduler-backing — noop backed by real SnapshotScheduler; SnapshotHandle = {epoch}
- 22 rocks-config-from-persistence — RocksConfig::from_persistence single mapping
- 23 search-sidecar-layout — DELETE branch: reader-less checkpoint sidecar copy removed, exclusion test-pinned
- 24 rocksstore-metrics-injection — recorder constructor injection; install slot deleted
- 25/26 replica-offset-owner + state-offset-split — ReplicaOffset owner; offset_at_save rename (serde alias)
- 27 replconf-codec — ACK/GETACK grammar in one codec, golden round-trips
- 28 psync-handoff-typed — PSYNC_HANDOFF sentinel → typed Dispatched/FrameAction
- 29 split-brain-divergence-record — DivergenceRecord computed by primary handler
- 30 checkpoint-installer — CheckpointStager + socket-free receiver
- 31 cluster-propose-seam — ClusterWriter propose→forward→redirect saga owner
- 32 cluster-error-typed-response — ClusterResponse::Error(ClusterError) + Epoch(ConfigEpoch)
- 33 cluster-snapshot-leader-id — leader_id field deleted; LeaderReader seam (live bug: debug UI)
- 34 cluster-wire-renderer — cluster_wire module; NODES/SHARDS golden-pinned; SLOTS ordering deterministic
- 35 raft-bus-rpc-split — RaftRpc/BusRpc envelope; dead defensive arms deleted (breaking wire, pre-prod)
- 36 even-slot-partition-fn — pure even_slot_ranges, both bootstrap paths
- 37 cluster-command-single-enum — total adapter, dead error branch deleted (rescoped cleanup)

Post-merge review fix: 65966195 — WATCH false-negative when field-only hash expiry co-occurs
with a whole-key removal in one expiry cycle (14×18 interaction; global-epoch bump now
unconditional on fields_expired > 0).

Follow-ups: issues/15-round10-followups.md
