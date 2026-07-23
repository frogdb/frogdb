# Area F: Cluster operations (agent report, verified evidence)

**Premise correction (doc nit, file with cleanup):** integration_cluster.rs:6-9 doc-comment ("Many tests marked #[ignore]… CLUSTER commands currently return hardcoded standalone responses") is FULLY STALE. Zero real #[ignore] attributes; CLUSTER INFO/NODES/SLOTS/SHARDS render live ClusterState (commands/cluster/mod.rs:182-448); mutating subcommands via real Raft. Hardcoded strings only when ctx.cluster_state == None (genuine standalone). Delete comment.

### 1. `info-current-epoch-folds-raft-term`
cluster_current_epoch = max(config_epoch, raft_term) (commands/cluster/mod.rs:247) but CLUSTER NODES per-node epochs = raw config_epoch (cluster/src/wire.rs:136). Nothing asserts agreement; every election bumps term → INFO epoch can exceed max NODES epoch.
- expected: Redis clients (redis-cli --cluster check, lettuce/ioredis) compare and flag divergence as config inconsistency.
- tests: after leader re-election w/o topology change, assert INFO epoch <= max(NODES config_epoch); unit on folding helper.
- **L2 / C2.**

### 2. `info-slots-fail-pfail-hardcoded`
cluster_slots_pfail/fail literal 0 (mod.rs:254-255); slots_ok = slots_assigned unconditionally (:269 "for now"). FAIL-flagged primary's slots still count OK (INFO detects FAIL for cluster_state at :205-208). Zero tests (grep slots_fail → 0).
- tests: mark slot-owning primary failed (setup exists :9631), assert slots_fail>0, slots_ok<assigned.
- **L2 / C2.**

### 3. `epoch-persistence-masked-by-term`
test_cluster_epoch_persists (integration_cluster.rs:8026) asserts only post >= pre (:8063) on the composite epoch — raft term bumps on post-restart election, so passes EVEN IF persisted config_epoch reset to 0. Cannot detect the exact loss it guards (clustering.md:148-161 Config-Epoch invariant).
- tests: parse per-node config_epoch from CLUSTER NODES pre/post restart, assert equality; unit round-trip nonzero epoch through cluster/src/storage.rs.
- **L2 / C3.**

### 4. `source-crash-mid-migration-unasserted`
test_source_dies_during_migration (integration_cluster.rs:3599) kills source mid-migration then only eprintln!s cluster info + SETSLOT STABLE — NO assertions (comment says "Verify cluster can handle" but verifies nothing: no orphaned-record cleanup, no half-transfer check, no ownership convergence).
- tests: after source death assert (a) slot resolves to exactly one owner within bound, (b) target has no lingering migration entry, (c) keys readable on exactly one node.
- **L2 / C3** (stuck migration / dual ownership).

### 5. `jepsen-slot-migration-checker-noop` — DEDUPE with G:slot-migration-checker-noop
slot_migration.clj:403 unconditional :valid? true. (concurrent_migration.clj:292 + migration_recovery.clj:324 DO have real predicates.)
- **L3 / C2** (per this agent; G scored L3/C3 — reconcile in adversarial pass).

### 6. `no-cluster-topology-in-turmoil` — overlaps G:no-deterministic-sim (raft half)
simulation.rs (4309 lines) zero cluster coverage — no Raft/MOVED/ASK/migration/multi-node (only internal-shard hash-tag tests :525-536). Timing-sensitive paths get no deterministic seed-reproducible coverage.
- tests: sim_harness cluster topology; writes through MOVED redirects; leader partition mid-migration; assert single-owner convergence deterministically.
- **L3 / C2.**

### 7. `leader-only-fd-false-failover`
Documented blind spot (clustering.md:123,230-234: leader-only TCP probe) untested for false positives. test_asymmetric_node_failure (:1197) kills leader + checks re-election — NOT the inverse (follower partitioned from leader but client-reachable → MarkNodeFailed + auto-failover against live node).
- tests: asymmetric partition isolating one primary from Raft leader only; assert failover behavior + fate of client-accepted writes on "failed" primary.
- **L2 / C3.**

### 8. `multi-exec-across-migration-boundary`
MULTI/EXEC cluster coverage = READONLY-replica only (integration_cluster.rs:6655-6776, cluster_sharded_pubsub_tcl.rs:40-181). No test commits BeginSlotMigration between queue-time and EXEC. Slot validation at queue time (guards.rs:567); EXEC path (transaction.rs:234-241) resolves + executes w/o re-running MOVED/ASK/TRYAGAIN. Tx queued while owning slot executes against now-MIGRATING slot.
- tests: MULTI + queue keyed write; SETSLOT MIGRATING from other conn; EXEC; assert semantics deliberate + documented.
- **L1 / C2.**

### 9. `sharded-pubsub-subscriber-not-dropped-on-slot-move`
cluster_sharded_pubsub_tcl.rs covers SPUBLISH-in-MULTI + MOVED-on-replica; nothing covers SSUBSCRIBEd client whose slot migrates/fails over. Redis 7+ unsubscribes shard-channel subscribers when slot leaves node. Migration-complete fanout wakes blocked keyed clients (slot_migration/events.rs:37-45 SlotMigrated) — no evidence sharded-pubsub subscribers dropped/redirected, no test. (NOTE area-C agent found integration_pubsub.rs:1274 sharded-pubsub slot-migration SUNSUBSCRIBE test — RECONCILE in adversarial pass.)
- **L2 / C2.**

### 10. `wait-in-cluster-untested`
Zero WAIT coverage in cluster mode (grep WAIT integration_cluster.rs → 0). Which replicas count toward numreplicas; behavior during/after failover unspecified by test.
- tests: WAIT after write in 3-node cluster; ack count matches replica set; no hang across failover.
- **L2 / C2.**

### 11. `keyspace-notifications-and-scan-in-cluster-undefined`
Keyspace notifications (grep keyspace|keyevent → 0) + cluster SCAN (grep "SCAN" → 0) no cluster tests. Which node emits events; SCAN per-node semantics undocumented + unverified.
- tests: subscribe __keyevent@0__:set on each node; write; assert only owning primary emits. SCAN per primary; union == keyspace, no bleed.
- **L1 / C1.**

## Verified NOT gaps
MOVED/ASK single-key routing (test_moved_redirect_wrong_node, test_ask_redirect_during_migration, test_asking_flag_cleared_after_use); multi-key TRYAGAIN (test_mset_keys_in_migrating_slot_returns_tryagain, guards.rs:634); blocked-client MOVED on migration (test_blocking_command_during_migration_gets_moved — real asserts); TTL-preserving migration; Failover state-machine atomicity/idempotency/reparenting (cluster/src/state.rs:1519-1705); CROSSSLOT rejection; CLUSTER NODES/SLOTS/SHARDS golden tests (wire.rs, mod.rs); DBSIZE-sum (:9561) asserts; FLUSHDB per-node (:9470) one survival assert.
