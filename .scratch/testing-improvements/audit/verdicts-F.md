# Verdicts F (cluster)
1 info-current-epoch-folds-raft-term: ADJUSTED L2/C1. Divergence real, but proposed assertion contradicts Redis semantics (currentEpoch legitimately may exceed config epochs); redis-cli --cluster check flags epoch COLLISIONS not drift. Observability-only. Reframe task: pin INFO-vs-NODES epoch relationship deliberately + test collision behavior.
2 info-slots-fail-pfail-hardcoded: CONFIRMED L2/C2 (partially mitigated: cluster_state:fail still surfaces).
3 epoch-persistence-masked-by-term: CONFIRMED L2/C3.
4 source-crash-mid-migration-unasserted: CONFIRMED L2/C3 (only a panic could fail the test today).
5 jepsen-slot-migration-checker-noop: CONFIRMED L3/C2 FINAL (dedupe w/ G; sibling workloads carry real predicates over same machinery → false-confidence class, not sole defense).
6 no-cluster-topology-in-turmoil: CONFIRMED L3/C2. MERGE with G's raft half into one "deterministic multi-node cluster+raft sim" task; keep G replication-sim half separate.
7 leader-only-fd-false-failover: CONFIRMED L2/C3 (failure_detector.rs:10 leader-only MarkNodeFailed; inverse asymmetric case untested; real split-brain path w/ auto-failover).
8 multi-exec-across-migration-boundary: CONFIRMED L1/C2.
9 sharded-pubsub-subscriber-not-dropped-on-slot-move: REFUTED. integration_pubsub.rs:1274 test_ssubscribe_client_receives_sunsubscribe_on_slot_migration covers migration path e2e (SETSLOT MIGRATING→IMPORTING→NODE, asserts ["sunsubscribe",ch,0]; prod path dispatch_cluster.rs:10 → drain_sharded_channels_for_slot). Residue: failover-variant (primary death) untested — nit, roll into failover coverage tasks if anywhere.
10 wait-in-cluster-untested: CONFIRMED L2/C2 (all WAIT grep hits are "Wait for" comments).
11 keyspace-notifications-and-scan-in-cluster-undefined: CONFIRMED L1/C1.
