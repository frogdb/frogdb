# status.primaryPod is an ordinal-0 heuristic, wrong after failover

Status: ready-for-agent

`frogdb-operator/src/controller.rs` sets `status.primary_pod = {name}-0` whenever
`replicas > 1` or mode=cluster. This conflates "ordinal-0 pod" with the actual
Primary/Raft-Leader:

- Standalone mode: wrong after a failover promotes a different ordinal.
- Cluster mode: there is no fixed primary at all; the field is misleading.

Fix options (decide in implementation):

1. Query actual role (INFO replication / cluster metadata) during reconcile and report truth.
2. Omit the field in cluster mode; keep it standalone-only with real role detection.
3. Rename to something honest (e.g. `bootstrapPod`) if role tracking is out of scope.

See `frogdb-operator/CONTEXT.md` flagged ambiguities.
