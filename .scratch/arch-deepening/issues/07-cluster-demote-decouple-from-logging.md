# 07 — Decouple cluster Role Demotion from `split_brain_log_enabled`; wire HealthProbe

Status: needs-triage

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

- [ ] With `split_brain_log_enabled = false` and cluster mode on, a DemotionEvent still demotes the node (test)
- [ ] With it true, the log line appears; demotion behavior identical either way
- [ ] Runtime demote registers the `shared_offset` HealthProbe; failure detector observes offsets from a runtime-demoted Replica (test at the probe seam)
- [ ] Config docs for `split_brain_log_enabled` describe it as log-only

## Blocked by

None - can start immediately

## Source

Round-8 P05 agent report; gating decision recorded above (was the only HITL item — now resolved).
