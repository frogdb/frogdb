# Self-fence fires -CLUSTERDOWN on a standalone primary under load (flaky FM-REPLICATION-027 test, misleading error)

Status: needs-triage
Type: AFK
Origin: whole-suite run during rework-05 verification, 2026-08-04
(`test_replica_handles_rapid_reconnect::case_2_with_persistence` failed with
`expected OK, got Error(b"CLUSTERDOWN The cluster is down (quorum lost, writes rejected)")`;
passes 2/2 in isolation, `case_1_in_memory` passed in the same run; second sighting 2026-08-05 in the rework-10 verification suite run, again isolated-pass after)
Severity: likelihood 2/3 (needs armed fence + ACK lag past the freshness window; whole-suite CPU
contention reproduces it), consequence 2/3 (spurious write refusals on standalone primaries;
misleading operator diagnostics)
Area: Replication / write gating — **locked area, behavior changes are spec-first**

## Root cause (traced, not speculative)

- `self-fence-on-replica-loss` defaults **true**
  (`frogdb-server/crates/config/src/replication.rs:230`).
- `ReplicationQuorumChecker` is installed unconditionally — every server, every role
  (`frogdb-server/crates/server/src/server/replication_init.rs:304-321`).
- Arming is **latched forever** once any replica has ever streamed
  (`frogdb-server/crates/replication-runtime/src/quorum.rs:171-179`); once armed + enabled,
  `has_quorum()` requires ≥1 *fresh* streaming replica (ACK within
  `replica-freshness-timeout-ms`).
- The guard turns lost quorum into
  `CLUSTERDOWN The cluster is down (quorum lost, writes rejected)`
  (`frogdb-server/crates/server/src/connection/guards.rs:347-355`) — even when cluster mode is
  off.

Flake mechanism in `test_replica_handles_rapid_reconnect` (integration_replication.rs:5486,
FM-REPLICATION-027): cycle N's replica streams → primary armed for life; cycle N+1's replica
passes `wait_for_connected_slave`, but under CPU contention its ACK cadence falls outside the
freshness window → `count_fresh_streaming_replicas() == 0` → SET refused. The fence is doing
exactly what it was built to do; the test (and possibly the defaults) disagree with it.

## Questions to settle (ready-for-human)

1. **Error text.** `-CLUSTERDOWN` on a non-cluster deployment is actively misleading (Redis
   emits CLUSTERDOWN only in cluster mode). The self-fence refusal deserves its own error
   (e.g. `-NOREPLICAS`-adjacent or a dedicated `-SELFFENCE ...`) or at least text that names
   self-fencing and the config knob. Observability-accuracy rule applies.
2. **Default.** `self-fence-on-replica-loss = true` means every standalone primary that ever
   had a replica attach refuses writes whenever it has no fresh replica — including after a
   deliberate, clean replica decommission. Redis has no equivalent default-on behavior
   (min-replicas-to-write defaults 0). Is default-on the intended posture, or should the
   default be false / should a clean replica disconnect disarm?
3. **Test.** Whichever way 1-2 go, the FM-REPLICATION-027 forcing test must either tolerate
   the fence (await freshness, not just connection) or run with the fence disabled explicitly
   — decide after 2, since the right fix depends on the intended default.

Spec impact: the self-fence rows in `.scratch/hardening/specs/replication-failure-modes.md`
(FM-REPLICATION spec is LOCKED) must be updated first if behavior or error text changes;
`just mutants-diff frogdb-replication-runtime` required before pushing any quorum.rs change.

## Related

- Hardening issue 01 (broadcast-lag flake watch) — same "passes isolated, fails under
  whole-suite load" family, different mechanism.
- Rework 06-09 (solicited ACK / divergence epoch-latch) introduced the surrounding machinery.
