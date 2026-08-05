# Self-fence fires -CLUSTERDOWN on a standalone primary under load (flaky FM-REPLICATION-027 test, misleading error)

Status: done
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

## Resolution

All three questions were settled by the user; the answers are implemented on branch
`hardening-issue-30`.

**1. Error text — a dedicated `SELFFENCE` code.** The fence now refuses with
`SELFFENCE writes rejected: no fresh streaming replica (self-fence-on-replica-loss)`, naming both
the mechanism and the knob that turns it off. The wording became the checker's rather than the
gate's: `QuorumChecker::quorum_lost_error()` (`frogdb-server/crates/core/src/command.rs`) defaults
to the old `CLUSTER_DOWN_QUORUM_LOST` and is overridden by `ReplicationQuorumChecker`, so the one
write-gate rung in `guards.rs` serves both fences without knowing which is installed. The cluster
fence keeps `-CLUSTERDOWN` (FM-CLUSTER-059), and `SelfFenceGate` delegates to its inner checker so
wrapping does not launder the wording.

**2. Default — stays `true`, but a clean departure disarms.** A streaming replica's exit is now
classified: `ReplicaDeparture::{Graceful, Lost}`, produced by `ReplicaSession::run`'s single exit
handler and stored on the tracker as cross-session state (it must outlive the session map). An
orderly EOF, the primary's own broadcaster closing, and a primary-initiated `request_disconnect()`
are graceful; every error, write timeout, lag disconnect and broadcast `Lagged` overrun is lost,
and an unrecorded departure keeps fencing — the conservative default is the one that stays. The
fence's latch is dropped only when nothing is registered as streaming *and* the last departure was
graceful, so a deliberately decommissioned replica leaves the primary writable while a replica that
is still attached but silent (the partition case) keeps it fenced.

Two ordering invariants fell out of this and are pinned by tests: the departure is recorded
*before* `unregister_replica` (otherwise a predecessor's graceful record answers for a link that
actually died), and it is *cleared* as a replica enters `Phase::Streaming` (otherwise a record
outlives the replica generation it describes).

**3. Test.** `test_replica_handles_rapid_reconnect` now awaits fence-relevant state instead of the
connection: `wait_for_fresh_streaming_replicas` reads the same registry the fence does, through the
`slaveN:` projection (`state=online` + `lag=0`), and each cycle drains the departing session before
the next attaches. The graceful-departure disarm removes the flake's root cause as well, since a
shut-down replica no longer leaves the fence armed.

Because a clean shutdown no longer fences, the integration tests that forced the fence with
`replica.shutdown()` could no longer force anything. `LagProxy` grew an independent ACK-leg stall
(`stall_acks`/`resume_acks`), which produces an attached-but-silent replica deterministically, and
`fenced_by_a_silent_replica` shares that setup across the five self-fence tests and the
promoted-primary write-gate test.

Spec: FM-REPLICATION-041 rewritten (SELFFENCE wording, the disarm, the `quorum_lost_error()`
delegation), FM-REPLICATION-062 added for the departure classification, FM-CLUSTER-059 extended
with the delegation. Docs: `website/src/content/docs/architecture/replication.md` and the
`self_fence_on_replica_loss` doc comment.
