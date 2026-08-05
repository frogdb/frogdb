# cluster_failover::test_node_restart_rejoins_cluster flaky under whole-suite load

Status: needs-triage
Type: AFK
Origin: whole-suite verification during the regression unfreeze, 2026-08-05
(`left: "fail", right: "ok"` at `cluster_failover.rs:107`; failed try 1, passed under
nextest's configured retries in the same run)
Severity: likelihood 1/3 (one sighting, retry-masked), consequence 1/3 (test-signal noise;
no product behavior implicated yet)
Area: Cluster failover — **locked area**; any production fix is spec-first

## What was seen

During the 7929-test post-unfreeze run, the restarted node's health was still `"fail"` when
the test asserted `"ok"` — i.e. the failure detector had not yet observed the rejoin at
assertion time. Passes on retry and in targeted runs. Same "passes isolated, fails under
CPU contention" family as hardening 01 (broadcast-lag) and 30 (self-fence), both of which
turned out to be assertions racing an asynchronous convergence rather than awaiting it.

## Suggested triage

Check whether the test awaits failure-detector convergence (health flip to `ok`) with a
deadline, or asserts after a fixed sleep/single poll. If the latter, de-race by awaiting
the observable the assertion reads (as done for FM-REPLICATION-027 in issue 30). If the
detector genuinely never converges under load within a generous deadline, that is a real
liveness finding — escalate to ready-for-human.
