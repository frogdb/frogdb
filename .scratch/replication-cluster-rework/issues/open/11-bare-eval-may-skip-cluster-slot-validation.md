# Bare EVAL/EVALSHA/FCALL appear to bypass cluster slot validation (stage-ordering hole)

Status: needs-triage — code-derived finding, needs a forcing test to confirm
PRD: [replication-cluster-rework/epoch-fold-redesign.md](../epoch-fold-redesign.md)
Type: AFK
Origin: rework-02 design research, 2026-08-04 (see
[migration-pause-barrier-brief-2026-08-04.md](../../migration-pause-barrier-brief-2026-08-04.md) §9)
Severity: likelihood 3/3 if real (every bare script on a non-owner node), consequence 3/3
(script executes locally on a non-owner instead of -MOVED — acked writes on the wrong node;
strictly wider than issue 03's finalization window)
Area: Cluster / dispatch ordering / scripting

## Finding

`PRE_DISPATCH_ORDER` (`frogdb-server/crates/server/src/connection/dispatch.rs:119-137`) runs
`ConnectionCommand` at index 9 and `ClusterSlotValidation` at index 14, and
`dispatch_connection_command` (`dispatch.rs:182-213`) short-circuits dispatch. EVAL/EVALSHA/
FCALL are `ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)`
(`scripting_conn_command.rs`), so a bare script appears to be consumed at stage 9 and never
reach slot validation at stage 14.

The *intent* is clearly that scripts be validated: `is_cluster_exempt`
(`connection/guards.rs:696-708`) deliberately refuses to exempt Scripting, with a doc comment
at `:673-695` explaining scripts are slot-routed. But there is no `MUST_PRECEDE` pair
(`dispatch.rs:844-888`) encoding `(ClusterSlotValidation, ConnectionCommand)`, and no test
pins a bare `EVAL` receiving `-MOVED` on a non-owner node — only the MULTI-queued case
(FM-TXN-030) and cross-slot-in-script (`test_eval_cross_slot_returns_error`, which comes from
`classify_script_shards`, not `validate_cluster_slots`).

## What to do

1. Forcing test first: 2-node cluster, send bare `EVAL ... 1 <key-owned-by-other-node>` to the
   non-owner. Expect `-MOVED`; if the script executes locally, the hole is confirmed.
2. If confirmed: fix the stage ordering (or validate slots inside the scripting connection
   command), add the `MUST_PRECEDE` pair so the ordering is pinned structurally, and give the
   spec an FM-CLUSTER row forced by the new test.
3. Note for rework 02: its Lua acceptance test passes vacuously while this hole exists —
   verify/fix this FIRST.

## Related

- Issue 03 (lua internal write validation) — this is the wider version of the same class.
- Redis ships per-`redis.call` re-validation (`script.c:493-554`, `scriptVerifyClusterState`).
