# Bare EVAL/EVALSHA/FCALL appear to bypass cluster slot validation (stage-ordering hole)

Status: confirmed — forcing test written and failing (2026-08-04); ready-for-agent (fix deferred to
the cluster hardening phase's bug-closing step)
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

## Confirmation (2026-08-04)

Forcing test: `test_bare_eval_on_non_owner_returns_moved`
(`frogdb-server/crates/server/tests/cluster_slots.rs`). 3-node cluster, probe slot 4483, owner
found by probing; control `GET <key>` on the non-owner returns `-MOVED` as expected, then a bare
`EVAL "redis.call('SET', KEYS[1], ...); return 1" 1 <key>` is sent to the same non-owner.

Result — **the hole is real**:

```
thread 'test_bare_eval_on_non_owner_returns_moved' panicked at
frogdb-server/crates/server/tests/cluster_slots.rs:1730:5:
bare EVAL naming a key owned by another node must be MOVED, got: Integer(1)
```

`Integer(1)` is the script's own return value: it ran to completion on the non-owner and its
`SET` landed there — an acked write on a node that does not own the slot, while the identical
`GET` on the same connection is redirected. Consequence 3/3 confirmed.

The test is left in the tree as `#[ignore]` naming this issue, so the fix has a ready-made
regression test: delete the `#[ignore]` when the ordering is fixed. Specced as a known bug:
FM-CLUSTER-030 in `.scratch/hardening/specs/cluster-failure-modes.md`, which cites this issue as
its gap (an `#[ignore]`d test is absent from `cargo nextest list`, so the failure-mode lint cannot
resolve it as a `Forced by` witness — re-point the row at the test when the `#[ignore]` goes).

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
