# Bare EVAL/EVALSHA/FCALL appear to bypass cluster slot validation (stage-ordering hole)

Status: done
PRD: [replication-cluster-rework/epoch-fold-redesign.md](../epoch-fold-redesign.md)
Type: AFK
Origin: rework-02 design research, 2026-08-04 (see
[migration-pause-barrier-brief-2026-08-04.md](../../migration-pause-barrier-brief-2026-08-04.md) §9)
Severity: likelihood 3/3 if real (every bare script on a non-owner node), consequence 3/3
(script executes locally on a non-owner instead of -MOVED — acked writes on the wrong node;
strictly wider than issue 03's finalization window)
Area: Cluster / dispatch ordering / scripting

## Resolution (2026-08-05)

**Root cause** — pure stage ordering, not a missing check. `validate_cluster_slots` already
computed the right verdict for a script (`is_cluster_exempt` refuses to exempt
`ConnectionLevelOp::Scripting`, and the scripting specs declare `KeySpec::Dynamic` +
`dynamic_keys` = the `numkeys` slice), but `PRE_DISPATCH_ORDER` ran `ConnectionCommand` — the
stage that *dispatches* the scripting family — before `ClusterSlotValidation`, so the script
short-circuited dispatch and the verdict was never taken.

**Fix shape** — hoist, do not duplicate. `DispatchStage::ClusterSlotValidation` moves from
position 14 to position 9 of `PRE_DISPATCH_ORDER`, immediately after `PauseGate` and immediately
before `ConnectionCommand`, and the ordering is pinned structurally by a new `MUST_PRECEDE` pair
`(ClusterSlotValidation, ConnectionCommand)` (plus `(CommandLookup, ClusterSlotValidation)`, which
keeps unknown-command/arity errors outranking a redirect as in Redis). One stage, one
`coordinator.route` call, one source of truth for the routing verdict; no slot logic is duplicated
into the scripting entry point.

The hoist is behavior-neutral for everything except scripts: every stage between the old and new
position is either cluster-exempt by strategy (`ServerWide`, `ClusterSlotSubcommand`'s `CLUSTER`,
and the non-scripting connection families) or keyless (`REPLCONF`/`PSYNC`/`WAIT`), and
`validate_cluster_slots` returns `None` before it touches `take_asking` in both cases. In
cluster-disabled mode the function still returns at its first `?`, so there is zero change there.
`MigratingTryAgain` deliberately stayed put: it has no exemption predicate, so hoisting it above
`ServerWide` would newly subject `MIGRATE` (`KeySpec::Dynamic`) to the presence probe.

**Files** — `frogdb-server/crates/server/src/connection/dispatch.rs` (stage order, stage arm,
`MUST_PRECEDE`, the two pinning tests), `frogdb-server/crates/server/src/connection/guards.rs`
(`is_cluster_exempt` doc records what now enforces the intent).

**Tests** — `test_bare_eval_on_non_owner_returns_moved` un-`#[ignore]`d;
`test_bare_script_family_on_non_owner_returns_moved` (EVAL_RO/EVALSHA/EVALSHA_RO/FCALL/FCALL_RO,
with the script and function library loaded *on the non-owner* so a NOSCRIPT cannot masquerade as
the redirect); `test_keyless_script_is_never_redirected` (the `numkeys 0` negative), all in
`frogdb-server/crates/server/tests/cluster_slots.rs`; `load_bearing_ordering_invariants` now also
tagged FM-CLUSTER-030.

**Spec** — FM-CLUSTER-030 rewritten from a KNOWN-BUG/`MISSING` row to the real contract, forced by
the four tests above.

**Residual** — a script's *undeclared* runtime writes are still unvalidated (issue 03), and a
multi-key script against a half-migrated slot still misses the `TRYAGAIN` probe (noted on
FM-CLUSTER-030 and issue 03).

**Audit of the other stage-9 key-carrying commands** — nothing else needs fixing. Every other
`ConnectionLevel` command that declares keys already takes the same `coordinator.route` verdict
through its own seam, or is exempt on purpose:

- `WATCH` (`KeySpec::All`, `ConnectionLevel(Transaction)`) — short-circuits even earlier, at
  `TransactionControl`, and calls `validate_watch_slots` there (issue 04 / FM-TXN-048).
- `SSUBSCRIBE` (`KeySpec::All`) and `SPUBLISH` (`KeySpec::First`), both
  `ConnectionLevel(PubSub)` — `is_cluster_exempt` exempts them, but the pub/sub executor routes
  each channel through `coordinator.route` itself
  (`pubsub_conn_command.rs:328-355`, `:493-510`), which is what per-channel redirects need: one
  `SSUBSCRIBE` can name several channels and must interleave a redirect per channel, which a
  single-slot gauntlet stage cannot express.
- `DEBUG` (`KeySpec::Dynamic`) — name-exempt on purpose (node-scoped admin, as in Redis); its
  `dynamic_keys` yields at most one key, so it cannot reach `check_migrating_multikey` either.
- `CONTAINER_COMMANDS` (`connection/util.rs:246`) is only a subcommand-name lookup table for
  ACL/stats attribution — it has no dispatch role and no key handling.

**Commits** — `c41454e6` (the hoist), `7ac577d4` (tests, spec, issue move).

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
FM-CLUSTER-030 in `specs/cluster.md`, which cites this issue as
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
