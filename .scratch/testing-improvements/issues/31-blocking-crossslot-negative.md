# Blocking multi-key commands don't have CROSSSLOT negative coverage

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: cluster / blocking commands (pubsub-streams area C)

## Context

`blocking_nil_shape_regression.rs:103` deliberately hash-tags every key onto the same slot
"so the command validates" for its blocking-nil-shape assertions — the cross-slot rejection
path itself is never exercised for blocking multi-key commands. The only CROSSSLOT test in the
suite is for `MSET` (`integration_cluster.rs:7736`); there is no equivalent for `BLPOP`,
`BLMPOP`, `BZMPOP`, or `BLMOVE`.

Single-key CROSSSLOT validation is known to work and is guarded at queue/dispatch time
elsewhere in the cluster path (see `guards.rs:567` and related), but nothing pins that a
blocking multi-key command presented with keys in different slots is rejected immediately with
`CROSSSLOT` and never enters the blocking/wait path. A regression here would manifest as a
client blocking forever (or against a wrong node) instead of getting a fast, correct error —
worse than a silent behavior change because it hangs.

Verdict (adversarial pass): CONFIRMED L2/C2 — no reframing needed.

## What to build

Integration tests, in cluster mode, that pick keys landing in different slots (no hash-tag
trick) for each of `BLPOP`, `BLMPOP`, `BZMPOP`, and `BLMOVE`, and assert an immediate
`CROSSSLOT` error with no blocking/timeout behavior observed.

## Acceptance criteria

- [ ] `BLPOP key1{slotA} key2{slotB} 0` (or equivalent non-tagged keys) returns `CROSSSLOT`
      immediately (bounded, non-blocking) in a cluster-mode integration test.
- [ ] Same coverage added for `BLMPOP`, `BZMPOP`, and `BLMOVE` (source/dest in different slots).
- [ ] Tests assert the command does not enter the blocking wait path (e.g., returns well under
      any configured timeout, or asserts before a timeout would matter).
- [ ] Existing hash-tagged blocking tests (`blocking_nil_shape_regression.rs:103`) are left
      intact — this adds the negative path, not a replacement.

## Blocked by

None - can start immediately

## References

- `server/tests/blocking_nil_shape_regression.rs:103`
- `server/tests/integration_cluster.rs:7736` (existing MSET CROSSSLOT test, pattern to follow)
- `.scratch/testing-improvements/audit/C-pubsub-streams.md` (`blocking-multikey-crossslot-rejection-untested`)
- `.scratch/testing-improvements/audit/verdicts-C.md`

## Resolution

Verified current behavior first: `ConnectionHandler::validate_cluster_slots`
(`server/src/connection/guards.rs`) runs at the `ClusterSlotValidation`
pre-dispatch stage (`server/src/connection/dispatch.rs`,
`PRE_DISPATCH_ORDER`), strictly *before* the terminal `Execute` stage that
would ever hand a command to the blocking/wait-queue machinery. Its
`SlotValidator::same_slot` check fires on the command's own keys
unconditionally (it doesn't even need to know which node owns them), so a
cross-slot blocking command is already rejected with `CROSSSLOT` before it
can ever reach `route_and_execute`/the shard's wait queue. The behavior was
correct; it just had no pinning test, exactly as the issue described.

Added cluster-mode integration tests, one per acceptance-criteria command
plus the two extra members of the blocking family the parent context called
out (BZPOPMIN/BZPOPMAX/BRPOPLPUSH), in
`server/tests/integration_cluster.rs` (new section following the existing
MSET/MULTI CROSSSLOT tests, sharing an `assert_blocking_multikey_crossslot_no_block`
helper):
- `test_blpop_cross_slot_returns_crossslot_immediately`
- `test_blmpop_cross_slot_returns_crossslot_immediately`
- `test_bzpopmin_cross_slot_returns_crossslot_immediately`
- `test_bzpopmax_cross_slot_returns_crossslot_immediately`
- `test_bzmpop_cross_slot_returns_crossslot_immediately`
- `test_blmove_cross_slot_returns_crossslot_immediately`
- `test_brpoplpush_cross_slot_returns_crossslot_immediately`

Each test picks two keys the same node owns in different cluster slots (no
hash-tag trick -- that's precisely the untested path), sends the command
with an effectively-infinite blocking timeout (`0`), and asserts three
things: (1) the exact `CROSSSLOT` wire error, (2) the reply arrives in well
under 1.5s (bounding out a full-blocking regression, which would otherwise
only surface as a 3s test-harness read timeout), and (3)
`ClusterTestNode::blocked_client_count()` (new passthrough added to
`frogdb-test-harness`, mirroring `TestServer::blocked_client_count()`) is `0`
immediately after -- the PUBSUB-style introspection asked for, proving no
waiter was even partially/transiently registered. A trailing `PING` on the
same connection confirms the RESP2 stream is still clean (no stray
out-of-band wake frame from a leaked waiter).

Also covered the standalone/cross-internal-shard case: FrogDB shards its
keyspace even without cluster mode (`internal_shard = CRC16(key) % 16384 %
num_shards`, `core/src/shard/partition.rs`), and
`ConnectionHandler::route_and_execute` (`server/src/connection/routing.rs`)
rejects any multi-key command whose keys land on different internal shards
with the identical `CROSSSLOT` wire error -- which is exactly why
`blocking_nil_shape_regression.rs`'s existing timeout tests hash-tag their
keys ("so the command validates"). That negative path had no coverage
either, so the same seven commands got a standalone analogue appended to
`redis-regression/tests/blocking_nil_shape_regression.rs` (existing
hash-tagged tests left untouched, as required): `blpop_cross_shard_...`,
`blmpop_cross_shard_...`, `bzpopmin_cross_shard_...`,
`bzpopmax_cross_shard_...`, `bzmpop_cross_shard_...`,
`blmove_cross_shard_...`, `brpoplpush_cross_shard_...`, all suffixed
`_returns_crossslot_immediately`, using a standalone server pinned to 4
shards and the same immediate-reply + `blocked_client_count() == 0` + `PING`
assertions (`frogdb_core` added as a dev-dependency of
`frogdb-redis-regression` to compute per-shard keys).

No bug found -- the CROSSSLOT rejection path was already correct in both
modes; this closes the coverage gap only. All 14 new tests plus the
pre-existing MSET/MULTI/EXEC CROSSSLOT tests and the untouched
`blocking_nil_shape_regression.rs` timeout tests pass, run 3x each for
stability (see commit for exact `nextest` summaries).
