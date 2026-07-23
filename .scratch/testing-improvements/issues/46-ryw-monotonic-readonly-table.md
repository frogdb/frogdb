# Table-driven READONLY enforcement + monotonic replica-read regression coverage

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: Replication

## Context

`consistency.md` carries [Design intent] rows for read-your-writes (line 27) and monotonic reads
(line 38) that are never converted to executable tests. READONLY enforcement on replicas exists and
is centrally wired on the WRITE command flag, but the only tests exercising it cover a hand-picked
subset of commands: `integration_replication.rs:3940-3975` checks SET/DEL/ZADD only. Nothing proves
the enforcement generalizes to every WRITE-flagged command in the registry (GETEX, GETDEL, SETRANGE,
EXPIRE, COPY, PFMERGE, etc.) — a hand-picked list can pass while a newly added WRITE command slips
through un-enforced.

Verdict on this gap is ADJUSTED to L2/C1: READONLY enforcement is centrally implemented on the WRITE
flag (not per-command special-casing), so this is not a suspected live bug — the missing coverage
only pins design intent and guards against a future regression in the central enforcement path or in
flag assignment for a new command. Replica lazy-expiry-on-read behavior is also untested but is a
distinct sub-topic (see task 32, `replica-expiry-ttl-drift`, E#3) and should not be duplicated here.

No monotonic-read test exists either: nothing writes to the primary, waits for replica ack, then
loops reads on the replica asserting the value never regresses to a pre-write state.

## What to build

1. A registry-driven (not hand-listed) integration test that enumerates every command carrying the
   WRITE flag from the command registry and, for each, executes it against a replica connection,
   asserting a READONLY rejection. The test must fail if a new WRITE command is registered without
   READONLY enforcement — i.e., it walks the registry rather than a fixed table maintained by hand.
2. A monotonic-read regression test: write a value to the primary, `WAIT` for replica
   acknowledgment, then perform a tight loop of reads on the replica asserting the value is never
   observed reverting to its pre-write state.

## Acceptance criteria

- [x] Registry-driven test enumerates every WRITE-flagged command (not a hand-picked subset) and
      asserts READONLY error against a replica connection for each.
- [x] Test fails automatically if a future WRITE command is added to the registry without READONLY
      enforcement (proves registry-driven, not list-driven).
- [x] Monotonic-read test: primary write, WAIT ack, tight replica-read loop asserts the value never
      reverts to a prior state across repeated reads.
- [x] Replica lazy-expiry-on-read behavior explicitly out of scope here; cross-reference task 32
      (`replica-expiry-ttl-drift`, E#3) rather than duplicating.
- [x] `consistency.md` RYW (line 27) and monotonic-reads (line 38) rows relabeled from
      [Design intent] to [Tested] once these land.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/E-replication.md #7 (`read-your-writes-monotonic-staleness-untested`)
- .scratch/testing-improvements/audit/verdicts-E.md #7 (ADJUSTED L2/C1)
- frogdb-server/crates/server/tests/integration_replication.rs:3940-3975
- consistency.md lines 27 (RYW), 38 (monotonic reads)
- Task 32 (`replica-expiry-ttl-drift`, E#3) — related but distinct

## Resolution

Both tests added to `frogdb-server/crates/server/tests/integration_replication.rs`, appended after
the existing hand-picked `test_replica_readonly_enforcement`:

- `test_replica_rejects_every_write_command` — builds the real production registry
  (`frogdb_server::register_commands`, the same function `server/init.rs` calls at startup — not a
  test-only stand-in), filters by `CommandFlags::WRITE`, and issues each command against a live
  replica connection in a `start_primary_replica_pair` harness. A per-command `special_args_for`
  table supplies realistic minimal arguments for commands whose arity/shape needs it (e.g.
  `XREADGROUP`, `ZUNIONSTORE`, `JSON.STRAPPEND`); everything else falls back to filler args sized
  to the command's declared `Arity`. Argument *validity* doesn't affect the result being tested:
  `PreChecks` (which houses READONLY enforcement in `run_pre_checks`, `connection/guards.rs`) runs
  before `Arity` validation in `PRE_DISPATCH_ORDER`, and `write_defers_to_cluster_redirect` — the
  only path that would inspect key args before the READONLY check — short-circuits to `false` in
  non-cluster mode, so no `entry.keys(args)` call happens under malformed args. The test asserts
  `write_commands.len() >= 100` as a canary against `register_commands`/`CommandFlags::WRITE`
  changing shape silently.
- `test_replica_read_monotonic_after_primary_writes` — primary writes an increasing integer
  sequence (1..=100) to a hash-tagged key; a tight loop reads the replica after each write (3 reads
  per round) and asserts each observed value is `>=` the last one seen. After the loop, `WAIT 1
  5000` confirms replica ack, then a bounded poll loop confirms the replica converges to the final
  written value, with the same non-regression assertion applied throughout, plus a final
  `windows(2)` check across the whole observed sequence.

**Result: registry enumerated 171 WRITE-flagged commands from the production registry as of this
run. Every single one correctly returned a `READONLY` error against a replica — no live bug found.**
This closes out the "any WRITE command a replica accepts is a real bug" concern raised in the task
brief (re: issue 33's pre-checks changes): there is no accept-on-replica regression to fix. The
value of this task is the safety net itself — any future WRITE command that skips the flag or
otherwise bypasses `run_pre_checks` will now fail `test_replica_rejects_every_write_command`
automatically, with no list to remember to update.

Replica lazy-expiry-on-read was not touched; it remains tracked under task 32
(`replica-expiry-ttl-drift`, E#3) as this issue specifies.

`website/src/content/docs/architecture/consistency.md` updated:
- Read-Your-Writes table gained a `WAIT`-acked write, then read from the replica` row marked
  **Tested**, citing `test_replica_read_monotonic_after_primary_writes`. (The same-connection RYW
  row was already relabeled **Tested** by an unrelated prior commit, issue 56's Jepsen `ryw`
  workload — not part of this task's contribution.)
- `### Monotonic Reads` relabeled `[Design intent]` -> `[Tested]`, citing the same test, and noting
  it exercises a stronger cross-node (primary-write / replica-read) case than the original
  same-connection framing.
- "Reads from a Replica" section cross-references the Monotonic Reads section so the two rows read
  consistently.

Build/test discipline: `just fmt frogdb-server` (no changes needed), `just lint frogdb-server`
(clean, both default and `--features turmoil --tests`), and the three new/touched tests run
together 3 consecutive times locally, all green (~1.03-1.09s per run). See the parent report for
exact nextest summary lines.
