# SPOP replication diverges primaries and replicas (nondeterministic verbatim propagation)

Status: done
Type: bug
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 3/3 (score 9)
Area: replication

## Context

Steady-state replication broadcasts the original client command verbatim — `record.handler.name()`
+ `record.args` (`core/src/shard/post_execution.rs:395-423`), with `args = command.args.as_slice()`
and no rewrite hook (`execution.rs:388-390,427-430`). The replica re-executes the same command
through full handler re-execution (`server/src/replication/executor.rs:56-82,121-134`).

`SpopCommand` is flagged `WRITE|FAST` only (`commands/src/set.rs:879-884`) and pops a random member
via a fresh RNG call on each node (`types/src/types/set.rs:279-288`, corroborated at `set.rs:284`
with a fresh `rand::rng()` per invocation). The primary broadcasts `SPOP <args>` verbatim; the
replica re-executes it with its own RNG and removes a *different* member. When the set has more
members than are popped (i.e. it isn't fully drained), primary and replica permanently diverge on
that key. The `NONDETERMINISTIC`/`NO_PROPAGATE` command flags exist (`core/src/command.rs:860-861`;
`commands/src/basic.rs:227-233`) but are never consulted in the broadcast path — they currently only
feed `COMMAND INFO`.

This is a confirmed live bug, corroborated by `slowlog_tcl.rs:16`, which documents in-repo that
"FrogDB does not implement replication command rewriting (SPOP->DEL, ... blocked BLPOP->LPOP) for
replication purposes." Redis rewrites `SPOP` to `SREM`/`DEL` before propagating specifically to
avoid this class of divergence. Existing coverage does not catch it:
`tcl_spop_propagate_as_del_or_unlink` (`set_tcl.rs:1038-1075`) only checks the local effect on one
node; `test_set_operations_replicate` (`integration_replication.rs:3409`) only exercises `SADD`,
never `SPOP`. There is no SPOP replication test anywhere in the suite.

## What to build

- Fix: rewrite `SPOP` before broadcast (e.g. to `SREM <popped-members>` or `DEL` when the set is
  fully drained), or otherwise deterministically suppress/replace verbatim propagation for
  nondeterministic commands — consulting the existing `NONDETERMINISTIC` flag in the broadcast path
  instead of leaving it dead.
- Test: primary+replica, `SADD` 5 members, `SPOP` 2 on the primary, `WAIT`, assert `SMEMBERS` equal
  on both nodes.
- Test: turmoil property test — random `SADD`/`SPOP` sequences against a primary+replica pair,
  assert quiescent equality after convergence.

## Acceptance criteria

- [ ] `SPOP` propagates deterministically (rewrite or equivalent) so primary and replica sets are
      identical after `WAIT`, for both partial-drain and full-drain cases
- [ ] Integration test: `SADD`/`SPOP`/`WAIT`/`SMEMBERS` equality on primary+replica
- [ ] Turmoil property test: random `SADD`/`SPOP` interleavings converge to equal sets
- [ ] `NONDETERMINISTIC`/`NO_PROPAGATE` flags are either consulted by the broadcast path or the
      dead-flag state is explicitly resolved (documented as info-only, or wired up)

## Blocked by

None - can start immediately

## References

- `core/src/shard/post_execution.rs:395-423` — verbatim broadcast, no rewrite hook
- `core/src/shard/execution.rs:388-390,427-430`
- `server/src/replication/executor.rs:56-82,121-134` — replica full re-execution
- `commands/src/set.rs:879-884` (SPOP flags), `:284` (fresh RNG per node)
- `types/src/types/set.rs:279-288`
- `core/src/command.rs:860-861`; `commands/src/basic.rs:227-233` — dead NONDETERMINISTIC/NO_PROPAGATE flags
- `server/tests/set_tcl.rs:1038-1075` — tcl_spop_propagate_as_del_or_unlink (local-effect-only)
- `server/tests/integration_replication.rs:3409` — test_set_operations_replicate (SADD only)
- `server/tests/slowlog_tcl.rs:16` — corroborating doc comment
- Source: `.scratch/testing-improvements/audit/E-replication.md` gap #1, `.scratch/testing-improvements/audit/verdicts-E.md` #1

## Resolution

Fixed 2026-07-23 (branch `worktree-agent-a520614f4f2045a9e`).

### Mechanism: `ReplicationOverride` (general deterministic-propagation seam)

A general rewrite mechanism, not an SPOP one-off — mirroring Redis's
`rewriteClientCommandVector` / `alsoPropagate` / `preventCommandPropagation`:

- `frogdb-core/src/command.rs`: new `ReplicationOverride` enum —
  `Rewrite(SmallVec<[SynthesizedCommand; 1]>)` (broadcast these synthesized
  commands instead of the original, in order) or `Suppress` (broadcast
  nothing). Deposited by a command execution via the new
  `CommandContext::rewrite_propagation(name, args)` (appending; repeated calls
  synthesize several commands) / `CommandContext::suppress_propagation()`.
  Threaded as `CommandEffects::repl_override` →
  `WriteCommandMeta`/`ScriptWriteRecord` → `WriteRecord.repl_override`
  (exhaustive destructures make a missed drain site a compile error).
- `frogdb-core/src/shard/post_execution.rs`: the replication-broadcast write
  effect resolves every record through a single `replication_forms()` helper:
  override rewrite, dynamic suppression, static `NO_PROPAGATE`, else verbatim.
  Works uniformly for single commands, MULTI/EXEC framing (rewrites substitute
  inside the group; an all-suppressed group ships no MULTI/EXEC at all),
  scatter parts, internal removals, and script sub-command writes. Only the
  broadcast consumes the override — WAL persistence, keyspace notifications,
  WATCH versioning, tracking invalidation, and search indexing still run from
  the original command record.
- Extensibility: issue 02 (served blocking pops never broadcast) can
  synthesize its own broadcast commands with `SynthesizedCommand` /
  `broadcast_command_on_shard` or deposit overrides from the waiter-satisfaction
  seam; nothing in the mechanism assumes SPOP.

### SPOP fix (`frogdb-commands/src/set.rs`)

Redis-parity propagation (`t_set.c`):

- partial drain → rewrite to `SREM <key> <popped members…>`;
- full drain (incl. `count > cardinality` and last-member single pop) →
  rewrite to `DEL <key>`;
- nothing popped (missing key, `SPOP key 0`) → `write_was_noop = true`; the
  whole effect pipeline is skipped (also fixes previously-wrong `spop`
  keyspace events + WATCH dirtying + verbatim broadcast on missing keys).
- SPOP is now flagged `RANDOM | NONDETERMINISTIC` (matching Redis's
  `write random fast`).

### Dead-flag criterion resolved: both flags are now live

- `NO_PROPAGATE` is consulted by the broadcast effect (a flagged write never
  ships; local effects still run) — pinned by
  `no_propagate_flag_suppresses_broadcast`.
- `NONDETERMINISTIC` on a WRITE command is an enforced replication contract:
  the broadcast effect debug-asserts that such a record never reaches verbatim
  propagation (it must deposit an override or declare `write_was_noop`) —
  pinned by `nondeterministic_write_without_override_panics_in_debug`. Both
  flag docs in `core/src/command.rs` now state this.
- `slowlog_tcl.rs` / `set_tcl.rs` doc comments claiming "FrogDB does not
  implement replication command rewriting" updated.

### Turmoil replica support (enabler for the property test)

Real primary+replica pairs now run under turmoil — the previously
"deferred to Phase 5" abstraction noted in `simulation.rs`:

- `server/src/server/replication_init.rs`: `#[cfg(feature = "turmoil")]`
  replica connect factory (`turmoil::net::TcpStream`) so the replica dials its
  primary through the simulated network.
- `server/src/connection.rs`: the primary-side PSYNC handoff turmoil stub
  ("PSYNC handoff not supported in turmoil simulation mode") was stale — the
  handoff seam takes a type-erased `BoxedStream`, which turmoil's `TcpStream`
  boxes into directly. Both cfg arms now perform the real handoff (non-turmoil
  behavior unchanged).
- `tests/common/sim_helpers.rs`: `real_frogdb_primary` / `real_frogdb_replica`
  bootstrap helpers (unique per-host data dirs; full sync ships the minimal
  RDB, all data flows through the live stream).

### Tests

- Unit (`frogdb-core`, `shard::post_execution::tests`): rewrite replaces
  verbatim; multi-command rewrites ship in order; `Suppress` ships nothing;
  `NO_PROPAGATE` consulted; MULTI/EXEC substitution; all-suppressed
  transaction ships no framing; debug-assert on unrewritten nondeterministic
  writes. 20/20 pass.
- Integration (`server/tests/integration_replication.rs`, primary+replica over
  real TCP, in-memory + persistence variants): `test_spop_partial_drain_replicates`
  (SADD 5 / SPOP 2 / WAIT / SMEMBERS equal),
  `test_spop_full_drain_replicates_as_delete`,
  `test_spop_count_exceeding_cardinality_replicates`,
  `test_spop_single_member_replicates`.
- Turmoil property test (`server/tests/simulation.rs`,
  `test_spop_replication_convergence_random_workload`): seeded random
  SADD/SPOP interleavings (150 ops over 3 keys; single, counted, and
  oversized-count SPOPs) against a real primary+replica pair under turmoil's
  simulated network, across 3 seeds (each seed varies both the workload and
  the message-timing schedule); quiescent equality of every set asserted after
  WAIT-acked quiescence.

Test evidence (Blacksmith testbox, aarch64 Linux, 2026-07-23):

- targeted: `Summary [0.141s] 20 tests run: 20 passed, 833 skipped`
  (frogdb-core post_execution) and
  `Summary [1.155s] 8 tests run: 8 passed, 1807 skipped` (SPOP integration)
- full suite (`just test`): `Summary [204.945s] 6864 tests run: 6864 passed
  (2 slow), 1 skipped`
- full turmoil simulation suite (incl. the new convergence property test):
  `Summary [1.888s] 88 tests run: 88 passed, 594 skipped`
- `just lint` (clippy, whole workspace): clean.

### Remaining members of the nondeterministic-write class (not fixed here)

Surveyed all command handlers for RNG/clock use. SPOP was the only RNG-based
write. The remaining divergers are all *clock*-based:

- `XADD` with `*` (and `ms-*`): entry ID minted from the node clock
  (`StreamId::generate` → `SystemTime::now`), propagated verbatim → replica
  mints a different ID. Redis rewrites XADD to the concrete generated ID.
  Same-mechanism fix is straightforward (deposit a rewrite with the concrete
  ID substituted) but XADD's approximate trim (`MAXLEN ~ … LIMIT`, node-local
  granularity simulation in `StreamValue::trim`) also diverges and needs the
  Redis-style exact-count trim rewrite — deserves its own issue rather than a
  partial fix here.
- `TS.ADD`/timeseries auto-timestamps (`SystemTime::now` in `timeseries.rs`):
  same shape as XADD `*`.
- Relative TTLs (`EXPIRE`/`SET EX`/`GETEX`/`HEXPIRE`…): replica computes its
  own absolute deadline — already tracked as audit E-replication gap #3.

None of these were flagged `NONDETERMINISTIC`, so the new debug-assert does
not fire for them; whoever fixes them should add the flag together with the
rewrite (the assert then enforces it).
