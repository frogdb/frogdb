# SPOP replication diverges primaries and replicas (nondeterministic verbatim propagation)

Status: needs-triage
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
