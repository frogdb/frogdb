# A panic anywhere in a dependency kills a shard worker

Status: done
Type: bug (availability) + hardening (structural)
Severity: likelihood 2/3 (one unauthenticated trigger already known), consequence 3/3 (a shard
worker dies; every key it owns becomes unavailable) — score 6
Area: core / shard event loop

## Problem

`rg 'catch_unwind'` over `frogdb-server/crates/core/src/shard/` and
`frogdb-server/crates/server/src/` returns exactly one hit, and it is a test
(`transaction_conn_command.rs:621`). The shard event loop has **no panic isolation at all**, so a
panic raised anywhere below it — including inside a third-party dependency — takes the worker
down.

This is not hypothetical: round-2 issue 63 (`FT.SEARCH … LIMIT 0 0`) is an unauthenticated-
reachable shard panic, and it panics on an `assert_ne!` **inside tantivy/usearch** —
`search/src/wire.rs:121-122` parses, `:337-341` computes `limit: offset + limit == 0`, and the
C++/Rust index layer asserts. No `unwrap()` exists anywhere on the path, which is why a grep for
panicking constructs is the wrong instrument here (the campaign-2 W1 survey counted 62 such sites
and none of them was this bug).

The fix that generalizes is structural: catch at the boundary, so an unbounded class of
remote-crash defects degrades to an error reply.

## Fix

Wrap shard message execution in `std::panic::catch_unwind` (`AssertUnwindSafe` at the message
boundary), convert a caught panic into an `-ERR internal error` reply plus a counted metric and a
`error!` log carrying the command name, and keep the worker alive. Decide and record:

- Whether a caught panic should poison anything (an in-flight MULTI, a held continuation lock, a
  VLL entry) — a panic mid-transaction must not leave the lock owned by a dead command.
- Whether repeated panics on one shard should escalate (e.g. a counter that trips a health signal)
  rather than silently absorbing a loop.
- Interaction with `panic = "abort"` if any profile sets it — verify the release profile does not,
  or the isolation is inert where it matters.

Fix issue 63's arithmetic separately; the isolation is the backstop, not the fix.

## Forcing test

A shard-level test that injects a panicking command and asserts: the client gets an error reply,
the worker still answers the next command, the metric incremented, and no lock or transaction
state survives the panic. Plus the issue-63 regression once its arithmetic is fixed.

## Comments

Found by the campaign-2 chokepoint-lint survey, 2026-08-07, as the structural replacement for a
rejected `unwrap()`-grep lint (survey candidate C8).

## Resolution

Implemented 2026-08-07 in `b7df2845`. The seam and every decision below live in
`frogdb-server/crates/core/src/shard/panic_guard.rs`; issue 63's arithmetic was fixed
separately first (`563bdcd1`), as the issue required.

### Where the guards sit

One outer net plus four inner guards, because `catch_unwind` at the message boundary
alone keeps the worker alive but drops the reply oneshot — the client would hang until
its own timeout. The inner guards each answer in that path's own reply shape:

| Site (`PanicSite`) | Where | Reply |
|---|---|---|
| `command` | `dispatch_core.rs` `Execute` | `-ERR internal error` |
| `scatter` | `dispatch_core.rs` `ScatterRequest` | error-shaped `PartialResult` via `scatter_error_reply` |
| `transaction` | `dispatch_core.rs` `ExecTransaction` | `TransactionResult::Error` |
| `transaction_command` | `execution.rs` `execute_transaction`, per queued command | that slot only |
| `vll_execute` | `vll.rs` `handle_vll_execute` | error-shaped `PartialResult`, locks released |
| `message` | `event_loop.rs` data-plane arm | outer net; worker survives, no reply |

The event loop's **maintenance arms are deliberately left fail-stop** — a panic in
expiry/eviction/replication upkeep is not a per-client failure and there is no client to
answer, so absorbing it would hide corruption behind a healthy-looking shard.

Each catch emits one `error!` (shard, site, command, panic message) and one
`frogdb_shard_panics_isolated_total{shard, site}` tick through the typed handle
(`lint-metrics-chokepoint` passes); a dashboard panel was regenerated with
`just dashboard-gen` + `just helm-gen`.

### Decision (a) — poisoning semantics

Three pieces of command-scoped state are reset or released on the panic path:

1. **VLL locks.** `handle_vll_execute` releases exactly as on the success path, so
   `dequeue_for_execution`/`release_after_execution` stay paired across an unwind. Without
   this, the op's key intents survive and `executing_ops` stays incremented — which blocks
   every later request on those keys *and* any parked continuation lock forever. This is
   the load-bearing decision and is rowed as
   [FM-VLL-005](../../../../specs/vll.md#fm-vll-005--a-granted-op-panics-while-executing).
2. **`Store::suppress_touch`** is cleared, or the next command inherits a foreign
   `no-touch` and silently stops updating LRU/LFU metadata.
3. **Pending serve propagations** are dropped: a half-built propagation from a command
   that never completed must not be attributed to the next one.

The **continuation lock is deliberately not touched**. It is owned by a *connection*
running a cross-shard script/MULTI, not by the panicking command, and its release is
already tied to the coordinator's guard (`acquire_continuation_and_run` drops
`release_txs` on every path, panic included — FM-VLL-002). Force-releasing it here would
hand the shard to someone else while the script is still mid-flight.

**Inside MULTI**: the panic is caught **per queued command**, one frame below the
`ExecTransaction` guard. That command's slot in the EXEC array becomes
`-ERR internal error` and the remaining queued commands still run. Three reasons: it is
Redis's own rule (a runtime error inside EXEC fails that command, not the batch); it
keeps one contract rather than two (the single-command guard says the same thing); and
an EXECABORT-for-all would lie about commands that already applied when rollback mode is
off. Catching per command also keeps `execute_transaction`'s frame alive, so the
rollback snapshot taken before the panicking write is still in the undo list if the
batch's WAL write later fails.

*Residual hazard, recorded not fixed*: the dead command contributes no
`WriteCommandMeta`, so a mutation it half-applied is neither written to the WAL nor
broadcast to replicas. Isolation cannot make a torn write atomic — this is one more
reason a non-zero counter is always a bug to chase, never a steady state to tolerate.

### Decision (b) — escalation

**Counter and log only. No auto-kill, no health-signal trip, for now.** Auto-killing a
shard that panics repeatedly reintroduces exactly the availability loss this issue
removes: a client looping a bad command would take the shard down, which is the
pre-fix behaviour with extra steps. The `{shard, site}` label pair makes a panic loop
trivially visible on the dashboard and alertable on rate, which is what an operator
actually needs. Fail-stop is retained where isolation does not apply (worker-task
unwind outside the guarded boundary → `shard_supervisor` → `process::abort()`), so
genuinely structural failures still stop the node. Revisit if a real panic loop is ever
observed in the field.

### Decision (c) — `panic = "abort"`

**Verified absent.** No `[profile.*]` in any workspace manifest sets `panic`; the root
`Cargo.toml` defines `dev`, `release`, `docker` and `profiling` without it, so unwinding
(and therefore this isolation) is live in release builds. Nothing to flag. A future
`panic = "abort"` would silently make every guard here inert.

### Forcing tests

In `panic_guard.rs`, driving a real `ShardWorker` with an injected panicking command:

- `a_panicking_command_is_answered_with_an_error_and_the_shard_keeps_serving` — client
  gets `-ERR internal error`, counter == 1, `suppress_touch` is already cleared *before*
  the next command runs, the next command is answered normally, counter still 1.
- `a_panic_inside_exec_fails_only_that_command` — EXEC of `[FINE, BOOM, FINE]` returns
  `Success` with `[+OK, -ERR internal error, +OK]`.
- `a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_serving` (tagged
  `// FM-VLL-005`) — panics through a genuine latent path (`scatter_write_handler`'s
  `panic!` on an unregistered command) while holding a write lock; asserts the keyed
  error reply, the counter, an empty lock table, *and* that a second op on the same key
  is still grantable — the last is what actually proves `executing_ops` was decremented.

Plus 4 unit tests over the guard itself and the RESP-level issue-63 pin
(`regression_ft_search_limit_zero_zero_is_count_only_and_shard_survives`).

The `suppress_touch` reset was mutation-checked by hand (delete the reset in
`recover_from_panic` → the test must fail). It did *not* fail on the first attempt: with
the assertion placed after the liveness command, the healthy command reset the flag on
its own way out and the mutant survived. Reordering the assertion ahead of the liveness
command made it forcing. The other recovery steps were not individually mutation-checked
— a `cargo mutants` pass over `shard/panic_guard.rs`, `shard/vll.rs` and
`shard/dispatch_core.rs` would be the honest follow-up.
