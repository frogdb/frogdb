# A write script aborted by `lua-time-limit` has its partial effects committed *and* replicated

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/09 F4 · MASTER.md §3 (consistency violations), §2 T6, §7 (decision required)
Score: severity 5 · likelihood 3 · effort 3 · priority 18
Area: frogdb-core / scripting

## Context

When a script exceeds `lua-time-limit`, the instruction hook aborts it mid-way — and the shard
then commits and replicates whatever the script had already written. The result is a
partially-applied, partially-replicated script. Redis never does this: its busy threshold only
starts replying `BUSY` to *other* clients, and a script that has written is explicitly
`UNKILLABLE`. FrogDB's own kill path honours that rule and returns `Unkillable` when
`has_writes`; the timeout path does not consult it at all.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly. MASTER.md §7 lists the script-timeout write policy as a decision that must
be settled before the test can assert anything, hence `needs-triage`.

## Evidence

The instruction hook (`scripting/src/sandbox.rs:209-228`) raises
`mlua::Error::RuntimeError("BUSY script running for {} ms")` on elapsed >
`timeout_ms + grace_ms` with **no `has_writes` consultation** — it has no access to the
execution state at all, only the kill flag. The abort propagates out of `execute`, but
`core/src/shard/scripting.rs:114-130` has already drained `ctx.effects.script_writes` and
calls `run_script_write_effects` unconditionally — the comment at `:126-129` states this is
deliberate ("Runs even when the script itself errored mid-way"), which is right for a
*script-raised* error and wrong for a *server-imposed* abort. No test anywhere sets
`lua_time_limit_ms` low and asserts what survives. Default `lua_time_limit_ms` is 5000
(`core/src/scripting/config.rs:40`). Contrast the kill path at `lua_vm.rs:472-478`, which
returns `Unkillable` when `has_writes`.

## What to fix

1. Give the timeout path access to the execution state so it can distinguish an abort of a
   read-only script from an abort of a script that has already written.
2. Implement the chosen policy from `## Options` below — either roll the partial effects back,
   or let them survive and guarantee they replicate as one MULTI/EXEC-framed batch.
3. Whichever policy is chosen, make the propagated batch and the primary's applied set
   identical, so primary and replica agree exactly on the partial set.
4. Reconcile the deliberate comment at `core/src/shard/scripting.rs:126-129` with the chosen
   policy so the next reader is not misled.

## Options

Reproduced verbatim from proposals/09 F4:

- *(a)* `shard_driver` with a low limit and the `fake-wal` seam: assert the WAL contains a
  MULTI/EXEC-framed prefix of the writes (effort 3). Deterministic, no replica.
- *(b)* Two-node server integration: assert primary and replica converge after the abort
  (effort 4). Proves the real consequence, slower and clock-sensitive.
- **Recommendation**: (a) first as the regression net; (b) once, as the consistency proof.

The policy question the test forces: assert either "no writes survive" or "writes survive and
replicate identically" — but *pin* one.

## Acceptance criteria

Criterion 1 was **restated** when option A was chosen (see `## Resolution` below): under
Redis/Valkey parity a write script is never aborted at all, so "the write script returns BUSY"
is no longer the behaviour to assert. What replaces it is the split below.

- [x] With `lua-time-limit` set low, a script that overruns it **without writing** returns a
      `BUSY` error to the client (RESP error *code* `BUSY`), and a script that has **written**
      is not aborted and runs to completion.
      (`a_read_only_script_that_overruns_the_time_limit_is_aborted_with_busy`,
      `a_write_dirty_script_is_never_aborted_by_the_time_limit`)
- [x] The number of keys actually present on the primary equals the number the replica sees —
      primary and replica agree exactly on the applied set. Asserted at level 3 against the
      propagated frames, which is the side the criterion is about; no `WAIT`/replica needed.
- [x] The propagated batch is MULTI/EXEC-framed. Asserted via the `RecordingBroadcaster`
      rather than the `fake-wal` seam — it observes the replication side directly.
- [x] The chosen policy is asserted explicitly: **option A**, "a write-dirty script is never
      aborted by the deadline", is the subject of
      `a_write_dirty_script_is_never_aborted_by_the_time_limit` and of three unit tests on the
      hook decision itself.

## Test boundary

Level 4 (server integration) for the full property — it needs `CONFIG SET lua-time-limit` and a
replica. The primary-only half (partial effects present + framed) is worth doing first at level
3 (`shard_driver`) because it is cheap and deterministic; level 3 alone cannot observe the
replication half, which is why the level-4 case is still required.

## Depends on

issue 05 (I5 — "shard busy running a script" fixture),
`.scratch/testing-improvements-round2/issues/`

## Verification of the premise (2026-08-02)

The premise is **half right**, and the half that is wrong is the half this issue is filed under.

Confirmed by a new level-3 scenario
(`frogdb-server/crates/shard-harness/tests/script_timeout_effects.rs`), run with
`lua_time_limit_ms = 50`, `lua_timeout_grace_ms = 0` and
`for i=1,1e9 do redis.call('SET', KEYS[1]..i, i) end`:

* The script **is** aborted mid-way and the completed writes **do** survive. Measured:
  9985 keys applied on the primary.
* **There is no replication divergence.** `run_script` drains one `script_writes` vec and that
  same vec drives both the store effects and the propagation, and
  `run_script_write_effects` (`core/src/shard/post_execution.rs:635-661`) selects
  `EffectScope::Transaction` whenever `writes.len() > 1`, which frames the batch as
  `MULTI … EXEC`. Measured: 9985 applied, 9987 frames propagated — `MULTI`, 9985 × `SET`,
  `EXEC`. So acceptance criteria 2 and 3 already hold, and criterion 3's `fake-wal` route was
  not needed: the `RecordingBroadcaster` observes the replication side directly, which is the
  side the criterion is actually about.
* Acceptance criterion 1 was **broken for a different reason**: `ScriptError::Timeout`
  rendered as `ERR BUSY Lua script running. …`, i.e. the RESP error *code* was `ERR`, with
  `BUSY` demoted to prose. `website/src/content/docs/architecture/glossary.md` already
  documents this error as `-BUSY`. Fixed in `core/src/scripting/error.rs` — the code is now
  `BUSY`. This is policy-independent, so it was fixed rather than deferred.

What is therefore left of this issue is **only** the policy question in `## Options`, which is
a product decision, not a defect. Per the standing rule ("if a fix requires a product
decision, stop and ask"), it is recorded below rather than guessed at.

## Open question — does a server-imposed abort get to keep its partial writes?

Both candidate answers are internally consistent; they trade atomicity against availability,
and the choice has an ops story attached, so it is not the implementing agent's to make.

**Option A — a write-dirty script is never aborted (Redis/Valkey parity).**
Redis's `busy-reply-threshold` (née `lua-time-limit`) *never* terminates a script: it only
starts answering `-BUSY` to other clients, and once the script is `SCRIPT_WRITE_DIRTY`,
`SCRIPT KILL` returns `-UNKILLABLE` and the only exit is `SHUTDOWN NOSAVE`. FrogDB already
implements exactly this rule on its *kill* path (`LuaVm::request_kill` →
`ScriptError::Unkillable`); the timeout path simply does not consult `has_writes`. Making it
consult the flag — abort read-only scripts on the deadline, never abort a script that has
written — would make the two paths agree and delete the partial-application class outright.

Shape of the change: hoist `has_writes` out of `ExecutionState`'s mutex into an
`Arc<AtomicBool>` shared by `LuaVm`, `ScriptCommandGate` and a new
`TimeoutHook.has_writes` field (an atomic, not the mutex, because `mark_write` already holds
that mutex on the same thread the mlua instruction hook runs on — re-entrancy hazard), then
return `Ok(VmState::Continue)` instead of the BUSY `RuntimeError` when the flag is set.
Roughly 40 lines, no new seams.

Cost: a runaway write script blocks its shard **forever**. Today `lua-time-limit` is the only
thing that stops it, because `request_kill` already refuses on `has_writes`. Redis accepts
exactly this cost; FrogDB shards are single-threaded, so the blast radius is one shard rather
than the whole node. Needs an operator answer to "what do I do now" (`SHUTDOWN NOSAVE`
equivalent?) before it can ship.

**Option B — abort, and pin "writes survive and replicate identically" as the policy.**
This is today's behaviour. It is already replication-safe (see above), it bounds script
runtime, and it is defensible under "scripts are not transactions". The cost is that the
surviving prefix is wall-clock dependent — not reproducible, and the client that received
`-BUSY` is told nothing about how much landed. Redis has no equivalent behaviour to point at.

**Option C — abort and roll back.** Rejected without a decision needed: script effects are
applied to the store as they execute and there is no undo log or snapshot for them, so this
is a large piece of new machinery for a case Redis solves by not aborting.

**Recommendation: A**, because it makes the timeout path agree with the kill path that is
already in the tree, and because "partially applied, non-reproducible" is a worse failure for
a database than "one shard is stuck and the operator is told why". But B is a legitimate
product choice for a system that would rather stay available, and if B is chosen the only work
left is to say so in the comment at `core/src/shard/scripting.rs` (already amended to state
the facts and point here) and to tick criterion 4.

**Blocking on:** an answer of A or B. Everything else in this issue is done.
**Answered 2026-08-04: A.** See `## Resolution` below.

## Work landed so far

- `frogdb-server/crates/core/src/scripting/error.rs` — `ScriptError::Timeout` now renders with
  the `BUSY` error code instead of `ERR BUSY`.
- `frogdb-server/crates/core/src/shard/scripting.rs` — the comment at the unconditional
  `run_script_write_effects` call now distinguishes a script-raised error from a
  server-imposed abort, states the applied-set/propagated-batch identity, and points at this
  open question instead of implying the behaviour is intended (criterion 4, partially).
- `frogdb-server/crates/shard-harness/tests/script_timeout_effects.rs` — level 3, 2 tests:
  `a_script_that_overruns_the_time_limit_is_aborted_with_busy` (RED before the error.rs fix),
  `a_timed_out_write_script_replicates_exactly_what_it_applied` (criteria 2 + 3).

No `FM-REPLICATION-*` row was added: `scripts/failure-modes.py`'s `NEXTEST_CRATES` does not
include `frogdb-shard-harness`, so a row naming these tests would fail `just lint-failure-modes`
in the "test does not exist" direction. The row is worth writing once the policy is settled and
the level-4 (two-node) half is added in `frogdb-server`, which *is* a listed crate.

## Resolution: Option A implemented (2026-08-04)

The user chose **option A**. `lua-time-limit` now bounds a script only until it writes; a
write-dirty script is never aborted by the deadline, exactly as Redis/Valkey's
`busy-reply-threshold` behaves. This makes the timeout path agree with the kill path that was
already in the tree, and it deletes the partially-applied-script class rather than pinning a
policy for it: a server-imposed abort is now reachable only for a read-only script, which by
construction has nothing to leave behind.

**What changed**

- `frogdb-server/crates/scripting/src/sandbox.rs` — `TimeoutHook` gained a
  `write_dirty: Option<Arc<AtomicBool>>` field, and the deadline branch of the instruction hook
  now goes through a new pure predicate `sandbox::deadline_aborts(elapsed_ms, budget_ms,
  write_dirty)` — `elapsed > budget && !write_dirty`. When the flag is set the hook returns
  `Ok(VmState::Continue)` and the script keeps running. The predicate's doc comment carries the
  policy, the Redis citation, and the **operator escape**: a runaway *write* script holds its
  shard until `SHUTDOWN NOSAVE`, same as Redis, with a blast radius of one shard because FrogDB
  shards are single-threaded. The kill-flag check is untouched and still runs first — the
  write-dirty rule is enforced where the flag is *raised*, not where it is read. FUNCTION LOAD's
  hook passes `write_dirty: None` (library top-level code cannot call `redis.call`).
- `frogdb-server/crates/core/src/scripting/lua_vm.rs` — `has_writes` moved out of
  `ExecutionState`'s mutex into a `LuaVm.write_dirty: Arc<AtomicBool>` shared with the gate and
  the hook. It has to be an atomic: the mlua instruction hook runs on the very thread that holds
  that mutex while `mark_write` records a write, so reading it through the mutex would deadlock
  re-entrantly. `mark_write`/`has_writes`/`request_kill` now use the atomic; `request_kill` no
  longer needs the state lock at all (it can no longer fail on lock contention).
  `prepare_execution` and `cleanup_execution` clear the flag alongside `ExecutionState::reset`.
- `frogdb-server/crates/core/src/scripting/gate.rs` — `ScriptCommandGate` carries the
  `write_dirty` atomic and raises it in `mark_write`. Its `state: Arc<Mutex<ExecutionState>>`
  field became dead as a direct consequence and was removed rather than left to warn.
- `frogdb-server/crates/core/src/shard/scripting.rs` — the `run_script_write_effects` comment
  (criterion 4) now states the settled policy: only a *script-raised* error can leave writes
  behind, which is what Redis does too; a server-imposed abort cannot, because neither the
  deadline nor `SCRIPT KILL` terminates a write-dirty script.
- `frogdb-server/crates/core/src/scripting/config.rs` — `lua_time_limit_ms` /
  `lua_timeout_grace_ms` doc comments say the limit bounds a script only until it writes.

**Tests**

- `frogdb-server/crates/shard-harness/tests/script_timeout_effects.rs` — both level-3 tests were
  reworked. Under option A the old `for i=1,1e9 do SET end` probe never terminates, so **both
  probes are now bounded** (wall-clock deadline plus an iteration cap):
  - `a_read_only_script_that_overruns_the_time_limit_is_aborted_with_busy` — read-only overrun
    still aborts with the `BUSY` error code, applies nothing, propagates nothing.
  - `a_write_dirty_script_is_never_aborted_by_the_time_limit` (replaces
    `a_timed_out_write_script_replicates_exactly_what_it_applied`) — a script that SETs, spins
    ~10× past the limit, then SETs again returns its value, lands **both** keys, and propagates
    exactly `MULTI, SET, SET, EXEC`. RED against the pre-fix code (BUSY reply, one key).
- `frogdb-server/crates/scripting/src/sandbox.rs` — three new unit tests on the hook seam:
  `test_deadline_aborts_only_a_clean_script` (the pure predicate at every corner),
  `test_hook_aborts_an_overrunning_clean_script`, `test_hook_never_aborts_a_write_dirty_script`,
  plus `test_hook_kill_flag_beats_write_dirty` pinning that the hook's kill check is unaffected.
- Kill-path parity needed no change and is still covered by
  `core::scripting::lua_vm::tests::test_kill_request` (write-dirty → `Unkillable`).

**Verified**: `just check frogdb-scripting`, `just check frogdb-core` (clean, no warnings),
`just test frogdb-scripting` 70/70, `just test frogdb-core scripting` 78/78,
`just test frogdb-shard-harness script_timeout` 2/2. The frozen redis-regression timeout tests
are unaffected — they all use `while true do end`, which is read-only and still aborts.
