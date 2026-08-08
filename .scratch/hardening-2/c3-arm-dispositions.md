# C3 arm dispositions — FINAL (decision 4: investigate-and-propose)

Status: **LANDED.** Investigation complete, both forcing tests written and passing, dispositions
approved, and the C3 lint built on top of them: `scripts/continuation-lock-gate.py`, recipe
`just lint-continuation-lock`, 14th member of `just lint-gates`. The lint is a **count pin**, not
a full 64-arm classification — see [`docs/agents/seam-lints.md`](../../docs/agents/seam-lints.md)
(§ "`lint-continuation-lock`: a count pin instead of a full classification") for the shape and the
script's module docstring for why. Both arms below are pinned `EXEMPT` with their reason and their
forcing-test name, and the lint fails if either test disappears.

The two known bypasses found alongside them — `CoreMsg::ExecTransaction` (round-2 issue 50) and
`ScriptingMsg::FunctionCall` (hardening-2 issue 05) — are pinned as tracked named-gap entries:
they warn on every run and hard-fail the moment either arm gains a gate call, forcing promotion to
`GATE` when the fix lands.

Supersedes an earlier reading-only write-up (removed; see git history). The gate and the two
arms are unchanged from it; this document records the settled disposition, the concrete
evidence, the forcing-test name, and whether a real bug was found.

## The gate (unchanged)

`ShardWorker::can_execute_during_lock(conn_id)` (`core/src/shard/worker.rs:855-862`): rejects work
from any connection that is not the continuation-lock owner while a continuation lock is held. C3
will assert that every shard-message arm mutating state either calls this gate (GATE) or is
classified EXEMPT with a documented reason.

---

## Arm 1 — `VllMsg::VllExecute` → `handle_vll_execute` (`core/src/shard/vll.rs:40-75`)

`execute_scatter_part(&op.keys, &op.operation, 0)` — `conn_id` hardcoded to `0`, no gate call.

### FINAL DISPOSITION: **EXEMPT** — the VLL two-phase protocol is the isolation seam.

### Evidence (settling the WIP open question)

The open question was: does the VLL queue actually serialize a held continuation lock against a
*different* connection's `VllExecute` drain? Read of `frogdb-server/crates/vll/src/shard.rs`
(`VllShardState`) settles it — **yes, completely**. The continuation lock and the op queue are the
*same* object, and three interlocking rules make a foreign mutation mid-lock unreachable:

1. **Grant requires a fully drained shard.** `request_continuation_lock` (shard.rs:302-326) grants
   only when `is_drained()` — `executing_ops == 0` **and** the queue is empty (shard.rs:334-336). A
   dequeued-but-still-executing op keeps `executing_ops > 0`, so the lock cannot be granted over an
   in-flight op; an undrained shard *parks* the request instead.
2. **Drain barrier while held OR parked.** `enqueue_lock_request` refuses every incoming SCA op with
   `ShardBusy` when `continuation_held_or_pending()` is true (shard.rs:145-151, 329-331). So while a
   foreign connection holds — or is even waiting for — the continuation lock, no new op can be
   enqueued.
3. **`dequeue_for_execution` only returns queued ops.** With enqueue refused, the queue stays empty,
   so `handle_vll_execute`'s `dequeue_for_execution(txid)` returns `None` and the arm sends a default
   `PartialResult` **without executing anything** (`vll.rs:45-48`).

Therefore an op only ever reaches `execute_scatter_part` if it was legitimately enqueued, which can
only happen when no continuation lock is held or pending. Any op that executes to *drain* the shard
(so a parked continuation request can be granted) runs strictly **before** the lock is held, and
belongs to whoever enqueued it — never a foreign connection mid-lock. The hardcoded `conn_id = 0` is
a drain-path sentinel that never races the lock owner; re-checking `can_execute_during_lock` here
would be redundant with the queue ordering that already granted the op.

### Forcing test

`vll_execute_cannot_mutate_a_held_key_under_a_foreign_continuation_lock`
(`core/src/shard/vll.rs`, `mod tests`).

Connection A holds the continuation lock; connection B submits a VLL **MSET write** on a held key.
The test asserts B's enqueue is refused with `ShardBusy`, `handle_vll_execute` for B mutates nothing
(`!store.contains(key)`), and the lock stays owned by A. A positive control then releases the lock
and re-runs the identical op, asserting it **does** execute and mutate — proving the refusal was the
lock, not a broken op. Fails if a future change lets SCA work enqueue under a held continuation lock
(the mutation would land mid-lock).

### Real bug? **NO.** The exemption is sound; isolation holds.

---

## Arm 2 — `CoreMsg::GetVersion` (`core/src/shard/dispatch_core.rs:101-139`)

Calls `purge_if_expired(key)` (physically removes already-expired keys) and
`apply_lazy_purge_effects_no_version_bump()` (fires tracking invalidation, search-index deletion,
`expired` keyspace notification, XREADGROUP drain), with no gate call.

### FINAL DISPOSITION: **EXEMPT with documented reason** — lazy expiry of already-dead keys, no version bump.

### Evidence

Read of the store seams and the worker drain confirms both isolation-preserving properties:

1. **Only already-expired keys are purged.** `purge_if_expired` → `check_and_delete_expired`
   (`store/hashmap.rs:1166-1168`) removes a key only if it is past its deadline. A key still live at
   watch time is left in place, so a continuation-lock owner never sees a key it holds vanish out
   from under a live view. `live_at_watch` is computed by the non-destructive `exists_unexpired`
   probe (`store/hashmap.rs:961-969`) before the purge.
2. **The version bump is withheld.** `apply_lazy_purge_effects_no_version_bump` calls
   `drain_lazy_purge_effects(false)` (`worker.rs:729-731`); with `bump_version == false` the
   `bump_versions_for` branch (`worker.rs:836-850`) is skipped. So purging an already-expired key
   does not advance any slot version — including a slot shared with a *live* watched key — and the
   lock owner's WATCH set is not spuriously aborted (F3: an already-stale watch is a nonexistent
   watch). The client-visible removal effects still fire, matching Redis/Valkey, which emit `expired`
   on lazy expiry regardless of the triggering command.

No *semantic* write occurs: the only mutation is the physical eviction of keys already logically
dead, which the lock owner would itself observe as absent. This is not a write the continuation lock
is there to protect against.

### Forcing test

`get_version_purges_only_expired_keys_without_bumping_under_continuation_lock`
(`core/src/shard/dispatch_core.rs`, `mod tests`).

While a foreign connection holds the continuation lock, a live key and an already-expired key —
hash-tag colocated on one slot (`{s}`) so a spurious bump would corrupt the live key's version — are
watched via `GetVersion`. Asserts the live key survives (`exists_unexpired`), the expired key is
purged (`!contains`), `live_at_watch == [true, false]`, and **neither** slot version advances. Fails
if a future change purges a live key here, or bumps the version on the lazy purge — either of which
flips the disposition to GATE.

### Real bug? **NO.** The exemption is sound; the live key is untouched and no version bump leaks.

---

## Verdict

Neither forcing test uncovered an isolation bug. Both arms are correctly **EXEMPT**, and the two
tests are now permanent, compile-gated evidence that would fail if either invariant regresses.

C3 classification as landed in `scripts/continuation-lock-gate.py`:

- `GATE` (5): `CoreMsg::Execute`, `CoreMsg::ScatterRequest`, `ScriptingMsg::EvalScript`,
  `ScriptingMsg::EvalScriptSha`, `ScriptingMsg::ScriptSubCommand` — each must contain a
  `can_execute_during_lock(` call *in the arm body*.
- `EXEMPT { VllExecute: "two-phase VLL protocol serializes drain vs. continuation lock; see
  vll_execute_cannot_mutate_a_held_key_under_a_foreign_continuation_lock" }`
- `EXEMPT { GetVersion: "lazy expiry of already-dead keys, no version bump; see
  get_version_purges_only_expired_keys_without_bumping_under_continuation_lock" }`
- `GATE_GAP` (2, warn + tracked): `CoreMsg::ExecTransaction` (round-2 issue 50),
  `ScriptingMsg::FunctionCall` (hardening-2 issue 05). Note the proposal above listed
  `ExecTransaction` under `GATE` on the assumption its fix had landed — it has not; the arm still
  has no gate call, so it rides the named-gap idiom instead.
- Everything else (55 arms across the 11 shard `*Msg` enums) is covered by the per-enum count pin,
  cross-checked against the enum variants in `message.rs` in both directions.
