# A command that returns `Err` still versions, persists and propagates — no rollback invariant exists

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §2 T6
Score: aggregate of 4 findings
Area: frogdb-core / shard execution · frogdb-commands (json, sorted_set, blocking) · frogdb-core scripting

## Context

`core/src/shard/execution.rs:240-243` builds a `WriteCommandMeta` even when a handler returns
`Err` — only `write_was_noop` suppresses it. So a **failed command still bumps the keyspace
version, still persists to the WAL, and still propagates**. `core/src/shard/rollback.rs` exists but
covers WAL-persistence failure only, not handler failure. The four instances below are all
"mutate, then error": the client is told the write failed while half of it survived and replicated.

This is **one table-driven invariant over the registry, not four command fixes**: *a command that
returns `Err` leaves the keyspace version and the serialized value of every key in its
`WalStrategy::actions()` unchanged, writes no WAL record, and propagates nothing.* Area 07 raised
it explicitly as cross-cutting and assigned it to the core-engine seam.

## Evidence

- **The seam.** `core/src/shard/execution.rs:240-243` turns a handler `Err` into a response but
  still builds a `WriteCommandMeta`; `core/src/shard/rollback.rs:31-70` derives its snapshot from
  `handler.wal_strategy().actions(args)` and is only entered on the WAL-failure path. The
  `rollback_mode` arm at `core/src/shard/execution.rs:436-491` has `line_counts` 0 for 440,
  453-464, 466, 469-479, 481-482, 484-485, 487-489 — the entire arm including
  `capture_write_snapshot`, `rollback_snapshot`, `WalRollbacks::inc` and the
  `IOERR WAL persistence failed` response. `rollback.rs`'s 8 unit tests all hand-simulate the
  mutation; none runs a real command and none involves a real WAL failure.
- **JSON multi-path mutation drops its rollback snapshot on the error path.** *(07/F3)*
  `types/src/json.rs:362-386` — the loop does `*value = new_val` per matched path then
  `_ => return Err(JsonError::NotANumber)` mid-iteration; `update_cached_size()` (`:388`) is also
  skipped, so the cached memory size goes stale. Same shape in `num_mult_by` (`:393`) and `set`
  (`:306-311`). At the command layer `commands/src/json/basic.rs:109-112` deliberately takes
  `let snapshot = json.clone();` for the growth-limit rollback, but the very next line
  `json.set(...).map_err(...)?` returns **without restoring it**. `json/numeric.rs` is 70.4%
  covered; the multi-result branch (`:112-125`) is `untested`.
- **`*STORE` commands destroy the destination before validating.** *(06/F6)*
  `sorted_set/store_remove.rs:85-88` — `let Some(zset) = ctx.store.get_zset(src)? else {
  ctx.store.delete(&dest); return Ok(Response::Integer(0)) }` fires **before** any
  `parse_score_bound`/`parse_lex_bound`/`parse_i64` at `:91-119`. Same ordering at
  `sorted_set/set_ops.rs:457-463` (ZINTERSTORE) and `:764-770` (ZDIFFSTORE). ZUNIONSTORE is
  correctly ordered, proving the intent. Separately the *legitimate* empty-result-deletes-destination
  contract has **no assertion anywhere** — `set_ops.rs:285-289`, `:506-510`, `:794-798`,
  `set.rs:542`, `:594/605/617`, `:669/688`.
- **`BLMOVE`/`BRPOPLPUSH` pop and delete the source before type-checking the destination.**
  *(06/F7)* `commands/src/blocking.rs:243-265` (BLMOVE) and `:814-833` (BRPOPLPUSH) pop from the
  source and delete it when empty *before* touching the destination, with no undo. The
  non-blocking sibling gets this right (`list.rs:831` calls `check_list(dest)?` first), and the
  *blocked* path is guarded by the `Undo` in `core/src/shard/blocking.rs:562-590` and **is** tested
  (`list_tcl.rs:1895`, `:1926`) — so the invariant is known and only the immediate path is
  unprotected. The immediate-path test at `list_tcl.rs:1582-1594` asserts the error prefix and
  never re-reads the source list. `blocking.rs` has **zero** in-crate tests across 905 LOC.
- **Script timeout commits and replicates partial effects.** *(09/F4)*
  `scripting/src/sandbox.rs:209-228` raises `BUSY script running for {} ms` with **no `has_writes`
  consultation**, while `core/src/shard/scripting.rs:114-130` has already drained
  `ctx.effects.script_writes` and calls `run_script_write_effects` unconditionally — the comment at
  `:126-129` says this is deliberate, which is right for a *script-raised* error and wrong for a
  *server-imposed* abort. FrogDB's own kill path returns `Unkillable` for exactly this case
  (`lua_vm.rs:472-478`).

## What to fix

1. Add the invariant as a table-driven assertion over the registry at the `shard_driver` boundary:
   for each command with a well-formed argument vector that fails, snapshot the keys named by
   `WalStrategy::actions(args)` before and after, and assert version, serialized value, WAL record
   count and propagation are all unchanged.
2. Fix `execution.rs:240-243` so the `Err` path does not build a `WriteCommandMeta`, or extend
   `rollback.rs` to cover handler failure as well as WAL failure.
3. Reorder the four `*STORE` sites to validate before deleting the destination, matching
   ZUNIONSTORE.
4. Give the `BLMOVE`/`BRPOPLPUSH` immediate path the same `Undo` the blocked path already has.
5. Restore the JSON snapshot on every error return, not only the growth-limit one; validate all
   matches before mutating any (RedisJSON's own behaviour).
6. Decide the script-timeout write policy (see issue 30) and pin it.

## Acceptance criteria

- [ ] A table-driven test over the registry asserts the invariant and names the offending command
      on failure.
- [ ] `{"a":1,"b":"x"}`, `JSON.NUMINCRBY doc '$.*' 1` ⇒ error **and** `JSON.GET doc '$'` is
      byte-identical to the pre-command document **and** the key version did not change. Fails
      today. Same for `NUMMULTBY`, `STRAPPEND`, and a nested `JSON.SET` tripping `NotAnObject`.
- [ ] For each of the six `*STORE` commands, the malformed / wrong-typed / missing-source variant
      leaves `EXISTS dst == 1` with the original content and returns the correct error. Fails today
      for ZRANGESTORE, ZINTERSTORE, ZDIFFSTORE.
- [ ] The legitimate empty-result contract is asserted: `dst` is deleted **and** a `del` keyspace
      notification is emitted (via `capture_keyspace`).
- [ ] `RPUSH src a; SET dst notalist; BLMOVE src dst LEFT RIGHT 0` ⇒ WRONGTYPE **and**
      `LRANGE src 0 -1 == ["a"]` **and** `EXISTS src == 1`. Same for BRPOPLPUSH and for the
      single-element case where the pop deletes the source. Fails today.
- [ ] A script exceeding `lua-time-limit` pins one policy: either no writes survive, or writes
      survive and the WAL contains a MULTI/EXEC-framed prefix that the replica reproduces exactly.

## Test boundary

**Level 3** (`shard_driver`) for the invariant and for all four instances — the assertion is about
version, WAL record and propagation, which only the worker's effect pipeline produces; a level-4
test over RESP can see the error and the value but not the version bump or the WAL record, and
`redis-regression` cannot see the notification. The JSON type-level atomicity half is **level 2**
(crate API on `JsonValue`). The `BLMOVE` case needs a new scenario file that enters through
`commands/src/blocking.rs::execute()` — the existing `block_wait` harness API bypasses argument
parsing. The script-timeout consistency proof is **level 4** (needs `CONFIG SET lua-time-limit`
plus a replica).

## Depends on

Issue 01, `.scratch/testing-improvements-round2/issues/` (`shard_driver` harness extension — the
blocking-command entry wrapper is exactly what 06/F7 needs). Issue 05,
`.scratch/testing-improvements-round2/issues/` ("shard busy running a script" fixture) for the
script-timeout arm. Issue 30, `.scratch/testing-improvements-round2/issues/` for the script-timeout
write policy.

## Re-triage 2026-08-06

**Verdict: partially-fixed**

One of the five findings is closed; the seam itself and the three command-level "mutate, then
error" instances are all live. No table-driven rollback invariant exists anywhere. Per-claim:

- **The seam (`execution.rs`) — still valid.** `crates/core/src/shard/execution.rs:241-244` still
  turns a handler `Err` into a response and falls through; `:304-317` still builds the
  `WriteCommandMeta` for any `is_write` command whose effects were not `write_was_noop`. The `Err`
  outcome is never consulted. Refs old → new: `:240-243` → `:241-244`; the meta construction the
  body describes is now at `:299-317`; `CommandEffects::into_write_meta` is at `:57-77`.
- **JSON multi-path mutation — still valid.** `crates/types/src/json.rs:356-390` — `num_incr_by`
  still writes `*value = new_val` per path inside the loop and still has
  `_ => return Err(JsonError::NotANumber)` at `:383`, skipping `update_cached_size()` at `:388`.
  `num_mult_by` at `:393` is the same shape. `crates/commands/src/json/basic.rs:109-112` still
  takes `let snapshot = json.clone();` and then `json.set(...).map_err(...)?` without restoring it.
- **`*STORE` destroys the destination before validating — still valid.**
  `crates/commands/src/sorted_set/store_remove.rs:84-85` still does `ctx.store.delete(&dest)` in
  the missing-source `else` branch, ahead of the bound parsing at `:91-119`. Same ordering in
  `sorted_set/set_ops.rs` (ZINTERSTORE / ZDIFFSTORE).
- **`BLMOVE` / `BRPOPLPUSH` — still valid, and this is a live data-loss bug.**
  `crates/commands/src/blocking.rs:243-265`: the element is popped at `:243-252`, the source is
  deleted when empty at `:256` (`delete_if_empty_list`), and only then, at `:261-265`, does the
  destination type check `return Err(CommandError::WrongType)` — with no undo. The popped element
  is gone from the source and never reached the destination. `BRPOPLPUSH` routes through the same
  code (`:851`, "BRPOPLPUSH is equivalent to BLMOVE source dest RIGHT LEFT"). Ref old → new:
  `:243-265` unchanged; `:814-833` → `:851`.
- **Script timeout commits and replicates partial effects — FIXED.** Closed as round-2 issue 60
  (now in `issues/done/`), resolved 2026-08-04 with **option A**: `lua-time-limit` bounds a script
  only until it writes, so a server-imposed abort is reachable only for a read-only script.
  `crates/scripting/src/sandbox.rs` gained `TimeoutHook.write_dirty` + the
  `sandbox::deadline_aborts(elapsed_ms, budget_ms, write_dirty)` predicate; the `BUSY script
  running for {} ms` raise moved to `:265`. `crates/core/src/shard/scripting.rs:142-145` now
  documents that only a *script-raised* error can leave writes behind. Acceptance criterion 5 is
  discharged; item 6 of "What to fix" is closed.

Remaining work is items 1-5 of "What to fix" and acceptance criteria 1-4.
