# 46 — Keyspace-event follow-ups deferred from proposal 44

**Status:** implemented (items 1, 2, 4); item 3 recorded as won't-fix-for-now (see its verdict)
**Severity:** Minor–Important (event-accuracy remnants; no data-behavior changes except item 4's decision)
**Found:** during [44-keyspace-event-key-accuracy.md](44-keyspace-event-key-accuracy.md) phases 1–4
and their reviews (2026-07-11/12). Four independent items; each shippable alone.
**Implemented:** 2026-07-12 on this branch — item 1 (GEOADD `zadd` + no-op silence), item 4
(RENAME k k / SMOVE k k m self-move short-circuits per verified Redis source), item 2 (scripted
writes routed through the canonical `WRITE_EFFECT_ORDER` pipeline). Per-item status below.

## 1. GEOADD should emit `zadd` (Important — known under-emission)

`frogdb-server/crates/commands/src/geo.rs` GEOADD spec is `EventSpec::Suppressed` with a comment
documenting the gap (added in commit `84fee7f0`): Redis geo.c `geoaddCommand` routes through
`zaddGenericCommand`, which emits `zadd` (NOTIFY_ZSET) on the key. Fix: flip to
`Emits { class: ZSET, name: "zadd" }` — GEOADD is `KeySpec::First`, single-key, so blanket
`Emits` is correct and passes validate(). Confirm FrogDB's no-op path (GEOADD of identical
members) sets `write_was_noop` (or does not claim a write) so a no-op stays silent; add the
pubsub integration test pair (emits on add, silent on no-op).

**Status: DONE.** Spec flipped to `Emits { ZSET, "zadd" }`; execute() now sets `write_was_noop`
when nothing was added or changed (and cleans up an empty zset created by `get_or_create`, e.g.
GEOADD XX on a missing key). Tests: `test_geoadd_emits_zadd`, `test_geoadd_noop_emits_nothing`
in `integration_pubsub.rs`.

## 2. Scripted writes never emit keyspace events (Important — pre-existing systemic)

Found by phase-1 implementer: the scripting execution path (`CommandInvoker`, proposal 07b's
seam) drops `ctx.keyspace_events` deposits and never runs the notifications effect for commands
invoked via `redis.call(...)`. Redis emits events for script-driven writes exactly as for direct
ones. Scope: trace where the scripting invoker builds/consumes the write summary
(`frogdb-server/crates/core/src/scripting/`), route its write records through the same
`WRITE_EFFECT_ORDER` notifications step (or at minimum the notifications + WATCH-bump subset it
currently skips — audit what else is skipped while there: replication of scripted writes works,
but check tracking invalidation too). Tests: EVAL that SETs a key → subscriber receives `set`;
EVAL LPUSH wakes a blocked BLPOP with correct events; scripted no-op stays silent.

**Status: DONE — and the audit found the gap was much wider than notifications.** The scripting
path skipped the ENTIRE write-effect pipeline, not just events: the "replication of scripted
writes works" note above was wrong — `post_execution.rs` is the only production broadcast/WAL
site and scripts never reached it, so scripted writes were also unreplicated, un-WAL'd, did not
bump the WATCH version, did not invalidate tracking, did not wake blocked waiters, and skipped
search-index upkeep.

Fix (the seam): `ScriptInvoker::run_local` records each *effective* local write (WRITE flag,
`Ok` result, not `write_was_noop`) as an owned `ScriptWriteRecord` (handler Arc, args,
dirty_delta, hll_wal_delta, drained keyspace-event deposits) into the outer
`CommandContext::script_writes`; after the script completes, the shard worker drains them
through `ShardWorker::run_script_write_effects` → the canonical `run_write_effects` /
`WRITE_EFFECT_ORDER` pipeline (`EffectScope::Command` for one write, `EffectScope::Transaction`
for several — MULTI/EXEC-framed replication, matching Redis effects replication). Cross-shard
sub-commands (`execute_script_sub_command`) run the same pipeline on the key-owner shard.
FCALL routes through the same drain. EVAL/EVALSHA/FCALL/sub-command dispatch became async to
reach the pipeline. Tests: `test_eval_scripted_set_emits_set_event`,
`test_eval_multi_write_script_emits_each_event`, `test_eval_scripted_lpush_wakes_blocked_blpop`
(waiter-wake), `test_eval_scripted_noop_stays_silent` (integration_pubsub.rs) and
`test_scripted_write_dirties_watch` (integration_transactions.rs).

## 3. Secondary `del` events on emptied keys (decision item — codebase-wide)

Redis emits `del` (NOTIFY_GENERIC) whenever a write leaves a key empty and it is removed: LPOP
emptying a list, SREM emptying a set, LMOVE draining its source, an empty-result STORE deleting
its dest (partially handled — GEOSEARCHSTORE/SORT-STORE do emit `del` since proposal 44), the
woken-after-block serve emptying its key. FrogDB currently skips these uniformly (deliberate:
proposal 44 open decision 1, kept consistent rather than piecemeal). Deciding to match Redis
means finding the single choke point where "write emptied the key → key deleted" happens
(store layer or per-command) and emitting there once, not sprinkling ~30 call sites. If the
delete happens in one store-layer seam, this is small; if it is per-command, weigh cost vs the
parity win and record the verdict here either way. Inventory of known sites is in the proposal-44
implementation-status section and task reports (`.superpowers/sdd/task-44p{2,3,4}-report.md`).

**Verdict (2026-07-12): WON'T FIX for now — no single choke point exists; recorded per the
"weigh cost vs parity win" clause.** Investigation findings:

- The "write emptied the key → delete it" decision is **per-command, not store-layer**: ~39
  emptiness-gated `store.delete(key)` sites across 17 command files
  (`set.rs`, `list.rs`, `hash.rs`, `sorted_set/{basic,pop,set_ops,store_remove}.rs`, `geo.rs`,
  `bitmap.rs`, `string.rs`, `generic.rs`, `expiry.rs`, `blocking.rs`, `sort.rs`,
  `json/basic.rs`, `vectorset/vrem.rs`, …) plus the woken-serve paths in
  `core/src/shard/blocking.rs`. The store trait has no remove-if-empty seam; `Store::delete`
  is generic (DEL, expiry, cleanup) and cannot distinguish event-worthy deletions.
- A store-layer hook cannot even in principle make the call alone: the sites split into two
  semantic classes only the handler knows apart — (a) *a write emptied a previously-visible
  key* (LPOP drains a list, SREM empties a set, SMOVE drains its source) where Redis emits
  `del`, and (b) *cleanup of a container `get_or_create` made and the command left empty*
  (ZADD XX/GT on a missing key, GEOADD XX) where the key was never observably created and
  Redis emits nothing.
- The event-spec architecture compounds the cost: runtime deposits
  (`CommandContext::notify_event`) are consumed **only** for `EventSpec::Dynamic` commands, so
  emitting a secondary `del` from e.g. SREM (blanket `Emits`) would require either flipping
  every affected command to `Dynamic` (losing the static spec) or extending the notifications
  effect to drain deposits for all spec kinds — a pipeline contract change — *and still*
  touching every one of the ~39 sites to route through a `ctx.delete_emptied(key)` helper.
  That is exactly the "sprinkling ~30 call sites" this item forbids.
- If parity is wanted later, the viable path is: (1) extend the notifications effect to emit
  spec-driven events *plus* any deposits for every EventSpec kind, (2) add a
  `CommandContext::delete_emptied(&mut self, key)` helper that deletes and deposits
  `del`/GENERIC, (3) migrate class-(a) sites to it mechanically (sed-able), leaving class-(b)
  sites on plain `delete`. Deferred until something actually needs the `del` parity; FrogDB's
  current uniform skip stays consistent (proposal 44 open decision 1). Partial exception
  already shipped and kept: empty-result STORE-family dest deletion emits `del`
  (GEOSEARCHSTORE / SORT-STORE et al., since proposal 44).

## 4. Self-move duplicate events: RENAME k k, SMOVE k k m (Minor — parity nit)

Final review (2026-07-12): FrogDB deposits both the from-event and the to-event on the same key
for self-moves (`generic.rs` RENAME; `set.rs:932` SMOVE removes-then-recreates and deposits
`srem`+`sadd`), where Redis short-circuits src==dst without events (and RENAME k k is an error in
Redis when the key exists? — VERIFY in db.c before changing behavior; accuracy over assumption).
Fix: early-return the src==dst case to match verified Redis behavior, adjust deposits, pin with
tests (including LMOVE x x rotate, which phase 4 tested — keep its behavior consistent with
whatever Redis does).

**Status: DONE — Redis semantics verified against redis/redis unstable source before changing
anything:**

- db.c `renameGenericCommand`: `RENAME k k` is **NOT an error** when the key exists — the
  samekey check runs *after* the source lookup (`lookupKeyWriteOrReply` → "no such key" for a
  missing source, even when src==dst) and short-circuits with plain `OK` (RENAME) or `0`
  (RENAMENX), before any modification or `notifyKeyspaceEvent` call.
- t_set.c `smoveCommand`: `if (srcset == dstset)` short-circuits with 1 if the member is
  present, 0 otherwise — no writes, no `srem`/`sadd`.
- t_list.c `lmoveGenericCommand`: NO same-key check — `LMOVE x x` rotates and emits both the
  pop and the push event. FrogDB already matched; kept
  (`test_lmove_self_move_rotate_emits_both_events`).

FrogDB now early-returns src==dst in RENAME (after the source-exists check) and SMOVE with
`write_was_noop`, so the whole effect pipeline (events, WAL, replication, WATCH bump) is
skipped, matching Redis returning before `signalModifiedKey`/`server.dirty++`. RENAMENX's
dest-exists no-op path also sets `write_was_noop` now. Tests: `test_rename_self_emits_nothing`,
`test_rename_self_missing_key_errors`, `test_renamenx_self_returns_zero_silently`,
`test_smove_same_key_emits_nothing` in `integration_pubsub.rs`.

## Sequencing

Items 1 and 4 are small and test-first. Item 2 is a real seam change — treat it as the main
course, with its own review. Item 3 starts as an investigation (find the choke point) and may
close as won't-fix with a recorded reason.
