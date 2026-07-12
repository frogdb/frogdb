# 46 — Keyspace-event follow-ups deferred from proposal 44

**Status:** proposed
**Severity:** Minor–Important (event-accuracy remnants; no data-behavior changes except item 4's decision)
**Found:** during [44-keyspace-event-key-accuracy.md](44-keyspace-event-key-accuracy.md) phases 1–4
and their reviews (2026-07-11/12). Four independent items; each shippable alone.

## 1. GEOADD should emit `zadd` (Important — known under-emission)

`frogdb-server/crates/commands/src/geo.rs` GEOADD spec is `EventSpec::Suppressed` with a comment
documenting the gap (added in commit `84fee7f0`): Redis geo.c `geoaddCommand` routes through
`zaddGenericCommand`, which emits `zadd` (NOTIFY_ZSET) on the key. Fix: flip to
`Emits { class: ZSET, name: "zadd" }` — GEOADD is `KeySpec::First`, single-key, so blanket
`Emits` is correct and passes validate(). Confirm FrogDB's no-op path (GEOADD of identical
members) sets `write_was_noop` (or does not claim a write) so a no-op stays silent; add the
pubsub integration test pair (emits on add, silent on no-op).

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

## 4. Self-move duplicate events: RENAME k k, SMOVE k k m (Minor — parity nit)

Final review (2026-07-12): FrogDB deposits both the from-event and the to-event on the same key
for self-moves (`generic.rs` RENAME; `set.rs:932` SMOVE removes-then-recreates and deposits
`srem`+`sadd`), where Redis short-circuits src==dst without events (and RENAME k k is an error in
Redis when the key exists? — VERIFY in db.c before changing behavior; accuracy over assumption).
Fix: early-return the src==dst case to match verified Redis behavior, adjust deposits, pin with
tests (including LMOVE x x rotate, which phase 4 tested — keep its behavior consistent with
whatever Redis does).

## Sequencing

Items 1 and 4 are small and test-first. Item 2 is a real seam change — treat it as the main
course, with its own review. Item 3 starts as an investigation (find the choke point) and may
close as won't-fix with a recorded reason.
