# `n` (new-key) and `m` (keymiss) notify-keyspace-events classes accepted but never emit

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 2/3 (score 6)
Area: pubsub

## Context

The `n` (new-key) and `m` (keymiss) keyspace-notification classes parse successfully
(`crates/core/src/keyspace_event.rs:72-74`, round-trip at `:133-139`, unit-tested at `:272-274`),
so `CONFIG SET notify-keyspace-events Knm` succeeds without error — but there is no emission site
for either class anywhere in `crates/commands/src` or `crates/core/src`. An exhaustive grep for
`emit` callers and any `NEW`/`MISS` event-spec references found nothing: no `EventSpec` carries a
`NEW` or `MISS` variant, `notify_event` only handles `GENERIC`/`LIST`/`SET`/`STRING`/`ZSET` classes,
and there is no read-path keymiss call site at all. The config is accepted but silently inert — a
user who enables `Knm` gets no events and no error, which is a surprising silent swallow.

Expected behavior per Redis parity (checked against actual version history): `keymiss` events since
Redis 6.0 (fired on a read that misses), `new` events since Redis 7.4 (fired on first key creation).
This is distinct from the separately-tracked stream-event-class gap (issue 27) — this is about
event *classes* (`n`/`m`) that don't exist anywhere in the codebase, not about a specific command
family's events being under-asserted.

## What to build

- Either implement emission for both classes:
  - `keymiss`: fire on any read command that misses (key absent/expired) when `m` is enabled.
  - `new`: fire on first creation of a key (any type) when `n` is enabled.
- ...or explicitly reject `n`/`m` flags in `notify-keyspace-events` config parsing (pinning the
  policy that these classes are unsupported) rather than silently accepting and swallowing them.
- Tests: `CONFIG SET notify-keyspace-events Km` + `GET` on a missing key -> assert `keymiss` event
  fires (or, if rejecting, assert `CONFIG SET` errors). `CONFIG SET notify-keyspace-events Kn` +
  `SET newkey val` -> assert `new` event fires (or errors, per chosen policy).

## Acceptance criteria

- [ ] `notify-keyspace-events` with `n` and/or `m` either emits the corresponding events correctly,
      or `CONFIG SET` rejects those flags with a clear error — no silent accept-and-swallow
- [ ] If implemented: integration test asserting `keymiss` fires on a read miss
- [ ] If implemented: integration test asserting `new` fires on first key creation (and not on
      subsequent overwrites of an existing key)
- [ ] If rejected: integration test asserting `CONFIG SET notify-keyspace-events Knm` errors

## Blocked by

None - can start immediately.

## References

- `crates/core/src/keyspace_event.rs:72-74` (parse), `:133-139` (round-trip), `:272-274` (unit test)
- Grep across `crates/commands/src` and `crates/core/src` for `NEW`/`MISS` emission sites — none found
- Source: `.scratch/testing-improvements/audit/C-pubsub-streams.md`
  `keyspace-new-and-keymiss-events-never-emitted`, `.scratch/testing-improvements/audit/verdicts-C.md` (same,
  "Exhaustive grep: no EventSpec carries NEW/MISS; notify_event only GENERIC/LIST/SET/STRING/ZSET;
  no read-path keymiss site. Redis versions check out (keymiss 6.0, new 7.4).")

## Resolution

**Decision: implemented emission for both classes** (the issue's preferred path), rather than
rejecting the flags at config parse.

### Redis semantics verified (against Redis 8.x source, not memory)

- `new` (`n`): fired from `dbAddInternal` on the *first* creation of a key (any type), emitted
  **before** the command's own type event. Fires unconditionally on creation regardless of the
  command; overwriting an existing key does not fire it. Event name `new`, payload = key.
- `keymiss` (`m`): fired from `lookupKeyReadWithFlags` when a **read** lookup misses (key absent or
  logically expired). Never fired from write lookups (`lookupKeyWrite` passes `LOOKUP_WRITE`), and
  never when the read hits. Event name `keymiss`, payload = key.
- Both classes are **excluded from the `A` alias** (`NOTIFY_ALL` omits `KEY_MISS` and `NEW`), so
  `A` alone must deliver neither — already correctly encoded in `ALL_TYPES`
  (`keyspace_event.rs`), asserted by the pre-existing `test_all_alias`.

### Implementation

Emitted at the single-shard direct-command + `MULTI`/`EXEC` execution seam (`shard/execution.rs`),
which satisfies the acceptance criteria:

- **`keymiss`**: emitted in the `FirstKey`/`EveryKey` lookup-existence probe in
  `execute_command_body` — the same seam that computes keyspace hit/miss stats — in the miss
  branch, gated on `!is_write` (Redis read-lookup-only rule). Covers `GET`, `MGET` (`EveryKey`),
  hash/set/zset reads, etc.
- **`new`**: `execute_command_body` snapshots which of a write's keys are absent *before* the
  handler runs (only when `n` is enabled — one atomic load otherwise), then keeps those that exist
  afterward. These `newly_created_keys` thread through `WriteCommandMeta` -> `WriteRecord`, and
  `emit_keyspace_notifications_for_command` emits `new` for them **before** the command's own
  `EventSpec` event (matching Redis ordering). Guarded so it is inert unless `n` is set.
- Helper `ShardWorker::new_events_enabled()` and the emit calls reuse the existing
  `emit_keyspace_notification` fast path (atomic load + flag-intersect), preserving the
  "< 1ns when disabled" cost model.

### Test evidence

Integration tests in `crates/server/tests/integration_pubsub.rs` (all pass on the Linux testbox):

- `test_new_event_fires_on_key_creation` — `En` + `SET freshkey` delivers `new` -> `freshkey`.
- `test_new_event_not_fired_on_overwrite` — overwrite of an existing key delivers no `new`.
- `test_new_precedes_type_event` — `E$n` + `SET` delivers `new` then `set`, in that order.
- `test_keymiss_event_fires_on_read_miss` — `Em` + `GET absentkey` delivers `keymiss` -> `absentkey`.
- `test_keymiss_not_fired_on_read_hit` — a hitting `GET` delivers no `keymiss`.
- `test_keymiss_not_fired_for_write_command` — `GETDEL` (write, first-key) miss delivers no `keymiss`.
- `test_all_alias_excludes_new_and_keymiss` — under `KEA`, `SET`/`GET`-miss deliver the ordinary
  `set` event but neither `new` nor `keymiss`.

Result lines: `frogdb-server integration_pubsub`: 104 tests run, 104 passed;
`frogdb-core keyspace`: 21 tests run, 21 passed (includes `test_all_alias` A-exclusion).

### Scope / known gaps (narrow, documented as follow-ups)

- **`new` for script- and cross-shard-scatter-created keys is not emitted.** Scripted sub-commands
  (`redis.call`) execute against the store directly, bypassing the `execute_command_body`
  creation-diff seam; cross-shard scatter writes (e.g. multi-slot `MSET` in cluster mode) build
  `WriteRecord`s via `WriteRecord::new` (empty `newly_created_keys`). Both are cluster/scripting-only
  paths; the direct + `MULTI`/`EXEC` coverage meets the acceptance criteria.
- **`keymiss` for cross-shard scatter reads (cluster-mode `MGET`/`EXISTS`/`TOUCH`) is not emitted**
  — those miss verdicts flow through `record_lookup_existence` (bool sequence, no key). Standalone
  `MGET`/`EXISTS`/`TOUCH` go through the single-shard seam and do emit `keymiss`.
- **`keymiss` for `LookupSpec::Reported` commands** (only `GETEX`, which reports Hit/Miss on the
  context rather than through the existence probe) is not emitted — a single-command minor gap.
