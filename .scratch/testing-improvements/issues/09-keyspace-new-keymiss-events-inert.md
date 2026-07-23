# `n` (new-key) and `m` (keymiss) notify-keyspace-events classes accepted but never emit

Status: needs-triage
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
