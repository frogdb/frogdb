# `CLIENT INFO` is missing fields Redis 8.6 emits

Status: ready-for-agent
Type: bug (introspection accuracy)
Area: connection / client

## Problem

Observed diff against real Redis 8.6.1's `CLIENT INFO`/`CLIENT LIST` output: FrogDB is missing
`watch`, `rbs`, `rbp`, `tot-net-in`, `tot-net-out`, and `io-thread`.

## Ruling (truthful-inert shim policy — same standard as issue 07)

Add each field only where a truthful value exists:

- `watch` — the connection's real `WATCH` count. Should already be tracked somewhere for
  `MULTI`/`WATCH` bookkeeping; surface it.
- `tot-net-in` / `tot-net-out` — real cumulative byte counters for the connection, if the
  connection layer tracks them (or can cheaply be made to).
- `io-thread` — the owning shard/worker id, **only if** that id-to-connection mapping is honest
  (i.e. the connection really is pinned to or served primarily by that worker). If FrogDB's
  threading model doesn't map cleanly onto Redis's io-thread concept, don't fabricate a number —
  either omit the field or find the closest honest equivalent and document the mapping in the
  issue's `## Comments`.
- `rbs` / `rbp` (read buffer size / peak) — Redis reports these from its own buffer
  implementation, which FrogDB doesn't share. Rather than fabricate a plausible-looking number,
  either omit them or emit an honest equivalent from FrogDB's actual connection buffer if one
  exists. Do not invent buffer sizes.

## Where to look

Find the `CLIENT INFO` formatter in the server crate (likely near the other `CLIENT`
subcommands, adjacent to where the existing field list is built).

## Acceptance criteria

- [ ] `watch` reports the real per-connection WATCH count
- [ ] `tot-net-in`/`tot-net-out` report real cumulative byte counters, or the issue's
      `## Comments` documents why no honest counter exists
- [ ] `io-thread` reports an honest worker/shard id, or is omitted with the reasoning recorded
- [ ] `rbs`/`rbp` are either honest values or omitted — never fabricated
- [ ] `CLIENT INFO`/`CLIENT LIST` regression coverage for whichever fields are added

Size: S
