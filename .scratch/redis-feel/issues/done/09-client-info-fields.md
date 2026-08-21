# `CLIENT INFO` is missing fields Redis 8.6 emits

Status: done
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

## Comments

Implemented `watch`, `tot-net-in`, `tot-net-out`, `rbs`, `rbp`; `io-thread` omitted.

- `watch` — real live count from `ConnectionState::watched_key_iter()`, synced into the client
  registry on the same periodic cadence as memory stats (`ClientRegistry::update_watch_count`,
  wired from `ConnectionHandler::maybe_sync_stats`).
- `tot-net-in`/`tot-net-out` — sourced from the already-tracked `ClientStats::bytes_recv`/
  `bytes_sent`; the registry just wasn't attaching `stats` to the `ClientInfo` it built for
  `list()`/`get()` (`stats: None` unconditionally) — now `Some(entry.stats.clone())`.
- `rbs`/`rbp` — `rbs` reuses the real current `query_buf_size` (same value FrogDB already tracked
  for `qbuf`); `rbp` is a new high-water mark (`ConnectionState::query_buf_peak`), sampled at the
  same periodic memory-sync point rather than every read. It's a real observed peak over sampled
  points, not a fabricated buffer capacity — cheaper than Redis's continuous tracking but never
  dishonest.
- `io-thread` — **omitted**. FrogDB does assign each connection a round-robin "home shard"
  at accept time (`acceptor.rs`'s `RoundRobinAssigner`, threaded through as
  `ConnectionHandler::shard_id` / `connection/routing.rs:78`), but that id is a *data-shard*
  routing default for keyless commands, not an I/O-thread pin: FrogDB runs on tokio's
  multi-threaded scheduler, and a connection's actual socket I/O and command execution are not
  bound to any single OS thread the way Redis's io-thread field describes. Reporting the
  home-shard id under the `io-thread` key would imply a threading guarantee FrogDB doesn't make,
  so the field is left out rather than mapped to something misleading.

## Resolution

CLIENT INFO/LIST emit the Redis 8 field set with truthful values; watch= wired to real counts via the periodic stats sync; io-thread deliberately omitted (no honest mapping). Wave 1, commit 2f71b949.
