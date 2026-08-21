# Redis feel test — 2026-08-15 findings

State: active

A hands-on side-by-side "feel test" ran ~130 identical commands through `redis-cli` against
FrogDB and real Redis 8.6.1, plus single-connection MULTI, pub/sub, keyspace notifications,
blocking ops, SCAN, Lua, the RESP3 handshake, and a pass of ops-tooling probes (`CONFIG GET`,
`LATENCY HISTORY`, `OBJECT FREQ`, `CLIENT INFO`, memtier). The data path matched line-for-line,
including exact error strings, `EXECABORT` semantics, notification streams, `BLPOP` wake
ordering, and SCAN completeness (500/500 keys). Every gap found is in the introspection/metadata
surface, not the data path.

Rulings were settled with the user 2026-08-20 and are recorded in
[`adr/0005-truthful-redis-86-surface.md`](../../adr/0005-truthful-redis-86-surface.md): ship
`cmd-full` in every distributable artifact, advertise `redis_version`/HELLO `version` 8.6.0 while
keeping `server: frogdb`, and answer Redis-shaped probes with the **Truthful-Inert Shim** policy
(glossary entry in [`frogdb-server/CONTEXT.md`](../../frogdb-server/CONTEXT.md)) — a truthful
answer when one exists, Redis's own error when it doesn't, never a fabricated value.

See [`PRD.md`](PRD.md) for the full writeup and acceptance criteria.

Issues: [open](issues/open/) / [done](issues/done/)
