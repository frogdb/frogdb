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

## Second sweep — 2026-09-01 stub/fabrication audit

A follow-up pass asked a narrower question: which commands are stubbed out or return dummy data
*unintentionally* — excluding the deliberate deviations recorded in ADR-0005 and the compat
matrix. Findings became issues 22–35, in three groups:

- **Behavior stubbed while the data exists** (22–24): `LATENCY HISTOGRAM` returns an empty array
  though `latencystats` already renders real percentiles; `XREADGROUP` refuses multiple streams
  though `XREAD` serves N; `RESTORE IDLETIME/FREQ` and `CLIENT KILL USER` parse their arguments
  and discard them.
- **`INFO` and `CLIENT LIST` fabrications** (25–29): uptime, CPU, allocator and script-cache
  fields, keyspace `expires`, tracking counters, and the `user`/`resp`/`redir`/`run_id`
  identity literals.
- **Commands absent from the advertised 8.6.1 surface** (30–35): `FAILOVER`, `RESTORE-ASKING`,
  `SFLUSH`, `TRIMSLOTS`, `XCFGSET`, a `SENTINEL` deviation that was never written down, and a
  phantom `MAXMEMORY` row the compat-matrix generator manufactures from a test-suite name.

Issues needing a product ruling before work starts are `needs-triage`; the mechanical ones are
`ready-for-agent`.
