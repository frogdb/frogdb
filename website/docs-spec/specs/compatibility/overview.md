# Spec: compatibility/overview.mdx
Status: rewrite
Audiences: A1, A2, A4

Goal: The reader leaves knowing exactly what FrogDB targets for compatibility
(Redis 8.x wire protocol, RESP2 and RESP3), that any Redis client library
connects unmodified, and the complete, honest list of behavioral deltas from
Redis: the RocksDB-backed persistence model, TOML configuration mapping, Lua
5.4 scripting differences, single-database model, Raft-based clustering, and
cross-slot / multi-shard semantics — plus the enumerated set of commands and
features FrogDB intentionally does not support. A skeptic (A2) should finish
this page believing the compatibility claim is stated precisely and its limits
are disclosed, not hidden.

Not in scope:
- Per-command supported/unsupported status. That is the [Command
  matrix](/compatibility/command-matrix/) — link to it, do not reproduce it.
- Test-suite coverage evidence. That is [Test suite
  results](/compatibility/test-suite/) — link, do not reproduce.
- How compatibility is verified (methodology). That is [Testing
  methodology](/compatibility/testing-methodology/) — link.
- Any production-migration / dual-write guidance. The old
  `compatibility/migration-guide.mdx` is cut (PLAN §4); do NOT link to it and
  do NOT add "See Also" cards pointing at it.
- Extensions (ES.*, JSON, TS, FT.*, probabilistic, vector). Those are the
  Extensions section; mention only in passing if a delta requires it.
- Config parameter tables (belong in Operations → Configuration and the
  generated Reference → Configuration reference). Link, don't inline.

Sources of truth (author must read before writing prose):
- `website/src/content/docs/compatibility/redis-differences.mdx` — the current
  page being rewritten; mine its delta content, but re-verify every claim.
- `frogdb-server/crates/commands/src/lib.rs` and
  `frogdb-server/crates/server/src/server/register.rs` — the command registry.
  Use only to confirm that a command named as "not supported" is genuinely a
  stub/absent (e.g. `SELECT`, `SWAPDB`, `MOVE`, `MODULE` are registered as
  stubs in `crate::commands::stub`; `BGREWRITEAOF`, `SYNC`, `SAVE` are stubs).
- `versions.json` (work item S6) — the ONLY source for the FrogDB version, the
  targeted Redis version, and the Lua version. See "Generated data".
- `website/src/data/config-reference.json` — to confirm the TOML config keys
  and durability-mode enum values used in the config-mapping table are real
  (e.g. `persistence.durability-mode` values, `memory.maxmemory`,
  `server.allow-cross-slot-standalone`). Do not invent keys; every
  `frogdb.toml` key shown must exist in this generated reference.

VERIFY-BEFORE-WRITING (do not state these until confirmed against source):
- **Redis target version string.** The current page says "Redis 8.x" in prose
  but "FrogDB 0.1.0". Internally the server reports `redis_version:7.2.0`
  (`frogdb-server/crates/server/src/commands/info.rs`) and the Lua shim reports
  `REDIS_VERSION 7.2.0`, while the regression crate targets "Redis 8.6.0". This
  is an unreconciled spread. The page MUST render the target version from
  S6/`versions.json`, never a hardcoded literal. If S6 is not yet emitting a
  Redis-target value when this page is written, leave a `{/* TODO: pull Redis
  target from versions.json (S6) */}` placeholder and use no numeric literal
  in prose — do not paper over the discrepancy.
- **Lua version.** Current page claims Lua 5.4. Confirm against the scripting
  crate before restating (`frogdb-server/crates/core/src/scripting/` /
  `frogdb-server/crates/scripting/`). Source it from S6 if S6 emits it.
- **`redis.setresp()` unsupported**, **strict `KEYS[]` validation**,
  **single-shard script requirement** — re-confirm each against the scripting
  crate; keep only the ones still true.
- **Tiered storage** section — confirm `tiered-lru`/`tiered-lfu` policy names
  still exist in `config-reference.json` before restating.

Existing content: `compatibility/redis-differences.mdx` (rename/replace at the
new slug `compatibility/overview`). Preserve the substance of its Architecture,
Persistence, Configuration, Clustering, Cross-Slot, Lua, Pub/Sub, Database
Model, Not Supported, and Tiered Storage sections — but strip the hardcoded
"FrogDB 0.1.0" / "Redis 8.x" literals (S6), delete the "See Also" card to the
cut Migration Guide, and repoint the "Commands Reference" card at the new
Command matrix slug.

Structure (H2/H3 outline, one line each):
- Intro (no heading): One paragraph — FrogDB implements RESP2 and RESP3 and
  targets compatibility with Redis {version from S6} commands; standard Redis
  client libraries connect without modification; this page documents only the
  deltas, and standard Redis behavior is documented at redis.io. Use the
  precise phrasing from PLAN §7 (not "speaks Redis").
- `## Architecture` — the comparison table (Threading, Storage engine, Cluster
  coordination, Config format, Allocator, Lua version). Keep it factual; no
  performance adjectives.
- `## Persistence` — RocksDB WAL + SST instead of RDB/AOF; forkless `BGSAVE`;
  no `BGREWRITEAOF`; durability modes (`async`/`periodic`/`sync`) vs Redis
  `appendfsync`; RDB files not importable. Link to Operations → Persistence.
- `## Configuration` — TOML vs redis.conf; the redis.conf→frogdb.toml mapping
  table (verify each key against `config-reference.json`); `FROGDB_` env-var
  prefix with `__` nesting. Link to Reference → Configuration reference.
- `## Clustering` — Raft control plane vs gossip; no `CLUSTER MEET`;
  deterministic failover; 16384 slots mapped to internal shard workers;
  `CLUSTER BUMPEPOCH` unsupported; manual replica migration. Link to Operations
  → Clustering.
- `## Cross-slot & multi-shard operations` — internal sharding even in
  standalone; multi-key CROSSSLOT rules; `server.allow-cross-slot-standalone`.
  - `### Transactions` — MULTI/EXEC slot rules; standalone cross-shard via VLL;
    WATCH per-key versioning. Link to Architecture → VLL.
  - `### Blocking commands` — same-shard requirement; cross-shard CROSSSLOT.
- `## Lua scripting` — strict `KEYS[]` validation (with the short
  works/errors example), single-shard requirement, `redis.setresp()`
  unsupported, Lua-version note. Gate all on the VERIFY items above.
- `## Pub/Sub` — standalone identical to Redis; cluster deltas (regular
  PUBLISH fan-out, sharded SSUBSCRIBE/SPUBLISH routing, per-client
  subscription limit config key).
- `## Database model` — single db0; `SELECT` non-zero errors; `MOVE`/`SWAPDB`
  unsupported.
- `## Not supported` — the table of intentionally-unsupported commands/features
  with one-line reasons (SELECT non-zero, MOVE/SWAPDB, MODULE, CONFIG REWRITE,
  BGREWRITEAOF, SYNC, DEBUG unsafe subcommands, diskless replication, ACL
  selectors v2, etc.). Cross-link the Command matrix for the exhaustive view.
- `## Tiered storage` — hot/warm two-tier as an eviction alternative; requires
  persistence + `tiered-lru`/`tiered-lfu`; no open-source Redis equivalent.
- `## See also` — LinkCards to Command matrix, Test suite results, Testing
  methodology, Configuration reference. NOT the migration guide.

Generated data:
- **S6 `versions.json`** — supplies FrogDB version, targeted Redis version, Lua
  version. Consumed via an Astro import (e.g. `import versions from
  '../../../data/versions.json'`) or a shared `<Version>` helper if PLAN's S6
  provides one. This page is the canonical example of the "no hardcoded version
  strings" rule (PLAN §6, §7). Zero numeric version literals may appear in the
  committed `.mdx` prose.
- No other generated JSON; this is a prose delta page. The command list and
  test evidence live on their own generated pages, which this page links to.

Drift guards:
- Version strings come from S6 only; the S6 `--check` CI job fails the build if
  `versions.json` drifts from `Cargo.toml`/`rust-toolchain.toml`/the declared
  Redis-target const, so no stale version can survive here.
- S7 (code-path check) does not cover prose config keys; the reviewer must
  confirm every `frogdb.toml` key and every durability/eviction enum value
  shown appears in `config-reference.json`. Prefer linking to the generated
  Configuration reference over re-listing keys, to minimize drift surface.
- Content policy (PLAN §6): no command counts, no crate counts, no latency
  numbers. Durability trade-offs stay qualitative ("`sync` waits for fsync
  before acknowledging") until benchmarks exist.
