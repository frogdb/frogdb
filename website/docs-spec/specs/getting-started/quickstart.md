# Spec: getting-started/quickstart.mdx
Status: rewrite
Audiences: A1 (primary — evaluator who wants to run real commands in minutes), A4 (test-deployment operator taking a first pass at config/persistence).

Goal: Starting from an installed binary, the reader (1) starts `frogdb-server`, (2) connects with `redis-cli`, (3) runs commands across 2–3 different data types and sees correct output, (4) enables/understands persistence, and (5) knows where configuration lives and how to generate a starter config — with a clear pointer onward. The reader leaves believing "it runs and behaves like Redis," having done it, not read a promise.

Not in scope:
- Installing the binary — that is the Installation page (link at top).
- Exhaustive config reference / every parameter — Operations → Configuration and the generated Configuration reference own that. Show only the handful needed to get going and link out.
- Client-library tutorials, ORMs, framework integrations (PLAN §1: client-library authors are explicitly not an audience yet).
- Deployment, replication, clustering, security — Operations section.

Sources of truth (author MUST read before writing):
- `website/src/content/docs/getting-started/quickstart.mdx` — current page (being replaced). Its command examples are directionally right but several config values are wrong (see below).
- `frogdb-server/crates/server/src/main.rs` — CLI flags. Confirmed present: `-c/--config <FILE>`, `-b/--bind <ADDR>`, `-p/--port <PORT>`, `-s/--shards <N>` (help: "default: 1, \"auto\" = num_cpus"), `-l/--log-level`, `--generate-config` (prints default TOML to **stdout** and exits — so `frogdb-server --generate-config > frogdb.toml` is correct). Default port 6379.
- `frogdb-server/ops/deploy/deb/frogdb.toml` — a real generated config sample; use it as the authority for TOML section/field names and defaults. Key facts that correct the current page:
  - `[server] num-shards = 1` by default (NOT `0`; the current page's `num-shards = 0  # 0 = auto-detect` is WRONG). "auto" = number of CPUs is expressed via the `-s auto` CLI flag; describe auto-detect that way, and let the generated example show the real default.
  - `[persistence] enabled = true`, `data-dir = "/var/lib/frogdb/data"`, `durability-mode = "periodic"`, `sync-interval-ms = 1000`. Persistence is ON by default. Valid `durability-mode` values: VERIFY the exact set against the config enum/loader (`frogdb-server/crates/server/src/config/`); the current page lists `async, periodic, sync` — confirm before repeating.
  - `[memory] maxmemory = 0` (0 = no limit) and `maxmemory-policy = "noeviction"` by default. The current page's `maxmemory = 4294967296` / `allkeys-lru` are illustrative, not defaults — if shown, label them as examples, not defaults.
- `frogdb-server/docker/Dockerfile` — for the Docker start-path (bundles `redis-cli` via redis-tools; default `CMD ["frogdb-server"]`; port 6379; `/data` volume; env-var config).
- `Justfile` — no dedicated quickstart recipe; `frogdb-server` is the entrypoint. Use plain binary invocations.

Existing content: Replace `website/src/content/docs/getting-started/quickstart.mdx`. Keep the Start → Connect → Config → Next-steps flow and the Docker/Compose tabs (accurate). Rewrite the config TOML to match real defaults. Re-evaluate the Client Libraries CardGrid (see below).

Structure:

- **Intro (1–2 sentences)**: FrogDB implements RESP2 and RESP3, so a standard Redis client such as `redis-cli` connects unchanged. Assume the reader has a binary (link Installation). Keep it short.

- **## Start the server**. Show the simplest first: `frogdb-server` (defaults: port 6379, persistence enabled). Then a couple of useful variants: `frogdb-server --config frogdb.toml`, and `frogdb-server -b 0.0.0.0 -s auto` (bind all interfaces, shard-per-CPU) — describe `-s auto` as "one shard per CPU core" and note the default is a single shard. Keep the Docker and Docker-Compose tabs from the current page (verified against the Dockerfile); ensure the image name matches whatever the Installation page settled on (published tag vs locally-built `frogdb:latest`) — cross-reference, do not invent a third name.

- **## Connect**. `redis-cli -p 6379`. One sentence: any Redis client works; this guide uses `redis-cli`.

- **## Run some commands** (the core of the page). Demonstrate 2–3 distinct data types with real REPL transcripts (prompt `127.0.0.1:6379>`), e.g.:
  - Strings: `SET`/`GET` (and maybe `INCR` or `EXPIRE` to show TTL).
  - Hashes: `HSET`/`HGETALL`.
  - Sorted sets: `ZADD`/`ZRANGE ... WITHSCORES` — good "it really behaves like Redis" demo.
  - Optionally one list (`LPUSH`/`LRANGE`).
  Keep outputs accurate to Redis semantics (FrogDB targets Redis 8.x compatibility; these are core commands, safe to show). End with a pointer to the command matrix for exact per-command support and to Compatibility → differences for behavioral deltas. Do not claim "every command works" — link to evidence instead.

- **## Persistence**. Explain that persistence is enabled by default (RocksDB-backed) and where data lands (`data-dir`, default `/var/lib/frogdb/data` in the packaged config; the plain binary uses the configured/relative default — VERIFY the out-of-the-box default data dir for a bare `frogdb-server` run vs the deb config). Show the `[persistence]` block with real fields (`enabled`, `data-dir`, `durability-mode`, `sync-interval-ms`) and describe the durability trade-off qualitatively per PLAN §6: e.g. `sync` waits for fsync before acknowledging a write; `periodic` flushes on an interval. No latency numbers. Link to Operations → Persistence & durability for the full model (recovery, tiering).

- **## Configuration**. Show `frogdb-server --generate-config > frogdb.toml` (verified: writes default TOML to stdout). Show a SMALL excerpt of the generated file with real defaults (`[server]`, `[persistence]`, and one of `[memory]`), values matching `ops/deploy/deb/frogdb.toml`. Do not reproduce the whole file — link to the generated Example config page (S4) and Configuration reference (config-reference.json). Mention the four config surfaces exist (TOML, CLI flags, env vars, `CONFIG SET`) with a one-line pointer to Operations → Configuration for precedence and runtime-mutability; do not detail them here (DRY).

- **## Client libraries** — DECISION: keep a TRIMMED version, do not delete outright. Rationale: A1 wants to know "can I point my app's Redis client at this?" and a one-line "yes, any RESP2/RESP3 Redis client" plus a link serves that. But the current 6-card grid (redis-py, ioredis, redis-rs, go-redis, Jedis, redis-cli) implies a verified compatibility matrix that does not exist and is client-author-audience bloat (explicitly out of scope, PLAN §1). Replace the grid with: one sentence ("Any RESP2/RESP3-capable Redis client library works; this guide uses `redis-cli`.") and at most a single link to redis.io's client list. Only name a specific client if its compatibility is actually verified in the repo's tests — otherwise name none. VERIFY-BEFORE-WRITING: whether any client library is exercised in FrogDB's test suite; if not, list zero clients by name.

- **## Next steps** (LinkCards). Route to: Compatibility & Correctness (overview/differences + command matrix), Operations → Persistence & durability, Operations → Configuration, and Extensions (incl. Event Sourcing). VERIFY-BEFORE-WRITING: exact slugs against the final IA — the current page links `/compatibility/redis-differences/`, `/guides/event-sourcing/`, `/operations/deployment/`, `/reference/commands/`, several of which are renamed or moved in this restructure (§4 dissolves `guides/`; event sourcing moves to Extensions). Do not emit links to pages that will not exist.

Generated data:
- `frogdb-server --generate-config` output → the Example config page (S4); this page links to it and shows only a short excerpt.
- `config-reference.json` (existing, via `docs-gen`) → the Configuration reference; linked, not duplicated.
- `versions.json` (S6) → any version string (e.g. the Redis compatibility target if mentioned); do not hardcode.
- `commands.json` (S1/S2) → the command matrix the "run some commands" section links to for per-command status.

Drift guards:
- Config values shown are copied from generated sources (`ops/deploy/deb/frogdb.toml` today; the S4 example-config page long-term), and the `just deb-gen --check` / docs `--check` jobs fail the build if they drift — this fixes the current `num-shards = 0` and default-maxmemory errors.
- `--generate-config` is the canonical way to get a config, so the page never hand-maintains a full config listing.
- Command examples use only core Redis commands and link to the generated matrix for support status, so the page makes no standalone compatibility claim to go stale.
- Client libraries are described generically (RESP2/RESP3) rather than as an enumerated, unverified matrix, removing a drift/over-claim liability.
- Link checker (`just docs-link-check`) catches renamed IA targets.
