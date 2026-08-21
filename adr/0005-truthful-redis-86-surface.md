# FrogDB presents a truthful Redis 8.6 surface

A hands-on side-by-side session against real Redis 8.6.1 (2026-08-15) showed the data path
matching line-for-line while the introspection surface broke the drop-in illusion: shipped
binaries compiled only `core-profile` (no streams/HLL/geo/JSON despite the compat matrix
advertising them), version fields disagreed with each other (`redis_version:7.2.0` in INFO,
`version 0.1.0` in HELLO) and with the 8.6.0 regression target, and Redis-shaped probes from
common tooling (`CONFIG GET appendonly`, `LATENCY HISTORY`, `OBJECT FREQ`) got empty, strict,
or fabricated answers. We ruled three linked decisions:

1. **Ship the full command surface.** Every distributable artifact (Docker image, cross-built
   binaries, macOS tarballs, deb, Homebrew) builds with `cmd-full`. `core-profile` remains the
   *development* default solely to keep iteration builds and the build cache small — it is a
   build-speed tier, not a product tier, and the published compat matrix describes shipped
   binaries.
2. **Advertise the tested compat target everywhere.** INFO's `redis_version` and HELLO's
   `version` both report `ADVERTISED_REDIS_VERSION`
   (`frogdb-server/crates/types/src/redis_version.rs`, `8.6.0` when this was ruled) — the
   version the 2,298-test regression port actually validates — so version-gating clients
   enable the 8.x features we implement. HELLO's
   `server` field stays `frogdb` and `frogdb_version` carries the product version (Valkey
   precedent: honest identity, compat version in the version fields). Alternatives rejected:
   staying at 7.2.0 permanently under-advertises implemented features; reporting the product
   version broke handshake-parsing clients.
3. **Truthful-inert shims, never fabrication.** Redis-shaped queries are answered whenever a
   truthful answer exists (`appendonly` → `no` — there is no AOF; unknown `LATENCY HISTORY`
   event → empty array — the history *is* empty). Where Redis errors on unbacked state, so do
   we (`OBJECT FREQ` without an LFU policy). This extends the standing
   observability-accuracy-over-parity rule to the compat surface: a probe may get a modest
   answer, but never a misleading one.

Consequences: claiming the compat target makes every unadvertised gap a bug by definition — the
compat matrix and its CI drift-check (`docs-gen --check`) are the enforcement mechanism, and new
Redis-version bumps are deliberate events (retarget suite, then bump the advertised version).
Shipped-image size and build time grow with `cmd-full`. The shim policy gives tooling
compatibility without an "everything Redis says" mimicry table; the cost is that each new
probe needs a truthfulness judgment call rather than a copy-paste answer (glossary:
Truthful-Inert Shim, `frogdb-server/CONTEXT.md`).

**2026-08-21 — target moved to 8.6.1.** The side-by-side session that produced this ADR ran
against Redis 8.6.1 while the vendored metadata and the advertised version sat at 8.6.0; both
constants now read `8.6.1`, closing that skew. Upstream 8.6.1 is a four-commit patch release:
in the trimmed command projection FrogDB vendors, the only change is the added `HOTKEYS HELP`
subcommand (a row the vendor script skips, so `generated.rs` is byte-identical apart from
provenance). The behavior deltas are a CRLF-injection fix in error and status replies, an
always-emitted `# Hotkeys` INFO header, and an RDB-load hash-table expansion fix.
