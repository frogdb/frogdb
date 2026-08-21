# Redis feel test — closing the introspection gap

Status: ruled 2026-08-20 — see [ADR-0005](../../adr/0005-truthful-redis-86-surface.md)
Author: 2026-08-15 (feel test), rulings recorded 2026-08-20

## 1. What happened

A hands-on "feel test" (2026-08-15) ran redis-cli side by side against a locally built FrogDB
and a real Redis 8.6.1, issuing ~130 identical commands across strings, hashes, lists, sets,
sorted sets, expiry, and generic key commands, plus dedicated passes over:

- single-connection `MULTI`/`EXEC`/`DISCARD`, including deliberate `EXECABORT` triggers
- pub/sub and keyspace notifications
- blocking ops (`BLPOP` wake ordering across two connections)
- `SCAN` cursor completeness (500 keys seeded, 500 returned)
- Lua scripting (`EVAL`/`EVALSHA`/`FCALL`)
- the RESP3 `HELLO` handshake
- ops-tooling probes: `COMMAND INFO`/`COMMAND DOCS`, `CONFIG GET`, `LATENCY HISTORY`,
  `OBJECT ENCODING`/`OBJECT FREQ`, `CLIENT INFO`, `INFO`, and a `memtier_benchmark` load-gen run
  against `just dev`

**The data path matched line-for-line**, including exact Redis error strings, `EXECABORT`
semantics, the notification stream shape, `BLPOP` wake ordering, and SCAN's 500/500 completeness.
Every discrepancy found is in the **introspection/metadata surface** — the layer tooling and
operators use to ask FrogDB about itself, not the layer that stores and returns data.

## 2. Rulings (2026-08-20, see ADR-0005 for full rationale)

1. **Ship the full command surface.** Every distributable artifact (Docker image, cross-built
   binaries, macOS tarballs, deb, Homebrew) builds with `--features cmd-full`, not
   `core-profile`. `core-profile` stays the *development* default for build-cache/iteration
   speed only — it is a build-speed tier, not a product tier, and the published compat matrix
   describes what ships.
2. **Advertise the tested compat target everywhere.** `INFO` reports `redis_version:8.6.0`,
   `HELLO` reports `version 8.6.0` — the version the regression port actually validates.
   `server` stays `frogdb`; `frogdb_version` stays the product version.
3. **Truthful-inert shims, never fabrication.** A Redis-shaped probe gets a truthful answer
   wherever one exists (`CONFIG GET appendonly` → `no`; unknown `LATENCY HISTORY` event → empty
   array), and Redis's own error where Redis would error on unbacked state (`OBJECT FREQ`
   without an LFU policy). See the **Truthful-Inert Shim** glossary entry in
   `frogdb-server/CONTEXT.md`.

## 3. Issues

All `ready-for-agent` except 11 (`needs-triage` — harness friction, not a product gap; needs a
decision on scope before a fix is specified).

| # | title | size |
|---|---|---|
| [01](issues/open/01-ship-cmd-full-surface.md) | Ship `cmd-full` in every distributable artifact | — |
| [02](issues/open/02-command-info-real-metadata.md) | `COMMAND INFO` returns real arity/key-spec/ACL metadata | S |
| [03](issues/open/03-command-docs-derive-metadata.md) | `COMMAND DOCS` derives from registry metadata, not placeholders | M/L |
| [04](issues/open/04-unknown-command-error-fidelity.md) | Unknown-command error matches Redis byte-for-byte | S/M |
| [05](issues/open/05-object-encoding-missing-key-nil.md) | `OBJECT ENCODING` on a missing key returns nil, not a double-`ERR` error | S |
| [06](issues/open/06-advertise-8-6-0.md) | Advertise `redis_version`/HELLO `version` 8.6.0 | S |
| [07](issues/open/07-truthful-inert-shims.md) | Truthful-inert shims: `appendonly`, `LATENCY HISTORY`, `OBJECT FREQ` | S |
| [08](issues/open/08-info-surface-alignment.md) | `INFO` default section list matches Redis 8.6 | S-M |
| [09](issues/open/09-client-info-fields.md) | `CLIENT INFO` carries the fields Redis 8.6 emits | S |
| [10](issues/open/10-lua-strict-keys-doc-fix.md) | Fix docs claiming strict Lua `KEYS[]` enforcement | S |
| [11](issues/open/11-dev-harness-friction.md) | `just dev` readiness timeout + silent memtier failure (needs-triage) | — |

Cross-references: 02 and 03 both touch `crates/commands/src/basic.rs`'s `COMMAND` subcommand
dispatch and should land with an eye to each other's diff. 06 implements ADR-0005 ruling 2
directly. 07 implements ruling 3. 01 implements ruling 1.

## 4. Acceptance

The feel test is re-run from the same script used 2026-08-15, diffing FrogDB's output against
real Redis 8.6.1 for the same ~130-command matrix plus the tooling-probe pass. The rerun is
clean **modulo documented intentional deviations**:

- `LOLWUT` (cosmetic, not a compat target)
- `DEBUG` subcommand gating text (FrogDB's `DEBUG` surface is deliberately narrower — see
  `frogdb-server/CONTEXT.md`)
- single-DB errors (`SELECT`/`SWAPDB` on a non-zero index — FrogDB does not implement multiple
  logical databases; out of this PRD's scope)
- `OBJECT ENCODING` name strings that reflect FrogDB's actual storage encodings where they
  legitimately differ from Redis's (the *nil-on-missing-key* behavior in issue 05 is not one of
  these — that is a real bug)

Every other diff is a defect against one of the 11 issues above, until the corresponding issue
is closed.

## 5. Out of scope

- New command families or data-path behavior changes — the data path already matches.
- Multi-database (`SELECT`) support.
- `LOLWUT` art parity.
