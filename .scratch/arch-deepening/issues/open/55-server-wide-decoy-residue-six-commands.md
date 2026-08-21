# 55 — Six server-wide commands still carry shard-side bodies the SV6 sweep excluded; two of them destroy data

Status: needs-triage

## What to build

Proposal 67's SV6 sweep replaced the shard-side `execute()` body of every
`ExecutionStrategy::ServerWide` command with a loud refusal — the pattern documented at
`frogdb-server/crates/server/src/commands/search.rs:3-7` and implemented as
`Err(CommandError::Internal { message: "internal: server-wide command reached shard executor" })`
— precisely so a routing regression yields an `ERR` instead of a fabricated single-shard answer.
It excluded **eight** commands because they carried real bodies. Proposal 98 clears two of them
(SCAN and KEYS, via its H1). **The remaining six are owned by nobody.**

The census re-verified on `origin/main`: `git grep -n 'ExecutionStrategy::ServerWide'
frogdb-server/crates` returns 42 hits; six are not spec declarations (`core/src/scripting/gate.rs`
and `server/src/connection/guards.rs` pattern-match the variant, `connection/dispatch.rs` has a
doc comment plus two destructures, `connection/transaction.rs` destructures it), leaving **36 spec
declarations** — 35 commands plus the gate's test probe. The residue, with **line numbers
re-derived against `origin/main`** (the proposal's cites are pre-`d48e1b44` and have drifted):

| Command | Site on `origin/main` | Shard-side body today |
|---|---|---|
| DBSIZE | `server/src/commands/server.rs:28` spec, `:51-54` body | returns `ctx.store.len()` — **one shard's** key count as a plausible answer |
| FLUSHDB | `server/src/commands/server.rs:66` spec, `:89-108` body | `ctx.store.clear()` on **one shard**, replies `+OK` |
| FLUSHALL | `server/src/commands/server.rs:120` spec, `:143-159` body | `ctx.store.clear()` on **one shard**, replies `+OK` |
| RANDOMKEY | `commands/src/generic.rs:703` spec, `:726-736` body | already refuses, but as `CommandError::InvalidArgument{"RANDOMKEY should be handled by connection handler"}` — a client-facing `ERR` in the wrong class |
| ES.ALL | `commands/src/event_sourcing/all.rs:17` spec, `:40-50` body | already refuses, as `CommandError::Internal{"ES.ALL should be dispatched server-wide"}` — right class, non-canonical string |
| gate test probe | `core/src/scripting/gate.rs:1232-1261` (`ServerWideProbe`) | not a command; a `#[cfg(test)]` fixture that inflates the census |

**Correction to the proposal's table, found while sanity-checking:** it lists RANDOMKEY as a
"one-shard pick" and ES.ALL as a "one-shard read". Neither is true on `origin/main` — both already
return errors. The genuinely dangerous residue is the **three in `server.rs`**, and two of those
(FLUSHDB, FLUSHALL) are *write* decoys, which is strictly worse than SCAN's read decoy: a
one-shard `clear()` reached by a future dispatch refactor **destroys data and returns `+OK`**.
DBSIZE fabricates a wrong count silently. RANDOMKEY and ES.ALL are a smaller, real problem —
inconsistent refusal shapes across a family whose whole point is that the refusal is uniform and
greppable.

None of this is LIVE today: server-wide dispatch (`connection/dispatch.rs`, `ServerWide` ordered
ahead of `Execute` in `PRE_DISPATCH_ORDER`), the EXEC door (`connection/transaction.rs`), the
scripting gate's `reject_server_wide` (`core/src/scripting/gate.rs:487-497`) and the cluster
`-CROSSSLOT` route all close before a shard executor is reached. That is exactly the trap the SV6
sweep exists to remove: the bodies read as live code, so nobody deletes them, and the day a
routing refactor opens a fifth door the failure is silent data loss rather than an error.

Fix direction: finish the sweep. Replace the three `server.rs` bodies and the two non-canonical
refusals with SV6's canonical form, and either exclude the `#[cfg(test)]` probe from the census or
annotate it so the count is 35 commands rather than 36 declarations. Each of the five needs the
same four-route reachability proof proposal 98 wrote for SCAN/KEYS, done per command — that
per-command proof is the actual work here, not the deletion.

Related, already filed:
`.scratch/testing-improvements-round2/issues/open/34-dead-code-deletion-sweep.md` item **06/F20**
covers the SCAN/KEYS pair only (proposal 98's H1); it does not touch these six.

## Acceptance criteria

- [ ] `DbsizeCommand::execute`, `FlushdbCommand::execute` and `FlushallCommand::execute`
      (`server/src/commands/server.rs`) no longer read or mutate `ctx.store`; each returns the
      canonical `CommandError::Internal { message: "internal: server-wide command reached shard
      executor" }`, with the four-route reachability argument recorded per command
- [ ] `RandomkeyCommand::execute` (`commands/src/generic.rs`) and `EsAllCommand::execute`
      (`commands/src/event_sourcing/all.rs`) return the same canonical error instead of their
      current bespoke `InvalidArgument`/`Internal` strings
- [ ] `DBSIZE`, `FLUSHDB`, `FLUSHALL` and `RANDOMKEY` behave identically on the wire before and
      after — the change is unreachable-path only
- [ ] The gate's `ServerWideProbe` is excluded from the SV6 census or annotated as a fixture, so
      "N of N covered" is stated over commands, not declarations
- [ ] Regression test `server_wide_commands_refuse_uniformly_at_shard_executor` (in the crate
      owning each body — `frogdb-server` for `server.rs`, `frogdb-commands` for `generic.rs` and
      `event_sourcing/all.rs`) constructs a `CommandContext` and calls each of the five
      `execute()` directly, asserting the identical canonical error and — for FLUSHDB/FLUSHALL —
      that the store is **unchanged** afterwards
- [ ] `just test frogdb-server server_wide_commands_refuse`

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 98 (`.scratch/arch-deepening/proposals/98-scan-grammar-unify.md`),
defect I3 / ruling R3.

## Comments
