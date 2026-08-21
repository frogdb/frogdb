# `COMMAND DOCS` is placeholder text, not real metadata

Status: done
Type: bug (introspection accuracy)
Area: commands / registry / macros

## Problem

`COMMAND DOCS <name>` returns fabricated placeholder metadata for every command: summary is
literally `"<NAME> command"`, group is always `generic`, `since` is always `1.0.0`, and the name
echoed back is uppercased regardless of what the registry actually knows.

Site: `frogdb-server/crates/commands/src/basic.rs:153-179` ignores the registry entirely and
synthesizes these strings inline.

## Ruling

Extend the existing `#[command(...)]` derive macro
(`frogdb-server/crates/frogdb-macros/src/command.rs`) with **required** docs metadata:

```
#[command(..., docs(group = "...", since = "...", summary = "..."))]
```

Missing `docs(...)` on a `#[command(...)]`-annotated type is a **compile error**, not a
fallback — this is what keeps the registry from silently regressing back to placeholders.
Expose the fields via the `Command` trait, and have the `COMMAND DOCS` arm in `basic.rs` read
them from the registry instead of synthesizing.

## Seeding the data

1. Extend `website/scripts/vendor-redis-commands.py` (currently strips summaries at :12-13 and
   :91-92) to also capture the summary field from its source data.
2. Write a one-shot script that rewrites the ~260 Redis-command `#[command(...)]` attribute
   sites using the vendored data in `website/src/data/redis-commands-8x.json` — this is the bulk
   of the work.
3. FrogDB-extension commands (`ES.*`, `FT.*`, timeseries, bloom-family, vectorset, admin
   commands) have no Redis source to vendor from — hand-write one-line summaries for these in
   the same sweep.

## Follow-up (not required for this issue's acceptance)

Once the registry carries real docs metadata for FrogDB-extension commands, `docs-gen`/the
website could consume it instead of maintaining a separate hand-written source — worth a
follow-up issue once this lands, not blocking it.

## Cross-reference

[Issue 02](../) (`COMMAND INFO`) touches the same `basic.rs` dispatch — coordinate the diffs.

## Acceptance criteria

- [ ] `docs(...)` is a required field on `#[command(...)]`; a command missing it fails to
      compile
- [ ] `COMMAND DOCS get` returns real group/since/summary matching Redis 8.6's docs for `GET`
- [ ] All ~260 vendored Redis commands carry real summary/group/since sourced from
      `redis-commands-8x.json`
- [ ] All FrogDB-extension commands carry a hand-written one-line summary
- [ ] `just docs-gen --check` stays green

Size: M/L — the bulk is the ~500-attribute-site sweep (commands plus subcommand structs)

## Resolution

CommandSpec gained a required CommandDocs field (compile-enforced coverage; the #[derive(Command)] macro named here was dead code). 419 sites: 251 vendored from redis-commands-8x.json, 134 hand-written extension summaries. COMMAND DOCS emits Redis-8-shaped summary/since/group/complexity. Wave 2, commit 81103aeb. arguments/history residue in issue 12.
