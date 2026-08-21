# Truthful-inert shims: `CONFIG GET appendonly`, `LATENCY HISTORY`, `OBJECT FREQ`

Status: done
Type: bug (introspection accuracy)
Area: config / latency / object

## Ruling (ADR-0005, ruling 3)

A Redis-shaped probe gets a truthful answer wherever one exists, and Redis's own error where
Redis errors on unbacked state. Never fabricate. See the **Truthful-Inert Shim** glossary entry
in `frogdb-server/CONTEXT.md`. Three concrete fixes:

## (a) `CONFIG GET appendonly` has no truthful answer today

There is no AOF in FrogDB, so `appendonly` should truthfully report `no`. Fix:

1. Add a VIRTUAL_PARAMS row for `appendonly` in `frogdb-server/crates/config/src/params.rs`.
2. Add a `NoopParam` arm in `frogdb-server/crates/server/src/runtime_config.rs`, following the
   existing pattern used for e.g. `save`/`hz` (around :2493-2545).
3. Bump the `ParamId::ALL.len()` golden asserts in
   `frogdb-server/crates/config/src/param_id.rs:359,370` to account for the new param.

While here, sweep for other standard Redis config names that ops tooling commonly probes
(`aof-*` variants, `dir`, `dbfilename`, ... — `save` is already handled) and add a truthful
`NoopParam`/real-value row **only** where a truthful value actually exists. Don't add a shim
just to silence a probe that has no honest answer.

## (b) `LATENCY HISTORY <unknown-event>` currently errors

It lists valid event names and rejects unknown ones. Redis returns an **empty array** for an
unknown event — the history for that event genuinely is empty, which is a truthful answer, not a
shim requiring fabrication. Change the unknown-event branch to return an empty array instead of
an error.

## (c) `OBJECT FREQ` answers even without an LFU eviction policy configured

This is the inverse problem — FrogDB currently gives a (fabricated/misleading) answer where
Redis errors. Redis requires `maxmemory-policy` to be one of the LFU variants before `OBJECT
FREQ` means anything, and errors otherwise. Gate FrogDB's `OBJECT FREQ` on the same condition and
return Redis's error string when the policy isn't LFU.

## Acceptance criteria

- [ ] `CONFIG GET appendonly` returns `no`; `ParamId::ALL.len()` golden tests updated and passing
- [ ] `LATENCY HISTORY <unknown-event>` returns an empty array, not an error
- [ ] `OBJECT FREQ` errors (matching Redis's error string) unless `maxmemory-policy` is an LFU
      variant, and returns a real frequency value when it is
- [ ] Sweep of other standard config-name probes documented in the issue's `## Comments` even if
      no action is taken for a given name (so the "no truthful answer exists" judgment call is
      recorded, not silently skipped)

Size: S

## Resolution

appendonly noop param answers 'no' (golden counts 124->125, 77->78); LATENCY HISTORY unknown event returns empty array; OBJECT FREQ errors without an LFU policy with Redis's exact text. AOF/RDB param sweep declined all values that would fabricate state. Wave 1, commit 2f71b949; LATENCY RESET integer-reply fix followed in wave 3.
