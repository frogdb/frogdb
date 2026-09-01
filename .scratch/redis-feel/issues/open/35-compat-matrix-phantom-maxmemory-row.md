# Compat matrix invents a `MAXMEMORY` command that does not exist in Redis

Status: ready-for-agent
Type: bug (generated docs)
Area: website / compat tooling

## Problem

`website/src/data/command-matrix.json` contains:

```json
{"name": "MAXMEMORY", "status": "unsupported", "arity": null,
 "note": "Present in Redis 8.6.1; not implemented in FrogDB."}
```

There is no `MAXMEMORY` command in Redis. It is absent from the vendored upstream metadata
(`website/src/data/redis-commands-8x.json`) — verified — and `maxmemory` is a *config parameter*,
which FrogDB supports via `CONFIG SET maxmemory`. So the published compat matrix asserts a false
fact about Redis and counts a phantom against FrogDB's unsupported total.

## Root cause

`website/scripts/matrix-gen.py:187`:

```python
all_names = sorted(set(frogdb_by_name) | set(redis_by_name) | set(command_impact))
```

`command_impact` keys come from `compat-exclusions.json`, which derives them from *test-suite
names*, not from a command registry. `maxmemory_regression.rs` / `maxmemory_tcl.rs` produce the
key `MAXMEMORY` (`compat-exclusions.json:3729`). That name then reaches the status branch at
`:206`:

```python
if not in_frogdb:
    status = "unsupported"
    note = f"Present in Redis {target_version}; not implemented in FrogDB."
```

The note claims presence in Redis without ever consulting `in_redis`. Any suite name that is not a
real command produces the same fabricated row. `MAXMEMORY` is currently the only one — a check of
`command_impact - commands.json - redis-commands-8x.json` returns exactly `['MAXMEMORY']` — so the
fix is small and can be locked down before it recurs.

## Fix

1. In `matrix-gen.py`, make the `not in_frogdb` branch depend on `in_redis`: only emit
   "Present in Redis <target>; not implemented in FrogDB." when the name is actually in the
   vendored upstream list.
2. A name present *only* in `command_impact` is a data bug in the exclusions input, not a command.
   Fail the generation with a clear message naming the offending key rather than silently emitting
   a row — the generator already runs under `--check` in CI, so this turns a silent falsehood into
   a build failure.
3. Fix the source: `compat-exclusions.json`'s suite-to-command mapping should map
   `maxmemory_*.rs` to the commands those suites actually exercise (or to no command), so the key
   stops being manufactured. Fix it in the generator (`website/scripts/compat-gen.py`), not the
   generated JSON.

## Also spotted while confirming this

`compat-exclusions.json:1315` excludes a `maxmemory_tcl.rs` test with the reason
"DUMP/RESTORE not implemented". Both `DUMP` and `RESTORE` are implemented
(`frogdb-server/crates/server/src/commands/persistence.rs`). That exclusion reason is stale and the
test may now pass — worth a sweep of exclusion reasons that assert a command is missing, since each
one is a claim that can rot.

## Acceptance criteria

- [ ] `MAXMEMORY` no longer appears in `command-matrix.json`; the `unsupported` summary count drops
      by one
- [ ] A `command_impact` key matching no command in either registry fails generation with a message
      naming the key
- [ ] Regression coverage for the generator asserting both of the above
- [ ] The stale "DUMP/RESTORE not implemented" exclusion is re-checked and either re-run or given a
      current reason

Size: S
