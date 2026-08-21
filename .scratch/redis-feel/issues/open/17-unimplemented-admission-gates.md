# `noscript` / `loading` / `stale` are advertised but never enforced

Status: needs-triage

## Origin

Wave-D2 flag-parity work: building the permanent
`vendored_command_flags_agree_with_command_info_flags` gate meant deciding, per
flag, whether our value and upstream's are even claims about the same thing. Three
flags had to be dropped from the comparison entirely (`UNCOMPARED_FLAGS` in
`frogdb-server/crates/server/src/server/upstream_metadata_tests.rs`) because
FrogDB has no reader for them.

## What is wrong

`CommandFlags::NOSCRIPT`, `CommandFlags::LOADING` and `CommandFlags::STALE` are
declared on ~200 command specs and emitted in `COMMAND INFO`, but a
tree-wide search finds no consumer of any of the three. Each names an admission
gate Redis implements in `processCommand`:

- **`noscript`** — Redis refuses the command when called from a script. FrogDB's
  Lua sandbox restricts globals rather than the command surface, so
  `redis.call('SUBSCRIBE', ...)` is gated (if at all) by something other than
  this flag.
- **`loading`** — Redis serves only `loading`-flagged commands while an RDB/AOF
  load is in progress. FrogDB serves every command during recovery.
- **`stale`** — Redis refuses non-`stale` commands on a replica whose link to
  the primary is down and `replica-serve-stale-data no`. FrogDB applies no
  per-command refusal there.

So `COMMAND INFO` currently advertises three admission policies FrogDB does not
have. Under ADR-0005 that is the wrong direction: the reply should describe what
FrogDB does.

## Why it matters

`loading` and `stale` are the load-bearing ones. A client or proxy that reads
`stale` to decide what it may send to a link-down replica gets an answer FrogDB
will not honor in either direction — it serves everything. `replica-serve-stale-data`
already exists as a config knob elsewhere in the tree, which makes the absent
enforcement a config that silently does nothing rather than a feature we never
claimed.

## Candidate direction

Two ends, and either is defensible as long as flag and behavior agree:

1. **Implement the gates.** `loading` and `stale` are small: one check in the
   dispatch path against the recovery/link state, keyed on the flag. That also
   gives `replica-serve-stale-data` a meaning. `noscript` needs the scripting
   crate to consult the flag before dispatching a `redis.call`.
2. **Stop advertising them**, the way `CommandFlags::RANDOM` was dropped in D2 —
   drop the bits and the wire spellings, and record the omission.

Option 1 is preferred for `loading`/`stale` (real compatibility behavior behind
an existing knob) and option 2 is the fallback if the gate is deliberately out of
scope. Once either lands, remove the flag from `UNCOMPARED_FLAGS` so the parity
gate starts covering it.
