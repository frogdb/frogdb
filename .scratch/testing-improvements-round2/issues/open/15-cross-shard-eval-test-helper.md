# Cross-shard EVAL test helper — the cross-slot knob exists, the scripting-side helper does not

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I15
LOE: ~0.5 day (estimated)
Tier: B
Area: frogdb-test-harness / scripting (cross-shard EVAL)
Asked by: 12 (F3). **Dropped from `MASTER.md` §6.**

## Context

The VLL audit needs an `EVAL` whose declared keys land on different shards, to exercise the
multi-shard script path. The harness already permits cross-slot access in standalone mode;
what is missing is the small scripting-side helper that picks keys guaranteed to span shards
and runs the script against them. Half a day, and it was dropped from the `MASTER.md`
summary, so it currently looks unrequested.

## Evidence

- **Current state**: the `allow_cross_slot_standalone` knob already exists in
  `test-harness/src/server.rs`; only the scripting-side helper for "run an `EVAL` whose keys
  span shards" is missing.

## What to build

1. A helper that, given a running server, returns a set of keys guaranteed to hash to
   different shards — computed, not hardcoded, so it survives a shard-count change.
2. A wrapper that runs an `EVAL` with those keys declared as `KEYS`, under
   `allow_cross_slot_standalone`.
3. Use it from the VLL/scripting tests that need the multi-shard script path.

## Acceptance criteria

- [ ] The helper asserts, at call time, that the keys it returns map to at least two distinct
      shards; it fails loudly rather than silently returning same-shard keys.
- [ ] The key selection is derived from the live shard count, not a hardcoded key list.
- [ ] At least one test runs an `EVAL` over cross-shard keys through the helper and asserts
      the script's effects on both shards.
- [ ] The helper enables `allow_cross_slot_standalone` itself, so callers cannot forget it.

## Test boundary

Level 4 — the behaviour is cross-shard routing of a script's declared keys, which requires the
real server and the cross-slot config knob; a level-3 single-shard driver has no second shard
to span.

## Depends on

Nothing.
