# Registry-wide argument-fuzz property harness — no test asserts "no command unwinds"

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I11
LOE: 2–4 days (estimated)
Tier: B
Area: frogdb-core / `shard_driver` property harness (all registered commands)
Asked by: 06 (F5) — *"the biggest ask"* in that area. **Dropped from `MASTER.md` §6.**

## Context

Argument-handling bugs — panics on adversarial scalars, unbounded allocations from a hostile
count field — are found one command at a time today, which means they are found once per
audit and never again. One property harness over the whole registry closes the class. The
core audit called this its biggest ask, and the consolidation's first pass dropped it from
`MASTER.md` §6, so it has been under-weighted so far.

## Evidence

- **Shape**: built on `shard_driver`; for every registered command, drive adversarial scalars
  into every arity position and assert "never unwinds".
- **Why it ranks**: one harness closes an entire bug class across all ~250 commands rather
  than per-area. Several of the unbounded-allocation findings (06/F9, 07/F14, 10/F6) are
  instances of what it would catch generically.

## What to build

1. A property harness on top of `shard_driver` that enumerates every registered command and,
   for each arity position, drives adversarial scalars (empty, oversized, negative, huge
   counts, non-UTF-8, boundary integers).
2. The assertion is "never unwinds" — a panic, abort or poisoned shard is a failure; an error
   reply is a pass.
3. An explicit, documented skip list for commands that cannot participate (destructive,
   blocking, requires external state), so silence is not mistaken for coverage.
4. A memory bound on the run, so an unbounded allocation surfaces as a harness failure rather
   than an OOM-killed test process.

## Acceptance criteria

- [ ] The harness enumerates commands from the registry rather than a hand-written list, so a
      newly registered command is covered automatically.
- [ ] Every arity position of every non-skipped command receives adversarial scalars, and any
      unwind fails the test with the command name and the offending argument.
- [ ] The skip list is explicit, in-repo, and each entry carries a one-line reason.
- [ ] The harness fails, not OOMs, when a command allocates past a configured bound.

## Test boundary

Level 3 — the harness needs real dispatch and a real shard worker to observe an unwind, but
nothing about argument handling requires a socket; running ~250 commands × arity positions
through level 4 would be prohibitively slow.

## Depends on

Issue 01, `.scratch/testing-improvements-round2/issues/`.

## Re-triage 2026-08-06

**Verdict: still-valid**

No registry-wide argument-fuzz harness exists. The `shard_driver` harness moved out of
`core/tests/shard_driver/` into its own crate — `frogdb-server/crates/shard-harness/`
(`src/harness.rs` = `ShardDriver`, `src/generator.rs` = the proptest schedule generator) — and
neither it nor any of its 13 scenario files enumerates the registry; the only registry
enumeration in the tree is `RecordingBroadcaster::command_names`
(`shard-harness/src/recording_broadcaster.rs:61`), which is unrelated. The nearest existing thing
is the `cmd_dispatch` fuzz target (`testing/fuzz/fuzz_targets/cmd_dispatch.rs`), which now
actually runs nightly again after `45591265` restored fuzz CI — but it fails every criterion
here: it drives a **hand-written 37-command list** (`COMMANDS`, lines 11-19) rather than the
registry, it `continue`s past any arity the handler rejects (line 63-65) instead of driving every
arity position, and it has no documented skip list and no memory bound. `proptest_commands.rs`
in `frogdb-server/crates/server/tests/` only re-implements scalar parsers, not dispatch.
