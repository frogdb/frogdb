# "Shard busy running a script" fixture — nothing in the suite talks to a shard mid-EVAL

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I5
LOE: 1–2 days (estimated)
Tier: B
Area: scripting / test fixtures (`server/tests/`)
Asked by: 09 (F4, F8, F15)

## Context

Every interesting scripting failure mode — busy-script rejection, `SCRIPT KILL`, the
unkillable-script path, partial-effect commit on timeout — requires a second connection to
observe a shard while a script is still running on it. No test in the suite sets that up, so
those paths are exercised only by whatever incidentally reaches them. Three findings in the
scripting audit are blocked on the same missing fixture.

## Evidence

- **Current state**: does not exist. Nothing in the suite starts a long-running EVAL and then
  talks to the same shard on a second connection.
- **Shape**: spawn a bounded-but-slow script, wait until the shard is observably busy, hand
  back both connections, guarantee teardown **even if the script cannot be killed** — the last
  clause matters because 09/F4 is precisely the `Unkillable` path.

## What to build

1. A fixture that spawns a bounded-but-slow script on a known shard and returns once the
   shard is *observably* busy — a polled observable signal, not a sleep.
2. Hand back both the script-running connection and a second connection bound to the same
   shard.
3. Teardown that always completes, including on the `Unkillable` path where `SCRIPT KILL`
   cannot stop the script — the fixture must not be able to hang a test run.

## Acceptance criteria

- [ ] The fixture returns only after the shard is observably busy, verified by an assertion
      rather than a fixed sleep.
- [ ] A test using the fixture issues a command on the second connection and asserts the
      busy-script response.
- [ ] A test that deliberately leaves an `Unkillable` script running still tears down and the
      suite proceeds; no test process is left behind.
- [ ] The script's duration is bounded, so a failed kill terminates on its own within the
      test timeout.

## Test boundary

Level 4 — the behaviour is inherently about two concurrent connections observing one shard,
which requires the connection layer; `shard_driver` has no notion of a second client.

## Depends on

Nothing.
