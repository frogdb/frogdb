# Dev-harness friction: `just dev` readiness timeout and silent memtier failure

Status: needs-triage
Type: tooling annoyance
Area: dev tooling / load testing

## What happened

Two annoyances hit while running the 2026-08-15 feel test, neither blocking the test itself but
both worth deciding on.

### (a) `just dev` readiness timeout doesn't account for cold-build compile time

`testing/load/scripts/dev_server.py`'s readiness wait killed the build twice with `Error: FrogDB
failed to start within timeout` — the timeout clock apparently starts before or during the
build rather than after the binary starts running, so a cold (uncached) compile alone can exceed
it. This is worse than it looks on this machine specifically: a build running under macOS
background QoS gets its disk I/O throttled, stretching compile time further and making the race
more likely to lose.

Candidate direction: start the readiness wait clock only after the build step completes and the
server process is actually launched, not before.

### (b) memtier load generator exits 2 immediately after server readiness

`dev_server.py`'s memtier load-generation step exited with code 2 immediately after the server
reported ready, on 2026-08-15. `memtier_benchmark` 8.6.1 (Homebrew, `/opt/homebrew/bin`) was
present and presumably the version invoked. The failure's stderr/stdout was swallowed by the
harness, so the actual cause is unknown.

Candidate direction: surface memtier's stderr instead of swallowing it, then re-run to find the
actual cause.

## Why `needs-triage`

Both are dev-harness friction, not product defects the feel test's compat-surface work depends
on. Scope needs a decision before this turns into a spec: is (a) a straightforward timing fix,
or does the readiness protocol need a rethink (e.g. a distinct "building" vs "waiting for
health-check" phase reported to the user)? Is (b) a memtier-version compatibility issue, a
harness argument-construction bug, or something in the server's startup sequence that changed
underneath it? Needs someone to reproduce with stderr surfaced before a fix can be scoped.

## Next step

Reproduce (b) with memtier stderr surfaced (candidate direction above) to get an actual error
message, then re-file or re-triage both items with enough detail to become `ready-for-agent`.
