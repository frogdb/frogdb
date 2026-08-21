# Dev-harness friction: `just dev` readiness timeout and silent memtier failure

Status: done
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

## Resolution

Both fixed in `testing/load/scripts/dev_server.py` (plus a new `just build-server` Justfile
recipe).

**(a) Readiness timeout vs. compile time.** Split into two explicit phases: `build_server()`
runs `just build-server <debug|release>` (new recipe: `cargo build -p frogdb-server`, unbounded,
output streamed live) to completion *before* the server binary is ever spawned; only after that
does `wait_for_port()`'s clock start, timing just the process's own startup, not compile time.
Phase transitions (`==> Building FrogDB...`, `==> Build complete.`, `==> Starting FrogDB...`,
`==> Waiting for FrogDB to be ready...`) are printed so it's obvious where time goes. Also fixed
a related bug found while reproducing: `main()` didn't line-buffer stdout, so when output was
redirected to a file/log (not a TTY — the exact scenario the phase-transition prints exist for)
Python fully block-buffered them and they only flushed at process exit, all out of order relative
to the subprocess (cargo/server) output they're supposed to bracket. Added
`sys.stdout.reconfigure(line_buffering=True)` at the top of `main()`.

**(b) memtier exit code 2 — root cause.** Harness bug, not a memtier-version or server bug.
`dev_server.py` passed `--print-interval 10` to memtier_benchmark, but that flag does not exist
in the installed Homebrew build (`memtier_benchmark v=2.3.0`, not 8.6.1 as originally reported —
apparently the formula was updated since). memtier's own arg parser rejects unrecognized options
by printing full `--help` usage and exiting 2, which is exactly the observed symptom; with
stderr no longer swallowed (memtier's stdout/stderr now captured to
`.dev-server-memtier.log`, tail printed on nonzero exit) the real message was immediately visible:
`memtier_benchmark: unrecognized option '--print-interval'`. Fix: dropped `--print-interval 10`
from the hardcoded `memtier_cmd` args (no replacement needed — memtier has no equivalent
"periodic table print" flag in this version, and the harness doesn't consume that output
anyway).

**Verification**: built the server from a clean cold-compile worktree (isolated from concurrent
sessions' in-progress WIP that had `main`'s workspace build broken at the time — see below), ran
`just dev`, confirmed phase-transition prints now appear interleaved with build/server output in
real time, confirmed memtier started and connected (5 clients), and confirmed sustained real
traffic for 30+ seconds (`frogdb_commands_total{command="SET"}` climbed 42092 → 96493,
`GET` 4675 → 10719 over ~20s) with memtier still running and zero errors. Dev server and memtier
processes killed and confirmed gone; state file cleaned up.

**Aside — unrelated build breakage observed on `main` mid-task**: at the time this issue was
worked, `main`'s workspace build was broken by another session's in-progress uncommitted WIP:
`frogdb-server/crates/replication/src/fullsync.rs` had added a `coverage` field to
`FullSyncMetadata`, but `frogdb-server/crates/replication/src/replica_session.rs` (two
construction sites, lines ~783 and ~892) hadn't been updated to match yet (`error[E0063]: missing
field 'coverage' in initializer of 'FullSyncMetadata'`). Not this issue's concern (pre-existing,
unrelated, actively being worked by someone else) — worked around by building this fix in an
isolated `git worktree` off a clean `HEAD` rather than touching or waiting on the shared tree.
