# 07 — DEBUG command to backdate key expiry (kill the real-clock sleep in sim tests)

Status: needs-triage
Type: AFK
Origin: lazy-expiry plan review 2026-07-21

## What to build

A DEBUG subcommand (e.g. `DEBUG EXPIRE-BACKDATE <key> <ms>`) that rewrites a key's expiry timestamp to lie `<ms>` in the past, making the key already-expired without any wall-clock wait.

Motivation: TTL expiry is evaluated against real-clock `std::time::Instant` while turmoil advances a virtual clock, so every sim test that needs an elapsed TTL carries a `std::thread::sleep(50ms)` (the stage-1 lazy-expiry repros in `frogdb-server/crates/server/tests/simulation.rs`, and the F3 pin) — a real wall-clock stall inside a simulation that is otherwise deterministic and virtual-time-driven. This cross-clock hazard is also why the S7 (`client_pause_write_vs_exec`) expiry sub-assertion was dropped entirely rather than pinned (see [issue 03](./03-turmoil-clippy-lint-gap.md), which corrected the doc comment's rationale for the drop to cite this real-clock-TTL blocker specifically, not a client-observability argument). A backdate command makes "key is now expired" deterministic and instant under turmoil: set the expiry timestamp directly into the past instead of waiting for real time to catch up to it.

Note: Redis has no equivalent DEBUG subcommand for this. DEBUG is an unstable, non-parity-constrained interface (no client-visible protocol guarantee), so this is a pure test-infrastructure addition — there is no cross-engine parity obligation to reconcile.

## Acceptance criteria

- [ ] `DEBUG EXPIRE-BACKDATE <key> <ms>` (or equivalent name) implemented in the DEBUG command family, mirroring how `DEBUG SET-ACTIVE-EXPIRE` is wired (`frogdb-server/crates/server/src/connection/debug_conn_command.rs`, `debug_handler.rs`)
- [ ] Existing sim-test `std::thread::sleep` TTL-elapse waits replaced with the backdate command where applicable (stage-1 lazy-expiry repros, the F3 pin, and any `regression_`-flipped pins the lazy-expiry-parity fix-stage plan lands)
- [ ] S7 (`client_pause_write_vs_exec`) expiry sub-assertion feasibility revisited now that the real-clock-TTL blocker has a fix — either pinned or the doc comment updated to explain why it still cannot be, now that backdating is available
- [ ] Brief doc note added wherever DEBUG subcommands are documented (mirroring the existing `DEBUG SET-ACTIVE-EXPIRE` doc entry)

## Blocked by

None, but this should land **after** the lazy-expiry fix-stage plan (`docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`) completes, to avoid churning the very `std::thread::sleep`-based tests that plan is actively flipping from `#[ignore]`d repros to active `regression_` pins.
