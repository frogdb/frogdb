# 03 — `just lint` never clippy-lints turmoil test bodies; S7 doc-comment rationale wrong

Status: done
Type: AFK
Origin: phase-4b task-14 implementer + reviewer findings

## What to build

Two small related cleanups:

1. **Lint gap:** `just lint frogdb-server` runs clippy without `--features turmoil`, so every `#[cfg(feature = "turmoil")]` test body (all of `tests/simulation.rs`'s sim tests) escapes clippy; only rustc warnings surface via the nextest build. Add a turmoil-featured clippy line to the `lint` (or `concurrency`) recipe in `Justfile`. Currently clean (verified 2026-07-21), so this is prevention.
2. **Doc-comment fix:** the S7 test's rationale for dropping the expiry sub-assertion (`simulation.rs`, `client_pause_write_vs_exec` doc comment) claims suppression is not client-observable. Imprecise: `OBJECT ENCODING` (raw `ctx.store.get`) and `OBJECT REFCOUNT` (raw `ctx.store.contains`) bypass the expiry check and DO observe physical presence of a suppressed-but-elapsed key. The real blocker is that constructing the scenario needs a real-clock TTL elapse under turmoil (cross-clock flakiness). Correct the comment so nobody concludes OBJECT is expiry-safe.

## Acceptance criteria

- [x] `just lint` (or a recipe it calls) clippy-checks the server crate with `--features turmoil --tests`, zero warnings
- [x] S7 doc comment states the real-clock-TTL rationale and notes OBJECT ENCODING/REFCOUNT observe physical presence

## Blocked by

None - can start immediately.
