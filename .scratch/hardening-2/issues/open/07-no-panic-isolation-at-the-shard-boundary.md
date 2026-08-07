# A panic anywhere in a dependency kills a shard worker

Status: ready-for-agent
Type: bug (availability) + hardening (structural)
Severity: likelihood 2/3 (one unauthenticated trigger already known), consequence 3/3 (a shard
worker dies; every key it owns becomes unavailable) — score 6
Area: core / shard event loop

## Problem

`rg 'catch_unwind'` over `frogdb-server/crates/core/src/shard/` and
`frogdb-server/crates/server/src/` returns exactly one hit, and it is a test
(`transaction_conn_command.rs:621`). The shard event loop has **no panic isolation at all**, so a
panic raised anywhere below it — including inside a third-party dependency — takes the worker
down.

This is not hypothetical: round-2 issue 63 (`FT.SEARCH … LIMIT 0 0`) is an unauthenticated-
reachable shard panic, and it panics on an `assert_ne!` **inside tantivy/usearch** —
`search/src/wire.rs:121-122` parses, `:337-341` computes `limit: offset + limit == 0`, and the
C++/Rust index layer asserts. No `unwrap()` exists anywhere on the path, which is why a grep for
panicking constructs is the wrong instrument here (the campaign-2 W1 survey counted 62 such sites
and none of them was this bug).

The fix that generalizes is structural: catch at the boundary, so an unbounded class of
remote-crash defects degrades to an error reply.

## Fix

Wrap shard message execution in `std::panic::catch_unwind` (`AssertUnwindSafe` at the message
boundary), convert a caught panic into an `-ERR internal error` reply plus a counted metric and a
`error!` log carrying the command name, and keep the worker alive. Decide and record:

- Whether a caught panic should poison anything (an in-flight MULTI, a held continuation lock, a
  VLL entry) — a panic mid-transaction must not leave the lock owned by a dead command.
- Whether repeated panics on one shard should escalate (e.g. a counter that trips a health signal)
  rather than silently absorbing a loop.
- Interaction with `panic = "abort"` if any profile sets it — verify the release profile does not,
  or the isolation is inert where it matters.

Fix issue 63's arithmetic separately; the isolation is the backstop, not the fix.

## Forcing test

A shard-level test that injects a panicking command and asserts: the client gets an error reply,
the worker still answers the next command, the metric incremented, and no lock or transaction
state survives the panic. Plus the issue-63 regression once its arithmetic is fixed.

## Comments

Found by the campaign-2 chokepoint-lint survey, 2026-08-07, as the structural replacement for a
rejected `unwrap()`-grep lint (survey candidate C8).
