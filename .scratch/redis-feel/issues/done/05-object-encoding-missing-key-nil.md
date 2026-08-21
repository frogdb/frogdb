# `OBJECT ENCODING` on a missing key returns a doubled "ERR ERR no such key" error, not nil

Status: done
Type: bug (error fidelity)
Area: commands / generic

## Problem

`OBJECT ENCODING nosuchkey` against FrogDB returns `ERR ERR no such key` — a visibly doubled
`ERR` prefix — where Redis 8.6 returns a **nil reply**, not an error at all.

## Root cause

`frogdb-server/crates/commands/src/generic.rs:431-433` constructs a `CommandError::InvalidArgument`
whose message already embeds a literal `"ERR "` prefix, and `CommandError`'s `Display` impl
(`frogdb-server/crates/types/src/error.rs:119`) prefixes `ERR` again on top — hence the double
prefix. Sibling call sites at `generic.rs:111` and `generic.rs:209` construct their errors
correctly (no embedded prefix).

## Fix

`OBJECT ENCODING` on a missing key is not an error condition in Redis — return `Response::null()`
instead of any `CommandError`. While in this file, sweep for other double-`"ERR "`-embed sites
(the same footgun as here) and fix them too.

## Existing test that codifies the wrong behavior

`frogdb-server/crates/redis-regression/tests/introspection_tcl.rs:774-784` currently asserts the
error-and-double-prefix behavior. It must be flipped to assert a nil reply as part of this fix,
not left passing against the old behavior.

## Acceptance criteria

- [ ] `OBJECT ENCODING nosuchkey` returns nil, matching Redis 8.6
- [ ] `introspection_tcl.rs:774-784` updated to assert nil
- [ ] Sweep of `generic.rs` (and any other file using the same `CommandError::InvalidArgument`
      pattern) for other double-`"ERR "` embeds; each one found gets its own fix or its own
      filed issue if out of scope
- [ ] `OBJECT ENCODING` on an existing key is unaffected (regression-tested)

Size: S

## Resolution

OBJECT ENCODING (and FREQ) on a missing key return nil, matching Redis 8.6.1 source; regression test flipped; three more double-ERR sites fixed (DELEX x2, XSETID). Wave 1, commit 2f71b949.
