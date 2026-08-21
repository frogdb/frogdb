# Unknown-command error uppercases the name and never lists the offending args

Status: done
Type: bug (error-message fidelity)
Area: connection / protocol

## Problem

FrogDB's unknown-command error uppercases the command name regardless of how the client typed
it, and always appends the literal string `", with args beginning with:"` without ever actually
listing the arguments. Redis preserves the input's original case and lists up to N quoted
arguments after that clause.

## Duplicated sites (all need the fix)

- `frogdb-server/crates/server/src/connection/guards.rs:491-496`
- `frogdb-server/crates/server/src/connection/guards.rs:545-560`
- `frogdb-server/crates/server/src/connection/routing.rs:40-47`
- `frogdb-server/crates/core/src/shard/execution.rs:136`

Original-case bytes are available — `ParsedCommand.name`
(`frogdb-server/crates/protocol/src/command.rs:27-45`) — but only the uppercase lookup key
currently gets plumbed through to the error path.

## Fix

Write one shared formatting helper: original-case name, a bounded list of quoted args, CRLF-safe
(there is already an injection regression test to run against —
`frogdb-server/crates/protocol/src/response.rs:1670-1698`). Use it at all four sites. Consider a
seam-lint gate ("unknown-command errors go through the helper"), consistent with the existing
seam-lint family (`agents/seam-lints.md`).

## Acceptance criteria

- [ ] `NOTACOMMAND arg1` → `ERR unknown command 'NOTACOMMAND', with args beginning with:
      'arg1', ` — byte-for-byte match with Redis
- [ ] Lowercase input (`notacommand arg1`) echoes back lowercase, not uppercased
- [ ] All four sites route through the shared helper (grep confirms no duplicate inline
      formatting remains)
- [ ] The CRLF-injection regression test at `response.rs:1670-1698` still passes against the new
      helper
- [ ] All four call sites covered by a regression test asserting the exact error string

Size: S/M

## Resolution

Shared frogdb_protocol::format_unknown_command_error at all four emit sites; original case, quoted-args clause, 128-byte truncation — verified byte-for-byte against Redis 8.6.1. Wave 1, commit 2f71b949.
