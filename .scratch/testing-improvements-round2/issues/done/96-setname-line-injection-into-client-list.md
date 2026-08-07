# `CLIENT SETNAME` / `HELLO … SETNAME` allow line injection into `CLIENT LIST`

Status: done
Triage: 2026-08-07 — scheduled into hardening-2 **W4 security spec** (same client-controlled-substring
injection family as round2 #38, which is fixed). #38 covered every client string reaching an *error*
reply; this is the two remaining client strings reaching a *data* reply (`CLIENT LIST`). Fix is a
single shared `0x21..=0x7e` validator at both call sites (`client_conn_command.rs:210`,
`auth_conn_command.rs:414`) with the Redis error text; low risk, server crate only. Forcing test
below is the acceptance gate.
Type: bug (protocol / injection)
Origin: surfaced by the round2 #38 CRLF-sanitizer fix, 2026-08-07 — the wider error-site sweep
Area: frogdb-server / connection command handling

## Problem

Distinct from #38 (which was CR/LF *frame* injection into error replies, now fixed at the encoder
boundary). This is line injection into a **length-framed bulk reply** — `CLIENT LIST` — via the
client name, which is echoed verbatim into one space-and-newline-delimited row per connection.

Two sites:

1. `CLIENT SETNAME` (`frogdb-server/crates/server/src/connection/client_conn_command.rs:210`)
   rejects only the space byte (`' '`). Redis rejects every byte outside the printable range
   `!`..`~` (`networking.c` `clientSetName`) precisely so `CLIENT LIST` rows stay splittable. A
   name containing `\n` forges an extra `CLIENT LIST` row.
2. `HELLO … SETNAME` (`frogdb-server/crates/server/src/connection/auth_conn_command.rs:414`)
   applies **no** validation at all — not even the space check — and is reachable pre-auth.

## Fix

Match Redis: reject any client name byte outside `0x21..=0x7e` at both sites (single shared
validator), with the Redis error text (`ERR Client names cannot contain spaces, newlines or
special characters.`). Fold `HELLO SETNAME` through the same validator.

## Forcing test

`CLIENT SETNAME` / `HELLO 2 SETNAME` with a name containing `\n`, `\r`, and a `0x00` byte each
return an error and do not alter the connection name; a socket-level test asserting `CLIENT LIST`
remains one row per connection after an attempted injection.

## Resolution

Fixed 2026-08-07. Shared `validate_client_name` (printable ASCII `0x21..=0x7e`, Redis parity, empty
name still clears) added at `connection/util.rs`, routed through both `client_setname`
(`client_conn_command.rs`) and the HELLO SETNAME arm (`auth_conn_command.rs`). HELLO validates at its
application point (after the inline-AUTH clause), matching Redis option order. Forcing tests:
`test_client_setname_rejects_line_injection` + `test_hello_setname_rejects_line_injection`
(`integration_client.rs`, socket-level, assert error + name unchanged + `CLIENT LIST` stays one row
per connection) and `validate_client_name_rejects_non_printable` (`util.rs` unit) — 7/7 pass, no
regression.

## Comments

Found while auditing the six error-interpolation sites for round2 #38 (2026-08-07). The #38 fix
covers every client-controlled substring reaching an *error* reply; these two are the remaining
client-controlled substrings reaching a *data* reply. Candidate for the W4 security spec
(hardening-2), same family as #38.
