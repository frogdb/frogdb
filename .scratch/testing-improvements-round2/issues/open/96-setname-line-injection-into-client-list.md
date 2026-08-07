# `CLIENT SETNAME` / `HELLO … SETNAME` allow line injection into `CLIENT LIST`

Status: needs-triage
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

## Comments

Found while auditing the six error-interpolation sites for round2 #38 (2026-08-07). The #38 fix
covers every client-controlled substring reaching an *error* reply; these two are the remaining
client-controlled substrings reaching a *data* reply. Candidate for the W4 security spec
(hardening-2), same family as #38.
