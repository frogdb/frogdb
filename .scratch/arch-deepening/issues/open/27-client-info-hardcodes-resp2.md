# 27 — `CLIENT INFO`/`CLIENT LIST` report `resp=2` for every client, including RESP3 ones

Status: needs-triage

## What to build

`ClientInfo::to_client_list_entry` (`frogdb-server/crates/core/src/client_registry/info.rs:51-105`)
builds the `CLIENT LIST`/`CLIENT INFO` line from a single `format!` template at `info.rs:83`. The
`resp=` field is not a placeholder — it is the string literal `resp=2` baked into the template,
between `redir=-1` and `lib-name={}`. Every client on every server reports `resp=2`, including a
connection that successfully negotiated RESP3 via `HELLO 3`.

The value is not merely unpopulated: `ClientInfo` (`info.rs:14-48`) has **no protocol-version field
at all**, and the registry never receives one — `grep protocol_version` across
`crates/core/src/client_registry/` returns nothing, while the connection task holds the real value
in `state.protocol_version` and commits it before replying to `HELLO`
(`crates/server/src/connection/auth_conn_command.rs`, `set_protocol_version` on the pre-reply
path). So the fix is not a one-line template edit: it needs a `resp` (or `protocol_version`) field
on `ClientInfo`, threaded through the registry entry and the six `ClientInfo` construction sites
(`client_registry/mod.rs:660`, `:685`, `:753`, plus three in tests), and a registry update on HELLO
negotiation and on the reset path that restores RESP2. That is why the source proposal recorded it
as needing an owner rather than claiming it as a hotfix.

Blast radius is observability, and it is LIVE on main. It matters more than a cosmetic wrong field
because `resp` is the value operators use to confirm a client actually got the protocol it asked
for, and RESP3 negotiation is exactly the thing a per-connection egress path branches on — a
misreported `resp` turns a protocol-negotiation incident into an undiagnosable one. Redis reports
the client's real negotiated RESP version in this field. Per the standing preference that
observability accuracy beats Redis parity, a hardcoded value is worse than an absent one: the
correct outcome is a real value, and if that is judged too expensive, dropping the field is
preferable to lying.

Adjacent, distinct, already filed: issue 79 (F16) in
`.scratch/testing-improvements-round2/issues/open/` covers `oll`/`omem`/`output_list_*` hardcoded
zeros in the same format string; proposal 86's H2 covers `obl` reading an always-empty buffer.
Neither touches `resp`.

## Acceptance criteria

- [ ] A connection that has issued `HELLO 3` reports `resp=3` in its own `CLIENT INFO` and in
      another connection's `CLIENT LIST`; a RESP2 connection reports `resp=2`.
- [ ] The value follows the connection back down when the protocol version is reset, rather than
      latching.
- [ ] Regression test `test_client_info_reports_negotiated_resp_version`
      (`frogdb-server/crates/server/tests/`): a RESP3 client and a RESP2 client are both connected;
      `CLIENT LIST` from a third connection shows `resp=3` and `resp=2` on the right rows. Fails
      against today's tree.
- [ ] `just test frogdb-server client_info_resp` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 86 (`.scratch/arch-deepening/proposals/86-resp3-egress-codec.md`),
§Recorded, not claimed — hardcoded `resp=2` at `client_registry/info.rs:83`.

## Comments
