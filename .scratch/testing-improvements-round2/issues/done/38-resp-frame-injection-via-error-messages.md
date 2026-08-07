# RESP frame injection — error messages are not CRLF-sanitized

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/08 F3 · MASTER.md §3
Score: severity 4 · likelihood 2 · effort 1 · priority 15
Area: frogdb-protocol / error encoding

## Context

The client-controlled command name is interpolated raw into `-ERR unknown command '{cmd_name}'` at
six sites and the encoder writes it verbatim, so a name containing `\r\n+OK\r\n` puts three frames
on the wire where the client expects one. The client's *next* reply read then consumes
attacker-authored frames, so a pooled client library can attribute a forged `+OK` to a later
command on the same connection — a protocol-level confused deputy, not a cosmetic formatting
issue. It is reachable pre-auth on any exposed port. Redis maps CR/LF to spaces before replying;
there is no equivalent anywhere in this workspace.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- The command name is client-controlled binary and is interpolated raw:
  `crates/server/src/connection/guards.rs:485` — `"ERR unknown command '{cmd_name}', with args
  beginning with:"` — and identically at `guards.rs:540`,
  `crates/server/src/connection/routing.rs:44-46`, `crates/core/src/shard/execution.rs:134`,
  `crates/core/src/shard/scripting.rs:193`, `crates/core/src/scripting/gate.rs:423`.
- The encoder writes the string verbatim — `redis-protocol-6.0.0/src/resp2/encode.rs`:
  ```rust
  fn gen_error(x, data: &str) { do_gen!(x, gen_be_u8!(Error.to_byte()) >> gen_slice!(data.as_bytes()) >> gen_slice!(CRLF)) }
  ```
- Redis maps CR/LF to spaces before replying (`sdsmapchars(s, "\r\n", "  ", 2)` in
  `addReplyErrorFormatInternal`). `rg` finds no equivalent anywhere in this workspace.
- So `*1\r\n$10\r\nAB\r\n+OK\r\nX\r\n` yields
  `-ERR unknown command 'AB\r\n+OK\r\nX', with args beginning with:\r\n` — three frames on the
  wire where the client expects one.

## What to fix

1. Add a single sanitiser at the error-encoding boundary (not at the six call sites) that maps
   `\r` and `\n` to spaces in every error payload, RESP2 and RESP3.
2. Audit the other five interpolation sites for any additional client-controlled substring
   (arguments, key names) that reaches an error string.
3. Add the encoder-level table test and the socket-level stream test below.

## Acceptance criteria

- [x] Unit test at the protocol boundary: for a table of error payloads containing `\r`, `\n`,
      `\r\n`, and a trailing newline, the encoded RESP2 **and** RESP3 bytes contain exactly one
      terminating `\r\n` and no interior CR/LF (invariant P8). **Fails today.**
- [x] Socket test in `redis-regression/tests/protocol_tcl.rs`, next to the existing parity cases:
      send a command name containing `\r\n+OK\r\n`, then `PING`, and assert the reply stream is
      exactly one error frame followed by exactly one `+PONG`.
- [x] Both tests cover the pre-auth path, since the unknown-command error is reachable before
      `AUTH`.

## Test boundary

**1** for the encoder table — pure encoding. **4** for the stream test: the desync only manifests
as a *stream* property across two commands on one connection, which needs a real socket; nothing
below level 4 can observe frame boundaries as the client sees them.

## Depends on

Nothing. Note the structural point in `MASTER.md` §6: the real RESP decoder (`FrogDbResp2`) lives
in `server/src/connection/{codec,frame_io,util}.rs`, not the protocol crate — the sanitiser must
land where the encoder actually is.

## Re-triage 2026-08-06

**Verdict: still-valid**

Reproduces; no sanitiser exists anywhere. All six interpolation sites survive, with two line
corrections: `frogdb-server/crates/server/src/connection/guards.rs:492` (was 485) and
`guards.rs:547` (was 540); unchanged are `connection/routing.rs:44`,
`frogdb-server/crates/core/src/shard/execution.rs:134`,
`core/src/scripting/gate.rs:451` (was 423) and `core/src/shard/scripting.rs:208` (was 193). The
conversion boundary is `frogdb-server/crates/protocol/src/response.rs:229-231` (RESP2
`WireResponse::Error` → `Resp2BytesFrame::Error`) and `response.rs:303-306` (RESP3
`Resp3BytesFrame::SimpleError`) — both hand the payload to `Str::from_inner` verbatim with no
CR/LF mapping, and the RESP2 `BlobError` downgrade at `response.rs:242-249` is a third unguarded
path. A grep for any `sanitize`/CRLF-mapping helper across `crates/protocol/src` and
`crates/server/src/connection` finds nothing. Note the same two `Str::from_inner(...).expect("error
messages must be valid UTF-8")` calls are a latent panic surface if any error payload ever reaches
them without a prior `from_utf8_lossy` — worth folding into the same sanitiser.

## Resolution

Fixed 2026-08-07. Confirmed live: the pre-fix encoder emitted
`-ERR unknown command 'AB\r\n+OK\r\nX', with args beginning with:\r\n` — three frames — and the
new tests fail against it (verified by reverting the sanitiser body and re-running: 5 of 7 new
unit tests fail).

**The fix — one sanitiser at the encoding boundary.**
`frogdb_protocol::sanitize_error_message(Bytes) -> Str`
(`frogdb-server/crates/protocol/src/response.rs`) is the single chokepoint every CRLF-framed error
payload passes through. All three unguarded paths now call it:

- RESP2 `WireResponse::Error` → `Resp2BytesFrame::Error`
- RESP2 `WireResponse::BlobError` downgrade → `Resp2BytesFrame::Error`
- RESP3 `WireResponse::Error` → `Resp3BytesFrame::SimpleError`

RESP3 `BlobError` is deliberately *not* sanitised: `!<len>\r\n<bytes>\r\n` is length-framed, so an
embedded CR/LF cannot start a frame, and it is the documented escape hatch for byte-exact error
payloads. `resp3_blob_error_keeps_crlf_because_it_is_length_framed` pins that asymmetry.

**Semantics — Redis parity.** Redis applies `sdsmapchars(s, "\r\n", "  ", 2)` in
`addReplyErrorFormatInternal` (`networking.c`) and again on the command name in
`commandCheckExistence` (`server.c`): every `\r` and every `\n` becomes one space, in place,
byte-length preserving, no truncation, no escaping. Matched exactly. One deliberate divergence:
Redis also `sdstrim`s leading/trailing CR/LF before mapping, so a trailing newline disappears
there and becomes a trailing space here — mapping alone is sufficient for the framing invariant,
and staying length-preserving keeps `estimate_resp2_frame_size` byte-exact.

**Latent panic folded in.** The two `Str::from_inner(..).expect("error messages must be valid
UTF-8")` calls are gone; a non-UTF-8 payload is now `from_utf8_lossy`-replaced instead of
panicking the connection task.

**Audit of the other five interpolation sites** (`guards.rs:492`, `guards.rs:547`,
`connection/routing.rs:44`, `core/src/shard/execution.rs:134`, `core/src/scripting/gate.rs:460`,
`core/src/shard/scripting.rs:208`): every one builds its message with `Response::error(format!(..))`
→ `Response::Error(Bytes)` → the boundary above. None bypasses it, so none needs a second
sanitiser. The wider sweep found ~119 `error(format!` sites, many interpolating *other*
client-controlled bytes — key names and consumer-group names (`shard/blocking.rs:497`, `:1144`),
index names (`shard/search/{query,create,spellcheck,tagvals}.rs`), function/library names
(`shard/functions.rs:39,47`, `server/function_store.rs:121`), subcommand and metric names
(`connection/hotkeys.rs:90,159,167`), plus every Lua-authored `redis.error_reply` string. All of
them ride the same boundary and are now covered — which is the argument for fixing at the
conversion boundary rather than at the call sites.

Two out-of-scope findings, *not* fixed here (neither is RESP frame injection — both are
line-injection into a length-framed bulk reply):

1. `CLIENT SETNAME` (`server/src/connection/client_conn_command.rs:210`) rejects only `' '`.
   Redis rejects every byte outside `!`..`~`, precisely so `CLIENT LIST` rows stay splittable; a
   FrogDB client name containing `\n` forges a `CLIENT LIST` row.
2. `HELLO … SETNAME` (`server/src/connection/auth_conn_command.rs:414`) applies *no* validation at
   all, not even the space check — and it is reachable pre-auth.

**Ordering note (divergence from Redis, not a defect found here).** FrogDB's `DispatchStage` runs
`PreChecks` (NOAUTH) *before* `CommandLookup`, whereas Redis 7's `processCommand` calls
`commandCheckExistence` before the auth check. So on a `requirepass` server FrogDB answers NOAUTH
rather than the unknown-command error. The pre-auth reachability that makes this a security issue
is therefore the passwordless-exposed-port posture (and any `AUTH`-exempt path), which is what the
socket test exercises: a connection that has issued no `AUTH`.

**Tests.** `frogdb-protocol` (`response.rs`, unit / level 1) — `resp2_error_…`,
`resp3_simple_error_…`, `resp2_blob_error_downgrade_encodes_as_exactly_one_frame` (each over a
10-row payload table: bare CR, bare LF, CRLF, the `+OK` injection, a multi-frame injection,
trailing LF, trailing CRLF, leading CRLF, newlines-only, invalid UTF-8),
`error_sanitizer_maps_crlf_to_spaces_like_redis`,
`error_sanitizer_is_lossy_not_panicking_on_invalid_utf8`,
`resp3_blob_error_keeps_crlf_because_it_is_length_framed`.
`frogdb-redis-regression` (`tests/protocol_tcl.rs`, socket / level 4) —
`error_reply_with_embedded_crlf_does_not_inject_frames`.

**Failure-mode row: none added.** The FM spec areas are `{txn, vll, persistence, replication,
cluster, blocking}` (`.scratch/hardening/specs/`); there is no protocol or security spec, and
`just lint-failure-modes` would reject a tag naming a row that no spec file defines. P8 comes from
`proposals/08-protocol.md`, not from a failure-mode spec. Per hardening-2, security becomes a
formal area under W4 — the row belongs there, witnessed by the tests above.

**Follow-up for hardening-2 C10.** The PRD assumed the fix would live in `Response::error` and
that the lint would pin that constructor against 9 direct enum constructions. Landing the
sanitiser at the *encoding* boundary makes those 9 constructions harmless, so the C10 lint should
be re-scoped: pin that `Resp2BytesFrame::Error`, `Resp3BytesFrame::SimpleError`, and the RESP2
`BlobError` downgrade are constructed nowhere but inside `sanitize_error_message`'s callers in
`protocol/src/response.rs`. That is a 3-site, one-file rule instead of a workspace-wide
constructor rule.
