# RESP frame injection — error messages are not CRLF-sanitized

Status: ready-for-agent
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

- [ ] Unit test at the protocol boundary: for a table of error payloads containing `\r`, `\n`,
      `\r\n`, and a trailing newline, the encoded RESP2 **and** RESP3 bytes contain exactly one
      terminating `\r\n` and no interior CR/LF (invariant P8). **Fails today.**
- [ ] Socket test in `redis-regression/tests/protocol_tcl.rs`, next to the existing parity cases:
      send a command name containing `\r\n+OK\r\n`, then `PING`, and assert the reply stream is
      exactly one error frame followed by exactly one `+PONG`.
- [ ] Both tests cover the pre-auth path, since the unknown-command error is reachable before
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
