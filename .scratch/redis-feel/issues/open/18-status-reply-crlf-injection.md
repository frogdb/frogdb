# Simple-status replies are not CRLF-sanitized — Redis closed this in 8.6.1

Status: needs-triage
Type: bug (security / protocol framing)
Area: protocol / scripting

## Problem

`sanitize_error_message` (`frogdb-server/crates/protocol/src/response.rs:209`) is the single
chokepoint every CRLF-framed *error* payload passes through, pinned by the `lint-error-sanitize`
seam gate (round-2 issue 38: a hostile command name could inject a forged `+OK` frame into a
pre-auth error reply). Simple strings carry the same framing invariant — `+<body>\r\n`, body must
contain no CR/LF — and get no such treatment:

- `to_resp2_frame`: `WireResponse::Simple(s) => Resp2BytesFrame::SimpleString(s)` (`:276`)
- `to_resp3_frame`: `WireResponse::Simple(s) => Resp3BytesFrame::SimpleString { data: s, .. }` (`:343`)

The reachable path is Lua: `frogdb-server/crates/core/src/scripting/executor.rs:398` turns a
script's `{ok = <string>}` table into `Response::Simple(Bytes::from(o))` verbatim. A script that
returns attacker-influenced data as a status — `return {ok = redis.call('GET', KEYS[1])}` — puts
the stored value straight into a `+…` frame, so a value containing `\r\n-WRONGPASS forged\r\n`
becomes two frames where the client expects one. That is the exact desynchronisation issue 38
fixed for errors.

## Upstream

Redis 8.6.1 is a security release whose one advisory is *"a user can manipulate data read by a
connection by injecting `\r\n` sequences into a Redis error reply"*. The fix adds
`addReplyErrorSdsExSafe` and `addReplyStatusSafe` (`src/networking.c`) and routes four sites
through them:

| Site | Upstream change |
| --- | --- |
| `src/script_lua.c` `luaReplyToRedisReply` (the `ok` field) | `addReplyStatusSafe` |
| `src/script_lua.c` `luaCallFunction` (script error text) | `addReplyErrorSdsExSafe` |
| `src/functions.c` `functionListCommand` (`Unknown argument %s`) | `addReplyErrorSdsSafe` |
| `src/module.c` `RM_ReplyWithSimpleString` | `addReplyStatusSafe` |

`addReplyStatusFormat` additionally trims trailing CR/LF and maps interior CR/LF to spaces.

FrogDB already covers the two *error* rows via `sanitize_error_message`. The two *status* rows are
open: the Lua `ok` path above, and — if FrogDB ever grows a module reply API — its equivalent.

## Candidate direction

Mirror the error design rather than upstream's per-site one: sanitize at the encode chokepoint so
new status-reply sites cannot reintroduce the hole, and extend `lint-error-sanitize` to cover
`SimpleString` alongside `Error`/`SimpleError`. Needs a ruling on the cost, since `+OK` is the
hottest reply on the write path and today it hands a static `Bytes` through untouched — the
sanitizer validates UTF-8 before its no-CRLF fast path. Options: (a) encode-time sanitize for all
simple strings, (b) sanitize only where non-static bytes enter (`executor.rs:398` and the
`Response::Simple(Bytes::from(..))` construction sites in `debug_conn_command.rs:402`,
`connection.rs:854`, `persistence_handler.rs:179`, `migrate.rs:359`), matching upstream, (c) a
newtype that makes an unsanitized dynamic status unconstructable.

## Acceptance

- A Lua script returning `{ok = "a\r\n+OK"}` puts exactly one frame on the wire, on RESP2 and
  RESP3, with the CR/LF mapped to spaces.
- The `crlf_injection_payloads` table in `protocol/src/response.rs` gains a status-frame
  counterpart asserting the single-frame invariant.
- Whichever chokepoint is chosen is pinned by a seam lint, so a new status-reply site cannot
  bypass it silently.

## Origin

Found while bumping `REDIS_COMPAT_TARGET` 8.6.0 → 8.6.1 (2026-08-21) by reading the four-commit
upstream delta. Not caught by any FrogDB test or by the vendored-metadata join — this is a
behavior gap, not a metadata gap.
