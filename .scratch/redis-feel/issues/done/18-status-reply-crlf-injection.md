# Simple-status replies are not CRLF-sanitized — Redis closed this in 8.6.1

Status: done
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

## Ruling (2026-08-21)

**Newtype + seam lint** — candidate direction (c), not (a) or (b).

`WireResponse::Simple` / `Response::Simple` stop carrying a raw `Bytes`. The payload becomes a
`SafeStatus` newtype with a private field and exactly two constructors:

- `SafeStatus::from_static(&'static str)` — `const fn`, for author-written literals (`"OK"`,
  `"PONG"`, `"QUEUED"`, `"NOKEY"`, ...). Its CR/LF scan is `const`-evaluable, so a literal
  carrying CR/LF is a compile error and the hot `+OK` path pays nothing (the scan folds away).
- `SafeStatus::sanitized(impl Into<Bytes>)` — for dynamic content. Maps every CR and every LF
  byte to a space, in place, byte-length preserving, no truncation — the same `sdsmapchars`
  semantics `sanitize_error_message` applies to error payloads. Buffers with nothing to map are
  handed through untouched (no allocation).

The encode paths (`to_resp2_frame` / `to_resp3_frame`) stay pass-through: safety is a property of
the type, not of the encoder, so a new status-reply site cannot bypass it by construction.

Deliberate divergence from `sanitize_error_message`: no lossy UTF-8 replacement. A RESP simple
string is `Bytes`, not `Str`, so there is no UTF-8 requirement to satisfy; CR and LF are ASCII and
never occur inside a multi-byte sequence, so the byte-level map is exactly the CR/LF half of the
error sanitizer while preserving non-UTF-8 payloads instead of mangling them.

Pinned by a new seam lint, `lint-status-sanitize` (`scripts/status-sanitize.py`, compile-free, in
`just lint-gates`):

1. Both enums declare `Simple(SafeStatus)`, and `SafeStatus`'s field is private — so no crate can
   build a status from a raw `Bytes`.
2. The raw `SafeStatus(..)` tuple construction appears only inside the two sanctioned
   constructors.
3. Repo-wide (non-test): every `SafeStatus::from_static(..)` argument is a string literal, so the
   const-checked escape hatch cannot be handed a runtime-derived `&'static str`.

## Acceptance

- A Lua script returning `{ok = "a\r\n+OK"}` puts exactly one frame on the wire, on RESP2 and
  RESP3, with the CR/LF mapped to spaces.
- The `crlf_injection_payloads` table in `protocol/src/response.rs` gains a status-frame
  counterpart asserting the single-frame invariant.
- Whichever chokepoint is chosen is pinned by a seam lint, so a new status-reply site cannot
  bypass it silently.

## Resolution

Implemented as ruled.

`SafeStatus` lives in `frogdb-server/crates/protocol/src/response.rs`, next to
`sanitize_error_message`, and is the payload of both `WireResponse::Simple` and
`Response::Simple`. `from_static` is a `const fn` whose CR/LF scan folds away for literals (and
makes a CR/LF-bearing literal a compile error in const position); `sanitized` maps CR and LF to
spaces byte-for-byte and returns the input `Bytes` untouched — no allocation — when there is
nothing to map. `Deref<Target = Bytes>` plus `PartialEq<str>`/`PartialEq<&str>` keep every
existing read site and pattern guard compiling, so only *construction* sites had to change.
`Response::ok()`/`pong()`/`queued()` are now the idiomatic way to build the three hot statuses.

Dynamic producers found and routed through `SafeStatus::sanitized`:

| site | source of the bytes |
| --- | --- |
| `core/src/scripting/executor.rs:399` | Lua `{ok = <string>}` — **the reachable exploit** |
| `server/src/connection.rs:854` | MONITOR event line (contains client-authored argv) |
| `server/src/connection/debug_conn_command.rs:402` | `DEBUG` key name echo |
| `commands/src/generic.rs:64` | `TYPE` — `key_type.as_str()` |
| `commands/src/command_meta.rs:238`, `:702` | `format!("@{cat}")`, `flag.to_lowercase()` |
| `server/src/migrate.rs:359`, `test-harness/src/server.rs:1345` | upstream `SimpleString` frame relay |

Everything else was an author-written literal and went to `from_static`.

Seam lint `lint-status-sanitize` (`scripts/status-sanitize.py`) is wired into `just lint-gates`
and documented in `agents/seam-lints.md`. All three rules were proven to fire: regressing a
variant to `Simple(Bytes)`, making the newtype field `pub`, adding a third raw-constructing `fn`,
and passing a non-literal to `from_static` each fail the gate; the clean tree passes.

Tests:

- `protocol/src/response.rs` — RESP2 and RESP3 single-frame assertions over the shared
  `crlf_injection_payloads` table, sanitizer-equivalence with `sanitize_error_message`,
  no-allocation pass-through, non-UTF-8 preservation, `const` construction, and the
  `should_panic` literal guard.
- `core/src/scripting/executor.rs` — `lua_ok_status_is_crlf_sanitized` (the forcing test, in the
  crate where the fix lives).
- `server/tests/integration_scripting.rs` —
  `test_eval_ok_status_cannot_inject_a_second_frame`: raw TCP, `{ok = redis.call('GET', ...)}`
  over a stored `OK\r\n-WRONGPASS forged\r\n$3\r\nfoo\r\n`, asserts exactly one `+…\r\n` frame and
  that `PING` still answers `+PONG` on the same socket.

One edit landed in a locked crate: `replication-runtime/src/test_shards.rs:157`, a mechanical
`Response::Simple(Bytes::from_static(b"OK"))` → `Response::ok()` in test-support code. Semantics
identical; made because the type change would otherwise leave the crate uncompilable.

## Origin

Found while bumping `REDIS_COMPAT_TARGET` 8.6.0 → 8.6.1 (2026-08-21) by reading the four-commit
upstream delta. Not caught by any FrogDB test or by the vendored-metadata join — this is a
behavior gap, not a metadata gap.
