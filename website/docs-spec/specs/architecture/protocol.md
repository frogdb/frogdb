# Spec: architecture/protocol.md
Status: update (several wire/limit/negotiation details and the dependency version
are wrong)
Audiences: A3 (architecture-curious), A5 (contributors)

Goal: The reader understands how FrogDB parses and serializes the Redis wire
protocol: the RESP2/RESP3 type set FrogDB models (`Response`/`WireResponse`), the
`ParsedCommand` it decodes into, the custom `FrogDbResp2` codec that wraps the
`redis-protocol` crate to tolerate Redis client edge cases, how RESP3 is
negotiated with `HELLO`, the protocol-level frame limits (hardcoded consts, not
config), and how server-assisted client-side caching (client tracking) rides RESP3
push frames. The reader leaves able to trace a byte on the wire to a decoded
command and back, and knows which limits/behaviors are FrogDB-specific.

Not in scope:
- Command dispatch/execution after parsing — architecture/request-flows and
  architecture/execution (link).
- Full per-command RESP shapes — that is redis.io + the command matrix. This page
  covers the codec and type system, not per-command encodings.
- Pub/sub semantics beyond the RESP3 push frame — architecture/connection and the
  relevant command docs.

Sources of truth (author MUST read; several current claims are wrong, so verify
each against source):
- `frogdb-server/crates/protocol/src/response.rs` — `WireResponse` and `Response`
  enums (all RESP2 + RESP3 variants, plus `NullArray` and internal non-wire
  variants), `to_resp3_frame`.
- `frogdb-server/crates/protocol/src/version.rs` — `ProtocolVersion` (`Resp2`
  default, `Resp3`).
- `frogdb-server/crates/protocol/src/command.rs` — `ParsedCommand { name: Bytes,
  args: Vec<Bytes> }`.
- `frogdb-server/crates/protocol/src/error.rs` — `ProtocolError` (EmptyCommand,
  ExpectedArray, InvalidFrame, Incomplete, FrameTooLarge).
- `frogdb-server/crates/server/src/connection/codec.rs` — `FrogDbResp2` codec
  (wraps `redis_protocol::codec::Resp2` via an `inner` field; pre-processes the
  `BytesMut` buffer for empty `\r\n` lines, negative multibulk `*-N`, oversized
  lengths). Consts `PROTO_MAX_BULK_LEN`, `PROTO_MAX_MULTIBULK_LEN`.
- `frogdb-server/crates/server/src/connection/handlers/auth.rs` — `handle_hello`
  (accepts versions `2..=3`, NOPROTO otherwise; sets protocol version with **no**
  downgrade guard).
- `frogdb-server/crates/server/src/connection/handlers/client.rs` +
  `.../state.rs` — `CLIENT TRACKING` parsing, `TrackingMode`
  (Default/OptIn/OptOut/Broadcast) + NOLOOP/PREFIX/REDIRECT flags.
- workspace `Cargo.toml` — `redis-protocol = { version = "6", features = ["bytes",
  "codec", "resp3"] }` (confirm; do not hardcode in prose — cite via note or S6).

Existing content: current `architecture/protocol.md`. The Response/RESP3 type
tables and the general "decode to ParsedCommand, encode from Response" framing are
sound. FACTUAL DISCREPANCIES to fix:

1. **Dependency version wrong.** The "Crate Configuration" snippet says
   `redis-protocol = { version = "5", features = ["bytes", "codec"] }`. Actual is
   version **6** with features `["bytes", "codec", "resp3"]`. Correct it, or better,
   remove the hardcoded snippet and describe the dependency in prose (per §6 no
   hardcoded versions — the exact version is a drift liability; state "the
   `redis-protocol` crate" and let source be authority).
2. **The custom codec is `FrogDbResp2`, not the stock `Resp2`.** The current page's
   "Tokio Codec" section shows `Framed::new(socket, Resp2::default())` as if FrogDB
   uses the upstream codec directly. It does not — it uses `FrogDbResp2`, which
   wraps the upstream `Resp2` decoder and pre-sanitizes the buffer to tolerate
   Redis client quirks (empty `\r\n` lines, `*-N` negative multibulk, oversized
   lengths). Add a dedicated section on `FrogDbResp2` and what it adds over the
   upstream decoder — this is a genuinely FrogDB-specific detail A3 will want.
3. **Frame limits are hardcoded consts, not config fields.** The
   "Configuration" table (`frame_timeout_ms`, `max_frame_size`,
   `max_bulk_string_length`, `max_array_elements`) is fabricated — none of those
   config field names exist. The real limits are:
   - `PROTO_MAX_BULK_LEN = 512 MiB` (536870912) — bulk-string cap.
   - `PROTO_MAX_MULTIBULK_LEN = 1_048_576` (1024×1024) — the doc's "1000000" is
     wrong; the value is 1048576.
   - There is **no** `frame_timeout_ms` — remove it.
   Replace the config table with a "Protocol limits" note citing the two real
   consts as consts (not runtime config), and state they are not `CONFIG`-tunable.
4. **HELLO cannot-downgrade claim is false.** `handle_hello` sets the protocol
   version unconditionally; a client that did `HELLO 3` can `HELLO 2` back to
   RESP2. Remove "cannot downgrade within the same session." Keep: RESP2 default,
   `HELLO 2`/`HELLO 3` accepted, NOPROTO for anything outside `2..=3` (also 0/1).
   Use the real NOPROTO string from source.
5. **Response enum omits `NullArray`.** Add `NullArray` (`*-1\r\n`, distinct from
   RESP3 `Null` `_\r\n`) to the type coverage. Optionally note the `Response` enum
   also carries internal non-wire variants (`BlockingNeeded`, `RaftNeeded`,
   `MigrateNeeded`, `SlotMigrationNeeded`) used by dispatch — mention only if the
   page claims the enum is "purely RESP2/RESP3," which it should not.
6. Client tracking: all seven flags (Default/OPTIN/OPTOUT/NOLOOP/BCAST/PREFIX/
   REDIRECT) are supported, but only Default/OptIn/OptOut/Broadcast are
   `TrackingMode` enum values; NOLOOP is a bool, PREFIX/REDIRECT are stored fields.
   State this precisely rather than calling all seven "modes." Note the validation
   rules (PREFIX requires BCAST; OPTIN/OPTOUT mutually exclusive and incompatible
   with BCAST).

Structure (H2/H3 outline):

## Overview
- FrogDB implements RESP2 and RESP3 and targets Redis 8.x wire compatibility.
  Inbound bytes → `ParsedCommand`; outbound `Response`/`WireResponse` → RESP2 or
  RESP3 frames depending on the negotiated version. Names the `redis-protocol`
  crate as the parsing foundation (no version literal in prose).

## Decoding: the FrogDbResp2 codec
- `ParsedCommand { name, args }`. The `FrogDbResp2` wrapper: what it pre-sanitizes
  and why (Redis client edge cases the upstream strict decoder rejects). RESP2
  `BytesFrame` in; note RESP3 output is produced separately via
  `WireResponse::to_resp3_frame`. `ProtocolError` variants and how each is handled.

## The response type system
- `Response`/`WireResponse` RESP2 variants + RESP3 variants + `NullArray`. Keep the
  RESP3 type reference table (it is good) and the "Benefits of RESP3" list. Add
  `NullArray` and the `Null` vs `NullArray` distinction.

## Protocol negotiation (HELLO)
- RESP2 default; `HELLO`/`HELLO 2`/`HELLO 3`; NOPROTO (real string) for out-of-range
  versions. State plainly that downgrade IS allowed (no session lock). Clients that
  never HELLO behave as RESP2 (Redis-compatible).

## Client tracking (server-assisted caching)
- RESP3 push invalidation; `TrackingMode` values vs the NOLOOP/PREFIX/REDIRECT
  flags; validation rules. Cross-link the relevant command docs.

## Protocol limits
- `PROTO_MAX_BULK_LEN` (512 MiB) and `PROTO_MAX_MULTIBULK_LEN` (1048576) as
  compile-time consts, not `CONFIG`-tunable. What happens on overflow (which
  `ProtocolError` / connection outcome). Remove the fabricated config table.

## Error handling and framing recovery
- Keep the recoverable vs unrecoverable framing-error framing, but verify each row
  against the real codec behavior (empty array, non-array top-level, oversized
  length, etc.) before keeping it — the current tables mix real and invented
  cases. Connections are not closed on ordinary command errors (Redis-compatible);
  confirm which framing errors actually close the connection in `FrogDbResp2`.

Generated data: none embedded. Dependency version and any Redis target version via
source / `versions.json` (S6). Error strings referenced (e.g. NOPROTO) must match
the glossary error table and source.

Drift guards:
- S7 code-path check covers `crates/protocol/...` and
  `server/src/connection/codec.rs`, `.../handlers/auth.rs`, `.../handlers/client.rs`.
- The two protocol-limit consts are `const` literals; if quoted numerically, cite
  the source line for reviewers and re-verify on edit (they are not generated).
- Do not hardcode the `redis-protocol` version in prose; it changed from 5→6 once
  already and is a proven drift point.
- Enum variant lists (`Response`, `TrackingMode`, `ProtocolError`) must match
  source; a reviewer diffs them on each edit until a generator exists.
