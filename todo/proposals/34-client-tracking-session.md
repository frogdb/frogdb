# Proposal: Client Tracking Session

Status: implemented
Date: 2026-07-04

## Problem

Client-side caching (CLIENT TRACKING) had two structural defects and one behavioral bug, all
symptoms of the same missing seam: no single owner for the tracking *session* — the unit made of
(a) the `ConnectionState` transition, (b) the local delivery plumbing (invalidation channel or
REDIRECT forwarding task), and (c) the per-shard registration.

### 1. BCAST prefixes: checked as if accumulating, stored as replacing

The `CLIENT TRACKING ON` handler checked new prefixes for overlap against prefixes *already on
the connection* (`handlers/client.rs:657-665`) — a check that is only meaningful if a second
`ON BCAST PREFIX` call **adds** to the first (Redis semantics: prefixes accumulate until
`TRACKING OFF`). But the write path **replaced**: `enable_tracking` (`state.rs:956-963`)
overwrote the whole `TrackingState` with only the new batch. Net effect of
`ON BCAST PREFIX a:` followed by `ON BCAST PREFIX b:`:

- `CLIENT TRACKINGINFO` reported only `b:` (Redis reports both).
- The overlap check ran against the *latest batch only*, so `ON BCAST PREFIX a:`,
  `ON BCAST PREFIX b:`, `ON BCAST PREFIX a:x` was **accepted** (Redis rejects `a:x` against
  `a:`) — the connection-level no-overlap invariant silently broke after two calls.
- Client memory accounting (`compute_client_memory`) undercounted tracking prefixes.

Invalidation delivery itself kept working *by accident*: shard-side
`BroadcastTable::register` (`core/src/tracking.rs:250-274`) is additive, so the shards
accumulated what the connection state forgot. Two owners of "which prefixes is this connection
tracking," disagreeing after the second call.

Redis also enforces mode-switch rules FrogDB lacked entirely (networking.c `clientCommand`):
switching BCAST on/off, or OPTIN↔OPTOUT, while tracking is enabled is an error — the client
must pass through `TRACKING OFF` first. FrogDB silently replaced the mode, which combined with
the shard-side accumulation could leave a non-BCAST connection still broadcast-registered on
every shard.

### 2. Teardown invariant triplicated with divergent shard messages

Logical teardown = tell shards to forget the registration + drop `invalidation_tx`/`rx` + abort
`redirect_task`. Three call sites each hand-rolled a different subset:

| Site | Shard message | Local plumbing |
|---|---|---|
| `TRACKING OFF` (`client.rs:767-791`) | `TrackingUnregister` | channels + task, inline |
| `RESET` (`auth.rs:579-587`) | `ConnectionClosed` (gated) | channels + task, inline, gated |
| Connection close (`lifecycle.rs:43-64`) | `ConnectionClosed` (gated) | task only, inline |

Nothing enforced that `TrackingUnregister` and `ConnectionClosed` agree on the tracking half; a
future field added to shard-side tracking state would need three call-site audits.

### 3. REDIRECT task leak on re-enable

Every `CLIENT TRACKING ON ... REDIRECT n` spawned a fresh forwarding task
(`client.rs:712-735`) without aborting the previous one — only OFF/RESET/close aborted. Repeated
re-enables leaked one task per call.

## Redis semantics (verified against redis.io + t_tracking.c / networking.c)

- **Prefixes accumulate**; flags (`NOLOOP`, `OPTIN`/`OPTOUT`) and `REDIRECT` are **replaced** on
  every `ON` call (enableTracking clears and re-sets the flag mask each time — an `ON` call
  omitting `OPTIN` after `ON OPTIN` drops back to default mode).
- New prefixes are collision-checked against the connection's existing prefixes and against each
  other (`checkPrefixCollisionsOrReply`); *equal* strings and the empty prefix count as overlap
  (`stringCheckPrefix` compares the common length).
- A bare `ON BCAST` registers the **empty prefix** ("match all") and — Redis quirk —
  **skips the collision check** for it: `ON BCAST PREFIX a:` then `ON BCAST` silently widens
  the registration to all keys, and a repeated bare `ON BCAST` is idempotent. Preserved as-is
  for parity (the tcl-derived regression test `tcl_trackinginfo_bcast_empty_prefix` already
  accepts the `[""]` TRACKINGINFO shape this produces).
- Mode-switch errors: "You can't switch BCAST mode on/off …" and "You can't switch OPTIN/OPTOUT
  mode …". Adopted verbatim, as are Redis's specific two-prefix overlap messages. The
  OPTIN+OPTOUT conflict keeps FrogDB's existing "mutually exclusive" text
  (`introspection_tcl.rs:1250` asserts it).

## Implemented design

### State half: `ConnectionState::enable_tracking` becomes the semantic gate

`enable_tracking` now takes a `TrackingEnableRequest` (the raw parsed flags — `bcast`, `optin`,
`optout`, `noloop`, `prefixes`, `redirect`) and returns
`Result<Vec<Bytes>, TrackingEnableError>`. All rejection rules live in this one transition, in
Redis's check order: PREFIX-without-BCAST, BCAST switch, OPTIN/OPTOUT-with-BCAST, OPTIN+OPTOUT,
OPTIN↔OPTOUT switch, then prefix collisions against the accumulated union and within the batch.
On success it merges the batch into the union (bare BCAST normalizes to the empty prefix) and
returns **the new batch only** — which is exactly what the caller must send to the shards,
because shard-side broadcast registration is additive. The error enum implements `Display` with
the wire messages, so the handler's mapping is `format!("ERR {err}")`.

This keeps the proposal-04 seam: the handler parses and does I/O; the state owns every invariant
and is unit-testable with no tokio runtime, shards, or sockets.

### I/O half: the tracking session cluster (`connection/lifecycle.rs`)

Three methods own the session; no call site re-implements a subset:

- `tracking_session_enable(req)` — state transition, then (aborting any stale REDIRECT task —
  fixes the leak) wire the delivery path and fan out `TrackingBroadcastRegister` /
  `TrackingRegister` to every shard.
- `tracking_session_disable()` — `disable_tracking()` early-return preserved, then
  `TrackingUnregister` fan-out + local teardown.
- `tracking_session_teardown_local()` — drop `invalidation_tx`/`rx`, abort `redirect_task`.
  Idempotent.

Call sites: `CLIENT TRACKING ON`/`OFF` (`handlers/client.rs`) call enable/disable; `RESET`
(`handlers/auth.rs`) and connection close (`notify_connection_closed`) call
`tracking_session_teardown_local()`.

### Teardown message decision: OFF differs from RESET/close deliberately

`TRACKING OFF` sends `TrackingUnregister` — it must *not* touch pub/sub state, because the
connection lives on with its subscriptions. RESET and connection close send `ConnectionClosed`,
whose tracking half is **identical** to `TrackingUnregister` (compare core's
`dispatch_tracking.rs:18-22` with the `ConnectionClosed` arm in `dispatch_pubsub.rs:109-115`)
and additionally drops pub/sub subscriptions — one message covers both teardowns those paths
need, with no double-teardown (`ConnectionClosed`'s subscription removal is idempotent against
RESET's own `exit_pubsub`). The equivalence and the reasoning are documented at the session
cluster; a shared shard-side helper for the three `self.tracking.*` removals would make it
compiler-enforced, but core's shard dispatch is outside this change's scope.

## Testing impact

Socket-free state tests (`state.rs`):

- Prefix accumulation across two ON calls; second call returns only the new batch.
- Overlap rejection against the accumulated union (not just the latest batch), including the
  equal-prefix case; rejected calls leave state untouched.
- Within-batch overlap rejection.
- Bare-BCAST empty-prefix behavior: registered unchecked, idempotent, collides with later
  explicit prefixes, and the Redis widening quirk.
- Mode-switch rejections (BCAST both directions, OPTIN↔OPTOUT) and re-allowance after OFF.
- Flags/redirect replaced per call while prefixes accumulate; OPTIN not re-specified drops to
  default mode.
- RESET-vs-OFF teardown equivalence: both leave tracking state field-for-field identical to
  fresh.

Integration (`tests/integration_client.rs`): prefix accumulation end-to-end (TRACKINGINFO union
+ invalidations for both prefixes after the second ON), overlap-vs-earlier-call rejection, and
mode-switch errors. Existing tracking suites (`integration_client.rs` tracking tests,
`redis-regression` `tracking_tcl`/`tracking_regression`/`introspection_tcl`) pass unchanged —
`tcl_bcast_adding_prefix` now passes for the right reason instead of via the shard-side
accident.

## Risks / open questions

- **TRACKINGINFO shape change for bare BCAST**: prefixes now report `[""]` instead of `[]`.
  This matches Redis; the tcl-parity test accepts both shapes.
- **Repeated identical prefix now errors** (`ON BCAST PREFIX a:` twice): matches Redis's
  `stringCheckPrefix` equality-counts-as-overlap. Clients that re-assert their prefix set must
  use OFF→ON (as they must in Redis).
- **Shard-side dedup**: with the widening quirk a connection can hold `""` plus explicit
  prefixes; a key matching several prefixes is sent once per match by
  `BroadcastTable::invalidate_matching` (within one message). Redis likewise delivers per-prefix
  duplicates. Left as-is; core tracking tables were read-only for this change.
