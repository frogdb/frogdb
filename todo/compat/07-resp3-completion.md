# 7. RESP3 Protocol Completion

**Tests**: 22 (zset_tcl.rs: 9, scripting_tcl.rs: 2, protocol_tcl.rs: 6, pubsub_tcl.rs: 5)
**Source files**: See per-group breakdown below
**Status**: RESP3 infrastructure exists (HELLO negotiation, protocol-version-aware serialization,
`WireResponse` variants for all RESP3 types). Gaps are in specific command handlers that don't yet
produce RESP3-correct output and in RESP3 attribute support.

---

## Architecture

### WireResponse Variants (all exist)

All necessary RESP3 wire types are already defined in
`frogdb-server/crates/protocol/src/response.rs`:

| Variant           | Wire type       | Status      |
|-------------------|-----------------|-------------|
| `Map`             | `%` map         | Implemented |
| `Set`             | `~` set         | Implemented |
| `Double`          | `,` double      | Implemented |
| `Null`            | `_` null        | Implemented |
| `Push`            | `>` push        | Implemented |
| `Boolean`         | `#` boolean     | Implemented |
| `BigNumber`       | `(` bignumber   | Implemented |
| `VerbatimString`  | `=` verbatim    | Implemented |
| `BlobError`       | `!` blob error  | Implemented |
| `Attribute`       | `\|` attribute  | Stub only   |

### Protocol-Version-Aware Serialization Strategy

- `ProtocolVersion` enum in `frogdb-server/crates/protocol/src/version.rs` (Resp2/Resp3)
- Connection state holds `protocol_version` in `frogdb-server/crates/server/src/connection/state.rs:276`
- `CommandContext` receives `protocol_version` for shard-routed commands
- `WireResponse::to_resp2_frame()` downgrades RESP3 types (Map -> flat Array, Double -> BulkString, etc.)
- `WireResponse::to_resp3_frame()` encodes native RESP3 types
- `send_wire_response()` in `frame_io.rs` selects encoding path based on protocol version
- Pub/sub messages already use `to_response_with_protocol()` which emits `Push` for RESP3

### Key Gap Areas

1. **Blocking zset responses** (`blocking.rs`): BZPOPMIN/BZPOPMAX/BZMPOP format scores as bulk
   strings unconditionally; no access to `protocol_version` in `WaitEntry`
2. **RESP3 attributes**: `WireResponse::Attribute` is a stub that just returns the inner value;
   needs proper attribute map encoding for CLIENT TRACKING invalidation messages
3. **Scripting return values**: `lua_to_response()` always returns Array for Lua tables; needs
   RESP3 Map return for hash-like tables when protocol is RESP3
4. **DEBUG subcommands**: No DEBUG SET-ACTIVE-EXPIRE / DEBUG CHANGE-REPL-ID for big number,
   bool, verbatim string test fixtures

---

## Implementation Steps

### Group 1: Zset Commands (9 tests)

**Files to modify**:
- `frogdb-server/crates/commands/src/sorted_set/pop.rs` (ZPOPMIN, ZPOPMAX, ZMPOP)
- `frogdb-server/crates/commands/src/sorted_set/set_ops.rs` (ZINTER)
- `frogdb-server/crates/commands/src/sorted_set/store_remove.rs` (ZRANGESTORE)
- `frogdb-server/crates/commands/src/sorted_set/pop.rs` (ZRANDMEMBER)
- `frogdb-server/crates/core/src/shard/blocking.rs` (BZPOPMIN, BZPOPMAX, BZMPOP)
- `frogdb-server/crates/core/src/shard/wait_queue.rs` (WaitEntry)
- `frogdb-server/crates/commands/src/utils.rs` (score_response helper)

**Current state**:
- ZPOPMIN/ZPOPMAX: Already RESP3-aware. With count returns nested `[[member, Double], ...]`.
  Without count returns flat `[member, Double]`. Uses `scored_array_resp3()` and
  `scored_array_with_scores_resp3()`.
- ZINTER: Already RESP3-aware via `scored_array_resp3()`.
- ZRANDMEMBER: Already RESP3-aware via `scored_array_resp3()`.
- ZRANGESTORE: Returns `Integer(count)` which is protocol-agnostic. The test likely validates the
  source ZRANGE response format, not ZRANGESTORE itself.

**Remaining work**:
1. **BZPOPMIN/BZPOPMAX** (blocking path in `blocking.rs:438-484`): Score is formatted as
   `Bytes::from(score.to_string())` unconditionally. Needs RESP3 Double. Add
   `protocol_version: ProtocolVersion` to `WaitEntry` so the shard can format the response
   correctly. In RESP3, return `[key, member, Double(score)]` instead of
   `[key, member, BulkString(score)]`.
2. **BZMPOP** (blocking path in `blocking.rs:486-511`): Same issue - elements are
   `[member, BulkString(score)]` pairs. In RESP3, score should be `Double`.
3. **Propagate protocol_version to WaitEntry**: The connection handler creates blocking entries in
   `frogdb-server/crates/server/src/connection/handlers/blocking.rs`. Pass
   `self.state.protocol_version` through the blocking flow.

**Test mapping**:
- `ZINTER RESP3 - $encoding` -> ZINTER WITHSCORES returns Double scores (already works)
- `Basic $popmin/$popmax - $encoding RESP3` -> ZPOPMIN/ZPOPMAX without count (already works)
- `$popmin/$popmax with count - $encoding RESP3` -> ZPOPMIN/ZPOPMAX with count (already works)
- `$popmin/$popmax - $encoding RESP3` -> same as above variant
- `BZPOPMIN/BZPOPMAX readraw in RESP$resp` -> blocking pop score format (needs fix)
- `ZMPOP readraw in RESP$resp` -> ZMPOP already works; readraw validates wire encoding
- `BZMPOP readraw in RESP$resp` -> blocking BZMPOP score format (needs fix)
- `ZRANGESTORE RESP3` -> ZRANGESTORE + ZRANGE source validation
- `ZRANDMEMBER with RESP3` -> ZRANDMEMBER WITHSCORES returns Double (already works)

### Group 2: Scripting (2 tests)

**Files to modify**:
- `frogdb-server/crates/core/src/scripting/executor.rs` (`lua_to_response()` at line 320)
- `frogdb-server/crates/core/src/scripting/lua_vm.rs` (CommandExecutionContext)

**Current state**:
- `lua_to_response()` converts Lua tables to `Response::Array` by iterating numeric indices 1..N.
  Non-sequential (hash-like) tables where iteration yields nothing produce `Response::Null`.
- `response_to_lua()` already converts `Response::Map` to Lua tables correctly.
- `CommandExecutionContext` already carries `protocol_version`.

**Remaining work**:
1. **Script with RESP3 map**: When `protocol_version` is RESP3, a Lua table with string keys
   (not numeric sequence) should be returned as `Response::Map` instead of `Response::Null` or
   `Response::Array`. Implementation:
   - Detect hash-like table (non-sequential keys) in `lua_to_response()`
   - When RESP3: iterate all pairs via `t.pairs()` and build `Response::Map`
   - When RESP2: preserve current behavior (return as array or null per Redis compat)
   - Need access to `protocol_version` in `lua_to_response()` (executor already has it via `self`)
2. **Script return recursive object**: Redis RESP3 scripts that return nested maps/arrays.
   The recursive conversion must handle `Map` at any nesting depth. Extend `lua_to_response()`
   to detect map-like tables recursively.

### Group 3: Protocol / Attributes (6 tests)

**Files to modify**:
- `frogdb-server/crates/protocol/src/response.rs` (Attribute variant encoding)
- `frogdb-server/crates/server/src/connection/frame_io.rs` (attribute delivery)
- `frogdb-server/crates/server/src/connection/handlers/client.rs` (CLIENT TRACKING)
- `frogdb-server/crates/server/src/connection/handlers/debug.rs` (DEBUG subcommands)

**Current state**:
- `WireResponse::Attribute(Box<WireResponse>)` exists but is a stub. `to_resp3_frame()` just
  returns the inner value, ignoring the attribute map.
- RESP3 attributes in Redis are metadata prepended before a response. They use the `|` type
  prefix followed by a map of attribute key-value pairs, then the actual data.
- `invalidation_to_response()` in `frame_io.rs` already uses `Response::Push` for key
  invalidation messages (correct for RESP3 push notifications).

**Remaining work**:
1. **Redesign `WireResponse::Attribute`**: Change from `Attribute(Box<WireResponse>)` to
   `Attribute { attrs: Vec<(WireResponse, WireResponse)>, data: Box<WireResponse> }` to carry
   both the attribute map and the inner data.
2. **Encode attributes in `to_resp3_frame()`**: The `redis_protocol` crate's `Resp3BytesFrame`
   variants have an `attributes: Option<...>` field on every frame type. Instead of a separate
   Attribute frame, set the `attributes` field on the inner frame's RESP3 encoding. This matches
   the RESP3 spec where attributes prefix the next datum.
3. **RESP3 attributes test**: Requires CLIENT TRACKING to produce invalidation attributes.
   When a tracked key is modified, RESP3 returns the next command's response prefixed with
   attribute metadata indicating invalidation. This is different from Push (out-of-band) delivery.
4. **RESP3 attributes on RESP2**: Should be a no-op (attributes stripped, inner value returned).
   Current stub already does this.
5. **DEBUG subcommands for big number / bool / verbatim**: Add `DEBUG RESP3` or similar test
   fixtures that return `BigNumber`, `Boolean`, `VerbatimString` responses. These tests need a
   command that produces these types. Options:
   - Add `DEBUG RESP3 BIGNUMBER <value>` / `DEBUG RESP3 BOOLEAN <value>` /
     `DEBUG RESP3 VERBATIM <value>` subcommands
   - Or implement the actual commands that produce these types (OBJECT ENCODING, etc.)

### Group 4: Pub/Sub Push Messages (5 tests)

**Files to modify**:
- `frogdb-server/crates/core/src/pubsub.rs` (`to_response_with_protocol()`)
- `frogdb-server/crates/server/src/connection/dispatch.rs` (PING in pubsub mode)
- `frogdb-server/crates/server/src/connection.rs` (pubsub message delivery loop)

**Current state**:
- `PubSubMessage::to_response_with_protocol()` already emits `Response::Push(items)` for RESP3
  and `Response::Array(items)` for RESP2.
- PING in pubsub RESP3 mode already returns `Response::pong()` (SimpleString) or
  `Response::bulk(message)` - correct per Redis behavior.
- Connection loop at `connection.rs:605` already calls `to_response_with_protocol(self.state.protocol_version)`.
- Existing regression tests (`pubsub_regression.rs`) already pass for PING in RESP3.

**Remaining work**:
1. **Pub/Sub PING on RESP$resp**: May already pass. The test verifies PING format varies by
   protocol version. Existing code handles this.
2. **PubSub messages with CLIENT REPLY OFF**: Verify messages are still delivered when
   CLIENT REPLY OFF is set (push messages bypass reply suppression in RESP3). May need to
   check that the Push delivery path in `connection.rs` ignores reply mode.
3. **publish to self inside multi**: PUBLISH inside MULTI queues; the message delivery to the
   same connection must use Push type in RESP3. Verify transaction execution path sends push
   notifications correctly.
4. **publish to self inside script**: Same as above but within EVAL. Messages published during
   script execution are delivered after the script returns.
5. **unsubscribe inside multi, and publish to self**: Combination test. UNSUBSCRIBE in MULTI +
   PUBLISH in same MULTI. The unsubscribe confirmation and message delivery must both use
   correct RESP3 format.

---

## Integration Points

| Handler Location | Current Response | RESP3 Needed |
|---|---|---|
| `core/src/shard/blocking.rs:450-453` (BZPopMin) | `Array[key, member, BulkString(score)]` | `Array[key, member, Double(score)]` |
| `core/src/shard/blocking.rs:474-477` (BZPopMax) | `Array[key, member, BulkString(score)]` | `Array[key, member, Double(score)]` |
| `core/src/shard/blocking.rs:496-498` (BZMPop elements) | `Array[member, BulkString(score)]` | `Array[member, Double(score)]` |
| `core/src/scripting/executor.rs:320-353` (lua_to_response) | Always Array for tables | Map for hash-like tables in RESP3 |
| `protocol/src/response.rs:108-109` (Attribute) | Stub, returns inner | Proper attribute map + data |
| `server/src/connection/dispatch.rs:412-430` (PING pubsub) | Already correct | Already correct |
| `core/src/pubsub.rs:149-153` (message delivery) | Already correct | Already correct |

---

## FrogDB Adaptations

None expected. This is a pure protocol-layer concern. All changes are in response formatting
and serialization, not in data model or storage semantics.

---

## Tests

### zset_tcl.rs (9 tests)

- `ZINTER RESP3 - $encoding`
- `Basic $popmin/$popmax - $encoding RESP3`
- `$popmin/$popmax with count - $encoding RESP3`
- `$popmin/$popmax - $encoding RESP3`
- `BZPOPMIN/BZPOPMAX readraw in RESP$resp`
- `ZMPOP readraw in RESP$resp`
- `BZMPOP readraw in RESP$resp`
- `ZRANGESTORE RESP3`
- `ZRANDMEMBER with RESP3`

### scripting_tcl.rs (2 tests)

- `Script with RESP3 map`
- `Script return recursive object`

### protocol_tcl.rs (6 tests)

- `RESP3 attributes`
- `RESP3 attributes readraw`
- `RESP3 attributes on RESP2`
- `test big number parsing` — also needs DEBUG
- `test bool parsing` — also needs DEBUG
- `test verbatim str parsing` — also needs DEBUG

### pubsub_tcl.rs (5 tests)

- `Pub/Sub PING on RESP$resp`
- `PubSub messages with CLIENT REPLY OFF`
- `publish to self inside multi`
- `publish to self inside script`
- `unsubscribe inside multi, and publish to self`

---

## Verification

### Testing strategy

1. **Unit tests**: Extend existing tests in `protocol/src/response.rs` for Attribute encoding
   with proper attribute map + data structure.
2. **Integration tests**: Extend `frogdb-server/crates/server/tests/resp3.rs` with tests for
   each command group (use `connect_resp3()` + `HELLO 3` pattern from existing tests).
3. **Regression tests**: Port exclusions from `zset_tcl.rs`, `scripting_tcl.rs`,
   `protocol_tcl.rs`, `pubsub_tcl.rs` to passing tests.

### How to test

```bash
# Run existing RESP3 integration tests
just test frogdb-server test_hello

# Run zset regression tests (RESP3 variants)
just test frogdb-redis-regression zpopmin_resp3
just test frogdb-redis-regression ping_in_resp3

# Run scripting tests
just test frogdb-redis-regression scripting

# Full regression suite
just test frogdb-redis-regression
```

### Client requirements

- Tests use `server.connect_resp3()` from `frogdb-test-harness` which provides a raw RESP3
  frame client (reads/writes `Resp3Frame` directly).
- No external RESP3 client library needed; the test harness handles protocol switching via
  `HELLO 3`.
- For readraw tests, validate the raw byte encoding matches expected RESP3 wire format.

### Implementation order

1. Blocking zset RESP3 (add `protocol_version` to `WaitEntry`, format scores as Double)
2. Scripting RESP3 map (extend `lua_to_response` for hash-like tables)
3. RESP3 attributes (redesign `Attribute` variant, integrate with CLIENT TRACKING)
4. DEBUG RESP3 test fixtures (BigNumber, Boolean, VerbatimString producers)
5. Pub/Sub edge cases (CLIENT REPLY OFF, MULTI, script interactions)
