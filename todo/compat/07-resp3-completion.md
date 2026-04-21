# 7. RESP3 Protocol Completion (22 tests — mixed files)

**Status**: RESP3 infrastructure exists (HELLO, protocol negotiation); some commands don't produce
correct RESP3 responses
**Scope**: Ensure all commands produce correct RESP3 map/set/double/null responses when negotiated.

## Work

- Audit ZINTER, ZPOPMIN/MAX, ZMPOP, BZMPOP, ZRANGESTORE, ZRANDMEMBER for RESP3 output
- Audit HRANDFIELD for RESP3 output
- Implement RESP3 attributes (key tracking invalidation messages)
- Implement RESP3 pub/sub push messages
- Implement RESP3 map returns from Lua scripts
- Ensure PING works correctly in pub/sub RESP3 mode

**Key files to modify**: Response serialization layer, individual command handlers, protocol crate

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
