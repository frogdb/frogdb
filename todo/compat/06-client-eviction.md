# 6. Client Eviction (`maxmemory-clients`) (22 tests — `client_eviction_tcl.rs` + `maxmemory_tcl.rs`)

**Status**: Not implemented (maxmemory for keys exists, not for clients)
**Scope**: Implement per-client memory tracking and eviction when aggregate client memory exceeds limit.

## Work

- Add `maxmemory-clients` config (absolute bytes or percentage of maxmemory)
- Track per-client memory: argv buffer, query buffer, multi buffer, output buffer, watched keys, pub/sub subscriptions, tracking prefixes
- Implement eviction loop (evict largest clients first, until below limit)
- Support `CLIENT NO-EVICT` flag (already partially exists)
- Handle edge cases: eviction during output buffer growth, interaction with output buffer limits

**Key files to modify**: `core/src/client_registry/`, config module, connection handler

## Tests

### client_eviction_tcl.rs (15 tests)

- `client evicted due to large argv`
- `client evicted due to large query buf`
- `client evicted due to large multi buf`
- `client evicted due to percentage of maxmemory`
- `client evicted due to watched key list`
- `client evicted due to pubsub subscriptions`
- `client evicted due to tracking redirection`
- `client evicted due to client tracking prefixes`
- `client evicted due to output buf`
- `client no-evict $no_evict`
- `avoid client eviction when client is freed by output buffer limit`
- `decrease maxmemory-clients causes client eviction`
- `evict clients only until below limit`
- `evict clients in right order (large to small)`
- `client total memory grows during $type`

### maxmemory_tcl.rs (7 tests)

- `eviction due to output buffers of many MGET clients, client eviction: false`
- `eviction due to output buffers of many MGET clients, client eviction: true`
- `eviction due to input buffer of a dead client, client eviction: false`
- `eviction due to input buffer of a dead client, client eviction: true`
- `eviction due to output buffers of pubsub, client eviction: false`
- `eviction due to output buffers of pubsub, client eviction: true`
- `client tracking don't cause eviction feedback loop` — also requires RESP3 (`HELLO 3`)
