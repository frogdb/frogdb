# 12. Lazyfree & Async Deletion (7 tests — `lazyfree_tcl.rs`)

**Status**: Partial (UNLINK exists); missing counters and blocking FLUSHALL ASYNC behavior
**Scope**: Implement `lazyfreed_objects`/`lazyfree_pending_objects` counters and blocking FLUSHALL ASYNC.

## Work

- Add `lazyfreed_objects` counter (total objects freed asynchronously)
- Add `lazyfree_pending_objects` gauge (objects awaiting async free)
- Implement blocking FLUSHALL ASYNC (blocks client until complete, optimized path)
- Handle FLUSHALL SYNC inside MULTI (must NOT optimize to async)
- Handle client disconnect during blocking FLUSHALL
- Implement CONFIG RESETSTAT to clear counters

**Key files to modify**: `core/src/shard/`, INFO stats section, FLUSHALL handler

## Tests

- `lazy free a stream with all types of metadata`
- `lazy free a stream with deleted cgroup`
- `FLUSHALL SYNC optimized to run in bg as blocking FLUSHALL ASYNC`
- `Run consecutive blocking FLUSHALL ASYNC successfully`
- `FLUSHALL SYNC in MULTI not optimized to run as blocking FLUSHALL ASYNC`
- `Client closed in the middle of blocking FLUSHALL ASYNC`
- `Pending commands in querybuf processed once unblocking FLUSHALL ASYNC`
