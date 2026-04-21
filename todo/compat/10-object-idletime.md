# 10. OBJECT IDLETIME & Access Tracking (6 tests — `introspection2_tcl.rs` + `maxmemory_tcl.rs`)

**Status**: OBJECT IDLETIME command exists; unclear if LRU clock updates correctly, no-touch mode
may need verification
**Scope**: Verify and fix access time tracking, implement no-touch mode from scripts.

## Work

- Verify TTL/TYPE/EXISTS don't update access time
- Verify TOUCH command updates access time
- Implement/verify `CLIENT NO-TOUCH ON` suppresses access time updates
- Verify TOUCH from within scripts still updates access time
- Verify `OBJECT FREQ` returns LFU frequency

**Key files to modify**: `core/src/shard/` (key access paths), `connection/handlers/client.rs`

## Tests

### introspection2_tcl.rs (5 tests)

- `TTL, TYPE and EXISTS do not alter the last access time of a key`
- `TOUCH alters the last access time of a key`
- `Operations in no-touch mode do not alter the last access time of a key`
- `Operations in no-touch mode TOUCH alters the last access time of a key`
- `Operations in no-touch mode TOUCH from script alters the last access time of a key`

### maxmemory_tcl.rs (1 test)

- `lru/lfu value of the key just added` — implement `OBJECT IDLETIME`/`FREQ` tracking
