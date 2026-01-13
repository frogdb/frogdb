# FrogDB VLL (Very Lightweight Locking)

This document specifies FrogDB's VLL transaction coordination mechanism, based on DragonflyDB's approach.

## Overview

VLL provides atomic multi-shard operations without traditional mutex-based locking. Key principles:

- **Intent-based locking**: Operations declare keys they'll access
- **Out-of-order execution**: Non-conflicting operations bypass the queue
- **Transaction ID ordering**: Global monotonic counter prevents deadlocks
- **Shard locks for conflicts**: Only operations with key conflicts wait

## Design Goals

1. Allow concurrent execution of non-conflicting operations
2. Provide atomic semantics for multi-key operations
3. Prevent deadlocks via deterministic ordering
4. Minimize latency for single-shard operations

---

## Key Concepts

### Transaction IDs

Every operation receives a transaction ID from a global atomic counter:

```rust
static NEXT_TXID: AtomicU64 = AtomicU64::new(1);

fn next_txid() -> u64 {
    NEXT_TXID.fetch_add(1, Ordering::SeqCst)
}
```

- Lower IDs have priority in conflict resolution
- Used for deterministic ordering across shards
- Multi-key operations share a single txid across all target shards

### Intent Locks

Operations declare their key access pattern before execution:

- **Read intent**: Operation will read the key
- **Write intent**: Operation will modify the key

Non-conflicting operations (different keys, or read-read on same key) proceed without waiting.

### Out-of-Order Execution

Operations that don't conflict with pending operations can execute immediately:

> "A transaction that can execute before it reaches the front of the queue is known as an out-of-order transaction."
> — [DragonflyDB Transactions](https://www.dragonflydb.io/blog/transactions-in-dragonfly)

This allows high throughput even with long-running operations in the queue.

### Shard Locks

For operations with conflicts, shard-level locks provide atomicity:

- Acquired only when operations conflict (same key with at least one write)
- Used for MULTI/EXEC, Lua scripts, dependent operations (like SINTERSTORE)
- Lower txid acquires lock first (deterministic ordering prevents deadlocks)

---

## Operation Types

### Single-Key Operations

Single-key operations (GET, SET, INCR, etc.) route directly to the owning shard:

```
Client → Parse → Route to owner shard → Execute → Response
```

No VLL coordination needed unless there's a conflict with a pending multi-key operation.

### Multi-Key Operations (Same Shard)

Operations like MSET with hash-tagged keys that all route to the same shard:

```
Client → Parse → Route to owner shard → Execute atomically → Response
```

Atomicity is inherent since all keys are on one shard.

### Multi-Key Operations (Cross-Shard)

Operations spanning multiple shards (requires `allow_cross_slot_standalone = true`):

1. **Request Phase**: Coordinator assigns txid, sends intent to all target shards
2. **Lock Phase**: Each shard queues the operation by txid order
3. **Execute Phase**: When all shards are ready, execute in parallel
4. **Response Phase**: Aggregate results, respond to client

```
┌─────────────────────────────────────────────────────────────────┐
│                   Multi-Shard VLL Flow                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Coordinator assigns txid = 42                                │
│                                                                  │
│  2. Send intent to shards:                                       │
│     Shard A: "txid 42 wants to write key1"                      │
│     Shard B: "txid 42 wants to write key2"                      │
│                                                                  │
│  3. Shards queue by txid order (lower = higher priority)        │
│                                                                  │
│  4. When txid 42 reaches front on all shards → execute          │
│                                                                  │
│  5. Aggregate responses → return to client                      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Conflict Detection

### Key-Level Conflicts

| Operation A | Operation B | Conflict? |
|-------------|-------------|-----------|
| Read        | Read        | No        |
| Read        | Write       | Yes       |
| Write       | Read        | Yes       |
| Write       | Write       | Yes       |

### Resolution

When conflicts occur:
1. Operations are ordered by txid
2. Lower txid executes first
3. Higher txid waits in queue
4. No deadlocks possible (total ordering)

---

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `vll_max_queue_depth` | 10000 | Max pending operations per shard |
| `scatter_gather_timeout_ms` | 5000 | Timeout for multi-shard operations |

When `vll_max_queue_depth` is exceeded, new operations receive `-BUSY` error.

---

## Comparison with Alternatives

| Aspect | Traditional 2PL | VLL |
|--------|-----------------|-----|
| Lock granularity | Per-key locks | Intent-based |
| Deadlock handling | Detection/timeout | Prevention (ordering) |
| Non-conflicting ops | May wait for locks | Execute immediately |
| Implementation complexity | Moderate | Lower |

---

## Implementation Details

*To be specified during implementation of Phase 4 (Multi-Shard Operations).*

---

## References

- [DragonflyDB Transactions Blog](https://www.dragonflydb.io/blog/transactions-in-dragonfly)
- [VLL Paper](https://www.vldb.org/pvldb/vol6/p901-ren.pdf) - "VLL: a lock manager redesign for main memory database systems"
- [CONCURRENCY.md](CONCURRENCY.md) - Thread model and shard architecture
- [TRANSACTIONS.md](TRANSACTIONS.md) - MULTI/EXEC transaction semantics
