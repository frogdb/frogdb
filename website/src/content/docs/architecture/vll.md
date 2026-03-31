---
title: "VLL (Very Lightweight Locking)"
description: "Design rationale and internals of FrogDB's VLL transaction coordination mechanism."
sidebar:
  order: 11
---
Design rationale and internals of FrogDB's VLL transaction coordination mechanism.

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

## VLL Use Cases

VLL coordinates atomicity for ALL operations touching multiple internal shards:

| Operation Type | Example Commands | VLL Behavior |
|----------------|------------------|--------------|
| Multi-key commands | MGET, MSET, DEL (multi), RENAME, COPY | Lock all touched shards |
| Transactions | MULTI/EXEC | Lock shards on EXEC |
| Lua scripts | EVAL, EVALSHA | Lock shards before execution |
| Blocking commands | BLPOP (multi-key), BRPOP (multi-key) | Lock during wake evaluation |
| Set operations | SINTER, SUNION, SDIFF, SMOVE | Lock all source shards |
| Sorted set ops | ZUNIONSTORE, ZINTERSTORE, ZDIFF | Lock source and dest shards |
| List operations | LMOVE, BLMOVE, LMPOP | Lock source and dest shards |

**Single-node vs Cluster behavior:**
- In standalone mode with `allow-cross-slot-standalone = true`: VLL coordinates cross-shard atomicity
- In cluster mode: Cross-slot operations return `-CROSSSLOT` error (VLL only applies within a node)

---

## How VLL Works

### Intent Table

Each shard maintains an intent table tracking which keys have pending operations:

```rust
pub struct IntentTable {
    /// key -> list of pending intents
    intents: HashMap<Bytes, Vec<Intent>>,
}

pub struct Intent {
    txid: u64,
    mode: LockMode,
    ready_tx: oneshot::Sender<ShardReadyResult>,
}

pub enum LockMode {
    Shared,     // Read-only access (multiple concurrent OK)
    Exclusive,  // Write access (exclusive)
}
```

### Transaction Flow

1. **Acquire txid** from global atomic counter
2. **Register intents** on all participating shards (sorted order to prevent deadlocks)
3. **Wait for all shards** to signal ready
4. **Execute** on all shards
5. **Release intents** on all shards

### Deadlock Prevention

Lock acquisition in sorted shard order prevents circular waits. The global txid counter ensures a total ordering -- operations with lower txid execute before higher ones.

### Out-of-Order Execution

Non-conflicting operations can bypass the queue. If operation A holds a Shared lock on key X, and operation B wants a Shared lock on key X, B can proceed immediately. Only Exclusive locks block.

---

## Atomicity Semantics

VLL provides **execution atomicity** (isolation) through ordered multi-shard locking:

1. **Lock Phase:** Acquire write locks on all target shards (sorted order prevents deadlock)
2. **Execute Phase:** Execute writes on each shard while holding all locks
3. **Release Phase:** Release all locks

| Scenario | Behavior | Data State |
|----------|----------|------------|
| **Single-shard operation** | True atomicity (all-or-nothing) | Consistent |
| **Multi-shard success** | All shards execute with isolation | Consistent, globally ordered |
| **Lock acquisition timeout** | Client receives `-TIMEOUT` | **No changes** |
| **Failure during execution** | Error returned; partial writes persist | **Partial state** (no rollback) |
| **Coordinator crash mid-op** | Locks timeout and release | **Partial state possible** |

**Key Insight:** VLL provides **execution atomicity** (no interleaving) but **not failure atomicity** (no rollback). This matches Redis and DragonflyDB behavior.

---

## Why VLL vs Traditional Approaches

Traditional approaches and their drawbacks:
- **Global Mutexes**: Contention at scale, not suitable for shared-nothing
- **2PC with separate coordinator**: Expensive overhead, blocking protocol
- **Optimistic CC**: High abort rate under contention

VLL advantages:
- Ordered locking with deadlock prevention (no 2PC overhead)
- Minimal lock contention (each shard is single-threaded internally)
- Simple failure model (no complex rollback logic)
- Deterministic ordering via txid counter

---

## VLL Queue Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `scatter-gather-timeout-ms` | 5000 | Timeout for entire multi-shard operation (VLL + scatter-gather) |
| `vll-max-queue-depth` | 10000 | Max pending operations per shard before rejecting new ones |

**Queue Overflow:** When `vll-max-queue-depth` is reached, new operations receive `-ERR shard queue full, try again later`. Metric: `frogdb_vll_queue_rejections_total`.

**Why a Single Timeout?** VLL is guaranteed deadlock-free by design (ordered transaction IDs). A separate "VLL queue timeout" would add complexity without benefit. A slow shard is a node health issue, not a locking issue.

---

## Continuation Locks (Lua/MULTI)

For operations that need full shard access (Lua scripts, MULTI/EXEC with unknown key access patterns), VLL supports continuation locks that provide exclusive access to an entire shard:

```rust
ShardMessage::VllContinuationLock {
    txid: u64,
    ready_tx: oneshot::Sender<ShardReadyResult>,
    execute_rx: oneshot::Receiver<ExecuteSignal>,
}
```

This blocks all other operations on the shard until the continuation completes, but allows the script/transaction to access any key without declaring intents upfront.
