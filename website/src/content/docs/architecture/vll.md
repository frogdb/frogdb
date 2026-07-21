---
title: "VLL (Very Lightweight Locking)"
description: "Design rationale and internals of FrogDB's VLL transaction coordination mechanism."
sidebar:
  order: 11
---
Design rationale and internals of FrogDB's VLL transaction coordination mechanism.

## Overview

VLL (Very Lightweight Locking) coordinates atomic operations that span more than
one internal shard without a global mutex, a two-phase-commit coordinator, or
optimistic retries. Each operation declares the keys it will touch as *intents*
in a per-shard lock table; a global monotonically increasing transaction id
(`txid`) gives every operation a place in a total order; and **Selective
Contention Analysis (SCA)** uses the declared intents to let non-conflicting
operations acquire their locks and execute out of order. Only operations whose
intents actually conflict — an exclusive (write) lock overlapping another lock on
the same key — wait, and then only behind lower-`txid` operations.

- **Intent-based locking** — operations declare their keys up front.
- **Selective Contention Analysis** — the intent table's whole purpose is to make
  contention visible so non-conflicting work skips the queue.
- **Total `txid` order** — a monotonic counter makes the wait-for relation
  acyclic, so the scheme is deadlock-free by construction.
- **Locks only where they conflict** — an operation is blocked solely by
  conflicting intents with a smaller `txid`.

The crate lives at `frogdb-server/crates/vll/`; its module header names SCA
directly.

## Design Goals

1. Allow concurrent execution of operations that do not conflict.
2. Provide atomic multi-key semantics across internal shards.
3. Prevent deadlocks through a deterministic total order rather than lock-ordering
   discipline at each call site.
4. Keep single-shard operations cheap — VLL is only engaged when an operation
   touches more than one shard.

---

## VLL Use Cases

VLL coordinates atomicity for operations that touch more than one internal shard:

| Operation type | Example commands | VLL behavior |
|----------------|------------------|--------------|
| Multi-key commands | MGET, MSET, DEL (multi), COPY | Declare intents on every touched shard |
| Transactions | MULTI/EXEC | Continuation lock on EXEC |
| Lua scripts | EVAL, EVALSHA | Continuation lock before execution |
| Multi-key blocking | BLPOP / BRPOP over several keys | Locks during wake evaluation |
| Set operations | SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SMOVE | Lock source and destination shards |
| Sorted-set operations | ZUNIONSTORE, ZINTERSTORE | Lock source and destination shards |
| List operations | LMOVE, BLMOVE, LMPOP | Lock source and destination shards |

**Standalone vs cluster.** In standalone mode with `allow-cross-slot-standalone`
enabled, VLL coordinates cross-shard atomicity for keys that hash to different
slots. In cluster mode, cross-slot operations are rejected with `-CROSSSLOT`;
VLL only ever coordinates shards *within* a single node. `allow-cross-slot-standalone`
defaults to `false` (`frogdb-server/crates/config/src/server.rs`).

---

## How VLL Works

### Lock table

Each shard worker owns a single lock table that records, per key, both which
transactions intend to access it and which of those intents have been granted a
lock. Because a granted lock is a flag on the intent entry, the two views can
never fall out of step, and releasing a transaction is one operation: drop its
intents. The table is owned exclusively by its shard worker (`&mut self`, no
atomics).

The following is illustrative; the authoritative types are `LockTable` and
`Intent` in `frogdb-server/crates/vll/src/lock_table.rs` and `LockMode` in
`frogdb-server/crates/vll/src/types.rs`.

```rust
pub struct LockTable {
    // Per key, intents ordered by txid. The BTreeMap ordering is what
    // gives SCA its "only lower txids can block me" semantics.
    keys: HashMap<Bytes, BTreeMap<u64, Intent>>,
}

struct Intent {
    mode: LockMode,   // Read or Write
    granted: bool,    // multiple Read grants may coexist; a Write grant is exclusive
}

pub enum LockMode {
    Read,   // shared read access
    Write,  // exclusive write access
}
```

Read/Read is the only non-conflicting combination; Read/Write, Write/Read, and
Write/Write all conflict.

### Transaction flow

1. **Acquire a `txid`** from the global monotonic counter.
2. **Declare intents** on every participating shard (via `VllLockRequest`).
3. **Wait for readiness** — each shard replies `ShardReadyResult::Ready` once its
   locks are granted, or `Failed` if they cannot be.
4. **Execute** — the coordinator sends `VllExecute` to each ready shard.
5. **Release** — completing, failing, or aborting a transaction removes its
   intents, which releases any locks it held.

### Deadlock prevention

Deadlock-freedom follows from the total `txid` order, not from a lock-ordering
convention. Within each key's intent map, waiters are ordered by `txid`, and an
operation can be blocked *only* by a conflicting intent with a strictly smaller
`txid`. Because every shard observes the same global `txid` order, the resulting
wait-for relation cannot contain a cycle — there is no combination of
transactions that each wait on the other.

### Selective Contention Analysis

SCA is the mechanism behind "out-of-order execution." When a transaction tries to
acquire its locks, the shard checks each key against only the *lower*-`txid`
intents already declared on that key:

- If no lower-`txid` intent conflicts with the requested mode, the transaction may
  proceed — even past queued lower-`txid` operations that conflict with *other*
  keys but not with this one.
- Grants are all-or-nothing across the transaction's keys, and a second check
  refuses the grant if any *already-granted* intent from another transaction
  conflicts, regardless of `txid` order.

Two operations that touch disjoint keys never interact — each key's intent map is
independent — so they run concurrently. Two readers of the same key proceed
together. A writer waits only for conflicting lower-`txid` work on the keys it
actually touches. When a transaction releases (or aborts before ever being
granted), it disappears from the ordering and whatever it was gating can advance;
each shard re-scans all pending operations after a release, not just the head, so
newly unblocked operations proceed regardless of their queue position. This is
the source of the crate's name: the intent table exists to support SCA.

The per-shard queue that preserves `txid` priority is `TransactionQueue` in
`frogdb-server/crates/vll/src/queue.rs`; the shard state machine that drives
declare → grant → execute lives alongside the lock table.

---

## Atomicity Semantics

VLL provides **execution atomicity** (isolation) but **not failure atomicity**
(rollback):

| Scenario | Behavior | Data state |
|----------|----------|------------|
| Single-shard operation | Runs to completion without interleaving | Consistent |
| Multi-shard success | All shards execute under held locks, globally ordered | Consistent |
| Lock-acquisition timeout | Coordinator aborts every shard still holding locks; client receives an `ERR` | No changes |
| Failure mid-execution | Shards that already received `VllExecute` complete; the rest are aborted | Partial writes persist (no rollback) |
| Coordinator abandons mid-op | Un-executed participants are aborted; executed ones release their own locks | Partial state possible |

**Key point:** once a shard has received `VllExecute`, its write proceeds and
persists; there is no cross-shard undo. Abort is best-effort and only targets
participants that have *not yet* been told to execute. This matches Redis and
DragonflyDB, which likewise do not roll back partially applied multi-key work.

There is no dedicated VLL timeout error code. A lock-acquisition failure surfaces
to the client with the generic prefix as `ERR VLL lock acquisition failed`; the
internal `VllError::QueueFull` / `LockTimeout` values are logged, not sent as
distinct wire codes.

---

## Why VLL vs Traditional Approaches

| Approach | Drawback VLL avoids |
|----------|---------------------|
| Global mutex | Serializes all multi-shard work; contention scales with core count |
| Two-phase commit with a separate coordinator | Extra round trips and a blocking prepare phase |
| Optimistic concurrency control | High abort/retry rate under contention |

VLL keeps ordered locking with deadlock-freedom from the `txid` total order, adds
no 2PC prepare phase, keeps each shard single-threaded internally (so
per-shard locking is cheap), and has a simple failure model because it does not
attempt rollback.

---

## VLL Queue Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `scatter-gather-timeout-ms` | 5000 | Overall deadline for a multi-shard operation (VLL locking plus scatter-gather execution) |
| `max-queue-depth` (under `[vll]`) | 10000 | Maximum pending operations per shard before new ones are rejected |

Defaults are defined in `frogdb-server/crates/config/src/server.rs` and
`frogdb-server/crates/config/src/vll.rs`; treat those files as the source of
truth. A validator enforces that the per-shard and lock-acquisition timeouts are
each smaller than `scatter-gather-timeout-ms`.

**Queue overflow.** When a shard's pending queue is full, the lock request fails
with `VllError::QueueFull`; the client receives `ERR VLL lock acquisition failed`.
There is no VLL-specific rejection counter — the outcome is recorded on the
scatter-gather counter (`frogdb_scatter_gather_total{status="error"}`); crossing
the internal depth warning threshold emits a log warning only.

**Why a single timeout?** VLL is deadlock-free by design, so there is no separate
"waiting for locks" deadline to tune. A shard that is slow to grant is a node
health problem, surfaced by the one scatter-gather timeout, not a locking problem.

---

## Continuation Locks (Lua / MULTI)

Some operations cannot declare their keys up front — a Lua script or a MULTI/EXEC
block may touch any key. For these, VLL grants a **continuation lock**: exclusive
access to an entire shard for the duration of the operation. While a continuation
lock is held, the shard is exclusive and SCA is bypassed — a competing lock
request from another connection is refused (`ShardBusy`) rather than interleaved.

The message and guard are illustrative of the real
`frogdb-server/crates/core/src/shard/message.rs` variant and the
`ContinuationGuard` in `frogdb-server/crates/vll/src/coordinator.rs`:

```rust
ShardMessage::VllContinuationLock {
    txid: u64,
    conn_id: u64,
    ready_tx: oneshot::Sender<ShardReadyResult>,
    release_rx: oneshot::Receiver<()>,
}
```

The coordinator acquires the lock on every participating shard, runs the caller's
work, and drops an RAII `ContinuationGuard`; dropping the guard sends the release
signal on `release_rx` for each shard. This lets a script or transaction touch any
key without declaring intents, at the cost of holding the whole shard exclusively.

For the scatter-gather channel plumbing that carries these messages, see
[Request Flows](/architecture/request-flows/) and
[Concurrency Model](/architecture/concurrency/).
