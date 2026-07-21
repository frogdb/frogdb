---
title: "Blocking Commands"
description: "How FrogDB implements blocking commands (BLPOP, BLMOVE, BZPOPMIN, XREAD, …) in a shared-nothing architecture."
sidebar:
  order: 10
---
How FrogDB implements blocking commands (BLPOP, BLMOVE, BZPOPMIN, XREAD, …) in a shared-nothing architecture.

## Industry Comparison

The relevant design tension is that Redis blocks in a single-threaded event loop,
while FrogDB, like DragonflyDB, is multi-threaded and shared-nothing: a key is
owned by exactly one shard, so a waiter can only be registered and woken on the
shard that owns its keys.

| Aspect | FrogDB |
|--------|--------|
| Threading | Multi-threaded, shared-nothing |
| Wait state | Per-shard wait queue owned by the shard worker |
| Multi-key ordering | First key with data in the command's key order |
| Multi-shard blocking | Rejected with `-CROSSSLOT` (colocate with hash tags) |
| Fairness | FIFO per key, per operation kind |

FrogDB follows Redis command semantics on a per-shard architecture: a blocking
command first attempts its operation, and only if no data is available does it
register a waiter on the owning shard. (Redis and DragonflyDB behavior is
version-dependent and documented by those projects; the table above states
FrogDB's own behavior, which is what the rest of this page describes.)

---

## Architecture

### The blocking operation set

The operations that can block are enumerated by `BlockingOp` in
`frogdb-server/crates/types/src/types/mod.rs`. It is broader than the list
family — it also covers sorted-set pops and **stream blocking**:

```rust
pub enum BlockingOp {
    BLPop,
    BRPop,
    BLMove { dest: Bytes, src_dir: Direction, dest_dir: Direction }, // BLMOVE / BRPOPLPUSH
    BLMPop { direction: Direction, count: usize },
    BZPopMin,
    BZPopMax,
    BZMPop { min: bool, count: usize },
    XRead { after_ids: Vec<StreamId>, count: Option<usize> },
    XReadGroup { group: Bytes, consumer: Bytes, noack: bool, count: Option<usize> },
}
```

### Per-shard wait queue

Each shard worker owns a `ShardWaitQueue`
(`frogdb-server/crates/core/src/shard/wait_queue.rs`). Waiters are stored in a slab
and indexed per key, in registration order, so wakes are FIFO:

```rust
// illustrative — see wait_queue.rs for the full type
pub struct WaitEntry {
    pub conn_id: u64,
    pub keys: Vec<Bytes>,
    pub op: BlockingOp,
    pub response_tx: oneshot::Sender<Response>,
    pub deadline: Option<Instant>,
    pub protocol_version: ProtocolVersion,
}

#[derive(Default)]
pub struct ShardWaitQueue {
    waiters_by_key: HashMap<Bytes, VecDeque<usize>>, // per-key FIFO of slot indices
    entries: Vec<Option<WaitEntry>>,                 // slab of wait entries
    // free-slot list, per-connection index, and per-key/global caps elided
}
```

Because the queue lives on the shard, there is no cross-thread lock on the wait
state; the shard that owns a key is the only one that ever enqueues or wakes its
waiters. FIFO order is preserved *within an operation kind*: a wake for a list
push serves the oldest list waiter and leaves stream waiters on the same key
untouched.

---

## Registration and Notification Flow

Blocking is an *attempt-then-register* handoff, not an up-front block:

1. The command handler runs the operation immediately. If data is available it
   returns the result and never blocks.
2. If no data is available, the handler returns
   `Response::BlockingNeeded { keys, timeout, op }` — a control signal, not a wire
   response (defined in `frogdb-server/crates/protocol/src/response.rs`, not as a
   shard message).
3. The connection layer intercepts that signal and sends
   `ShardMessage::BlockWait { conn_id, keys, op, response_tx, deadline, protocol_version }`
   to the owning shard, which builds a `WaitEntry` and registers it.
4. A `BlockingWaitCoordinator` on the connection side awaits the response, racing
   it against the command's deadline and `CLIENT UNBLOCK`. On any outcome it sends
   `ShardMessage::UnregisterWait { conn_id }` to clean up.

Wake-on-write is driven from the writing command. When, for example, an `LPUSH`
adds data, the shard runs its satisfaction driver for that key: while waiters
exist, it re-validates the key, pops the oldest matching waiter, re-checks that
waiter's deadline and receiver (closing a lost-wakeup race), and hands the value
over. `BLMOVE`/`BRPOPLPUSH` cascade to the destination key with a bounded fan-out
depth.

The message names above are the real `ShardMessage` variants in
`frogdb-server/crates/core/src/shard/message.rs`; the registration and wake logic
live in `frogdb-server/crates/core/src/shard/blocking.rs` and
`frogdb-server/crates/core/src/shard/dispatch_blocking.rs`. For the end-to-end
sequence including the connection task, see
[Request Flows](/architecture/request-flows/).

---

## Cross-Shard Blocking

Multi-key blocking commands require all keys to hash to the same slot (their
command spec sets `requires_same_slot`). If the keys span slots, routing rejects
the command before it reaches a shard:

```
-CROSSSLOT Keys in request don't hash to the same slot
```

Blocking across independently owned shards would require distributed wait
coordination, which defeats the shared-nothing model. The intended escape hatch is
[hash tags](/architecture/storage/): `BLPOP {queue}:high {queue}:low 0` forces both
keys into one slot so the wait can live on a single shard.

---

## Connection State Integration

A connection with a registered waiter is in the BLOCKED state: `blocked` is `Some`
and it issues no further commands until the wait resolves. The wait ends when:

- the key receives data and the waiter is served,
- the command's own timeout deadline elapses,
- another connection issues `CLIENT UNBLOCK` for this connection, or
- the connection disconnects (the coordinator unregisters the waiter).

There is no server-side idle timeout that closes a blocked connection — see
[Connection Management](/architecture/connection/). The only deadline is the one
carried by the blocking command itself.

---

## Blocking inside MULTI/EXEC and Lua

Blocking commands are **not rejected** inside transactions or scripts — they
execute in non-blocking mode, matching Redis:

| Context | Behavior |
|---------|----------|
| `BLPOP` inside MULTI/EXEC | Runs immediately; if no data, returns nil (never blocks) |
| `redis.call('BLPOP', …)` in Lua | Runs immediately; if no data, returns nil/false |

The rationale is exactly why they must not block: a transaction or script holds
the shard's execution path (a MULTI/EXEC or Lua block runs under a
[VLL continuation lock](/architecture/vll/)), so blocking there would stall the
shard rather than wait politely. Redis resolves this the same way — a `BLPOP` in a
`MULTI` degrades to a non-blocking pop. FrogDB converts the internal
`BlockingNeeded` signal to nil in both contexts.

---

## Edge Cases

**Multiple clients on the same key.** FIFO within an operation kind: the oldest
matching waiter is served first, and each push serves one waiter.

**Type change while blocked.** For streams, if the key is overwritten with a
non-stream type, blocked `XREADGROUP` waiters are drained with
`WRONGTYPE Operation against a key holding the wrong kind of value`; a `BLMOVE`
whose destination is the wrong type is rejected without consuming the source; and
the immediate attempt on a wrong-type existing key returns `-WRONGTYPE` directly.

**Key deleted while blocked.** For lists and sorted sets, a deleted or emptied key
leaves the waiter blocked — it will be woken by a future write or time out; no
error is sent. For streams, blocked `XREADGROUP` waiters on a deleted key are
drained with `NOGROUP No such consumer group '<group>' for key name '<key>'`,
while a plain `XREAD` waiter stays blocked (matching Redis).

---

## Cluster Mode: Slot Migration Interaction

When a slot migrates to another node, clients blocked on keys in that slot are
unblocked with a `-MOVED` redirect rather than being left to wait out their
timeout. The path is wired end-to-end: the Raft state machine emits a
slot-migration-complete event, a dispatcher fans it out to the owning shard as
`ShardMessage::SlotMigrated { slot, target_addr }`, and the shard drains every
waiter for keys in that slot, sending each `MOVED <slot> <host>:<port>` (IPv6 hosts
bracketed). A dedicated counter records these migration-driven unblocks.

This is stated as FrogDB's own behavior; it lets a blocked client follow the slot
to its new owner immediately instead of timing out and retrying.
