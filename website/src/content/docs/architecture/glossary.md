---
title: "Glossary"
description: "Terminology, wire error codes, and abbreviations used throughout FrogDB documentation."
sidebar:
  order: 16
---
Terminology, wire error codes, and abbreviations used throughout FrogDB
documentation. Each term links to the page that explains it in depth; the error
codes are quoted from source.

---

## Architecture terms

### Internal Shard
A thread-local partition within a single FrogDB node. Each internal shard owns its
own key-value store, memory budget, expiry index, and WAL writer, and is driven by
one worker task.

Keys map to internal shards through the cluster slot, not a separate hash:
`internal_shard = CRC16(key) % 16384 % num_shards`. The CRC16 slot is computed
once and reused, so a key's slot and its internal shard are derived from the same
value.

See [Concurrency Model](/architecture/concurrency/),
[Storage Engine](/architecture/storage/).

### Hash Slot
A logical partition (0–16383) used to distribute keys across nodes in cluster mode,
compatible with Redis Cluster. `slot = CRC16(key) % 16384`.

**Relationship:** `key → slot(key) → node → internal_shard(key)`.

See [Clustering](/architecture/clustering/).

### Slot vs Shard: quick reference

| Term | Scope | Derivation | Range | Answers |
|------|-------|-----------|-------|---------|
| **Hash Slot** | Cluster (multi-node) | `CRC16(key) % 16384` | 0–16383 | Which node owns a key |
| **Internal Shard** | Node (multi-thread) | `slot % num_shards` | 0 to num_shards−1 | Which worker processes a key |

The `-CROSSSLOT` error uses "slot" terminology for Redis compatibility even when
the underlying constraint is at the internal-shard level.

### Hash Tag
A `{tag}` substring in a key name that controls slot assignment: only the content
between the first `{` and the next `}` is hashed. `{user:123}:profile` and
`{user:123}:sessions` therefore share a slot, enabling multi-key operations on
related keys.

See [Storage Engine](/architecture/storage/).

### VLL (Very Lightweight Locking)
FrogDB's mechanism for coordinating atomic operations across internal shards
without a global lock, using declared key intents, a total transaction-id order,
and Selective Contention Analysis.

See [VLL](/architecture/vll/).

### Raft
The consensus protocol (via the `openraft` library) that FrogDB uses for the
cluster control plane — slot ownership, membership, and epoch changes are agreed
through Raft, not a gossip protocol. Data replication itself uses PSYNC, not Raft.

See [Clustering](/architecture/clustering/).

---

## Persistence terms

### WAL (Write-Ahead Log)
A sequential log of write operations backed by RocksDB, providing durability and
crash recovery. Durability is set by `DurabilityMode`:

- `async` — the OS flushes on its own schedule
- `periodic` — fsync on a fixed interval (default 1000 ms)
- `sync` — fsync before acknowledging each write

See [Persistence & Durability](/architecture/persistence/).

### Snapshot
A point-in-time capture of all key-value data. FrogDB produces snapshots without
forking the process (forkless), coordinated by epoch rather than by an OS
copy-on-write fork.

See [Persistence & Durability](/architecture/persistence/).

### Epoch
A logical marker used to coordinate a snapshot: the epoch is recorded when a
snapshot begins, shards write values as of that epoch, and concurrent writes are
captured by the WAL.

See [Persistence & Durability](/architecture/persistence/).

### COW (Copy-on-Write)
Copying data only when it is modified. FrogDB uses explicit copy-on-write buffers
for forkless snapshots instead of relying on a `fork()` copy-on-write address
space.

See [Storage Engine](/architecture/storage/).

---

## Protocol terms

### RESP (Redis Serialization Protocol)
The wire protocol FrogDB speaks. RESP2 covers simple strings, errors, integers,
bulk strings, and arrays; RESP3 adds maps, sets, booleans, doubles, big numbers,
verbatim strings, and out-of-band push messages.

See [Protocol](/architecture/protocol/).

### CROSSSLOT
The error returned when a multi-key operation references keys in different slots.
Resolve it by colocating the keys with a hash tag.

### Error Code Reference

Wire error strings are quoted from source; the canonical registry is
`CommandError` in `frogdb-server/crates/types/src/error.rs`, with ACL codes in
`frogdb-server/crates/acl/src/error.rs` and scripting codes in
`frogdb-server/crates/core/src/scripting/error.rs`.

| Error | Emitted message | Context |
|-------|-----------------|---------|
| `-CROSSSLOT` | `CROSSSLOT Keys in request don't hash to the same slot` | Multi-key op spanning slots |
| `-MOVED` | `MOVED <slot> <host>:<port>` | Slot owned by another node (redirect) |
| `-ASK` | `ASK <slot> <host>:<port>` | Key being migrated (one-shot redirect) |
| `-WRONGTYPE` | `WRONGTYPE Operation against a key holding the wrong kind of value` | Wrong data type for command |
| `-OOM` | `OOM command not allowed when used memory > 'maxmemory'` | Write rejected under maxmemory |
| `-CLUSTERDOWN` | `CLUSTERDOWN The cluster is down` | Cluster unavailable / slot unserved |
| `-TRYAGAIN` | `TRYAGAIN Multiple keys request during rehashing of slot` | Multi-key op mid-migration |
| `-READONLY` | `READONLY You can't write against a read only replica.` | Write sent to a replica |
| `-NOAUTH` | `NOAUTH Authentication required.` | Command before AUTH |
| `-NOPERM` | `NOPERM this user has no permissions to run the '<command>' command` | ACL denial (command / key / channel variants) |
| `-NOPROTO` | `NOPROTO sorry, this protocol version is not supported` | HELLO with an unsupported RESP version |
| `-BUSYKEY` | `BUSYKEY Target key name already exists` | Destination key exists (e.g. RESTORE) |
| `-BUSYGROUP` | `BUSYGROUP Consumer Group name already exists` | XGROUP CREATE on existing group |
| `-NOGROUP` | `NOGROUP No such consumer group` | Stream group missing |
| `-NOSCRIPT` | `NOSCRIPT No matching script. Please use EVAL.` | EVALSHA on unknown SHA |
| `-BUSY` | `ERR BUSY Lua script running. Allow up to <ms>ms for it to finish.` | A script is still running |
| `-EXECABORT` | `EXECABORT Transaction discarded because of previous errors.` | EXEC after a queued error |
| `-VERSIONMISMATCH` | `VERSIONMISMATCH expected <expected> actual <actual>` | Optimistic version check failed |

**Not emitted by FrogDB.** `MASTERDOWN`, `NOREPLICAS`, and `LOADING` exist only as
passthrough *labels* used when deciding whether to wrap a Lua error — FrogDB never
produces them (there is no RDB-load phase). `MISCONF` and `NOREPL` do not exist at
all. Timeouts are reported with the generic `ERR` prefix (for example
`ERR timeout …`); there is no `-TIMEOUT` code and no VLL lock-timeout wire error.

---

## Eviction terms

### LRU (Least Recently Used)
An eviction policy that removes keys by last-access time. FrogDB approximates LRU
by sampling, as Redis does, rather than maintaining an exact ordering.

See [Storage Engine](/architecture/storage/).

### LFU (Least Frequently Used)
An eviction policy that removes keys by access frequency, using a logarithmic
counter with decay.

See [Storage Engine](/architecture/storage/).

---

## Data-structure terms

### Griddle
A Rust hash-map implementation with incremental resize: the cost of growing the
table is spread across subsequent inserts to avoid a single large rehash latency
spike.

See [Storage Engine](/architecture/storage/).

---

## Transaction terms

### MULTI/EXEC
Redis transaction commands: `MULTI` begins queuing commands and `EXEC` runs the
queued batch. FrogDB provides execution atomicity (no interleaving) but does not
roll back on a mid-batch error; durability follows the configured WAL mode.

See [Consistency Model](/architecture/consistency/).

### WATCH
Optimistic locking: watched keys are recorded with the version observed at WATCH
time, and a subsequent `EXEC` aborts if any watched key changed.

### Pipelining
Sending several commands without waiting for each reply, to amortize round-trip
latency. Pipelining is not transactional — pipelined commands may interleave with
other clients' commands.

---

## Replication terms

### Primary
A node that accepts writes for its slots (also called "master" in Redis
terminology).

See [Replication](/architecture/replication/).

### Replica
A node that applies data streamed from a primary, serves reads, and can be promoted
on failover.

### PSYNC
The partial-synchronization protocol a replica uses to resume from its last offset
after a brief disconnect instead of taking a full resync.

### Replication Backlog
A bounded ring buffer of recent RESP-encoded writes, indexed by replication offset,
that backs PSYNC partial resync (and split-brain reconciliation). It is distinct
from the live broadcast channel that streams writes to currently-connected
replicas.

See [Replication](/architecture/replication/).

### Replication ID
An identifier for a replication history line, used with the offset to decide
whether a reconnecting replica can PSYNC or must fully resync.

---

## Metrics terms

### Throughput
Operations completed per unit time. Used qualitatively in these docs to describe
design trade-offs; FrogDB publishes no throughput figures until reproducible
benchmarks exist.

---

## Abbreviations

| Abbrev | Meaning |
|--------|---------|
| AOF | Append-Only File (Redis persistence format) |
| RDB | Redis Database (snapshot format) |
| TTL | Time To Live (key expiration) |
| OOM | Out Of Memory |
| ACL | Access Control List |
| TLS | Transport Layer Security |
| FIFO | First In, First Out |
| CRC16 | Cyclic Redundancy Check, 16-bit (slot hashing) |
| USDT | User Statically-Defined Tracing (probes) |
