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

### Lock Timeout Behavior

Lock timeouts ensure operations don't wait indefinitely and that locks don't become orphaned. The timeout system operates at multiple levels.

**Timeout Hierarchy:**

```
┌─────────────────────────────────────────────────────────────────┐
│                     Timeout Hierarchy                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  scatter_gather_timeout_ms (5000ms default)                     │
│  └── Total time for entire multi-shard operation                │
│      │                                                           │
│      ├── vll_lock_acquisition_timeout_ms (4000ms default)       │
│      │   └── Time to acquire locks on ALL shards                │
│      │                                                           │
│      └── Remaining: execution + response aggregation             │
│                                                                  │
│  connection_timeout_ms (60000ms default)                        │
│  └── Maximum time for continuation locks (MULTI/Lua)            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Configuration:**

```toml
[vll]
# Maximum time to acquire locks across all participating shards
# Must be less than scatter_gather_timeout_ms to leave time for execution
vll_lock_acquisition_timeout_ms = 4000

# Per-shard lock wait timeout (should be <= lock_acquisition_timeout)
# Operations waiting longer than this on a single shard are aborted
vll_per_shard_lock_timeout_ms = 2000

# Interval for checking and cleaning up timed-out operations
vll_timeout_check_interval_ms = 100
```

**Timeout Calculation for Multi-Shard Operations:**

```rust
fn calculate_operation_budget(config: &VllConfig) -> OperationBudget {
    let total = config.scatter_gather_timeout_ms;
    let lock_budget = config.vll_lock_acquisition_timeout_ms;
    let execution_budget = total - lock_budget;

    OperationBudget {
        total_ms: total,
        lock_acquisition_ms: lock_budget,
        execution_ms: execution_budget,
    }
}
```

**Timeout Behavior by Phase:**

| Phase | Timeout | Behavior on Expiry | Error Response |
|-------|---------|-------------------|----------------|
| Lock acquisition | `vll_lock_acquisition_timeout_ms` | Abort operation, release acquired locks | `-TIMEOUT lock acquisition timed out` |
| Per-shard wait | `vll_per_shard_lock_timeout_ms` | Remove from shard queue, notify coordinator | Internal error to coordinator |
| Total operation | `scatter_gather_timeout_ms` | Abort entire operation | `-TIMEOUT operation timed out` |
| Continuation lock | `connection_timeout_ms` | Release lock on connection close | N/A (connection closed) |

**Lock Acquisition Timeout Flow:**

```
Multi-shard MSET starts (txid=42)
       │
       ▼
Send lock requests to Shard A, B, C
       │
       ├── Shard A: Locks acquired (200ms)
       ├── Shard B: Locks acquired (500ms)
       └── Shard C: Waiting for txid=30... (blocked)
              │
              ▼
       vll_lock_acquisition_timeout_ms expires
              │
              ▼
       Coordinator aborts:
         - Send VllAbort to Shard A (releases locks)
         - Send VllAbort to Shard B (releases locks)
         - Send VllAbort to Shard C (removes from queue)
              │
              ▼
       Return: -TIMEOUT lock acquisition timed out
```

**Why Default is 4000ms (80% of scatter_gather_timeout):**

1. **Most time spent waiting for locks**: Lock acquisition is typically the bottleneck
2. **Leave buffer for execution**: 1000ms is usually sufficient for execution
3. **Fast failure is better**: Detecting timeout early allows client retry
4. **Matches Redis behavior**: Redis MULTI/EXEC has similar timeout characteristics

### Timeout Coordination

To avoid race conditions between independent timeouts, shards report their status explicitly
to the coordinator:

```rust
/// Shard reports lock acquisition result to coordinator
enum ShardLockResult {
    /// Locks acquired successfully
    Acquired { elapsed_ms: u64 },
    /// Timed out waiting for locks
    Timeout { elapsed_ms: u64, waiting_for_txid: Option<u64> },
    /// Rejected (queue full, key doesn't exist, etc.)
    Rejected { reason: String },
}

/// Coordinator tracks shard status
struct CoordinatorState {
    shard_results: HashMap<ShardId, Option<ShardLockResult>>,
    overall_deadline: Instant,
    started_at: Instant,
}

impl CoordinatorState {
    fn handle_shard_result(&mut self, shard: ShardId, result: ShardLockResult) {
        match result {
            ShardLockResult::Acquired { .. } => {
                self.shard_results.insert(shard, Some(result));
                // Check if all shards ready
                if self.all_shards_ready() {
                    self.signal_execute();
                }
            }
            ShardLockResult::Timeout { .. } | ShardLockResult::Rejected { .. } => {
                // Coordinator KNOWS shard failed (explicit notification)
                // No need to wait for overall timeout - abort immediately
                self.abort_all_shards();
            }
        }
    }
}
```

**Timeout Invariants:**

```toml
# ENFORCED AT CONFIG LOAD TIME:
# per_shard < lock_acquisition < scatter_gather
#
# This ensures:
# 1. Shard timeout fires BEFORE coordinator timeout
# 2. Coordinator receives explicit failure notification
# 3. No ambiguous "is it stuck or just slow?" window

vll_per_shard_lock_timeout_ms = 2000      # Fires first
vll_lock_acquisition_timeout_ms = 4000    # Coordinator-level
scatter_gather_timeout_ms = 5000          # Overall operation
```

**Timeout Check Precision:**

```toml
# Reduce from 100ms default to 50ms for tighter timeout accuracy
# Worst-case timeout overshoot: 50ms (acceptable)
# CPU overhead: ~20 checks/second per shard (negligible)
vll_timeout_check_interval_ms = 50
```

**Metrics:**

| Metric | Description |
|--------|-------------|
| `frogdb_vll_lock_timeout_total` | Operations that timed out during lock acquisition |
| `frogdb_vll_lock_wait_ms` | Histogram of time waiting for locks |
| `frogdb_vll_operations_total{result="timeout"}` | Total timeout failures |

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

This section specifies the data structures, protocols, and algorithms for VLL implementation.

### Data Structures

#### Per-Key Lock State

Lock state is stored inline with each entry for cache locality. Uses atomic operations for lock-free acquisition:

```rust
/// Lock state for a single key
///
/// Encoding: 0 = unlocked, 1-254 = reader count, 255 = exclusive write lock
pub struct KeyLockState {
    lock_counter: AtomicU8,
}

impl KeyLockState {
    const EXCLUSIVE: u8 = 255;
    const MAX_READERS: u8 = 254;

    /// Try to acquire read lock. Returns true if successful.
    pub fn try_read_lock(&self) -> bool {
        loop {
            let current = self.lock_counter.load(Ordering::Acquire);
            if current == Self::EXCLUSIVE || current == Self::MAX_READERS {
                return false; // Exclusive held or reader overflow
            }
            if self.lock_counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ).is_ok() {
                return true;
            }
            // CAS failed, retry
        }
    }

    /// Try to acquire exclusive (write) lock. Returns true if successful.
    pub fn try_write_lock(&self) -> bool {
        self.lock_counter.compare_exchange(
            0,
            Self::EXCLUSIVE,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ).is_ok()
    }

    /// Release read lock. Panics if not holding read lock.
    pub fn read_unlock(&self) {
        let prev = self.lock_counter.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0 && prev != Self::EXCLUSIVE);
    }

    /// Release exclusive lock. Panics if not holding exclusive lock.
    pub fn write_unlock(&self) {
        let prev = self.lock_counter.swap(0, Ordering::Release);
        debug_assert_eq!(prev, Self::EXCLUSIVE);
    }

    /// Check if any lock is held (for debugging/metrics).
    pub fn is_locked(&self) -> bool {
        self.lock_counter.load(Ordering::Acquire) != 0
    }
}
```

**Integration with Entry:**

```rust
pub struct Entry {
    pub value: Value,
    pub metadata: KeyMetadata,
    pub lock: KeyLockState,  // VLL per-key lock
}
```

#### Intent Lock Table

Each shard maintains an intent table for Selective Contention Analysis (SCA):

```rust
/// Access mode for a key
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Read,
    Write,
}

/// An intent to access a key at a future point
#[derive(Debug)]
pub struct LockIntent {
    pub txid: u64,
    pub mode: LockMode,
    pub acquired: bool,  // Has this intent been converted to actual lock?
}

/// Intent table tracking declared lock intentions per shard
///
/// Enables out-of-order execution: operations can check if their keys
/// conflict with any pending lower-txid operations.
pub struct IntentTable {
    /// Key -> list of pending intents, sorted by txid
    intents: HashMap<Bytes, Vec<LockIntent>>,
    /// Total intent count for monitoring
    total_intents: usize,
}

impl IntentTable {
    /// Declare intent to access a key. Called when operation is queued.
    pub fn declare(&mut self, key: Bytes, txid: u64, mode: LockMode) {
        let intents = self.intents.entry(key).or_default();
        // Insert in txid order (binary search for position)
        let pos = intents.partition_point(|i| i.txid < txid);
        intents.insert(pos, LockIntent { txid, mode, acquired: false });
        self.total_intents += 1;
    }

    /// Check if operation can proceed without waiting (SCA).
    /// Returns true if no conflicting lower-txid intents exist.
    pub fn can_proceed(&self, key: &[u8], txid: u64, mode: LockMode) -> bool {
        if let Some(intents) = self.intents.get(key) {
            for intent in intents {
                if intent.txid >= txid {
                    break; // No lower-txid intents remain
                }
                if intent.acquired {
                    continue; // Already executing, will complete soon
                }
                // Check conflict matrix
                if intent.mode == LockMode::Write || mode == LockMode::Write {
                    return false; // Conflict with pending lower-txid
                }
                // Read-read: no conflict, continue checking
            }
        }
        true
    }

    /// Remove all intents for a transaction (on completion or abort).
    pub fn remove_txid(&mut self, txid: u64) {
        self.intents.retain(|_, intents| {
            let before = intents.len();
            intents.retain(|i| i.txid != txid);
            self.total_intents -= before - intents.len();
            !intents.is_empty()
        });
    }

    /// Mark intent as acquired (lock granted).
    pub fn mark_acquired(&mut self, key: &[u8], txid: u64) {
        if let Some(intents) = self.intents.get_mut(key) {
            if let Some(intent) = intents.iter_mut().find(|i| i.txid == txid) {
                intent.acquired = true;
            }
        }
    }
}
```

#### Transaction Queue

Per-shard queue ordered by transaction ID:

```rust
/// State of a pending operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingOpState {
    /// Waiting for locks on this shard
    WaitingForLocks,
    /// Locks acquired, waiting for coordinator signal
    LocksAcquired,
    /// Currently executing
    Executing,
}

/// A pending multi-shard operation on this shard
pub struct PendingOp {
    pub txid: u64,
    pub keys: Vec<Bytes>,
    pub modes: Vec<LockMode>,  // Parallel to keys
    pub state: PendingOpState,
    /// Channel to notify coordinator when locks acquired
    pub ready_tx: Option<oneshot::Sender<ShardReadyResult>>,
    /// Channel to receive execute signal from coordinator
    pub execute_rx: Option<oneshot::Receiver<ExecuteSignal>>,
    /// Timestamp for timeout tracking
    pub started_at: Instant,
}

/// Transaction queue per shard
pub struct TransactionQueue {
    /// Pending operations ordered by txid
    pending: BTreeMap<u64, PendingOp>,
    /// Currently executing txid (None if idle)
    executing: Option<u64>,
    /// Max queue depth from config
    max_depth: usize,
}

impl TransactionQueue {
    /// Enqueue a new operation. Returns Err if queue full.
    pub fn enqueue(&mut self, op: PendingOp) -> Result<(), VllError> {
        if self.pending.len() >= self.max_depth {
            return Err(VllError::QueueFull);
        }
        self.pending.insert(op.txid, op);
        Ok(())
    }

    /// Remove completed operation and return it.
    pub fn remove(&mut self, txid: u64) -> Option<PendingOp> {
        if self.executing == Some(txid) {
            self.executing = None;
        }
        self.pending.remove(&txid)
    }

    /// Cleanup expired operations. Returns list of expired txids.
    pub fn cleanup_expired(&mut self, timeout: Duration) -> Vec<u64> {
        let now = Instant::now();
        let mut expired = Vec::new();
        self.pending.retain(|&txid, op| {
            if now.duration_since(op.started_at) > timeout {
                expired.push(txid);
                false
            } else {
                true
            }
        });
        expired
    }
}
```

#### Coordinator State

Tracks multi-shard transaction progress:

```rust
/// Phase of a multi-shard transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxPhase {
    /// Sending lock requests to shards
    Locking,
    /// All shards have locks, executing
    Executing,
    /// Gathering results from shards
    Gathering,
    /// Completed successfully
    Committed,
    /// Failed - locks released, partial state may persist (no rollback)
    Failed,
}

/// Result from shard lock acquisition
pub enum ShardReadyResult {
    Ready,
    Conflict { blocking_txid: u64 },
    Error(VllError),
}

/// Signal to shard to begin or abort execution
pub struct ExecuteSignal {
    pub proceed: bool,  // false = abort
}

/// Coordinator's view of a shard's participation
struct ShardParticipation {
    shard_id: usize,
    keys: Vec<Bytes>,
    modes: Vec<LockMode>,
    ready_rx: oneshot::Receiver<ShardReadyResult>,
    execute_tx: oneshot::Sender<ExecuteSignal>,
}

/// State for an active multi-shard transaction
pub struct CoordinatorState {
    pub txid: u64,
    pub phase: TxPhase,
    pub shards: HashMap<usize, ShardParticipation>,
    pub timeout: Duration,
    pub started_at: Instant,
}
```

---

### Message Types

VLL messages added to the `ShardMessage` enum:

```rust
pub enum ShardMessage {
    // ... existing variants ...

    /// Request shard to acquire locks for a VLL transaction
    VllLockRequest {
        txid: u64,
        keys: Vec<Bytes>,
        modes: Vec<LockMode>,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        execute_rx: oneshot::Receiver<ExecuteSignal>,
    },

    /// Execute operation after locks acquired
    VllExecute {
        txid: u64,
        command: VllCommand,
        result_tx: oneshot::Sender<VllShardResult>,
    },

    /// Abort transaction (release locks, no rollback - partial state persists)
    VllAbort {
        txid: u64,
    },

    /// Request continuation (full shard) lock for Lua/MULTI
    VllContinuationLock {
        txid: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        execute_rx: oneshot::Receiver<ExecuteSignal>,
    },
}

/// Commands executable under VLL coordination
pub enum VllCommand {
    MGet { keys: Vec<Bytes> },
    MSet { pairs: Vec<(Bytes, Bytes)> },
    Del { keys: Vec<Bytes> },
    Exists { keys: Vec<Bytes> },
    SInter { keys: Vec<Bytes> },
    SUnion { keys: Vec<Bytes> },
    SDiff { keys: Vec<Bytes> },
}

/// Result from shard execution
pub struct VllShardResult {
    pub shard_id: usize,
    pub result: Result<PartialVllResult, VllError>,
}

pub enum PartialVllResult {
    Values(Vec<Option<Bytes>>),  // MGET
    Count(usize),                // DEL, EXISTS
    Ok,                          // MSET
    SetMembers(HashSet<Bytes>),  // Set operations
}
```

---

### Lock Acquisition Protocol

#### Coordinator Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    VLL Coordinator Protocol                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. Acquire global txid from NEXT_TXID atomic counter                   │
│                                                                          │
│  2. Partition keys by shard:                                            │
│     shard_keys: BTreeMap<usize, Vec<(Bytes, LockMode)>>                 │
│                                                                          │
│  3. Send VllLockRequest to shards IN SORTED SHARD ORDER:                │
│     for (shard_id, keys) in shard_keys.iter() { ... }                   │
│     ↑ BTreeMap iteration is sorted - prevents deadlocks                 │
│                                                                          │
│  4. Wait for all ShardReadyResult::Ready (with timeout):                │
│     - Any error/timeout → abort all shards, return error                │
│     - All ready → proceed to step 5                                     │
│                                                                          │
│  5. Send ExecuteSignal{proceed: true} to all shards                     │
│                                                                          │
│  6. Gather VllShardResult from all shards                               │
│                                                                          │
│  7. Aggregate results based on command type                             │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Shard-Side Lock Handling

```rust
async fn handle_vll_lock_request(
    &mut self,
    txid: u64,
    keys: Vec<Bytes>,
    modes: Vec<LockMode>,
    ready_tx: oneshot::Sender<ShardReadyResult>,
    execute_rx: oneshot::Receiver<ExecuteSignal>,
) {
    // 1. Declare intents in intent table
    for (key, mode) in keys.iter().zip(modes.iter()) {
        self.intent_table.declare(key.clone(), txid, *mode);
    }

    // 2. Create and enqueue pending operation
    let op = PendingOp {
        txid,
        keys: keys.clone(),
        modes,
        state: PendingOpState::WaitingForLocks,
        ready_tx: Some(ready_tx),
        execute_rx: Some(execute_rx),
        writes: Vec::new(),
        started_at: Instant::now(),
    };

    if let Err(e) = self.tx_queue.enqueue(op) {
        let _ = ready_tx.send(ShardReadyResult::Error(e));
        return;
    }

    // 3. Try to acquire locks (may succeed or queue)
    self.try_acquire_locks_for_txid(txid).await;
}

async fn try_acquire_locks_for_txid(&mut self, txid: u64) {
    let op = match self.tx_queue.pending.get_mut(&txid) {
        Some(op) if op.state == PendingOpState::WaitingForLocks => op,
        _ => return,
    };

    // Check SCA: can we proceed out of order?
    let can_proceed = op.keys.iter().zip(op.modes.iter()).all(|(key, mode)| {
        self.intent_table.can_proceed(key, txid, *mode)
    });

    if !can_proceed {
        return; // Must wait for lower-txid operations
    }

    // Try to acquire actual per-key locks
    let mut acquired = Vec::new();
    for (key, mode) in op.keys.iter().zip(op.modes.iter()) {
        let success = match mode {
            LockMode::Read => self.store.try_read_lock(key),
            LockMode::Write => self.store.try_write_lock(key),
        };

        if success {
            acquired.push((key.clone(), *mode));
            self.intent_table.mark_acquired(key, txid);
        } else {
            // Release acquired locks, stay in queue
            for (k, m) in acquired {
                self.store.release_lock(&k, m);
            }
            return;
        }
    }

    // All locks acquired!
    op.state = PendingOpState::LocksAcquired;
    if let Some(ready_tx) = op.ready_tx.take() {
        let _ = ready_tx.send(ShardReadyResult::Ready);
    }
}
```

---

### Selective Contention Analysis (SCA)

SCA enables out-of-order execution for non-conflicting operations:

```
Transaction Queue (ordered by txid):
┌─────────────────────────────────────────────────────────────────┐
│ txid=10: WRITE key_a    │ txid=15: READ key_b   │ txid=20: WRITE key_c │
│ (executing)             │ (waiting)             │ (waiting)            │
└─────────────────────────────────────────────────────────────────┘

Without SCA: txid=15 and txid=20 must wait for txid=10
With SCA:    txid=15 and txid=20 can execute immediately (no conflict)

SCA Algorithm:
1. For each key in transaction's working set:
   a. Check intent_table.can_proceed(key, txid, mode)
   b. If any key has conflicting lower-txid intent → wait
2. If all keys clear → acquire locks and execute
```

---

### Failure Handling (No Rollback)

When a cross-shard operation fails:

```rust
async fn handle_shard_failure(&mut self, txid: u64) {
    let op = match self.tx_queue.remove(txid) {
        Some(op) => op,
        None => return,
    };

    // Release locks - writes that completed remain in place
    for (key, mode) in op.keys.iter().zip(op.modes.iter()) {
        self.store.release_lock(key, *mode);
    }

    // Remove from intent table
    self.intent_table.remove_txid(txid);

    // Note: No rollback of writes - partial state persists
    // This matches Redis/DragonflyDB behavior
}
```

**Why this is acceptable:**
- Single-shard operations (the common case) are always atomic
- Cross-shard operations are opt-in via `allow_cross_slot_standalone`
- Users choosing cross-shard mode accept the partial failure semantics
- Hash tags provide a zero-overhead way to ensure atomicity for related keys

---

### Continuation Locks

For Lua scripts and MULTI/EXEC, full shard atomicity is required:

```rust
/// Continuation lock blocks entire shard until released
struct ContinuationLock {
    txid: u64,
}

async fn handle_continuation_lock(
    &mut self,
    txid: u64,
    ready_tx: oneshot::Sender<ShardReadyResult>,
    execute_rx: oneshot::Receiver<ExecuteSignal>,
) {
    // Wait for all pending operations to complete
    while !self.tx_queue.pending.is_empty() {
        self.process_next_pending().await;
    }

    // Acquire continuation lock (blocks all new operations)
    self.continuation_lock = Some(ContinuationLock { txid });

    let _ = ready_tx.send(ShardReadyResult::Ready);

    // Wait for coordinator signal
    match execute_rx.await {
        Ok(ExecuteSignal { proceed: true }) => {
            // Execution proceeds with full shard access
        }
        _ => {
            self.continuation_lock = None;
        }
    }
}
```

**Continuation Lock Behavior:**
- While held, shard rejects all non-VLL operations with `-ERR shard busy`
- VLL operations from same txid proceed
- Released after Lua script or MULTI/EXEC completes

### Continuation Lock Cleanup

When a client disconnects while holding a continuation lock:

1. **Detection**: TCP connection close detected by shard's connection handler
2. **Immediate release**: Continuation lock released synchronously on disconnect detection
3. **Transaction abort**: Any in-progress MULTI transaction is discarded (no EXEC)
4. **Blocked operations**: WATCH state cleared, blocked commands (future) unblocked

**Guarantees:**
- Lock held for at most `connection_timeout_ms` after network failure
- Other operations on the shard resume immediately after lock release
- No orphaned locks - cleanup is synchronous with connection teardown

### Orphaned Lock Prevention and Cleanup

Orphaned locks occur when lock-holding operations fail to release their locks. FrogDB employs multiple mechanisms to prevent and clean up orphaned locks.

**Scenarios That Can Create Orphaned Locks:**

| Scenario | Cause | Detection Method |
|----------|-------|------------------|
| Client disconnect during MULTI | Network failure, client crash | Connection close event |
| Lua script timeout | Script exceeds `lua_time_limit_ms` | Script timeout handler |
| Coordinator crash during multi-shard op | Process crash, OOM | Shard-side timeout |
| Shard crash during lock hold | Process crash, panic | Restart recovery |
| VLL queue operation timeout | Deadlock, slow shard | Per-operation timeout |

**Cleanup Mechanisms:**

#### 1. Connection-Based Cleanup (Primary)

```rust
impl Connection {
    async fn on_disconnect(&mut self, shard_tx: &ShardSender) {
        // Release any continuation lock held by this connection
        if let Some(txid) = self.continuation_lock_txid.take() {
            shard_tx.send(ShardMessage::VllAbort { txid }).await;
        }

        // Clear MULTI state
        self.multi_state = None;

        // Clear WATCH state
        self.watched_keys.clear();
    }
}
```

#### 2. Timeout-Based Cleanup (Backup)

Per-shard background task scans for stale locks:

```rust
async fn orphaned_lock_scanner(shard: &mut Shard, config: &VllConfig) {
    let mut interval = tokio::time::interval(
        Duration::from_millis(config.vll_timeout_check_interval_ms)
    );

    loop {
        interval.tick().await;

        // Check continuation locks
        if let Some(ref lock) = shard.continuation_lock {
            if lock.started_at.elapsed() > config.max_continuation_lock_duration() {
                warn!(
                    "Releasing orphaned continuation lock for txid={}",
                    lock.txid
                );
                shard.continuation_lock = None;
                shard.metrics.orphaned_locks_cleaned.inc();
            }
        }

        // Check pending VLL operations
        let expired_txids = shard.tx_queue.cleanup_expired(
            Duration::from_millis(config.vll_lock_acquisition_timeout_ms)
        );

        for txid in expired_txids {
            // Release any per-key locks
            if let Some(op) = shard.tx_queue.pending.get(&txid) {
                for (key, mode) in op.keys.iter().zip(op.modes.iter()) {
                    shard.store.release_lock(key, *mode);
                }
            }
            shard.intent_table.remove_txid(txid);
            shard.metrics.orphaned_locks_cleaned.inc();
        }
    }
}
```

#### 3. Startup Recovery

After crash or restart, all lock state is cleared:

```rust
fn recover_shard(shard: &mut Shard) {
    // Lock state is in-memory only - cleared on restart
    // All entries start unlocked
    for entry in shard.store.entries.values_mut() {
        entry.lock = KeyLockState::new();
    }

    // Clear any transaction queue state
    shard.tx_queue = TransactionQueue::new(shard.config.vll_max_queue_depth);
    shard.intent_table = IntentTable::new();
    shard.continuation_lock = None;

    info!("Shard {} lock state cleared during recovery", shard.id);
}
```

#### 4. Multi-Shard Transaction Recovery (Snapshot + WAL)

When recovering from snapshot + WAL, multi-shard operations require special handling to detect
and skip partial operations.

**Invariant:** Snapshot represents a consistent point-in-time BEFORE any in-flight VLL operation
completes.

**WAL Entry Format for Multi-Shard Operations:**

```rust
struct WalEntry {
    /// Global transaction ID (for grouping multi-shard ops)
    txid: u64,
    /// Total shards involved (0 = single-shard operation)
    shard_count: u8,
    /// This shard's index in the operation (0..shard_count)
    shard_index: u8,
    /// The actual operation
    operation: Operation,
    /// RocksDB sequence number
    sequence: u64,
}
```

**Recovery Algorithm:**

```rust
fn recover_from_wal(
    snapshot_seq: u64,
    wal_entries: impl Iterator<Item = WalEntry>,
) {
    // Track multi-shard operations by txid
    let mut pending_multi_shard: HashMap<u64, MultiShardState> = HashMap::new();

    for entry in wal_entries.filter(|e| e.sequence > snapshot_seq) {
        if entry.shard_count == 0 {
            // Single-shard operation: apply immediately
            apply_entry(&entry);
            continue;
        }

        // Multi-shard operation: collect all shards before applying
        let state = pending_multi_shard
            .entry(entry.txid)
            .or_insert_with(|| MultiShardState::new(entry.shard_count));

        state.add_entry(entry);

        if state.is_complete() {
            // All shards present: apply atomically
            for entry in state.entries() {
                apply_entry(entry);
            }
            pending_multi_shard.remove(&entry.txid);
        }
    }

    // Any remaining entries in pending_multi_shard are partial operations
    for (txid, state) in pending_multi_shard {
        warn!(
            "Skipping partial multi-shard operation txid={}: {}/{} shards present",
            txid, state.present_count(), state.expected_count()
        );
        metrics.partial_ops_skipped.inc();
    }
}
```

**Recovery Semantics:**

| Scenario | WAL State | Recovery Action |
|----------|-----------|-----------------|
| Single-shard op | Complete entry | Apply |
| Multi-shard op, all shards | All `shard_count` entries with same `txid` | Apply all |
| Multi-shard op, partial | Missing some shards | **Skip all** (rollback by omission) |
| Multi-shard op, crash mid-write | Some shards wrote, some didn't | Skip all (partial writes not durable) |

**Example: MSET k1 v1 k2 v2 (k1 on Shard A, k2 on Shard B)**

```
Normal completion:
  WAL entries: [
    {txid=42, shard_count=2, shard_index=0, op=SET k1 v1},
    {txid=42, shard_count=2, shard_index=1, op=SET k2 v2},
  ]
  Recovery: Both entries present → apply both

Crash after Shard A writes but before Shard B:
  WAL entries: [
    {txid=42, shard_count=2, shard_index=0, op=SET k1 v1},
    // shard_index=1 missing
  ]
  Recovery: Only 1/2 shards present → skip txid=42 entirely
```

**Important:** This provides **rollback-by-omission** for multi-shard operations. If a crash
occurs mid-operation, the incomplete operation is skipped during recovery, ensuring consistency
across shards.
```

**Cleanup Timing:**

| Mechanism | When Triggered | Maximum Lock Duration |
|-----------|----------------|----------------------|
| Connection cleanup | TCP close detected | ~100ms (TCP keepalive detection) |
| Timeout cleanup | Background scan | `vll_lock_acquisition_timeout_ms` + scan interval |
| Restart recovery | Server start | Immediate (lock state is volatile) |

**Configuration:**

```toml
[vll]
# Maximum duration for a continuation lock before forced cleanup
# Should be >= connection_timeout_ms
max_continuation_lock_ms = 65000

# Interval for orphaned lock scanning
vll_timeout_check_interval_ms = 100

# Enable aggressive cleanup (scan more frequently, tighter timeouts)
# Recommended for high-throughput environments
aggressive_lock_cleanup = false
```

**Interaction with Transaction State:**

| Lock Type | State After Cleanup | Data Impact |
|-----------|--------------------|--------------------|
| Continuation (MULTI) | Transaction discarded | No writes applied (EXEC never called) |
| Continuation (Lua) | Script aborted | Partial writes may persist (no rollback) |
| Per-key VLL lock | Operation aborted | Partial writes may persist (no rollback) |
| Intent (not acquired) | Intent removed | No data impact |

**Metrics:**

| Metric | Description |
|--------|-------------|
| `frogdb_vll_orphaned_locks_total` | Total orphaned locks detected and cleaned |
| `frogdb_vll_orphaned_lock_duration_ms` | Duration locks were held before cleanup |
| `frogdb_continuation_lock_disconnects_total` | Continuation locks released due to disconnect |
| `frogdb_continuation_lock_timeouts_total` | Continuation locks released due to timeout |

**Debugging Orphaned Locks:**

If orphaned locks are suspected:

```
# Check current lock state
DEBUG VLL LOCKS

# Sample output:
# vll_queue_depth: 5
# continuation_lock: txid=12345, held_for_ms=2340
# pending_intents: 12
# locked_keys: ["user:1", "user:2"]
```

---

### VLL-Coordinated Commands

When `allow_cross_slot_standalone = true`, these commands use VLL for atomicity:

| Command | Atomicity | On Partial Failure |
|---------|-----------|-------------------|
| MSET | Execution atomic (no interleaving) | Error returned; partial writes may persist |
| MGET | Atomic snapshot | N/A (read-only) |
| DEL | Execution atomic (no interleaving) | Error returned; partial deletes may persist |

**Behavior:**
- Coordinator acquires intents on all shards
- All shards execute atomically via VLL ordering
- On any shard failure: operation returns error to client
- **No rollback**: Partial state from successful shards persists (Redis/DragonflyDB-compatible)

**Design Rationale:**

FrogDB does **not** implement cross-shard rollback, following Redis and DragonflyDB's approach:

> "For Redis, the utility of rollbacks would not outweigh the costs in terms of performance and additional complexity." — [Redis Blog](https://redis.io/blog/you-dont-need-transaction-rollbacks-in-redis/)

**Why no rollback:**
1. **Complexity**: Distributed rollback requires two-phase commit or compensation logic
2. **Performance**: Write tracking and undo operations add overhead to every write
3. **Rollback can fail**: If rollback itself fails (shard crash), state becomes inconsistent anyway
4. **Client responsibility**: Idempotent operations and application-level compensation are cleaner

**Client guidance:**
- Design operations to be idempotent where possible
- Use hash tags to keep related keys on same shard (avoids cross-shard entirely)
- For critical operations, verify state after errors and retry/compensate as needed

---

### Transaction Conflict Resolution

VLL uses **queuing**, not cancellation, for conflicting transactions:

**Scenario: Two MULTI/EXEC transactions on overlapping keys**

```
Client A: MULTI → SET foo 1 → SET bar 2 → EXEC (txid=100)
Client B: MULTI → SET foo 3 → SET baz 4 → EXEC (txid=101)
```

**Behavior:**
1. Transaction with lower txid (A, txid=100) acquires locks first
2. Transaction B queued on `foo` - waits for A to complete
3. A executes, releases locks
4. B acquires locks, executes
5. Both transactions complete successfully (no rollback)

**Key points:**
- No transaction is cancelled due to conflicts
- txid ordering is deterministic - earlier transaction always wins
- Deadlocks impossible - ordered lock acquisition
- WATCH provides opt-in abort semantics if keys modified

### WATCH Behavior (Optimistic Locking)

WATCH provides transaction abort (not queue) semantics:

```
Client A: WATCH foo → MULTI → SET foo 1 → EXEC
Client B: SET foo 99  (between WATCH and EXEC)
```

**Result:** Client A's EXEC returns `nil` (transaction aborted, not queued).

WATCH is checked at EXEC time - if any watched key modified since WATCH,
transaction is discarded without executing.

---

### Error Types

```rust
#[derive(Debug, Clone, thiserror::Error)]
pub enum VllError {
    #[error("VLL queue full on shard {0}")]
    QueueFull(usize),

    #[error("Timeout waiting for locks")]
    Timeout,

    #[error("Conflict with transaction {blocking_txid}")]
    Conflict { blocking_txid: u64 },

    #[error("Shard {0} unavailable")]
    ShardUnavailable(usize),

    #[error("Shard {0} dropped request")]
    ShardDropped(usize),

    #[error("Partial failure: {0}")]
    PartialFailure(String),
}
```

**Error to RESP Mapping:**

| VllError | RESP Response |
|----------|---------------|
| QueueFull | `-BUSY shard queue full, try again later` |
| Timeout | `-TIMEOUT operation timed out` |
| Conflict | Internal retry or `-ERR conflict` |
| ShardUnavailable | `-ERR shard unavailable` |

---

### Metrics

VLL exposes the following metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_vll_transactions_total` | Counter | Total VLL transactions |
| `frogdb_vll_ooo_executions_total` | Counter | Out-of-order executions (SCA) |
| `frogdb_vll_queue_depth` | Gauge | Current pending operations per shard |
| `frogdb_vll_lock_wait_ms` | Histogram | Time waiting for lock acquisition |
| `frogdb_vll_partial_failures_total` | Counter | Cross-shard operations with partial failure |

---

## References

- [DragonflyDB Transactions Blog](https://www.dragonflydb.io/blog/transactions-in-dragonfly)
- [VLL Paper](https://www.vldb.org/pvldb/vol6/p901-ren.pdf) - "VLL: a lock manager redesign for main memory database systems"
- [CONCURRENCY.md](CONCURRENCY.md) - Thread model and shard architecture
- [TRANSACTIONS.md](TRANSACTIONS.md) - MULTI/EXEC transaction semantics
