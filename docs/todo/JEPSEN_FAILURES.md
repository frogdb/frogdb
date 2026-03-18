# Jepsen Test Failures

**Date discovered:** 2026-03-18
**Test suite:** raft-extended (Elle rw-register workload on 5-node Raft cluster)
**Checker:** Elle cycle-based (jepsen.tests.cycle.wr) checking `:strict-serializable`

## Passing Suites

All tests in the **replication** (5 tests) and **raft** (9 tests) suites pass.

## Failing Tests

### 1. clock-skew — FAIL

**Anomalies detected:** G-single-item-realtime, G1b, G1c-realtime, lost-update, cycle-search-timeout

**Results file:** `testing/jepsen/frogdb/store/frogdb-elle-rw-register-clock-skew-docker-cluster/latest/results.edn`

#### G-single-item-realtime (real-time ordering violation)
A cycle of transactions on a single key violates real-time ordering. If T1 completed before T2 started,
T2 must not appear before T1 in the serialization order. The cycle involves ~100+ transactions on keys
in the 4400 range, where values written by later-completing transactions are read by earlier-starting ones.

Example from the cycle trace:
- T1 reads key 4431 = 111
- T2 reads key 4431 = 116 and writes 4431 = 119
- T3 writes 4431 = 125
- T4 reads 4431 = 126
- These form a real-time dependency chain that cycles back, violating linearizability.

#### G1b (intermediate/dirty reads)
Transactions read intermediate values written by another transaction's MULTI/EXEC block before
that block finished. A reader sees value V written by a writer that also wrote V+1 in the same
transaction — the reader observed the intermediate state.

#### G1c-realtime (circular information flow)
Long dependency chains (76+ transactions) where the "Then" section shows a sequence of real-time
and write-read dependencies forming a cycle. The final transaction in the chain wrote a value read
by the first transaction — a contradiction.

---

### 2. disk-failure — FAIL

**Anomalies detected:** G1b, G1c-realtime, lost-update, cycle-search-timeout

**Results file:** `testing/jepsen/frogdb/store/frogdb-elle-rw-register-disk-failure-docker-cluster/latest/results.edn`

#### G1b (intermediate/dirty reads)
Multiple instances. Example:
- **Reader** (index 13681, process 4): reads key 70 = 127
- **Writer** (index 14104, process 1): writes key 70 = 127 then key 70 = 128 in the same MULTI/EXEC
- The reader saw the intermediate value 127 before the writer's transaction committed the final value 128.
- This occurs at multiple points in the history (around indices 13681, 52503, 57275).

#### G1c-realtime (circular information flow)
76-transaction dependency chain forming a cycle. The chain is built from:
- Real-time ordering (T_n completed before T_{n+1} started, with ~0.000-0.001s gaps)
- Write-read dependencies (T_n wrote value V which T_m later read)
- Contradiction: T76 wrote key 279 = 128, which was read by T1 — but T1 precedes T76 in real-time.

---

### 3. slow-network — FAIL

**Anomalies detected:** G1b, G1c-realtime, internal, lost-update, cycle-search-timeout

**Results file:** `testing/jepsen/frogdb/store/frogdb-elle-rw-register-slow-network-docker-cluster/latest/results.edn`

#### G1b (intermediate/dirty reads)
Same pattern as disk-failure. Example:
- **Reader** (index 31327, process 4): reads key 155 = 126
- **Writer** (index 31740, process 3): writes key 155 = 126, then 155 = 127, then 155 = 128 in one MULTI/EXEC
- Reader saw intermediate value 126 before the transaction committed 128.

#### G1c-realtime (circular information flow)
Multiple cycles found. The first involves 38 transactions around key 53 — T38 writes 53 = 128, read by T1,
but T38 is real-time-after T1. The second involves 33 transactions around key 1715 with similar structure.

#### internal (internal consistency)
A transaction read different values for the same key within a single MULTI/EXEC block — the key changed
between two reads within the same transaction. This means concurrent writes are interleaving with
the reads inside the MULTI/EXEC.

---

### 4. memory-pressure — UNKNOWN (crashed)

**Results file:** `testing/jepsen/frogdb/store/frogdb-elle-rw-register-memory-pressure-docker-cluster/latest/results.edn` (empty/corrupt — ClosedChannelException during serialization)

The JVM ran out of heap while serializing the test.jepsen results file. The test completed execution
but could not write results. Needs to be re-run in isolation (not as part of a batch via `test-all`).

---

## Root Cause Analysis

### The core problem: MULTI/EXEC is not isolated

FrogDB's MULTI/EXEC implementation provides **execution serialization** (no command interleaving
within the shard event loop) but **not full isolation** from concurrent readers.

**Code path:**

1. Commands are queued in memory on the connection during MULTI
   (`frogdb-server/crates/server/src/connection/handlers/transaction.rs:27-40`)

2. On EXEC, all commands are sent to the shard as `ShardMessage::ExecTransaction`
   (`frogdb-server/crates/server/src/connection/handlers/transaction.rs:175-282`)

3. The shard executes each command **sequentially but individually**:
   (`frogdb-server/crates/core/src/shard/execution.rs:164-206`)
   ```rust
   for (i, command) in commands.iter().enumerate() {
       let response = self.execute_command(command, ...).await;
       results.push(response);
   }
   ```

4. **Each command's side effects are applied immediately** — version incremented, WAL entry written,
   and replication broadcast sent **per individual command**, not as a batch:
   (`frogdb-server/crates/core/src/shard/pipeline.rs:41-79`)
   - `self.increment_version()` — per command
   - `self.persist_by_strategy(handler, args)` — per command WAL entry
   - `self.replication_broadcaster.broadcast_command(...)` — per command broadcast

### Why this causes the observed anomalies

**G1b (dirty/intermediate reads):**
When a MULTI/EXEC block writes key K = 127 then K = 128, the write K = 127 is immediately
visible in the store after the first command executes. A concurrent reader on a **different
connection** (served by a different async task) can read K = 127 before the second command
(K = 128) executes. In a correct implementation, neither write should be visible until EXEC
completes.

Under normal operation, the shard event loop may process interleaved reads between
transaction commands if:
- The shard is processing commands from multiple connections concurrently
- The `.await` in `execute_command` yields, allowing another connection's read to run
- Under fault conditions (disk failure, slow network), these yields become more frequent
  due to WAL write latency

**G1c-realtime / G-single-item-realtime:**
These are consequences of the same isolation failure. When transactions can observe intermediate
states, dependency cycles become possible: T1 reads a value from T2's intermediate state, while
T2 (in real-time) follows T1. The cycle-based checker detects these as G1c-realtime violations.

**lost-update:**
Two concurrent MULTI/EXEC blocks read a key's value, compute new values, and write them.
Because neither transaction's writes are isolated, both read the same initial value and
one update is lost.

**internal:**
Within a single MULTI/EXEC block, two reads of the same key return different values. This
means another connection's write was applied between the two reads in the same transaction.
This confirms that the shard event loop is interleaving operations from different connections
during transaction execution.

### Why these only manifest under fault injection

The `raft` and `replication` suites (which test without exotic fault injection) pass because:
- Under normal conditions, the shard event loop processes transaction commands fast enough
  that interleaving is rare or doesn't create detectable cycles
- The fault injection nemeses (clock-skew, disk-failure, slow-network) slow down WAL writes
  and network operations, causing more `.await` yields during `execute_command`, increasing
  the window for interleaved reads

## Fix Direction

The fix should ensure that MULTI/EXEC provides **snapshot isolation** or **full serialization**:

1. **Option A: Buffer writes until EXEC** — Execute commands against a copy/overlay of the
   data, apply all writes atomically at EXEC time. This matches what `docs/spec/TRANSACTIONS.md`
   claims ("Entire transaction written as single RocksDB WriteBatch").

2. **Option B: Hold shard lock during EXEC** — Prevent any other connection's commands from
   executing on the shard while a MULTI/EXEC is in progress. This is simpler but blocks
   concurrent readers.

3. **Option C: Version-stamped reads** — Give each transaction a consistent read snapshot
   at EXEC start time, so reads within the transaction see a frozen point-in-time view.

### Key files to modify

| File | Lines | What to change |
|------|-------|----------------|
| `frogdb-server/crates/core/src/shard/execution.rs` | 164-206 | `execute_transaction` — must batch writes |
| `frogdb-server/crates/core/src/shard/pipeline.rs` | 20-80 | `run_post_execution` — must defer WAL/broadcast until EXEC |
| `frogdb-server/crates/core/src/shard/worker.rs` | 470-484 | Version increment — should be single increment per txn |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 80-90 | May need to hold exclusive access during txn |
