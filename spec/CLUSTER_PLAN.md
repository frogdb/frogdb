# FrogDB Clustering Implementation Plan

## Overview

This plan covers the implementation of FrogDB clustering as specified in the ROADMAP.md and detailed in CLUSTER.md, REPLICATION.md, PERSISTENCE.md, and related specifications.

## Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Phasing** | Replication first | Lower risk, incremental delivery. Phase 1 = standalone replication |
| **Orchestrator** | Build reference implementation | Simple orchestrator for testing. Not production-grade |
| **Testing** | Unit tests first | Write unit tests as each component is built |
| **Blocking Cmds** | Client timeouts | Matches Redis/Valkey behavior. Document known edge cases |

## Current State Analysis

### What Exists
- **Stub Commands**: All cluster/replication commands exist as stubs (CLUSTER, PSYNC, REPLCONF, WAIT, REPLICAOF, READONLY, ASKING, etc.) in `crates/server/src/commands/stub.rs`
- **Replication Abstractions**: `ReplicationConfig` enum and `ReplicationTracker` trait defined in `crates/core/src/noop.rs`
- **HTTP Infrastructure**: Metrics server exists using hyper with `/metrics`, `/health/live`, `/health/ready` endpoints
- **Blocking Infrastructure**: `ShardWaitQueue` and `WaitEntry` for blocking operations
- **Slot Constant**: `REDIS_CLUSTER_SLOTS = 16384` defined

### What Needs to Be Built
1. Primary-Replica Replication Protocol (PSYNC, WAL streaming)
2. Admin HTTP API (orchestrator communication)
3. Cluster Commands Implementation
4. Cluster Topology & State Management
5. Slot Migration Protocol
6. Failover Support
7. Testing Infrastructure (including reference orchestrator)

---

## Implementation Phases

### Phase 1: Primary-Replica Replication (First Milestone)
**Goal**: Enable standalone primary-replica replication without cluster topology

This phase delivers a working replication system where one FrogDB node can replicate to another.

#### 1.1 Replication State Management
**Files:** `crates/core/src/replication/state.rs`

```rust
pub struct ReplicationState {
    pub replication_id: String,        // 40-char hex (20 random bytes)
    pub secondary_id: Option<String>,  // Previous primary's ID (for PSYNC continuity)
    pub replication_offset: u64,       // Current WAL position
}
```

- Generate ReplicationId: `uuid::Uuid::new_v4()` → hex encode → 40 chars
- Persist to `data/replication_state.json`
- Load on startup (generate new ID if file missing)

**Unit Tests:**
- ID generation format validation
- State persistence round-trip
- ID regeneration on corruption

#### 1.2 REPLICAOF/SLAVEOF Command
**Files:** `crates/server/src/commands/replication.rs`

- `REPLICAOF <host> <port>` - become replica
- `REPLICAOF NO ONE` - return to standalone
- Update `ReplicationConfig` enum in server state
- Initiate connection task to primary

**Behavior:**
```
REPLICAOF 127.0.0.1 6379
+OK  (async - connection starts in background)
```

#### 1.3 PSYNC Protocol (Replica Side)
**Files:** `crates/server/src/replication/replica.rs`

Connection sequence:
1. Connect to primary
2. `AUTH <password>` (if configured)
3. `REPLCONF listening-port <port>`
4. `REPLCONF capa eof psync2`
5. `PSYNC <repl_id> <offset>` or `PSYNC ? -1` (first sync)

Handle responses:
- `+FULLRESYNC <id> <offset>` → receive checkpoint, then stream
- `+CONTINUE` → resume streaming from offset
- `-ERR` → retry with exponential backoff

**State Machine:**
```
CONNECTING → AUTHENTICATING → HANDSHAKING → SYNCING → STREAMING
     │              │               │           │
     └──────────────┴───────────────┴───────────┴──→ CONNECTING (on error)
```

#### 1.4 WAL Streaming (Primary Side)
**Files:** `crates/server/src/replication/primary.rs`

- Register replica connections
- Use RocksDB `GetUpdatesSince(sequence)` for incremental data
- Frame format (per REPLICATION.md):
  ```
  [4 bytes magic: FRPL] [1 byte version] [1 byte flags]
  [8 bytes sequence] [4 bytes length] [payload...]
  ```
- Stream frames as WAL entries arrive
- Track per-replica offset

**No buffer limits:** Primary streams at replica's consumption pace (TCP backpressure).

#### 1.5 REPLCONF Command
**Files:** `crates/server/src/commands/replication.rs`

| Subcommand | Direction | Purpose |
|------------|-----------|---------|
| `REPLCONF listening-port <port>` | R→P | Announce replica port |
| `REPLCONF capa eof psync2` | R→P | Capability negotiation |
| `REPLCONF ACK <offset>` | R→P | Acknowledge WAL position |
| `REPLCONF GETACK *` | P→R | Request ACK from replica |

#### 1.6 Full Sync (FULLRESYNC)
**Files:** `crates/server/src/replication/fullsync.rs`

When replica cannot partial sync (offset too old or no matching repl_id):
1. Primary creates RocksDB checkpoint
2. Primary sends checkpoint metadata (size, checksum)
3. Primary streams checkpoint files
4. Replica receives and validates (SHA256)
5. Replica loads checkpoint
6. Switch to incremental streaming

**Memory limit:** If `fullsync_max_memory_mb` exceeded, reject FULLRESYNC with error.

#### 1.7 WAIT Command
**Files:** `crates/server/src/commands/replication.rs`

```
WAIT <numreplicas> <timeout_ms>
```

- Wait for N replicas to ACK current offset
- Return count of replicas that ACK'd
- Use `ReplicationTracker::wait_for_acks()` method

**Implementation:**
```rust
async fn wait_command(ctx: &CommandContext, args: &[Bytes]) -> Result<Response> {
    let num_replicas = parse_int(args[0])?;
    let timeout_ms = parse_int(args[1])?;
    let current_seq = ctx.replication_tracker.current_sequence();
    let acked = ctx.replication_tracker.wait_for_acks(
        current_seq, num_replicas, timeout_ms
    ).await;
    Ok(Response::Integer(acked as i64))
}
```

#### 1.8 INFO Replication
**Files:** `crates/server/src/commands/info.rs`

Add replication section to INFO command:
```
# Replication
role:master (or slave)
connected_slaves:1
slave0:ip=127.0.0.1,port=6380,state=online,offset=12345,lag=0
master_replid:abc123...
master_repl_offset:12345
```

#### Phase 1 File Summary

| File | Action | Purpose |
|------|--------|---------|
| `crates/core/src/replication/mod.rs` | Create | Module structure |
| `crates/core/src/replication/state.rs` | Create | ReplicationState, ID generation |
| `crates/core/src/replication/tracker.rs` | Create | Concrete ReplicationTracker impl |
| `crates/server/src/replication/mod.rs` | Create | Server-side replication module |
| `crates/server/src/replication/primary.rs` | Create | Primary node handling |
| `crates/server/src/replication/replica.rs` | Create | Replica node handling |
| `crates/server/src/replication/fullsync.rs` | Create | FULLRESYNC protocol |
| `crates/server/src/commands/replication.rs` | Create | REPLCONF, PSYNC, WAIT, REPLICAOF |
| `crates/server/src/commands/info.rs` | Modify | Add replication section |
| `crates/server/src/config.rs` | Modify | Add replication config options |

#### Phase 1 Verification

```bash
# Start primary
cargo run -- --port 6379

# Start replica
cargo run -- --port 6380

# Make replica follow primary
redis-cli -p 6380 REPLICAOF 127.0.0.1 6379

# Verify replication
redis-cli -p 6379 SET testkey "hello"
redis-cli -p 6380 GET testkey  # Should return "hello"

# Check replication status
redis-cli -p 6379 INFO replication

# Test synchronous replication
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 WAIT 1 5000  # Wait for 1 replica to ACK within 5s
```

---

### Phase 2: Admin API & Cluster Topology
**Goal**: Enable orchestrator communication and topology management

#### 2.1 Admin HTTP Server
**Files:** `crates/server/src/admin_api.rs`

Separate HTTP server (not on RESP port):
- Default: `127.0.0.1:6380` (localhost only for security)
- Security options: bearer token, mTLS

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/admin/cluster` | POST | Receive topology update from orchestrator |
| `/admin/cluster` | GET | Return current topology |
| `/admin/health` | GET | Detailed health check |
| `/admin/replication` | GET | Replication status |
| `/admin/role` | POST | Change role (PROMOTE/DEMOTE) |

#### 2.2 Cluster Topology State
**Files:** `crates/core/src/cluster/topology.rs`

```rust
pub struct ClusterTopology {
    pub epoch: u64,                    // Monotonic version number
    pub nodes: HashMap<NodeId, NodeInfo>,
    pub slot_map: [NodeId; 16384],     // Slot → owning node
    pub my_node_id: NodeId,
}

pub struct NodeInfo {
    pub id: NodeId,
    pub address: SocketAddr,
    pub role: NodeRole,
    pub slots: Vec<SlotRange>,
    pub replicas: Vec<NodeId>,
}
```

#### 2.3 Configuration Extensions
**Files:** `crates/server/src/config.rs`

```toml
[cluster]
enabled = false                          # Enable cluster mode
admin_bind = "127.0.0.1"
admin_port = 6380
admin_auth_token = ""                    # Bearer token for admin API
min_replicas_to_write = 0                # Synchronous replication quorum
self_fence_timeout_ms = 30000            # Self-fence if orchestrator unreachable
node_timeout_ms = 15000                  # Node considered dead after this
```

#### 2.4 CLUSTER Commands
**Files:** `crates/server/src/commands/cluster.rs`

| Command | Description |
|---------|-------------|
| `CLUSTER SLOTS` | Return slot ranges and nodes (RESP array) |
| `CLUSTER SHARDS` | Return shard info (RESP map format) |
| `CLUSTER NODES` | Return nodes in Redis cluster format |
| `CLUSTER INFO` | Return cluster state summary |
| `CLUSTER KEYSLOT <key>` | Return slot number for key |
| `CLUSTER COUNTKEYSINSLOT <slot>` | Count keys in slot |
| `CLUSTER GETKEYSINSLOT <slot> <count>` | Get key names in slot |
| `CLUSTER MYID` | Return this node's ID |

#### 2.5 Reference Orchestrator (Testing Tool)
**Files:** `tools/orchestrator/` (new crate)

Simple orchestrator for integration testing:
- Push topology to nodes
- Trigger failovers
- Monitor node health
- NOT production-grade

```rust
// tools/orchestrator/src/main.rs
#[tokio::main]
async fn main() {
    let nodes = vec!["127.0.0.1:6380", "127.0.0.1:6381"];
    let topology = build_initial_topology(&nodes);
    push_topology(&nodes, &topology).await;
    // Health check loop, failover triggers, etc.
}
```

#### Phase 2 File Summary

| File | Action | Purpose |
|------|--------|---------|
| `crates/server/src/admin_api.rs` | Create | Admin HTTP server |
| `crates/core/src/cluster/mod.rs` | Create | Cluster module |
| `crates/core/src/cluster/topology.rs` | Create | Topology state |
| `crates/server/src/commands/cluster.rs` | Create | CLUSTER commands |
| `crates/server/src/config.rs` | Modify | Cluster config |
| `tools/orchestrator/Cargo.toml` | Create | Orchestrator crate |
| `tools/orchestrator/src/main.rs` | Create | Orchestrator main |

#### Phase 2 Verification

```bash
# Start node with cluster enabled
cargo run -- --port 6379 --cluster-enabled --admin-port 6380

# Push topology via curl
curl -X POST http://127.0.0.1:6380/admin/cluster \
  -H "Authorization: Bearer token" \
  -d '{"epoch": 1, "nodes": [...]}'

# Verify topology
redis-cli -p 6379 CLUSTER SLOTS
redis-cli -p 6379 CLUSTER INFO
```

---

### Phase 3: Client Protocol & Redirects
**Goal**: Enable Redis Cluster client compatibility

#### 3.1 Slot Calculation & Hash Tags
**Files:** `crates/core/src/cluster/routing.rs`

```rust
pub fn key_slot(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16(hash_key) % 16384
}

fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    // Find substring between first { and first } after it
    let start = key.iter().position(|&c| c == b'{')?;
    let end = key[start+1..].iter().position(|&c| c == b'}')?;
    if end > 0 {
        Some(&key[start+1..start+1+end])
    } else {
        None // Empty tag like {} means use full key
    }
}
```

#### 3.2 Slot Ownership Validation
**Files:** `crates/server/src/pipeline.rs` (or command execution path)

Before command execution:
1. Extract keys from command
2. Calculate slot for each key
3. Check if all keys in same slot (or reject CROSSSLOT)
4. Check if we own the slot
5. Check MIGRATING/IMPORTING state

#### 3.3 MOVED Redirection
Return when key is on different node:
```
-MOVED <slot> <host>:<port>
```
Example: `-MOVED 3999 127.0.0.1:6381`

#### 3.4 ASK Redirection
During slot migration, when key not found locally:
```
-ASK <slot> <host>:<port>
```
Client must send `ASKING` before command on target node.

#### 3.5 ASKING Command
**Files:** `crates/server/src/commands/cluster.rs`

- Sets per-connection flag allowing one command to execute for importing slot
- Flag cleared after next command
- No persistence required

#### 3.6 READONLY/READWRITE Commands
**Files:** `crates/server/src/commands/cluster.rs`

| Command | Effect |
|---------|--------|
| `READONLY` | Allow reads from replica (returns possibly stale data) |
| `READWRITE` | Require primary for all commands (default) |

Per-connection state tracking.

#### 3.7 CROSSSLOT Validation
Reject multi-key commands spanning different slots:
```
-CROSSSLOT Keys in request don't hash to the same slot
```

**VLL Note:** In cluster mode, VLL only coordinates within-node internal shards. Cross-node operations always return CROSSSLOT.

#### Phase 3 File Summary

| File | Action | Purpose |
|------|--------|---------|
| `crates/core/src/cluster/routing.rs` | Create | Slot calculation, hash tags |
| `crates/server/src/commands/cluster.rs` | Modify | ASKING, READONLY, READWRITE |
| `crates/server/src/pipeline.rs` | Modify | Slot validation in command path |

#### Phase 3 Verification

```bash
# Test slot calculation
redis-cli -p 6379 CLUSTER KEYSLOT mykey
redis-cli -p 6379 CLUSTER KEYSLOT {user:1}:name

# Test MOVED redirect (when key on different node)
redis-cli -p 6379 GET key_on_other_node
# Should return: (error) MOVED 1234 127.0.0.1:6380

# Test CROSSSLOT
redis-cli -p 6379 MSET key1 val1 key2 val2
# Should return: (error) CROSSSLOT Keys in request don't hash to the same slot

# Same slot via hash tag
redis-cli -p 6379 MSET {user}:a val1 {user}:b val2
# Should succeed
```

---

### Phase 4: Failover Support
**Goal**: Enable automatic and manual failover

#### 4.1 Internal Role Commands (Admin API)
**Files:** `crates/server/src/admin_api.rs`

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/admin/role` | POST `{"action": "promote", "epoch": N}` | Promote to primary |
| `/admin/role` | POST `{"action": "demote", "new_primary": "addr"}` | Demote to replica |

#### 4.2 Replica Promotion State Machine
**Files:** `crates/core/src/cluster/failover.rs`

```
REPLICA_CONNECTED ─[PROMOTE cmd]─▶ PROMOTING
                                      │
                              1. Stop replication stream
                              2. Flush pending WAL
                              3. Wait for WAL sync
                                      │
                                      ▼
                                 BECOMING_PRIMARY
                                      │
                              4. Generate new ReplicationId
                              5. Store secondary_id
                              6. Set role = PRIMARY
                                      │
                                      ▼
                                 PRIMARY_READY
                                      │
                              7. Accept writes
                              8. Accept replica connections
```

#### 4.3 Self-Fencing
**Files:** `crates/core/src/cluster/fencing.rs`

When orchestrator unreachable for `self_fence_timeout_ms`:
- Self-fence: stop accepting writes
- Mode: `readonly` or `reject_all` (configurable)
- Resume on orchestrator reconnection

```rust
pub struct FencingState {
    orchestrator_last_contact: Instant,
    is_self_fenced: bool,
}
```

#### 4.4 Manual Failover (CLUSTER FAILOVER)
**Files:** `crates/server/src/commands/cluster.rs`

| Mode | Behavior |
|------|----------|
| Default (graceful) | Wait for replica to catch up, then switch |
| `FORCE` | Immediate promotion, may lose data |
| `TAKEOVER` | Immediate, no handshake (emergency) |

#### 4.5 Split-Brain Handling
When old primary receives demotion after accepting divergent writes:
1. Compare sequence numbers
2. Log divergent operations to `data/split_brain_discarded.log`
3. Roll back to last_replicated_seq
4. Connect as replica to new primary

**Log Format:**
```
timestamp=2024-01-15T10:30:45Z old_primary=node-abc seq_start=12345 seq_end=12400 ops_lost=55
[MSET key1 value1 key2 value2]
[INCR counter]
```

#### 4.6 Blocking Commands During Failover
Per decision: rely on **client timeouts** (matching Redis/Valkey behavior).
- Blocked BLPOP/BRPOP clients timeout when primary fails
- Client retries on new primary
- Document known edge cases in user docs

#### Phase 4 File Summary

| File | Action | Purpose |
|------|--------|---------|
| `crates/core/src/cluster/failover.rs` | Create | Failover state machine |
| `crates/core/src/cluster/fencing.rs` | Create | Self-fencing logic |
| `crates/server/src/commands/cluster.rs` | Modify | CLUSTER FAILOVER |
| `crates/server/src/admin_api.rs` | Modify | Role change endpoints |

#### Phase 4 Verification

```bash
# Test manual failover
redis-cli -p 6380 CLUSTER FAILOVER  # On replica

# Test self-fencing (stop orchestrator, wait for timeout)
# Node should become read-only

# Verify split-brain log exists after divergent writes
cat data/split_brain_discarded.log
```

---

### Phase 5: Slot Migration
**Goal**: Enable live slot migration between nodes

#### 5.1 Migration State Machine
**Files:** `crates/core/src/cluster/migration.rs`

```rust
pub enum SlotMigrationState {
    Normal,
    Migrating { target: NodeId, keys_sent: usize },
    Importing { source: NodeId, keys_received: usize },
}
```

#### 5.2 Migration Protocol Commands (DFLYMIGRATE)
**Files:** `crates/server/src/commands/migrate.rs`

| Command | Direction | Purpose |
|---------|-----------|---------|
| `DFLYMIGRATE INIT <id> <source> <shards> <slots>` | Orch→Target | Initiate migration |
| `DFLYMIGRATE FLOW <id> <shard>` | Source→Target | Establish data flow |
| `DFLYMIGRATE DATA <id> <shard> <seq> <count> ...` | Source→Target | Key batches |
| `DFLYMIGRATE ACK <id> <shard> <keys> <bytes>` | Source→Target | Shard completion |
| `DFLYMIGRATE FINALIZE <id>` | Orch→Both | Complete migration |

#### 5.3 Key Transfer Algorithm

**Phase A: Snapshot**
- Create point-in-time key iterator for slot
- Iterator sees keys as they existed at migration START

**Phase B: Streaming**
- Batch keys (default: 100 keys or 1MB per batch)
- DUMP format for serialization (includes TTL)
- Track writes during migration:
  - Key in iterator but already sent → forward to target
  - New key (not in iterator) → apply locally, queue for migration

**Phase C: Finalize**
- Orchestrator sends DFLYMIGRATE FINALIZE to both nodes
- Source: clear MIGRATING, delete migrated keys (async, low priority)
- Target: clear IMPORTING, accept direct writes

#### 5.4 ASK Redirection During Migration

| Scenario | Response |
|----------|----------|
| Source has key | Execute locally |
| Source doesn't have key, MIGRATING | `-ASK <slot> <target>` |
| Target, IMPORTING, no ASKING | `-MOVED <slot> <source>` |
| Target, IMPORTING, ASKING set | Execute locally |

#### 5.5 Failure Recovery

| Failure | Recovery |
|---------|----------|
| Source crashes | Promote replica (has all keys), target discards partial |
| Target crashes | Source aborts, remains owner |
| Timeout | Source aborts, keys remain, target cleanup on topology update |

#### Phase 5 File Summary

| File | Action | Purpose |
|------|--------|---------|
| `crates/core/src/cluster/migration.rs` | Create | Migration state machine |
| `crates/server/src/commands/migrate.rs` | Create | DFLYMIGRATE commands |
| `crates/server/src/replication/fullsync.rs` | Modify | Reuse DUMP serialization |

#### Phase 5 Verification

```bash
# Use reference orchestrator to trigger migration
./tools/orchestrator/target/release/orchestrator migrate --slot 1234 --from nodeA --to nodeB

# Monitor migration progress
redis-cli -p 6379 CLUSTER SLOTS  # During migration, shows MIGRATING/IMPORTING

# Verify key moved
redis-cli -p 6379 GET migrated_key
# Should return: (error) MOVED 1234 127.0.0.1:6380
```

---

### Phase 6: Testing & Validation
**Goal**: Comprehensive testing for correctness

Per decision: **Unit tests first**, integration tests after each phase.

#### 6.1 Unit Tests (Per Phase)

| Phase | Unit Test Coverage |
|-------|-------------------|
| 1 | Replication ID generation, PSYNC parsing, frame encoding/decoding |
| 2 | Topology JSON parsing, epoch comparison, slot mapping |
| 3 | Slot calculation, hash tag extraction, CRC16 |
| 4 | Failover state transitions, fencing logic |
| 5 | Migration state machine, DUMP/RESTORE format |

#### 6.2 Integration Tests
**Files:** `tests/integration/cluster/`

- Multi-node setup harness
- Primary-replica sync verification
- Failover scenario tests
- Migration scenario tests
- Client redirect handling

#### 6.3 Reference Orchestrator Tests
**Files:** `tools/orchestrator/tests/`

- Push topology to nodes
- Trigger failovers programmatically
- Validate cluster state after operations

#### 6.4 Future: Distributed Correctness
Per ROADMAP, Jepsen/Turmoil tests for linearizability are a future enhancement.

---

## Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Control Plane** | Orchestrated (not gossip) | Deterministic updates, simpler impl, like DragonflyDB |
| **Replication Scope** | Full dataset per replica | Simpler protocol, any replica can be promoted |
| **WAL Streaming** | RocksDB `GetUpdatesSince()` | Native integration, efficient delta transfer |
| **Partial Failure** | No rollback | Matches Redis/DragonflyDB, use hash tags for atomicity |
| **Blocking Failover** | Client timeouts | Matches Redis/Valkey, document edge cases |

---

## Implementation Order Summary

```
Phase 1: Replication (FIRST MILESTONE)
   └── REPLICAOF, PSYNC, REPLCONF, WAIT, WAL streaming
         ↓
Phase 2: Admin API & Topology
   └── Admin HTTP server, CLUSTER commands, reference orchestrator
         ↓
Phase 3: Client Protocol
   └── MOVED, ASK, ASKING, READONLY, CROSSSLOT
         ↓
Phase 4: Failover
   └── CLUSTER FAILOVER, self-fencing, split-brain handling
         ↓
Phase 5: Migration
   └── DFLYMIGRATE protocol, live slot migration
         ↓
Phase 6: Testing
   └── Integration tests, chaos testing
```

---

## Estimated Scope

| Phase | Complexity | Key Deliverables |
|-------|------------|------------------|
| 1 | **High** | Working primary-replica replication |
| 2 | **Medium** | Admin API, topology management, reference orchestrator |
| 3 | **Medium** | Redis Cluster client compatibility |
| 4 | **High** | Automatic/manual failover |
| 5 | **High** | Live slot migration |
| 6 | **Medium** | Comprehensive test suite |

**Recommendation**: Deliver Phase 1 as first milestone. Each subsequent phase delivers incremental value.
