# FrogDB Request Flow Diagrams

High-level component-interaction diagrams showing how requests move between major architectural components. Each diagram shows named components, the messages passed between them, and the channel types used.

**Key source files:**
- `crates/server/src/acceptor.rs` — `Acceptor`
- `crates/server/src/connection.rs` — `ConnectionHandler`
- `crates/core/src/shard.rs` — `ShardWorker`, `ShardMessage`, `ScatterOp`
- `crates/server/src/scatter/executor.rs` — `ScatterGatherExecutor`
- `crates/core/src/vll/` — `IntentTable`, `TransactionQueue`, `ExecuteSignal`
- `crates/core/src/pubsub.rs` — `PubSubMessage`, `ShardSubscriptions`
- `crates/server/src/replication/` — `PrimaryReplicationHandler`, `ReplicaReplicationHandler`, `ReplicaCommandExecutor`
- `crates/core/src/cluster/` — `ClusterRaft`, `ClusterStateMachine`, `ClusterState`

---

## 1. System Architecture Overview

All major components and their communication channels.

```mermaid
flowchart TB
    Client([Client TCP])

    subgraph Server
        A[Acceptor]

        subgraph "Per-Connection"
            CH[ConnectionHandler]
            SGE[ScatterGatherExecutor]
        end

        subgraph "Shard Layer"
            SW0[ShardWorker 0]
            SW1[ShardWorker 1]
            SWN[ShardWorker N]
        end

        subgraph "Replication (Primary)"
            PRH[PrimaryReplicationHandler]
        end

        subgraph "Cluster"
            CR[ClusterRaft]
            CSM[ClusterStateMachine]
            CS[ClusterState]
        end
    end

    subgraph "Replica Node"
        RRH[ReplicaReplicationHandler]
        RCE[ReplicaCommandExecutor]
        RSW[ShardWorker replica]
    end

    Client -->|TCP| A
    A -->|"tokio::spawn per conn"| CH
    CH -->|"ShardMessage (mpsc)"| SW0 & SW1 & SWN
    SW0 & SW1 & SWN -.->|"Response (oneshot)"| CH
    CH -->|"creates per scatter op"| SGE
    SGE -->|"VllLockRequest / VllExecute (mpsc)"| SW0 & SW1 & SWN
    SGE -.->|"PartialResult (oneshot)"| CH
    SW0 & SW1 & SWN -->|"ReplicationFrame (broadcast)"| PRH
    PRH -->|TCP stream| RRH
    RRH -->|"ReplicationFrame (mpsc)"| RCE
    RCE -->|"ShardMessage::Execute (mpsc)"| RSW
    CH -->|"ClusterCommand via Raft client"| CR
    CR -->|"Raft log apply"| CSM
    CSM -->|"mutates"| CS
```

### Channel Summary

| From | To | Channel | Message |
|------|----|---------|---------|
| ConnectionHandler | ShardWorker | `mpsc::Sender<ShardMessage>` | `ShardMessage::*` |
| ShardWorker | ConnectionHandler | `oneshot::Sender<Response>` | `Response` |
| ScatterGatherExecutor | ShardWorker | `mpsc::Sender<ShardMessage>` | `VllLockRequest`, `VllExecute` |
| ShardWorker | ScatterGatherExecutor | `oneshot::Sender<ShardReadyResult>` | `ShardReadyResult::Ready` |
| ShardWorker | ConnectionHandler | `mpsc::UnboundedSender<PubSubMessage>` | `PubSubMessage::*` |
| ShardWorker | PrimaryReplicationHandler | `broadcast::Sender<ReplicationFrame>` | `ReplicationFrame` |
| ReplicaReplicationHandler | ReplicaCommandExecutor | `mpsc::Sender<ReplicationFrame>` | `ReplicationFrame` |
| ReplicaCommandExecutor | ShardWorker | `mpsc::Sender<ShardMessage>` | `ShardMessage::Execute` |

---

## 2. Single-Key Command (GET, SET, INCR, LPUSH, ...)

A command targeting one key is hashed to its owning shard and executed directly.

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ConnectionHandler
    participant SW as ShardWorker

    C->>CH: RESP frame (e.g. SET key val)
    CH->>CH: shard_for_key(key, num_shards)
    CH->>SW: ShardMessage::Execute { command, response_tx } (mpsc)
    SW->>SW: command.execute(ctx, args)
    SW-->>CH: Response (oneshot)
    CH->>C: RESP response
```

**Key routing:** `CRC16(key) mod 16384 → slot`, then `slot mod num_shards → shard_id`. Hash tags `{...}` override to use only the tag contents for hashing.

---

## 3. Scatter-Gather Command (MGET, MSET, DEL, EXISTS, ...)

Multi-key commands spanning multiple shards use VLL (Very Lightweight Locking) for atomic cross-shard execution.

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ConnectionHandler
    participant SGE as ScatterGatherExecutor
    participant SW1 as ShardWorker A
    participant SW2 as ShardWorker B

    C->>CH: RESP frame (e.g. MGET k1 k2 k3)
    CH->>CH: Group keys by shard
    CH->>SGE: execute(operation, keys_by_shard)

    Note over SGE: Phase 1 — Acquire global txid

    par Lock all shards (sorted order)
        SGE->>SW1: ShardMessage::VllLockRequest { txid, keys, ready_tx, execute_rx } (mpsc)
        SGE->>SW2: ShardMessage::VllLockRequest { txid, keys, ready_tx, execute_rx } (mpsc)
    end

    Note over SGE: Phase 2 — Wait for all shards ready

    SW1-->>SGE: ShardReadyResult::Ready (oneshot)
    SW2-->>SGE: ShardReadyResult::Ready (oneshot)

    Note over SGE: Phase 3 — Signal execution

    par Send proceed signal
        SGE-->>SW1: ExecuteSignal { proceed: true } (oneshot)
        SGE-->>SW2: ExecuteSignal { proceed: true } (oneshot)
    end

    par Gather results
        SGE->>SW1: ShardMessage::VllExecute { txid, response_tx } (mpsc)
        SGE->>SW2: ShardMessage::VllExecute { txid, response_tx } (mpsc)
    end

    SW1-->>SGE: PartialResult (oneshot)
    SW2-->>SGE: PartialResult (oneshot)

    Note over SGE: Phase 4 — Merge via MergeStrategy

    SGE-->>CH: Merged Response
    CH->>C: RESP response
```

**Merge strategies:** `OrderedArray` (MGET — reassemble in key order), `SumIntegers` (DEL/EXISTS — sum counts), `AllOk` (MSET — all must succeed).

---

## 4. Blocking Command (BLPOP, BRPOP, BLMOVE, ...)

Blocking commands suspend the connection until data arrives or the timeout expires.

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ConnectionHandler
    participant SW as ShardWorker
    participant WQ as ShardWaitQueue

    C->>CH: BLPOP key1 key2 timeout
    CH->>SW: ShardMessage::Execute { command, response_tx } (mpsc)
    SW->>SW: Attempt pop — no data available
    SW-->>CH: Response::BlockingNeeded { keys, timeout, op } (oneshot)

    CH->>SW: ShardMessage::BlockWait { keys, op, response_tx, deadline } (mpsc)
    SW->>WQ: Register WaitEntry for keys

    alt Data arrives (another client pushes)
        SW->>WQ: Wake first waiter for key
        WQ-->>CH: Response with popped data (oneshot)
    else Timeout expires
        CH->>SW: ShardMessage::UnregisterWait { conn_id } (mpsc)
        WQ-->>CH: Nil / timeout response (oneshot)
    end

    CH->>C: RESP response
```

---

## 5. Pub/Sub (SUBSCRIBE, PUBLISH, SSUBSCRIBE, SPUBLISH)

Broadcast pub/sub fans out to all shards; sharded pub/sub routes to the channel's owner shard.

```mermaid
sequenceDiagram
    participant Sub as Subscriber Client
    participant SCH as Subscriber ConnectionHandler
    participant SW as ShardWorker (all shards)
    participant PCH as Publisher ConnectionHandler
    participant Pub as Publisher Client

    Note over Sub,SCH: Subscribe phase

    Sub->>SCH: SUBSCRIBE channel
    SCH->>SW: ShardMessage::Subscribe { channels, conn_id, sender: pubsub_tx } (mpsc)
    SW-->>SCH: subscription count (oneshot)
    SCH->>Sub: +subscribe confirmation

    Note over Pub,PCH: Publish phase

    Pub->>PCH: PUBLISH channel message
    PCH->>SW: ShardMessage::Publish { channel, message } (mpsc)
    SW->>SW: Match channel against ShardSubscriptions
    SW->>SCH: PubSubMessage::Message { channel, payload } (unbounded mpsc via pubsub_tx)
    SCH->>Sub: +message push
    SW-->>PCH: receiver count (oneshot)
    PCH->>Pub: Integer reply
```

**Sharded pub/sub** (`SSUBSCRIBE`/`SPUBLISH`) routes to a single shard via `shard_for_key(channel)` instead of broadcasting to all shards.

---

## 6. Transaction (MULTI / EXEC)

Commands are queued locally in the ConnectionHandler during MULTI, then sent as a batch to a single shard on EXEC.

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ConnectionHandler
    participant SW as ShardWorker

    C->>CH: MULTI
    CH->>CH: TransactionState.queue = Some(Vec::new())
    CH->>C: +OK

    C->>CH: SET {user}:name Alice
    CH->>CH: queue_command — track target shard
    CH->>C: +QUEUED

    C->>CH: INCR {user}:visits
    CH->>CH: queue_command — same shard confirmed
    CH->>C: +QUEUED

    C->>CH: EXEC
    CH->>SW: ShardMessage::ExecTransaction { commands, watches, response_tx } (mpsc)
    SW->>SW: Check WATCH versions (if any)
    SW->>SW: Execute all queued commands atomically
    SW-->>CH: TransactionResult::Success(Vec of Response) (oneshot)
    CH->>C: Array of results
```

**Constraints:** All keys in a transaction must hash to the same shard. Multi-shard transactions return a `CROSSSLOT` error. Use hash tags to colocate keys.

---

## 7. Replication (Primary → Replica)

Write commands are broadcast from the primary's shard workers to replicas via WAL streaming.

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ConnectionHandler
    participant SW as ShardWorker (primary)
    participant RB as ReplicationBroadcaster
    participant PRH as PrimaryReplicationHandler
    participant TCP as TCP Stream
    participant RRH as ReplicaReplicationHandler
    participant RCE as ReplicaCommandExecutor
    participant RSW as ShardWorker (replica)

    C->>CH: SET key value
    CH->>SW: ShardMessage::Execute (mpsc)
    SW->>SW: Execute write on store

    par Response to client
        SW-->>CH: Response (oneshot)
        CH->>C: +OK
    and Replicate
        SW->>RB: broadcast_command(command)
        RB->>PRH: ReplicationFrame (broadcast channel)
        PRH->>TCP: Stream frame bytes to replica
        TCP->>RRH: Receive frame bytes
        RRH->>RCE: ReplicationFrame (mpsc)
        RCE->>RSW: ShardMessage::Execute (mpsc)
        RSW->>RSW: Replay write on replica store
    end
```

**Initial sync:** Replica sends `PSYNC` → ConnectionHandler hands off TCP connection to `PrimaryReplicationHandler` → full RDB snapshot + WAL stream, or incremental WAL resume from offset.

---

## 8. Cluster Consensus (Raft)

Mutating cluster operations (CLUSTER MEET, ADDSLOTS, etc.) go through Raft consensus before being applied.

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ConnectionHandler
    participant SW as ShardWorker
    participant CR as ClusterRaft
    participant CSM as ClusterStateMachine
    participant CS as ClusterState

    C->>CH: CLUSTER MEET host port
    CH->>SW: ShardMessage::Execute (mpsc)
    SW-->>CH: Response::RaftNeeded { op: ClusterCommand } (oneshot)

    CH->>CR: Propose ClusterCommand::AddNode via Raft client
    CR->>CR: Raft consensus (leader replicates log)
    CR->>CSM: Apply committed ClusterCommand
    CSM->>CS: Mutate ClusterStateInner (nodes, slot_assignment)
    CR-->>CH: Commit result
    CH->>C: +OK
```

**Read-only cluster commands** (`CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER SLOTS`) read directly from the `ClusterState` `Arc<RwLock<...>>` without going through Raft.
