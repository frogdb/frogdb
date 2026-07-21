---
title: "Clustering"
description: "Bootstrap a Raft-coordinated FrogDB cluster, understand slot ownership and the admin API, and know the failure modes."
---

A FrogDB cluster shards the keyspace across nodes using 16384 hash slots. Cluster *metadata* — membership, slot ownership, failover decisions, and config epochs — is coordinated by Raft consensus, while the *data* itself is replicated over the PSYNC data plane. This page covers the architecture split, slot routing, bootstrapping, topology management, the admin API, and failure modes. For the data-sync mechanics beneath the cluster, see [Replication](/operations/replication/).

## Architecture at a glance

FrogDB separates the control plane from the data plane. Raft (built on the openraft consensus library) is the single source of truth for cluster metadata; it is never on the request path for key reads and writes.

| Plane | Mechanism | Responsibilities |
|---|---|---|
| Control (metadata) | Raft consensus | Cluster membership, slot-ownership changes, failover decisions, config epochs. |
| Data | PSYNC / WAL streaming | Key-value replication, read/write execution, slot-data migration. |

This is the same division of labor as Redis Cluster's config-epoch metadata versus its data replication, but the metadata is agreed by Raft rather than gossip, which makes topology changes explicit and linearizable.

## Slots and key routing

Every key maps to one of 16384 hash slots using the Redis-compatible CRC16 (XMODEM) hash:

```
slot = CRC16(key) % 16384
```

Slots are owned by primary nodes. Internally, each node also routes keys to a thread-per-core shard, and that routing is derived from the *same* hash:

```
internal_shard = CRC16(key) % 16384 % num_shards
```

Because both levels start from `CRC16(key)`, a key's cluster slot and its internal shard are computed from the same value. (CRC16 is used for all key routing; the xxhash64 function appears only inside probabilistic data structures, never in slot or shard assignment.)

### Hash tags

A key containing `{tag}` hashes on the tag content only, so related keys colocate on the same slot — and, because both routing levels share the hash, on the same internal shard:

```bash
# Both keys hash on "user:123" and land in the same slot
SET {user:123}:profile "..."
SET {user:123}:settings "..."

# Guaranteed same-slot, so this multi-key op is allowed
MGET {user:123}:profile {user:123}:settings
```

Multi-key operations whose keys span more than one slot are rejected. See [Redis compatibility differences](/compatibility/overview/) for cross-slot behavior.

## Node roles

**Primary** — owns one or more slot ranges, accepts writes for those slots, streams changes to its replicas, and answers client requests for owned slots (or replies `-MOVED` to redirect a client to the owning node).

**Replica** — holds a full copy of its primary's dataset, serves reads only when the client opts in with `READONLY`, and is a candidate for promotion during failover.

## Bootstrapping a cluster

Cluster mode is off by default. Each node enables it under `[cluster]`, sets a `node-id`, exposes a `cluster-bus-addr` for Raft traffic, and lists the initial members in `initial-nodes`:

```toml
[cluster]
enabled = true
node-id = 1
cluster-bus-addr = "10.0.0.1:16379"
initial-nodes = [
  "10.0.0.1:16379",
  "10.0.0.2:16379",
  "10.0.0.3:16379",
]
```

`initial-nodes` seeds the member set. The node with the lowest node-id bootstraps the Raft cluster by calling `initialize`; this step is idempotent, so restarting a bootstrapped node does not re-bootstrap it. On first bootstrap that node auto-assigns all 16384 slots evenly across the initial primaries as contiguous ranges and replicates the assignment to followers through Raft.

Once the nodes are up, verify the cluster with:

```bash
redis-cli CLUSTER INFO
redis-cli CLUSTER NODES
redis-cli CLUSTER SHARDS
```

The `[cluster]` keys and defaults:

| Key | Default | Purpose |
|---|---|---|
| `enabled` | `false` | Enable cluster mode. |
| `node-id` | `0` | This node's ID (`0` auto-generates one). |
| `client-addr` | `""` | Client-facing address (defaults to the server bind/port). |
| `cluster-bus-addr` | `127.0.0.1:16379` | Raft/cluster-bus address (conventionally the client port + 10000). |
| `initial-nodes` | `[]` | Seed member cluster-bus addresses. |
| `data-dir` | `./frogdb-cluster` | Directory for Raft logs and snapshots. |
| `election-timeout-ms` | `1000` | Raft election timeout. |
| `heartbeat-interval-ms` | `250` | Raft heartbeat interval (must be below the election timeout). |
| `connect-timeout-ms` | `5000` | Cluster-bus connect timeout. |
| `request-timeout-ms` | `10000` | Cluster-bus RPC timeout. |
| `auto-failover` | `false` | Let the Raft leader promote a replica automatically. |
| `fail-threshold` | `5` | Consecutive failures before a node is marked failed. |
| `self-fence-on-quorum-loss` | `true` | Reject writes when this node cannot form a quorum. |
| `replica-priority` | `100` | Promotion preference during auto-failover (lower is preferred; `0` never promotes). |

See the [Configuration reference](/reference/configuration/) for the generated, authoritative list.

## Managing topology

`CLUSTER` inspection subcommands are served locally from the node's view of the Raft-replicated state. Topology-mutating subcommands are routed through Raft so the change is agreed and applied consistently across the cluster.

| Local inspection | Raft-routed mutation |
|---|---|
| `INFO`, `NODES`, `MYID`, `SLOTS`, `SHARDS`, `KEYSLOT`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `HELP` | `MEET`, `FORGET`, `ADDSLOTS`, `DELSLOTS`, `SETSLOT`, `REPLICATE`, `FAILOVER`, `RESET`, `SAVECONFIG`, `SET-CONFIG-EPOCH` |

## Admin API

The HTTP observability server (default port 9090) exposes read-only cluster and node administration endpoints. When `[http] token` is set, requests to the protected `/admin/*` and `/debug/*` paths must carry an `Authorization: Bearer <token>` header.

| Endpoint | Method | Notes |
|---|---|---|
| `/admin/health` | GET | Liveness and cluster-enabled state. |
| `/admin/cluster` | GET | Cluster state snapshot (read-only — it does not accept a topology push). |
| `/admin/role` | GET | This node's role. |
| `/admin/nodes` | GET | Known nodes. |
| `/admin/upgrade-status` | GET | Rolling-upgrade status. |
| `/admin/shutdown` | POST | Trigger a graceful shutdown. |
| `/admin/transfer-leader` | POST | **Not implemented** — returns an error (the current openraft version has no leadership-transfer API). |

A separate RESP-protocol admin listener can be enabled under `[admin]` (default port 6382, `bind` 127.0.0.1, disabled by default) for administrative commands over the Redis protocol.

```toml
[admin]
enabled = true
bind = "127.0.0.1"
port = 6382
```

Secure the admin surfaces with network isolation and, for the HTTP endpoints, an `[http] token`. See [Security](/operations/security/) for TLS and network-boundary guidance.

## Failover and failure modes

| Scenario | Behavior |
|---|---|
| Node failure | With `auto-failover = true`, the Raft leader promotes a replica after `fail-threshold` consecutive failures, choosing among candidates by `replica-priority`. With the default `auto-failover = false`, promotion is manual via `CLUSTER FAILOVER`. |
| Quorum loss | With `self-fence-on-quorum-loss = true` (default), a node that cannot form a quorum rejects writes with `CLUSTERDOWN` while continuing to serve reads, preventing split-brain divergence. |
| Raft leader loss | A standard openraft election runs, governed by `election-timeout-ms` and `heartbeat-interval-ms`. Explicit leadership transfer is not implemented. |

## Monitoring

Inspect cluster health and membership with `CLUSTER INFO`, `CLUSTER NODES`, and `CLUSTER SHARDS`; `CLUSTER INFO` reports the cluster state (`ok`/`fail`), the number of assigned slots, known nodes, and the config epoch. Exported Prometheus metrics are listed in the [Metrics reference](/reference/metrics/).

## See also

- [Replication](/operations/replication/) — the PSYNC data plane and write quorum.
- [Deployment](/operations/deployment/) — packaging and the Kubernetes operator status.
- [Security](/operations/security/) — admin-API authentication and TLS.
