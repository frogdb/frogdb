# Jepsen Troubleshooting Guide

## Docker Issues

### Stale containers from previous runs

**Symptoms**: Connection refused, wrong cluster state, tests fail immediately.

**Fix**:
```bash
just jepsen-down
docker ps -a | grep frogdb            # Verify all stopped
just jepsen-up <topology>             # Fresh start
```

If containers won't stop:
```bash
docker compose -f testing/jepsen/docker-compose.yml down -v
docker compose -f testing/jepsen/frogdb/docker-compose.replication.yml down -v
docker compose -f testing/jepsen/frogdb/docker-compose.raft-cluster.yml down -v
```

### Port conflicts

**Symptoms**: `Bind for 0.0.0.0:16379 failed: port is already allocated`

**Cause**: Another process or stale container is using the port.

**Fix**:
```bash
lsof -i :16379                        # Find what's using the port
just jepsen-down                      # Stop Jepsen containers
# Or kill the conflicting process
```

Parallel mode base ports (from `run.py`):
- Single: 16379
- Replication: 17379
- Raft: 18379

### Docker image is stale

**Symptoms**: Tests pass but test behavior doesn't reflect recent code changes.

**Fix**:
```bash
just jepsen-image-info                 # Check image build time/git hash
just docker-build-debug                # Rebuild
```

The `--build` flag on `run.py` auto-detects staleness via `/tmp/frogdb-jepsen-build-stamp`
(SHA256 of all Rust source files and Cargo manifests).

### Health check failures

**Symptoms**: `docker compose --wait` hangs or times out.

**Cause**: FrogDB server crashes on startup. Check container logs:
```bash
docker logs frogdb-single-n1          # Single topology
docker logs frogdb-repl-n1            # Replication topology
docker logs frogdb-raft-n1            # Raft topology
```

Container naming pattern: `frogdb-{topology}-{node}` where topology is `single`, `repl`, or `raft`.

## Cluster Convergence Issues

### Cluster state never reaches "ok" (90s timeout)

**Symptoms**: `Timed out waiting for cluster-state=ok`

**Possible causes**:
1. **Stale Raft state** — Containers have data from previous runs
   - Fix: `just jepsen-down` + `just jepsen-up raft` (volumes are removed on down)
   - `cluster_db.clj` also wipes `/data/raft` and `/data/cluster` on convergence failure
2. **Not enough nodes** — Raft requires a quorum (default initial nodes: 3)
   - Check `FROGDB_CLUSTER__INITIAL_NODES` in docker-compose.raft-cluster.yml
3. **Network issues** — Nodes can't reach each other on 172.21.0.0/16
   - `docker exec frogdb-raft-n1 ping 172.21.0.3`

### Leader election timeout (30s)

**Symptoms**: `Timed out waiting for leader election`

**Common causes**:
- Too few nodes for quorum (need majority of initial nodes)
- Stale Raft log prevents clean election
- Network partitions left from previous nemesis

**Debug**:
```bash
docker exec frogdb-raft-n1 redis-cli -p 6379 CLUSTER INFO
# Look for: cluster_state, cluster_current_epoch, cluster_my_epoch
```

### Slot assignment failures

**Symptoms**: `Not all slots are assigned` or operations fail with `CLUSTERDOWN`

16384 slots are distributed evenly across masters in batches of 1000.

**Debug**:
```bash
docker exec frogdb-raft-n1 redis-cli -p 6379 CLUSTER NODES
# Each master line shows slot ranges: e.g., "0-5460"
```

### Multi-leader detection

**Symptoms**: Multiple nodes claim to be master for the same slots

This indicates a Raft consistency bug. Collect:
1. `CLUSTER NODES` output from all 5 nodes
2. Full Jepsen log (`jepsen.log`)
3. Container logs for all nodes

## Checker Interpretation

### Elle anomalies

Elle detects cycles in transaction dependency graphs. Common anomaly types:

| Anomaly | Meaning | Severity |
|---------|---------|----------|
| `G0` | Write cycle — two txns each overwrote the other | Critical |
| `G1a` | Aborted read — read a value from an aborted txn | Critical |
| `G1b` | Intermediate read — read intermediate state | Critical |
| `G1c` | Circular information flow — write-read cycle | Critical |
| `G2` | Anti-dependency cycle | Violates serializability |
| `G-single` | Single anti-dependency | Violates snapshot isolation |
| `G-nonadjacent` | Non-adjacent anti-dependency | Violates snapshot isolation |

**Reading Elle output**:
- `:anomalies {:G0 [...]}` — List of specific cycle instances
- `:not #{:strict-serializable}` — Which consistency model was violated
- `:also-not #{:serializable}` — Additional models also violated
- `:cycle-search-timeout true` — Elle ran out of time checking (NOT a failure)

### Knossos linearizability failures

**Symptoms**: `results.edn` has `:valid? false` with `:model` and `:final-paths`

**Reading the output**:
- `:model` shows the expected register state
- `:final-paths` shows the operation sequence that couldn't be linearized
- Look for concurrent operations where a read saw a stale value

**False positives**: Very short time limits (< 10s) can cause Knossos to timeout and
report `:valid? :unknown` — this is not a failure. Increase `--time-limit`.

### Counter mismatches

**Symptoms**: `:expected 150, :actual 147`

**Meaning**: 3 successful INCRBY operations were lost (not persisted or replicated).

**Investigation**:
1. Check `history.txt` for `:ok` adds that may not have been durable
2. Look for crash/restart events in `jepsen.log` near the time of lost operations
3. Check if the counter was read before all crash recovery completed

## Network Issues

### iptables rules not cleaned up

**Symptoms**: Nodes can't communicate after a partition test, even after heal.

**Fix** (inside containers):
```bash
docker exec frogdb-raft-n1 iptables -F
docker exec frogdb-raft-n1 iptables -X
# Repeat for all nodes
```

Normally `nemesis.clj`'s `heal-all!()` flushes rules, but if a test crashes mid-partition
the rules may persist.

### Partition heal failures

**Symptoms**: `heal-all!` runs but nodes still can't communicate.

**Cause**: The container may have been restarted (losing iptables state) while other
containers still have blocking rules.

**Fix**: Flush iptables on ALL nodes, not just the one that was restarted.

## JVM Issues

### AWT hang on macOS

**Symptoms**: Test hangs indefinitely after analysis phase (during Elle cycle visualization).

**Cause**: Elle's `rhizome.viz` calls `Toolkit.getDefaultToolkit()` which blocks on XPC
connection to hiservices on macOS without a display.

**Fix**: Already applied in `project.clj`:
```clojure
:jvm-opts ["-Djava.awt.headless=true"]
```

If this line is missing or overridden, the hang will return.

### OutOfMemoryError

**Symptoms**: `java.lang.OutOfMemoryError: Java heap space` during analysis.

**Cause**: Large history files exceed default heap size.

**Fix**: Add to `project.clj`:
```clojure
:jvm-opts ["-Djava.awt.headless=true" "-Xmx4g"]
```

### Leiningen dependency resolution

**Symptoms**: `Could not find artifact` or dependency conflicts.

**Fix**:
```bash
cd testing/jepsen/frogdb
lein deps                              # Download dependencies
lein clean                             # Clean compiled classes
```

## Common Red Herrings

### High `:info` count in stats

`:info` means the operation's outcome is **indeterminate** — the client couldn't confirm
success or failure (usually due to timeout or connection loss). This is normal under
nemesis. Only `:fail` with unexpected values indicates a real problem.

### "Connection refused" during nemesis

Expected when a node is killed or partitioned. The client should handle this gracefully
(via `with-error-handling`) and return `:info` or `:fail`.

### Timeout on final reads

If the server is still recovering when final reads happen, some reads may timeout.
This is usually benign — check whether the checker had enough successful final reads
to verify correctness.

### CLUSTERDOWN during slot migration

Normal and expected. The cluster briefly enters a "fail" state during slot migration.
`with-clusterdown-retry` (in `client.clj`) handles this by retrying with backoff.

### Stale slot mapping after failover

After a leader election or failover, the client's cached slot→node mapping may be stale.
MOVED redirects update the mapping. This causes a brief spike in `:info` results but
is self-correcting.
