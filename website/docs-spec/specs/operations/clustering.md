# Spec: operations/clustering.md

Status: rewrite (near-total — the current page describes a control plane that does
not exist in the source)
Audiences: A4 (primary), A3, A2

Goal: The reader can bootstrap a Raft-coordinated FrogDB cluster, understands
that cluster *metadata* (membership, slot ownership, failover, config epochs) is
managed by Raft consensus while *data* is replicated over the PSYNC data plane,
knows how slots map to nodes, which `CLUSTER` subcommands and admin endpoints
exist, and what happens under node failure, leader loss, and quorum loss.

> CRITICAL FOR THE AUTHOR: the current `operations/clustering.md` is pervasively
> **stale**. It describes an "external orchestrator" that pushes JSON topology to
> an admin API. **No orchestrator exists anywhere in the source.** Do not carry
> any orchestrator content forward. Verify every sentence against the sources
> below. When in doubt, cut.

Not in scope:
- Deep Raft internals / log replication mechanics — that is Architecture →
  Clustering (which is *also* stale and needs the same rewrite; flag it, don't
  fix it here).
- Replica data-sync mechanics (PSYNC, checkpoints) — that is
  [Replication](/operations/replication/); link, don't duplicate.
- Client-side cluster routing / MOVED-ASK behavior beyond a one-line mention.

Sources of truth (read before writing):
- `frogdb-server/crates/cluster/src/lib.rs` — module doc comment (lines ~1–43)
  is authoritative: "Raft-based cluster coordination … data replication uses the
  existing PSYNC protocol." Includes a "what Raft coordinates (metadata only)"
  table. `ClusterRaft = Raft<TypeConfig>` (~64–68). Uses **openraft 0.9**
  (`Cargo.toml` ~154–155).
- `frogdb-server/crates/config/src/cluster.rs` — every `[cluster]` config key +
  defaults.
- `frogdb-server/crates/config/src/admin.rs` — the RESP `[admin]` listener
  (`enabled` default false, `port` default **6382**, `bind` 127.0.0.1).
- `frogdb-server/crates/server/src/server/cluster_init.rs` — bootstrap:
  `initial-nodes` seeds membership; the lowest node-id bootstraps and calls
  `raft.initialize`; the bootstrap node auto-assigns all 16384 slots evenly and
  replicates via Raft.
- `frogdb-server/crates/server/src/commands/cluster/mod.rs` — implemented
  `CLUSTER` subcommands (inspection vs Raft-routed mutation).
- `frogdb-server/crates/server/src/observability_server.rs` (~233–259) and
  `.../server/src/admin/handlers.rs` — the real `/admin/*` HTTP endpoints and
  bearer-token middleware.
- `frogdb-server/crates/core/src/shard/helpers.rs` — `slot_for_key` and
  `shard_for_key`, both CRC16-based; hash-tag extraction.
- `config/src/cluster.rs` fields: `self_fence_on_quorum_loss`, `auto_failover`,
  `fail_threshold`, `replica_priority` (failure-mode behavior).

Existing content: `operations/clustering.md` (rewrite; keep only the correct
skeleton — 16384 slots, CRC16, hash tags, node roles, slot colocation — and
replace the entire control-plane half). `operations/kubernetes.mdx` is being cut;
its cluster pointers land in [Deployment](/operations/deployment/).

## Verified facts (authoritative — supersede the current page)

**Control plane = Raft (openraft 0.9).** Raft coordinates, per the lib.rs table:
cluster membership, slot-ownership changes, failover decisions, config epochs.
NOT via Raft (the data plane): key-value replication, PSYNC/WAL streaming,
read/write ops, slot-data migration.

**Slot routing — both levels are CRC16, NOT dual-hash.**
- Cluster slot: `slot = CRC16(key) % 16384` (`helpers.rs` ~62–65).
- Internal shard: `CRC16(key) % 16384 % num_shards` (`helpers.rs` ~55–59).
- Hash tags `{tag}` extracted at `helpers.rs` ~34–39, shared by both — so a hash
  tag colocates at both levels.
- Note: xxhash64 is used ONLY inside probabilistic types (bloom/cuckoo/cms/topk),
  never in key routing. Both routing levels are CRC16 — do not write a "xxhash64
  shard routing vs CRC16 slots" dual-hash split; it is inaccurate. Write the page
  from the source.

**Bootstrap** (`cluster_init.rs`): `[cluster] initial-nodes` seeds the member
set; the node with the lowest node-id bootstraps (idempotent/restart-safe) by
calling `raft.initialize`; that node auto-assigns 16384 slots evenly across
initial primaries and replicates the assignment through Raft to followers.

**`[cluster]` config keys (real):** `enabled` (default false), `node-id`,
`client-addr`, `cluster-bus-addr` (default `127.0.0.1:16379`), `initial-nodes`,
`data-dir`, `election-timeout-ms` (1000), `heartbeat-interval-ms` (250),
`connect-timeout-ms`, `request-timeout-ms`, `auto-failover` (**false**),
`fail-threshold` (5), `self-fence-on-quorum-loss` (**true**), `replica-priority`
(100; 0 = never promote). The current page's `orchestrator-contact-timeout-ms`,
`topology-refresh-interval-ms`, and `[cluster.admin]` block **do not exist** —
delete them.

**CLUSTER subcommands implemented** (`commands/cluster/mod.rs`):
- Inspection (served locally): `INFO`, `NODES`, `MYID`, `SLOTS`, `SHARDS`,
  `KEYSLOT`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `HELP`.
- Topology-mutating (routed through Raft): `MEET`, `FORGET`, `ADDSLOTS`,
  `DELSLOTS`, `SETSLOT`, `REPLICATE`, `FAILOVER`, `RESET`, `SAVECONFIG`,
  `SET-CONFIG-EPOCH`.

**Admin HTTP API — real endpoints** (on the `[http]` observability server,
default port 9090, bearer-token-protected when `http.token` set): `GET
/admin/health`, `GET /admin/cluster` (**read-only** cluster state — not a
topology push), `GET /admin/role`, `GET /admin/nodes`, `GET
/admin/upgrade-status`, `POST /admin/shutdown`, `POST /admin/transfer-leader`
(**returns not-implemented** — openraft 0.9 lacks the API; state this honestly).
Separately, a RESP admin listener exists via `[admin]` (default port **6382**,
disabled by default). There is NO admin listener on port 6380 (6380 is the
default `tls-port` — a real RESP TLS port documented in Security — not an admin
port) and NO `POST /admin/cluster`.

**Failure modes (real):**
- Node failure → if `auto-failover=true`, the Raft leader promotes a replica
  after `fail-threshold` consecutive failures, ordered by `replica-priority`.
  Default is manual (`auto-failover=false`).
- Quorum loss → with `self-fence-on-quorum-loss=true` (default) the node rejects
  writes with **CLUSTERDOWN** while keeping reads available (split-brain
  prevention).
- Raft leader loss → standard openraft election
  (`election-timeout-ms`/`heartbeat-interval-ms`); leadership transfer is not
  implemented.

Structure (H2/H3 — one line each):

- Intro: FrogDB clusters shard the keyspace across nodes using 16384 hash slots,
  with cluster metadata coordinated by Raft consensus and data replicated over
  PSYNC. One sentence pointing readers to Replication for the data-sync side.
- **## Architecture at a glance** — the metadata-plane (Raft) vs data-plane
  (PSYNC) split, as a short table sourced from the lib.rs doc comment. This
  replaces the entire "Orchestrated vs Gossip" section.
- **## Slots and key routing** — `slot = CRC16(key) % 16384`; slots are owned by
  primaries; internal shard routing is also CRC16-derived so hash tags colocate
  at both levels. Hash-tag `{tag}` example (keep the current page's example, it's
  correct). One line: cross-slot multi-key ops are rejected (link Compatibility).
- **## Node roles** — Primary (owns slot ranges, accepts writes, streams to
  replicas, replies or `-MOVED`) and Replica (full copy, read-only unless
  `READONLY`, failover candidate). Keep the current page's role bullets; they are
  correct.
- **## Bootstrapping a cluster** — `[cluster] enabled=true`, `node-id`,
  `cluster-bus-addr`, `initial-nodes`; lowest node-id bootstraps and auto-assigns
  slots; restart-safe. A minimal 3-node `initial-nodes` config example. Then
  bring nodes up and verify with `CLUSTER INFO` / `CLUSTER NODES` / `CLUSTER
  SHARDS`.
- **## Managing topology** — `CLUSTER` mutation subcommands go through Raft
  (MEET/ADDSLOTS/SETSLOT/REPLICATE/FAILOVER…); inspection subcommands are local.
  Present the two lists.
- **## Admin API** — the real `GET /admin/*` endpoints on port 9090 (bearer
  token) plus the optional RESP `[admin]` listener (port 6382, off by default).
  Be explicit that `/admin/cluster` is read-only and `transfer-leader` is not
  implemented. Secure via network isolation + `http.token`; link Security.
- **## Failover and failure modes** — a table: node failure (auto vs manual per
  `auto-failover`, `fail-threshold`, `replica-priority`), quorum loss
  (CLUSTERDOWN self-fence, reads stay up), leader loss (election). Replaces the
  bogus "Orchestrator Failure Modes" table.
- **## Monitoring** — `CLUSTER INFO`, `CLUSTER NODES`, config-epoch tracking.
  Cluster metric names are UNVERIFIED — do NOT copy
  `frogdb_cluster_last_orchestrator_contact` (fabricated); either verify real
  cluster metric names against the metrics registry / Metrics reference at write
  time or omit the metric list and link to Metrics reference.
- **## See also** — Replication, Deployment (Helm/K8s), Security (admin auth).

Generated data:
- Links only: Reference → Configuration reference (for `[cluster]`/`[admin]`
  fields, via `config-reference.json`), Reference → Metrics (for any cluster
  metrics), Compatibility → Command matrix (`compatibility/command-matrix`, CLUSTER
  family, via `commands.json` / S1).
- No generated component is embedded on this page; it is prose + verified config
  snippets.

Drift guards:
- `just docs-gen-check` keeps the linked `[cluster]`/`[admin]` config reference
  honest against `config/src/`.
- Code-path check (S7) — every `crates/...` path this spec cites (cluster,
  cluster_init, observability_server, helpers.rs) must exist; keeps the file
  references from rotting.
- The CLUSTER subcommand list should come from / be cross-checked against the
  generated command matrix (S1/S2) rather than hand-maintained.
- Content policy: no orchestrator language, no fabricated metric names, no
  latency/throughput numbers. If a fact cannot be traced to source, it is cut.
- FOLLOW-UP the author should flag (not fix): `architecture/clustering.md`,
  `operations/security.md` (ACL-sync via orchestrator), `backup-restore.md`
  (orchestrator promotion), and `architecture/glossary.md` all still reference
  the nonexistent orchestrator and need the same correction.
