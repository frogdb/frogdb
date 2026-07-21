# Spec: architecture/clustering.md
Status: rewrite (the current page's central model — an external orchestrator
control plane — is wrong; the real control plane is internal Raft consensus)
Audiences: A3 (architecture-curious), A5 (contributors)

Goal: The reader understands FrogDB's cluster design: two-level sharding (CRC16
hash slots for cross-node routing, a second slot-derived split into internal
shards within a node), the **Raft-for-metadata + PSYNC-for-data** split (cluster
membership, slot ownership, failover, and config epochs go through an internal
Raft consensus group built on `openraft`; key-value data replicates over the
PSYNC/command-streaming path from architecture/replication), the config-epoch
conflict-resolution model, the routing decision a node makes per command
(`RouteDecision`), the read-only `/admin/*` observability endpoints, and the
`version_gate` mechanism that keeps binary features dormant during a rolling
upgrade until it is finalized. The reader leaves understanding *why* metadata is
strongly consistent (Raft) while data stays fast (async command replication),
and can map each concept to source.

Not in scope:
- Operator bootstrap/config, `CLUSTER` command usage, failure runbooks —
  Operations → Clustering (link, do not duplicate).
- The Kubernetes operator (`frogdb-operator`) — that is a deployment concern, and
  it is the one place an "orchestrator/admin-API" framing legitimately applies;
  do not conflate it with the topology control plane.
- Data-plane replication internals (checkpoint transfer, command streaming) —
  architecture/replication owns those; link.
- Slot migration data movement mechanics beyond the routing states.

Sources of truth (author MUST read; re-verify every claim — the current page is
substantially wrong, so do NOT reuse its assertions without source confirmation):
- `frogdb-server/crates/cluster/src/lib.rs` — the module doc comment is the
  canonical statement of the design. It explicitly says "Raft-based cluster
  coordination … Raft consensus for cluster metadata coordination, while data
  replication uses the existing PSYNC protocol," and carries a "What Raft
  Coordinates (Metadata Only)" table. Quote/paraphrase this faithfully.
  `ClusterRaft = Raft<TypeConfig>`.
- `frogdb-server/crates/cluster/src/network.rs` — Raft RPC set (AppendEntries,
  Vote, InstallSnapshot) over TCP; leader forwarding (`ForwardedWrite`).
- `frogdb-server/crates/cluster/src/storage.rs` — RocksDB-backed Raft log store.
- `frogdb-server/crates/cluster/src/state.rs` — `RaftStateMachine`, `config_epoch`.
- `frogdb-server/crates/cluster/src/commands.rs` — Raft commands: `IncrementEpoch`,
  HARD/SOFT reset, `FinalizeUpgrade`.
- `frogdb-server/crates/cluster/src/types.rs` — `NodeRole` (Primary/Replica only),
  `NodeInfo`, `NodeFlags`, `SlotRange`, `SlotMigration`/`MigrationState`,
  `ConfigEpoch`, `CLUSTER_SLOTS = 16384`, slot→node `BTreeMap` + `get_node_slots`.
- `frogdb-server/crates/cluster/src/version_gate.rs` — `VERSION_GATES`,
  `VersionGateEntry`, `is_gate_active`, `pending_gates`.
- `frogdb-server/crates/core/src/shard/helpers.rs` — `slot_for_key` (CRC16 XMODEM,
  hash-tag extraction, `% 16384`) and `shard_for_key` (`slot % num_shards`).
- `frogdb-server/crates/server/src/slot_migration/routing.rs` — `RouteDecision`
  enum (LocalServe / LocalServeMigrating / AcceptImporting / Moved / Unassigned).
- `frogdb-server/crates/server/src/observability_server.rs` — the actual `/admin/*`
  and `/health/*` routes.
- workspace `Cargo.toml` — `openraft = { version = "0.9", … }` (confirm; do not
  hardcode the version in prose — see S6).

Existing content: current `architecture/clustering.md`. The two-level sharding
diagram and the epoch concept survive; almost everything else needs correction.
FACTUAL DISCREPANCIES that MUST be fixed (verify each against source; cite in the
drift note, not in reader-facing prose):

1. **Control plane model is inverted.** The page describes an *external
   orchestrator* pushing topology via a POST admin API, with a "Orchestrated vs
   Gossip" rationale table. This is false. FrogDB uses an **internal Raft
   consensus group** (`openraft`) for topology/slot-ownership/failover/epoch. There
   is no external orchestrator and no gossip; topology changes are Raft
   `client_write`s committed to the replicated state machine, forwarded to the
   leader if needed. Rewrite the entire "Orchestrated Control Plane" section as
   "Raft-coordinated control plane." Replace the gossip-comparison table with the
   real "What Raft coordinates (metadata) vs what stays on PSYNC (data)" split
   from `lib.rs`. Remove the JSON "topology push" payload example unless a real
   equivalent exists in source.
2. **Internal-shard hash is wrong.** The page says `internal_shard =
   xxhash64(key) % num_shards`. Actual `shard_for_key` computes the CRC16 slot,
   then `slot % num_shards` — it is CRC16-derived, not xxhash64. (xxhash64 appears
   only in probabilistic data structures, never in key routing.) Fix the routing
   formula, the diagram caption, and the "Hash Tag Full Colocation" `route_key`
   snippet (no function named `route_key` exists; describe `slot_for_key` /
   `shard_for_key`). Colocation still holds because both levels derive from the
   same hash-tagged CRC16 slot — state that correctly.
3. **Slot-ownership enum is wrong.** No `SlotValidationResult` with
   NoKeys/Owned/CrossSlot/Redirect/Migrating/Importing exists. The real
   per-command routing enum is `RouteDecision` (LocalServe, LocalServeMigrating,
   AcceptImporting, `Moved { slot, owner, addr }`, `Unassigned { slot }`), with
   cross-slot/same-shard checks handled separately and migration lifecycle in
   `MigrationState { Initiated, Migrating, Completing }`. Replace the enum and its
   action table with the real types and the wire errors they map to
   (`-MOVED`, `-ASK`, `-TRYAGAIN`, `-CROSSSLOT`, `-CLUSTERDOWN`).
4. **Admin API table is wrong.** Endpoints live in `observability_server.rs` and
   are a **read-only observability plane**, not a control plane. `/admin/cluster`
   is **GET only** (there is no POST topology push); `/admin/replication` and
   `/admin/ready` do **not** exist (readiness is `/health/ready` and `/readyz`).
   Real routes also include `/admin/role`, `/admin/nodes`,
   `/admin/upgrade-status`, `POST /admin/shutdown`, `POST /admin/transfer-leader`.
   Rebuild the table from source and reframe it as observability/ops, cross-linking
   Operations → Debug UI & HTTP API. The "topology change" verbs belong to Raft
   commands, not HTTP.
5. Config epoch is correct in spirit; fix mechanics to source (`ConfigEpoch = u64`,
   `IncrementEpoch` Raft command increments, HARD reset zeroes, higher epoch wins).
6. Node roles: `NodeRole` has only `Primary`/`Replica` (Redis Display
   `master`/`slave`). No `Orchestrator` node type. The "Orchestrator" glossary
   term (see glossary spec) should be cut or redefined.

Structure (H2/H3 outline):

## Overview
- FrogDB runs single-node or clustered. Clustered adds a Raft consensus group for
  metadata and reuses the primary/replica data path for keys. State the design
  thesis up front: strongly consistent metadata (Raft) + fast async data
  (PSYNC). 16384 CRC16 hash slots for Redis-client compatibility.

## Two-level sharding
- Level 1 (cross-node): `slot = CRC16_XMODEM(hashtag(key)) % 16384` → owning node.
- Level 2 (within node): `internal_shard = slot % num_shards` → owning shard
  worker. Both derive from the same hash-tagged slot, so hash tags colocate keys
  at both levels. Keep the corrected two-level diagram. Note the frogctl client
  computes CRC16 independently (CCITT variant) — mention only if relevant to
  contributors reading both code paths.

## Raft-coordinated control plane
- What Raft owns (membership, slot ownership, failover, config epoch) vs what it
  does NOT (key data, PSYNC/WAL streaming, reads/writes, slot-migration data),
  straight from `lib.rs`. `openraft` foundation: log storage (RocksDB), network
  RPCs, state machine, leader forwarding. Contrast briefly with Redis Cluster
  gossip — but as a factual design difference, not the old marketing table.

## Config epochs
- `ConfigEpoch` u64, monotonic via `IncrementEpoch`, higher wins in conflict
  resolution; reset semantics (HARD zeroes, SOFT preserves). How a stale node
  detects it must re-sync metadata.

## Slot ownership and routing
- Per-command `RouteDecision` variants and the wire outcome each produces
  (`-MOVED`, `-ASK`, `-TRYAGAIN`, `-CROSSSLOT`, unassigned/`-CLUSTERDOWN`).
  Migration lifecycle states. Cross-slot rejection ties to
  architecture/consistency and the glossary.

## Node lifecycle and failover
- Join → Raft membership change; slot assignment; primary failure → Raft-driven
  failover (leader election / promotion) rather than an external decision. Demotion
  path. Verify the real lifecycle in source before writing; do not carry over the
  current page's orchestrator-driven steps.

## Rolling upgrades: version gating
- `version_gate.rs`: features declared with a `min_version` stay dormant until the
  cluster's finalized `active_version` reaches it (`FinalizeUpgrade` after all
  nodes report ≥ target). `pending_gates` visibility. Current real gate example:
  `extended_info_fields`. Cross-link Operations for the upgrade procedure.

## Observability endpoints
- The read-only `/admin/*` + `/health/*` routes and what they expose; link
  Operations → Debug UI & HTTP API and Reference → Metrics. Explicitly note these
  are observability, not the control plane.

Generated data: none embedded. `openraft` version, `num_shards`/slot constants,
and any version string come from source consts / `versions.json` (S6) — never
hardcode.

Drift guards:
- S7 code-path check must cover all `crates/cluster/...`,
  `core/src/shard/helpers.rs`, `server/src/slot_migration/routing.rs`,
  `server/src/observability_server.rs` paths cited.
- The `/admin/*` route table is hand-mirrored from `observability_server.rs`;
  flag for reviewers to re-diff on each edit (no generator today). Consider a
  follow-up check that greps the router for `/admin` routes and asserts parity
  with this page — note as S8-adjacent.
- Enum variant names (`RouteDecision`, `MigrationState`, `NodeRole`) must match
  source exactly; a rename must fail S7 or be caught in review.
- No hardcoded `openraft`/Redis/slot-count numbers in prose beyond the fixed 16384
  slot constant (which is a protocol invariant, safe to state).
