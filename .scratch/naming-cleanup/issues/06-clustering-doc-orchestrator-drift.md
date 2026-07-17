# clustering.md still describes the superseded external Orchestrator

Status: done

Decision (2026-07-17): embedded Raft (openraft) is the current cluster-metadata design; the
external Orchestrator model is superseded (see
`frogdb-server/docs/adr/0001-raft-cluster-metadata.md`). The website glossary was updated, but
`website/src/content/docs/architecture/clustering.md` (orchestrator sections, topology-push
flow) still describes the old model. Rewrite those sections around the Raft metadata plane,
Config Epoch, and node self-coordination. Check `consistency.md` and `architecture.md` for the
same drift.

## Comments

Resolved (2026-07-17). All claims re-grounded against `frogdb-cluster` (`lib.rs`, `state.rs`,
`types.rs`, `commands.rs`), `frogdb-server` (`slot_migration/routing.rs`, `failure_detector.rs`,
`replication_quorum.rs`), and `frogdb-core` (`shard/partition.rs`), plus ADR 0001.

**`website/src/content/docs/architecture/clustering.md`** — rewritten:
- Frontmatter `description` + intro: "orchestrator protocol" -> Raft metadata plane / Config Epoch.
- Intro bullets + new grounding paragraph: Orchestrated control plane -> embedded Raft; data path
  never goes through Raft; ADR 0001 supersedes the Orchestrator design.
- Terminology table: dropped the **Orchestrator** row; added **Raft Metadata Plane** and
  **Config Epoch**; "Slot" -> "Hash Slot".
- "Orchestrated Control Plane" section fully replaced by **Raft Metadata Plane** (what Raft owns,
  rationale vs gossip, odd-quorum requirement, `ClusterCommand` -> `client_write` -> quorum-commit
  -> `apply_command` propagation, `apply_local` bootstrap path, `DemotionEvent` /
  `SlotMigrationCompleteEvent`).
- "Topology Push Flow" (JSON admin-push) removed; replaced by "How a Topology Change Propagates".
- "Epoch System" -> **Config Epoch** (cluster-wide `config_epoch` + per-node `NodeInfo.config_epoch`,
  same-transition bump, successor claims epoch).
- "Slot Ownership Validation": fictional `SlotValidationResult` enum replaced with the real
  `RouteDecision` (`LocalServe` / `LocalServeMigrating` / `AcceptImporting` / `Moved` / `Unassigned`)
  and real wire codes (MOVED/ASK/CROSSSLOT/CLUSTERDOWN); CROSSSLOT via `SlotValidator::same_slot`.
- New **Slot Migration** section: two-point ownership swap (`BeginSlotMigration` /
  `CompleteSlotMigration` / `CancelSlotMigration`), MIGRATING/IMPORTING derived by node-id compare,
  keys transfer over the data path.
- Node Lifecycle/Demotion (fictional `handle_demotion`/`new_topology`) replaced by **Failover**:
  leader-only TCP failure detection, `MarkNodeFailed`, composite atomic `Failover` command
  (force vs graceful), lag-based successor scoring.
- Node-to-Node Communication: added the Raft cluster bus + failure-probe rows (previous text
  claimed nodes only connect for replication/migration).
- "Admin API" (`/admin/cluster` topology POST/GET) replaced by **HTTP Endpoints** grounded in real
  routes (`/health/live`, `/health/ready`, `/healthz`, `/admin/upgrade-status`).
- Config Homogeneity + Two-Level Sharding: corrected `xxhash64(tag) % num_shards` to the real
  `CRC16(key) % 16384 % num_shards`.

**`website/src/content/docs/architecture/consistency.md`** — Split-Brain Window section: "receives
demotion topology" and "Client-side epoch validation" / fabricated `fencing_timeout_ms` replaced
with the real self-fencing (quorum-loss `-CLUSTERDOWN` via `ReplicationQuorumChecker`) + Config
Epoch demotion mechanism.

**`architecture.md`** — no change needed: its only "orchestration" mentions are the `frogdb-server`
crate's generic "I/O and concurrency orchestration", not the cluster control plane.

### Contradictions found / how handled
- Doc's `SlotValidationResult` enum does not exist in code; the real type is `RouteDecision`
  (`slot_migration/routing.rs`). Rewrote to match code.
- Doc's internal-shard hash was `xxhash64 % num_shards`; code (`shard/partition.rs`) uses
  `CRC16(key) % 16384 % num_shards`. `xxhash64` appears in the codebase only for CMS/Cuckoo
  probabilistic structures, never for shard routing. Corrected in clustering.md.
- **Out-of-scope drift, NOT fixed (flagged for follow-up):** the same wrong `xxhash64` internal-shard
  formula still appears in `storage.md`, `glossary.md`, `architecture.md`, and `concurrency.md`.
  Left untouched to keep this issue scoped; recommend a separate sweep to correct all four to CRC16.
- `fencing_timeout_ms` (consistency.md) is not a real config key; only self-fencing via replica
  quorum freshness exists. Rewrote rather than cite the nonexistent knob.
- ADR 0001 lives outside the website content tree, so it is referenced by name/path (no broken
  relative link).
