# Operator Context

Kubernetes operator for FrogDB (`frogdb-operator/`) plus deployment tooling
(`frogdb-server/ops/*`: Helm charts, deb packaging, `frogdb-admin`). Reconciles the `FrogDB`
custom resource into StatefulSets, Services, ConfigMaps, and PDBs. Config vocabulary is
single-sourced from the server — see
[ADR-0001](../docs/adr/0001-operator-imports-server-config-crate.md).

## Language

**FrogDB (CR)**:
The custom resource kind (`frogdb.io/v1alpha1`, plural `frogdbs`) describing one FrogDB
deployment in either mode.
_Avoid_: FrogDBCluster (no such kind)

**Mode**:
The CRD's deployment-topology switch (`spec.mode`): `standalone` or `cluster`. An
operator-invented abstraction over the server's independent `[cluster].enabled` +
`[replication].role` toggles.

**Standalone Mode**:
One StatefulSet where pod-0 is the **Primary** and higher ordinals are **Replicas** (async
replication) when `replicas > 1`.

**Cluster Mode**:
A Raft-based FrogDB cluster; `spec.replicas` is the member count (odd, ≥3 for quorum).

**Cluster Tuning Block**:
The `spec.cluster` field: Raft/bus knobs (`busPort`, election/heartbeat timings,
`autoFailover`). It is not "the cluster" — only its tuning.
_Avoid_: calling `spec.cluster` "the cluster spec" in prose

**Kubernetes cluster**:
Always spelled out in full. Bare "cluster" in this context means the FrogDB cluster.

**Primary / Replica**:
Data-plane node roles, same meaning as the server context.

**Raft Leader**:
The elected node of the cluster-metadata consensus. Never called "primary" — leadership moves
independently of data roles.

**Rolling Upgrade**:
The image-tag-driven pod-by-pod restart, tracked via `status.currentVersion` /
`targetVersion` / `upgradeInProgress` and the `Upgrading` condition.
_Avoid_: conflating with config rollouts (below)

**Config Hash Rollout**:
The restart triggered by the `frogdb.io/config-hash` pod-template annotation when generated
`frogdb.toml` changes. Not an upgrade; not tracked in upgrade status.

**Operator-managed / Helm-managed**:
The two deployment paths for FrogDB: via the CR + reconciler, or directly via the
`frogdb-server/ops/helm/frogdb/` chart (no CRD). Same server, different control planes.

**Debug Bundle**:
The server support archive, fetched by `frogdb-admin`. Canonical term per the context map;
`frogdb-admin`'s "diagnostic bundle" wording is a deprecated alias
(`.scratch/naming-cleanup/issues/07-debug-bundle-alias.md`).

## Relationships

- The **FrogDB (CR)** owns its StatefulSet, headless + client Services, ConfigMap, PDB, and
  optional ServiceMonitor via owner references.
- **Mode** gates which vocabulary applies: Standalone Mode uses **Primary**/**Replica**
  ordinal roles; Cluster Mode uses cluster members with a **Raft Leader**.
- `spec.replicas` drives StatefulSet size, the initial Raft peer list, and the PDB's
  quorum-derived `minAvailable` (`replicas/2 + 1` in Cluster Mode).
- `FrogDBConfigSpec` → generated `frogdb.toml` (via the server's `frogdb-config` types) →
  ConfigMap → mounted into the pod → consumed by the server binary; its hash drives **Config
  Hash Rollouts**.

## Example dialogue

> **Dev:** "After a failover in **Standalone Mode**, does `status.primaryPod` follow the new
> Primary?"
> **Domain expert:** "Yes — the reconciler probes each pod's replication role (`/admin/role`)
> and reports the pod that actually answers as Primary, so a promoted higher ordinal shows up
> correctly. If the probe can't reach the pods it leaves the field unset rather than guessing. In
> **Cluster Mode** the field is omitted entirely — there's no fixed primary; the **Raft Leader**
> moves independently (issue 02)."

## Flagged ambiguities

- "cluster" had five senses — resolved: **Kubernetes cluster** (always spelled out), **Cluster
  Mode** (`spec.mode`), **Cluster Tuning Block** (`spec.cluster`), the server's `[cluster]`
  TOML section (server vocabulary), and `ClusterTestHarness` (test-only).
- `status.primaryPod` used to hardcode the ordinal-0 pod, conflating it with the actual
  Primary — resolved (issue 02): the reconciler now probes each pod's replication role via
  `/admin/role` on the metrics port and reports the real Primary in **Standalone Mode**, leaving
  the field unset if the role can't be determined; in **Cluster Mode** the field is omitted
  entirely because Raft leadership has no fixed primary.
- "replica" is dual-purpose by mode (async-replication follower vs Raft member count) —
  resolved by always naming the mode when count semantics matter.
- `spec.upgrade.autoFinalize` was documented but dead (no reconcile path read it) — resolved
  (issue 04): `frogctl upgrade` drives finalization, so the entire `spec.upgrade` block
  (`UpgradeSpec`, including the also-unread `minUpgradeDelaySecs`) was removed from the CRD.
- Client port is named `frogdb` in the operator's StatefulSet but `redis` in the Helm chart —
  same port, unreconciled label naming.
