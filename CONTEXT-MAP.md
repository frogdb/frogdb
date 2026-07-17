# Context Map

FrogDB is a multi-context repo. Each bounded context has its own `CONTEXT.md` glossary and may
have context-scoped ADRs under `<context>/docs/adr/`. Workspace-wide ADRs live in
[`docs/adr/`](./docs/adr/).

## Contexts

- [Server](./frogdb-server/CONTEXT.md) — the Redis-compatible database engine
  (`frogdb-server/`): protocol, commands, storage, persistence, replication, cluster, scripting,
  search. Most work lives here.
- [Operator](./frogdb-operator/CONTEXT.md) — the Kubernetes operator (`frogdb-operator/`) plus
  deployment tooling (`frogdb-server/ops/*`: Helm charts, deb packaging, `frogdb-admin`).
- [CLI](./frogctl/CONTEXT.md) — the `frogctl` command-line client for operating a running
  FrogDB.

## Relationships

- **Operator → Server (config schema, compile-time)**: the operator imports the server's
  `frogdb-config` crate and emits `frogdb.toml` through the server's own serde types, so any
  server-side config rename breaks the operator build instead of drifting silently. See
  [ADR-0001](./docs/adr/0001-operator-imports-server-config-crate.md).
- **Operator → Server (runtime)**: the operator's ConfigMap-mounted `frogdb.toml` and
  `FROGDB_<SECTION>__<KEY>` env overrides are consumed directly by the `frogdb-server` binary.
  Section/key vocabulary is the server's; the operator invents no config terms of its own except
  **Mode** (see the operator glossary).
- **CLI → Server**: `frogctl` talks to a node on three planes — the RESP **data plane**
  (port 6379), the **Admin API** (HTTP, port 6380), and the **Metrics API** (HTTP, port 9090).
  It normalizes the server's wire-compat vocabulary (`master`/`slave` in INFO fields) to the
  canonical **Primary**/**Replica** in all output.
- **Operator ∥ Helm chart**: two independent deployment paths exist for the server —
  **operator-managed** (CRD + reconciler) and **Helm-managed**
  (`frogdb-server/ops/helm/frogdb/`, no CRD). Both belong to the operator context.

## Shared vocabulary rules

- Node roles are **Primary**/**Replica** everywhere in code, docs, issues, and CLI output.
  `master`/`slave` appear only at the Redis wire-compat boundary (INFO fields, `NodeRole`
  Display) and must not leak into new prose.
- The support archive produced by the server is a **Debug Bundle** (config section
  `[debug-bundle]`). "Diagnostic bundle" is a deprecated alias.
