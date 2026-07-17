# CLI Context

`frogctl`: the command-line client for operating a running FrogDB. Every invocation is
one-shot — parse flags, run one command, exit. No REPL, no saved profiles; connection info is
per-invocation flags.

## Language

**Data Plane**:
The RESP connection (default port 6379) used for commands, scans, pub/sub, MONITOR.

**Admin API**:
The server's admin HTTP endpoint (default port 6380), reached via `--admin-url`.

**Metrics API**:
The server's metrics/health HTTP endpoint (default port 9090), reached via `--metrics-url`.
_Avoid_: "Observability API" (drifted doc-comment wording)

**Output Mode**:
The global rendering choice: `table` | `json` | `raw` (`--output`).

**Client** (subcommand):
Operates on *server-side* RESP connections (CLIENT LIST/KILL/PAUSE). Not frogctl's own
connection (that is the internal `ConnectionContext`) and not "frogctl the client".

**Fan-out**:
The `--all <addrs>` pattern where a command queries a list of nodes and merges results
(health, hotshards, slowlog).

**Cluster Topology**:
The slot/node ownership graph (`cluster topology`).
_Avoid_: bare "topology"

**Replication Topology**:
The Primary→Replica link tree (`replication topology`).
_Avoid_: bare "topology"

**Export Archive**:
The `backup export` artifact: a directory with `manifest.json` plus per-key data files.

**Primary / Replica**:
Same as the server context. frogctl remaps the wire's `master`/`slave` INFO values to
Primary/Replica in all rendered output.

### Canonical command homes

One concept, one home. The `data` namespace keeps only its own concepts:

- **bigkeys / memkeys** → `debug memory` (canonical)
- **export / import** → `backup` (canonical)
- `data keyspace`, `data pipe`, `data slot` → stay under `data` (not aliases)
- The four `data` alias commands (`bigkeys`, `memkeys`, `export`, `import`) are slated for
  removal — `.scratch/naming-cleanup/issues/03-frogctl-data-aliases.md`.

## Relationships

- Every command uses **GlobalOpts** to reach one node on up to three planes: **Data Plane**,
  **Admin API**, **Metrics API**.
- **Output Mode** is orthogonal to all commands: every result renders as table, JSON, or raw.
- Shared engines back multiple commands: the scan engine powers `scan`, `data keyspace`, and
  `debug memory bigkeys/memkeys`; the backup engine powers `backup export/import`.
- Most read-state commands (`health`, `stat`, `replication`, `client info`) parse the server's
  INFO output through one shared parser.

## Example dialogue

> **Dev:** "Does `frogctl client list` show who's connected through frogctl?"
> **Domain expert:** "It shows every RESP connection *the server* has — the **Client**
> subcommand always means server-side connections. frogctl's own link is invisible plumbing."

## Flagged ambiguities

- "client" is overloaded (server-side connection / the frogctl binary / the redis-rs builder) —
  resolved: the **Client** subcommand always means server-side connections; say "frogctl"
  for the tool itself.
- "topology" named two graphs — resolved: **Cluster Topology** vs **Replication Topology**.
- One error message says `frog debug vll` instead of `frogctl` — typo, tracked in
  `.scratch/naming-cleanup/issues/05-frog-binary-name-typo.md`.
