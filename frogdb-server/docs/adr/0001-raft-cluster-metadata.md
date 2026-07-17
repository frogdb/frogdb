# Embedded Raft for cluster metadata

Cluster topology (slot ownership, node roles, Config Epoch) needs a consistent owner. Redis
Cluster uses gossip; DragonflyDB uses an external orchestrator; FrogDB initially planned the
orchestrated model. We decided instead to embed Raft (openraft) in the nodes themselves: the
cluster self-coordinates metadata with no external control-plane service to deploy, and
metadata changes get real consensus rather than gossip convergence. The data path never goes
through Raft — consensus applies to metadata only.

## Consequences

- Cluster mode requires an odd member count (≥3) for quorum; the operator enforces this and
  derives PDB `minAvailable` from it.
- Older docs describing an external "Orchestrator" are superseded (see the server
  `CONTEXT.md` glossary).
