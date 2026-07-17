# clustering.md still describes the superseded external Orchestrator

Status: ready-for-agent

Decision (2026-07-17): embedded Raft (openraft) is the current cluster-metadata design; the
external Orchestrator model is superseded (see
`frogdb-server/docs/adr/0001-raft-cluster-metadata.md`). The website glossary was updated, but
`website/src/content/docs/architecture/clustering.md` (orchestrator sections, topology-push
flow) still describes the old model. Rewrite those sections around the Raft metadata plane,
Config Epoch, and node self-coordination. Check `consistency.md` and `architecture.md` for the
same drift.
