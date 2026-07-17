# Naming cleanup

Follow-up work from the 2026-07-17 domain-vocabulary interview that produced `CONTEXT-MAP.md`
and the per-context `CONTEXT.md` files. Each issue is an independent, mechanical-to-small
change aligning code and docs with the canonical vocabulary those files define.

Canonical decisions (see the CONTEXT.md files for full definitions):

- Warm-tier data movement is **spill / unspill** (not demote/promote).
- Node roles are **Primary/Replica**; "role demotion/promotion" for role changes.
- The support archive is a **Debug Bundle** (not "diagnostic bundle").
- Cluster metadata is owned by the embedded **Raft metadata plane**; the external
  "Orchestrator" design is superseded.
- frogctl: one concept, one home — `debug memory` owns bigkeys/memkeys, `backup` owns
  export/import.
