# 03 — Post-nemesis leak checks in final analysis

Status: needs-triage

## Parent

[PRD](../../PRD.md) W1.

## What to build

Extend the final-analysis sweep to call the existing introspection commands after the last
nemesis recovery: `MEMORY-CHECK`, `EXPIRY-INDEX-CHECK`, `LOCKTABLE`, `WAITQUEUE`. The claim
checked is "no fault leaves residue": no locks still held, no waiters stranded, no expiry
index divergence, no memory-accounting drift once the cluster has quiesced. Reuse the
surface-map pattern from `invariant.clj` (replication-correctness issue 13) so each check
reports under its own result key and known-benign residues can be allowlisted narrowly with
an issue citation.

## Acceptance criteria

- [ ] All four checks run at final analysis on multi-node workloads, non-gating
      connectivity errors, gating residue findings
- [ ] Seeded-violation evidence: at least one check proven to fail the analysis when its
      command reports residue (throwaway seed, reverted, absent from tree)
- [ ] Clean run evidence: store id with all four checks green after a nemesis schedule
- [ ] Any real residue found files an issue in the owning campaign's tracker

## Blocked by

None — independent of issue 01 (these are checks, not faults).
