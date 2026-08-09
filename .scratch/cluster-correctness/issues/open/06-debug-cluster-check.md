# 06 — DEBUG CLUSTER CHECK

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W5; exposure ruled in §8 D3 (always compiled, admin/DEBUG gated).

## What to build

`DEBUG CLUSTER CHECK`: admin/debug command running the issue-02 catalog against the
node's current `ClusterStateInner` and returning violations as a RESP array (empty =
clean). Always compiled — Jepsen runs release binaries — gated behind the existing
admin/DEBUG surface like its DEBUG siblings. Read-only.

Update the command docs (website) and any DEBUG command enumeration the docs generate.

## Acceptance criteria

- [ ] Command returns `id + detail` per violation, empty array when clean, in release
      builds
- [ ] Gated exactly as sibling DEBUG commands (no new auth surface)
- [ ] Integration test: inject a violating state via test hooks, command reports it
- [ ] Docs updated

## Blocked by

- Issue 02 (`.scratch/cluster-correctness/issues/`) — the catalog is what it runs.
