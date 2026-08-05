# Mixed-version warning logs the maximum node version under a name that reads as the majority version

Status: done
Type: AFK
Origin: frogdb-cluster mutation gap-fill, 2026-08-05
Severity: likelihood 2/3 (any mixed-version cluster), consequence 1/3 (log-accuracy only, no
state effect)
Area: Cluster / observability

## Problem

In `frogdb-server/crates/cluster/src/commands.rs`, the mixed-version warning computes the
*highest* version among the other versioned nodes but emits it under a `cluster_version`
field whose name reads as the cluster's consensus/majority version. Observability-accuracy
rule: a field whose name implies one aggregation must not silently carry another.

## Fix direction

Either rename the field to what it is (`max_peer_version` or similar) or compute the value
the name implies. The forcing test
`mixed_version_warning_compares_against_the_other_versioned_nodes` pins the current
comparison behavior — update it alongside.

Note: `frogdb-cluster` is LOCKED (gate 0.80) — spec-first if the change touches specced
behavior, and `just mutants-diff frogdb-cluster` before pushing.

## Resolution (2026-08-05)

Fixed. Renamed the field the mixed-version warning logs from `cluster_version` to
`max_peer_version` in `frogdb-server/crates/cluster/src/commands.rs`'s `AddNode` arm, along
with the local binding (`majority_version` → `max_peer_version`) and the comment that had
described it as "the majority version in the cluster" — it was always `Iterator::max()` over
the other versioned peers' version strings, never a majority or consensus computation.

**Spec first.** FM-CLUSTER-001's Invariant row in
`.scratch/hardening/specs/cluster-failure-modes.md` now names the field explicitly
(`max_peer_version`, not a name like `cluster_version` that would misread it as the cluster's
consensus version), so a future rename of this field is a spec change, not a silent drift.

**Forcing test.** `mixed_version_warning_compares_against_the_other_versioned_nodes`
(`commands.rs`) now asserts the event carries `max_peer_version` and does *not* carry
`cluster_version`, and is tagged `// FM-CLUSTER-001` and added to that row's `Forced by` list —
previously the test existed but wasn't wired into the spec index.

Verified: `just lint-failure-modes` (245 failure modes, 1203 test references/tags, OK),
`just check frogdb-cluster`, `just test frogdb-cluster` (202/202 passed), `just fmt
frogdb-cluster` (no changes), `just lint frogdb-cluster` (clean), `just mutants-diff
frogdb-cluster` (5 mutants tested: 2 caught, 3 unviable, 0 missed).
