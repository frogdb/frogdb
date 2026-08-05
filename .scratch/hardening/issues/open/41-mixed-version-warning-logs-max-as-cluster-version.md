# Mixed-version warning logs the maximum node version under a name that reads as the majority version

Status: needs-triage
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
