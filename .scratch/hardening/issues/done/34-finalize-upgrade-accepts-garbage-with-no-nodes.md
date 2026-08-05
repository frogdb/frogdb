# `FinalizeUpgrade` on an empty cluster stores an unparseable version as `active_version`

Status: done
Type: bug (version gating)
Severity: likelihood 1/3 (needs a finalize against a cluster with zero members — bootstrap or
post-`RESET HARD`), consequence 2/3 (`active_version` becomes a string no gate can parse, so every
version gate is permanently inactive and the operation is documented irreversible) — score 3
Area: cluster / rolling upgrade

## Problem

`frogdb-server/crates/cluster/src/commands.rs:444-476` validates the *target* version only inside
the per-node loop:

```rust
ClusterCommand::FinalizeUpgrade { version } => {
    for node in state.nodes.values() {
        let node_v = Version::parse(&node.version).map_err(..)?;
        let target = Version::parse(version).map_err(..)?;   // inside the loop
        if node_v < target { return Err(..); }
    }
    state.active_version = Some(version.clone());
    Ok(ClusterResponse::Ok)
}
```

With `state.nodes` empty the loop body never runs, so the target is never parsed and
`active_version` is set to whatever string arrived — `"not-a-version"`, `""`, anything.

The consequence is not a panic. `is_gate_active_in` treats an unparseable `active_version` as
"inactive" (FM-CLUSTER-009, deliberately fail-closed), so every gated feature is silently off. And
`FinalizeUpgrade` is documented as irreversible, so there is no path back other than
`CLUSTER RESET HARD` or a second finalize with a valid version — the latter works today only
because the arm is idempotent by overwrite, which is itself accidental.

`test_finalize_upgrade_invalid_target_version` exists and passes, but it seeds nodes first, so it
exercises the in-loop parse and not this hole.

## Fix

Hoist the target parse above the loop, where it belongs — it is loop-invariant anyway, so this is
also the correct shape:

```rust
let target = Version::parse(version).map_err(|e| ClusterError::InvalidOperation(
    format!("invalid target version {version}: {e}")))?;
for node in state.nodes.values() { .. }
```

## Tests that should exist

- `finalize_upgrade_rejects_an_unparseable_target_with_no_nodes`
- `finalize_upgrade_with_no_nodes_accepts_a_valid_target` — the empty-cluster success path is
  legitimate (bootstrap) and must survive the fix.

## Spec impact

FM-CLUSTER-008's `NOT observable` gains "a target version that no gate can parse being accepted";
the two tests above join its `Forced by`.

## Resolution

Fixed in `commands.rs` (`FinalizeUpgrade` arm). `semver::Version::parse(&version)` is hoisted above
the per-node loop it was loop-invariant over, so an unparseable target is
`InvalidOperation("invalid target version '<v>': <err>")` regardless of how many nodes are
registered. Previously the parse lived inside `for (node_id, node) in &inner.nodes`, so an empty
(or single-node-only, pre-registration) topology skipped it entirely and stored the raw string as
`active_version` — which then silently disabled every version gate, because
`is_gate_active_in` treats an unparseable active version as "no version" and fails closed
(FM-CLUSTER-009).

Forcing tests: `finalize_upgrade_rejects_an_unparseable_target_with_no_nodes` (loops
`["not-a-version", "", "1", "v1.2.3"]`, asserts the refusal and that `active_version()` stays
`None`) and `finalize_upgrade_with_no_nodes_accepts_a_valid_target` (the empty topology is still a
legal fast path — it must accept, not refuse). Both in `frogdb-cluster`, `state.rs`.

Spec: FM-CLUSTER-008's Observable / NOT observable / Invariant updated; `Forced by` gained both
tests.
