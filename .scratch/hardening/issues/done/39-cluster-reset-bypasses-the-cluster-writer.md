# `CLUSTER RESET` bypasses `ClusterWriter`, so a follower answers with a raw Raft error

Status: done
Type: bug (error surface)
Severity: likelihood 2/3 (any `CLUSTER RESET` that lands on a follower), consequence 1/3 (the
operation is refused with an unhelpful error instead of a redirect; nothing is corrupted) — score 3
Area: cluster / `CLUSTER RESET`

## Problem

Every other administrative `CLUSTER` subcommand proposes through `ClusterWriter::propose`, which
gives the three-way outcome specced as FM-CLUSTER-047/048/049: committed (with or without a
state-machine rejection), forwarded to the leader, or `REDIRECT <id> <addr>` / `CLUSTERDOWN No
leader available`.

`handle_reset_command` calls `raft.client_write(..)` directly. So on a follower it surfaces
openraft's `ForwardToLeader` as prose:

```
ERR Raft error: APIError(ForwardToLeader(ForwardToLeader { leader_id: Some(2), leader_node: .. }))
```

The operator gets a Rust `Debug` rendering of an internal error type instead of the `REDIRECT` every
sibling command produces, and `frogctl`/clients that follow redirects cannot follow this one.

`ResetCluster` is also the only `ClusterCommand` with no `ClusterWriter` call site at all, which is
why it silently missed the migration when the writer was introduced.

## Fix

Route it through `ClusterWriter::propose` like the others. The reset arm itself is infallible
(FM-CLUSTER-006), so the only new behavior is the redirect/forward handling the writer already
implements — no new error paths.

Watch for one wrinkle: `CLUSTER RESET HARD` re-keys this node inside replicated state and the caller
separately updates the local `self_node_id` atomic. On the `Proposed::Forwarded` path the leader
applied the entry, so the local side-effect ordering needs the same care as
FM-CLUSTER-048's "the follower must not perform the leader's side effects".

## Tests that should exist

- `reset_on_a_follower_yields_a_redirect_not_a_raft_error`
- `reset_forwarded_to_the_leader_reports_forwarded`

## Spec impact

FM-CLUSTER-006 gains a sentence stating that reset proposes through the writer like every other
admin command; the spec's GAPS entry 10 is deleted.

## Resolution

`handle_reset_command` now goes through the new `ClusterWriter::propose_reset`, so reset inherits
the same three-way outcome as every sibling admin command: committed, forwarded, or
`REDIRECT`/`CLUSTERDOWN`. `ClusterCommand::ResetCluster` no longer has a `raft.client_write` call
site anywhere.

The reset-specific bookkeeping moved into the writer rather than staying at the connection layer,
because that is what makes it testable without a live multi-node Raft. `propose_reset` snapshots the
peer list *before* proposing (a committed reset empties the topology, so afterwards there is nothing
left to forget), proposes, and returns `ResetProposed::{Applied { forget_nodes }, Rejected(err)}`.

The wrinkle the issue flagged is resolved in favour of "the identity update runs on the forwarded
path too". FM-CLUSTER-048's rule is about the *leader's* side effects (the voter-add); re-keying
`self_node_id` is this node's own bookkeeping, and the entry that re-keyed it was applied on the
leader either way. A follower that kept announcing its old id would contradict the replicated
topology it is about to receive. It does **not** run on `Redirect`/`Raft`/`Rejected` — nothing
landed there.

`network_factory.remove_node` stays at the connection layer; the cluster crate holds the factory
only behind the `LeaderForwarder` seam, and widening that seam to reach `remove_node` would couple
the writer to the concrete factory and cost the fakeability the tests depend on.

Forcing tests (`frogdb-cluster`, `writer.rs`, all fake-driven):
`reset_on_a_follower_yields_a_redirect_not_a_raft_error`,
`reset_forwarded_to_the_leader_reports_forwarded`,
`reset_committed_on_the_leader_keeps_a_soft_reset_identity`,
`reset_rejected_by_the_state_machine_changes_nothing_local`.

Spec: FM-CLUSTER-006's Observable/NOT-observable/Invariant/Outcome/Forced-by all extended; GAPS
entry 10 deleted (the list renumbered).
