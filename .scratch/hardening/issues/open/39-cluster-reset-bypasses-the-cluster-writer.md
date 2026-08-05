# `CLUSTER RESET` bypasses `ClusterWriter`, so a follower answers with a raw Raft error

Status: needs-triage
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
