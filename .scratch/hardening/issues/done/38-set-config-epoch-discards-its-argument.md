# `CLUSTER SET-CONFIG-EPOCH` parses its argument, discards it, and bumps instead

Status: done
Type: bug (command semantics)
Severity: likelihood 2/3 (any operator or tool using the documented Redis command), consequence 2/3
(the command reports `+OK` for something it did not do; the resulting epoch is unpredictable) —
score 4
Area: cluster / `CLUSTER SET-CONFIG-EPOCH`

## Problem

The handler parses the requested epoch into `_epoch_num` — the leading underscore is in the source
— and then proposes a plain `ClusterCommand::IncrementEpoch`, which is `config_epoch += 1`. The
client is told `+OK`.

So `CLUSTER SET-CONFIG-EPOCH 42` on a cluster at epoch 7 yields epoch 8, and reports success. An
operator using the command for its documented purpose (manually resolving an epoch collision across
a freshly assembled cluster) gets an unrelated value and no indication anything went wrong.

## Redis comparison

`CLUSTER SET-CONFIG-EPOCH <n>` in Redis sets the node's `configEpoch` to exactly `n`, and is
refused unless the node's current epoch is 0 and the node knows no other nodes — it is a
bootstrap-only command precisely because an arbitrary epoch assignment is otherwise unsafe.

## Options

1. **Implement it.** A new `ClusterCommand::SetConfigEpoch { node_id, epoch }` arm with Redis'
   guards (node epoch is 0, cluster has no other members). Under FrogDB's Raft control plane the
   need is much weaker than in Redis — collisions are already resolved automatically and
   deterministically (FM-CLUSTER-011), which is the whole reason this was never finished.
2. **Refuse it.** `ERR SET-CONFIG-EPOCH is not supported; config epochs are assigned by the
   cluster` and a line in the spec's deviations table. Honest, and consistent with `CLUSTER
   BUMPEPOCH`, which is already unsupported for the same reason.

Option 2 is the better fit for the current design. Either way the present behavior — accept, ignore,
do something else, report success — is not defensible.

## Tests that should exist

- `set_config_epoch_sets_the_requested_value` (option 1), or
- `set_config_epoch_is_refused_with_an_explanation` (option 2)

## Spec impact

A new row under the config-epoch section (010..017), and the deviations table's
`CLUSTER SET-CONFIG-EPOCH` line is rewritten to describe the chosen behavior rather than the
current hole.

## Resolution

Implemented rather than refused (the issue listed both options and leaned toward refusing).
Implementing restores exact Redis parity, deletes a Redis-deviations row instead of rewording it,
and puts the guard logic inside `frogdb-cluster` where the mutation gate measures it — a refusal
would have lived in the un-gated server layer.

Redis' contract, matched verbatim (`clusterCommand`, `CLUSTER SET-CONFIG-EPOCH`): accepted only
while `dictSize(server.cluster->nodes) == 1` and `myself->configEpoch == 0`; it assigns *exactly*
the given epoch and ratchets `currentEpoch` up to it if it was lower. FrogDB adds one refusal Redis
cannot need — `NodeNotFound`, because the topology is replicated and the receiving node might not
be in it yet.

Wiring, end to end: `ClusterCommand::SetConfigEpoch { node_id, epoch }` (`cluster/src/types.rs`),
the state-machine arm with both guards plus the FM-CLUSTER-010 counter ratchet
(`cluster/src/commands.rs`), `RaftClusterOp::SetConfigEpoch` (`protocol/src/response.rs`), the arm
in `raft_op_to_command` (`server/src/connection/util.rs` — a total adapter, so the new variant was
a compile error until wired), and `cluster_set_config_epoch` now emitting the op with the parsed
value instead of `IncrementEpoch` (`server/src/commands/cluster/admin.rs`).

Forcing tests, all in `frogdb-cluster` `commands.rs`: `set_config_epoch_assigns_the_exact_value_requested`,
`set_config_epoch_never_lowers_the_cluster_counter`,
`set_config_epoch_refused_once_the_node_knows_another_node`,
`set_config_epoch_refused_once_the_node_holds_an_epoch`,
`set_config_epoch_on_an_unknown_node_is_not_found`. End-to-end witnesses in `cluster_topology.rs`:
`test_cluster_set_config_epoch_assigns_the_exact_value_on_a_lone_node` and
`test_cluster_set_config_epoch_refused_once_the_node_knows_a_peer`, which replace
`test_cluster_set_config_epoch_returns_ok` — that test asserted the bug (epoch merely "increased").

Spec: new row FM-CLUSTER-076, numbered next-free but filed with the config-epoch group; the
Redis-deviations row for `CLUSTER SET-CONFIG-EPOCH` now reads "Identical"; GAPS entry 9 deleted.
