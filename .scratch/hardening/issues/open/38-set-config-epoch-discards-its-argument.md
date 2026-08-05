# `CLUSTER SET-CONFIG-EPOCH` parses its argument, discards it, and bumps instead

Status: needs-triage
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
