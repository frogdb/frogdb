# `CLUSTER INFO` gossip counters are hardcoded `0`

Status: needs-triage
Type: bug (observability accuracy)
Severity: likelihood 3/3 (every `CLUSTER INFO` on every clustered node reports all seven),
consequence 1/3 (an operator diagnosing a cluster-bus problem reads seven confident zeros) —
score 4
Area: cluster / `CLUSTER INFO`

## Problem

`ClusterInfoReport::render()` (`frogdb-server/crates/server/src/commands/cluster/mod.rs`) emits
these as literals with no source behind any of them:

```
cluster_stats_messages_ping_sent:0
cluster_stats_messages_pong_sent:0
cluster_stats_messages_sent:0
cluster_stats_messages_ping_received:0
cluster_stats_messages_pong_received:0
cluster_stats_messages_received:0
total_cluster_links_buffer_limit_exceeded:0
```

This is the cluster analogue of issue 29 (`total_net_repl_*_bytes`) and the same principle applies:
per `feedback_observability_accuracy`, missing data beats misleading data. Seven zeros on a cluster
that has been exchanging bus traffic for a week tell an operator the bus is idle.

The mitigating difference from issue 29: FrogDB has **no gossip protocol at all**, so `ping`/`pong`
are not merely uncounted, they do not exist. Reporting `0` for a message type that is never sent is
arguably honest. The `_sent`/`_received` totals and `total_cluster_links_buffer_limit_exceeded` are
not — real bus traffic (Raft RPCs, forwarded writes, pub/sub fan-out, health probes) does flow, and
those counters are where an operator would look for it.

`CLUSTER NODES`' `ping-sent`/`pong-recv` fields (always `0 0`) are the same class, but are
positional in a line format every client parses, so they cannot be dropped. They are pinned by
FM-CLUSTER-071 and listed in the spec's deviations table.

## Options

1. **Count real bus traffic.** Two `AtomicU64`s on the bus (`cluster-runtime/src/bus.rs`) plus two
   on the network factory's client side, published through the same handle `CLUSTER INFO` already
   reads. Feeds `_sent`/`_received` honestly; `ping`/`pong` stay `0` and move into the deviations
   table as "no gossip protocol".
2. **Drop the four fields with no source** (`ping_sent`, `pong_sent`, `ping_received`,
   `pong_received`) and keep only what is counted. Client-visible, so it is a decision, not a
   cleanup — `CLUSTER INFO` is a key-value block and clients tolerate absent keys far better than
   `CLUSTER NODES` tolerates absent columns.

`total_cluster_links_buffer_limit_exceeded` has no analogue at all (there is no per-link output
buffer with a limit), so it is a drop candidate under either option.

## Tests that should exist

- `cluster_stats_messages_sent_grows_with_bus_traffic`
- `cluster_stats_messages_received_grows_on_the_receiving_node`
- `cluster_info_omits_gossip_counters_that_have_no_source` (option 2)

## Spec impact

FM-CLUSTER-074 gains a row-mate for the counters themselves; the deviations table's "Gossip
counters in `CLUSTER INFO`" line loses its "filed as issue 37" caveat.
