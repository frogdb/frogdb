# `CLUSTER INFO` gossip counters are hardcoded `0`

Status: done
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

## Resolution

Both options, split by field: count what is real (option 1), drop what has no source (option 2).

Checking Redis settled the drop question. `clusterGenNodesDescription`'s sibling for `CLUSTER INFO`
loops over the message types and `continue`s when a type's counter is zero, so a real Redis node
that has never sent a `MEET` does not print a `cluster_stats_messages_meet_sent` line at all. Absent
per-type lines are therefore *normal* output that every client already tolerates, and omitting the
four gossip lines on a server with no gossip protocol is parity, not divergence. Redis always emits
the two totals and `total_cluster_links_buffer_limit_exceeded` unconditionally, so those three lines
stay.

Per-field decisions:

| Field | Decision | Why |
| --- | --- | --- |
| `cluster_stats_messages_ping_sent` | omit | No gossip protocol; Redis omits a zero-valued per-type line |
| `cluster_stats_messages_pong_sent` | omit | Same |
| `cluster_stats_messages_ping_received` | omit | Same |
| `cluster_stats_messages_pong_received` | omit | Same |
| `cluster_stats_messages_sent` | computed | Real bus frames, counted at the wire seams |
| `cluster_stats_messages_received` | computed | Same |
| `total_cluster_links_buffer_limit_exceeded` | keep `0` | Measured, not fabricated: no per-link output-buffer limit exists to exceed, and Redis always emits the line |

The counters live in `frogdb-cluster/src/stats.rs` (`ClusterBusStats`, one `AtomicU64` per
direction, `snapshot()` returning a `Copy` pair) — inside the mutation-gated crate. One instance per
node, owned by `ClusterNetworkFactory` and handed to the bus context, so the outbound client side
and the inbound server side of the same node accumulate into the same pair.

Counting sits at the four points where a frame crosses the wire, never at a call site that might not
reach it: `try_send_on_framed` records a send after `framed.send(...)` returns and a receive after
the response frame arrives; `parse_rpc_message` records a receive when a frame arrives at all
(decodable or not — it crossed the wire); `send_rpc_response` records a send after the write. A
connection that fails to open counts nothing, which `a_connection_that_never_opens_counts_nothing`
pins.

`ClusterInfoReport::bus_stats` is an `Option`, populated from
`ctx.network_factory.map(|nf| nf.bus_stats().snapshot())`. Unknown reads as an absent line rather
than a zero — the same treatment `cluster_raft_term` already gets — so a node that cannot read its
own counters says nothing instead of claiming an idle bus.

Verified no consumer parses the four removed keys: `website/`, `docs/`, and
`test-harness/src/cluster_helpers.rs` mention none of them.

- Spec: **FM-CLUSTER-077** added (bus counters are live and monotone; the gossip lines are absent,
  not zero). FM-CLUSTER-073 and FM-CLUSTER-074 Bug-refs re-pointed here. The deviations table's
  "Gossip counters in `CLUSTER INFO`" row rewritten to "Not a deviation any more".
- Tests (all `// FM-CLUSTER-077`):
  - `frogdb-cluster` `stats::tests::a_fresh_counter_pair_reads_zero`,
    `the_two_directions_are_counted_independently`, `counters_accumulate_across_threads`
  - `frogdb-cluster` `network::tests::cluster_stats_messages_sent_grows_with_bus_traffic`,
    `cluster_stats_messages_received_grows_on_the_receiving_node`,
    `a_connection_that_never_opens_counts_nothing`
  - `frogdb-server` `commands::cluster::tests::cluster_info_omits_gossip_counters_that_have_no_source`,
    `cluster_info_reports_the_live_bus_counters`,
    `cluster_info_omits_the_bus_totals_when_they_cannot_be_read`
