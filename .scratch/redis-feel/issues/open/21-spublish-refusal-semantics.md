# SPUBLISH refusal semantics now that the subscribe path slot-routes

Status: needs-triage

## Origin

Filed from the FM-CLUSTER-070 editorial fix (2026-08-28). The spec's original
justification for SPUBLISH's local-delivery fallback — "no other node would
deliver either, because FrogDB's SSUBSCRIBE does not slot-route subscribers" —
was false: SSUBSCRIBE slot-routes (`pubsub_conn_command.rs` route loop), and
SUNSUBSCRIBE does too since b3fc2bb0. The editorial fix rewrote the rationale
(best-effort delivery to stranded/gap subscribers); the behavior question it
was gating is now open on its own merits.

## The question

Setup: shard channel `news{x}` -> slot 1234, publisher connected to node A.

| # | Slot state | FrogDB today | Redis 8 |
|---|---|---|---|
| 1 | A owns it | local delivery | same |
| 2 | B owns it, address known | forward over cluster bus to B | `MOVED` |
| 3 | nobody owns it (slot-map gap) | local delivery + warn | `CLUSTERDOWN` |
| 4 | owner known, unaddressable | local delivery + warn | `CLUSTERDOWN` |

Since subscribers are routed at subscribe time, local delivery in 3/4 reaches
only stranded registrations (subscribed before ownership moved) or gap-window
subscribers. Decide per case:

- Case 2: keep bus forwarding (smart-proxy, kinder to clients than MOVED —
  documented deviation) or adopt `MOVED`?
- Cases 3/4: keep best-effort local delivery (named, warn-logged — current
  FM-CLUSTER-070 pin) or refuse `CLUSTERDOWN` (Redis parity; stranded
  subscriber then never gets the message, Redis-identical loss)?

## Cost

Behavior change touches locked `frogdb-cluster`/`frogdb-cluster-runtime` +
server: spec-first FM row rewrite, new forcing tests, mutants gate 0.80.
