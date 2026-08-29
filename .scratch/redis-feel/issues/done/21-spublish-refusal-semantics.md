# SPUBLISH refusal semantics now that the subscribe path slot-routes

Status: done

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

## Ruling (2026-08-29)

- **Case 2: keep bus forwarding.** Smart-proxy stays; documented deviation
  from Redis's `MOVED`. No change.
- **Cases 3/4: refuse `CLUSTERDOWN`** (Redis parity). Best-effort local
  delivery to stranded/gap-window subscribers is dropped — the publisher's
  receiver count must not imply health that isn't there; cluster breakage
  surfaces to the publisher instead of silent partial delivery.

Implementation is spec-first: rewrite the FM-CLUSTER-070 pin (cases 3/4 rows
change from "local delivery + warn" to a pinned `CLUSTERDOWN` error — match
Redis's error text shape and pin ours), forcing tests for both refusal cases
plus a case-2 forward-still-works test, `just mutants-diff` on touched locked
crates (gate 0.80). Sequencing: land after the cluster-40 read-consistency
spec rows merge (same spec file).

## Resolution (2026-08-29)

Landed as ruled: case 2 keeps bus forwarding, cases 3/4 refuse.

**Pinned reply.** Both unplaceable cases answer `CLUSTERDOWN Hash slot <slot> not
served`, built by `frogdb_types::redirect::clusterdown_slot` — the constructor
`SSUBSCRIBE`/`SUNSUBSCRIBE` and the slot fence already use, so the publish and
subscribe paths cannot drift on the wire. Real Redis 8 answers
`-CLUSTERDOWN Hash slot not served` (`CLUSTER_REDIR_DOWN_UNBOUND` in
`clusterRedirectClient`) from the same two states: `getNodeByQuery` returns
`NULL` for the slot whenever it cannot name a node for it. FrogDB's text adds
the slot number — the pre-existing house rendering shared by every slot refusal;
the `CLUSTERDOWN` code clients match on is byte-identical.

**Spec.** `specs/cluster.md` FM-CLUSTER-070 retitled "`SPUBLISH` refuses a slot
the cluster cannot place"; Observable/NOT observable/Invariant/Outcome
variant/Forced by all rewritten, and the prose paragraph now records the Redis
mechanism and separates this refusal from FM-CLUSTER-107's quorum-fence
`ClusterDownStaleRead`. FM-CLUSTER-069's `SPUBLISH` sentence updated for the new
outcome type. The `Redis deviations` row shrank to case 2 only (forwarding to a
*reachable* owner instead of `MOVED`).

**Code.** `frogdb-cluster-runtime/src/pubsub.rs`: new `SpublishRefusal`
(`Unowned` / `OwnerUnaddressable{owner}`); `SpublishOutcome` is now
`Forwarded(usize) | Local | Refused{slot, cause}` (`Local(ShardRoute)` and
`remote_count()` are gone); `ShardRoute::delivers_locally()` is `Local` only;
`forward_spublish`'s two fallback arms now warn "refusing with CLUSTERDOWN"
and return `Refused`. `frogdb-server` `pubsub_conn_command.rs`: new pure
`spublish_forward_reply(SpublishOutcome) -> Option<Response>` projects the three
outcomes, and `handle_spublish` uses it. No best-effort local-delivery path
remains for cases 3/4.

**Seam.** `lint-redirect-seam` extended to reject an inline
`Response::error("CLUSTERDOWN Hash slot ...")` outside `types/src/redirect.rs`;
`agents/seam-lints.md` row updated.

**Tests** (all green). `frogdb-cluster-runtime` (counts for the 0.80 gate):
`cluster_forward_delivers_locally_when_this_node_owns_the_slot`,
`cluster_forward_refuses_an_unowned_slot`,
`cluster_forward_refuses_an_owner_this_node_cannot_address`,
`cluster_forward_refusals_stay_distinguishable_by_cause`,
`cluster_route_names_a_reachable_remote_owner`,
`cluster_forward_reports_the_owners_subscriber_count`,
`test_local_forwarder_forward_delivers_locally`. `frogdb-server`:
`spublish_refusal_replies_clusterdown_naming_the_slot` (pins the literal text
for both causes), `spublish_forwarded_outcome_replies_the_owners_count`,
`spublish_local_outcome_defers_to_local_delivery`.

**Gates.** `just lint-spec` green, `just lint-gates` green, `just spec-gen`
regenerated the website mirror, `just mutants-diff frogdb-cluster-runtime`
7 mutants: 6 caught, 0 missed, 1 unviable. The unviable one is
`replace forward_spublish -> SpublishOutcome with Default::default()` at
`pubsub.rs:295` — `SpublishOutcome` implements no `Default`, so the mutant does
not compile; it is not a coverage gap.
