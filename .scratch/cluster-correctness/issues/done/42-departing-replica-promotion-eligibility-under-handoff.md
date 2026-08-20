# 42: Departing replica must not be a promotion candidate for the slot under handoff

Status: done

## Origin

Ruling R3 (2026-08-20, [campaign ledger](../../../formal-spec/2026-08-19-quint-completeness-campaign.md),
implemented in `6a20ad01`) amended FM-CLUSTER-097 with the ending-drain exemption: a
session that has already classified its departure drains its held buffer past the
barrier floor. The amendment's safety argument — what closes the exemption's
combination with FM-REPLICATION-062's Graceful self-fence disarm — is that **a
departing replica is not a promotion candidate for the slot under handoff**, so the
drained tail cannot become a promoted primary's history while ownership is moving.

## What is wrong

Nothing enforces that protection. The R3 implementation surveyed candidate
eligibility and found it rests entirely on `replica_priority`:

- TR-CLUSTER-021 (`specs/cluster.md:362`): candidates = replicas of the failing
  primary; score = priority + lag; priority-0 excluded. No link-state filter.
- FM-CLUSTER-056 (`specs/cluster.md:1384`) states the **opposite tendency**
  explicitly: a candidate whose offset could not be determined is scored as worst,
  **not excluded** — so a replica whose link just died inside a barrier window (and
  which therefore received the drained tail) remains selectable.
- FM-CLUSTER-057 / FM-CLUSTER-058 cover priority-0 filtering and tiebreak only.

FM-CLUSTER-097's trailing prose (post-`6a20ad01`) discloses the gap in as many
words: the protection is not yet pinned by a row of its own.

## What is needed

- A row (cluster promotion/eligibility family) stating: while a slot is under
  handoff, a replica whose feed session for the handing-off node has classified
  its departure (Graceful or Lost) is not an eligible promotion candidate for that
  slot, until it re-attaches and re-syncs. Reconcile the wording with
  FM-CLUSTER-056's keep-in-set rule — 056 governs offset-unknown scoring for
  healthy links, this row governs handoff-window departures; the row must say
  which wins when both apply.
- A forcing test on the candidate-selection path (departing replica present,
  handoff in flight → not selected; selected again after re-sync).
- FM-CLUSTER-097's prose updated to cite the new row instead of naming the gap.

## Related

- Issue 37 (offset-unknown candidates rank last, MAJ-9 ruling): same selection
  path, different rule — 37 is staleness ranking, this is handoff-window
  eligibility. Land whichever first; the second reconciles wording with the first.
- FM-REPLICATION-062 (Graceful disarms the self-fence) — the interaction R3's
  protection exists to close.

## Resolution

Landed together with [issue 37](37-promotion-validity-bound-offset-unknown-last.md).
Selection is now **filter → tier → bound → score** (TR-CLUSTER-021); this issue owns the
filter, 37 owns the tier and the bound. **A filter beats a ranking** — that is the division
the two rows state explicitly, and it is what keeps 37's availability floor from smuggling a
departed replica back in as a last resort.

**Spec.** New **FM-CLUSTER-106**. FM-CLUSTER-056 narrowed to the scored tier, so it no
longer reads as a licence to keep a departing replica in the set. FM-CLUSTER-097's trailing
prose now cites FM-CLUSTER-106 instead of naming the gap.

**Where the deciding node learns each fact.** The *handoff* predicate comes from the
replicated `migrations` map via `ClusterState::snapshot()`, so every node computing
selection sees the same handoff state; the only local input is the lease comparison every
reader of a prepared record already performs, and an expired prepared record reads as absent
here exactly as it does everywhere else. The *departure* predicate is the replica-side mirror:
the source end of the feed session lives on the failed primary, which is by hypothesis
unreachable, so the candidate self-reports `master_link_up()` — the same handle INFO's
`master_link_status` renders — on the `HealthProbe` it is already being asked for its offset.
It rides a new narrow `ReplicaLinkState` seam rather than threading the whole
`RoleController` into the cluster bus. `Option<bool>`, not `bool`: a probe that failed learned
nothing, and nothing is treated as departed, so the filter is fail-closed on its own error path.

Promotion in FrogDB is whole-node, so the slot-scoped rule is enforced at node granularity:
the filter arms whenever *any* slot the successor would inherit is under a live handoff. That
is the conservative direction, and it costs nothing outside a handoff window.

**Forcing tests** (all in `frogdb-cluster-runtime`, the mutated crate):
`a_departed_replica_is_not_a_candidate_while_a_slot_is_under_handoff`,
`a_re_attached_replica_is_a_candidate_again`,
`outside_a_handoff_window_a_departed_replica_is_still_a_candidate`,
`a_probe_that_learned_nothing_is_treated_as_departed_under_handoff`,
`the_handoff_filter_beats_the_last_resort_availability_floor`,
`auto_failover_skips_the_departed_replica_of_a_slot_under_handoff`,
`auto_failover_abandons_when_every_candidate_departed_under_handoff`,
`a_lapsed_handoff_lease_stops_filtering_candidates`,
`the_health_probe_reports_this_nodes_inbound_link_state`.
