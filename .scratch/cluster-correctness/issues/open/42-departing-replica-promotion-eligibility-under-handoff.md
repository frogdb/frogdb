# 42: Departing replica must not be a promotion candidate for the slot under handoff

Status: ready-for-agent

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
