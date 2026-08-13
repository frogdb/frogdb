# 27 — Write admission is gated on TCP-probe-derived local quorum, not on Raft liveness

Status: ready-for-agent

## Parent

[Adversarial design review — `specs/cluster.md` + the 2026-08-13 rulings](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-cluster.md),
finding **H3**. Spec-gap finding, distinct from the amended rulings on issues 14–20 and 25: issue
20's ruling and amendment fixed what happens *after* a partition heals (Raft becomes the
reconciliation channel; the eviction fence is now required for the admin path). This finding is
about the *during-partition* window, which neither ruling nor amendment touched.

## What is wrong

FM-CLUSTER-055 (quorum counts conservatively, over locally-probed peers) and FM-CLUSTER-059 (the
self-fence knob) key write admission on point-to-point TCP reachability, not on consensus-layer
liveness. GAP 4 already notes that liveness is a bare TCP connect, so a wedged-but-listening node
reads as healthy.

Issue 20's own evidence is the proof: node 0 is partitioned from the Raft leader, still reaches one
peer, counts 2-of-3, does **not** fence, and serves slots that have already been failed over to
node 2. Ruling 20 fixes the post-heal half; the during-partition window — in which two nodes serve
one slot — is untouched by that ruling.

The general defect: the quantity being measured (can I open a TCP socket to some peers?) is not
the quantity that matters (is my view of who owns this slot still current?). A wedged peer, a peer
that answers `HealthProbe` unconditionally (GAP 4: "without consulting quorum, fence, or loading
state"), or a peer on the same losing side of the partition all count as evidence of health today.

Practice: etcd/raft use CheckQuorum + PreVote so a leader that cannot hear a majority steps down;
CRDB refuses to serve a range without a valid lease; Kafka's leader stops accepting writes when it
cannot maintain the ISR. All three make "am I still authoritative?" a function of consensus-layer
liveness, not of point-to-point reachability.

## What to build

Spec-first. Derive the write-admission self-fence from the Raft layer instead of from TCP-probed
peer quorum:

- A node fences keyed traffic when it has not heard from, or applied an entry from, a Raft leader
  within an election timeout — the CheckQuorum shape (etcd/CRDB/Kafka precedent).
- TCP probe reachability is no longer admission evidence: a wedged-but-listening peer or a
  same-partition peer must not count toward the quorum that admits writes. (Issue-20 evidence: the
  partitioned node kept serving because it counted a same-side reachable peer.)
- Add a spec-preamble paragraph in `specs/cluster.md` stating the principle explicitly: **using a
  clock to STOP serving is fail-closed and safe; using a clock to ADMIT traffic is the rejected
  anti-pattern.** This is what keeps the election-timeout fence from being "fixed away" later as a
  wall-clock smell — it reconciles this row with ruling 17's removal of the `barrier_ms` admission
  window, which is the opposite case (a clock used to *admit* `Complete`, correctly deleted).

Amend FM-CLUSTER-055/059 (or add a new row alongside them) to state the election-timeout-derived
fence; the preamble paragraph is a `specs/cluster.md`-header addition, not a new FM row.

## Acceptance criteria

- [ ] FM-CLUSTER-055 and/or FM-CLUSTER-059 amended (or a new row added) to state the
      election-timeout/Raft-liveness self-fence, replacing TCP-probe-derived quorum as admission
      evidence; the fail-closed-vs-admit preamble paragraph added to `specs/cluster.md`;
      `just lint-spec` green
- [ ] Forcing test in `frogdb-cluster-runtime` reproducing issue 20's evidence shape (node
      partitioned from the Raft leader, still reaches one peer, slot already failed over
      elsewhere) fails first against today's TCP-probe self-fence, then is fixed
- [ ] `just mutants-diff frogdb-cluster-runtime` (and `frogdb-cluster` if the fence predicate moves
      there) triaged

## Blocked by

None.

## Ruling (2026-08-13)

**The write-admission self-fence keys off consensus-layer liveness: a node fences keyed traffic when it has not heard from (or applied an entry from) a Raft leader within an election timeout — CheckQuorum shape (etcd/CRDB/Kafka precedent). TCP probe reachability is no longer admission evidence: a wedged-but-listening peer or a same-partition peer must not count (issue-20 evidence: partitioned node kept serving). Additionally, add a spec-preamble paragraph stating the principle: using a clock to STOP serving is fail-closed and safe; using a clock to ADMIT traffic is the rejected anti-pattern — so the election-timeout fence is not later "fixed" away as a wall-clock smell.**
