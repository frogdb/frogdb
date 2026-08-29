# 40: Read-consistency contract stated + serve-stale knob — reads on fenced nodes become a documented choice

Status: done

## Origin

Distsys-review MIN-5 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **row + stale-serve knob** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)) —
the larger option: state the contract AND give operators the Redis-parity control.

## What is wrong

`specs/cluster.md` has no read-consistency contract. The self-fence covers writes
only; no row documents what a read may return on a fenced or partitioned node, and
there is no stated stale-read non-guarantee. Applications cannot reason about read
staleness, and the current always-serve behavior is undocumented rather than
chosen. Redis exposes `replica-serve-stale-data` with documented semantics; CRDB
documents follower reads and their staleness bound precisely.

## What to build (spec-first; cluster locked, gate 0.80)

1. Spec rows first:
   - Read-contract row: with the knob at its default, reads on a fenced or
     partitioned node may return stale data; **no staleness bound is offered**.
     Cross-ref the write self-fence rows (writes rejected, reads served).
   - Knob row: `serve-stale-reads = true|false` (name per config conventions —
     follow the existing cluster config family). `false` → reads on a node whose
     cluster link is down/fenced return a pinned error (Redis parity text shape:
     `-MASTERDOWN`-equivalent; pick and pin ours, document any deviation in the
     deviations table). Administrative/introspection commands (INFO, CONFIG,
     CLUSTER, PING, ...) remain allowed — enumerate the exemption set in the row,
     mirroring Redis's carve-outs.
2. Config: knob in the cluster config family, **live-mutable** (matches the
   26-param live-mutability standard; golden config updated).
3. Code: read dispatch consults fence/link state when the knob is `false`; error
   reply comes from the redirect/error seam (seam-lints apply).
4. Forcing tests:
   - Default (`true`): fenced node serves stale reads; write still fenced.
   - `false`: fenced node rejects reads with the pinned error; exempt commands
     still answer; flipping the knob live changes behavior without restart.
5. Docs: config reference + deviations table entry (name/semantics vs
   `replica-serve-stale-data`).

## Cross-references

- [Issue 39](39-distsys-review-minors-sweep.md): sibling distsys-review minors —
  this one graduated to its own issue by ruling.
- Write self-fence rows (FM-CLUSTER family) — the contract this row completes.
- Redis `replica-serve-stale-data` semantics — parity baseline.

## Acceptance criteria

- [ ] Read-contract + knob rows landed; `just lint-spec` green
- [ ] Knob live-mutable; golden config updated
- [ ] Forcing tests: both knob states + live flip + exemption set
- [ ] Deviations table entry; config docs updated
- [ ] `just mutants-diff` on frogdb-cluster (locked, 0.80) triaged

## Blocked by

None — can start immediately.

## Amendment (2026-08-21) — the replication half is absorbed; the cluster half is not

[redis-feel issue 17](../../../redis-feel/issues/done/17-unimplemented-admission-gates.md)
shipped `replication.replica-serve-stale-data` (live-mutable, default **`no`** —
a deliberate deviation from Redis's `yes`) and the `-MASTERDOWN` gate behind it.
Its exemption set is flag-driven (`CommandFlags::STALE`) rather than a
hand-enumerated admin/introspection list, which satisfies this issue's
"enumerate the exemption set" intent in a stronger form: the enumeration *is*
what `COMMAND INFO` advertises, so the two cannot drift.

Do **not** add a second `serve-stale-reads` knob in the cluster config family.
One knob, one wire name, Redis's spelling. What this issue proposed as item 2
is done.

### What is still open here

- Items 1 and 3 for the **cluster** case. The shipped gate keys on the
  *replication* link (`RoleController::primary_target` +
  `master_link_up`). A node that is fenced or partitioned at the **Raft** layer
  while its replication link is healthy — or a primary with no link at all — is
  not covered and still serves reads.
- The `specs/cluster.md` read-consistency contract rows. Nothing spec-side was
  written under issue 17.
- Whether the cluster case should reuse `replica-serve-stale-data` (one knob,
  two staleness sources) or gets its own row is the open design question; the
  ruling above predates the shipped knob.

## Resolution (2026-08-29)

Closed in two halves, as the 2026-08-21 amendment predicted.

**The knob half was absorbed by [redis-feel issue 17](../../../redis-feel/issues/done/17-unimplemented-admission-gates.md)**
(commits `785fd51a..51f0526e`, ~2026-08-22): `replication.replica-serve-stale-data`,
live-mutable, default **`no`** — the deliberate deviation from Redis's `yes`, ruled
by the user on the CockroachDB/FoundationDB fail-fast argument. Its exemption set is
`CommandFlags::STALE`, the set `COMMAND INFO` advertises, so the gate and the
enumeration cannot drift. No second knob was built, per the amendment. A happy
coincidence found while closing this: `no` is also Redis's *cluster* default
(`cluster-allow-reads-when-down no`), so the cluster half deviates from upstream in
name only, not in behavior.

**The cluster half was open, and the code gap was real.** Both fence consultation
sites were write-guarded — the gauntlet rung in `connection/guards.rs` tested
`CommandFlags::WRITE` before `has_quorum()`, and `ShardWriteSeam::admit` returned
early for non-writes. The shipped stale gate fired only on
`role_controller.primary_target().is_some() && !master_link_up()`, a replication-link
condition a quorum-fenced cluster primary never meets. `config/src/cluster.rs`
documented the behavior in as many words ("Reads remain available."), and unit test
`test_self_fence_read_allowed_when_quorum_lost` pinned it. So a partitioned node
refused the write that would diverge and then answered `GET` from its pre-partition
snapshot for as long as the partition lasted.

### What this change added

*Spec* — `specs/cluster.md` **FM-CLUSTER-107**, the read-consistency contract: what a
read gets on a quorum-fenced node at each knob setting, the `CommandFlags::STALE`
exemption set, and an explicit statement that **no staleness bound is offered** in
either direction (fenced = unavailable, unfenced = unbounded; no max-age argument, no
read timestamp, no follower-read lease). Cross-referenced from FM-CLUSTER-059 (the
write self-fence) and TR-CLUSTER-026 (whose CheckQuorum re-basing under issue 27 now
covers both halves at once). Two `## Redis deviations` entries: one against
`cluster-allow-reads-when-down` (same default, one fewer knob, a refusal that names
the mechanism), one recording that neither database offers bounded staleness.

*Code* — through the existing gate seam, no new knob and no new config family:

- `QuorumChecker::fences_stale_reads()` (`core/src/command.rs`), defaulting to
  `false`. The default is load-bearing: the replication replica-loss fence shares the
  write rung, and losing replicas costs durability, not currency — without it this
  change would have started refusing `GET` on a standalone primary running
  `min-replicas-to-write`.
- `SelfFenceGate::fences_stale_reads()` (`cluster-runtime/src/flags.rs`) — the only
  opt-in. `self_fence_on_quorum_loss() && !inner.has_quorum()`, which is exactly
  `!SelfFenceGate::has_quorum()`: one verdict, so a node can never refuse writes while
  claiming its reads are current, and the knob disarms both halves together. The flag
  is read first and short-circuits, because this runs once per command rather than
  once per write and `FailureDetector::has_quorum` walks the node table.
- `command_admission::quorum_stale_refusal` + `ClusterFence`
  (`core/src/command_admission.rs`) — the policy, a pure function beside
  `stale_refusal`. Kept as separate bodies: they answer to two different locked specs,
  and a shared body would make either area's spec-first edit a change to the other's
  contract.
- `CommandError::ClusterDownStaleRead` (`types/src/error.rs`) —
  `CLUSTERDOWN The cluster is down (quorum lost, stale reads refused)`. Redis's code,
  FrogDB's parenthetical, because one knob governs two fences and the operator needs
  to know which fired.
- The new gauntlet rung in `PreDispatchView::run_pre_checks`, placed with the existing
  stale gate and consulted *before* it: `-MASTERDOWN` says "wait for my primary",
  which is the wrong instruction for a node whose slots may already belong to someone
  else. Writes still hit the older write rung first and keep their own wording.
- Doc corrections where the old behavior was written down: `config/src/cluster.rs`,
  `website/src/content/docs/operations/clustering.md`,
  `website/src/content/docs/architecture/clustering.md`,
  `website/docs-spec/specs/operations/clustering.md`, and a new cluster subsection in
  `website/src/content/docs/compatibility/overview.mdx`.

### Forcing tests (all tagged `// FM-CLUSTER-107`)

`frogdb-cluster-runtime` (the locked crate the 0.80 gate measures) —
`a_quorum_fenced_gate_fences_stale_reads`,
`read_fencing_and_write_fencing_share_one_verdict`,
`the_self_fence_knob_disarms_read_fencing_live`,
`the_read_fence_short_circuits_on_the_knob`,
`the_trait_default_leaves_a_non_cluster_fence_serving_reads`.

`frogdb-core` (the policy) — `a_healthy_quorum_gates_nothing`,
`a_quorum_fenced_node_refuses_a_read_by_default`,
`a_stale_flagged_command_survives_the_quorum_fence`,
`the_serve_stale_data_knob_reopens_a_quorum_fenced_node`,
`the_cluster_fence_and_the_link_fence_name_different_mechanisms`.

`frogdb-server` (the rung) —
`test_self_fence_read_allowed_when_the_fence_does_not_claim_staleness` (the rewrite of
the test that pinned the old behavior — it now pins the *replication* fence's carve-out
instead), `the_gauntlet_refuses_a_read_on_a_quorum_fenced_node`,
`the_gauntlet_names_the_write_fence_for_a_write`,
`the_gauntlet_serves_stale_flagged_commands_on_a_fenced_node`,
`the_gauntlet_honours_serve_stale_data_on_a_fenced_node`.

### Verification

- `just lint-spec` — green (309 failure modes, 1705 tags; the one warning is the
  pre-existing FM-CLUSTER-104 gap tracked by issue 32).
- `just mutants-diff frogdb-cluster-runtime` — 4 mutants in the changed lines,
  **4 caught, 0 survivors**. No triage needed.
- `just test frogdb-cluster-runtime 'fence|stale'` — 10/10;
  `just test frogdb-core 'quorum|stale'` — 13/13;
  `just test frogdb-server 'self_fence|gauntlet'` — 17/17, which includes the seven
  pre-existing `integration_replication::test_self_fence_*` tests that prove the
  replication fence still serves reads.

### Follow-up filed

[Issue 45](../open/45-allocation-free-quorum-read.md) — `FailureDetector::has_quorum`
allocates a `Vec<NodeInfo>` (and a `String` per node) on every call. That was
acceptable at once-per-write; this change makes it once-per-command on a fenced node.
The knob short-circuit keeps the common path free, but the fenced path should not
allocate to answer a boolean.
