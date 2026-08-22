# 40: Read-consistency contract stated + serve-stale knob — reads on fenced nodes become a documented choice

Status: ready-for-agent

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
