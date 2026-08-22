# c31-06: The drain barrier, held-write disposition, and client-visible slot semantics

Status: DRAFT — pending wave-0 approval
Wave: 4 (parallel with c31-07)
Size: L
Crates: `frogdb-cluster-runtime`, `frogdb-server`

> Per [R1](../2026-08-22-work-item-rulings.md#r1--issue-31-campaign-staging-clustered-waves),
> each LOCKED row lands atomically with its forcing test and implementation.
>
> [R3](../2026-08-22-work-item-rulings.md#r3--inv_no_hold_during_staged_flip-adoption-time-rule)
> binds the hold-release side: the safety boundary is the **applied** role flip, not the staging.
> A latched drain-hold region may lawfully coexist with a staged, unadopted flip; the hold-release
> is atomic with the adoption's applied write. This cluster owns the hold-release half of the
> adoption-time invariant, jointly forced with c31-04.

This cluster owns the source node's local half of a migration: the drain barrier that holds writes
while the slot seals, the disposition of those held writes at every exit, and everything a client
can observe about a slot in migration. Under source authority the client-facing story simplifies
dramatically — no `ASK`, no split slots, no importing gates — and a large fraction of the work is
*retiring* behaviour rather than adding it.

## Owned rows

### Client-visible slot semantics

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| FM-CLUSTER-026 | Rewritten | The importing gates collapse: `ASKING` is a no-op under source authority | blast radius "Rewritten" |
| FM-CLUSTER-027 | Rewritten | The `RESTORE` exemption is retired; routing `MOVED <slot> <source>` is the winning gate | blast radius "Rewritten" |
| FM-CLUSTER-028 | Rewritten | The source serves the whole slot and never `ASK`s; clients never observe a split slot | blast radius "Rewritten" |
| FM-CLUSTER-029 | Rewritten | `WATCH` routing re-derived under source authority | blast radius "Rewritten" |
| FM-CLUSTER-038 | Unchanged, stated | Blocked clients wake at `Complete` | blast radius "Unchanged, stated" |
| FM-CLUSTER-092 | Rewritten | A write caught by the barrier wakes **redirected** — inverted under source authority | blast radius "Rewritten" |
| FM-CLUSTER-093 | Rewritten | A transaction parked across finalization is redirected, not committed | blast radius "Rewritten" |
| FM-CLUSTER-094 | Rewritten | A script in flight across a handoff leaves no write on the former owner | blast radius "Rewritten" |
| FM-CLUSTER-095 | Rewritten, **arm split** | The finalization-refusal arm is **retired**; the ownership-moved arm is kept. The SlotFence generation mechanism is unchanged | blast radius "Rewritten" |
| FM-CLUSTER-096 | Rewritten | Unpinnable batches: the parked-batch disposition at each exit, and the drain-covers-continuations containment | blast radius "Rewritten" |

### Barrier and fence

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| FM-CLUSTER-079 | Rewritten | Unpinnable-command folding is the mechanism behind §3's unpinnable held batches; the row gains the byte cap and the per-exit disposition table | blast radius "Rewritten" (V8-M5) |
| FM-CLUSTER-080 | Exemption **retired** | The `MIGRATE` slot-pause exemption is retired: under §3's exempt-set rule `MIGRATE` and `RESTORE` are held like any other write. `only_migrate_and_cluster_are_slot_pause_exempt` is **re-pointed** at the new exempt set (`CLUSTER` only). **V14-m4 disambiguation**: this is the *slot-pause seal's* set; the staged-flip fence's set is different and member-enumerated (c31-04) | blast radius "Retired" (V14-m4) |
| FM-CLUSTER-081 | Unchanged, stated | The `CLUSTER` exemption survives as the sole member of the slot-pause seal's exempt set | blast radius "Unchanged, stated" |
| FM-CLUSTER-082 | Rewritten | "Neither can release the other" now composes with a barrier armed by replicated phase and re-derived at boot; the independence claim survives | blast radius "Rewritten" (V8-M5) |
| FM-CLUSTER-083 | Rewritten | The Outcome enumeration gains a third member: a parked `EXEC` whose batch is unpinnable is answered `-TRYAGAIN` | blast radius "Rewritten" (V8-M5) |
| FM-CLUSTER-088 | Unchanged, stated | Cross-slot independence holds because the held-byte cap is **per-migration** | blast radius "Unchanged, stated" |
| FM-CLUSTER-090 | *(owned by c31-05)* | Cross-referenced here: the barrier's action under the new phase machine | — |
| FM-CLUSTER-104 | Rewritten, extended | **Same-node re-arm only** — a successor never inherits the barrier. §3's boot-reconstruction rule becomes the row's restart arm | blast radius "Rewritten" (V7-m5) |
| TR-CLUSTER-016 | Rewritten | Its precondition named the slot-scoped write barrier that §8 deletes; rewritten to ordinary replica-feed backpressure | blast radius "Rewritten" |
| TR-CLUSTER-026 | Unchanged, stated | The self-fence gains the held-set release row (§3) | blast radius "Unchanged, stated" |
| TR-CLUSTER-034 | Unchanged, stated | Per-node arm and release reaction | blast radius "Unchanged, stated" |

### New rows this cluster writes

1. The **sealed-fence invariant** and its companion — a local fence is never weaker than the
   sealed fence — plus the self-fence's held-set release.
2. The **held-write disposition table**, including unpinnable batches and the
   **demotion-wins precedence rule**.
3. The **totality claim** over record-removing transitions: every transition that removes a record
   has an exit row in the table. The design names seven such transitions; the forcing test
   `every_record_removing_transition_has_a_held_write_exit_row` walks the writer-join row rather
   than a hand list.
4. The **§3 exempt-set rule** — the seal exempts only the `CLUSTER` family — with an explicit
   cross-reference to c31-04's staged-flip fence row and a sentence saying the two sets differ.
5. The **enumerated non-command mutators**: eviction and active expiry are suspended for a sealed
   slot; lazy expiry on read is classified separately.
6. The **fence-reconstruction-at-boot rule**.
7. The **per-migration cap scoping** with the node-wide sizing note
   (`cluster-migration-barrier-max-bytes`, default 4 MiB, stamped at `Begin` by c31-05).
8. The **automatic-cutover deviation row** (§6).
9. The **Redis-deviation rows** (§6): no `ASK`, `ASKING` is a no-op, `MIGRATE` is not used for
   resharding, no split markers.
10. The **latch level rule** with its armed-barrier scope.
11. The **§3 `RemoveNode` exit row** with both arms (V16-M1) and the **§3 `ReportRunIdentity` exit
    row** (V17-M1) — the transitions themselves belong to c31-03 and c31-01, but their held-write
    exits belong to the table and therefore here.

## What to build

### 1. Spec deltas (first)

1. The §3 rows: the exempt-set rule, the disposition table with all seven exits, the totality
   claim, the cap scoping, the latch level rule, the boot-reconstruction rule, the non-command
   mutator enumeration.
2. The barrier failure-mode rewrites (079, 082, 083, 104) and FM-CLUSTER-080's retirement.
3. The client-visible rewrites (026-029, 092-096) and FM-CLUSTER-095's arm split.
4. TR-CLUSTER-016's rewrite; TR-CLUSTER-026 and -034's stated extensions.
5. The §6 deviation rows.

### 2. Forcing tests (second, observed failing)

Barrier tests in `frogdb-cluster-runtime`; client-visible tests in `frogdb-server` (with the
mutation-score caveat: any assertion about `cluster-runtime` behaviour needs a `cluster-runtime`
test too).

- `only_migrate_and_cluster_are_slot_pause_exempt` — **re-pointed**, not deleted; the new
  assertion is that `CLUSTER` alone is exempt and `MIGRATE`/`RESTORE` are held
- `slot_pause_exempt_set_differs_from_the_staged_flip_fence_exempt_set` (V14-m4)
- `every_record_removing_transition_has_a_held_write_exit_row` (the totality walk)
- `held_writes_are_paid_a_real_reply_at_every_exit`
- `demotion_wins_the_disposition_precedence`
- `an_unpinnable_parked_exec_is_answered_tryagain`
- `held_bytes_are_capped_per_migration_not_node_wide`
- `two_concurrent_slot_migrations_hold_independently`
- `a_write_caught_by_the_barrier_wakes_redirected`
- `a_transaction_parked_across_finalization_is_redirected_not_committed`
- `a_script_in_flight_across_a_handoff_leaves_no_write_on_the_former_owner`
- `asking_is_a_no_op`
- `a_client_never_observes_a_split_slot`
- `routing_moved_names_the_source_for_the_whole_migration`
- `watch_routes_to_the_source_until_complete`
- `eviction_and_active_expiry_are_suspended_for_a_sealed_slot`
- `lazy_expiry_on_read_is_classified_and_behaves_as_declared`
- `a_restarted_source_reconstructs_its_fence_from_replicated_phase`
- `a_successor_does_not_inherit_the_predecessors_barrier`
- `a_local_fence_is_never_weaker_than_the_sealed_fence`
- `blocked_clients_wake_at_complete`
- `an_applied_role_flip_releases_every_held_slot_on_that_node` (**[R3]**, joint with c31-04)

### 3. Implementation surface

- `frogdb-cluster-runtime/src/handoff_barrier.rs` — the arming/release logic, the per-migration
  byte cap, the boot reconstruction, the held-set release on self-fence.
- `frogdb-cluster-runtime/src/migration_events.rs` — the release events c31-05's transitions emit
  arrive here.
- `frogdb-server` routing and command dispatch — `ASKING` becomes a no-op, `MOVED` names the
  source, the `RESTORE` exemption is removed, the `EXEC`/`WATCH`/script paths get their
  dispositions.
- The eviction and active-expiry paths gain a sealed-slot suspension.

## Acceptance criteria

- [ ] All owned rows amended; FM-CLUSTER-080's exemption retired **with its forcing test
      re-pointed in the same change** — leaving the old assertion alive pins the behaviour being
      removed; FM-CLUSTER-095's finalization-refusal arm retired the same way.
- [ ] The disposition table is **total** over record-removing transitions, and its forcing test is
      a walk over the writer-join row, not a hand-maintained list.
- [ ] The §3 exempt-set row and c31-04's staged-flip fence row cross-reference each other and
      state that their sets differ (V14-m4).
- [ ] `just lint-spec` green.
- [ ] Every forcing test observed failing before implementation, green after.
- [ ] `just mutants-diff frogdb-cluster-runtime` run and triaged; `just mutants-diff frogdb-server`
      where server code changed.
- [ ] `just lint` / `just lint-gates` green — the **redirect-reply seam** is directly load-bearing:
      every `-MOVED` / `-TRYAGAIN` / `-CLUSTERDOWN` in the disposition table must route through it.
- [ ] The cross-shard VLL hole (FM-CLUSTER-096) is **restated, not closed**, with its changed
      consequence and its containment written down.

## Blocked by

- **c31-05** (wave 3) — the disposition table is a total function of the record's exits; the cap is
  a `Begin`-stamped record field.
- **c31-03** (wave 2) — the `RemoveNode` exit row's two arms follow that cluster's prune semantics.
- **c31-04** (wave 2) — the exempt-set cross-reference and the joint R3 invariant.

## Blocks

- c31-08 (TR-CLUSTER-035's held-write obligation needs the table to exist).

## Related trackers

Closes on landing: cluster-correctness issue 29 (cap ambiguity — resolved by the per-migration cap
row) and issue 32 (a restarted source never re-arms its barrier — FM-CLUSTER-104's restart arm and
the §3 boot-reconstruction rule).
