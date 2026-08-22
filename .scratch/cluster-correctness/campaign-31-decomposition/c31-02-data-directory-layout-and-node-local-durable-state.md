# c31-02: Data-directory layout, node-local durable state, and boot markers

Status: DRAFT — pending wave-0 approval
Wave: 1 (parallel with c31-01)
Size: M
Crates: `frogdb-persistence`, `frogdb-recovery`

> Per [R1](../2026-08-22-work-item-rulings.md#r1--issue-31-campaign-staging-clustered-waves),
> each LOCKED row lands atomically with its forcing test and implementation.

This cluster owns the persistence half of the campaign's foundation: the data-directory layout
raise, the two new FrogDB-owned top-level entries, the unconditional layout marker, and the durable
homes for the node-local state that c31-04's staged-flip protocol depends on. It touches **no**
`specs/cluster.md` row at all, which is what makes it safely parallel with c31-01.

The design doc's revision-23 through revision-25 review produced a **declared, numbered list of
persistence amendments** with an explicit cumulative count: five rows amended, seven invariant
deltas, six declarations. This cluster is exactly that list.

## Owned rows

| # | Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|---|
| (1) | FM-PERSISTENCE-049 | Amended | `verify`'s outcome set gains a third member — an existing directory with its `layout_version` **raised**, published by a second marker write. The fourth exit (the populated-directory bail) must be named wherever the outcome set is enumerated | blast radius, revision-23 declared amendment 1 |
| (2) | FM-PERSISTENCE-049 | Amended | The marker phase moves **out of** the `rocks_backed` gate; the layout marker becomes unconditional | blast radius, revision-23 declared amendment 2 |
| (4) | FM-PERSISTENCE-048 | Amended | `contains_foreign_files`'s excused set widens by exactly two FrogDB-owned top-level entries (`raft`, `replication_state.json`); the "and nothing else" clause is retained verbatim | blast radius, revision-23 declared amendment 4 |
| (5) | FM-PERSISTENCE-057 | Amended | The layout's declared path set grows from four paths to six, adding `frogdb_node_identity` and `frogdb_raft_discard`; the single-owner rule extends over both | blast radius, revision-23 declared amendment 5 |
| (6a) | FM-PERSISTENCE-057 | Amended | The row's own excused-set sentence widens in step with (4) — a second, distinct delta on the same Invariant cell | blast radius, revision-23 declared amendment 6a |
| (6b) | FM-PERSISTENCE-059 | Amended | The excused-set sentence widens for `contains_foreign_files` but **not** for `pending_install`, whose "exactly those names" clause stays `staging*` / `backup` | blast radius, revision-23 declared amendment 6b |

Declared amendment **(3)** (TR-CLUSTER-035's Raft-store discard obligation) belongs to this same
numbered list but is a `specs/cluster.md` row and lands in **c31-08** with the row's two other
deltas. It is named here so the numbered list is not silently broken.

### Rows read as binding constraints and **not** moved

Record these as read-and-confronted in the implementation notes; do not amend them.

- **FM-PERSISTENCE-050** — the layout-version gate. Gains a **stated downgrade cost** (a
  version-2 directory is not openable by a version-1 binary) and a recovery row describing the
  operator's path. The row itself is unchanged.
- **FM-PERSISTENCE-051** — the marker's own integrity rules.
- **FM-PERSISTENCE-023** — the fsync discipline the new durable state must obey.
- **FM-PERSISTENCE-059's mint-once clause** — unchanged; only the excused-set sentence moves (6b).
- **TR-PERSISTENCE-051** — read as the transition-side constraint on the marker write.

### Hard internal ordering

**Declared amendment (4) must land before or with (2).** Making the marker unconditional (2) means
a non-`rocks_backed` directory now contains a FrogDB-owned entry that
`contains_foreign_files` has not been taught to excuse; shipping (2) alone turns every such
directory into a foreign-files refusal. The design doc calls this out explicitly. Land (4) first,
or land (2) and (4) in one commit.

## What to build

### 1. Spec deltas (first)

Amend `specs/persistence.md`:

1. FM-PERSISTENCE-048's excused set — the two named entries, "and nothing else" retained.
2. FM-PERSISTENCE-057's path set (four → six) and its excused-set sentence.
3. FM-PERSISTENCE-059's `contains_foreign_files` sentence only; leave `pending_install` alone and
   add a note saying so, because the two sentences read alike and a future sweep will otherwise
   "fix" the asymmetry.
4. FM-PERSISTENCE-049's outcome set (three outcomes plus the named fourth exit) and the marker's
   move out of the `rocks_backed` gate.
5. A recovery row for FM-PERSISTENCE-050's stated downgrade cost.

`specs/persistence.md` **has** a `Forced by` column; use it. Every amended row names its forcing
tests there, and `just lint-spec` will check both directions.

### 2. Forcing tests (second, observed failing)

In `frogdb-persistence` and `frogdb-recovery` respectively, matching the crate that owns the
mutated code:

- `verify_raises_layout_version_on_an_existing_directory`
- `layout_raise_publishes_a_second_marker_write`
- `verify_bails_on_a_populated_directory_without_a_marker`
- `marker_is_written_even_when_not_rocks_backed`
- `contains_foreign_files_excuses_raft_and_replication_state_and_nothing_else`
- `layout_declares_six_paths_each_with_a_single_owner`
- `pending_install_name_set_is_unchanged_by_the_excused_set_widening`
- `node_identity_survives_process_restart`
- `raft_discard_mark_survives_process_restart`
- `stage_counter_state_is_fsynced_before_the_stage_is_reported`
- `a_version_one_directory_is_refused_by_a_version_two_binary_with_a_named_cost`

### 3. Implementation surface

- `frogdb-persistence/src/data_dir.rs` (around `:154-161`) — `DATA_DIR_LAYOUT_VERSION` 1 → 2; the
  marker write moves out of the `rocks_backed` branch.
- `frogdb-recovery/src/data_dir.rs` — `:61-173` is the verify path; `:88-95` the foreign-files
  check; `:110-121` the marker read; `:188-196` the layout enumeration. The raise-and-republish
  outcome is new control flow here, not a tweak.
- `frogdb-recovery/src/lib.rs` (`:188-190`) and `frogdb-recovery/src/cluster.rs` (`:23-25`) — the
  boot ordering that reads the discard mark before the Raft store is opened.
- New durable stores: `frogdb_node_identity` (the incarnation whose contract c31-01 declares) and
  `frogdb_raft_discard` (the mark c31-08's HARD reset writes).
- The **pending-transition record** (`{kind, target_upstream, stage_id, adopted,
  staged_registration_seq}`) and **`stage_counter_state`** get their durable homes and fsync
  discipline here. Their *protocol* — the level-triggered adoption, the fail-closed exits, the
  mint-time floor — is c31-04's; this cluster provides read/write/fsync/boot-load and nothing more.

## Acceptance criteria

- [ ] All six declared amendments landed, with (4) at or before (2).
- [ ] `Forced by` populated for every amended `specs/persistence.md` row; `just lint-spec` green.
- [ ] Every forcing test observed failing before implementation, green after.
- [ ] `just mutants-diff frogdb-persistence` and `just mutants-diff frogdb-recovery` run and
      triaged.
- [ ] The five read-not-moved rows recorded in the implementation notes as confronted, with the
      FM-PERSISTENCE-050 downgrade cost written down rather than left implicit.
- [ ] Declared amendment (3) explicitly deferred to c31-08 with a cross-reference, so the numbered
      list reads complete.
- [ ] `just lint` and `just lint-gates` green.

## Blocked by

Nothing. This cluster can start immediately on approval.

## Blocks

- c31-04 (the pending-transition record and `stage_counter_state` need durable homes)
- c31-08 (`frogdb_raft_discard` must exist before TR-CLUSTER-035's HARD path can mark it)
