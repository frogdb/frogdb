# 05 — Frozen encoding fixtures for ClusterCommand + ClusterStateInner

Status: done

## Parent

[PRD](../../PRD.md) §3 W2 (round-2 87/F6).

## What to build

Golden JSON fixtures checked into `frogdb-cluster` tests for every `ClusterCommand`
variant and a populated `ClusterStateInner`, with round-trip assertions both directions
(serialize matches the golden file; the golden file deserializes to the value). A silent
serde rename is a rolling-upgrade wire break today — Raft log entries and snapshots cross
node versions.

## Acceptance criteria

- [x] Golden files cover all 18 command variants + a state with every collection
      populated (nodes, slots, migrations, live handoff, nonzero `handoff_seq`)
- [x] Renaming any serde field fails a test naming the golden file
- [x] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

None - can start immediately.

## Resolution

`frogdb-server/crates/cluster/src/encoding_golden.rs` (a `#[cfg(test)]` module) pins the
JSON encodings under `frogdb-server/crates/cluster/testdata/encoding/`: one file per
`ClusterCommand` variant (18) plus `state-inner.json`. JSON is the real vehicle — the Raft
log stores serialized `Entry<TypeConfig>` and a snapshot is a serialized
`ClusterStateInner` (`storage.rs`), both `serde_json` — so the field names and variant tags
in these files *are* the cross-version wire format.

Coverage, and why each piece is there:

- `fixture_name` matches every variant with **no wildcard arm**, so a new
  `ClusterCommand` variant fails to compile until it is named and recorded. A count
  assertion (`every_cluster_command_variant_has_a_fixture`) catches the other half: a
  variant named in the match but missing from the fixture table, and two fixtures sharing
  a file.
- Every field in a fixture is off its default, and same-typed neighbours
  (`source_node`/`target_node`, `barrier_ms`/`lease_ms`) carry different values, so a
  swapped pair shows up as a diff instead of encoding identically. `NodeInfo::version` is
  pinned to a literal rather than `CARGO_PKG_VERSION`: a release bump must not churn a
  fixture whose subject is the shape of the record.
- The state fixture carries two primaries and a replica naming one of them, slots owned by
  both, a migration with a prepared+drained `SlotHandoff` **and** a second with
  `handoff: None` (the field is `#[serde(default)]`, so both renderings are contract),
  `handoff_seq` past the live handoff's `seq`, a nonzero `config_epoch`, an
  `active_version`, and the Raft bookkeeping (`last_applied_log`, a non-default
  `last_membership`) that a restore reads back.
- `assert_golden` checks **both** directions. Encode-side catches a rename this version
  would write; decode-side catches the mirror image, where a `#[serde(alias)]` or a
  `default` silently absorbs a difference the encode check would not see. `ClusterCommand`
  has no `PartialEq`, and adding derives to production types to serve a test is the wrong
  trade, so the decoded value is compared by its `Debug` rendering.
- Mismatches report only the first differing line. These documents run past a hundred
  lines; printing both in full buries the one line a reviewer needs.

Regeneration is `UPDATE_GOLDEN=1 just test frogdb-cluster encoding_golden`, and the failure
message says so. The diff it produces is the review artifact: every line is something a
peer on the old encoding has to be able to read.

Verification. Both negative cases were run against the real types and then reverted:

- `#[serde(rename = "src")]` on `SlotMigration::source_node` →
  `cluster_state_encoding_matches_its_golden_file` fails with
  `the encoding of state-inner no longer matches testdata/encoding/state-inner.json`,
  `line 61: recorded: "source_node": 1 / current: "src": 1`.
- `#[serde(rename = "CancelMigration")]` on `ClusterCommand::CancelSlotMigration` →
  `cluster_command_encodings_match_their_golden_files` fails naming
  `testdata/encoding/command-cancel-slot-migration.json`.

`just test frogdb-cluster` 237/237, `just lint frogdb-cluster` plus
`cargo clippy -p frogdb-cluster --tests` clean (the scoped lint recipe does not pass
`--tests`, and this module is test-only), `just fmt`.

`just mutants-diff frogdb-cluster` after this change: 13 mutants, 10 caught, 3 unviable,
0 missed — nothing to triage. The count is unchanged from the issue-01 commit because the
whole module is `#[cfg(test)]`, which cargo-mutants does not mutate; what it does confirm
is that the fixtures did not weaken the crate's existing coverage.

No `FM-CLUSTER-NNN` tags: these tests pin an encoding, not a failure mode, and the spec has
no row for wire compatibility to attach them to.
