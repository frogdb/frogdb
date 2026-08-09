//! Frozen encodings for the two types that cross node versions.
//!
//! Both of them travel as `serde_json`: the Raft log stores every
//! [`ClusterCommand`] as a serialized `Entry<TypeConfig>`
//! ([`crate::storage`]), and a Raft snapshot is
//! [`ClusterStateInner`] serialized whole
//! ([`ClusterState::build_snapshot`](crate::state::ClusterState), installed by
//! `install_snapshot`). JSON is self-describing, so the *field names and
//! variant tags are the wire format*: renaming `source_node`, retagging a
//! command variant, or dropping a `#[serde(default)]` breaks a node reading a
//! log or snapshot written by its peer. During a rolling upgrade that is
//! exactly what happens, and the symptom is a follower that cannot apply the
//! leader's entries rather than a compile error.
//!
//! A round-trip test cannot see any of that: a renamed field round-trips
//! perfectly against itself. Only a golden file recorded by the *previous*
//! version can, so every `ClusterCommand` variant and one fully populated
//! `ClusterStateInner` are pinned byte-for-byte in `testdata/encoding/`.
//!
//! # Changing an encoding on purpose
//!
//! ```text
//! UPDATE_GOLDEN=1 just test frogdb-cluster encoding_golden
//! ```
//!
//! rewrites the fixtures from the current types. The diff it produces is the
//! review artifact: every line of it is a change some other node has to be able
//! to read, so it belongs in the commit message with the compatibility argument
//! (a `#[serde(alias)]`, a `#[serde(default)]`, or a version gate).

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::state::ClusterStateInner;
use crate::types::{
    ClusterCommand, NodeFlags, NodeInfo, NodeRole, SlotHandoff, SlotMigration, SlotRange,
};

/// Where the fixtures live. `CARGO_MANIFEST_DIR` rather than `include_str!` so
/// the regeneration mode can write them back.
fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/encoding")
}

/// True when the run is regenerating fixtures instead of checking them.
fn updating() -> bool {
    std::env::var_os("UPDATE_GOLDEN").is_some_and(|v| !v.is_empty())
}

/// Assert `value`'s JSON is exactly `testdata/encoding/<name>.json`, and that
/// the file still decodes back to `value`.
///
/// Both directions are load-bearing. Encode-side catches a rename or a new
/// field: this version would write something the pinned peer cannot read.
/// Decode-side catches the mirror image — a field this version can no longer
/// read, or reads into a different value — which is the half a
/// serialize-only comparison misses when a `#[serde(alias)]` or a `default`
/// silently absorbs the difference.
fn assert_golden<T>(name: &str, value: &T)
where
    T: Serialize + DeserializeOwned + std::fmt::Debug,
{
    let path = golden_dir().join(format!("{name}.json"));
    let encoded = format!(
        "{}\n",
        serde_json::to_string_pretty(value).expect("the fixture value must serialize")
    );

    if updating() {
        std::fs::create_dir_all(golden_dir()).expect("creating testdata/encoding");
        std::fs::write(&path, &encoded).unwrap_or_else(|e| panic!("writing {name}.json: {e}"));
        return;
    }

    let golden = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "testdata/encoding/{name}.json is missing ({e}). A new fixture is recorded with \
             `UPDATE_GOLDEN=1 just test frogdb-cluster encoding_golden`."
        )
    });
    if encoded != golden {
        panic!(
            "the encoding of `{name}` no longer matches testdata/encoding/{name}.json:\n{}\n\nA \
             node running the pinned version reads this type out of the Raft log and out of \
             snapshots. If the change is intended, re-record with `UPDATE_GOLDEN=1 just test \
             frogdb-cluster encoding_golden` and say in the commit message how a peer on the old \
             encoding still reads the new one.",
            first_difference(&golden, &encoded)
        );
    }

    let decoded: T = serde_json::from_str(&golden).unwrap_or_else(|e| {
        panic!(
            "testdata/encoding/{name}.json no longer deserializes ({e}) — this version cannot \
             read a log entry or snapshot written by the pinned one."
        )
    });
    assert_eq!(
        format!("{decoded:?}"),
        format!("{value:?}"),
        "testdata/encoding/{name}.json still parses but decodes to a different value"
    );
}

/// The first line where the recorded and current encodings diverge.
///
/// These documents run to a hundred-odd lines; printing both in full buries the
/// one line that changed, which is the only thing a reviewer needs to judge
/// whether the change is wire-compatible.
fn first_difference(golden: &str, encoded: &str) -> String {
    let (golden_lines, encoded_lines): (Vec<&str>, Vec<&str>) =
        (golden.lines().collect(), encoded.lines().collect());
    for (i, (want, got)) in golden_lines.iter().zip(encoded_lines.iter()).enumerate() {
        if want != got {
            return format!(
                "  line {}:\n    recorded: {}\n    current:  {}",
                i + 1,
                want.trim_end(),
                got.trim_end()
            );
        }
    }
    format!(
        "  the shared prefix matches; the encodings differ in length ({} recorded lines vs {} \
         current)",
        golden_lines.len(),
        encoded_lines.len()
    )
}

/// A node record with every field off its default, so a fixture cannot pass by
/// accident: a dropped field would still have to reproduce these values.
fn fixture_node(id: u64, role: NodeRole, primary_id: Option<u64>) -> NodeInfo {
    NodeInfo {
        id,
        addr: format!("10.0.0.{id}:6379").parse().expect("fixture addr"),
        cluster_addr: format!("10.0.0.{id}:16379").parse().expect("fixture addr"),
        role,
        primary_id,
        config_epoch: 40 + id,
        flags: NodeFlags {
            handshake: false,
            fail: false,
            pfail: true,
            noaddr: false,
        },
        replica_priority: 7,
        // Pinned, never `CARGO_PKG_VERSION`: a release bump must not churn a
        // fixture whose subject is the *shape* of the record.
        version: "1.2.3".to_string(),
    }
}

/// The fixture name of a command, and the exhaustiveness gate for this module.
///
/// The match has no wildcard arm, so a new `ClusterCommand` variant fails to
/// compile here — which is the reminder to add it to [`command_fixtures`] and
/// record its golden file. Nothing else in the crate would notice: an
/// unpinned variant is exactly the one whose encoding nobody is guarding.
fn fixture_name(command: &ClusterCommand) -> &'static str {
    match command {
        ClusterCommand::AddNode { .. } => "command-add-node",
        ClusterCommand::RemoveNode { .. } => "command-remove-node",
        ClusterCommand::AssignSlots { .. } => "command-assign-slots",
        ClusterCommand::RemoveSlots { .. } => "command-remove-slots",
        ClusterCommand::SetRole { .. } => "command-set-role",
        ClusterCommand::IncrementEpoch => "command-increment-epoch",
        ClusterCommand::SetConfigEpoch { .. } => "command-set-config-epoch",
        ClusterCommand::Failover { .. } => "command-failover",
        ClusterCommand::MarkNodeFailed { .. } => "command-mark-node-failed",
        ClusterCommand::MarkNodeRecovered { .. } => "command-mark-node-recovered",
        ClusterCommand::BeginSlotMigration { .. } => "command-begin-slot-migration",
        ClusterCommand::PrepareSlotHandoff { .. } => "command-prepare-slot-handoff",
        ClusterCommand::ConfirmSlotHandoffDrained { .. } => "command-confirm-slot-handoff-drained",
        ClusterCommand::AbortSlotHandoff { .. } => "command-abort-slot-handoff",
        ClusterCommand::CompleteSlotMigration { .. } => "command-complete-slot-migration",
        ClusterCommand::CancelSlotMigration { .. } => "command-cancel-slot-migration",
        ClusterCommand::FinalizeUpgrade { .. } => "command-finalize-upgrade",
        ClusterCommand::ResetCluster { .. } => "command-reset-cluster",
    }
}

/// How many variants `ClusterCommand` has. Paired with [`fixture_name`]'s
/// wildcard-free match: the compiler catches a new variant, this catches a
/// fixture that was named but never added to the table.
const CLUSTER_COMMAND_VARIANTS: usize = 18;

/// One populated instance of every `ClusterCommand` variant.
///
/// Every numeric field within a variant carries a *different* value, so a
/// fixture that swapped two same-typed fields (`source_node`/`target_node`,
/// `barrier_ms`/`lease_ms`) shows up as a diff rather than encoding
/// identically.
fn command_fixtures() -> Vec<ClusterCommand> {
    vec![
        ClusterCommand::AddNode {
            node: fixture_node(1, NodeRole::Primary, None),
        },
        ClusterCommand::RemoveNode { node_id: 2 },
        ClusterCommand::AssignSlots {
            node_id: 3,
            slots: vec![SlotRange::new(0, 99), SlotRange::single(16383)],
        },
        ClusterCommand::RemoveSlots {
            node_id: 4,
            slots: vec![SlotRange::new(100, 199)],
        },
        ClusterCommand::SetRole {
            node_id: 5,
            role: NodeRole::Replica,
            primary_id: Some(6),
        },
        ClusterCommand::IncrementEpoch,
        ClusterCommand::SetConfigEpoch {
            node_id: 7,
            epoch: 8,
        },
        ClusterCommand::Failover {
            old_primary_id: 9,
            new_primary_id: 10,
            force: true,
        },
        ClusterCommand::MarkNodeFailed { node_id: 11 },
        ClusterCommand::MarkNodeRecovered { node_id: 12 },
        ClusterCommand::BeginSlotMigration {
            slot: 300,
            source_node: 13,
            target_node: 14,
        },
        ClusterCommand::PrepareSlotHandoff {
            slot: 301,
            source_node: 15,
            target_node: 16,
            barrier_ms: 1_500,
            lease_ms: 9_000,
            proposed_at_ms: 1_700_000_000_123,
        },
        ClusterCommand::ConfirmSlotHandoffDrained { slot: 302, seq: 17 },
        ClusterCommand::AbortSlotHandoff { slot: 303, seq: 18 },
        ClusterCommand::CompleteSlotMigration {
            slot: 304,
            source_node: 19,
            target_node: 20,
            proposed_at_ms: 1_700_000_000_456,
        },
        ClusterCommand::CancelSlotMigration { slot: 305 },
        ClusterCommand::FinalizeUpgrade {
            version: "1.2.3".to_string(),
        },
        ClusterCommand::ResetCluster {
            node_id: 21,
            new_node_id: Some(22),
        },
    ]
}

/// A state with every collection non-empty and every scalar off its default —
/// two primaries and a replica that names one of them, slots owned by both
/// primaries, an open migration carrying a prepared *and* drained handoff, a
/// handoff generation past that handoff's `seq`, a nonzero config epoch, a
/// finalized active version, and the Raft bookkeeping (`last_applied_log`,
/// `last_membership`) a restore reads back.
///
/// A default-valued field is a field whose absence from the JSON nobody would
/// notice, which is the whole failure this fixture exists to catch.
fn state_fixture() -> ClusterStateInner {
    let mut nodes = std::collections::BTreeMap::new();
    for node in [
        fixture_node(1, NodeRole::Primary, None),
        fixture_node(2, NodeRole::Primary, None),
        fixture_node(3, NodeRole::Replica, Some(1)),
    ] {
        nodes.insert(node.id, node);
    }

    let mut slot_assignment = std::collections::BTreeMap::new();
    slot_assignment.insert(0u16, 1u64);
    slot_assignment.insert(1, 1);
    slot_assignment.insert(16383, 2);

    let mut migrations = std::collections::BTreeMap::new();
    migrations.insert(
        7u16,
        SlotMigration {
            slot: 7,
            source_node: 1,
            target_node: 2,
            handoff: Some(SlotHandoff {
                seq: 4,
                prepared_at_ms: 1_700_000_000_000,
                barrier_ms: 1_500,
                lease_ms: 9_000,
                drained: true,
            }),
        },
    );
    // A second migration with no handoff prepared: `handoff` is
    // `#[serde(default)]`, so both the `Some` and the `None` rendering are part
    // of the contract.
    migrations.insert(8u16, SlotMigration::new(8, 2, 1));

    let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(6, 1), 42);
    let membership = openraft::Membership::new(
        vec![[1u64, 2, 3].into_iter().collect()],
        std::collections::BTreeMap::from([
            (1u64, openraft::BasicNode::new("10.0.0.1:16379")),
            (2, openraft::BasicNode::new("10.0.0.2:16379")),
            (3, openraft::BasicNode::new("10.0.0.3:16379")),
        ]),
    );

    ClusterStateInner {
        nodes,
        slot_assignment,
        config_epoch: 23,
        migrations,
        // Past the live handoff's `seq`: the generation counter is the one
        // piece of state a restore cannot re-derive (FM-CLUSTER-100), so a
        // fixture that left it at zero would pin nothing.
        handoff_seq: 5,
        last_applied_log: Some(log_id),
        last_membership: openraft::StoredMembership::new(Some(log_id), membership),
        active_version: Some("1.2.3".to_string()),
    }
}

/// Every variant is in the table exactly once, so the golden run below cannot
/// silently skip one. The compiler owns the other half: [`fixture_name`]'s
/// match rejects a new variant outright.
#[test]
fn every_cluster_command_variant_has_a_fixture() {
    let fixtures = command_fixtures();
    let mut names: Vec<&str> = fixtures.iter().map(fixture_name).collect();
    names.sort_unstable();
    let unique = names.len();
    names.dedup();
    assert_eq!(names.len(), unique, "two fixtures share a golden file");
    assert_eq!(
        names.len(),
        CLUSTER_COMMAND_VARIANTS,
        "ClusterCommand has {CLUSTER_COMMAND_VARIANTS} variants but the fixture table covers \
         {}: add the missing one and record its golden file with UPDATE_GOLDEN=1",
        names.len()
    );
}

#[test]
fn cluster_command_encodings_match_their_golden_files() {
    for command in command_fixtures() {
        assert_golden(fixture_name(&command), &command);
    }
}

#[test]
fn cluster_state_encoding_matches_its_golden_file() {
    assert_golden("state-inner", &state_fixture());
}
