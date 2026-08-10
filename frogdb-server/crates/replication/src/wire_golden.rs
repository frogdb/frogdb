//! Frozen wire-byte fixtures for [`ReplicationFrame`] and [`FullSyncMetadata`].
//!
//! These are the two encodings that cross the wire between a primary and a
//! replica on every full-sync and every steady-state write:
//! [`ReplicationFrame`] wraps each replicated command (`frame.rs`) and
//! [`FullSyncMetadata`] is the trailer a checkpoint stream ends with
//! (`fullsync.rs`). [`crate::version_compat`] gates *majors* at the `PSYNC`
//! handshake; nothing gates the byte *layout* within a major, so two nodes
//! running different patch builds of the same major are assumed to agree on
//! it by construction. A round-trip test (`frame.rs`'s
//! `test_frame_encode_decode`, `fullsync.rs`'s `test_metadata_serialization`)
//! cannot see a layout drift — reordering `sequence` and `shard_id`, say —
//! because encoding and decoding with the same build always agrees with
//! itself. Only a fixture recorded from a *previous* build can catch that,
//! which is what this module pins, byte-for-byte, in `testdata/wire/`.
//!
//! Every fixture is asserted in both directions: encoding the fixture's Rust
//! value must reproduce the checked-in bytes exactly, and decoding the
//! checked-in bytes must reproduce the fixture's value exactly. Either
//! direction alone would miss half of a real drift — an encoder that changed
//! but whose decoder happened to still parse the old shape, or the reverse.
//!
//! # A mismatch here is not "update the fixture"
//!
//! Unlike `frogdb-cluster`'s `encoding_golden` (JSON, self-describing, and
//! tolerant of additive changes via `#[serde(default)]`/`#[serde(alias)]`),
//! neither format here carries a self-describing shape. [`ReplicationFrame`]
//! has exactly one field that says which layout the rest of the header is —
//! [`crate::frame::FRAME_VERSION`] — and the decoder refuses anything but an
//! exact match (`frame.rs`'s `decode_accepts_only_this_builds_frame_version`).
//! [`FullSyncMetadata`] has no version field at all; its only protection is
//! the handshake-level major check. So a mismatch here means one of two
//! things, and the failure message ([`wire_break_message`]) spells out both:
//! a bug (fix the code, the fixture is correct), or an intentional layout
//! change, which is a wire break within the current major and has to be
//! accompanied by a version bump — never a silently regenerated fixture that
//! would hide the exact rolling-upgrade break this module exists to catch.
//!
//! # Recording a fixture
//!
//! ```text
//! UPDATE_GOLDEN=1 just test frogdb-replication wire_golden
//! ```
//!
//! writes every fixture below from the current encoders. Only run this after
//! deciding — and saying in the commit message — how a peer on the previous
//! version is refused rather than desynchronized (see above).

use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::frame::{FRAME_VERSION, FrameFlags, ReplicationFrame};
use crate::fullsync::FullSyncMetadata;

/// Where the fixtures live. `CARGO_MANIFEST_DIR` rather than `include_str!`
/// so the regeneration mode ([`updating`]) can write them back.
fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/wire")
}

/// True when the run is regenerating fixtures instead of checking them.
fn updating() -> bool {
    std::env::var_os("UPDATE_GOLDEN").is_some_and(|v| !v.is_empty())
}

/// Read `testdata/wire/<name>.hex` back into raw bytes.
///
/// Used after [`assert_golden_bytes`] to drive the decode-side assertion
/// from the checked-in file itself (not from a copy of the just-encoded
/// bytes still in memory), so a corrupted or hand-edited fixture is what the
/// decode-side check actually exercises.
fn read_golden_bytes(name: &str) -> Bytes {
    let path = golden_dir().join(format!("{name}.hex"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading testdata/wire/{name}.hex: {e}"));
    let bytes = hex::decode(text.trim())
        .unwrap_or_else(|e| panic!("testdata/wire/{name}.hex is not valid hex: {e}"));
    Bytes::from(bytes)
}

/// Assert `encoded` is exactly `testdata/wire/<name>.hex` (one line, lowercase
/// hex, so a binary diff is still readable in a PR and in `git diff`).
///
/// In `UPDATE_GOLDEN=1` mode this writes the fixture instead of checking it —
/// see the module docs for why that is a deliberate, opt-in escape hatch and
/// not the answer to a failing assertion.
fn assert_golden_bytes(name: &str, encoded: &[u8], type_name: &'static str) {
    let path = golden_dir().join(format!("{name}.hex"));
    let rendered = format!("{}\n", hex::encode(encoded));

    if updating() {
        std::fs::create_dir_all(golden_dir()).expect("creating testdata/wire");
        std::fs::write(&path, &rendered).unwrap_or_else(|e| panic!("writing {name}.hex: {e}"));
        return;
    }

    let golden = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "testdata/wire/{name}.hex is missing ({e}). A new fixture is recorded with \
             `UPDATE_GOLDEN=1 just test frogdb-replication wire_golden`."
        )
    });

    if rendered != golden {
        panic!(
            "{}",
            wire_break_message(name, type_name, &golden, &rendered)
        );
    }
}

/// The panic message for a golden-bytes mismatch.
///
/// A standalone function (rather than inline in [`assert_golden_bytes`]) so
/// its wording is itself under test —
/// [`tests::wire_break_message_tells_the_developer_to_bump_the_version`]
/// pins the rule it states, not just that *some* message fires.
fn wire_break_message(name: &str, type_name: &'static str, golden: &str, rendered: &str) -> String {
    format!(
        "`{type_name}` no longer encodes to testdata/wire/{name}.hex.\n\
         \n\
         recorded (golden): {golden_trimmed}\n\
         current  (build):  {rendered_trimmed}\n\
         \n\
         This fixture pins the {type_name} BYTE LAYOUT that crosses the wire between a primary \
         and a replica running different patch builds of the SAME major version. \
         `version_compat.rs` gates only the *major* announced at the PSYNC handshake — nothing \
         else stops two same-major nodes from silently disagreeing about this layout, and a \
         disagreement here is a rolling upgrade breaking mid-flight, not a compile error.\n\
         \n\
         If this diff is NOT something you meant to change: it is a bug — fix the code, do not \
         touch the fixture.\n\
         \n\
         If this diff IS an intentional layout change: it is a wire break within the current \
         major, so the fix is to BUMP THE VERSION — `FRAME_VERSION` in frame.rs for \
         `ReplicationFrame` (whose decoder already refuses any version but its own), or the \
         crate/handshake major that `version_compat.rs` checks for `FullSyncMetadata`, which \
         carries no version field of its own and is protected only by that gate. Do NOT \
         regenerate this fixture to make the failure go away — a regenerated fixture with no \
         version bump hides exactly the silent-drift rolling-upgrade break this file exists to \
         catch. Once the version is bumped, re-record with `UPDATE_GOLDEN=1 just test \
         frogdb-replication wire_golden` and say in the commit message how a peer on the old \
         version is refused rather than desynchronized.",
        golden_trimmed = golden.trim_end(),
        rendered_trimmed = rendered.trim_end(),
    )
}

/// Assert a [`ReplicationFrame`] fixture round-trips against its golden file
/// in both directions: encoding `frame` reproduces the checked-in bytes
/// exactly, and decoding the checked-in bytes reproduces every field of
/// `frame` exactly (not just that decoding succeeds).
fn assert_frame_golden(name: &str, frame: &ReplicationFrame) {
    let encoded = frame
        .encode()
        .unwrap_or_else(|e| panic!("fixture `{name}` failed to encode: {e}"));
    assert_golden_bytes(name, &encoded, "ReplicationFrame");

    let golden_bytes = read_golden_bytes(name);
    let decoded = ReplicationFrame::decode(golden_bytes)
        .unwrap_or_else(|e| panic!("testdata/wire/{name}.hex no longer decodes: {e}"));
    assert_eq!(
        decoded.version, frame.version,
        "{name}: version mismatch on decode"
    );
    assert_eq!(
        decoded.flags.bits(),
        frame.flags.bits(),
        "{name}: flags mismatch on decode"
    );
    assert_eq!(
        decoded.shard_id, frame.shard_id,
        "{name}: shard_id mismatch on decode"
    );
    assert_eq!(
        decoded.sequence, frame.sequence,
        "{name}: sequence mismatch on decode"
    );
    assert_eq!(
        decoded.payload, frame.payload,
        "{name}: payload mismatch on decode"
    );
}

/// Assert a [`FullSyncMetadata`] fixture round-trips against its golden file
/// in both directions, field-for-field (the type has no `PartialEq`, so the
/// comparison is spelled out rather than derived — see the module docs for
/// why this format in particular has no version field to fall back on).
fn assert_metadata_golden(name: &str, metadata: &FullSyncMetadata) {
    let encoded = metadata.to_bytes();
    assert_golden_bytes(name, &encoded, "FullSyncMetadata");

    let golden_bytes = read_golden_bytes(name);
    let decoded = FullSyncMetadata::from_bytes(&golden_bytes)
        .unwrap_or_else(|e| panic!("testdata/wire/{name}.hex no longer decodes: {e}"));
    assert_eq!(
        decoded.rdb_size, metadata.rdb_size,
        "{name}: rdb_size mismatch on decode"
    );
    assert_eq!(
        decoded.checksum, metadata.checksum,
        "{name}: checksum mismatch on decode"
    );
    assert_eq!(
        decoded.replication_id, metadata.replication_id,
        "{name}: replication_id mismatch on decode"
    );
    assert_eq!(
        decoded.replication_offset, metadata.replication_offset,
        "{name}: replication_offset mismatch on decode"
    );
}

/// One [`ReplicationFrame`] fixture per field and per flag bit, plus a
/// "kitchen sink" combining every field at once at a distinct, asymmetric
/// value — the shape that catches a field *swap* (e.g. `shard_id` and
/// `sequence` trading places) that a fixture varying one field at a time
/// would miss.
///
/// `shard_id` and `sequence` fixtures use byte patterns with every byte
/// distinct (`0x12_34`, `0x01_02_03_04_05_06_07_08`) rather than a repeated
/// or round value, so a byte-order transposition shows up as a mismatch
/// instead of accidentally re-deriving the same bytes.
fn frame_fixtures() -> Vec<(&'static str, ReplicationFrame)> {
    vec![
        (
            "frame-flags-none",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::NONE,
                shard_id: 0,
                sequence: 0,
                payload: Bytes::from_static(b"no-flags"),
            },
        ),
        (
            "frame-flag-compressed",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::COMPRESSED,
                shard_id: 0,
                sequence: 0,
                payload: Bytes::from_static(b"compressed-only"),
            },
        ),
        (
            "frame-flag-end-of-batch",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::END_OF_BATCH,
                shard_id: 0,
                sequence: 0,
                payload: Bytes::from_static(b"end-of-batch-only"),
            },
        ),
        (
            "frame-flag-require-ack",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::REQUIRE_ACK,
                shard_id: 0,
                sequence: 0,
                payload: Bytes::from_static(b"require-ack-only"),
            },
        ),
        (
            "frame-flags-all",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: {
                    let mut f = FrameFlags::NONE;
                    f.set(FrameFlags::COMPRESSED);
                    f.set(FrameFlags::END_OF_BATCH);
                    f.set(FrameFlags::REQUIRE_ACK);
                    f
                },
                shard_id: 0,
                sequence: 0,
                payload: Bytes::from_static(b"all-flags"),
            },
        ),
        (
            "frame-shard-control",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::NONE,
                shard_id: crate::frame::CONTROL_SHARD, // 0xFFFF
                sequence: 42,
                payload: Bytes::from_static(b"control-frame"),
            },
        ),
        (
            "frame-shard-tagged",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::NONE,
                shard_id: 0x1234,
                sequence: 99,
                payload: Bytes::from_static(b"data-frame-on-a-shard"),
            },
        ),
        (
            "frame-sequence-max",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::NONE,
                shard_id: 0,
                sequence: u64::MAX,
                payload: Bytes::from_static(b"max-sequence"),
            },
        ),
        (
            "frame-payload-empty",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::NONE,
                shard_id: 3,
                sequence: 5,
                payload: Bytes::new(),
            },
        ),
        (
            "frame-payload-binary",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: FrameFlags::NONE,
                shard_id: 9,
                sequence: 11,
                payload: Bytes::from_static(
                    b"\x00\x01\r\n\xfe\xff binary payload with control bytes",
                ),
            },
        ),
        (
            "frame-kitchen-sink",
            ReplicationFrame {
                version: FRAME_VERSION,
                flags: {
                    let mut f = FrameFlags::NONE;
                    f.set(FrameFlags::COMPRESSED);
                    f.set(FrameFlags::REQUIRE_ACK);
                    f
                },
                shard_id: 0xBEEF,
                sequence: 0x0102_0304_0506_0708,
                payload: Bytes::from_static(b"kitchen sink \r\n\x00 payload"),
            },
        ),
    ]
}

/// One [`FullSyncMetadata`] fixture per field boundary — all-zero, all-`0xFF`
/// checksum with max integers, a sequential-byte checksum that pins hex
/// encoding byte order, and a realistic-looking replication id — plus the
/// empty-string `replication_id` edge, which the wire format's `:`-split
/// parser must still see as exactly four fields.
fn metadata_fixtures() -> Vec<(&'static str, FullSyncMetadata)> {
    let mut sequential = [0u8; 32];
    for (i, byte) in sequential.iter_mut().enumerate() {
        *byte = i as u8;
    }

    vec![
        (
            "metadata-basic",
            FullSyncMetadata {
                rdb_size: 123_456,
                checksum: sequential,
                replication_id: "repl-basic-fixture".to_string(),
                replication_offset: 987_654_321,
            },
        ),
        (
            "metadata-zero",
            FullSyncMetadata {
                rdb_size: 0,
                checksum: [0u8; 32],
                replication_id: String::new(),
                replication_offset: 0,
            },
        ),
        (
            "metadata-max",
            FullSyncMetadata {
                rdb_size: u64::MAX,
                checksum: [0xFFu8; 32],
                replication_id: "z".repeat(64),
                replication_offset: u64::MAX,
            },
        ),
        (
            "metadata-typical-replid",
            FullSyncMetadata {
                rdb_size: 42,
                checksum: [
                    0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE,
                    0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD,
                    0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF,
                ],
                replication_id: "3b9f1c2a4d5e6f708192a3b4c5d6e7f809101112".to_string(),
                replication_offset: 555,
            },
        ),
    ]
}

/// Fixture count guards, paired with the dedup checks in
/// [`tests::frame_fixture_names_are_unique`] /
/// [`tests::metadata_fixture_names_are_unique`]: a fixture accidentally
/// dropped while editing this file shrinks the count and fails loudly,
/// rather than silently narrowing coverage.
const FRAME_FIXTURE_COUNT: usize = 11;
const METADATA_FIXTURE_COUNT: usize = 4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_fixture_names_are_unique() {
        let fixtures = frame_fixtures();
        let mut names: Vec<&str> = fixtures.iter().map(|(name, _)| *name).collect();
        names.sort_unstable();
        let unique = names.len();
        names.dedup();
        assert_eq!(
            names.len(),
            unique,
            "two ReplicationFrame fixtures share a golden file name"
        );
        assert_eq!(
            names.len(),
            FRAME_FIXTURE_COUNT,
            "expected {FRAME_FIXTURE_COUNT} ReplicationFrame fixtures, found {}",
            names.len()
        );
    }

    #[test]
    fn metadata_fixture_names_are_unique() {
        let fixtures = metadata_fixtures();
        let mut names: Vec<&str> = fixtures.iter().map(|(name, _)| *name).collect();
        names.sort_unstable();
        let unique = names.len();
        names.dedup();
        assert_eq!(
            names.len(),
            unique,
            "two FullSyncMetadata fixtures share a golden file name"
        );
        assert_eq!(
            names.len(),
            METADATA_FIXTURE_COUNT,
            "expected {METADATA_FIXTURE_COUNT} FullSyncMetadata fixtures, found {}",
            names.len()
        );
    }

    /// The byte-exact and value-exact assertion, both directions, for every
    /// `ReplicationFrame` fixture. A version bump, a reordered header field,
    /// or a changed flag bit position fails this test with
    /// [`wire_break_message`]'s explanation rather than a bare byte diff.
    #[test]
    fn replication_frame_encodings_match_their_golden_files() {
        for (name, frame) in frame_fixtures() {
            assert_frame_golden(name, &frame);
        }
    }

    /// Same as above for `FullSyncMetadata` — the checkpoint trailer that has
    /// no version field of its own to fall back on.
    #[test]
    fn full_sync_metadata_encodings_match_their_golden_files() {
        for (name, metadata) in metadata_fixtures() {
            assert_metadata_golden(name, &metadata);
        }
    }

    /// The failure message is part of the contract this module exists to
    /// enforce — a developer staring at a red CI run must be told the rule,
    /// not just shown two hex strings. Pinned directly against
    /// [`wire_break_message`] rather than by tripping a real mismatch, so
    /// this test needs no scratch fixture directory of its own.
    #[test]
    fn wire_break_message_tells_the_developer_to_bump_the_version() {
        let message = wire_break_message("frame-example", "ReplicationFrame", "aabb\n", "aacc\n");

        assert!(
            message.contains("version_compat.rs"),
            "must name the gate that does NOT cover this layout: {message}"
        );
        assert!(
            message.contains("SAME major version") || message.contains("same major"),
            "must state the rule is about layout drift within one major: {message}"
        );
        assert!(
            message.contains("BUMP THE VERSION") || message.contains("bump the version"),
            "must tell the developer to bump the version: {message}"
        );
        assert!(
            message.contains("Do NOT") || message.contains("do not"),
            "must explicitly forbid silently regenerating the fixture: {message}"
        );
        assert!(
            message.contains("FRAME_VERSION"),
            "must name the concrete version constant for ReplicationFrame: {message}"
        );
        assert!(
            message.contains("recorded (golden)") && message.contains("current  (build)"),
            "must show both the recorded and current bytes so the diff is visible: {message}"
        );
    }
}
