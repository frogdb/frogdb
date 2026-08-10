//! The replica half of the `PSYNC` decision: what to ask for, and what the
//! primary's answer means.
//!
//! The primary side has had this shape since ADR 0004:
//! `PartialSyncReplay::handle_partial_sync_request` (`primary/replay.rs`) takes
//! state plus a request plus the current offset and returns a `ReplayDecision`,
//! with `handle_psync` doing nothing but turn that decision into bytes. The
//! replica side did not — its half of the same protocol step lived inside
//! `ReplicaConnection::psync`, interleaved with socket reads, offset rewinds and
//! state locks, so the arms could only be exercised through a scripted primary
//! over a duplex stream.
//!
//! This module is the missing twin. Nothing in here reads a socket, a clock, a
//! lock or a shared cell: [`psync_request_args`] decides what to send,
//! [`select_psync_arm`] decides which arm the reply is, and
//! [`select_full_resync_payload`] decides which payload the envelope names.
//! `ReplicaConnection::psync` is then the I/O half, and selects nothing.

use std::io;

use crate::fullsync::{CHECKPOINT_MARKER, SNAPSHOT_MARKER};

/// Build the `(replication_id, offset)` pair for a reconnect `PSYNC` request
/// from the replica's **live applied** offset. A live offset of 0 means the
/// replica has never synced, so it asks for a full resync (`PSYNC ? -1`);
/// otherwise it resumes from its live head under its current replication id.
///
/// The regression guard is that it is fed [`crate::replica::offset::ReplicaOffset::current`],
/// not the lagging persisted `offset_at_save`: a resume from behind the applied
/// head would re-receive already-applied data or force a needless full resync.
pub fn psync_request_args(replication_id: &str, current_offset: u64) -> (String, i64) {
    if current_offset == 0 {
        ("?".to_string(), -1i64)
    } else {
        (replication_id.to_string(), current_offset as i64)
    }
}

/// Which arm of `PSYNC` the primary answered with — the replica-side twin of
/// [`crate::primary::ReplayDecision`].
///
/// Two arms and no third, for the same reason `ReplayDecision` has two: the
/// primary either grants the resume or replaces the dataset, and "neither" is
/// not a state the replica can be in. Anything else on the wire is an error
/// out of [`select_psync_arm`], not a variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PsyncArm {
    /// `+FULLRESYNC <replid> <offset>` — the primary is about to send a
    /// dataset. Neither half of the granted pair is adopted by the selection;
    /// see `ReplicaConnection::psync` for why the adoption waits for the
    /// payload's own trailer (FM-REPLICATION-001).
    FullResync {
        granted_id: String,
        granted_offset: u64,
    },
    /// `+CONTINUE [<replid>]` — the stream resumes where it left off. The id is
    /// optional because a primary that has not shifted its history sends the
    /// bare word, which is also what pre-PSYNC2 primaries sent.
    Continue { granted_id: Option<String> },
}

/// The payload a `+FULLRESYNC` envelope names — the `$<marker>` line that
/// follows the grant.
///
/// There is deliberately no "plain RDB" arm: a FrogDB primary sends a
/// checkpoint (it has RocksDB) or a live dataset (it does not), and both carry
/// the primary's actual keyspace. The data-less minimal RDB older primaries sent
/// is refused rather than represented (issue 67).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullResyncPayload {
    /// `$FROGDB_CHECKPOINT` — a file-count-prefixed RocksDB checkpoint.
    Checkpoint,
    /// `$FROGDB_SNAPSHOT` — a blob-count-prefixed serialized live keyspace.
    LiveDataset,
}

/// Select the arm of a `PSYNC` reply line. Pure over the trimmed line; performs
/// no I/O.
///
/// Total over its input: every line either selects an arm or produces the error
/// the connection returns verbatim. `-...` is the primary refusing (an
/// `ErrorKind::Other` carrying the primary's own text, because the reason is
/// the primary's to give — "loading the dataset", the version gate's refusal);
/// everything else is `InvalidData`, because a peer that answered a `PSYNC`
/// with something that is not a `PSYNC` answer is not a peer this replica can
/// keep talking to.
pub fn select_psync_arm(line: &str) -> io::Result<PsyncArm> {
    if line.starts_with("+FULLRESYNC") {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 3 {
            let granted_id = parts[1].to_string();
            let granted_offset: u64 = parts[2].parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid offset in FULLRESYNC")
            })?;
            Ok(PsyncArm::FullResync {
                granted_id,
                granted_offset,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed FULLRESYNC response",
            ))
        }
    } else if line.starts_with("+CONTINUE") {
        let parts: Vec<&str> = line.split_whitespace().collect();
        Ok(PsyncArm::Continue {
            granted_id: parts.get(1).map(|id| (*id).to_string()),
        })
    } else if let Some(rest) = line.strip_prefix('-') {
        Err(io::Error::other(format!("PSYNC error: {}", rest)))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected PSYNC response: {}", line),
        ))
    }
}

/// Select the payload a `+FULLRESYNC` envelope names, from the trimmed `$…`
/// line. Pure; performs no I/O.
///
/// Marker detection is a decision over the line alone — the count that follows
/// is read by the caller through the codec — so a new payload shape is added
/// here, in one `match`, rather than in the middle of a socket loop.
pub fn select_full_resync_payload(line: &str) -> io::Result<FullResyncPayload> {
    let Some(marker) = line.strip_prefix('$') else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected a checkpoint or dataset marker",
        ));
    };
    match marker {
        CHECKPOINT_MARKER => Ok(FullResyncPayload::Checkpoint),
        SNAPSHOT_MARKER => Ok(FullResyncPayload::LiveDataset),
        // Anything else — including the data-less minimal RDB older primaries
        // sent when persistence was disabled — carries no dataset this node can
        // install, and accepting it would mean keeping a stale keyspace while
        // claiming to be synced.
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported FULLRESYNC payload marker: {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(line: &str) -> io::Error {
        select_psync_arm(line).expect_err("expected a rejected PSYNC reply")
    }

    #[test]
    fn psync_request_args_asks_full_resync_when_never_synced() {
        let (id, offset) = psync_request_args("abc", 0);
        assert_eq!(id, "?");
        assert_eq!(offset, -1);
    }

    #[test]
    fn psync_request_args_resumes_from_the_live_offset() {
        let (id, offset) = psync_request_args("myid", 500);
        assert_eq!(id, "myid");
        assert_eq!(offset, 500);
    }

    /// The grant is read positionally, and a primary that appends fields it did
    /// not have to is not a protocol error — Redis's own `+FULLRESYNC` line has
    /// grown before.
    // FM-REPLICATION-001
    #[test]
    fn a_full_resync_grant_carries_the_id_and_the_offset() {
        assert_eq!(
            select_psync_arm("+FULLRESYNC abc123 4242").unwrap(),
            PsyncArm::FullResync {
                granted_id: "abc123".to_string(),
                granted_offset: 4242,
            }
        );
        assert_eq!(
            select_psync_arm("+FULLRESYNC abc123 0 something-later").unwrap(),
            PsyncArm::FullResync {
                granted_id: "abc123".to_string(),
                granted_offset: 0,
            },
            "trailing fields are ignored, not rejected"
        );
    }

    /// Every way a `+FULLRESYNC` line can fail to be one, with the message the
    /// connection surfaces. The arity failures are distinct from the parse
    /// failure because they fail at different points of the same line, and
    /// FM-REPLICATION-001's table pins both.
    // FM-REPLICATION-001
    #[test]
    fn a_full_resync_grant_that_is_not_a_pair_is_rejected() {
        for line in ["+FULLRESYNC", "+FULLRESYNC abc123"] {
            let e = err(line);
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{line}");
            assert_eq!(e.to_string(), "malformed FULLRESYNC response", "{line}");
        }
        for line in [
            "+FULLRESYNC abc123 nine",
            "+FULLRESYNC abc123 -1",
            "+FULLRESYNC abc123 4242.0",
            "+FULLRESYNC abc123 99999999999999999999999999",
            "+FULLRESYNC abc123 ''",
        ] {
            let e = err(line);
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{line}");
            assert_eq!(e.to_string(), "invalid offset in FULLRESYNC", "{line}");
        }
    }

    /// `+CONTINUE` with an id (a primary that shifted its history) and without
    /// (one that did not). The bare form is not an error: it is what a primary
    /// on the same history sends.
    // FM-REPLICATION-013
    #[test]
    fn a_continue_carries_an_optional_id() {
        assert_eq!(
            select_psync_arm("+CONTINUE").unwrap(),
            PsyncArm::Continue { granted_id: None }
        );
        assert_eq!(
            select_psync_arm("+CONTINUE newid").unwrap(),
            PsyncArm::Continue {
                granted_id: Some("newid".to_string())
            }
        );
        assert_eq!(
            select_psync_arm("+CONTINUE newid trailing").unwrap(),
            PsyncArm::Continue {
                granted_id: Some("newid".to_string())
            },
            "trailing fields are ignored, not rejected"
        );
    }

    /// A refusal is the primary's own text, passed through under
    /// `ErrorKind::Other` so the reconnect loop treats it as a link failure
    /// rather than a protocol violation.
    // FM-REPLICATION-013
    #[test]
    fn a_refusal_is_passed_through_verbatim() {
        let e = err("-ERR Can't SYNC while loading the dataset");
        assert_eq!(e.kind(), io::ErrorKind::Other);
        assert_eq!(
            e.to_string(),
            "PSYNC error: ERR Can't SYNC while loading the dataset"
        );
    }

    /// Everything that is neither arm nor refusal, including the empty line a
    /// half-closed socket can yield after trimming.
    // FM-REPLICATION-013
    #[test]
    fn anything_that_is_not_a_psync_answer_is_rejected() {
        for line in ["", "+OK", "+PONG", "$88", "FULLRESYNC abc 1", "*2"] {
            let e = err(line);
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{line}");
            assert_eq!(
                e.to_string(),
                format!("unexpected PSYNC response: {line}"),
                "the rejected line is quoted back so a log names what arrived"
            );
        }
    }

    /// The two payloads a FrogDB primary can name, and nothing else — including
    /// the bare bulk-length line an old primary's minimal RDB started with,
    /// which is refused rather than installed over the live keyspace.
    // FM-REPLICATION-001
    #[test]
    fn the_envelope_names_a_checkpoint_a_dataset_or_nothing_installable() {
        assert_eq!(
            select_full_resync_payload(&format!("${CHECKPOINT_MARKER}")).unwrap(),
            FullResyncPayload::Checkpoint
        );
        assert_eq!(
            select_full_resync_payload(&format!("${SNAPSHOT_MARKER}")).unwrap(),
            FullResyncPayload::LiveDataset
        );

        for (line, message) in [
            ("$88", "unsupported FULLRESYNC payload marker: 88"),
            ("$", "unsupported FULLRESYNC payload marker: "),
            (
                "$FROGDB_CHECKPOINT_V2",
                "unsupported FULLRESYNC payload marker: FROGDB_CHECKPOINT_V2",
            ),
            (
                "$frogdb_checkpoint",
                "unsupported FULLRESYNC payload marker: frogdb_checkpoint",
            ),
        ] {
            let e = select_full_resync_payload(line).expect_err(line);
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{line}");
            assert_eq!(e.to_string(), message, "{line}");
        }

        for line in ["", "+OK", "FROGDB_CHECKPOINT", "-ERR nope"] {
            let e = select_full_resync_payload(line).expect_err(line);
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{line}");
            assert_eq!(
                e.to_string(),
                "expected a checkpoint or dataset marker",
                "a line that is not an envelope at all fails before the marker \
                 vocabulary is consulted: {line}"
            );
        }
    }
}
