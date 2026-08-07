//! The replication handshake's version-compatibility rule.
//!
//! # The rule
//!
//! Two FrogDB binaries may share a replication stream **iff they report the
//! same major version**. A different major is *incompatible*: the primary
//! refuses the `PSYNC` with an error naming both versions, and nothing is
//! registered, counted or cut for that peer. Same major with a different minor
//! is *compatible* — it is exactly what a rolling upgrade looks like while it
//! is in flight — so the stream is served and the skew is reported once for
//! that session. A patch-only difference is compatible and silent.
//!
//! "Major" is the first dot-separated segment of the announced string, read
//! after any semver pre-release/build suffix is cut (`1.2.0-rc1` → major `1`,
//! minor `2`). The minor is the second segment when it is a number; a version
//! that carries no readable minor compares on its major alone.
//!
//! # Unknown is not incompatible
//!
//! `REPLCONF frogdb-version` is optional on the wire, so an absent or
//! unreadable version is what a replica predating the option, a peer that
//! skipped the option, and a non-FrogDB client all produce. This gate refuses
//! only what it can *prove* incompatible: an unreadable version yields
//! [`VersionVerdict::Unproven`], which is served and warned about, never
//! refused. Refusing it would take a data path down on a suspicion, and the
//! peers it would take down are precisely the ones that cannot be told why.
//!
//! This is the deliberate opposite of how a *finalization* gate must treat the
//! same field (FM-REPLICATION-049: "unknown must block, or the gate fails
//! open"). The two gates take opposite defaults because their actions are not
//! comparable: refusing to finalize an upgrade is reversible and costs an
//! operator a retry, while refusing to replicate costs availability and
//! durability. The unknown case is loud in both.

use std::fmt;

/// This binary's version — the primary half of every comparison this module
/// makes, and the same string the replica half of this crate announces from
/// [`crate::replica::connection`]. Both sides read `CARGO_PKG_VERSION` of this
/// one crate, which inherits the workspace version, so a primary and a replica
/// built from the same tree cannot disagree about what "this version" is.
pub const PRIMARY_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The comparable part of a version string: `major`, and `minor` when the
/// string carries a readable one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VersionParts {
    major: u64,
    minor: Option<u64>,
}

/// Read the major (and, when present, the minor) out of an announced version.
///
/// `None` means the string carries no readable major, which is the only thing
/// this gate refuses on — so a string it cannot read can never refuse anyone.
fn parse_version(version: &str) -> Option<VersionParts> {
    // Semver hangs pre-release and build metadata off the numeric core with
    // `-` / `+`; the core is the only part that decides compatibility.
    let core = version.split(['-', '+']).next().unwrap_or(version);
    let mut segments = core.split('.');
    let major = segments.next()?.parse::<u64>().ok()?;
    let minor = segments
        .next()
        .and_then(|segment| segment.parse::<u64>().ok());
    Some(VersionParts { major, minor })
}

/// A refused pair: the two majors that cannot share a replication stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MajorMismatch {
    /// This primary's version, verbatim.
    pub primary: String,
    /// What the replica announced, verbatim.
    pub replica: String,
    /// The major read out of [`Self::primary`].
    pub primary_major: u64,
    /// The major read out of [`Self::replica`].
    pub replica_major: u64,
}

impl MajorMismatch {
    /// The RESP error the refused replica is sent, and the text the primary
    /// logs.
    ///
    /// It names both versions, both majors, the rule that refused them, and the
    /// two moves that fix it — an operator who reads this in either node's log
    /// needs nothing else to act. Single line by construction: a RESP simple
    /// error is terminated by the CRLF the caller appends, so the message must
    /// not contain one.
    pub fn wire_error(&self) -> String {
        format!(
            "ERR PSYNC refused - replica announced FrogDB version {} (major {}) but this primary \
             is FrogDB version {} (major {}); replication requires both ends on the same major \
             version. Run the replica on a {}.x build, or move this primary to {}.x, then let it \
             reconnect.",
            self.replica,
            self.replica_major,
            self.primary,
            self.primary_major,
            self.primary_major,
            self.replica_major,
        )
    }
}

impl fmt::Display for MajorMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.wire_error())
    }
}

/// What the gate concluded about one replica's announced version.
///
/// Produced once per session, at the `PSYNC` that creates it — see
/// [`VersionVerdict::evaluate`] and the module rule above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionVerdict {
    /// Same major, and nothing provably skewed below it (equal minors, or a
    /// version with no readable minor). Served, silently.
    Compatible,
    /// Same major, different minor — a rolling upgrade in flight. Served, and
    /// reported once for the session.
    MinorSkew {
        /// This primary's version, verbatim.
        primary: String,
        /// What the replica announced, verbatim.
        replica: String,
    },
    /// Compatibility could not be *proved* either way, because at least one
    /// side carries no readable major: the replica announced nothing
    /// (`replica: None`), announced something unreadable (`replica:
    /// Some(raw)`), or — impossible for a released build, but the rule is
    /// total — this primary's own version is unreadable. Served, and reported
    /// once for the session, with both raw strings so the log says which side
    /// could not be read.
    Unproven {
        /// This primary's version, verbatim.
        primary: String,
        /// What the replica announced, verbatim; `None` if it announced
        /// nothing.
        replica: Option<String>,
    },
    /// Different majors. Refused — see [`MajorMismatch::wire_error`].
    IncompatibleMajor(MajorMismatch),
}

impl VersionVerdict {
    /// Apply the rule to one announced version.
    ///
    /// `announced` is [`crate::replica_session::ReplicaAnnouncement::version`]:
    /// `None` is *unknown*, never "old".
    pub fn evaluate(primary: &str, announced: Option<&str>) -> Self {
        let Some(replica) = announced else {
            return Self::Unproven {
                primary: primary.to_string(),
                replica: None,
            };
        };
        let (Some(ours), Some(theirs)) = (parse_version(primary), parse_version(replica)) else {
            return Self::Unproven {
                primary: primary.to_string(),
                replica: Some(replica.to_string()),
            };
        };
        if ours.major != theirs.major {
            return Self::IncompatibleMajor(MajorMismatch {
                primary: primary.to_string(),
                replica: replica.to_string(),
                primary_major: ours.major,
                replica_major: theirs.major,
            });
        }
        match (ours.minor, theirs.minor) {
            (Some(ours_minor), Some(theirs_minor)) if ours_minor != theirs_minor => {
                Self::MinorSkew {
                    primary: primary.to_string(),
                    replica: replica.to_string(),
                }
            }
            _ => Self::Compatible,
        }
    }

    /// The mismatch this verdict refuses on, or `None` for every verdict that
    /// admits the replica. The one place a caller may ask "does this stop the
    /// handshake?".
    pub fn refusal(&self) -> Option<&MajorMismatch> {
        match self {
            Self::IncompatibleMajor(mismatch) => Some(mismatch),
            Self::Compatible | Self::MinorSkew { .. } | Self::Unproven { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // FM-REPLICATION-064
    /// The everyday case: a matched pair says nothing and is served.
    #[test]
    fn an_identical_version_is_compatible() {
        assert_eq!(
            VersionVerdict::evaluate("1.4.2", Some("1.4.2")),
            VersionVerdict::Compatible
        );
    }

    // FM-REPLICATION-064
    /// A patch difference is below the granularity the rule speaks at: it is
    /// compatible *and* silent, or every patch-level rollout would fill the
    /// primary's log with warnings that mean nothing.
    #[test]
    fn a_patch_only_difference_is_compatible_and_silent() {
        assert_eq!(
            VersionVerdict::evaluate("1.4.2", Some("1.4.9")),
            VersionVerdict::Compatible
        );
    }

    // FM-REPLICATION-064
    /// Same major, different minor: served — refusing here would break the
    /// rolling upgrade this gate exists to make safe — and reported, because a
    /// pair that stays split is an upgrade that stalled.
    #[test]
    fn a_minor_difference_is_admitted_and_reported() {
        let verdict = VersionVerdict::evaluate("1.4.2", Some("1.5.0"));
        assert_eq!(
            verdict,
            VersionVerdict::MinorSkew {
                primary: "1.4.2".to_string(),
                replica: "1.5.0".to_string(),
            }
        );
        assert!(
            verdict.refusal().is_none(),
            "a minor skew must never stop the handshake"
        );
    }

    // FM-REPLICATION-064
    /// The refusal, in both directions — a replica ahead of the primary is
    /// refused exactly like one behind it. The rule is inequality, not order.
    #[test]
    fn a_different_major_is_refused_in_either_direction() {
        let newer = VersionVerdict::evaluate("1.4.2", Some("2.0.0"));
        let mismatch = newer.refusal().expect("a newer major must be refused");
        assert_eq!(mismatch.primary_major, 1);
        assert_eq!(mismatch.replica_major, 2);

        let older = VersionVerdict::evaluate("2.0.0", Some("1.4.2"));
        let mismatch = older.refusal().expect("an older major must be refused");
        assert_eq!(mismatch.primary_major, 2);
        assert_eq!(mismatch.replica_major, 1);
    }

    // FM-REPLICATION-064
    /// The error is the whole remedy an operator gets, so it must name both
    /// versions verbatim, both majors, and stay a single RESP line.
    #[test]
    fn the_refusal_error_names_both_versions() {
        let verdict = VersionVerdict::evaluate("1.4.2", Some("2.0.0"));
        let message = verdict.refusal().expect("refused").wire_error();
        assert!(message.starts_with("ERR "), "got: {message}");
        assert!(message.contains("1.4.2"), "primary version: {message}");
        assert!(message.contains("2.0.0"), "replica version: {message}");
        assert!(message.contains("major 1"), "primary major: {message}");
        assert!(message.contains("major 2"), "replica major: {message}");
        assert!(message.contains("1.x"), "the remedy for the replica");
        assert!(message.contains("2.x"), "the remedy for the primary");
        assert!(
            !message.contains('\r') && !message.contains('\n'),
            "a RESP simple error is one line: {message}"
        );
    }

    // FM-REPLICATION-064
    /// The vacuous-truth case, inverted: a peer that announced nothing is
    /// *unknown*, and unknown is served rather than refused — refusing it would
    /// drop every pre-option and non-FrogDB peer on a suspicion.
    #[test]
    fn an_unannounced_version_is_unproven_not_incompatible() {
        let verdict = VersionVerdict::evaluate("1.4.2", None);
        assert_eq!(
            verdict,
            VersionVerdict::Unproven {
                primary: "1.4.2".to_string(),
                replica: None,
            }
        );
        assert!(verdict.refusal().is_none(), "unknown must not refuse");
    }

    // FM-REPLICATION-064
    /// A version this gate cannot read keeps its raw string, so the warning
    /// reports what the peer actually said instead of a placeholder.
    #[test]
    fn an_unreadable_version_is_unproven_and_keeps_the_raw_string() {
        for raw in ["", "banana", "v1.2.3", " 1.2.3", "-1.2.3"] {
            let verdict = VersionVerdict::evaluate("1.4.2", Some(raw));
            assert_eq!(
                verdict,
                VersionVerdict::Unproven {
                    primary: "1.4.2".to_string(),
                    replica: Some(raw.to_string()),
                },
                "{raw:?} carries no readable major"
            );
            assert!(verdict.refusal().is_none(), "{raw:?} must not refuse");
        }
    }

    // FM-REPLICATION-064
    /// The rule is total over both operands: a primary whose own version
    /// cannot be read can prove nothing, so it refuses nobody.
    #[test]
    fn an_unreadable_primary_version_refuses_nobody() {
        let verdict = VersionVerdict::evaluate("nightly", Some("2.0.0"));
        assert_eq!(
            verdict,
            VersionVerdict::Unproven {
                primary: "nightly".to_string(),
                replica: Some("2.0.0".to_string()),
            }
        );
    }

    // FM-REPLICATION-064
    /// Pre-release and build metadata hang off the numeric core and do not
    /// change what a version *is*: `2.0.0-rc1` is major 2, and a release
    /// candidate of the next minor is a minor skew, not an incompatibility.
    #[test]
    fn a_pre_release_or_build_suffix_compares_on_its_numeric_core() {
        assert_eq!(
            VersionVerdict::evaluate("1.4.2", Some("1.4.2-rc1")),
            VersionVerdict::Compatible
        );
        assert_eq!(
            VersionVerdict::evaluate("1.4.2", Some("1.5.0+build7")),
            VersionVerdict::MinorSkew {
                primary: "1.4.2".to_string(),
                replica: "1.5.0+build7".to_string(),
            }
        );
        let verdict = VersionVerdict::evaluate("1.4.2", Some("2.0.0-rc1"));
        assert_eq!(
            verdict.refusal().expect("still major 2").replica_major,
            2,
            "a release candidate of the next major is still the next major"
        );
    }

    // FM-REPLICATION-064
    /// A version with no readable minor compares on its major alone: it is
    /// enough to refuse on (the major is what the rule speaks about) and not
    /// enough to call a skew.
    #[test]
    fn a_version_without_a_readable_minor_compares_on_its_major() {
        assert_eq!(
            VersionVerdict::evaluate("1.4.2", Some("1")),
            VersionVerdict::Compatible
        );
        assert_eq!(
            VersionVerdict::evaluate("1.4.2", Some("1.x")),
            VersionVerdict::Compatible
        );
        assert_eq!(
            VersionVerdict::evaluate("1.4.2", Some("2"))
                .refusal()
                .expect("major 2 is still refused")
                .replica_major,
            2
        );
    }

    // FM-REPLICATION-064
    /// The version this primary compares against is its own build's, not a
    /// literal: the constant and the string the replica half announces are the
    /// same `CARGO_PKG_VERSION`, so a same-build pair is always compatible.
    #[test]
    fn a_peer_built_from_this_tree_is_compatible_with_this_primary() {
        assert_eq!(
            VersionVerdict::evaluate(PRIMARY_VERSION, Some(PRIMARY_VERSION)),
            VersionVerdict::Compatible
        );
        assert!(
            parse_version(PRIMARY_VERSION).is_some(),
            "this build's own version must be readable, or the gate can never refuse anyone"
        );
    }
}
