//! Version gate registry for rolling upgrades.
//!
//! Features that require finalization before activation are registered here.
//! During a mixed-version window (new binary, old active version), all gated
//! code paths check [`is_gate_active`] before executing.

use semver::Version;

/// A version-gated feature entry.
pub struct VersionGateEntry {
    /// Human-readable feature name (e.g., "compact_type_bytes").
    pub name: &'static str,
    /// Minimum active version that enables this gate.
    pub min_version: Version,
    /// Description shown in `frog upgrade status`.
    pub description: &'static str,
}

/// Compile-time registry of all version-gated features.
///
/// Gates are added alongside the features they protect. This list starts empty
/// and grows as new gated features are introduced in future releases.
pub static VERSION_GATES: &[VersionGateEntry] = &[
    // Example (commented out — no gates exist yet):
    // VersionGateEntry {
    //     name: "compact_type_bytes",
    //     min_version: Version::new(0, 2, 0),
    //     description: "Compact serialization format (saves ~15% storage)",
    // },
];

/// Check whether a specific version gate is active.
///
/// Returns `true` only if `active_version` is `Some` and >= the gate's `min_version`.
/// Returns `false` if the gate name is not found (unknown gates are never active).
pub fn is_gate_active(gate_name: &str, active_version: Option<&str>) -> bool {
    is_gate_active_in(gate_name, active_version, VERSION_GATES)
}

/// Return all gates that would activate at the given version but are not yet
/// active (i.e., they are blocked pending finalization).
pub fn pending_gates(
    active_version: Option<&str>,
    target_version: &str,
) -> Vec<&'static VersionGateEntry> {
    pending_gates_in(active_version, target_version, VERSION_GATES)
}

/// Check whether a specific version gate is active against the given gate list.
fn is_gate_active_in(
    gate_name: &str,
    active_version: Option<&str>,
    gates: &[VersionGateEntry],
) -> bool {
    let active = match active_version {
        Some(v) => match Version::parse(v) {
            Ok(ver) => ver,
            Err(_) => return false,
        },
        None => return false,
    };

    gates
        .iter()
        .find(|g| g.name == gate_name)
        .is_some_and(|gate| {
            let is_active = active >= gate.min_version;
            if !is_active {
                tracing::debug!(
                    gate = gate.name,
                    active_version = %active,
                    required_version = %gate.min_version,
                    "Version gate suppressed — active version below minimum"
                );
            }
            is_active
        })
}

/// Return all gates from the given list that would activate at the target
/// version but are not yet active.
fn pending_gates_in<'a>(
    active_version: Option<&str>,
    target_version: &str,
    gates: &'a [VersionGateEntry],
) -> Vec<&'a VersionGateEntry> {
    let target = match Version::parse(target_version) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let active = active_version.and_then(|v| Version::parse(v).ok());

    gates
        .iter()
        .filter(|gate| {
            gate.min_version <= target && active.as_ref().is_none_or(|av| gate.min_version > *av)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test gates for exercising real gate logic.
    fn test_gates() -> Vec<VersionGateEntry> {
        vec![
            VersionGateEntry {
                name: "feature_a",
                min_version: Version::new(0, 2, 0),
                description: "Test feature A",
            },
            VersionGateEntry {
                name: "feature_b",
                min_version: Version::new(0, 3, 0),
                description: "Test feature B",
            },
        ]
    }

    // Tests against the static (empty) registry

    #[test]
    fn no_gates_means_nothing_active() {
        assert!(!is_gate_active("nonexistent", Some("1.0.0")));
        assert!(!is_gate_active("nonexistent", None));
    }

    #[test]
    fn none_active_version_means_inactive() {
        assert!(!is_gate_active("compact_type_bytes", None));
    }

    #[test]
    fn invalid_active_version_means_inactive() {
        assert!(!is_gate_active("compact_type_bytes", Some("not-a-version")));
    }

    #[test]
    fn pending_gates_empty_registry() {
        let pending = pending_gates(None, "0.2.0");
        assert!(pending.is_empty());
    }

    // Tests against test gates (real gate logic)

    #[test]
    fn gate_active_when_version_meets_minimum() {
        let gates = test_gates();
        assert!(is_gate_active_in("feature_a", Some("0.2.0"), &gates));
        assert!(is_gate_active_in("feature_a", Some("0.3.0"), &gates));
        assert!(is_gate_active_in("feature_b", Some("0.3.0"), &gates));
        assert!(is_gate_active_in("feature_b", Some("1.0.0"), &gates));
    }

    #[test]
    fn gate_inactive_when_version_below_minimum() {
        let gates = test_gates();
        assert!(!is_gate_active_in("feature_a", Some("0.1.0"), &gates));
        assert!(!is_gate_active_in("feature_b", Some("0.2.0"), &gates));
    }

    #[test]
    fn gate_inactive_when_no_active_version() {
        let gates = test_gates();
        assert!(!is_gate_active_in("feature_a", None, &gates));
        assert!(!is_gate_active_in("feature_b", None, &gates));
    }

    #[test]
    fn pending_gates_returns_unlockable_gates() {
        let gates = test_gates();
        // Upgrading from nothing to 0.2.0: only feature_a is pending
        let pending = pending_gates_in(None, "0.2.0", &gates);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].name, "feature_a");

        // Upgrading from nothing to 0.3.0: both are pending
        let pending = pending_gates_in(None, "0.3.0", &gates);
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn pending_gates_excludes_already_active() {
        let gates = test_gates();
        // Already at 0.2.0, upgrading to 0.3.0: only feature_b is pending
        let pending = pending_gates_in(Some("0.2.0"), "0.3.0", &gates);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].name, "feature_b");
    }

    #[test]
    fn pending_gates_empty_for_patch_upgrade() {
        let gates = test_gates();
        // Already at 0.2.0, upgrading to 0.2.1: no new gates
        let pending = pending_gates_in(Some("0.2.0"), "0.2.1", &gates);
        assert!(pending.is_empty());
    }
}
