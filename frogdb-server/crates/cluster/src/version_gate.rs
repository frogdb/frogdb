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
    let active = match active_version {
        Some(v) => match Version::parse(v) {
            Ok(ver) => ver,
            Err(_) => return false,
        },
        None => return false,
    };

    VERSION_GATES
        .iter()
        .find(|g| g.name == gate_name)
        .map_or(false, |gate| {
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

/// Return all gates that would activate at the given version but are not yet
/// active (i.e., they are blocked pending finalization).
pub fn pending_gates(
    active_version: Option<&str>,
    target_version: &str,
) -> Vec<&'static VersionGateEntry> {
    let target = match Version::parse(target_version) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let active = active_version.and_then(|v| Version::parse(v).ok());

    VERSION_GATES
        .iter()
        .filter(|gate| {
            gate.min_version <= target && active.as_ref().map_or(true, |av| gate.min_version > *av)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
