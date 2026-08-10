//! Shared invariant-catalog vocabulary: [`Violation`], [`Citation`], [`Tier`].
//!
//! Any area that keeps an invariant catalog over its own replicated state —
//! `frogdb-cluster`'s catalog over `ClusterStateInner`, `frogdb-replication`'s
//! catalog over `ReplicationView` — reports through this one vocabulary, so a
//! citation, a tier and a violation mean the same thing in every area's
//! `DEBUG ... CHECK` output, Jepsen path and scheduler rendering. The types
//! live here, in the crate every catalog-owning crate already depends on,
//! rather than in any one of them: see `.scratch/replication-correctness/PRD.md`
//! §8 D6.
//!
//! # Tiers
//!
//! There are exactly two, and no third:
//!
//! - [`Tier::Hard`] — the state is unreachable by any correct transition, so a
//!   violation is a defect by definition. This is the tier an owning crate's
//!   state-machine hook panics on.
//! - [`Tier::DocumentedException`] — the state is reachable today, the
//!   behavior is deliberate, and the entry carries the [`Citation`] that says
//!   so. The citation is a field of the variant, so an exception without one
//!   does not compile; [`Citation`]'s constructors are `const fn`s that reject
//!   the empty string, so inside a `static CATALOG` an exception citing `""`
//!   fails to compile too. The tier exists to force a known-dirty state into
//!   an explicit ruling rather than a silent shrug.

/// A single violated invariant, at one offending place in the state.
///
/// One check can produce several: a state with three dangling slot owners
/// reports three violations of the same id, so the detail names which slots
/// rather than only how many.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The stable catalog id, e.g. `"INV-REF-1"`. Stable across refactors —
    /// specs, issues and checker output all quote it.
    pub id: &'static str,
    /// What is wrong, naming the concrete ids involved.
    pub detail: String,
}

impl Violation {
    /// Construct a violation. Public so every catalog-owning crate's
    /// `check_*` functions can build one; the fields are also directly
    /// public, so this is a convenience rather than the only path.
    pub fn new(id: &'static str, detail: String) -> Self {
        Self { id, detail }
    }
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.id, self.detail)
    }
}

/// The ruling that makes a [`Tier::DocumentedException`] legitimate.
///
/// Constructed only through [`Citation::failure_mode`] or [`Citation::issue`],
/// both of which reject an empty reference. Because a catalog's entry table is
/// a `static`, those `const fn` assertions run at compile time: a
/// citation-less — or blank-citation — exception is a build error, not a
/// review comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Citation(&'static str);

impl Citation {
    /// Cite the failure-mode row that rules the state deliberate, e.g.
    /// `"FM-CLUSTER-033"`.
    pub const fn failure_mode(row: &'static str) -> Self {
        assert!(
            !row.is_empty(),
            "a DOCUMENTED-EXCEPTION must cite a failure-mode row"
        );
        Self(row)
    }

    /// Cite the issue that rules the state deliberate, e.g. a path under
    /// `.scratch/cluster-correctness/issues/`.
    pub const fn issue(reference: &'static str) -> Self {
        assert!(
            !reference.is_empty(),
            "a DOCUMENTED-EXCEPTION must cite an issue"
        );
        Self(reference)
    }

    /// The cited reference.
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

/// How seriously a catalog takes a violation of an entry. See the module
/// docs; there are two tiers and there is no third.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// A violation is a defect. Asserted by the owning crate's state-machine
    /// hook.
    Hard,
    /// The state is reachable and deliberate; the [`Citation`] says where
    /// that was ruled. Reported by an owning crate's "all violations" view,
    /// never asserted.
    DocumentedException(Citation),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_citation_carries_its_reference() {
        assert_eq!(
            Citation::failure_mode("FM-CLUSTER-033").as_str(),
            "FM-CLUSTER-033"
        );
        assert_eq!(
            Citation::issue("issues/open/02.md").as_str(),
            "issues/open/02.md"
        );
    }

    #[test]
    fn a_violation_renders_id_and_detail() {
        let violation = Violation::new("INV-REF-1", "a".to_string());
        assert_eq!(violation.to_string(), "INV-REF-1: a");
    }

    #[test]
    fn tier_hard_is_not_a_documented_exception() {
        assert!(matches!(Tier::Hard, Tier::Hard));
        let tier = Tier::DocumentedException(Citation::issue("some/issue.md"));
        assert!(matches!(tier, Tier::DocumentedException(_)));
    }

    // Compile-time discipline (verified by manual check, recorded in the
    // moving commit's history, since this crate has no trybuild harness):
    //
    //   const BAD: Citation = Citation::issue("");
    //
    // fails to build with "a DOCUMENTED-EXCEPTION must cite an issue" — the
    // `const fn` assert fires at compile time because evaluating it is
    // required to build the constant. The same holds for `failure_mode("")`.
}
