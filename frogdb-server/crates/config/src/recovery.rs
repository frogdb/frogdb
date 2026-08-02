//! Startup-recovery policy.
//!
//! Recovery reads the durable state back into memory before the server serves
//! anything. Most of what it needs is `[persistence]` (where the data dir is,
//! how RocksDB is tuned); this section is the part that is *policy* rather than
//! plumbing — what recovery should do when the state it finds is not entirely
//! readable.
//
// Every parameter here is boot-time only, and structurally so: recovery has
// finished before the first connection exists, so there is no live seam a
// `CONFIG SET` could act on. CONFIG GET reports the honest startup value.

use anyhow::Result;
use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// What recovery does when a stored value will not deserialize.
///
/// The parsed form of [`RecoveryConfig::on_decode_failure`]. Parsing lives in
/// [`RecoveryConfig::decode_failure_policy`] so the recovery crate never
/// compares the raw string itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnDecodeFailure {
    /// Skip the key, count it, surface it (metric + `ERROR` + INFO), keep going.
    /// A database where *nothing* decoded still refuses to start — that refusal
    /// needs no policy.
    #[default]
    Continue,
    /// Any key that fails to deserialize refuses the boot.
    Refuse,
}

/// Valid values for the `recovery-on-decode-failure` parameter.
///
/// Single source of truth shared by [`RecoveryConfig::validate`] and
/// [`RecoveryConfig::decode_failure_policy`], so a value that boots is a value
/// the policy branch understands.
pub const ON_DECODE_FAILURE_POLICIES: &[&str] = &["continue", "refuse"];

/// Recovery policy configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "recovery")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct RecoveryConfig {
    /// What to do when a stored value fails to deserialize during restore:
    /// `"continue"` (skip the key, count it, keep the rest of the keyspace) or
    /// `"refuse"` (fail startup on the first one).
    ///
    /// `continue` is the default because one bit-rotted value must not cost a
    /// whole keyspace, and because the unambiguous case — a database where
    /// nothing at all decoded — already refuses without any policy. Choose
    /// `refuse` when serving a silently smaller keyspace is worse than not
    /// serving.
    ///
    /// Boot-time only: recovery has already run by the time a client can issue
    /// `CONFIG SET`.
    #[serde(default = "default_on_decode_failure")]
    #[param(name = "recovery-on-decode-failure")]
    pub on_decode_failure: String,
}

fn default_on_decode_failure() -> String {
    DEFAULT_ON_DECODE_FAILURE.to_string()
}

/// Default for `recovery.on-decode-failure`.
pub const DEFAULT_ON_DECODE_FAILURE: &str = "continue";

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            on_decode_failure: default_on_decode_failure(),
        }
    }
}

impl RecoveryConfig {
    /// Validate the recovery configuration.
    pub fn validate(&self) -> Result<()> {
        if !ON_DECODE_FAILURE_POLICIES.contains(&self.on_decode_failure.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid recovery.on-decode-failure '{}', expected one of: {}",
                self.on_decode_failure,
                ON_DECODE_FAILURE_POLICIES.join(", ")
            );
        }
        Ok(())
    }

    /// The parsed decode-failure policy.
    ///
    /// An unrecognized value cannot reach here on a booted server —
    /// [`validate`](Self::validate) rejects it — so the fallback is the default
    /// rather than an error: a policy this conservative is the right answer for
    /// a value nobody validated (a hand-built config struct in a test).
    pub fn decode_failure_policy(&self) -> OnDecodeFailure {
        match self.on_decode_failure.to_lowercase().as_str() {
            "refuse" => OnDecodeFailure::Refuse,
            _ => OnDecodeFailure::Continue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_continue() {
        let config = RecoveryConfig::default();
        assert_eq!(config.on_decode_failure, "continue");
        assert_eq!(config.decode_failure_policy(), OnDecodeFailure::Continue);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn refuse_parses_and_validates() {
        let config = RecoveryConfig {
            on_decode_failure: "refuse".to_string(),
        };
        assert_eq!(config.decode_failure_policy(), OnDecodeFailure::Refuse);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn policy_parsing_is_case_insensitive() {
        let config = RecoveryConfig {
            on_decode_failure: "REFUSE".to_string(),
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.decode_failure_policy(), OnDecodeFailure::Refuse);
    }

    #[test]
    fn an_unknown_policy_is_rejected_at_boot() {
        let config = RecoveryConfig {
            on_decode_failure: "abort".to_string(),
        };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("invalid recovery.on-decode-failure"), "{err}");
        assert!(err.contains("continue, refuse"), "{err}");
    }

    #[test]
    fn every_accepted_value_parses_to_a_distinct_policy() {
        // The list `validate` accepts and the match `decode_failure_policy`
        // performs must not drift: each accepted string has to reach a policy,
        // and "continue" must not silently parse as "refuse".
        let policies: Vec<OnDecodeFailure> = ON_DECODE_FAILURE_POLICIES
            .iter()
            .map(|value| {
                RecoveryConfig {
                    on_decode_failure: value.to_string(),
                }
                .decode_failure_policy()
            })
            .collect();
        assert_eq!(
            policies,
            vec![OnDecodeFailure::Continue, OnDecodeFailure::Refuse]
        );
    }
}
