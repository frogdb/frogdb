//! Label enums for typed metrics.
//!
//! These enums provide compile-time safety for metric labels with a fixed
//! set of possible values. Each enum derives `MetricLabel` which generates
//! an `as_str()` method for Prometheus label values.
//!
//! Only enums whose variants match values the server actually emits belong
//! here — a label enum that diverges from the emitted strings is
//! documentation lying about the data.

use frogdb_metrics_derive::MetricLabel;

/// Connection rejection reasons.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectionReason {
    /// Max clients limit reached.
    #[label = "maxclients"]
    MaxClients,
    /// Authentication required but not provided.
    #[label = "auth_required"]
    AuthRequired,
    /// Server is shutting down.
    #[label = "shutdown"]
    Shutdown,
}

/// TLS handshake failure reasons.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsHandshakeError {
    /// The handshake itself failed.
    #[label = "handshake_error"]
    HandshakeError,
    /// The handshake timed out.
    #[label = "timeout"]
    Timeout,
}

/// Persistence error types.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistenceErrorType {
    /// WAL write error.
    #[label = "wal_write"]
    WalWrite,
    /// WAL flush/sync error.
    #[label = "wal_sync"]
    WalSync,
    /// Snapshot creation error.
    #[label = "snapshot"]
    Snapshot,
    /// Recovery/restore error.
    #[label = "recovery"]
    Recovery,
}

/// Which per-shard pub/sub resource limit fired a warning.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum PubsubLimitResource {
    /// Total subscription count approaching its shard limit.
    #[label = "total_subscriptions"]
    TotalSubscriptions,
    /// Unique channel count approaching its shard limit.
    #[label = "unique_channels"]
    UniqueChannels,
    /// Unique pattern count approaching its shard limit.
    #[label = "unique_patterns"]
    UniquePatterns,
}

/// How a Lua script was invoked.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptKind {
    /// EVAL with inline script source.
    #[label = "eval"]
    Eval,
    /// EVALSHA against the script cache.
    #[label = "evalsha"]
    Evalsha,
}

/// Why a Lua script execution failed.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptError {
    /// Scripting is not available on this shard.
    #[label = "not_available"]
    NotAvailable,
    /// The script itself failed during execution.
    #[label = "execution"]
    Execution,
    /// EVALSHA referenced a script missing from the cache.
    #[label = "noscript"]
    Noscript,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_enums_render_expected_strings() {
        assert_eq!(RejectionReason::MaxClients.as_str(), "maxclients");
        assert_eq!(TlsHandshakeError::Timeout.as_str(), "timeout");
        assert_eq!(PersistenceErrorType::Snapshot.as_str(), "snapshot");
        assert_eq!(
            PubsubLimitResource::UniqueChannels.as_str(),
            "unique_channels"
        );
        assert_eq!(ScriptKind::Evalsha.as_str(), "evalsha");
        assert_eq!(ScriptError::NotAvailable.as_str(), "not_available");
    }
}
