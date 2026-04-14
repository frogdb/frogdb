//! Scripting errors.

use bytes::Bytes;
use thiserror::Error;

/// Errors that can occur during script execution.
#[derive(Debug, Clone, Error)]
pub enum ScriptError {
    /// Script not found in cache (NOSCRIPT).
    #[error("NOSCRIPT No matching script. Please use EVAL.")]
    NoScript,

    /// Script execution timed out.
    #[error("ERR BUSY Lua script running. Allow up to {timeout_ms}ms for it to finish.")]
    Timeout { timeout_ms: u64 },

    /// Script exceeded memory limit.
    #[error("ERR script memory limit exceeded")]
    MemoryLimitExceeded,

    /// Key not declared in KEYS array (strict mode).
    #[error("ERR script tried to access undeclared key '{key}'")]
    UndeclaredKey { key: String },

    /// Keys belong to different shards (CROSSSLOT).
    #[error("CROSSSLOT Keys in request don't hash to the same slot")]
    CrossSlot,

    /// Forbidden command called from script.
    #[error("{0}")]
    ForbiddenCommand(String),

    /// Lua runtime error.
    ///
    /// The contained message is formatted with `ERR Error running script: `
    /// prefix unless it already starts with a known Redis error prefix (e.g.
    /// WRONGTYPE, MOVED, EXECABORT) — those are passed through verbatim so
    /// clients can match on the prefix, matching Redis behaviour.
    #[error("{}", Self::format_runtime(.0))]
    Runtime(String),

    /// Lua compilation error.
    #[error("ERR Error compiling script: {0}")]
    Compilation(String),

    /// Script called during script execution (nested).
    #[error("ERR nested script calls not allowed")]
    NestedScript,

    /// Script was killed by SCRIPT KILL.
    #[error("ERR script killed by user request")]
    Killed,

    /// Script cannot be killed because it has performed writes.
    #[error("UNKILLABLE The script has performed writes. Use SHUTDOWN NOSAVE to abort.")]
    Unkillable,

    /// Internal error.
    #[error("ERR {0}")]
    Internal(String),
}

/// Redis error prefixes that should be passed through verbatim from scripts
/// rather than being wrapped with "ERR Error running script:".
const PASSTHROUGH_PREFIXES: &[&str] = &[
    "WRONGTYPE",
    "MOVED",
    "ASK",
    "CLUSTERDOWN",
    "CROSSSLOT",
    "LOADING",
    "READONLY",
    "EXECABORT",
    "MASTERDOWN",
    "NOREPLICAS",
    "NOTBUSY",
];

impl ScriptError {
    /// Format a runtime error message, preserving known Redis error prefixes.
    fn format_runtime(msg: &str) -> String {
        // If the message already starts with a known Redis error prefix,
        // pass it through verbatim so clients can match on the prefix.
        for prefix in PASSTHROUGH_PREFIXES {
            if msg.starts_with(prefix) {
                return msg.to_string();
            }
        }
        format!("ERR Error running script: {msg}")
    }

    /// Convert to bytes for RESP encoding.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

impl From<crate::sync::LockError> for ScriptError {
    fn from(err: crate::sync::LockError) -> Self {
        ScriptError::Internal(format!("Lock error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noscript_error() {
        let err = ScriptError::NoScript;
        assert!(err.to_string().contains("NOSCRIPT"));
    }

    #[test]
    fn test_undeclared_key_error() {
        let err = ScriptError::UndeclaredKey {
            key: "mykey".to_string(),
        };
        assert!(err.to_string().contains("mykey"));
        assert!(err.to_string().contains("undeclared key"));
    }

    #[test]
    fn test_crossslot_error() {
        let err = ScriptError::CrossSlot;
        assert!(err.to_string().contains("CROSSSLOT"));
    }
}
