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
    #[error("ERR Error running script: {0}")]
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

impl ScriptError {
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
