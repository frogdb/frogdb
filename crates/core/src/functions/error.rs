//! Error types for Redis Functions.

use std::fmt;

/// Errors that can occur during function operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionError {
    /// Library not found.
    LibraryNotFound { name: String },

    /// Function not found.
    FunctionNotFound { name: String },

    /// Library already exists (and REPLACE not specified).
    LibraryAlreadyExists { name: String },

    /// Function name already registered by another library.
    FunctionNameConflict {
        function_name: String,
        existing_library: String,
    },

    /// Invalid library code - missing or malformed shebang.
    InvalidShebang { message: String },

    /// Invalid library code - no functions registered.
    NoFunctionsRegistered,

    /// Invalid function flags.
    InvalidFlags { message: String },

    /// Library parsing error.
    ParseError { message: String },

    /// Lua runtime error during library loading.
    LoadError { message: String },

    /// Function execution error.
    ExecutionError { message: String },

    /// Function called with FCALL_RO attempted a write operation.
    ReadOnlyViolation { function_name: String },

    /// Function was killed.
    Killed,

    /// Function cannot be killed (has performed writes).
    Unkillable,

    /// Invalid engine specified (only "lua" is supported).
    UnsupportedEngine { engine: String },

    /// Serialization/deserialization error for DUMP/RESTORE.
    SerializationError { message: String },

    /// Invalid RESTORE policy.
    InvalidRestorePolicy { policy: String },

    /// Internal error.
    Internal { message: String },
}

impl fmt::Display for FunctionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionError::LibraryNotFound { name } => {
                write!(f, "ERR Library not found: {}", name)
            }
            FunctionError::FunctionNotFound { name } => {
                write!(f, "ERR Function not found: {}", name)
            }
            FunctionError::LibraryAlreadyExists { name } => {
                write!(
                    f,
                    "ERR Library '{}' already exists. Use REPLACE to update.",
                    name
                )
            }
            FunctionError::FunctionNameConflict {
                function_name,
                existing_library,
            } => {
                write!(
                    f,
                    "ERR Function {} already exists in library {}",
                    function_name, existing_library
                )
            }
            FunctionError::InvalidShebang { message } => {
                write!(f, "ERR Missing or invalid shebang: {}", message)
            }
            FunctionError::NoFunctionsRegistered => {
                write!(f, "ERR No functions registered in library")
            }
            FunctionError::InvalidFlags { message } => {
                write!(f, "ERR Invalid function flags: {}", message)
            }
            FunctionError::ParseError { message } => {
                write!(f, "ERR Error parsing library: {}", message)
            }
            FunctionError::LoadError { message } => {
                write!(f, "ERR Error loading library: {}", message)
            }
            FunctionError::ExecutionError { message } => {
                write!(f, "ERR Error running function: {}", message)
            }
            FunctionError::ReadOnlyViolation { function_name } => {
                write!(
                    f,
                    "ERR FCALL_RO called but function '{}' performed a write",
                    function_name
                )
            }
            FunctionError::Killed => {
                write!(f, "ERR function killed")
            }
            FunctionError::Unkillable => {
                write!(
                    f,
                    "ERR function has performed writes and cannot be killed"
                )
            }
            FunctionError::UnsupportedEngine { engine } => {
                write!(f, "ERR Unsupported engine '{}'. Only 'lua' is supported.", engine)
            }
            FunctionError::SerializationError { message } => {
                write!(f, "ERR Serialization error: {}", message)
            }
            FunctionError::InvalidRestorePolicy { policy } => {
                write!(f, "ERR Invalid restore policy: {}", policy)
            }
            FunctionError::Internal { message } => {
                write!(f, "ERR Internal error: {}", message)
            }
        }
    }
}

impl std::error::Error for FunctionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = FunctionError::LibraryNotFound {
            name: "mylib".to_string(),
        };
        assert_eq!(err.to_string(), "ERR Library not found: mylib");

        let err = FunctionError::FunctionNameConflict {
            function_name: "myfunc".to_string(),
            existing_library: "otherlib".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "ERR Function myfunc already exists in library otherlib"
        );
    }
}
