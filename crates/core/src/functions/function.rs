//! Registered function representation.

use bitflags::bitflags;

bitflags! {
    /// Flags that control function behavior.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct FunctionFlags: u32 {
        /// Function does not perform any writes (can be called with FCALL_RO).
        const NO_WRITES = 0b0000_0001;
        /// Function is allowed to run when server is out of memory.
        const ALLOW_OOM = 0b0000_0010;
        /// Function is allowed to run when server is in stale state.
        const ALLOW_STALE = 0b0000_1000;
        /// Function does not require any keys.
        const NO_CLUSTER = 0b0001_0000;
    }
}

impl FunctionFlags {
    /// Parse flags from a list of flag strings.
    pub fn from_strings(flags: &[String]) -> Result<Self, String> {
        let mut result = FunctionFlags::empty();
        for flag in flags {
            match flag.to_lowercase().as_str() {
                "no-writes" => result |= FunctionFlags::NO_WRITES,
                "allow-oom" => result |= FunctionFlags::ALLOW_OOM,
                "allow-stale" => result |= FunctionFlags::ALLOW_STALE,
                "no-cluster" => result |= FunctionFlags::NO_CLUSTER,
                _ => return Err(format!("Unknown flag: {}", flag)),
            }
        }
        Ok(result)
    }

    /// Convert flags to a list of flag strings.
    pub fn to_strings(&self) -> Vec<String> {
        let mut result = Vec::new();
        if self.contains(FunctionFlags::NO_WRITES) {
            result.push("no-writes".to_string());
        }
        if self.contains(FunctionFlags::ALLOW_OOM) {
            result.push("allow-oom".to_string());
        }
        if self.contains(FunctionFlags::ALLOW_STALE) {
            result.push("allow-stale".to_string());
        }
        if self.contains(FunctionFlags::NO_CLUSTER) {
            result.push("no-cluster".to_string());
        }
        result
    }
}

/// A registered function within a library.
#[derive(Debug, Clone)]
pub struct RegisteredFunction {
    /// The function name.
    pub name: String,

    /// Function flags.
    pub flags: FunctionFlags,

    /// Optional description of the function.
    pub description: Option<String>,
}

impl RegisteredFunction {
    /// Create a new registered function.
    pub fn new(name: String, flags: FunctionFlags, description: Option<String>) -> Self {
        Self {
            name,
            flags,
            description,
        }
    }

    /// Check if this function allows write operations.
    pub fn allows_writes(&self) -> bool {
        !self.flags.contains(FunctionFlags::NO_WRITES)
    }

    /// Check if this function can be called with FCALL_RO.
    pub fn is_read_only(&self) -> bool {
        self.flags.contains(FunctionFlags::NO_WRITES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flags_from_strings() {
        let flags =
            FunctionFlags::from_strings(&["no-writes".to_string(), "allow-oom".to_string()])
                .unwrap();
        assert!(flags.contains(FunctionFlags::NO_WRITES));
        assert!(flags.contains(FunctionFlags::ALLOW_OOM));
        assert!(!flags.contains(FunctionFlags::ALLOW_STALE));
    }

    #[test]
    fn test_flags_to_strings() {
        let flags = FunctionFlags::NO_WRITES | FunctionFlags::ALLOW_STALE;
        let strings = flags.to_strings();
        assert!(strings.contains(&"no-writes".to_string()));
        assert!(strings.contains(&"allow-stale".to_string()));
        assert!(!strings.contains(&"allow-oom".to_string()));
    }

    #[test]
    fn test_flags_unknown() {
        let result = FunctionFlags::from_strings(&["unknown-flag".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_function_read_only() {
        let func = RegisteredFunction::new(
            "test".to_string(),
            FunctionFlags::NO_WRITES,
            None,
        );
        assert!(func.is_read_only());
        assert!(!func.allows_writes());

        let func2 = RegisteredFunction::new("test2".to_string(), FunctionFlags::empty(), None);
        assert!(!func2.is_read_only());
        assert!(func2.allows_writes());
    }
}
