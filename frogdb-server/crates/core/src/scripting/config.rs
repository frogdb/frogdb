//! Scripting configuration.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Configuration for Lua scripting.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScriptingConfig {
    /// Maximum execution time in milliseconds (0 = unlimited).
    #[serde(default = "default_lua_time_limit_ms")]
    pub lua_time_limit_ms: u64,

    /// Shared runtime-mutable time limit (used when CONFIG SET changes it).
    /// When present, `setup_timeout_hook` reads from this instead of `lua_time_limit_ms`.
    #[serde(skip)]
    pub lua_time_limit_override: Option<Arc<AtomicU64>>,

    /// Maximum memory per VM in megabytes (0 = unlimited).
    #[serde(default = "default_lua_heap_limit_mb")]
    pub lua_heap_limit_mb: usize,

    /// Grace period before forcible kill in milliseconds.
    #[serde(default = "default_lua_timeout_grace_ms")]
    pub lua_timeout_grace_ms: u64,

    /// Maximum number of scripts in cache.
    #[serde(default = "default_lua_script_cache_max_size")]
    pub lua_script_cache_max_size: usize,

    /// Maximum total size of cached scripts in bytes.
    #[serde(default = "default_lua_script_cache_max_bytes")]
    pub lua_script_cache_max_bytes: usize,
}

fn default_lua_time_limit_ms() -> u64 {
    5000
}

fn default_lua_heap_limit_mb() -> usize {
    256
}

fn default_lua_timeout_grace_ms() -> u64 {
    100
}

fn default_lua_script_cache_max_size() -> usize {
    10000
}

fn default_lua_script_cache_max_bytes() -> usize {
    104857600 // 100MB
}

impl Default for ScriptingConfig {
    fn default() -> Self {
        Self {
            lua_time_limit_ms: default_lua_time_limit_ms(),
            lua_time_limit_override: None,
            lua_heap_limit_mb: default_lua_heap_limit_mb(),
            lua_timeout_grace_ms: default_lua_timeout_grace_ms(),
            lua_script_cache_max_size: default_lua_script_cache_max_size(),
            lua_script_cache_max_bytes: default_lua_script_cache_max_bytes(),
        }
    }
}

impl ScriptingConfig {
    /// Get the effective lua time limit, preferring the runtime override if set.
    pub fn effective_lua_time_limit_ms(&self) -> u64 {
        if let Some(ref override_val) = self.lua_time_limit_override {
            override_val.load(Ordering::Relaxed)
        } else {
            self.lua_time_limit_ms
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ScriptingConfig::default();
        assert_eq!(config.lua_time_limit_ms, 5000);
        assert_eq!(config.lua_heap_limit_mb, 256);
        assert_eq!(config.lua_timeout_grace_ms, 100);
        assert_eq!(config.lua_script_cache_max_size, 10000);
        assert_eq!(config.lua_script_cache_max_bytes, 104857600);
    }
}
