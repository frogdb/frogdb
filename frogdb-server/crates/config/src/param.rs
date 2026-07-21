//! The config-parameter lifecycle as a single deep module.
//!
//! A runtime-mutable config parameter is *one* concept — its type, default,
//! legal values, error modes, how CONFIG SET applies it, how CONFIG GET renders
//! it, and whether a change propagates to shards. [`ConfigParam`] owns that whole
//! lifecycle in one literal, so adding or changing a parameter is a local edit
//! rather than a sweep across five files.
//!
//! Parameters have heterogeneous value types (`u64`, `usize`, `u8`, `f64`,
//! `String`, `EvictionPolicy`, …), so they cannot live in a single
//! `Vec<ConfigParam<T>>`. The registry stores `Box<dyn DynParam<C>>`: all generic
//! work (parse/validate/apply over the concrete `T`) is monomorphized inside the
//! impl, and only `&str`-in / `String`-out crosses the `dyn` boundary.
//!
//! The apply/get closures need access to live server state (the runtime config
//! lock, listpack atomics, the log-reload handle, ACL/client registries). Those
//! handles live in the heavy `server` crate, which this lightweight crate must
//! not depend on. [`ConfigParam`] is therefore generic over a *context* type `C`
//! supplied by the caller (the server uses its `ConfigManager`); this crate never
//! names a server type while keeping the whole lifecycle in one place.

use std::fmt;

/// Error type for CONFIG operations.
///
/// `Display` is wire-visible: CONFIG SET failures are returned to clients via
/// `to_string()`, so these strings are part of the protocol surface.
#[derive(Debug, Clone)]
pub enum ConfigError {
    /// Parameter is not mutable at runtime.
    ImmutableParameter(String),
    /// Parameter does not exist.
    UnknownParameter(String),
    /// Invalid value for the parameter.
    InvalidValue {
        /// The parameter name.
        param: String,
        /// A human-readable explanation of why the value was rejected.
        message: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::ImmutableParameter(name) => {
                write!(f, "ERR CONFIG parameter '{}' is not mutable", name)
            }
            ConfigError::UnknownParameter(name) => {
                write!(f, "ERR Unknown CONFIG parameter '{}'", name)
            }
            ConfigError::InvalidValue { param, message } => {
                write!(f, "ERR Invalid value for '{}': {}", param, message)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Which internal subsystem a successful set must propagate to.
///
/// CONFIG SET applies a value locally; some parameters additionally require
/// pushing the change to every shard worker. Keeping the propagation kind *on*
/// the parameter definition replaces the hardcoded `eviction_params` name list
/// that previously decided this out of band.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Propagation {
    /// No shard propagation needed.
    None,
    /// Rebuild `EvictionConfig` and notify all shards.
    Eviction,
    /// Send the key-memory-histograms toggle to all shards.
    KeyMemoryHistograms,
}

/// Everything a caller must know about one runtime-mutable parameter, in one
/// place: its type, default, legal values (via [`parse`](ConfigParam::parse)),
/// error modes, how CONFIG SET applies it at runtime, how CONFIG GET renders it,
/// and whether a change propagates to shards.
///
/// `C` is the application-supplied context the apply/get closures reach state
/// through (the server passes its `ConfigManager`). Function-pointer fields keep
/// `ConfigParam` cheap, `Send + Sync`, and free of captured state.
pub struct ConfigParam<T: 'static, C: ?Sized> {
    /// Redis-style name, e.g. `"maxmemory-policy"`.
    pub name: &'static str,

    /// Parse + reject in one place. The set of legal values *is* this function's
    /// domain, so there is no separate "valid list" to keep in sync.
    pub parse: fn(&str) -> Result<T, ConfigError>,

    /// Cross-field / range validation against the live context. `Ok(())` for the
    /// common case where `parse` already fully constrains the value.
    pub validate: fn(&T, &C) -> Result<(), ConfigError>,

    /// The compile-time default — ideally the same fn serde's `#[serde(default)]`
    /// uses, so the file default and the CONFIG default cannot diverge.
    pub default: fn() -> T,

    /// Read the live typed value back (for CONFIG GET). Must round-trip with
    /// [`render`](ConfigParam::render).
    pub get: fn(&C) -> T,

    /// Apply a validated value: write runtime state and run side effects (log
    /// reload, client eviction, atomics). Runtime-apply is part of the interface,
    /// not a caller concern.
    pub apply: fn(&C, T) -> Result<(), ConfigError>,

    /// Render a typed value for the wire (CONFIG GET parity, e.g. ms→seconds).
    pub render: fn(&T) -> String,

    /// Whether a successful set must propagate to shards, and how.
    pub propagation: Propagation,
}

/// Object-safe view used by the registry and by CONFIG GET/SET.
///
/// All generic work (parse/validate/apply over the concrete `T`) is monomorphized
/// inside the [`ConfigParam`] impl; only `&str`-in / `String`-out crosses the
/// `dyn` boundary.
pub trait DynParam<C: ?Sized>: Send + Sync {
    /// The Redis-style parameter name.
    fn name(&self) -> &'static str;
    /// Render the live value for CONFIG GET.
    fn get(&self, ctx: &C) -> String;
    /// Run the full set lifecycle (parse → validate → apply) for CONFIG SET.
    fn set(&self, ctx: &C, raw: &str) -> Result<(), ConfigError>;
    /// How a successful set propagates to shards.
    fn propagation(&self) -> Propagation;
    /// Whether this is a Redis-compatibility no-op parameter (accepts and
    /// ignores any value). Real lifecycle params return `false`; the dedicated
    /// no-op impl overrides this. Lets a consumer partition the registry by
    /// no-op-ness without downcasting through the `dyn` boundary — used by the
    /// server's `test_param_registry_consistency` to pin `info.noop` against the
    /// serving entry.
    fn is_noop(&self) -> bool {
        false
    }
}

impl<T: 'static, C: ?Sized> DynParam<C> for ConfigParam<T, C> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn get(&self, ctx: &C) -> String {
        (self.render)(&(self.get)(ctx))
    }

    /// The whole set lifecycle, in one ordered place: parse → validate → apply.
    fn set(&self, ctx: &C, raw: &str) -> Result<(), ConfigError> {
        let parsed = (self.parse)(raw)?; // parse + legal-value check, once
        (self.validate)(&parsed, ctx)?; // cross-field / range, once
        (self.apply)(ctx, parsed) // runtime-apply + side effects, once
    }

    fn propagation(&self) -> Propagation {
        self.propagation
    }
}

impl<T: 'static, C: ?Sized> ConfigParam<T, C> {
    /// Default no-op validator: accept whatever `parse` produced.
    pub fn no_validate(_value: &T, _ctx: &C) -> Result<(), ConfigError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// Minimal context for exercising the generic lifecycle without a server.
    #[derive(Default)]
    struct TestCtx {
        value: Cell<u64>,
        applied: Cell<bool>,
    }

    fn u64_param() -> ConfigParam<u64, TestCtx> {
        ConfigParam {
            name: "test-u64",
            parse: |s| {
                s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                    param: "test-u64".to_string(),
                    message: "must be a non-negative integer".to_string(),
                })
            },
            validate: |v, _ctx| {
                if *v > 100 {
                    Err(ConfigError::InvalidValue {
                        param: "test-u64".to_string(),
                        message: "must be <= 100".to_string(),
                    })
                } else {
                    Ok(())
                }
            },
            default: || 5,
            get: |ctx| ctx.value.get(),
            apply: |ctx, v| {
                ctx.value.set(v);
                ctx.applied.set(true);
                Ok(())
            },
            render: |v| v.to_string(),
            propagation: Propagation::None,
        }
    }

    #[test]
    fn dyn_get_renders_value() {
        let p = u64_param();
        let ctx = TestCtx::default();
        ctx.value.set(42);
        let dynp: &dyn DynParam<TestCtx> = &p;
        assert_eq!(dynp.get(&ctx), "42");
        assert_eq!(dynp.name(), "test-u64");
        assert_eq!(dynp.propagation(), Propagation::None);
    }

    #[test]
    fn set_runs_parse_validate_apply_in_order() {
        let p = u64_param();
        let ctx = TestCtx::default();
        let dynp: &dyn DynParam<TestCtx> = &p;
        assert!(dynp.set(&ctx, "10").is_ok());
        assert_eq!(ctx.value.get(), 10);
        assert!(ctx.applied.get());
    }

    #[test]
    fn set_rejects_unparseable_before_apply() {
        let p = u64_param();
        let ctx = TestCtx::default();
        let dynp: &dyn DynParam<TestCtx> = &p;
        let err = dynp.set(&ctx, "abc").unwrap_err();
        assert!(matches!(err, ConfigError::InvalidValue { .. }));
        // apply never ran: value untouched, applied flag still false.
        assert_eq!(ctx.value.get(), 0);
        assert!(!ctx.applied.get());
    }

    #[test]
    fn set_rejects_invalid_value_before_apply() {
        let p = u64_param();
        let ctx = TestCtx::default();
        let dynp: &dyn DynParam<TestCtx> = &p;
        let err = dynp.set(&ctx, "200").unwrap_err();
        assert!(matches!(err, ConfigError::InvalidValue { .. }));
        // validate failed after a successful parse, but before apply.
        assert!(!ctx.applied.get());
    }

    #[test]
    fn default_is_callable() {
        let p = u64_param();
        assert_eq!((p.default)(), 5);
    }

    #[test]
    fn config_error_display_is_stable() {
        assert_eq!(
            ConfigError::ImmutableParameter("bind".to_string()).to_string(),
            "ERR CONFIG parameter 'bind' is not mutable"
        );
        assert_eq!(
            ConfigError::UnknownParameter("nope".to_string()).to_string(),
            "ERR Unknown CONFIG parameter 'nope'"
        );
        assert_eq!(
            ConfigError::InvalidValue {
                param: "maxmemory".to_string(),
                message: "must be a non-negative integer".to_string(),
            }
            .to_string(),
            "ERR Invalid value for 'maxmemory': must be a non-negative integer"
        );
    }
}
