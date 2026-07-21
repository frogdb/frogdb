//! Redis version identifiers.
//!
//! FrogDB tracks two independent Redis version numbers:
//! - the version it *advertises* to clients, which pins the protocol and
//!   scripting surface clients should expect it to behave like.
//! - the upstream version its command behavior is *measured against*, used
//!   by the regression suite and the docs site's compatibility reporting.
//!
//! These are allowed to diverge — FrogDB can widen its compatibility target
//! ahead of bumping the version it advertises to clients.

/// The Redis version FrogDB advertises to clients, via `INFO`'s
/// `redis_version` field and the Lua `redis.REDIS_VERSION` binding.
pub const ADVERTISED_REDIS_VERSION: &str = "7.2.0";

/// The upstream Redis version FrogDB's command compatibility is measured
/// against, used by the regression suite and the docs site's compatibility
/// tables.
pub const REDIS_COMPAT_TARGET: &str = "8.6.0";
