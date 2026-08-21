//! Redis version identifiers.
//!
//! FrogDB tracks two independent Redis version numbers:
//! - the version it *advertises* to clients, which pins the protocol and
//!   scripting surface clients should expect it to behave like.
//! - the upstream version its command behavior is *measured against*, used
//!   by the regression suite and the docs site's compatibility reporting.
//!
//! These are allowed to diverge — FrogDB can widen its compatibility target
//! ahead of bumping the version it advertises to clients. As of the 8.6.0
//! advertise bump (ADR-0005 / issue 06) the two happen to agree; that is
//! coincidence, not an invariant — expect them to diverge again as the
//! compat target moves ahead of the next advertise bump.

/// The Redis version FrogDB advertises to clients: `INFO`'s `redis_version`
/// field, the Lua `redis.REDIS_VERSION` binding, and HELLO's `version` reply
/// field. This is the single source of truth for all three — never hardcode
/// the string at a call site.
pub const ADVERTISED_REDIS_VERSION: &str = "8.6.0";

/// The upstream Redis version FrogDB's command compatibility is measured
/// against, used by the regression suite and the docs site's compatibility
/// tables.
pub const REDIS_COMPAT_TARGET: &str = "8.6.0";
