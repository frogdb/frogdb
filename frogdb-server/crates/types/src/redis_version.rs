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

/// [`ADVERTISED_REDIS_VERSION`] packed as `(major << 16) | (minor << 8) |
/// patch`, matching Redis's `REDIS_VERSION_NUM` encoding. Derived at compile
/// time from the dotted string so the two can never drift apart the way
/// `frogdb-scripting`'s hand-maintained `REDIS_VERSION_NUM` once did (it
/// stayed pinned to 7.2.0 across the 8.6.0 advertise bump).
pub const ADVERTISED_REDIS_VERSION_NUM: i64 = parse_version_num(ADVERTISED_REDIS_VERSION);

/// Parses a `"major.minor.patch"` string into the packed `REDIS_VERSION_NUM`
/// encoding, at compile time.
const fn parse_version_num(version: &str) -> i64 {
    let bytes = version.as_bytes();
    let len = bytes.len();

    let mut first_dot = 0;
    while first_dot < len && bytes[first_dot] != b'.' {
        first_dot += 1;
    }
    let mut second_dot = first_dot + 1;
    while second_dot < len && bytes[second_dot] != b'.' {
        second_dot += 1;
    }

    let major = parse_u32(bytes, 0, first_dot);
    let minor = parse_u32(bytes, first_dot + 1, second_dot);
    let patch = parse_u32(bytes, second_dot + 1, len);

    ((major as i64) << 16) | ((minor as i64) << 8) | (patch as i64)
}

/// Parses the decimal digits `bytes[start..end]` into a `u32`, at compile
/// time.
const fn parse_u32(bytes: &[u8], start: usize, end: usize) -> u32 {
    let mut value: u32 = 0;
    let mut i = start;
    while i < end {
        value = value * 10 + (bytes[i] - b'0') as u32;
        i += 1;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_version_num_matches_advertised_version() {
        // 8.6.0 -> (8 << 16) | (6 << 8) | 0
        assert_eq!(ADVERTISED_REDIS_VERSION_NUM, 0x0008_0600);
    }

    #[test]
    fn parse_version_num_handles_multi_digit_components() {
        assert_eq!(
            parse_version_num("12.34.56"),
            (12i64 << 16) | (34 << 8) | 56
        );
    }
}
