//! Keyspace notification event type flags.
//!
//! Redis-compatible keyspace notifications publish events to special pub/sub
//! channels when keys are modified, expired, or evicted. The event flags
//! control which events are enabled and how they are delivered.

use bitflags::bitflags;

bitflags! {
    /// Keyspace notification event type flags.
    /// Matches Redis flag characters: K, E, g, $, l, s, h, z, x, e, t, m, d, A, n, o, c
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct KeyspaceEventFlags: u32 {
        /// K -- Keyspace events, published in `__keyspace@<db>__` channel prefix.
        const KEYSPACE    = 0b0000_0000_0000_0001;
        /// E -- Keyevent events, published in `__keyevent@<db>__` channel prefix.
        const KEYEVENT    = 0b0000_0000_0000_0010;
        /// g -- Generic commands (non-type specific): DEL, EXPIRE, RENAME, etc.
        const GENERIC     = 0b0000_0000_0000_0100;
        /// $ -- String commands.
        const STRING      = 0b0000_0000_0000_1000;
        /// l -- List commands.
        const LIST        = 0b0000_0000_0001_0000;
        /// s -- Set commands.
        const SET         = 0b0000_0000_0010_0000;
        /// h -- Hash commands.
        const HASH        = 0b0000_0000_0100_0000;
        /// z -- Sorted set commands.
        const ZSET        = 0b0000_0000_1000_0000;
        /// x -- Expired events (key TTL reached).
        const EXPIRED     = 0b0000_0001_0000_0000;
        /// e -- Evicted events (maxmemory policy).
        const EVICTED     = 0b0000_0010_0000_0000;
        /// t -- Stream commands.
        const STREAM      = 0b0000_0100_0000_0000;
        /// m -- Key miss events (access to non-existing key).
        const MISS        = 0b0000_1000_0000_0000;
        /// d -- Module key type events (unused in FrogDB).
        const MODULE      = 0b0001_0000_0000_0000;
        /// n -- New key events (key created for the first time).
        const NEW         = 0b0010_0000_0000_0000;
        /// o -- Overwritten events (key value replaced).
        const OVERWRITTEN = 0b0100_0000_0000_0000;
        /// c -- Type changed events (key type changed, e.g. hash -> string).
        const TYPE_CHANGED = 0b1000_0000_0000_0000;
        /// A -- Alias for "g$lshzxet" (all data-type event flags).
        const ALL_TYPES   = Self::GENERIC.bits() | Self::STRING.bits() | Self::LIST.bits()
                          | Self::SET.bits() | Self::HASH.bits() | Self::ZSET.bits()
                          | Self::EXPIRED.bits() | Self::EVICTED.bits() | Self::STREAM.bits();
    }
}

impl KeyspaceEventFlags {
    /// Parse a Redis-style flag string into bitflags.
    ///
    /// Returns `None` if the string contains invalid characters.
    pub fn from_flag_string(s: &str) -> Option<Self> {
        let mut flags = Self::empty();
        for ch in s.chars() {
            match ch {
                'K' => flags |= Self::KEYSPACE,
                'E' => flags |= Self::KEYEVENT,
                'g' => flags |= Self::GENERIC,
                '$' => flags |= Self::STRING,
                'l' => flags |= Self::LIST,
                's' => flags |= Self::SET,
                'h' => flags |= Self::HASH,
                'z' => flags |= Self::ZSET,
                'x' => flags |= Self::EXPIRED,
                'e' => flags |= Self::EVICTED,
                't' => flags |= Self::STREAM,
                'm' => flags |= Self::MISS,
                'd' => flags |= Self::MODULE,
                'n' => flags |= Self::NEW,
                'o' => flags |= Self::OVERWRITTEN,
                'c' => flags |= Self::TYPE_CHANGED,
                'A' => flags |= Self::ALL_TYPES,
                _ => return None,
            }
        }
        // If any event type is set but neither K nor E, implicitly enable both
        if flags.intersects(
            Self::ALL_TYPES
                | Self::MISS
                | Self::MODULE
                | Self::NEW
                | Self::OVERWRITTEN
                | Self::TYPE_CHANGED,
        ) && !flags.intersects(Self::KEYSPACE | Self::KEYEVENT)
        {
            flags |= Self::KEYSPACE | Self::KEYEVENT;
        }
        Some(flags)
    }

    /// Convert back to a Redis-style flag string.
    pub fn to_flag_string(self) -> String {
        let mut s = String::new();
        if self.contains(Self::KEYSPACE) {
            s.push('K');
        }
        if self.contains(Self::KEYEVENT) {
            s.push('E');
        }
        if self.contains(Self::GENERIC) {
            s.push('g');
        }
        if self.contains(Self::STRING) {
            s.push('$');
        }
        if self.contains(Self::LIST) {
            s.push('l');
        }
        if self.contains(Self::SET) {
            s.push('s');
        }
        if self.contains(Self::HASH) {
            s.push('h');
        }
        if self.contains(Self::ZSET) {
            s.push('z');
        }
        if self.contains(Self::EXPIRED) {
            s.push('x');
        }
        if self.contains(Self::EVICTED) {
            s.push('e');
        }
        if self.contains(Self::STREAM) {
            s.push('t');
        }
        if self.contains(Self::MISS) {
            s.push('m');
        }
        if self.contains(Self::MODULE) {
            s.push('d');
        }
        if self.contains(Self::NEW) {
            s.push('n');
        }
        if self.contains(Self::OVERWRITTEN) {
            s.push('o');
        }
        if self.contains(Self::TYPE_CHANGED) {
            s.push('c');
        }
        s
    }

    /// Check if notifications are enabled at all (any type flag set + at least one delivery channel).
    pub fn is_active(self) -> bool {
        let has_type = self.intersects(
            Self::ALL_TYPES
                | Self::MISS
                | Self::MODULE
                | Self::NEW
                | Self::OVERWRITTEN
                | Self::TYPE_CHANGED,
        );
        let has_channel = self.intersects(Self::KEYSPACE | Self::KEYEVENT);
        has_type && has_channel
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_string() {
        let flags = KeyspaceEventFlags::from_flag_string("").unwrap();
        assert_eq!(flags, KeyspaceEventFlags::empty());
        assert!(!flags.is_active());
    }

    #[test]
    fn test_invalid_chars() {
        assert!(KeyspaceEventFlags::from_flag_string("KX").is_none());
        assert!(KeyspaceEventFlags::from_flag_string("!").is_none());
        assert!(KeyspaceEventFlags::from_flag_string("KEa").is_none());
    }

    #[test]
    fn test_all_alias() {
        let flags = KeyspaceEventFlags::from_flag_string("KA").unwrap();
        assert!(flags.contains(KeyspaceEventFlags::KEYSPACE));
        assert!(flags.contains(KeyspaceEventFlags::GENERIC));
        assert!(flags.contains(KeyspaceEventFlags::STRING));
        assert!(flags.contains(KeyspaceEventFlags::LIST));
        assert!(flags.contains(KeyspaceEventFlags::SET));
        assert!(flags.contains(KeyspaceEventFlags::HASH));
        assert!(flags.contains(KeyspaceEventFlags::ZSET));
        assert!(flags.contains(KeyspaceEventFlags::EXPIRED));
        assert!(flags.contains(KeyspaceEventFlags::EVICTED));
        assert!(flags.contains(KeyspaceEventFlags::STREAM));
        // A does not include MISS, MODULE, NEW, OVERWRITTEN, TYPE_CHANGED
        assert!(!flags.contains(KeyspaceEventFlags::MISS));
        assert!(!flags.contains(KeyspaceEventFlags::MODULE));
        assert!(!flags.contains(KeyspaceEventFlags::NEW));
    }

    #[test]
    fn test_implicit_ke_addition() {
        // If type flags are set but neither K nor E, both are added
        let flags = KeyspaceEventFlags::from_flag_string("g").unwrap();
        assert!(flags.contains(KeyspaceEventFlags::KEYSPACE));
        assert!(flags.contains(KeyspaceEventFlags::KEYEVENT));
        assert!(flags.contains(KeyspaceEventFlags::GENERIC));
    }

    #[test]
    fn test_explicit_ke_no_implicit() {
        // If K is explicitly set, E should not be forced
        let flags = KeyspaceEventFlags::from_flag_string("Kg").unwrap();
        assert!(flags.contains(KeyspaceEventFlags::KEYSPACE));
        assert!(!flags.contains(KeyspaceEventFlags::KEYEVENT));
        assert!(flags.contains(KeyspaceEventFlags::GENERIC));
    }

    #[test]
    fn test_ke_only_not_active() {
        // K or E alone (without type flags) is not active
        let flags = KeyspaceEventFlags::from_flag_string("KE").unwrap();
        assert!(!flags.is_active());
    }

    #[test]
    fn test_roundtrip() {
        let inputs = ["KEg$lshzxet", "Kg", "E$l", "KEmn"];
        for input in inputs {
            let flags = KeyspaceEventFlags::from_flag_string(input).unwrap();
            let output = flags.to_flag_string();
            let reparsed = KeyspaceEventFlags::from_flag_string(&output).unwrap();
            assert_eq!(flags, reparsed, "Roundtrip failed for: {input}");
        }
    }

    #[test]
    fn test_is_active() {
        // Active: has both channel and type
        let flags = KeyspaceEventFlags::from_flag_string("Kg").unwrap();
        assert!(flags.is_active());

        // Active: implicit K+E
        let flags = KeyspaceEventFlags::from_flag_string("$").unwrap();
        assert!(flags.is_active());

        // Not active: empty
        let flags = KeyspaceEventFlags::from_flag_string("").unwrap();
        assert!(!flags.is_active());

        // Not active: only channels, no type
        let flags = KeyspaceEventFlags::from_flag_string("KE").unwrap();
        assert!(!flags.is_active());
    }

    #[test]
    fn test_all_individual_flags() {
        let flag_string = "KEg$lshzxetmdnoc";
        let flags = KeyspaceEventFlags::from_flag_string(flag_string).unwrap();
        assert!(flags.contains(KeyspaceEventFlags::KEYSPACE));
        assert!(flags.contains(KeyspaceEventFlags::KEYEVENT));
        assert!(flags.contains(KeyspaceEventFlags::GENERIC));
        assert!(flags.contains(KeyspaceEventFlags::STRING));
        assert!(flags.contains(KeyspaceEventFlags::LIST));
        assert!(flags.contains(KeyspaceEventFlags::SET));
        assert!(flags.contains(KeyspaceEventFlags::HASH));
        assert!(flags.contains(KeyspaceEventFlags::ZSET));
        assert!(flags.contains(KeyspaceEventFlags::EXPIRED));
        assert!(flags.contains(KeyspaceEventFlags::EVICTED));
        assert!(flags.contains(KeyspaceEventFlags::STREAM));
        assert!(flags.contains(KeyspaceEventFlags::MISS));
        assert!(flags.contains(KeyspaceEventFlags::MODULE));
        assert!(flags.contains(KeyspaceEventFlags::NEW));
        assert!(flags.contains(KeyspaceEventFlags::OVERWRITTEN));
        assert!(flags.contains(KeyspaceEventFlags::TYPE_CHANGED));
    }

    #[test]
    fn test_to_flag_string_order() {
        // Verify output order matches Redis convention
        let flags = KeyspaceEventFlags::from_flag_string("KEg$lshzxetmdnoc").unwrap();
        let output = flags.to_flag_string();
        assert_eq!(output, "KEg$lshzxetmdnoc");
    }
}
