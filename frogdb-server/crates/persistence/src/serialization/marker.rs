//! The closed set of persisted type markers.
//!
//! Each marker is one byte at the head of a serialized payload. The byte values
//! are the on-disk **and** on-the-wire (replication) format and MUST NOT change:
//! existing snapshots, WAL segments, and replica full-syncs already contain them.
//!
//! Making the set a closed `#[repr(u8)]` enum (rather than 17 loose `const`s and a
//! `u8` match) lets the compiler reason about completeness: the decode dispatch in
//! [`super::registry::decode_for`] matches this enum with no wildcard, so a marker
//! without a decode arm is a *compile* error instead of a silent `UnknownType` at
//! load time.

use super::SerializationError;

/// Every persisted type marker. The explicit discriminants are the historic wire
/// bytes and are pinned by a test (`marker_bytes_are_stable`) — do not change them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum TypeMarker {
    StringRaw = 0,
    StringInt = 1,
    SortedSet = 2,
    Hash = 3,
    List = 4,
    Set = 5,
    Stream = 6,
    Bloom = 7,
    HyperLogLog = 8,
    TimeSeries = 9,
    Json = 10,
    HashWithFieldExpiry = 11,
    Cuckoo = 12,
    TopK = 13,
    TDigest = 14,
    Cms = 15,
    VectorSet = 16,
}

impl TypeMarker {
    /// Single source of truth for "what markers exist", in wire order. Used by the
    /// registry-coverage and round-trip symmetry tests.
    pub(crate) const ALL: &'static [TypeMarker] = &[
        Self::StringRaw,
        Self::StringInt,
        Self::SortedSet,
        Self::Hash,
        Self::List,
        Self::Set,
        Self::Stream,
        Self::Bloom,
        Self::HyperLogLog,
        Self::TimeSeries,
        Self::Json,
        Self::HashWithFieldExpiry,
        Self::Cuckoo,
        Self::TopK,
        Self::TDigest,
        Self::Cms,
        Self::VectorSet,
    ];

    /// The on-wire byte for this marker.
    pub(crate) fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a wire byte into a marker. This is the one place an unknown marker
    /// byte becomes an error; the registry never has to guard for it.
    pub(crate) fn from_byte(b: u8) -> Result<Self, SerializationError> {
        Self::ALL
            .iter()
            .copied()
            .find(|m| m.as_byte() == b)
            .ok_or(SerializationError::UnknownType(b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire bytes are the on-disk/replication format and must never drift.
    #[test]
    fn marker_bytes_are_stable() {
        assert_eq!(TypeMarker::StringRaw.as_byte(), 0);
        assert_eq!(TypeMarker::StringInt.as_byte(), 1);
        assert_eq!(TypeMarker::SortedSet.as_byte(), 2);
        assert_eq!(TypeMarker::Hash.as_byte(), 3);
        assert_eq!(TypeMarker::List.as_byte(), 4);
        assert_eq!(TypeMarker::Set.as_byte(), 5);
        assert_eq!(TypeMarker::Stream.as_byte(), 6);
        assert_eq!(TypeMarker::Bloom.as_byte(), 7);
        assert_eq!(TypeMarker::HyperLogLog.as_byte(), 8);
        assert_eq!(TypeMarker::TimeSeries.as_byte(), 9);
        assert_eq!(TypeMarker::Json.as_byte(), 10);
        assert_eq!(TypeMarker::HashWithFieldExpiry.as_byte(), 11);
        assert_eq!(TypeMarker::Cuckoo.as_byte(), 12);
        assert_eq!(TypeMarker::TopK.as_byte(), 13);
        assert_eq!(TypeMarker::TDigest.as_byte(), 14);
        assert_eq!(TypeMarker::Cms.as_byte(), 15);
        assert_eq!(TypeMarker::VectorSet.as_byte(), 16);
    }

    #[test]
    fn all_is_complete_and_unique() {
        // Every marker 0..=16 is present exactly once and ALL has no extras.
        assert_eq!(TypeMarker::ALL.len(), 17);
        let mut bytes: Vec<u8> = TypeMarker::ALL.iter().map(|m| m.as_byte()).collect();
        bytes.sort_unstable();
        bytes.dedup();
        assert_eq!(bytes, (0u8..=16).collect::<Vec<_>>());
    }

    #[test]
    fn from_byte_round_trips_every_marker() {
        for &m in TypeMarker::ALL {
            assert_eq!(TypeMarker::from_byte(m.as_byte()).unwrap(), m);
        }
    }

    #[test]
    fn from_byte_rejects_unknown() {
        assert!(matches!(
            TypeMarker::from_byte(17),
            Err(SerializationError::UnknownType(17))
        ));
        assert!(matches!(
            TypeMarker::from_byte(255),
            Err(SerializationError::UnknownType(255))
        ));
    }
}
