//! The table's hash: keyed ahash, one key per table.
//!
//! Same hash family griddle and hashbrown default to, so a table-vs-griddle
//! lookup comparison is at hasher parity and measures the *structure*, not the
//! hash. It is taken directly rather than through griddle because the table needs
//! two things a default `RandomState` will not give:
//!
//! - **A key it chooses.** Every table is seeded, so a client cannot pick keys
//!   that collide in a bucket it does not know the seed of.
//! - **A seed it can reproduce.** Deterministic sims and fuzz replays need the
//!   same key to land in the same bucket every run, or a failing seed is not a
//!   failing seed.
//!
//! [`TableSeed::from_entropy`] is the production path and
//! [`TableSeed::from_u64`] the reproducible one; a table takes whichever it is
//! handed and never reaches for randomness itself.

use std::hash::{Hash, Hasher};

use ahash::RandomState;

/// The key a table hashes with.
///
/// Four words, because that is what `ahash`'s keyed constructor takes. Cheap to
/// copy and cheap to store, so a table keeps one rather than a `RandomState`
/// behind a pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableSeed([u64; 4]);

impl TableSeed {
    /// Draws a seed from the OS. The production path: a fresh key per table means
    /// an attacker who learns one shard's collision set learns nothing about the
    /// next.
    pub fn from_entropy() -> TableSeed {
        use rand::RngExt;
        let mut rng = rand::rng();
        TableSeed([rng.random(), rng.random(), rng.random(), rng.random()])
    }

    /// Derives a seed from one number, for a sim or a fuzz replay that has to put
    /// the same key in the same bucket on every run.
    ///
    /// The expansion is a SplitMix64 walk, so neighbouring seeds — `0`, `1`, `2`,
    /// which is exactly what a fuzzer feeds it — give unrelated keys rather than
    /// four nearly identical words.
    pub const fn from_u64(seed: u64) -> TableSeed {
        let mut state = seed;
        let mut words = [0u64; 4];
        let mut i = 0;
        while i < 4 {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            words[i] = z ^ (z >> 31);
            i += 1;
        }
        TableSeed(words)
    }

    /// The raw key words, for a caller that has to persist or log a seed to make
    /// a run reproducible.
    pub const fn words(&self) -> [u64; 4] {
        self.0
    }
}

/// The table's hasher: a [`TableSeed`] turned into something that hashes keys.
#[derive(Debug, Clone)]
pub struct TableHasher {
    state: RandomState,
    seed: TableSeed,
}

impl TableHasher {
    /// Builds a hasher over `seed`.
    pub fn new(seed: TableSeed) -> TableHasher {
        let [a, b, c, d] = seed.0;
        TableHasher {
            state: RandomState::with_seeds(a, b, c, d),
            seed,
        }
    }

    /// The seed this hasher was built from — what a test or a sim logs to make a
    /// failure reproducible.
    pub fn seed(&self) -> TableSeed {
        self.seed
    }

    /// Hashes a key.
    ///
    /// Bytes are hashed as bytes, deliberately not as `[u8]`: `Hash for [u8]`
    /// mixes in the length, and the table wants the hash of the key's contents so
    /// the same bytes hash the same however the caller holds them.
    #[inline]
    pub fn hash(&self, key: &[u8]) -> u64 {
        self.state.hash_one(Bytes(key))
    }
}

/// Hashes a byte string by its bytes alone.
struct Bytes<'a>(&'a [u8]);

impl Hash for Bytes<'_> {
    #[inline]
    fn hash<H: Hasher>(&self, h: &mut H) {
        h.write(self.0);
    }
}

/// The 8-bit fingerprint stored in a bucket's metadata block: the hash's top
/// byte, so it is independent of the low bits the directory routes on and stays
/// a full-strength filter at every depth.
#[inline]
pub const fn fingerprint(hash: u64) -> u8 {
    (hash >> 56) as u8
}

/// The routing bits stored beside the fingerprint: the hash's low 16 bits, which
/// carry the split bit for every depth under [`crate::layout::ROUTE_BITS`].
#[inline]
pub const fn route(hash: u64) -> u16 {
    hash as u16
}

/// The bucket a key calls home, computed from the two things a slot stores.
///
/// This is why it is `fp` and `route` and not some third hash slice: a split has
/// to place every entry it moves, and if home came from bits the slot does not
/// keep, "placing" would mean rehashing the key — the 808-hashes-to-move-404
/// cost the spike measured and follow-up 2 exists to remove. Reading home out of
/// stored metadata makes a split a pure copy.
///
/// The two fields are concatenated rather than added so the result is not a
/// function of either alone. In particular `fp` is not recoverable from `home`,
/// so entries sharing a home bucket still spread across all 256 fingerprints and
/// the in-bucket filter keeps its full strength.
#[inline]
pub const fn home(fp: u8, route: u16, regular_buckets: usize) -> usize {
    let bits = ((fp as u32) << 16) | route as u32;
    bits as usize % regular_buckets
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn the_same_seed_hashes_the_same_way_and_a_different_one_does_not() {
        let a = TableHasher::new(TableSeed::from_u64(7));
        let b = TableHasher::new(TableSeed::from_u64(7));
        let c = TableHasher::new(TableSeed::from_u64(8));

        assert_eq!(a.hash(b"user:1000"), b.hash(b"user:1000"));
        assert_ne!(a.hash(b"user:1000"), c.hash(b"user:1000"));
    }

    #[test]
    fn a_replayed_seed_round_trips_through_its_words() {
        let seed = TableSeed::from_u64(0xDEAD_BEEF);
        assert_eq!(TableHasher::new(seed).seed(), seed);
        assert_eq!(seed.words(), TableSeed::from_u64(0xDEAD_BEEF).words());
    }

    /// A fuzzer feeds 0, 1, 2. Those must not produce near-identical keys, or a
    /// whole corpus explores one hash.
    #[test]
    fn adjacent_seeds_give_unrelated_keys() {
        let mut seen = HashSet::new();
        for s in 0u64..64 {
            for w in TableSeed::from_u64(s).words() {
                assert!(seen.insert(w), "seed expansion repeated a key word");
            }
        }
    }

    #[test]
    fn entropy_seeds_differ_between_tables() {
        let a = TableSeed::from_entropy();
        let b = TableSeed::from_entropy();
        assert_ne!(a.words(), b.words());
    }

    #[test]
    fn fingerprint_and_route_read_opposite_ends_of_the_hash() {
        let h = 0xA1B2_C3D4_E5F6_0718u64;
        assert_eq!(fingerprint(h), 0xA1);
        assert_eq!(route(h), 0x0718);
    }

    /// Home has to spread over the bucket array *within one segment*, where the
    /// low `depth` route bits are all the same. If it did not, a deep segment
    /// would pile every key onto a handful of buckets.
    #[test]
    fn home_spreads_within_a_segment() {
        const REGULAR: usize = crate::layout::REGULAR_BUCKETS;
        let h = TableHasher::new(TableSeed::from_u64(3));
        let mut counts = vec![0usize; REGULAR];
        let mut total = 0;
        for i in 0..400_000u32 {
            let hash = h.hash(format!("key:{i}").as_bytes());
            // One segment's worth of keys at depth 11.
            if hash & 0x7FF == 0x2A {
                counts[home(fingerprint(hash), route(hash), REGULAR)] += 1;
                total += 1;
            }
        }
        assert!(total > 100, "not enough same-segment keys sampled: {total}");
        assert!(
            counts.iter().filter(|&&c| c > 0).count() > REGULAR / 2,
            "same-segment keys reached only {} of {REGULAR} home buckets",
            counts.iter().filter(|&&c| c > 0).count()
        );
    }

    /// Home must not be a function of the fingerprint, or every entry in a bucket
    /// would share a fingerprint class and the filter would stop filtering.
    #[test]
    fn one_home_bucket_still_sees_every_fingerprint() {
        const REGULAR: usize = crate::layout::REGULAR_BUCKETS;
        let mut fps = HashSet::new();
        for r in 0u16..=u16::MAX {
            for fp in [0u8, 1, 17, 200, 255] {
                if home(fp, r, REGULAR) == 7 {
                    fps.insert(fp);
                }
            }
        }
        assert_eq!(fps.len(), 5, "home bucket 7 excluded some fingerprints");
    }

    /// The fingerprint must not be a function of the bits the directory routes
    /// on, or every key in a deep segment shares one fingerprint and the filter
    /// stops filtering.
    #[test]
    fn fingerprints_stay_spread_among_keys_that_route_together() {
        let h = TableHasher::new(TableSeed::from_u64(1));
        let mut fps = HashSet::new();
        let mut found = 0;
        for i in 0..200_000u32 {
            let key = format!("key:{i}");
            let hash = h.hash(key.as_bytes());
            // Everything that lands in the same segment at depth 11.
            if hash & 0x7FF == 0x123 {
                fps.insert(fingerprint(hash));
                found += 1;
            }
        }
        assert!(found > 50, "not enough same-segment keys sampled: {found}");
        assert!(
            fps.len() > 30,
            "same-segment keys shared only {} fingerprints out of {found} keys",
            fps.len()
        );
    }
}
