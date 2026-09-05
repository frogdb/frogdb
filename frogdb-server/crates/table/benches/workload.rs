//! The three key/value shapes the spike measured, regenerated here.
//!
//! Reproduced from `.scratch/memory-architecture/spike-report-table.md` so issue
//! 11's numbers sit in the same table as the spike's rather than beside them:
//!
//! | shape | keys | values |
//! | --- | --- | --- |
//! | `counters` | `cnt:{i}`, mean 9.9 B | 100 % integers |
//! | `sessions` | `sess:{16 hex}:{i}`, mean 27.9 B | 43-byte tokens, 0 % inlinable |
//! | `redis-feel` | 8–48 B, mean 28.0 B | 45 % integers, 30 % ≤ 15 B, 20 % 16–64 B, 5 % 65–512 B |
//!
//! Generation is a SplitMix64 walk from a fixed seed rather than `rand`, so a
//! number in the report can be reproduced from the shape name and the key count
//! alone, on any machine, at any version of any dependency.

#![allow(dead_code)] // Each bench uses a subset; the shapes belong together.

/// A deterministic value: the number a `counters`-style entry holds, or the bytes
/// a `sessions`-style one does.
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
}

/// One generated entry.
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Value,
}

/// The shapes, by the names the spike report uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Counters,
    Sessions,
    RedisFeel,
}

impl Shape {
    pub const ALL: [Shape; 3] = [Shape::Counters, Shape::Sessions, Shape::RedisFeel];

    pub fn name(self) -> &'static str {
        match self {
            Shape::Counters => "counters",
            Shape::Sessions => "sessions",
            Shape::RedisFeel => "redis-feel",
        }
    }
}

/// SplitMix64. Small enough to read, good enough to shape a workload, and fixed
/// forever so a reported number stays reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..n`. Biased by at most `2^-64 * n`, which no workload shape
    /// can tell from uniform.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| (self.next() & 0xFF) as u8).collect()
    }

    fn hex(&mut self, chars: usize) -> String {
        (0..chars)
            .map(|_| char::from_digit((self.next() % 16) as u32, 16).expect("digit under 16"))
            .collect()
    }
}

/// `n` entries of `shape`, identical on every run and every machine.
pub fn generate(shape: Shape, n: usize) -> Vec<Entry> {
    let mut rng = Rng(0x5EED_1111_2222_3333);
    (0..n)
        .map(|i| match shape {
            Shape::Counters => Entry {
                key: format!("cnt:{i}").into_bytes(),
                value: Value::Int(rng.next() as i64 >> 8),
            },
            Shape::Sessions => Entry {
                key: format!("sess:{}:{i}", rng.hex(16)).into_bytes(),
                // A 43-byte token: past the 7-byte inline limit by design, so this
                // shape exercises the heap-record path on every value.
                value: Value::Bytes(rng.bytes(43)),
            },
            Shape::RedisFeel => {
                // Keys 8–48 B. The suffix carries `i` so keys stay unique however
                // the random prefix falls.
                let width = 8 + rng.below(41) as usize;
                let mut key = format!("k:{i}:").into_bytes();
                while key.len() < width {
                    key.push(b'a' + (rng.next() % 26) as u8);
                }
                let class = rng.below(100);
                let value = match class {
                    0..=44 => Value::Int(rng.next() as i64 >> 8),
                    45..=74 => {
                        let len = 1 + rng.below(15) as usize;
                        Value::Bytes(rng.bytes(len))
                    }
                    75..=94 => {
                        let len = 16 + rng.below(49) as usize;
                        Value::Bytes(rng.bytes(len))
                    }
                    _ => {
                        let len = 65 + rng.below(448) as usize;
                        Value::Bytes(rng.bytes(len))
                    }
                };
                Entry { key, value }
            }
        })
        .collect()
}

/// Keys that are *not* in `generate(shape, n)`, for measuring a lookup miss.
pub fn absent_keys(shape: Shape, n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| match shape {
            Shape::Counters => format!("cnt:absent:{i}").into_bytes(),
            Shape::Sessions => format!("sess:absent:{i}").into_bytes(),
            Shape::RedisFeel => format!("k:absent:{i}").into_bytes(),
        })
        .collect()
}

/// Mean key length, so a bench run can show it agrees with the shape the spike
/// measured rather than assuming it does.
pub fn mean_key_len(entries: &[Entry]) -> f64 {
    if entries.is_empty() {
        return 0.0;
    }
    entries.iter().map(|e| e.key.len()).sum::<usize>() as f64 / entries.len() as f64
}
