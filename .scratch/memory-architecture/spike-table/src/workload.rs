//! Key/value size distributions the sweep runs against.
//!
//! Three shapes, all generated from a fixed-seed xorshift so every variant and the
//! griddle baseline see byte-identical input:
//!
//! - `counters` — `cnt:<n>` keys, integer values. The shape INCR/HINCRBY traffic makes.
//! - `sessions` — `sess:<16 hex>` keys, 43-byte base64-ish tokens. Nothing inlines.
//! - `redis-feel` — the mixed shape: keys 8–48 B, values 45 % integers, 30 % ≤ 15 B,
//!   20 % 16–64 B, 5 % 65–512 B.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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

/// One generated pair. Values are either an integer or a byte string, matching
/// `StringValue`'s two forms in the shipped server.
pub struct Pair {
    pub key: Vec<u8>,
    pub int: Option<i64>,
    pub bytes: Vec<u8>,
}

impl Pair {
    pub fn value_len(&self) -> usize {
        match self.int {
            Some(_) => 8,
            None => self.bytes.len(),
        }
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Generates `n` distinct pairs for `shape`. Deterministic in `shape` and `n`.
pub fn generate(shape: Shape, n: usize) -> Vec<Pair> {
    let mut rng = Rng(0x5eed_1234_9e37_79b9);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(match shape {
            Shape::Counters => Pair {
                key: format!("cnt:{i}").into_bytes(),
                int: Some(rng.below(1_000_000) as i64),
                bytes: Vec::new(),
            },
            Shape::Sessions => {
                let mut key = b"sess:".to_vec();
                let r = rng.next();
                for d in 0..16 {
                    key.push(HEX[((r >> (d * 4)) & 0xf) as usize]);
                }
                // Distinctness: the 64-bit draw could repeat, so append the index.
                key.extend_from_slice(format!(":{i}").as_bytes());
                let mut token = Vec::with_capacity(43);
                while token.len() < 43 {
                    let r = rng.next();
                    for d in 0..8 {
                        if token.len() < 43 {
                            token.push(HEX[((r >> (d * 4)) & 0xf) as usize]);
                        }
                    }
                }
                Pair {
                    key,
                    int: None,
                    bytes: token,
                }
            }
            Shape::RedisFeel => {
                // `k{i}:` keeps keys distinct; random hex pads out to the drawn length.
                let key_len = 8 + rng.below(41) as usize; // 8..=48
                let mut key = format!("k{i}:").into_bytes();
                while key.len() < key_len {
                    key.push(HEX[rng.below(16) as usize]);
                }
                let roll = rng.below(100);
                let (int, bytes) = if roll < 45 {
                    (Some(rng.below(1 << 40) as i64), Vec::new())
                } else {
                    let len = if roll < 75 {
                        1 + rng.below(15) as usize // 1..=15
                    } else if roll < 95 {
                        16 + rng.below(49) as usize // 16..=64
                    } else {
                        65 + rng.below(448) as usize // 65..=512
                    };
                    let mut v = Vec::with_capacity(len);
                    while v.len() < len {
                        let r = rng.next();
                        for d in 0..8 {
                            if v.len() < len {
                                v.push(HEX[((r >> (d * 4)) & 0xf) as usize]);
                            }
                        }
                    }
                    (None, v)
                };
                Pair { key, int, bytes }
            }
        });
    }
    out
}

/// Summary of a generated set, printed alongside the sweep so the distributions are
/// on the record next to the numbers they produced.
pub struct Summary {
    pub keys: usize,
    pub key_mean: f64,
    pub key_le7: f64,
    pub key_le15: f64,
    pub int_values: f64,
    pub val_le7: f64,
    pub val_le15: f64,
    pub val_mean: f64,
}

pub fn summarize(pairs: &[Pair]) -> Summary {
    let n = pairs.len() as f64;
    let mut s = Summary {
        keys: pairs.len(),
        key_mean: 0.0,
        key_le7: 0.0,
        key_le15: 0.0,
        int_values: 0.0,
        val_le7: 0.0,
        val_le15: 0.0,
        val_mean: 0.0,
    };
    for p in pairs {
        s.key_mean += p.key.len() as f64;
        if p.key.len() <= 7 {
            s.key_le7 += 1.0;
        }
        if p.key.len() <= 15 {
            s.key_le15 += 1.0;
        }
        match p.int {
            Some(_) => {
                s.int_values += 1.0;
                s.val_mean += 8.0;
            }
            None => {
                s.val_mean += p.bytes.len() as f64;
                if p.bytes.len() <= 7 {
                    s.val_le7 += 1.0;
                }
                if p.bytes.len() <= 15 {
                    s.val_le15 += 1.0;
                }
            }
        }
    }
    s.key_mean /= n;
    s.val_mean /= n;
    s.key_le7 /= n;
    s.key_le15 /= n;
    s.int_values /= n;
    s.val_le7 /= n;
    s.val_le15 /= n;
    s
}
