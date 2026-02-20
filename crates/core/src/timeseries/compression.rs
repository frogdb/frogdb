//! Gorilla compression for time series data.
//!
//! Implements the Facebook Gorilla compression scheme:
//! - Delta-of-delta encoding for timestamps
//! - XOR encoding with leading/trailing zero optimization for values
//!
//! Reference: "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
//! by Pelkonen et al. (2015)

use bitvec::prelude::*;

/// Encode a series of (timestamp, value) samples into compressed bytes.
///
/// The encoding uses:
/// - Delta-of-delta for timestamps with variable-length encoding
/// - XOR with previous value for floats with leading/trailing zero optimization
pub fn encode_samples(samples: &[(i64, f64)]) -> Vec<u8> {
    if samples.is_empty() {
        return Vec::new();
    }

    let mut bits: BitVec<u8, Msb0> = BitVec::new();

    // Write header: number of samples (32 bits)
    let count = samples.len() as u32;
    for i in (0..32).rev() {
        bits.push((count >> i) & 1 != 0);
    }

    // First sample: full timestamp (64 bits) + full value (64 bits)
    let (mut prev_ts, mut prev_val) = samples[0];
    write_bits(&mut bits, prev_ts as u64, 64);
    write_bits(&mut bits, prev_val.to_bits(), 64);

    if samples.len() == 1 {
        return bits.into_vec();
    }

    // Second sample: delta timestamp + XOR value
    let (ts, val) = samples[1];
    let mut prev_delta = ts - prev_ts;
    write_bits(&mut bits, prev_delta as u64, 14); // 14-bit delta for first
    encode_value(&mut bits, prev_val, val);
    prev_ts = ts;
    prev_val = val;

    // Remaining samples: delta-of-delta + XOR
    for &(ts, val) in &samples[2..] {
        let delta = ts - prev_ts;
        let delta_of_delta = delta - prev_delta;

        encode_timestamp_dod(&mut bits, delta_of_delta);
        encode_value(&mut bits, prev_val, val);

        prev_delta = delta;
        prev_ts = ts;
        prev_val = val;
    }

    bits.into_vec()
}

/// Decode compressed bytes back into samples.
pub fn decode_samples(data: &[u8]) -> Vec<(i64, f64)> {
    if data.is_empty() {
        return Vec::new();
    }

    let bits = BitVec::<u8, Msb0>::from_slice(data);
    let mut pos = 0;

    // Read header: number of samples
    if bits.len() < 32 {
        return Vec::new();
    }
    let count = read_bits(&bits, &mut pos, 32) as usize;

    if count == 0 {
        return Vec::new();
    }

    let mut samples = Vec::with_capacity(count);

    // Read first sample
    if bits.len() < pos + 128 {
        return Vec::new();
    }
    let first_ts = read_bits(&bits, &mut pos, 64) as i64;
    let first_val = f64::from_bits(read_bits(&bits, &mut pos, 64));
    samples.push((first_ts, first_val));

    if count == 1 {
        return samples;
    }

    // Read second sample
    if bits.len() < pos + 14 {
        return samples;
    }
    let first_delta = read_bits(&bits, &mut pos, 14) as i64;
    let second_ts = first_ts + first_delta;
    let second_val = decode_value(&bits, &mut pos, first_val);
    samples.push((second_ts, second_val));

    // Read remaining samples
    let mut prev_delta = first_delta;
    let mut prev_ts = second_ts;
    let mut prev_val = second_val;

    for _ in 2..count {
        if pos >= bits.len() {
            break;
        }

        let dod = decode_timestamp_dod(&bits, &mut pos);
        let delta = prev_delta + dod;
        let ts = prev_ts + delta;
        let val = decode_value(&bits, &mut pos, prev_val);

        samples.push((ts, val));

        prev_delta = delta;
        prev_ts = ts;
        prev_val = val;
    }

    samples
}

/// Encode delta-of-delta timestamp using variable-length encoding.
///
/// Encoding scheme:
/// - 0: dod = 0 (same delta as before)
/// - 10 + 7 bits: dod in [-63, 64]
/// - 110 + 9 bits: dod in [-255, 256]
/// - 1110 + 12 bits: dod in [-2047, 2048]
/// - 1111 + 32 bits: full delta-of-delta
fn encode_timestamp_dod(bits: &mut BitVec<u8, Msb0>, dod: i64) {
    if dod == 0 {
        bits.push(false); // 0
    } else if (-63..=64).contains(&dod) {
        bits.push(true); // 1
        bits.push(false); // 0
                          // Write 7 bits (biased by 63)
        let biased = (dod + 63) as u64;
        write_bits(bits, biased, 7);
    } else if (-255..=256).contains(&dod) {
        bits.push(true); // 1
        bits.push(true); // 1
        bits.push(false); // 0
                          // Write 9 bits (biased by 255)
        let biased = (dod + 255) as u64;
        write_bits(bits, biased, 9);
    } else if (-2047..=2048).contains(&dod) {
        bits.push(true); // 1
        bits.push(true); // 1
        bits.push(true); // 1
        bits.push(false); // 0
                          // Write 12 bits (biased by 2047)
        let biased = (dod + 2047) as u64;
        write_bits(bits, biased, 12);
    } else {
        bits.push(true); // 1
        bits.push(true); // 1
        bits.push(true); // 1
        bits.push(true); // 1
                         // Write full 32 bits
        write_bits(bits, dod as u64, 32);
    }
}

/// Decode delta-of-delta timestamp.
fn decode_timestamp_dod(bits: &BitVec<u8, Msb0>, pos: &mut usize) -> i64 {
    if *pos >= bits.len() {
        return 0;
    }

    if !bits[*pos] {
        *pos += 1;
        return 0;
    }
    *pos += 1;

    if *pos >= bits.len() {
        return 0;
    }

    if !bits[*pos] {
        *pos += 1;
        // 7 bits
        let biased = read_bits(bits, pos, 7);
        return biased as i64 - 63;
    }
    *pos += 1;

    if *pos >= bits.len() {
        return 0;
    }

    if !bits[*pos] {
        *pos += 1;
        // 9 bits
        let biased = read_bits(bits, pos, 9);
        return biased as i64 - 255;
    }
    *pos += 1;

    if *pos >= bits.len() {
        return 0;
    }

    if !bits[*pos] {
        *pos += 1;
        // 12 bits
        let biased = read_bits(bits, pos, 12);
        return biased as i64 - 2047;
    }
    *pos += 1;

    // 32 bits (signed)
    let val = read_bits(bits, pos, 32);
    val as i32 as i64
}

/// Encode a value using XOR with previous value.
///
/// Encoding scheme:
/// - 0: XOR = 0 (same value)
/// - 10: XOR has same leading/trailing zeros as previous, just write meaningful bits
/// - 11: Write 5 bits leading zeros, 6 bits length, then meaningful bits
fn encode_value(bits: &mut BitVec<u8, Msb0>, prev: f64, current: f64) {
    let xor = prev.to_bits() ^ current.to_bits();

    if xor == 0 {
        bits.push(false); // 0
        return;
    }

    bits.push(true); // 1

    let leading = xor.leading_zeros() as usize;
    let trailing = xor.trailing_zeros() as usize;
    let meaningful_bits = 64 - leading - trailing;

    // Always use the full encoding for simplicity
    bits.push(true); // 1 (control bit for full encoding)

    // Write leading zeros (5 bits, max 31)
    let leading_clamped = leading.min(31);
    write_bits(bits, leading_clamped as u64, 5);

    // Write meaningful bit count (6 bits, 1-64, store as 0-63)
    write_bits(bits, (meaningful_bits - 1) as u64, 6);

    // Write the meaningful bits
    let meaningful_value = xor >> trailing;
    write_bits(bits, meaningful_value, meaningful_bits);
}

/// Decode a value using XOR with previous value.
fn decode_value(bits: &BitVec<u8, Msb0>, pos: &mut usize, prev: f64) -> f64 {
    if *pos >= bits.len() {
        return prev;
    }

    if !bits[*pos] {
        *pos += 1;
        return prev; // Same value
    }
    *pos += 1;

    if *pos >= bits.len() {
        return prev;
    }

    if !bits[*pos] {
        // Use previous leading/trailing (not implemented for simplicity)
        // Fall through to full decode
        *pos += 1;
    } else {
        *pos += 1;
    }

    // Read leading zeros (5 bits)
    let leading = read_bits(bits, pos, 5) as usize;

    // Read meaningful bit count (6 bits, stored as count-1)
    let meaningful_bits = read_bits(bits, pos, 6) as usize + 1;

    // Read meaningful bits
    let meaningful_value = read_bits(bits, pos, meaningful_bits);

    // Reconstruct XOR value
    let trailing = 64 - leading - meaningful_bits;
    let xor = meaningful_value << trailing;

    f64::from_bits(prev.to_bits() ^ xor)
}

/// Write n bits of value to the bit vector.
fn write_bits(bits: &mut BitVec<u8, Msb0>, value: u64, n: usize) {
    for i in (0..n).rev() {
        bits.push((value >> i) & 1 != 0);
    }
}

/// Read n bits from the bit vector at the given position.
fn read_bits(bits: &BitVec<u8, Msb0>, pos: &mut usize, n: usize) -> u64 {
    let mut value = 0u64;
    for _ in 0..n {
        if *pos >= bits.len() {
            break;
        }
        value = (value << 1) | (if bits[*pos] { 1 } else { 0 });
        *pos += 1;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_samples() {
        let samples: Vec<(i64, f64)> = vec![];
        let encoded = encode_samples(&samples);
        let decoded = decode_samples(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_single_sample() {
        let samples = vec![(1000i64, 42.5f64)];
        let encoded = encode_samples(&samples);
        let decoded = decode_samples(&encoded);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, 1000);
        assert!((decoded[0].1 - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_two_samples() {
        let samples = vec![(1000i64, 42.5f64), (1060i64, 43.0f64)];
        let encoded = encode_samples(&samples);
        let decoded = decode_samples(&encoded);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, 1000);
        assert_eq!(decoded[1].0, 1060);
        assert!((decoded[0].1 - 42.5).abs() < f64::EPSILON);
        assert!((decoded[1].1 - 43.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_multiple_samples_constant_delta() {
        let samples: Vec<(i64, f64)> = (0..10)
            .map(|i| (1000 + i * 60, 20.0 + i as f64 * 0.1))
            .collect();
        let encoded = encode_samples(&samples);
        let decoded = decode_samples(&encoded);
        assert_eq!(decoded.len(), samples.len());
        for (i, (ts, val)) in decoded.iter().enumerate() {
            assert_eq!(*ts, samples[i].0);
            assert!((val - samples[i].1).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_same_values() {
        let samples: Vec<(i64, f64)> = (0..5).map(|i| (1000 + i * 60, 42.0)).collect();
        let encoded = encode_samples(&samples);
        let decoded = decode_samples(&encoded);
        assert_eq!(decoded.len(), samples.len());
        for (decoded_sample, original) in decoded.iter().zip(samples.iter()) {
            assert_eq!(decoded_sample.0, original.0);
            assert!((decoded_sample.1 - original.1).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_compression_ratio() {
        // 100 samples with regular timestamps should compress well
        let samples: Vec<(i64, f64)> = (0..100)
            .map(|i| (1706000000000 + i * 60000, 20.0 + (i % 10) as f64 * 0.1))
            .collect();
        let encoded = encode_samples(&samples);
        let uncompressed_size = samples.len() * 16; // 8 bytes ts + 8 bytes value
                                                    // Should achieve some compression
        assert!(encoded.len() < uncompressed_size);
    }

    #[test]
    fn test_special_float_values() {
        let samples = vec![
            (1000i64, 0.0f64),
            (1060i64, f64::MAX),
            (1120i64, f64::MIN),
            (1180i64, f64::MIN_POSITIVE),
        ];
        let encoded = encode_samples(&samples);
        let decoded = decode_samples(&encoded);
        assert_eq!(decoded.len(), samples.len());
        for (i, (ts, val)) in decoded.iter().enumerate() {
            assert_eq!(*ts, samples[i].0);
            assert_eq!(*val, samples[i].1);
        }
    }

    #[test]
    fn test_negative_timestamps_delta() {
        // Decreasing timestamps (edge case)
        let samples = vec![(1000i64, 1.0f64), (900i64, 2.0f64), (800i64, 3.0f64)];
        let encoded = encode_samples(&samples);
        let decoded = decode_samples(&encoded);
        assert_eq!(decoded.len(), samples.len());
    }
}
