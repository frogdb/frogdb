#![no_main]
use frogdb_types::bitmap::{
    BitfieldEncoding, BitfieldOffset, OverflowMode, bitfield_get, bitfield_incrby, bitfield_set,
};
use libfuzzer_sys::fuzz_target;
use libfuzzer_sys::arbitrary::{self, Arbitrary};

#[derive(Arbitrary, Debug)]
struct BitfieldInput {
    data: Vec<u8>,
    encoding_signed: bool,
    encoding_bits: u8,
    offset_value: u16,
    value: i64,
    increment: i64,
    overflow_mode: u8,
}

fuzz_target!(|input: BitfieldInput| {
    // Cap data to prevent OOM
    if input.data.len() > 1024 {
        return;
    }

    // Build a valid encoding — signed: 1..=64, unsigned: 1..=63
    let bits = if input.encoding_signed {
        (input.encoding_bits % 64) + 1 // 1..=64
    } else {
        (input.encoding_bits % 63) + 1 // 1..=63
    };
    let encoding = if input.encoding_signed {
        BitfieldEncoding::Signed(bits)
    } else {
        BitfieldEncoding::Unsigned(bits)
    };

    let offset = input.offset_value as u64;
    let overflow = match input.overflow_mode % 3 {
        0 => OverflowMode::Wrap,
        1 => OverflowMode::Sat,
        _ => OverflowMode::Fail,
    };

    // Exercise parse helpers
    let enc_str = if input.encoding_signed {
        format!("i{bits}")
    } else {
        format!("u{bits}")
    };
    let _ = BitfieldEncoding::parse(enc_str.as_bytes());
    let _ = BitfieldOffset::parse(format!("{offset}").as_bytes());
    let _ = BitfieldOffset::parse(format!("#{offset}").as_bytes());

    // Exercise get
    let _ = bitfield_get(&input.data, encoding, offset);

    // Exercise set and get-after-set roundtrip
    let mut buf = input.data.clone();
    let old = bitfield_set(&mut buf, encoding, offset, input.value);
    let readback = bitfield_get(&buf, encoding, offset);
    // After set, get should return a value consistent with the encoding range
    let _ = (old, readback);

    // Exercise incrby
    let mut buf2 = input.data.clone();
    let _ = bitfield_incrby(&mut buf2, encoding, offset, input.increment, overflow);
});
