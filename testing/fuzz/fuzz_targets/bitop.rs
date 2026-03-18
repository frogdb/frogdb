#![no_main]
use frogdb_types::bitmap::{BitOp, bitcount, bitop, bitpos};
use libfuzzer_sys::fuzz_target;
use libfuzzer_sys::arbitrary::{self, Arbitrary};

#[derive(Arbitrary, Debug)]
struct BitopInput {
    data: Vec<u8>,
    extra: Vec<u8>,
    start: Option<i64>,
    end: Option<i64>,
    bit_mode: bool,
    find_bit: bool,
    op_index: u8,
}

fuzz_target!(|input: BitopInput| {
    // Cap data sizes to prevent OOM
    if input.data.len() > 256 || input.extra.len() > 256 {
        return;
    }

    // Exercise bitcount
    let _ = bitcount(&input.data, input.start, input.end, input.bit_mode);

    // Exercise bitpos with end_given = end.is_some()
    let bit = input.find_bit as u8;
    let _ = bitpos(
        &input.data,
        bit,
        input.start,
        input.end,
        input.bit_mode,
        input.end.is_some(),
    );

    // Split extra into up to 4 sources for bitop
    let chunk_size = (input.extra.len() / 4).max(1);
    let sources: Vec<&[u8]> = input
        .extra
        .chunks(chunk_size)
        .take(4)
        .collect();

    let all_sources: Vec<&[u8]> = std::iter::once(input.data.as_slice())
        .chain(sources.iter().copied())
        .collect();

    let op = match input.op_index % 4 {
        0 => BitOp::And,
        1 => BitOp::Or,
        2 => BitOp::Xor,
        _ => BitOp::Not,
    };

    // NOT only takes one source
    if matches!(op, BitOp::Not) {
        let _ = bitop(op, &[input.data.as_slice()]);
    } else {
        let _ = bitop(op, &all_sources);
    }
});
