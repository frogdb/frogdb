#![no_main]
use frogdb_search::{parse_ft_alter_args, parse_ft_create_args};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let segments: Vec<&[u8]> = data.split(|&b| b == 0).collect();
    if segments.len() > 64 {
        return;
    }
    let _ = parse_ft_create_args("fuzz_idx", &segments);
    let _ = parse_ft_alter_args(&segments);
});
