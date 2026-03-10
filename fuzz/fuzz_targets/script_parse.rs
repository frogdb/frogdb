#![no_main]
use frogdb_scripting::{load_library, parse_shebang};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let shebang_ok = parse_shebang(s).is_ok();
        if shebang_ok {
            let _ = load_library(s);
        }
    }
});
