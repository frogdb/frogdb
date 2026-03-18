#![no_main]
use frogdb_acl::{parse_acl_line, AclRule};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = AclRule::parse(s);
        let _ = parse_acl_line(s);
    }
});
