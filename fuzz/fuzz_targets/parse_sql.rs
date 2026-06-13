#![no_main]
//! Fuzz the SQL parser: arbitrary UTF-8 text must yield a parse error, never a
//! panic, OOM, or hang.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(sql) = std::str::from_utf8(data) {
        let _ = pgparser::parse(sql);
    }
});
