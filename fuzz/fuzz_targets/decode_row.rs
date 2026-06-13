#![no_main]
//! Fuzz the row decoder: arbitrary bytes must yield `Ok` or a `KvError`, never a
//! panic or unbounded allocation.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = kv::rowenc::decode_row(data);
});
