#![no_main]
//! Fuzz the primary-key decoder: arbitrary bytes must yield `Ok` or a `KvError`,
//! never a panic.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // `rowid_of` parses the `/<table>/<index>/<rowid>` key layout; table id 1 is
    // the first user table. The decoder must reject malformed keys gracefully.
    let _ = kv::key::rowid_of(1, data);
});
