#![no_main]
//! Fuzz the pgwire frontend decoders: arbitrary bytes from an untrusted client
//! must yield protocol errors, never a panic (the fuzzing-scale counterpart of
//! the `decode_startup_never_panics` / `decode_message_never_panics` proptests).

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Startup-phase decoder.
    let mut buf = BytesMut::from(data);
    let _ = pgwire::messages::frontend::decode_startup(&mut buf);

    // Steady-state decoder: drain the buffer like a real connection would,
    // guarding against a non-consuming decode so the target cannot loop forever.
    let mut buf = BytesMut::from(data);
    loop {
        let before = buf.len();
        match pgwire::messages::frontend::decode_message(&mut buf) {
            Ok(Some(_msg)) if buf.len() < before => continue, // made progress
            _ => break,                                       // need-more / error / no progress
        }
    }
});
