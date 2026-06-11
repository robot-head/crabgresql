//! Decoding of frontend (client → server) messages.
//!
//! All decode functions return `Ok(None)` when the buffer does not yet hold a
//! complete message, and never panic on malformed input.

use bytes::{Buf, Bytes, BytesMut};

use crate::error::PgError;

pub const PROTOCOL_3_0: i32 = 0x0003_0000; // 196608
pub const SSL_REQUEST_CODE: i32 = 80_877_103;
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;
pub const GSSENC_REQUEST_CODE: i32 = 80_877_104;

/// Matches real PostgreSQL's startup packet length cap (MAX_STARTUP_PACKET_LENGTH).
pub const MAX_STARTUP_PACKET_LEN: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    Startup { params: Vec<(String, String)> },
    SslRequest,
    GssEncRequest,
    CancelRequest { process_id: i32, secret_key: i32 },
}

pub fn decode_startup(buf: &mut BytesMut) -> Result<Option<StartupPacket>, PgError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if len < 8 || len as usize > MAX_STARTUP_PACKET_LEN {
        return Err(PgError::protocol(format!(
            "invalid startup packet length: {len}"
        )));
    }
    let len = len as usize;
    if buf.len() < len {
        return Ok(None);
    }
    let mut body = buf.split_to(len).freeze();
    body.advance(4); // length field
    let code = get_i32(&mut body)?;
    match code {
        SSL_REQUEST_CODE => Ok(Some(StartupPacket::SslRequest)),
        GSSENC_REQUEST_CODE => Ok(Some(StartupPacket::GssEncRequest)),
        CANCEL_REQUEST_CODE => Ok(Some(StartupPacket::CancelRequest {
            process_id: get_i32(&mut body)?,
            secret_key: get_i32(&mut body)?,
        })),
        PROTOCOL_3_0 => {
            let mut params = Vec::new();
            loop {
                let key = get_cstr(&mut body)?;
                if key.is_empty() {
                    break;
                }
                let value = get_cstr(&mut body)?;
                params.push((key, value));
            }
            Ok(Some(StartupPacket::Startup { params }))
        }
        other => Err(PgError::protocol(format!(
            "unsupported frontend protocol {}.{}; server supports 3.0",
            (other as u32) >> 16,
            (other as u32) & 0xffff,
        ))),
    }
}

// ---- checked readers (Bytes::get_* panic on underflow; never call those) ----

pub(crate) fn get_i32(buf: &mut Bytes) -> Result<i32, PgError> {
    if buf.len() < 4 {
        return Err(PgError::protocol("message truncated reading i32"));
    }
    Ok(buf.get_i32())
}

#[allow(dead_code)] // used from Task 4
pub(crate) fn get_i16(buf: &mut Bytes) -> Result<i16, PgError> {
    if buf.len() < 2 {
        return Err(PgError::protocol("message truncated reading i16"));
    }
    Ok(buf.get_i16())
}

#[allow(dead_code)] // used from Task 4
pub(crate) fn get_u8(buf: &mut Bytes) -> Result<u8, PgError> {
    if buf.is_empty() {
        return Err(PgError::protocol("message truncated reading byte"));
    }
    Ok(buf.get_u8())
}

#[allow(dead_code)] // used from Task 4
pub(crate) fn get_bytes(buf: &mut Bytes, n: usize) -> Result<Bytes, PgError> {
    if buf.len() < n {
        return Err(PgError::protocol("message truncated reading bytes"));
    }
    Ok(buf.split_to(n))
}

pub(crate) fn get_cstr(buf: &mut Bytes) -> Result<String, PgError> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| PgError::protocol("unterminated string"))?;
    let raw = buf.split_to(pos);
    buf.advance(1); // NUL
    String::from_utf8(raw.to_vec()).map_err(|_| PgError::protocol("string is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn startup_bytes(params: &[(&str, &str)]) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(PROTOCOL_3_0);
        for (k, v) in params {
            body.put_slice(k.as_bytes());
            body.put_u8(0);
            body.put_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0); // terminator
        let mut buf = BytesMut::new();
        buf.put_i32(body.len() as i32 + 4);
        buf.put_slice(&body);
        buf
    }

    #[test]
    fn decodes_startup_with_params() {
        let mut buf = startup_bytes(&[("user", "crab"), ("database", "crab")]);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            pkt,
            StartupPacket::Startup {
                params: vec![
                    ("user".into(), "crab".into()),
                    ("database".into(), "crab".into()),
                ]
            }
        );
        assert!(buf.is_empty(), "packet bytes fully consumed");
    }

    #[test]
    fn incomplete_packet_returns_none() {
        let full = startup_bytes(&[("user", "crab")]);
        let mut partial = BytesMut::from(&full[..full.len() - 3]);
        assert_eq!(decode_startup(&mut partial).expect("ok"), None);
    }

    #[test]
    fn decodes_ssl_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_i32(SSL_REQUEST_CODE);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(pkt, StartupPacket::SslRequest);
    }

    #[test]
    fn decodes_cancel_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        buf.put_i32(CANCEL_REQUEST_CODE);
        buf.put_i32(4242);
        buf.put_i32(-12345);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            pkt,
            StartupPacket::CancelRequest {
                process_id: 4242,
                secret_key: -12345
            }
        );
    }

    #[test]
    fn unknown_protocol_version_is_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(9);
        buf.put_i32(0x0002_0000); // protocol 2.0
        buf.put_u8(0);
        let err = decode_startup(&mut buf).expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[test]
    fn absurd_length_is_error_not_panic() {
        let mut buf = BytesMut::new();
        buf.put_i32(i32::MAX);
        buf.put_i32(PROTOCOL_3_0);
        assert!(decode_startup(&mut buf).is_err());
    }

    #[test]
    fn length_field_present_body_absent_returns_none() {
        let full = startup_bytes(&[("user", "crab")]);
        // Keep exactly 4 bytes: length field only.
        let mut partial = BytesMut::from(&full[..4]);
        assert_eq!(decode_startup(&mut partial).expect("ok"), None);
        // Buffer must be untouched (no split_to happened).
        assert_eq!(partial.len(), 4);
    }
}
