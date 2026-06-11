//! Server-side SCRAM-SHA-256 (RFC 5802/7677), on RustCrypto primitives.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use rand::Rng;
use rand::distr::Alphanumeric;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{PgError, sqlstate};

type HmacSha256 = Hmac<Sha256>;

pub const DEFAULT_ITERATIONS: u32 = 4096; // PostgreSQL's default

enum State {
    Initial,
    SentServerFirst {
        client_first_bare: String,
        server_first: String,
        full_nonce: String,
        gs2_header: &'static str,
    },
    Done,
}

pub struct ScramServer {
    password: String,
    salt: Vec<u8>,
    iterations: u32,
    server_nonce: String,
    state: State,
}

impl ScramServer {
    pub fn new(password: &str) -> Self {
        let salt: [u8; 16] = rand::rng().random();
        let server_nonce: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();
        Self::new_with(password, salt.to_vec(), DEFAULT_ITERATIONS, server_nonce)
    }

    /// Deterministic constructor for tests.
    pub fn new_with(password: &str, salt: Vec<u8>, iterations: u32, server_nonce: String) -> Self {
        Self {
            password: password.to_string(),
            salt,
            iterations,
            server_nonce,
            state: State::Initial,
        }
    }

    pub fn handle_client_first(&mut self, msg: &[u8]) -> Result<Vec<u8>, PgError> {
        if !matches!(self.state, State::Initial) {
            return Err(PgError::protocol("SCRAM: unexpected client-first message"));
        }
        let msg = std::str::from_utf8(msg)
            .map_err(|_| PgError::protocol("SCRAM: client-first is not UTF-8"))?;

        // gs2 header: we never advertise SCRAM-SHA-256-PLUS, so requiring
        // channel binding ("p=...") is a protocol violation.
        let (gs2_header, bare) = if let Some(rest) = msg.strip_prefix("n,,") {
            ("n,,", rest)
        } else if let Some(rest) = msg.strip_prefix("y,,") {
            ("y,,", rest)
        } else {
            return Err(PgError::protocol(
                "SCRAM: unsupported gs2 header (channel binding not offered)",
            ));
        };

        let client_nonce = attr(bare, 'r')?;
        let full_nonce = format!("{client_nonce}{}", self.server_nonce);
        let server_first = format!(
            "r={full_nonce},s={},i={}",
            B64.encode(&self.salt),
            self.iterations
        );

        self.state = State::SentServerFirst {
            client_first_bare: bare.to_string(),
            server_first: server_first.clone(),
            full_nonce,
            gs2_header,
        };
        Ok(server_first.into_bytes())
    }

    pub fn handle_client_final(&mut self, msg: &[u8]) -> Result<Vec<u8>, PgError> {
        let State::SentServerFirst {
            client_first_bare,
            server_first,
            full_nonce,
            gs2_header,
        } = std::mem::replace(&mut self.state, State::Done)
        else {
            return Err(PgError::protocol("SCRAM: unexpected client-final message"));
        };
        let msg = std::str::from_utf8(msg)
            .map_err(|_| PgError::protocol("SCRAM: client-final is not UTF-8"))?;

        let channel = attr(msg, 'c')?;
        if channel != B64.encode(gs2_header) {
            return Err(PgError::protocol("SCRAM: channel binding data mismatch"));
        }
        if attr(msg, 'r')? != full_nonce {
            return Err(PgError::protocol("SCRAM: nonce mismatch"));
        }
        let proof = B64
            .decode(attr(msg, 'p')?)
            .map_err(|_| PgError::protocol("SCRAM: proof is not valid base64"))?;
        let without_proof = msg
            .rsplit_once(",p=")
            .map(|(head, _)| head)
            .ok_or_else(|| PgError::protocol("SCRAM: missing proof"))?;

        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            self.password.as_bytes(),
            &self.salt,
            self.iterations,
            &mut salted,
        );
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = Sha256::digest(&client_key);
        let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
        let client_signature = hmac(&stored_key, auth_message.as_bytes());

        if proof.len() != 32 {
            return Err(PgError::fatal(
                sqlstate::INVALID_PASSWORD,
                "password authentication failed",
            ));
        }
        let recovered_key: Vec<u8> = proof
            .iter()
            .zip(client_signature.iter())
            .map(|(p, s)| p ^ s)
            .collect();
        let ok: bool = Sha256::digest(&recovered_key).ct_eq(&stored_key).into();
        if !ok {
            return Err(PgError::fatal(
                sqlstate::INVALID_PASSWORD,
                "password authentication failed",
            ));
        }

        let server_key = hmac(&salted, b"Server Key");
        let server_signature = hmac(&server_key, auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(server_signature)).into_bytes())
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Extracts the value of a comma-separated `x=value` attribute.
fn attr(msg: &str, name: char) -> Result<&str, PgError> {
    msg.split(',')
        .find_map(|part| part.strip_prefix(name).and_then(|p| p.strip_prefix('=')))
        .ok_or_else(|| PgError::protocol(format!("SCRAM: missing attribute '{name}'")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as B64;

    /// The exact exchange from RFC 7677 §3 (user "user", password "pencil").
    #[test]
    fn rfc_7677_test_vector() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server = ScramServer::new_with(
            "pencil",
            salt,
            4096,
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".into(),
        );

        let server_first = server
            .handle_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO")
            .expect("client-first ok");
        assert_eq!(
            server_first,
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".to_vec()
        );

        let server_final = server
            .handle_client_final(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .expect("proof verifies");
        assert_eq!(
            server_final,
            b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".to_vec()
        );
    }

    #[test]
    fn wrong_password_proof_is_rejected() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server = ScramServer::new_with(
            "not-pencil",
            salt,
            4096,
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".into(),
        );
        server
            .handle_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO")
            .expect("client-first ok");
        let err = server
            .handle_client_final(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::INVALID_PASSWORD);
    }

    #[test]
    fn channel_binding_gs2_header_y_is_accepted() {
        // tokio-postgres over plaintext sends "y,," when the server doesn't
        // advertise -PLUS; c= must then be base64("y,,") = "eSws".
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server = ScramServer::new_with("pw", salt, 4096, "SNONCE".into());
        let first = server
            .handle_client_first(b"y,,n=user,r=CNONCE")
            .expect("ok");
        assert!(first.starts_with(b"r=CNONCESNONCE,"));
    }

    #[test]
    fn requested_channel_binding_without_plus_is_rejected() {
        let mut server = ScramServer::new_with("pw", vec![0; 16], 4096, "S".into());
        let err = server
            .handle_client_first(b"p=tls-server-end-point,,n=user,r=CNONCE")
            .expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }
}
