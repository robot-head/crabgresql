//! Errors from decoding stored bytes. Our own writes never produce these, but
//! decoders must fail rather than panic on corrupt or truncated input.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KvError {
    #[error("corrupt row encoding: {0}")]
    CorruptRow(String),
}
