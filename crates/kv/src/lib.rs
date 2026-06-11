//! kv: ordered key-value storage with order-preserving key encoding and a
//! versioned row value encoding. The permanent storage seam for crabgresql.

pub mod error;
pub mod key;
pub mod keyenc;
pub mod rowenc;
pub mod store;

pub use error::KvError;
pub use store::{Kv, MemKv};
