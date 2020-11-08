#![deny(missing_docs)]
//! Key/Value store
mod error;
mod store;

pub use error::Result;
pub use store::KvStore;
