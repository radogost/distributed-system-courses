#![deny(missing_docs)]
//! Key/Value store
mod engine;
mod error;
mod store;

pub use engine::KvsEngine;
pub use error::Result;
pub use store::KvStore;
