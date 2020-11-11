#![deny(missing_docs)]
//! Key/Value store
mod client;
mod engines;
mod error;
pub mod protocol;
mod server;

pub use client::Client;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use server::Server;
