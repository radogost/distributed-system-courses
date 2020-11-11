use crate::error::Result;

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

/// Defines the storage interface called by KvsServer
pub trait KvsEngine {
    /// Set the value of a key
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the value of a key
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given key
    fn remove(&mut self, key: String) -> Result<()>;
}

pub(in crate::engines) fn write_engine_type(
    path: impl Into<PathBuf>,
    engine_type: &str,
) -> Result<()> {
    let path = path.into().join("engine");
    let mut f = File::create(path)?;
    f.write_all(engine_type.as_bytes())?;
    Ok(())
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
