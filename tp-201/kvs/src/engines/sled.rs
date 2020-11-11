use super::write_engine_type;
use crate::error::{KvsError, Result};
use crate::KvsEngine;

use std::path::PathBuf;

use sled;

/// Sled backed kv store
pub struct SledKvsEngine {
    db: sled::Db,
}

impl SledKvsEngine {
    /// Creates a new sled backed kv store
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        write_engine_type(&path, "sled")?;
        let db = sled::open(path)?;
        Ok(Self { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self.db.get(key)?;
        self.db.flush()?;
        Ok(value.map(|iv| String::from_utf8(iv.as_ref().to_vec()).unwrap()))
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let value = sled::IVec::from(value.as_bytes());
        self.db.insert(key, value)?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let prev = self.db.remove(&key)?;
        self.db.flush()?;
        if let Some(_) = prev {
            Ok(())
        } else {
            Err(KvsError::NoSuchKey(key))
        }
    }
}
