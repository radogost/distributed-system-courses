//! Error for key value store
use std::error::Error;
use std::fmt::{Display, Formatter};

use bincode;
use serde_json;
use sled;

/// Error type for key value store
#[derive(Debug)]
pub enum KvsError {
    /// IO Error
    IO(std::io::Error),
    /// Key does not exist
    NoSuchKey(String),
    /// Serde serialization error
    Serialize(String),
    /// Generic error
    Generic(String),
}

impl Display for KvsError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Error for KvsError {}

impl From<std::io::Error> for KvsError {
    fn from(error: std::io::Error) -> Self {
        KvsError::IO(error)
    }
}

impl From<serde_json::error::Error> for KvsError {
    fn from(error: serde_json::error::Error) -> Self {
        KvsError::Serialize(error.to_string())
    }
}

impl From<std::boxed::Box<bincode::ErrorKind>> for KvsError {
    fn from(error: std::boxed::Box<bincode::ErrorKind>) -> Self {
        KvsError::Serialize(error.as_ref().to_string())
    }
}

impl From<sled::Error> for KvsError {
    fn from(error: sled::Error) -> Self {
        KvsError::Generic(error.to_string())
    }
}

/// Result type for key value store
pub type Result<T> = std::result::Result<T, KvsError>;
