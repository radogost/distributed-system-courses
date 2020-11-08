//! Error for key value store
use std::error::Error;
use std::fmt::{Display, Formatter};

use serde_json;

/// Error type for key value store
#[derive(Debug)]
pub enum KvsError {
    /// IO Error
    IO(std::io::Error),
    NoSuchKey(String),
    /// Other Errors
    Other(String),
}

impl Display for KvsError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Some error occured")
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
        KvsError::Other(error.to_string())
    }
}

/// Result type for key value store
pub type Result<T> = std::result::Result<T, KvsError>;
