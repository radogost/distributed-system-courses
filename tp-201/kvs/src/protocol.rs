//! Defines requests and responses between client and server
use serde::{Deserialize, Serialize};

/// Command which clients send to server
#[derive(Serialize, Deserialize)]
pub enum CommandRequest {
    /// Set value for a key
    Set {
        /// The key
        key: String,
        /// Value of the key
        value: String,
    },
    /// Remove key
    Remove {
        /// Key to remove
        key: String,
    },
    /// Get value for key
    Get {
        /// The key
        key: String,
    },
}

/// Response with which server replies to its client
/// after receiving a command
#[derive(Serialize, Deserialize)]
pub enum CommandResponse {
    /// Success if Remove/Set was successful
    Success(),
    /// Failure with reason why a CommandRequest failed
    Failure(String),
    /// Value if a Get request was sucessful
    Value(String),
}
