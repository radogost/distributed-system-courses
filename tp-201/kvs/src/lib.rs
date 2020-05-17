#![deny(missing_docs)]
//! A simple key-value store
use std::collections::HashMap;

///
/// Stores key-value pairs
///
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Creates a new KvStore
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Returns a value for the given key
    pub fn get(&mut self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// Sets the value for a given key
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Removes a key from the KvStore
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
