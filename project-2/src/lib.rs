#![deny(missing_docs)]

//! A key-value store.

use std::collections::HashMap;

///A key-value Store of String
///
/// Example:
///
/// ```rust
/// use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
/// ```
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    ///Create a KvStore
    pub fn new() -> KvStore {
        KvStore {
            store: HashMap::new(),
        }
    }

    ///Set a key-value pair of String.
    ///
    /// If the key already exists, value will be overwritten.
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }
    ///Get the String value of a String key.
    ///
    /// Return NONE if the key does not exist.
    pub fn get(&mut self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    ///Remove the given key.
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
