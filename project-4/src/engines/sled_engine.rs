use crate::engines::KvsEngine;
use crate::Result;
use failure::format_err;
use sled::Db;
use std::path::Path;
///SledEngine
#[derive(Clone)]
pub struct SledEngine(Db);

impl SledEngine {
    ///open SledEngine
    pub fn open(path: &Path) -> Result<SledEngine> {
        let db = sled::open(path)?;
        Ok(SledEngine(db))
    }
}

impl KvsEngine for SledEngine {
    ///Set a key-value pair of String.
    ///
    /// If the key already exists, value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.0.insert(key, value.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }
    ///Get the String value of a String key.
    ///
    /// Return NONE if the key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self
            .0
            .get(key)?
            .map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    ///Remove the given key.
    fn remove(&self, key: String) -> Result<()> {
        self.0.remove(key)?.ok_or(format_err!("key not found"))?;
        self.0.flush()?;
        Ok(())
    }
}
