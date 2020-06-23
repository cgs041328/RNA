use crate::engines::KvsEngine;
use crate::Result;
use failure::format_err;
use sled::Db;
use std::{
    fs,
    io::{Read, Write},
    path::Path,
};
///SledEngine
pub struct SledEngine(Db);

impl SledEngine {
    ///open SledEngine
    pub fn open(path: &Path) -> Result<SledEngine> {
        let engine_path = path.join("type");
        let mut engine_type_file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&engine_path)?;
        let mut engine_type = String::new();
        engine_type_file.read_to_string(&mut engine_type)?;
        if engine_type.is_empty() {
            engine_type_file.write(b"sled")?;
            engine_type_file.flush()?;
        } else if engine_type != String::from("sled") {
            return Err(format_err!("Wrong engine"));
        }
        let db = sled::open(path)?;
        Ok(SledEngine(db))
    }
}

impl KvsEngine for SledEngine {
    ///Set a key-value pair of String.
    ///
    /// If the key already exists, value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.0.insert(key, value.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }
    ///Get the String value of a String key.
    ///
    /// Return NONE if the key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self
            .0
            .get(key)?
            .map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    ///Remove the given key.
    fn remove(&mut self, key: String) -> Result<()> {
        self.0.remove(key)?.ok_or(format_err!("key not found"))?;
        self.0.flush()?;
        Ok(())
    }
}
