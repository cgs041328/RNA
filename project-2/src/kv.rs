use crate::Result;
use failure::format_err;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;

///A key-value Store of String
///
/// Example:
///
/// ```rust
/// use kvs::KvStore;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
/// ```
pub struct KvStore {
    writer: BufWriter<File>,
    index: HashMap<String, String>,
    reader: BufReader<File>,
}

impl KvStore {
    ///Open a KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into().join("kvs.log");
        let path = path.as_path();
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(path)?;
        let writer = BufWriter::new(file);
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(KvStore {
            writer,
            reader,
            index: HashMap::new(),
        })
    }

    ///Set a key-value pair of String.
    ///
    /// If the key already exists, value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set { key, value };
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;
        Ok(())
    }
    ///Get the String value of a String key.
    ///
    /// Return NONE if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.build_index()?;
        Ok(self.index.get(&key).cloned())
    }

    ///Remove the given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(_) = self.get(key.clone())? {
            let command = Command::Remove { key };
            serde_json::to_writer(&mut self.writer, &command)?;
            self.writer.flush()?;
            Ok(())
        } else {
            Err(format_err!("Key not found"))
        }
    }

    fn build_index(&mut self) -> Result<()> {
        // self.index = HashMap::new();
        self.reader.seek(SeekFrom::Start(0))?;
        let stream = Deserializer::from_reader(&mut self.reader).into_iter::<Command>();
        for command in stream {
            match command? {
                Command::Set { key, value } => {
                    self.index.insert(key, value);
                }
                Command::Remove { key } => {
                    self.index.remove(&key);
                }
            }
        }
        Ok(())
    }
}

/// Struct representing a command
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
