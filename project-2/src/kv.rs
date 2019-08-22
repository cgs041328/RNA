use crate::Result;
use failure::format_err;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

///A key-value Store of String
///
/// Example:
///
/// ```rust
/// use kvs::{Result, KvStore};
/// use std::env::current_dir;
/// fn try_main() -> Result<()> {
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
/// Ok(())
/// }
/// ```
pub struct KvStore {
    writer: BufWriter<File>,
    index: HashMap<String, CommandPosition>,
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
        let mut store = KvStore {
            writer,
            reader,
            index: HashMap::new(),
        };
        store.build_index()?;
        Ok(store)
    }

    ///Set a key-value pair of String.
    ///
    /// If the key already exists, value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        self.writer.seek(SeekFrom::End(0))?;
        let before = self.writer.stream_position()?;
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;
        let after = self.writer.stream_position()?;
        let len = after - before;
        self.index.insert(
            key,
            CommandPosition {
                length: len,
                position: before,
            },
        );
        Ok(())
    }
    ///Get the String value of a String key.
    ///
    /// Return NONE if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            self.reader.seek(SeekFrom::Start(cmd_pos.position))?;
            let cmd_reader = self.reader.get_mut().take(cmd_pos.length);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)?{
                Ok(Some(value.to_owned()))
            }
            else{
                Err(format_err!("Invalid command"))
            }
        } else {
            Ok(None)
        }
    }

    ///Remove the given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.get(&key).is_some() {
            let command = Command::Remove { key: key.clone() };
            serde_json::to_writer(&mut self.writer, &command)?;
            self.writer.flush()?;
            self.index.remove(&key);
            Ok(())
        } else {
            Err(format_err!("Key not found"))
        }
    }

    fn build_index(&mut self) -> Result<()> {
        // self.index = HashMap::new();
        self.reader.seek(SeekFrom::Start(0))?;
        let mut pos = self.reader.stream_position()?;
        let mut stream = Deserializer::from_reader(&mut self.reader).into_iter::<Command>();

        while let Some(command) = stream.next() {
            let curr_pos = stream.byte_offset() as u64;
            match command? {
                Command::Set { key, value: _ } => {
                    self.index.insert(
                        key,
                        CommandPosition {
                            position: pos,
                            length: curr_pos - pos,
                        },
                    );
                }
                Command::Remove { key } => {
                    self.index.remove(&key);
                }
            }
            pos = curr_pos;
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

struct CommandPosition {
    position: u64,
    length: u64,
}
