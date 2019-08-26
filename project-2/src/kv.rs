use crate::Result;
use failure::format_err;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    collections::HashMap,
    ffi,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

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
    readers: HashMap<u64, BufReader<File>>,
    uncompacted_size: u64,
    current_gen: u64,
    path: PathBuf,
}

impl KvStore {
    ///Open a KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = HashMap::new();
        let gen_list = sort_gen_list(&path)?;
        let current_gen = gen_list.last().unwrap_or(&0) + 1;

        let mut uncompacted_size = 0;
        for gen in gen_list {
            let file = OpenOptions::new().read(true).open(log_path(&path, gen))?;
            let mut reader = BufReader::new(file);
            uncompacted_size += build_index(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }
        let writer = new_log_file(&path, current_gen, &mut readers)?;

        let store = KvStore {
            writer,
            readers,
            index,
            path,
            uncompacted_size,
            current_gen,
        };
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
        if let Some(_) = self.index.insert(
            key,
            CommandPosition {
                length: len,
                position: before,
                gen: self.current_gen,
            },
        ) {
            self.uncompacted_size += len;
        };

        if self.uncompacted_size > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }
    ///Get the String value of a String key.
    ///
    /// Return NONE if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Invalid command position");
            reader.seek(SeekFrom::Start(cmd_pos.position))?;
            let cmd_reader = reader.take(cmd_pos.length);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value.to_owned()))
            } else {
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
            let old_cmd = self.index.remove(&key).expect("key not found");
            self.uncompacted_size += old_cmd.length;
            Ok(())
        } else {
            Err(format_err!("Key not found"))
        }
    }
    fn compact(&mut self) -> Result<()> {
        let compact_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = new_log_file(&self.path, self.current_gen, &mut self.readers)?;

        let mut compact_writer = new_log_file(&self.path, compact_gen, &mut self.readers)?;
        let mut new_pos = 0;
        for cmd_pos in self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Invalid command position");
            reader.seek(SeekFrom::Start(cmd_pos.position))?;
            let mut cmd_reader = reader.take(cmd_pos.length);
            io::copy(&mut cmd_reader, &mut compact_writer)?;
            *cmd_pos = CommandPosition {
                length: cmd_pos.length,
                gen: compact_gen,
                position: new_pos,
            };
            new_pos += cmd_pos.length;
        }
        compact_writer.flush()?;

        let stale_gens: Vec<u64> = self
            .readers
            .keys()
            .filter(|&&gen| gen < compact_gen)
            .cloned()
            .collect();

        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }

        Ok(())
    }
}

fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReader<File>>,
) -> Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(log_path(&path, gen))?;
    let writer = BufWriter::new(file);
    let current_file = OpenOptions::new().read(true).open(log_path(&path, gen))?;
    let current_reader = BufReader::new(current_file);
    readers.insert(gen, current_reader);
    Ok(writer)
}

fn build_index(
    gen: u64,
    reader: &mut BufReader<File>,
    index: &mut HashMap<String, CommandPosition>,
) -> Result<u64> {
    reader.seek(SeekFrom::Start(0))?;
    let mut pos = reader.stream_position()?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted_size = 0;

    while let Some(command) = stream.next() {
        let curr_pos = stream.byte_offset() as u64;
        let length = curr_pos - pos;
        match command? {
            Command::Set { key, value: _ } => {
                if let Some(_) = index.insert(
                    key,
                    CommandPosition {
                        position: pos,
                        length,
                        gen,
                    },
                ) {
                    uncompacted_size += length;
                };
            }
            Command::Remove { key } => {
                if let Some(_) = index.remove(&key) {
                    uncompacted_size += length
                };
            }
        }
        pos = curr_pos;
    }
    Ok(uncompacted_size)
}
fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn sort_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(path)?
        .filter_map(|result| result.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(ffi::OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
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
    gen: u64,
}
