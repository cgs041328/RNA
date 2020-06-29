use crate::engines::KvsEngine;
use crate::Result;
use crossbeam_skiplist::SkipMap;
use failure::format_err;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::sync::{Arc, Mutex};
use std::{
    cell::RefCell,
    collections::BTreeMap,
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
/// use kvs::{Result, KvStore, KvsEngine};
/// use std::env::current_dir;
/// fn try_main() -> Result<()> {
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
/// Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct KvStore {
    writer: Arc<Mutex<KvStoreWriter>>,
    index: Arc<SkipMap<String, CommandPosition>>,
    reader: KvStoreReader,
    path: Arc<PathBuf>,
}

impl KvStore {
    ///Open a KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = BTreeMap::new();
        let mut index = SkipMap::new();
        let gen_list = sort_gen_list(&path)?;
        let current_gen = gen_list.last().unwrap_or(&0) + 1;

        let mut uncompacted_size = 0;
        for gen in gen_list {
            let file = OpenOptions::new().read(true).open(log_path(&path, gen))?;
            let mut reader = BufReader::new(file);
            uncompacted_size += build_index(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }
        let writer = new_log_file(&path, current_gen)?;

        let index = Arc::new(index);
        let path = Arc::new(path.into());
        let reader = KvStoreReader {
            path: Arc::clone(&path),
            readers: RefCell::new(readers),
        };

        let writer = KvStoreWriter {
            reader: reader.clone(),
            writer,
            current_gen,
            uncompacted_size,
            path: Arc::clone(&path),
            index: Arc::clone(&index),
        };

        Ok(KvStore {
            path,
            reader,
            index,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

impl KvsEngine for KvStore {
    ///Set a key-value pair of String.
    ///
    /// If the key already exists, value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }
    ///Get the String value of a String key.
    ///
    /// Return NONE if the key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(format_err!("Invalid command"))
            }
        } else {
            Ok(None)
        }
    }

    ///Remove the given key.
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

fn new_log_file(path: &Path, gen: u64) -> Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(log_path(&path, gen))?;
    let writer = BufWriter::new(file);
    Ok(writer)
}

struct KvStoreReader {
    readers: RefCell<BTreeMap<u64, BufReader<File>>>,
    path: Arc<PathBuf>,
}

impl Clone for KvStoreReader {
    fn clone(&self) -> KvStoreReader {
        KvStoreReader {
            readers: RefCell::new(BTreeMap::new()),
            path: Arc::clone(&self.path),
        }
    }
}

impl KvStoreReader {
    fn close_stale_handle(&self, gen: u64) {
        let mut readers = self.readers.borrow_mut();
        if readers.contains_key(&gen) {
            readers.remove(&gen);
        }
    }

    /// Read the log file at the given `CommandPos`.
    fn read_and<F, R>(&self, cmd_pos: CommandPosition, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReader<File>>) -> Result<R>,
    {
        // self.close_stale_handles();

        let mut readers = self.readers.borrow_mut();
        // Open the file if we haven't opened it in this `KvStoreReader`.
        // We don't use entry API here because we want the errors to be propogated.
        if !readers.contains_key(&cmd_pos.gen) {
            let reader = BufReader::new(File::open(log_path(&self.path, cmd_pos.gen))?);
            readers.insert(cmd_pos.gen, reader);
        }
        let reader = readers.get_mut(&cmd_pos.gen).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.position))?;
        let cmd_reader = reader.take(cmd_pos.length);
        f(cmd_reader)
    }

    // Read the log file at the given `CommandPos` and deserialize it to `Command`.
    fn read_command(&self, cmd_pos: CommandPosition) -> Result<Command> {
        self.read_and(cmd_pos, |cmd_reader| {
            Ok(serde_json::from_reader(cmd_reader)?)
        })
    }
}

struct KvStoreWriter {
    reader: KvStoreReader,
    writer: BufWriter<File>,
    uncompacted_size: u64,
    current_gen: u64,
    path: Arc<PathBuf>,
    index: Arc<SkipMap<String, CommandPosition>>,
}

impl KvStoreWriter {
    fn compact(&mut self) -> Result<()> {
        let compact_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = new_log_file(&self.path, self.current_gen)?;

        let mut compact_writer = new_log_file(&self.path, compact_gen)?;
        let mut new_pos = 0;
        for cmd_pos in self.index.iter() {
            let len = self.reader.read_and(*cmd_pos.value(), |mut entry_reader| {
                Ok(io::copy(&mut entry_reader, &mut compact_writer)?)
            })?;
            self.index.insert(
                cmd_pos.key().clone(),
                CommandPosition {
                    length: (*cmd_pos.value()).length,
                    gen: compact_gen,
                    position: new_pos,
                },
            );
            new_pos += len;
        }
        compact_writer.flush()?;

        let stale_gens = sort_gen_list(&self.path)?
            .into_iter()
            .filter(|&gen| gen < compact_gen);

        for stale_gen in stale_gens {
            self.reader.close_stale_handle(stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }

        Ok(())
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
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
        if let Some(_old_cmd) = self.index.get(&key) {
            self.uncompacted_size += len;
        }
        self.index.insert(
            key,
            CommandPosition {
                length: len,
                position: before,
                gen: self.current_gen,
            },
        );

        if self.uncompacted_size > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.get(&key).is_some() {
            let command = Command::Remove { key: key.clone() };
            serde_json::to_writer(&mut self.writer, &command)?;
            self.writer.flush()?;
            let old_cmd = self.index.remove(&key).expect("key not found");
            self.uncompacted_size += old_cmd.value().length;
            Ok(())
        } else {
            Err(format_err!("Key not found"))
        }
    }
}

fn build_index(
    gen: u64,
    reader: &mut BufReader<File>,
    index: &mut SkipMap<String, CommandPosition>,
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
                if let Some(old_cmd) = index.get(&key) {
                    uncompacted_size += old_cmd.value().length;
                }
                index.insert(
                    key,
                    CommandPosition {
                        position: pos,
                        length,
                        gen,
                    },
                );
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

#[derive(Debug, Clone, Copy)]
struct CommandPosition {
    position: u64,
    length: u64,
    gen: u64,
}
