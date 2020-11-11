//! A simple key-value store
use super::write_engine_type;
use crate::engines::KvsEngine;
use crate::error::{KvsError, Result};

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader, BufWriter, Error, ErrorKind, SeekFrom};
use std::path::PathBuf;
use std::rc::Rc;

use lazy_static::lazy_static;

use regex::Regex;

use serde::{Deserialize, Serialize};
use serde_json;

lazy_static! {
    static ref RE: Regex = Regex::new(r"segment_(\d).log").unwrap();
}

fn get_filepath(root: &PathBuf, gen: usize) -> PathBuf {
    let mut path = PathBuf::new();
    path.push(root);
    path.push(format!("segment_{}.log", gen));
    path
}

///
/// Stores key-value pairs
///
pub struct KvStore {
    path: PathBuf,
    gen: usize,
    writer: CommandWriter,
    readers: HashMap<String, (Rc<RefCell<CommandReader>>, usize)>,
    current_gen_reader: Rc<RefCell<CommandReader>>,
    total_lines: usize,
}

#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl KvStore {
    /// Creates a new KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        if !path.exists() {
            std::fs::create_dir(&path)?;
        } else if !path.is_dir() {
            return Err(KvsError::IO(Error::new(
                ErrorKind::InvalidInput,
                "Expected path to be a directory!",
            )));
        }
        write_engine_type(&path, "kvs")?;

        Self::setup(path)
    }

    fn setup(path: PathBuf) -> Result<Self> {
        let generations = Self::old_generations(&path)?;
        let gen = Self::new_gen_number(&generations);
        let new_gen_path = get_filepath(&path, gen);
        let writer = CommandWriter::new(&new_gen_path)?;
        let current_gen_reader = Rc::new(RefCell::new(CommandReader::new(&new_gen_path)?));
        let (lines, readers) = Self::restore_state(generations)?;

        Ok(KvStore {
            path,
            gen,
            writer,
            readers,
            current_gen_reader,
            total_lines: lines,
        })
    }

    fn write_command(&mut self, command: Command) -> Result<usize> {
        self.total_lines += 1;
        Ok(self.writer.write_command(command)?)
    }

    fn compact(&mut self) -> Result<()> {
        if self.total_lines > 2 * self.readers.len() {
            let generations_to_delete = Self::old_generations(&self.path)?;
            let new_gen = self.gen + 1;
            let new_gen_path = get_filepath(&self.path, new_gen);
            let mut writer = CommandWriter::new(&new_gen_path)?;

            for (reader, offset) in self.readers.values() {
                let mut reader = reader.borrow_mut();
                let command = reader.read_command(*offset)?;
                if let Command::Set { key: _, value: _ } = &command {
                    writer.write_command(command)?;
                }
            }

            for (_, gen_to_delete) in generations_to_delete {
                fs::remove_file(gen_to_delete)?;
            }

            let KvStore {
                gen,
                writer,
                readers,
                current_gen_reader,
                total_lines,
                ..
            } = Self::setup(self.path.clone())?;

            self.gen = gen;
            self.writer = writer;
            self.readers = readers;
            self.current_gen_reader = current_gen_reader;
            self.total_lines = total_lines;

            Ok(())
        } else {
            Ok(())
        }
    }

    fn old_generations(path: &PathBuf) -> Result<Vec<(usize, PathBuf)>> {
        let mut generations = fs::read_dir(&path)?
            .flat_map(|file| file)
            .map(|file| file.path())
            .filter(|file| file.is_file())
            .flat_map(|file| {
                if let Some(filepath) = file.to_str() {
                    RE.captures(filepath)
                        .map(|digit_match| digit_match.get(1))
                        .flatten()
                        .map(|digit| digit.as_str())
                        .map(|digit| digit.parse::<usize>())
                        .map(|digit| digit.ok())
                        .flatten()
                } else {
                    None
                }
            })
            .map(|gen| (gen, get_filepath(path, gen)))
            .collect::<Vec<_>>();
        generations.sort_by(|(gen1, _), (gen2, _)| gen1.cmp(gen2));

        Ok(generations)
    }

    fn new_gen_number(generations: &[(usize, PathBuf)]) -> usize {
        generations
            .iter()
            .map(|(gen, _)| gen)
            .max()
            .map(|gen| *gen + 1)
            .unwrap_or(1)
    }

    fn restore_state(
        generations: Vec<(usize, PathBuf)>,
    ) -> Result<(usize, HashMap<String, (Rc<RefCell<CommandReader>>, usize)>)> {
        let mut command_readers = HashMap::new();

        let mut lines = 0;
        for (_, path) in generations {
            let mut reader = BufReader::new(File::open(&path)?);
            let command_reader = Rc::new(RefCell::new(CommandReader::new(&path)?));
            let mut offset = 0;

            let mut buffer = String::new();
            loop {
                match reader.read_line(&mut buffer) {
                    Ok(0) => break,
                    Ok(size) => {
                        let command = serde_json::from_str(&buffer)?;
                        match command {
                            Command::Set { key, value: _ } => {
                                command_readers.insert(key, (Rc::clone(&command_reader), offset));
                            }
                            Command::Remove { key } => {
                                command_readers.remove(&key);
                            }
                        };
                        offset += size;
                        lines += 1;
                        buffer.clear();
                    }
                    Err(e) => return Err(KvsError::IO(e)),
                }
            }
        }

        Ok((lines, command_readers))
    }
}

impl KvsEngine for KvStore {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let command = self
            .readers
            .get(&key)
            .map(|(reader, offset)| {
                let mut reader = reader.borrow_mut();
                reader.read_command(*offset)
            })
            .transpose()?;

        match command {
            Some(Command::Remove { key: _ }) => unreachable!(),
            Some(Command::Set { key: _, value }) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let offset = self.write_command(command)?;
        let reader = Rc::clone(&self.current_gen_reader);
        self.readers.insert(key, (reader, offset));
        self.compact()?;

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.readers.contains_key(&key) {
            let command = Command::Remove { key: key.clone() };
            self.write_command(command)?;
            self.readers.remove(&key);
            self.compact()?;
            Ok(())
        } else {
            Err(KvsError::NoSuchKey(key))
        }
    }
}

struct CommandReader {
    reader: BufReader<File>,
}

impl CommandReader {
    fn new(path: &PathBuf) -> Result<Self> {
        let f = File::open(path)?;
        let reader = BufReader::new(f);
        Ok(Self { reader })
    }

    fn read_command(&mut self, offset: usize) -> Result<Command> {
        let mut buffer = String::new();
        self.reader.seek(SeekFrom::Start(offset as u64))?;
        self.reader.read_line(&mut buffer)?;
        Ok(serde_json::from_str(&buffer)?)
    }
}

struct CommandWriter {
    writer: BufWriter<File>,
    offset: usize,
}

impl CommandWriter {
    fn new(path: &PathBuf) -> Result<Self> {
        let f = File::create(path)?;
        let writer = BufWriter::new(f);

        Ok(Self { writer, offset: 0 })
    }

    fn write_command(&mut self, command: Command) -> Result<usize> {
        let command = serde_json::to_string(&command)?;

        let offset = self.offset;
        self.offset += self.writer.write(command.as_bytes())?;
        self.offset += self.writer.write("\n".as_bytes())?;
        self.writer.flush()?;

        Ok(offset)
    }
}
