//! Durable storage for the state Raft requires to survive crashes:
//! Layout:
//! meta.json -> term, voted_for, replaced atomically
//! log.jsonl
//!
//! Crash in the middle can leave a partial last line

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::types::{Entry, NodeId};

const META_FILE: &str = "meta.json";
const LOG_FILE: &str = "log.jsonl";

// State loaded from disk at startup
#[derive(Debug, Default)]
pub struct PersistentState {
    pub term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<Entry>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Meta {
    term: u64,
    voted_for: Option<NodeId>,
}

/// Handle to a node's data directory.
#[derive(Debug)]
pub struct Storage {
    dir: PathBuf,
    log_file: File,
}

impl Storage {
    pub fn open(dir: &Path) -> io::Result<(Self, PersistentState)> {
        fs::create_dir_all(dir)?;

        let meta: Meta = match fs::read(dir.join(META_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(invalid_data)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Meta::default(),
            Err(e) => return Err(e),
        };

        let (log, torn_tail) = read_log(&dir.join(LOG_FILE))?;
        let mut storage = Self {
            dir: dir.to_path_buf(),
            log_file: open_for_append(&dir.join(LOG_FILE))?,
        };
        if torn_tail {
            storage.rewrite_log(&log)?;
        }

        Ok((
            storage,
            PersistentState {
                term: meta.term,
                voted_for: meta.voted_for,
                log,
            },
        ))
    }

    //Record current term and vote
    pub fn save_meta(&mut self, term: u64, voted_for: Option<NodeId>) -> io::Result<()> {
        let bytes = serde_json::to_vec(&Meta { term, voted_for }).map_err(invalid_data)?;
        write_atomically(&self.dir, META_FILE, &bytes)
    }

    // append to end of log
    pub fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let bytes = encode_entries(entries)?;
        self.log_file.write_all(&bytes)?;
        self.log_file.sync_data()
    }

    /// Durably replace the whole log (used after truncation).
    pub fn rewrite_log(&mut self, entries: &[Entry]) -> io::Result<()> {
        let bytes = encode_entries(entries)?;
        write_atomically(&self.dir, LOG_FILE, &bytes)?;
        self.log_file = open_for_append(&self.dir.join(LOG_FILE))?;
        Ok(())
    }
}

/// Returns the complete entries and whether a torn (partial) last line was found.
fn read_log(path: &Path) -> io::Result<(Vec<Entry>, bool)> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), false)),
        Err(e) => return Err(e),
    };

    let mut entries = Vec::new();
    let mut segments = bytes.split(|&b| b == b'\n').peekable();
    let mut torn_tail = false;
    while let Some(segment) = segments.next() {
        let is_last = segments.peek().is_none();
        if is_last {
            // Text after the final newline: empty for a clean file,
            // otherwise a write that was interrupted by a crash.
            torn_tail = !segment.is_empty();
            break;
        }
        entries.push(serde_json::from_slice(segment).map_err(invalid_data)?);
    }
    Ok((entries, torn_tail))
}

fn encode_entries(entries: &[Entry]) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for entry in entries {
        serde_json::to_writer(&mut bytes, entry).map_err(invalid_data)?;
        bytes.push(b'\n');
    }
    Ok(bytes)
}

fn open_for_append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn write_atomically(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

fn invalid_data(e: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("raftkv-storage-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn set(term: u64, key: &str) -> Entry {
        Entry {
            term,
            command: Command::Set {
                key: key.into(),
                value: "v".into(),
            },
        }
    }

    #[test]
    fn empty_directory_starts_fresh() {
        let dir = TempDir::new("fresh");
        let (_, state) = Storage::open(&dir.0).expect("open");
        assert_eq!(state.term, 0);
        assert_eq!(state.voted_for, None);
        assert!(state.log.is_empty());
    }

    #[test]
    fn meta_and_log_survive_reopen() {
        let dir = TempDir::new("reopen");
        {
            let (mut storage, _) = Storage::open(&dir.0).expect("open");
            storage.save_meta(3, Some(2)).expect("meta");
            storage.append(&[set(1, "a"), set(2, "b")]).expect("append");
            storage.append(&[set(3, "c")]).expect("append");
        }
        let (_, state) = Storage::open(&dir.0).expect("reopen");
        assert_eq!(state.term, 3);
        assert_eq!(state.voted_for, Some(2));
        assert_eq!(state.log, vec![set(1, "a"), set(2, "b"), set(3, "c")]);
    }

    #[test]
    fn rewrite_replaces_the_log() {
        let dir = TempDir::new("rewrite");
        {
            let (mut storage, _) = Storage::open(&dir.0).expect("open");
            storage.append(&[set(1, "a"), set(1, "b")]).expect("append");
            storage.rewrite_log(&[set(1, "a")]).expect("rewrite");
            storage
                .append(&[set(2, "z")])
                .expect("append after rewrite");
        }
        let (_, state) = Storage::open(&dir.0).expect("reopen");
        assert_eq!(state.log, vec![set(1, "a"), set(2, "z")]);
    }

    #[test]
    fn torn_last_line_is_dropped_and_repaired() {
        let dir = TempDir::new("torn");
        {
            let (mut storage, _) = Storage::open(&dir.0).expect("open");
            storage.append(&[set(1, "a")]).expect("append");
        }
        // Simulate a crash halfway through writing the second entry.
        let mut file = open_for_append(&dir.0.join(LOG_FILE)).expect("open log");
        file.write_all(br#"{"term":1,"comm"#)
            .expect("partial write");
        drop(file);

        let (mut storage, state) = Storage::open(&dir.0).expect("reopen");
        assert_eq!(state.log, vec![set(1, "a")]);
        storage.append(&[set(1, "b")]).expect("append after repair");
        drop(storage);

        let (_, state) = Storage::open(&dir.0).expect("reopen again");
        assert_eq!(state.log, vec![set(1, "a"), set(1, "b")]);
    }

    #[test]
    fn corrupt_complete_line_is_an_error() {
        let dir = TempDir::new("corrupt");
        fs::create_dir_all(&dir.0).expect("mkdir");
        fs::write(dir.0.join(LOG_FILE), b"not json\n").expect("write");
        let err = Storage::open(&dir.0).expect_err("must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
