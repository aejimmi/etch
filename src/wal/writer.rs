//! WalBackend — Backend<T> + IncrementalSave for WAL-based persistence.
//!
//! Load: read snapshot.postcard → replay wal.bin entries → return state.
//! Save: write full snapshot + reset WAL (used by write_durable and shutdown).
//! IncrementalSave: append ops to WAL buffer, fsync on demand, snapshot when threshold hit.

use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::diff::Diffable;
use super::format::WalFile;
use super::op::Op;
use crate::backend::Backend;
use crate::error::Result;

const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 1000;

/// WAL-based persistence backend.
///
/// Stores state as a postcard snapshot + append-only WAL of diffs.
/// Snapshots compact the WAL when entry count exceeds the threshold.
pub struct WalBackend<T: Diffable> {
    dir: PathBuf,
    wal: Mutex<WalFile>,
    entry_count: AtomicU64,
    snapshot_threshold: u64,
    _phantom: PhantomData<T>,
}

impl<T: Diffable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Open a WAL backend in the given directory.
    ///
    /// Creates the directory if needed.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let wal_path = dir.join("wal.bin");
        let wal = WalFile::open(&wal_path)?;

        // Count existing entries for snapshot threshold tracking.
        let entry_count = if wal_path.exists() {
            let (entries, _) = WalFile::iter_entries(&wal_path)?;
            entries.len() as u64
        } else {
            0
        };

        Ok(Self {
            dir,
            wal: Mutex::new(wal),
            entry_count: AtomicU64::new(entry_count),
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
            _phantom: PhantomData,
        })
    }

    /// Set the snapshot threshold (number of WAL entries before compaction).
    pub fn set_snapshot_threshold(&mut self, threshold: u64) {
        self.snapshot_threshold = threshold;
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join("snapshot.postcard")
    }

    fn wal_path(&self) -> PathBuf {
        self.dir.join("wal.bin")
    }

    /// Load state: snapshot + WAL replay.
    fn load_state(&self) -> Result<T> {
        let snap_path = self.snapshot_path();
        let mut state = if snap_path.exists() {
            let bytes = std::fs::read(&snap_path)?;
            if bytes.is_empty() {
                T::default()
            } else {
                postcard::from_bytes(&bytes)?
            }
        } else {
            T::default()
        };

        // Replay WAL.
        let wal_path = self.wal_path();
        if wal_path.exists() {
            let (entries, valid_offset) = WalFile::iter_entries(&wal_path)?;
            let file_len = std::fs::metadata(&wal_path)?.len();

            for ops in &entries {
                state.apply(ops)?;
            }

            // Truncate at corruption point if needed.
            if valid_offset < file_len {
                WalFile::truncate_at(&wal_path, valid_offset)?;
            }
        }

        Ok(state)
    }

    /// Write a full snapshot and reset WAL.
    fn write_snapshot(&self, state: &T) -> Result<()> {
        let snap_path = self.snapshot_path();
        let tmp = snap_path.with_extension("tmp");
        let bytes = postcard::to_allocvec(state)?;

        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }

        std::fs::rename(&tmp, &snap_path)?;

        // Fsync directory.
        if let Ok(dir) = std::fs::File::open(&self.dir) {
            let _ = dir.sync_all();
        }

        // Reset WAL.
        self.wal.lock().reset()?;
        self.entry_count.store(0, Ordering::Release);

        Ok(())
    }
}

impl<T: Diffable + Serialize + DeserializeOwned + Default> Backend<T> for WalBackend<T> {
    fn load(&self) -> Result<T> {
        self.load_state()
    }

    fn save(&self, state: &T) -> Result<()> {
        self.write_snapshot(state)
    }
}

/// Trait for incremental (WAL-based) saves from the Store.
pub trait IncrementalSave<T>: Send + Sync {
    /// Append ops to WAL buffer (no fsync).
    fn save_ops(&self, ops: &[Op]) -> Result<()>;
    /// Flush BufWriter + fsync WAL file.
    fn sync(&self) -> Result<()>;
    /// Check if WAL has exceeded the snapshot threshold.
    fn should_snapshot(&self) -> bool;
    /// Write a full snapshot and reset WAL.
    fn snapshot(&self, state: &T) -> Result<()>;
}

impl<T: Diffable + Serialize + DeserializeOwned + Default> IncrementalSave<T> for WalBackend<T> {
    fn save_ops(&self, ops: &[Op]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        self.wal.lock().append(ops)?;
        self.entry_count.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.wal.lock().sync()
    }

    fn should_snapshot(&self) -> bool {
        self.entry_count.load(Ordering::Acquire) >= self.snapshot_threshold
    }

    fn snapshot(&self, state: &T) -> Result<()> {
        self.write_snapshot(state)
    }
}
