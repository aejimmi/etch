//! WalBackend — Backend<T> + IncrementalSave for WAL-based persistence.
//!
//! Load: read snapshot.postcard → replay wal.bin entries → return state.
//! Save: write full snapshot + reset WAL (used by write_durable and shutdown).
//! IncrementalSave: append ops to WAL buffer, fsync on demand, snapshot when threshold hit.

use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::diff::Replayable;
use super::format::WalFile;
use super::op::Op;
use crate::backend::Backend;
use crate::error::{Error, Result};

const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 1000;

/// Snapshot format magic bytes.
const SNAPSHOT_MAGIC: &[u8; 4] = b"ESNA";

/// Snapshot version: raw postcard payload.
const SNAPSHOT_VERSION_RAW: u8 = 1;

/// Snapshot version: zstd-compressed postcard payload.
const SNAPSHOT_VERSION_ZSTD: u8 = 2;

/// WAL-based persistence backend.
///
/// Stores state as a postcard snapshot + append-only WAL of diffs.
/// Snapshots compact the WAL when entry count exceeds the threshold.
pub struct WalBackend<T: Replayable> {
    dir: PathBuf,
    wal: Mutex<WalFile>,
    entry_count: AtomicU64,
    snapshot_threshold: u64,
    _phantom: PhantomData<T>,
}

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Open a WAL backend in the given directory.
    ///
    /// Creates the directory if needed.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let wal_path = dir.join("wal.bin");
        let wal = WalFile::open(&wal_path)?;

        // Count existing entries for snapshot threshold tracking.
        // Note: WalFile::open always creates the file, so it exists here.
        let (entries, _) = WalFile::iter_entries(&wal_path)?;
        let entry_count = entries.len() as u64;

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
                Self::decode_snapshot(&bytes)?
            }
        } else {
            T::default()
        };

        // Replay WAL.
        // Note: open() always creates the WAL file, so it exists here.
        let wal_path = self.wal_path();
        let (entries, valid_offset) = WalFile::iter_entries(&wal_path)?;
        let file_len = std::fs::metadata(&wal_path)?.len();

        for ops in &entries {
            state.apply(ops)?;
        }

        // Truncate at corruption point if needed.
        if valid_offset < file_len {
            WalFile::truncate_at(&wal_path, valid_offset)?;
        }

        state.after_load();
        Ok(state)
    }

    /// Decode a snapshot, detecting envelope version and optional compression.
    fn decode_snapshot(bytes: &[u8]) -> Result<T> {
        if bytes.len() >= 5 && &bytes[..4] == SNAPSHOT_MAGIC {
            let version = bytes[4];
            let payload = &bytes[5..];

            match version {
                SNAPSHOT_VERSION_RAW => Ok(postcard::from_bytes(payload)?),
                SNAPSHOT_VERSION_ZSTD => {
                    #[cfg(feature = "compression")]
                    {
                        let decompressed = zstd::decode_all(payload)?;
                        Ok(postcard::from_bytes(&decompressed)?)
                    }
                    #[cfg(not(feature = "compression"))]
                    {
                        Err(Error::invalid(
                            "snapshot",
                            "snapshot was written with zstd compression; enable the `compression` feature to read it",
                        ))
                    }
                }
                _ => Err(Error::SnapshotVersion {
                    version,
                    expected: SNAPSHOT_VERSION_RAW,
                }),
            }
        } else {
            Err(Error::invalid(
                "snapshot",
                "missing snapshot envelope (ESNA magic header); file may be corrupted",
            ))
        }
    }

    /// Write a full snapshot and reset WAL.
    fn write_snapshot(&self, state: &T) -> Result<()> {
        let snap_path = self.snapshot_path();
        let tmp = snap_path.with_extension("tmp");
        let payload = postcard::to_allocvec(state)?;

        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(SNAPSHOT_MAGIC)?;

            #[cfg(feature = "compression")]
            {
                f.write_all(&[SNAPSHOT_VERSION_ZSTD])?;
                let compressed = zstd::encode_all(&payload[..], 3)?;
                f.write_all(&compressed)?;
            }

            #[cfg(not(feature = "compression"))]
            {
                f.write_all(&[SNAPSHOT_VERSION_RAW])?;
                f.write_all(&payload)?;
            }

            f.sync_all()?;
        }

        std::fs::rename(&tmp, &snap_path)?;

        // Reset WAL before directory fsync so one fsync covers both
        // the snapshot rename and WAL truncation.
        self.wal.lock().reset()?;
        self.entry_count.store(0, Ordering::Release);

        // Fsync directory to ensure rename and WAL reset are durable.
        #[cfg(unix)]
        {
            let dir = std::fs::File::open(&self.dir)?;
            dir.sync_all()?;
        }

        Ok(())
    }
}

impl<T: Replayable + Serialize + DeserializeOwned + Default> Backend<T> for WalBackend<T> {
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

impl<T: Replayable + Serialize + DeserializeOwned + Default> IncrementalSave<T> for WalBackend<T> {
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
