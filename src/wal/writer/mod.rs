//! WalBackend — `Backend<T>` + `IncrementalSave` for WAL-based persistence.
//!
//! Load: read snapshot.postcard → replay wal.bin entries → return state.
//! Save: write full snapshot + reset WAL (used by write_durable and shutdown).
//! IncrementalSave: append ops to WAL buffer, fsync on demand, snapshot when threshold hit.

mod checkpoint;
mod compact;
mod inspect;
mod load;
mod recover;
mod replay;

use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::diff::Replayable;
use super::format::WalFile;
use super::op::Op;
use super::quarantine::Quarantine;
use super::report::ReplayReport;
use crate::backend::Backend;
use crate::error::{Error, Result};
use crate::store::CheckpointReport;

const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 1000;

/// How strictly a load treats recoverable data loss during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMode {
    /// Skip + quarantine + report, then come up. The default — compatible
    /// with prior behavior, but now the loss is visible in the [`ReplayReport`].
    Lenient,
    /// Abort the load with a typed error on any skip, quarantine, or
    /// snapshot discard. See [`crate::Error::ReplayLoss`] and
    /// [`crate::Error::SchemaVersionMismatch`].
    Strict,
}

/// Snapshot format magic bytes.
const SNAPSHOT_MAGIC: &[u8; 4] = b"ESNA";

/// Snapshot version: raw postcard payload of T (legacy, < 0.4.0).
const SNAPSHOT_VERSION_RAW: u8 = 1;

/// Snapshot version: zstd-compressed postcard payload of T (legacy, < 0.4.0).
const SNAPSHOT_VERSION_ZSTD: u8 = 2;

/// Snapshot version: msgpack-named SnapshotPayload with per-value versioning
/// (current, >= 0.4.0).
const SNAPSHOT_VERSION_MSGPACK: u8 = 3;

/// Snapshot version: zstd-compressed msgpack-named SnapshotPayload.
const SNAPSHOT_VERSION_MSGPACK_ZSTD: u8 = 4;

/// WAL-based persistence backend.
///
/// Stores state as a postcard snapshot + append-only WAL of diffs.
/// Snapshots compact the WAL when entry count exceeds the threshold.
///
/// A `Quarantine` is kept in-memory for the lifetime of the backend.
/// Values that failed migration during replay live here until either the
/// user retries migration, purges them, or a normal write supersedes them.
///
/// An exclusive OS-level advisory lock (via `std::fs::File::try_lock`) is
/// held on `<dir>/.lock` for the lifetime of this backend. A second process
/// attempting to open the same directory will get `Error::DatabaseLocked`.
/// The lock releases automatically when the process exits (or the backend
/// is dropped).
///
/// # Compaction exclusion
///
/// Snapshot writing is serialized per backend by `compact_gate`. The
/// foreground write path, the grouped-flush background thread, an explicit
/// [`Backend::save`], and [`crate::Store::checkpoint_to`] all funnel through
/// it, so `snapshot.tmp` has exactly one writer and `wal.prev` is rotated by
/// exactly one thread at a time.
pub struct WalBackend<T: Replayable> {
    dir: PathBuf,
    /// Kept alive to hold the exclusive file lock. Dropping releases it.
    #[allow(dead_code)]
    lock_file: std::fs::File,
    wal: Mutex<WalFile>,
    /// Serializes snapshot writing (and the checkpoint copy window) for this
    /// backend. Never taken while the WAL mutex or the state lock is held —
    /// the acquisition order is always `write_gate` → `compact_gate` → state.
    compact_gate: Mutex<()>,
    entry_count: AtomicU64,
    /// WAL entries before a full snapshot is taken. Atomic so it can be
    /// retuned after construction through the `Arc<WalBackend>` the
    /// `Store` holds (the corpus tier raises it far above the default so
    /// a high-churn workload stops rewriting the whole snapshot every
    /// `DEFAULT_SNAPSHOT_THRESHOLD` writes).
    snapshot_threshold: AtomicU64,
    quarantine: Mutex<Quarantine>,
    /// Set at `open` when the on-disk quarantine file was unreadable, so the
    /// next load can fold the event into its [`ReplayReport`] instead of
    /// printing it. `None` when the quarantine loaded cleanly.
    quarantine_load_note: Option<String>,
    /// Test-only: set while a snapshot body is executing, so the exclusion
    /// itself can be asserted rather than described in prose.
    #[cfg(test)]
    snapshot_in_flight: std::sync::atomic::AtomicBool,
    /// Test-only: number of times a snapshot body was entered while another
    /// was already in flight. Must always be zero.
    #[cfg(test)]
    snapshot_overlaps: AtomicU64,
    /// Test-only: number of snapshot bodies executed (a caller that queued
    /// behind another compaction and then found the WAL under threshold does
    /// not increment this).
    #[cfg(test)]
    snapshot_writes: AtomicU64,
    _phantom: PhantomData<T>,
}

/// Test-only guard that marks a snapshot body as in flight and records any
/// overlap. Dropping clears the flag, so an early `?` return cannot leak it.
#[cfg(test)]
pub(super) struct SnapshotProbe<'a> {
    in_flight: &'a std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl<'a> SnapshotProbe<'a> {
    /// Mark a snapshot body as entered, counting an overlap if one was
    /// already in flight.
    pub(super) fn enter(
        in_flight: &'a std::sync::atomic::AtomicBool,
        overlaps: &AtomicU64,
        writes: &AtomicU64,
    ) -> Self {
        if in_flight.swap(true, Ordering::AcqRel) {
            overlaps.fetch_add(1, Ordering::Release);
        }
        writes.fetch_add(1, Ordering::Release);
        Self { in_flight }
    }
}

#[cfg(test)]
impl Drop for SnapshotProbe<'_> {
    fn drop(&mut self) {
        self.in_flight.store(false, Ordering::Release);
    }
}

/// Number of times [`acquire_exclusive_lock`] retries a `WouldBlock` before
/// giving up. A genuine cross-process holder keeps the lock for its whole
/// lifetime and still fails after the full budget; this only smooths the
/// brief window where the OS has not yet published a just-released `flock`
/// (observable when a store is dropped and the same directory is reopened
/// immediately under heavy scheduling load).
const LOCK_ACQUIRE_ATTEMPTS: u32 = 20;

/// Backoff between lock-acquisition attempts (20 × 5 ms = 100 ms worst case).
const LOCK_ACQUIRE_BACKOFF: std::time::Duration = std::time::Duration::from_millis(5);

/// Take the exclusive advisory lock, retrying a transient `WouldBlock` for a
/// bounded budget before reporting [`Error::DatabaseLocked`]. A real IO error
/// is returned immediately (no retry).
fn acquire_exclusive_lock(lock_file: &std::fs::File, dir: &Path) -> Result<()> {
    for attempt in 0..LOCK_ACQUIRE_ATTEMPTS {
        match lock_file.try_lock() {
            Ok(()) => return Ok(()),
            Err(std::fs::TryLockError::WouldBlock) => {
                if attempt + 1 == LOCK_ACQUIRE_ATTEMPTS {
                    return Err(Error::DatabaseLocked {
                        dir: dir.display().to_string(),
                    });
                }
                std::thread::sleep(LOCK_ACQUIRE_BACKOFF);
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(Error::Io(e)),
        }
    }
    Err(Error::DatabaseLocked {
        dir: dir.display().to_string(),
    })
}

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Open a WAL backend in the given directory.
    ///
    /// Creates the directory if needed. Acquires an exclusive OS-level
    /// advisory lock on `<dir>/.lock` — if another process already holds it,
    /// returns `Error::DatabaseLocked` without touching any other files.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Acquire the exclusive lock BEFORE touching any other files.
        // Released automatically on process exit or when `lock_file` drops.
        let lock_path = dir.join(".lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        acquire_exclusive_lock(&lock_file, &dir)?;
        // Record our PID in the lock file so `ps aux | grep` can find who
        // is holding the DB. Best-effort; failure to write is not fatal.
        let _ = std::io::Write::write_all(
            &mut &lock_file,
            format!("pid={}\n", std::process::id()).as_bytes(),
        );

        let wal_path = dir.join("wal.bin");
        let wal = WalFile::open(&wal_path)?;

        // Count existing entries for snapshot threshold tracking.
        // Note: WalFile::open always creates the file, so it exists here.
        let (entries, _) = WalFile::iter_entries(&wal_path)?;
        let entry_count = entries.len() as u64;

        // Load any existing quarantine file. A corrupted quarantine file
        // is not fatal — start fresh (the user may have edited it) and stash
        // a note so the next load reports it instead of printing.
        let (quarantine, quarantine_load_note) = match Quarantine::load(&dir) {
            Ok(q) => (q, None),
            Err(e) => (
                Quarantine::new(),
                Some(replay::quarantine_unreadable_note(&e)),
            ),
        };

        Ok(Self {
            dir,
            lock_file,
            wal: Mutex::new(wal),
            compact_gate: Mutex::new(()),
            entry_count: AtomicU64::new(entry_count),
            snapshot_threshold: AtomicU64::new(DEFAULT_SNAPSHOT_THRESHOLD),
            quarantine: Mutex::new(quarantine),
            quarantine_load_note,
            #[cfg(test)]
            snapshot_in_flight: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            snapshot_overlaps: AtomicU64::new(0),
            #[cfg(test)]
            snapshot_writes: AtomicU64::new(0),
            _phantom: PhantomData,
        })
    }

    /// Test-only: how many times a snapshot body was entered concurrently
    /// with another. The compaction exclusion makes this always zero.
    #[cfg(test)]
    pub(crate) fn snapshot_overlaps(&self) -> u64 {
        self.snapshot_overlaps.load(Ordering::Acquire)
    }

    /// Test-only: how many snapshot bodies actually ran.
    #[cfg(test)]
    pub(crate) fn snapshot_writes(&self) -> u64 {
        self.snapshot_writes.load(Ordering::Acquire)
    }

    /// Set the snapshot threshold (number of WAL entries before
    /// compaction). `&self` — atomic, so it can be retuned through the
    /// shared `Arc<WalBackend>` after the `Store` is built.
    pub fn set_snapshot_threshold(&self, threshold: u64) {
        self.snapshot_threshold.store(threshold, Ordering::Release);
    }

    /// Snapshot of the current quarantine (clones entries).
    pub fn quarantined(&self) -> Vec<super::quarantine::QuarantinedEntry> {
        self.quarantine.lock().entries().to_vec()
    }

    /// Purge all quarantined entries (explicit, never automatic).
    /// Persists an empty quarantine to disk so the cleanup survives restart.
    pub fn purge_quarantine(&self) -> Result<()> {
        let mut q = self.quarantine.lock();
        q.clear();
        q.save(&self.dir)
    }

    fn snapshot_path(&self) -> PathBuf {
        replay::snapshot_path(&self.dir)
    }

    fn wal_path(&self) -> PathBuf {
        replay::wal_path(&self.dir)
    }

    /// Backup WAL from the previous compaction. Holds every op that was live
    /// when the snapshot was taken, including any that landed after the
    /// snapshot's state was captured. It is only ever superseded by the
    /// *next* compaction, which rewrites it after committing a snapshot that
    /// contains its ops — never by a load.
    fn wal_prev_path(&self) -> PathBuf {
        replay::wal_prev_path(&self.dir)
    }

    /// Load state, returning the accompanying [`ReplayReport`]. Lenient:
    /// recoverable loss is recorded in the report, never raised. This is the
    /// surface [`crate::Store::open_wal`] / `open_wal_with_report` build on.
    pub fn load_with_report(&self) -> Result<(T, ReplayReport)> {
        self.load_reporting(LoadMode::Lenient)
    }

    /// Load state under [`LoadMode::Strict`]: any skip, quarantine, or
    /// snapshot discard aborts with a typed error. Underpins
    /// [`crate::Store::open_wal_strict`].
    pub fn load_strict(&self) -> Result<(T, ReplayReport)> {
        self.load_reporting(LoadMode::Strict)
    }
}

impl<T: Replayable + Serialize + DeserializeOwned + Default> Backend<T> for WalBackend<T> {
    fn load(&self) -> Result<T> {
        self.load_reporting(LoadMode::Lenient)
            .map(|(state, _)| state)
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

    /// Compact the WAL if it is still over threshold, returning whether a
    /// snapshot was written.
    ///
    /// Unlike a bare `should_snapshot()` + `snapshot()` pair, the threshold
    /// is re-checked *and* the state to snapshot is obtained from `state_fn`
    /// under whatever exclusion the implementation uses for compaction. That
    /// is what stops two compaction paths (a foreground `write_durable` and
    /// the grouped flusher) from interleaving their snapshot temp files and
    /// their `wal.prev` rotations.
    ///
    /// The default implementation performs the unsynchronized check-then-act
    /// and exists so out-of-tree implementations keep compiling.
    fn compact_if_needed(&self, state_fn: &mut dyn FnMut() -> Result<T>) -> Result<bool> {
        if !self.should_snapshot() {
            return Ok(false);
        }
        let state = state_fn()?;
        self.snapshot(&state)?;
        Ok(true)
    }

    /// Write a consistent copy of the backend's on-disk state into `dest`.
    ///
    /// Called by [`crate::Store::checkpoint_to`] with the store's write gate
    /// held and after a flush, so the implementation only has to exclude its
    /// own background compaction. The default implementation reports
    /// [`Error::CheckpointUnsupported`].
    fn checkpoint_into(
        &self,
        _dest: &Path,
        _state_fn: &mut dyn FnMut() -> Result<T>,
    ) -> Result<CheckpointReport> {
        Err(Error::CheckpointUnsupported)
    }
}

impl<T: Replayable + Serialize + DeserializeOwned + Default> IncrementalSave<T> for WalBackend<T> {
    fn save_ops(&self, ops: &[Op]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // New writes supersede any existing quarantine entry for the same
        // (collection, key). This mirrors the behavior of
        // `apply_op_versioned_with_ctx` during WAL replay.
        {
            let mut q = self.quarantine.lock();
            let before = q.len();
            for op in ops {
                q.remove_key(op.collection(), op.key());
            }
            // Persist the change immediately so a crash doesn't leave a
            // stale entry on disk that would be re-quarantined on the next
            // open's snapshot load.
            if q.len() != before {
                q.save(&self.dir)?;
            }
        }
        self.wal.lock().append(ops)?;
        self.entry_count.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.wal.lock().sync()
    }

    fn should_snapshot(&self) -> bool {
        self.entry_count.load(Ordering::Acquire) >= self.snapshot_threshold.load(Ordering::Acquire)
    }

    fn snapshot(&self, state: &T) -> Result<()> {
        self.write_snapshot(state)
    }

    fn compact_if_needed(&self, state_fn: &mut dyn FnMut() -> Result<T>) -> Result<bool> {
        self.compact_if_needed_locked(state_fn)
    }

    fn checkpoint_into(
        &self,
        dest: &Path,
        state_fn: &mut dyn FnMut() -> Result<T>,
    ) -> Result<CheckpointReport> {
        self.checkpoint(dest, state_fn)
    }
}

#[cfg(test)]
#[path = "../crash_test.rs"]
mod crash_test;

#[cfg(test)]
#[path = "../crash_ext_test.rs"]
mod crash_ext_test;
