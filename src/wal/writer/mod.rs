//! WalBackend — `Backend<T>` + `IncrementalSave` for WAL-based persistence.
//!
//! Load: read snapshot.postcard → replay wal.bin entries → return state.
//! Save: write full snapshot + reset WAL (used by write_durable and shutdown).
//! IncrementalSave: append ops to WAL buffer, fsync on demand, snapshot when threshold hit.

mod compact;
mod load;

use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::diff::Replayable;
use super::format::WalFile;
use super::migration::MigrationSet;
use super::op::Op;
use super::quarantine::Quarantine;
use super::report::ReplayReport;
use crate::backend::Backend;
use crate::error::{Error, Result};

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
pub struct WalBackend<T: Replayable> {
    dir: PathBuf,
    /// Kept alive to hold the exclusive file lock. Dropping releases it.
    #[allow(dead_code)]
    lock_file: std::fs::File,
    wal: Mutex<WalFile>,
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
    _phantom: PhantomData<T>,
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
                Some(format!(
                    "quarantine file unreadable ({e}); started with an empty quarantine"
                )),
            ),
        };

        Ok(Self {
            dir,
            lock_file,
            wal: Mutex::new(wal),
            entry_count: AtomicU64::new(entry_count),
            snapshot_threshold: AtomicU64::new(DEFAULT_SNAPSHOT_THRESHOLD),
            quarantine: Mutex::new(quarantine),
            quarantine_load_note,
            _phantom: PhantomData,
        })
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

    /// Retry migration for all quarantined entries using the current
    /// migration registry. Entries that migrate successfully are returned
    /// so the caller can merge them into live state. Entries that still
    /// fail remain in quarantine with updated reason.
    ///
    /// The returned `Vec<Op>` is a set of synthetic Put ops — one per
    /// recovered entry, encoded in current-version format. Callers can
    /// feed these to `Store::apply_ops` to merge into state.
    pub fn retry_quarantine(&self) -> Result<Vec<Op>> {
        let migrations = T::migrations();
        let mut q = self.quarantine.lock();

        let mut recovered = Vec::new();
        let mut still_quarantined =
            Vec::<super::quarantine::QuarantinedEntry>::with_capacity(q.len());

        for entry in q.entries().iter().cloned() {
            // Reconstruct the versioned envelope the decoder expects.
            let mut envelope = Vec::with_capacity(2 + entry.value.len());
            envelope.extend_from_slice(&entry.version.to_le_bytes());
            envelope.extend_from_slice(&entry.value);

            // We don't know the collection's V type at this layer, so we
            // can only attempt to run the migration chain. The result is
            // still msgpack bytes. A successful migration means the caller
            // should be able to apply the resulting Op through the normal
            // path on its next replay.
            let from = entry.version;
            let to = guess_current_version(&migrations, entry.collection, from);

            if from == to || to == 0 {
                // No forward path registered; keep the entry.
                still_quarantined.push(entry);
                continue;
            }

            match migrations.migrate_chain(entry.collection, from, to, &entry.value) {
                super::migration::ChainResult::Migrated(new_bytes) => {
                    let mut new_env = Vec::with_capacity(2 + new_bytes.len());
                    new_env.extend_from_slice(&to.to_le_bytes());
                    new_env.extend_from_slice(&new_bytes);
                    recovered.push(Op::Put {
                        collection: entry.collection,
                        key: entry.key.clone(),
                        value: new_env,
                    });
                }
                _ => {
                    still_quarantined.push(entry);
                }
            }
        }

        q.clear();
        for entry in still_quarantined {
            q.insert(entry);
        }
        q.save(&self.dir)?;
        Ok(recovered)
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join("snapshot.postcard")
    }

    fn wal_path(&self) -> PathBuf {
        self.dir.join("wal.bin")
    }

    /// Backup WAL from the previous compaction. Preserves pre-compaction
    /// ops until we confirm the new snapshot is loadable on the next boot.
    fn wal_prev_path(&self) -> PathBuf {
        self.dir.join("wal.prev")
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

/// Find the highest target version reachable in a chain from `from` for
/// `collection`. Returns `from` if no forward migration exists.
fn guess_current_version(migrations: &MigrationSet, collection: u8, from: u16) -> u16 {
    let mut v = from;
    // Walk forward as long as a hop exists. Bounded by u16 range; in
    // practice chains are short.
    while migrations.has(collection, v) {
        v = match v.checked_add(1) {
            Some(next) => next,
            None => break,
        };
    }
    v
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
}

#[cfg(test)]
#[path = "../crash_test.rs"]
mod crash_test;

#[cfg(test)]
#[path = "../crash_ext_test.rs"]
mod crash_ext_test;
