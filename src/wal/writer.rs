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

use super::diff::{ReplayContext, ReplayFormat, Replayable};
use super::format::WalFile;
use super::migration::MigrationSet;
use super::op::Op;
use super::quarantine::Quarantine;
use super::snapshot::SnapshotPayload;
use crate::backend::Backend;
use crate::error::{Error, Result};

const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 1000;

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
    snapshot_threshold: u64,
    quarantine: Mutex<Quarantine>,
    _phantom: PhantomData<T>,
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
        match lock_file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Error::DatabaseLocked {
                    dir: dir.display().to_string(),
                });
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(Error::Io(e)),
        }
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
        // is not fatal — log and start fresh (the user may have edited it).
        let quarantine = match Quarantine::load(&dir) {
            Ok(q) => q,
            Err(e) => {
                eprintln!(
                    "etchdb: quarantine file unreadable ({}); starting with empty quarantine",
                    e
                );
                Quarantine::new()
            }
        };

        Ok(Self {
            dir,
            lock_file,
            wal: Mutex::new(wal),
            entry_count: AtomicU64::new(entry_count),
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
            quarantine: Mutex::new(quarantine),
            _phantom: PhantomData,
        })
    }

    /// Set the snapshot threshold (number of WAL entries before compaction).
    pub fn set_snapshot_threshold(&mut self, threshold: u64) {
        self.snapshot_threshold = threshold;
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

    /// Load state: snapshot + WAL replay.
    ///
    /// Quarantine is updated in-place on the backend's persistent store.
    /// New ops during replay that target quarantined keys correctly
    /// supersede old quarantine entries.
    fn load_state(&self) -> Result<T> {
        let migrations = T::migrations();

        // Lock the backend quarantine for the whole load. load_state is
        // only called once at startup (before anyone else holds a
        // reference), so this is contention-free in practice.
        let mut q_lock = self.quarantine.lock();

        let snap_path = self.snapshot_path();
        let mut state = if snap_path.exists() {
            let bytes = std::fs::read(&snap_path)?;
            if bytes.is_empty() {
                T::default()
            } else {
                match Self::decode_snapshot_into_state(&bytes, &migrations, &mut q_lock) {
                    Ok(s) => s,
                    Err(e) => {
                        let backup = self.dir.join("snapshot.backup");
                        let _ = std::fs::rename(&snap_path, &backup);
                        eprintln!(
                            "etchdb: snapshot unreadable ({}); preserved as {}; \
                             starting from default state and replaying WAL",
                            e,
                            backup.display()
                        );
                        T::default()
                    }
                }
            }
        } else {
            T::default()
        };

        // Replay wal.prev FIRST if present. This file is left behind by a
        // successful compaction (holds pre-compaction ops); we keep it
        // around until the next successful boot to protect against the
        // case where the new snapshot is corrupt or unreadable. Ops in
        // wal.prev that are also represented in the new snapshot are
        // harmless to re-apply (BTreeMap insert is idempotent).
        let wal_prev_path = self.wal_prev_path();
        let mut replayed_wal_prev = false;
        if wal_prev_path.exists() {
            match WalFile::iter_entries(&wal_prev_path) {
                Ok((prev_entries, _)) => {
                    let prev_format = match WalFile::version_of(&wal_prev_path) {
                        Ok(3) => ReplayFormat::LegacyPostcard,
                        _ => ReplayFormat::Versioned,
                    };
                    let mut prev_ctx = ReplayContext::new(prev_format, &migrations, &mut q_lock);
                    for ops in &prev_entries {
                        if let Err(e) = state.apply_with_ctx(ops, &mut prev_ctx) {
                            eprintln!("etchdb: skipped wal.prev entry during replay: {}", e);
                        }
                    }
                    replayed_wal_prev = true;
                }
                Err(e) => {
                    eprintln!("etchdb: wal.prev unreadable ({}), skipping", e);
                }
            }
        }

        // Replay current WAL.
        let wal_path = self.wal_path();
        let (entries, valid_offset) = WalFile::iter_entries(&wal_path)?;
        let file_len = std::fs::metadata(&wal_path)?.len();

        let wal_format = match WalFile::version_of(&wal_path) {
            Ok(3) => ReplayFormat::LegacyPostcard,
            _ => ReplayFormat::Versioned,
        };

        let mut ctx = ReplayContext::new(wal_format, &migrations, &mut q_lock);
        let mut entry_failures = 0u64;
        for ops in &entries {
            if let Err(e) = state.apply_with_ctx(ops, &mut ctx) {
                entry_failures += 1;
                eprintln!("etchdb: skipped WAL entry during replay: {}", e);
            }
        }
        if entry_failures > 0 {
            eprintln!(
                "etchdb: WAL replay: {} of {} entries failed. \
                 Individual op failures are logged per skip above.",
                entry_failures,
                entries.len()
            );
        }

        if valid_offset < file_len {
            WalFile::truncate_at(&wal_path, valid_offset)?;
        }

        // Once both WALs (and the snapshot) have been successfully replayed,
        // wal.prev has served its purpose and can be deleted. If we crash
        // before this point, wal.prev persists for the next boot.
        if replayed_wal_prev {
            let _ = std::fs::remove_file(&wal_prev_path);
        }

        // Auto-retry quarantine. If migrations are registered that can drain
        // existing quarantine entries, apply them now so the caller doesn't
        // have to explicitly call retry_quarantine. Recovered ops are written
        // to the WAL so they survive subsequent compactions.
        if !q_lock.is_empty() && migrations.is_nonempty() {
            let mut recovered_ops: Vec<Op> = Vec::new();
            let mut still_quarantined =
                Vec::<super::quarantine::QuarantinedEntry>::with_capacity(q_lock.len());

            for entry in q_lock.entries().iter().cloned() {
                let from = entry.version;
                let to = guess_current_version(&migrations, entry.collection, from);
                if from == to {
                    still_quarantined.push(entry);
                    continue;
                }
                match migrations.migrate_chain(entry.collection, from, to, &entry.value) {
                    super::migration::ChainResult::Migrated(new_bytes) => {
                        let mut env = Vec::with_capacity(2 + new_bytes.len());
                        env.extend_from_slice(&to.to_le_bytes());
                        env.extend_from_slice(&new_bytes);
                        recovered_ops.push(Op::Put {
                            collection: entry.collection,
                            key: entry.key.clone(),
                            value: env,
                        });
                    }
                    _ => still_quarantined.push(entry),
                }
            }

            if !recovered_ops.is_empty() {
                eprintln!(
                    "etchdb: auto-retry recovered {} quarantined entries with current migrations",
                    recovered_ops.len()
                );
                // Apply recovered ops to state.
                let mut scratch_q = super::quarantine::Quarantine::new();
                let mut ctx =
                    ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut scratch_q);
                state.apply_with_ctx(&recovered_ops, &mut ctx)?;

                // Persist recovered ops to WAL so they survive restart
                // without relying on quarantine.bin.
                self.wal.lock().append(&recovered_ops)?;
                self.wal.lock().sync()?;
                self.entry_count.fetch_add(1, Ordering::Release);

                // Replace quarantine contents with what's still stuck.
                q_lock.clear();
                for entry in still_quarantined {
                    q_lock.insert(entry);
                }
            }
        }

        // Persist any quarantine changes (additions, removals, retry drains).
        q_lock.save(&self.dir)?;

        // Prominent warning if quarantine has entries.
        if !q_lock.is_empty() {
            let mut by_reason: std::collections::BTreeMap<String, usize> =
                std::collections::BTreeMap::new();
            for entry in q_lock.entries() {
                let key = match &entry.reason {
                    super::quarantine::QuarantineReason::MissingMigration { from, to } => {
                        format!(
                            "MissingMigration collection {} {}->{}",
                            entry.collection, from, to
                        )
                    }
                    super::quarantine::QuarantineReason::MigrationFailed { from, to, .. } => {
                        format!(
                            "MigrationFailed collection {} {}->{}",
                            entry.collection, from, to
                        )
                    }
                    super::quarantine::QuarantineReason::MigrationPanicked { from, to, .. } => {
                        format!(
                            "MigrationPanicked collection {} {}->{}",
                            entry.collection, from, to
                        )
                    }
                    super::quarantine::QuarantineReason::FromFutureVersion { .. } => {
                        format!("FromFutureVersion collection {}", entry.collection)
                    }
                    super::quarantine::QuarantineReason::DecodeFailed { .. } => {
                        format!("DecodeFailed collection {}", entry.collection)
                    }
                };
                *by_reason.entry(key).or_insert(0) += 1;
            }
            eprintln!(
                "etchdb: quarantine contains {} entries across {} distinct failure modes. \
                 Call store.quarantined() to inspect, store.retry_quarantine() after \
                 registering migrations, or store.purge_quarantine() to drop.",
                q_lock.len(),
                by_reason.len()
            );
            for (reason, count) in &by_reason {
                eprintln!("  {count} × {reason}");
            }
        }

        drop(q_lock);

        state.after_load();
        Ok(state)
    }

    /// Decode a snapshot into state, dispatching by envelope version.
    ///
    /// v1/v2 (legacy postcard) → decode directly as T.
    /// v3/v4 (msgpack SnapshotPayload) → dispatch through T::from_snapshot
    /// with per-value migration and quarantine.
    fn decode_snapshot_into_state(
        bytes: &[u8],
        migrations: &MigrationSet,
        quarantine: &mut Quarantine,
    ) -> Result<T> {
        if bytes.len() < 5 || &bytes[..4] != SNAPSHOT_MAGIC {
            return Err(Error::invalid(
                "snapshot",
                "missing snapshot envelope (ESNA magic header); file may be corrupted",
            ));
        }

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
            SNAPSHOT_VERSION_MSGPACK => {
                let snap: SnapshotPayload =
                    rmp_serde::from_slice(payload).map_err(|e| Error::WalCorrupted {
                        offset: 0,
                        reason: format!("snapshot msgpack decode: {e}"),
                    })?;
                check_schema_drift::<T>(snap.schema_fingerprint, migrations);
                let mut ctx = ReplayContext::new(ReplayFormat::Versioned, migrations, quarantine);
                T::from_snapshot(snap, &mut ctx)
            }
            SNAPSHOT_VERSION_MSGPACK_ZSTD => {
                #[cfg(feature = "compression")]
                {
                    let decompressed = zstd::decode_all(payload)?;
                    let snap: SnapshotPayload =
                        rmp_serde::from_slice(&decompressed).map_err(|e| Error::WalCorrupted {
                            offset: 0,
                            reason: format!("snapshot msgpack decode: {e}"),
                        })?;
                    check_schema_drift::<T>(snap.schema_fingerprint, migrations);
                    let mut ctx =
                        ReplayContext::new(ReplayFormat::Versioned, migrations, quarantine);
                    T::from_snapshot(snap, &mut ctx)
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
                expected: SNAPSHOT_VERSION_MSGPACK,
            }),
        }
    }

    /// Write a full snapshot and reset WAL.
    ///
    /// Prefers the versioned (v3/v4) format if `T::to_snapshot` succeeds.
    /// Falls back to legacy postcard (v1/v2) format if T doesn't implement
    /// versioned snapshots (returns `SnapshotNotSupported`).
    fn write_snapshot(&self, state: &T) -> Result<()> {
        let snap_path = self.snapshot_path();
        let tmp = snap_path.with_extension("tmp");

        // Try new format first.
        let (version_byte, payload) = match state.to_snapshot() {
            Ok(snap) => {
                let encoded = rmp_serde::to_vec_named(&snap).map_err(|e| Error::WalCorrupted {
                    offset: 0,
                    reason: format!("snapshot msgpack encode: {e}"),
                })?;
                #[cfg(feature = "compression")]
                {
                    let compressed = zstd::encode_all(&encoded[..], 3)?;
                    (SNAPSHOT_VERSION_MSGPACK_ZSTD, compressed)
                }
                #[cfg(not(feature = "compression"))]
                {
                    (SNAPSHOT_VERSION_MSGPACK, encoded)
                }
            }
            Err(Error::SnapshotNotSupported) => {
                // Fall back to legacy postcard(T) for types that haven't
                // adopted the derive-macro-generated to_snapshot.
                let payload = postcard::to_allocvec(state)?;
                #[cfg(feature = "compression")]
                {
                    let compressed = zstd::encode_all(&payload[..], 3)?;
                    (SNAPSHOT_VERSION_ZSTD, compressed)
                }
                #[cfg(not(feature = "compression"))]
                {
                    (SNAPSHOT_VERSION_RAW, payload)
                }
            }
            Err(e) => return Err(e),
        };

        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(SNAPSHOT_MAGIC)?;
            f.write_all(&[version_byte])?;
            f.write_all(&payload)?;
            f.sync_all()?;
        }

        // Atomically rotate the WAL to wal.prev BEFORE committing the new
        // snapshot. Ordering: (1) new snapshot.tmp is fsync'd on disk;
        // (2) hold WAL lock, copy wal.bin → wal.prev, reset wal.bin;
        // (3) rename snapshot.tmp → snapshot.postcard; (4) fsync dir.
        //
        // Crash scenarios:
        // - After (2), before (3): old snapshot + wal.prev = full recovery.
        // - After (3), before (4): new snapshot + wal.prev = full recovery
        //   (wal.prev replay is idempotent against the new snapshot).
        // - After (4): wal.prev gets deleted on next successful boot.
        let wal_path = self.wal_path();
        let wal_prev_path = self.wal_prev_path();
        {
            let mut wal = self.wal.lock();
            wal.rotate_to_backup(&wal_path, &wal_prev_path)?;
        }
        self.entry_count.store(0, Ordering::Release);

        // Now commit the new snapshot. At this point wal.prev contains the
        // pre-compaction ops, so the snapshot-rename crash window is safe.
        std::fs::rename(&tmp, &snap_path)?;

        // Fsync directory to ensure rename and WAL reset are durable.
        #[cfg(unix)]
        {
            let dir = std::fs::File::open(&self.dir)?;
            dir.sync_all()?;
        }

        Ok(())
    }
}

/// Warn if the snapshot's fingerprint disagrees with the current binary.
///
/// A fingerprint mismatch means either:
/// - The developer bumped one or more collection versions (expected — a
///   migration should cover the difference), or
/// - The developer changed the struct shape without bumping the version
///   (unexpected — this is the "forgot to bump" bug).
///
/// We don't have enough information to tell these apart at runtime, so we
/// just log prominently. If no migrations are registered, the warning is
/// louder.
fn check_schema_drift<T: Replayable>(stored: u64, migrations: &MigrationSet) {
    let current = T::schema_fingerprint();
    if stored == 0 || current == 0 {
        return; // one side opted out of drift detection
    }
    if stored == current {
        return;
    }
    let has_migrations = migrations.is_nonempty();
    eprintln!(
        "etchdb: schema fingerprint changed since last snapshot \
         (stored={:x}, current={:x}){}",
        stored,
        current,
        if has_migrations {
            ". Migrations registered — expected if you bumped a version."
        } else {
            ". NO migrations registered. If you changed a struct without bumping its \
             version attribute, data may be silently dropped. Check store.quarantined() after load."
        }
    );
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
        self.entry_count.load(Ordering::Acquire) >= self.snapshot_threshold
    }

    fn snapshot(&self, state: &T) -> Result<()> {
        self.write_snapshot(state)
    }
}
