//! Snapshot writing and WAL compaction for [`WalBackend`].

use serde::{Serialize, de::DeserializeOwned};
use std::io::Write;
use std::sync::atomic::Ordering;

use super::WalBackend;
use crate::error::{Error, Result};
use crate::wal::diff::Replayable;

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Write a full snapshot and reset WAL.
    ///
    /// Prefers the versioned (v3/v4) format if `T::to_snapshot` succeeds.
    /// Falls back to legacy postcard (v1/v2) format if T doesn't implement
    /// versioned snapshots (returns `SnapshotNotSupported`).
    pub(super) fn write_snapshot(&self, state: &T) -> Result<()> {
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
                    (super::SNAPSHOT_VERSION_MSGPACK_ZSTD, compressed)
                }
                #[cfg(not(feature = "compression"))]
                {
                    (super::SNAPSHOT_VERSION_MSGPACK, encoded)
                }
            }
            Err(Error::SnapshotNotSupported) => {
                // Fall back to legacy postcard(T) for types that haven't
                // adopted the derive-macro-generated to_snapshot.
                let payload = postcard::to_allocvec(state)?;
                #[cfg(feature = "compression")]
                {
                    let compressed = zstd::encode_all(&payload[..], 3)?;
                    (super::SNAPSHOT_VERSION_ZSTD, compressed)
                }
                #[cfg(not(feature = "compression"))]
                {
                    (super::SNAPSHOT_VERSION_RAW, payload)
                }
            }
            Err(e) => return Err(e),
        };

        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(super::SNAPSHOT_MAGIC)?;
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

        crate::wal::format::maybe_crash("post_reset_pre_rename");

        // Now commit the new snapshot. At this point wal.prev contains the
        // pre-compaction ops, so the snapshot-rename crash window is safe.
        std::fs::rename(&tmp, &snap_path)?;

        crate::wal::format::maybe_crash("post_rename_pre_dir_fsync");

        // Fsync the directory so the rename (and the WAL reset performed by
        // rotate_to_backup) are durable.
        crate::wal::format::fsync_dir(&self.dir)?;

        Ok(())
    }
}
