//! Snapshot writing and WAL compaction for [`WalBackend`].
//!
//! # Ordering
//!
//! A compaction commits the new snapshot *before* it rotates the WAL:
//!
//! 1. encode the state and fsync it as `snapshot.tmp`;
//! 2. rename `snapshot.tmp` → `snapshot.postcard` and fsync the directory —
//!    the snapshot is now durable;
//! 3. copy `wal.bin` → `wal.prev` (fsync'd, with its directory entry made
//!    durable) and reset `wal.bin`.
//!
//! Step 3 is what makes the previous generation's `wal.prev` disappear, and it
//! only runs once a snapshot that contains that generation's ops is durable.
//! That is the whole of the retention rule: **a backup is only ever superseded
//! by a committed snapshot** (see [`crate::WalBackend::inspect`] and the load
//! path, neither of which deletes it).
//!
//! The reverse order — rotate first, then rename — has a window in which the
//! committed snapshot is the *old* one while `wal.prev` already holds the
//! *new* generation, so the ops of the generation before it exist nowhere on
//! disk.
//!
//! # Exclusion
//!
//! Everything below runs under the backend's `compact_gate`, so `snapshot.tmp`
//! has a single writer and two rotations can never interleave.

use serde::{Serialize, de::DeserializeOwned};
use std::io::Write;
use std::sync::atomic::Ordering;

use super::WalBackend;
use crate::error::{Error, Result};
use crate::wal::diff::Replayable;

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Write a full snapshot and reset the WAL, taking the per-backend
    /// compaction exclusion for the duration.
    ///
    /// Unconditional: this is the explicit-save path ([`crate::Backend::save`]
    /// and [`crate::IncrementalSave::snapshot`]), so it writes a snapshot even
    /// when the WAL is under threshold.
    pub(super) fn write_snapshot(&self, state: &T) -> Result<()> {
        let _compacting = self.compact_gate.lock();
        self.write_snapshot_locked(state)
    }

    /// Compact only if the WAL is *still* over threshold.
    ///
    /// The threshold is re-checked under the compaction exclusion, and the
    /// state to snapshot is obtained from `state_fn` under it too — so a
    /// caller that queued behind another compaction neither compacts again
    /// immediately nor writes a snapshot from a state older than the
    /// compaction that just committed.
    pub(super) fn compact_if_needed_locked(
        &self,
        state_fn: &mut dyn FnMut() -> Result<T>,
    ) -> Result<bool> {
        let _compacting = self.compact_gate.lock();
        if !self.over_snapshot_threshold() {
            return Ok(false);
        }
        let state = state_fn()?;
        self.write_snapshot_locked(&state)?;
        Ok(true)
    }

    /// Whether the live WAL has crossed the snapshot threshold.
    pub(super) fn over_snapshot_threshold(&self) -> bool {
        self.entry_count.load(Ordering::Acquire) >= self.snapshot_threshold.load(Ordering::Acquire)
    }

    /// Snapshot body. The caller must hold `compact_gate`.
    pub(super) fn write_snapshot_locked(&self, state: &T) -> Result<()> {
        #[cfg(test)]
        let _probe = super::SnapshotProbe::enter(
            &self.snapshot_in_flight,
            &self.snapshot_overlaps,
            &self.snapshot_writes,
        );

        let (version_byte, payload) = Self::encode_snapshot(state)?;
        self.commit_snapshot(version_byte, &payload)?;
        self.rotate_wal()
    }

    /// Serialize `state` into an envelope version byte plus payload bytes.
    ///
    /// Prefers the versioned (v3/v4) format if `T::to_snapshot` succeeds.
    /// Falls back to legacy postcard (v1/v2) format if T doesn't implement
    /// versioned snapshots (returns `SnapshotNotSupported`).
    fn encode_snapshot(state: &T) -> Result<(u8, Vec<u8>)> {
        match state.to_snapshot() {
            Ok(snap) => {
                let encoded = rmp_serde::to_vec_named(&snap).map_err(|e| Error::WalCorrupted {
                    offset: 0,
                    reason: format!("snapshot msgpack encode: {e}"),
                })?;
                #[cfg(feature = "compression")]
                {
                    Ok((
                        super::SNAPSHOT_VERSION_MSGPACK_ZSTD,
                        zstd::encode_all(&encoded[..], 3)?,
                    ))
                }
                #[cfg(not(feature = "compression"))]
                {
                    Ok((super::SNAPSHOT_VERSION_MSGPACK, encoded))
                }
            }
            Err(Error::SnapshotNotSupported) => {
                // Fall back to legacy postcard(T) for types that haven't
                // adopted the derive-macro-generated to_snapshot.
                let payload = postcard::to_allocvec(state)?;
                #[cfg(feature = "compression")]
                {
                    Ok((
                        super::SNAPSHOT_VERSION_ZSTD,
                        zstd::encode_all(&payload[..], 3)?,
                    ))
                }
                #[cfg(not(feature = "compression"))]
                {
                    Ok((super::SNAPSHOT_VERSION_RAW, payload))
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Write `snapshot.tmp`, fsync it, rename it over `snapshot.postcard`, and
    /// fsync the directory. On return the new snapshot is durable.
    ///
    /// The temp path is fixed rather than per-attempt; it is safe because the
    /// caller holds the compaction exclusion, and it means a crash cannot leak
    /// an unbounded pile of orphaned temp files.
    fn commit_snapshot(&self, version_byte: u8, payload: &[u8]) -> Result<()> {
        let snap_path = self.snapshot_path();
        let tmp = snap_path.with_extension("tmp");

        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(super::SNAPSHOT_MAGIC)?;
            f.write_all(&[version_byte])?;
            f.write_all(payload)?;
            f.sync_all()?;
        }

        crate::wal::format::maybe_crash("post_snapshot_tmp_fsync");
        std::fs::rename(&tmp, &snap_path)?;
        crate::wal::format::maybe_crash("post_rename_pre_dir_fsync");
        crate::wal::format::fsync_dir(&self.dir)?;
        Ok(())
    }

    /// Copy the live WAL to `wal.prev` and reset it. Safe only after
    /// [`Self::commit_snapshot`] has made the superseding snapshot durable.
    fn rotate_wal(&self) -> Result<()> {
        crate::wal::format::maybe_crash("post_snapshot_commit_pre_rotate");

        let wal_path = self.wal_path();
        let wal_prev_path = self.wal_prev_path();
        {
            let mut wal = self.wal.lock();
            wal.rotate_to_backup(&wal_path, &wal_prev_path)?;
        }
        self.entry_count.store(0, Ordering::Release);

        crate::wal::format::maybe_crash("post_wal_reset");
        Ok(())
    }
}
