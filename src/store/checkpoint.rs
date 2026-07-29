//! [`Store::checkpoint_to`] — a consistent on-disk copy of a live store.
//!
//! An external `cp -a` of an open store directory races compaction: a copier
//! that reads `snapshot.postcard` before the rename and `wal.bin` after the
//! reset gets a stale snapshot plus an empty WAL — a copy that opens cleanly
//! and is silently missing every write since the last compaction. A checkpoint
//! closes that window from the inside, holding the store's write gate and the
//! backend's compaction exclusion across the whole copy.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{Duration, Instant};

use super::Store;
use super::lock::{try_lock_gate_for, try_read_for};
use crate::backend::Backend;
use crate::error::{Error, Result};

/// Outcome of a [`Store::checkpoint_to`] call.
///
/// Carries only metadata — never row bytes — so it is safe to log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[must_use]
pub struct CheckpointReport {
    files: Vec<String>,
    bytes: u64,
    snapshot_forced: bool,
    elapsed: Duration,
}

impl CheckpointReport {
    /// Build a report. Crate-internal: only the checkpoint path produces one.
    pub(crate) fn new(
        files: Vec<String>,
        bytes: u64,
        snapshot_forced: bool,
        elapsed: Duration,
    ) -> Self {
        Self {
            files,
            bytes,
            snapshot_forced,
            elapsed,
        }
    }

    /// File names copied into the destination, in copy order. Never contains
    /// `.lock`.
    #[must_use]
    pub fn files(&self) -> &[String] {
        &self.files
    }

    /// Total bytes copied.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Whether the checkpoint wrote a snapshot before copying. False only
    /// when the live WAL was already empty and a snapshot was already
    /// committed, so no compaction was needed.
    #[must_use]
    pub fn snapshot_forced(&self) -> bool {
        self.snapshot_forced
    }

    /// Wall-clock duration of the checkpoint, including the forced snapshot.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }
}

impl<T: Clone + Send + Sync + 'static, B: Backend<T> + Send + Sync + 'static> Store<T, B> {
    /// Write a consistent copy of this store's directory into `dest`.
    ///
    /// The copy is taken as of the moment the call acquires the write gate:
    /// every write that returned `Ok` before then is present in `dest` with
    /// its committed value, and no write that never happened appears. Writes
    /// issued concurrently may or may not be included — but never partially.
    ///
    /// Per call:
    ///
    /// 1. take the write gate, so no new write is acknowledged;
    /// 2. [`Store::flush`], so grouped-mode ops that were acknowledged but
    ///    still buffered reach `wal.bin`;
    /// 3. force a snapshot under the backend's compaction exclusion, so the
    ///    copy set is small and no compaction can fire during the copy;
    /// 4. copy `snapshot.postcard`, `wal.bin`, and whichever of `wal.prev`,
    ///    `quarantine.bin` and `snapshot.backup` exist, fsyncing each file and
    ///    then `dest` itself.
    ///
    /// `.lock` is never copied: an advisory lock is not persistent, so a
    /// `.lock` in a copy is at best meaningless and at worst misleading (it
    /// carries the PID of a process on another machine). `wal.prev` *is*
    /// copied — it holds the pre-snapshot generation, and omitting it would
    /// leave the copy's durable state a subset of its acknowledged one.
    ///
    /// `dest` is created if absent, inherits the source directory's mode on
    /// Unix, and must not already contain a store file.
    ///
    /// Verify the result with [`crate::WalBackend::inspect`], which reads the
    /// copy without perturbing it.
    ///
    /// # Sizing
    ///
    /// A checkpoint transiently needs room for a second full copy of the
    /// store, plus one `snapshot.tmp` in the *source* directory for the
    /// forced snapshot. Budget `2 × store size + snapshot size` of free space.
    ///
    /// # Security
    ///
    /// A checkpoint contains every persisted value verbatim. It is exactly as
    /// sensitive as the store it came from — protect it the same way.
    ///
    /// # Errors
    ///
    /// - [`Error::CheckpointUnsupported`] if this store has no WAL backend
    ///   (for instance [`Store::memory`]).
    /// - [`Error::CheckpointDestInvalid`] if `dest` is the source directory,
    ///   nested inside it, contains it, or is not a directory.
    /// - [`Error::CheckpointDestNotEmpty`] if `dest` already holds a store
    ///   file. Nothing is written in that case — not even the forced
    ///   snapshot.
    /// - [`Error::Io`] on a copy or fsync failure (a full disk, typically).
    ///   The source directory is left intact.
    pub fn checkpoint_to(&self, dest: impl AsRef<Path>) -> Result<CheckpointReport> {
        let Some(inc) = self.incremental.clone() else {
            return Err(Error::CheckpointUnsupported);
        };
        let started = Instant::now();

        // Hold the gate for the whole checkpoint: no write is acknowledged
        // between the flush and the end of the copy.
        let _gate = try_lock_gate_for(
            &self.write_gate,
            "checkpoint_to",
            self.lock_deadlock_timeout(),
        )?;
        self.flush()?;

        let timeout = self.lock_deadlock_timeout();
        let state = &self.state;
        let mut state_fn = move || -> Result<T> {
            Ok(try_read_for(state, "checkpoint_to/snapshot", timeout)?.clone())
        };
        let report = inc.checkpoint_into(dest.as_ref(), &mut state_fn)?;
        Ok(CheckpointReport {
            elapsed: started.elapsed(),
            ..report
        })
    }
}

#[cfg(test)]
#[path = "checkpoint_test.rs"]
mod checkpoint_test;
