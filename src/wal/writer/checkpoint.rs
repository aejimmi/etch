//! Backend half of [`crate::Store::checkpoint_to`]: force a snapshot and copy
//! the store's file set, both under the compaction exclusion.
//!
//! The store has already taken its write gate and flushed, so nothing appends
//! to `wal.bin` while this runs; holding `compact_gate` additionally excludes
//! the grouped-flush background compactor, which takes no write gate.

use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::WalBackend;
use crate::error::{Error, Result};
use crate::store::CheckpointReport;
use crate::wal::diff::Replayable;
use crate::wal::format::{fsync_dir, maybe_crash};
use crate::wal::quarantine::QUARANTINE_FILE;

/// Files that make up a store, in copy order. `.lock` is deliberately absent:
/// an advisory lock is not persistent, and a copied one carries a foreign PID.
const STORE_FILES: &[&str] = &[
    "snapshot.postcard",
    "wal.bin",
    "wal.prev",
    "snapshot.backup",
    QUARANTINE_FILE,
];

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Force a snapshot and copy the store's files into `dest`.
    pub(super) fn checkpoint(
        &self,
        dest: &Path,
        state_fn: &mut dyn FnMut() -> Result<T>,
    ) -> Result<CheckpointReport> {
        let started = Instant::now();
        // Validate before anything is written, so a rejected destination
        // leaves the source untouched — including its snapshot.
        validate_dest(&self.dir, dest)?;

        let _compacting = self.compact_gate.lock();
        maybe_crash("checkpoint_pre_snapshot");

        let forced = self.force_snapshot(state_fn)?;
        maybe_crash("checkpoint_post_snapshot");

        prepare_dest(&self.dir, dest)?;
        let (files, bytes) = copy_store_files(&self.dir, dest)?;
        fsync_dir(dest)?;
        maybe_crash("checkpoint_post_copy");

        Ok(CheckpointReport::new(
            files,
            bytes,
            forced,
            started.elapsed(),
        ))
    }

    /// Write a snapshot so the copy set is a committed snapshot plus an empty
    /// WAL. Skipped only when the live WAL is already empty and a snapshot is
    /// already committed — then there is nothing to compact.
    fn force_snapshot(&self, state_fn: &mut dyn FnMut() -> Result<T>) -> Result<bool> {
        if self.entry_count.load(std::sync::atomic::Ordering::Acquire) == 0
            && self.snapshot_path().exists()
        {
            return Ok(false);
        }
        let state = state_fn()?;
        self.write_snapshot_locked(&state)?;
        Ok(true)
    }
}

/// Reject a destination that cannot hold an independent copy: the source
/// itself, anything nested either way, a non-directory, or a directory that
/// already holds a store file.
fn validate_dest(src: &Path, dest: &Path) -> Result<()> {
    if dest.exists() && !dest.is_dir() {
        return Err(invalid_dest(
            dest,
            "destination exists and is not a directory",
        ));
    }
    let src_real = src.canonicalize()?;
    let dest_real = resolve_dest(dest)?;

    if dest_real == src_real {
        return Err(invalid_dest(dest, "destination is the source directory"));
    }
    if dest_real.starts_with(&src_real) {
        return Err(invalid_dest(
            dest,
            "destination is inside the source directory",
        ));
    }
    if src_real.starts_with(&dest_real) {
        return Err(invalid_dest(
            dest,
            "destination contains the source directory",
        ));
    }
    reject_existing_store(dest)
}

/// Absolute path for a destination that may not exist yet: resolve the nearest
/// existing ancestor and re-append the remainder.
fn resolve_dest(dest: &Path) -> Result<PathBuf> {
    if dest.exists() {
        return Ok(dest.canonicalize()?);
    }
    let mut suffix = Vec::new();
    let mut cursor = dest;
    loop {
        let Some(parent) = cursor.parent() else {
            return Ok(dest.to_path_buf());
        };
        let Some(name) = cursor.file_name() else {
            return Ok(dest.to_path_buf());
        };
        suffix.push(name.to_os_string());
        if parent.exists() {
            let mut resolved = parent.canonicalize()?;
            for part in suffix.iter().rev() {
                resolved.push(part);
            }
            return Ok(resolved);
        }
        cursor = parent;
    }
}

/// A checkpoint never merges into an existing store.
fn reject_existing_store(dest: &Path) -> Result<()> {
    for name in STORE_FILES {
        if dest.join(name).exists() {
            return Err(Error::CheckpointDestNotEmpty {
                dir: dest.display().to_string(),
                file: (*name).to_string(),
            });
        }
    }
    Ok(())
}

/// Build the typed rejection for a structurally unusable destination.
fn invalid_dest(dest: &Path, reason: &str) -> Error {
    Error::CheckpointDestInvalid {
        dir: dest.display().to_string(),
        reason: reason.to_string(),
    }
}

/// Create the destination, mirroring the source directory's mode on Unix so a
/// copy of a private store is not world-readable.
fn prepare_dest(src: &Path, dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest)?;
    #[cfg(unix)]
    {
        let mode = std::fs::metadata(src)?.permissions();
        std::fs::set_permissions(dest, mode)?;
    }
    #[cfg(not(unix))]
    {
        let _ = src;
    }
    Ok(())
}

/// Copy every present store file, fsyncing each one. Returns the names copied
/// and the total bytes.
fn copy_store_files(src: &Path, dest: &Path) -> Result<(Vec<String>, u64)> {
    let mut files = Vec::with_capacity(STORE_FILES.len());
    let mut bytes = 0u64;
    for name in STORE_FILES {
        let from = src.join(name);
        if !from.exists() {
            continue;
        }
        bytes += copy_and_sync(&from, &dest.join(name))?;
        files.push((*name).to_string());
        maybe_crash("checkpoint_mid_copy");
    }
    Ok((files, bytes))
}

/// Copy one file and fsync the destination copy, so the checkpoint survives a
/// power loss right after it returns.
fn copy_and_sync(from: &Path, to: &Path) -> Result<u64> {
    let bytes = std::fs::copy(from, to)?;
    std::fs::OpenOptions::new()
        .write(true)
        .open(to)?
        .sync_all()?;
    Ok(bytes)
}
