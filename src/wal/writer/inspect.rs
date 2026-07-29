//! [`WalBackend::inspect`] — open a store directory, replay it in memory, and
//! report, without writing a single byte back.

use serde::{Serialize, de::DeserializeOwned};
use std::path::Path;

use super::WalBackend;
use super::replay::{self, LoadPlan};
use crate::error::{Error, Result};
use crate::wal::diff::Replayable;
use crate::wal::quarantine::Quarantine;
use crate::wal::report::ReplayReport;

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Replay a store directory and report what a load would find — writing
    /// nothing to `dir`.
    ///
    /// This performs the full load: snapshot decode, `wal.prev` replay,
    /// `wal.bin` replay, and quarantine settlement (including auto-retry
    /// against the current `T::migrations()`). The resulting state is
    /// discarded; only the [`ReplayReport`] is returned, and its counters are
    /// identical to those of [`WalBackend::load_with_report`] on the same
    /// directory.
    ///
    /// Unlike a load it does **not**: create or write `.lock`; create
    /// `wal.bin`; delete `wal.prev`; truncate a torn WAL tail; rename an
    /// undecodable `snapshot.postcard` to `snapshot.backup`; append recovered
    /// quarantine ops; or rewrite `quarantine.bin`. Where a load would perform
    /// one of those repairs, the report carries a note saying it *would*
    /// happen on a real open.
    ///
    /// # Verifying a checkpoint
    ///
    /// This is the read half of [`crate::Store::checkpoint_to`]: take a
    /// checkpoint, `inspect` the copy, and check
    /// [`ReplayReport::has_loss`]. Because inspection leaves the directory
    /// byte-identical, the bytes you verified are the bytes you keep.
    ///
    /// A persisted quarantine is a *state*, not a load failure: `inspect`
    /// reports its count and returns `Ok`. (`has_loss()` is true for any
    /// persisted quarantine, which is why a strict open of such a store fails
    /// forever and cannot serve as a verification.)
    ///
    /// # Concurrency
    ///
    /// `inspect` deliberately does not take the directory's exclusive lock —
    /// taking it would create `.lock` and would fail against a live writer.
    /// It is therefore only sound against a directory that has **no live
    /// writer**: a checkpoint, a backup, or a stopped node. Pointing it at a
    /// running store's directory can observe a compaction mid-flight and
    /// report a torn tail that no reader would ever see. Satisfying that
    /// constraint is the caller's job.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Io`] if the directory or its files cannot be
    /// read, and [`crate::Error::WalCorrupted`] if `wal.bin` has an
    /// unreadable header (bad magic or an unsupported version) — the same
    /// failures that would abort a real open.
    pub fn inspect(dir: impl AsRef<Path>) -> Result<ReplayReport> {
        let dir = dir.as_ref();
        if !dir.is_dir() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("{} is not a store directory", dir.display()),
            )));
        }
        let mut report = ReplayReport::default();

        // Mirror `open`'s handling: an unreadable quarantine file is not
        // fatal, it is a note plus an empty quarantine.
        let mut quarantine = match Quarantine::load(dir) {
            Ok(q) => q,
            Err(e) => {
                report.push_note(replay::quarantine_unreadable_note(&e));
                Quarantine::new()
            }
        };

        let (_state, plan) = replay::replay_dir::<T>(dir, &mut quarantine, &mut report)?;
        note_deferred_repairs(dir, &plan, &mut report);
        Ok(report)
    }
}

/// Record each repair a real open would perform, so the verdict is complete
/// even though the directory is untouched.
fn note_deferred_repairs(dir: &Path, plan: &LoadPlan, report: &mut ReplayReport) {
    if plan.snapshot_discarded {
        report.push_note(format!(
            "on a real open the undecodable snapshot would be preserved as {}",
            replay::snapshot_backup_path(dir).display()
        ));
    }
    if let Some(offset) = plan.truncate_wal_to {
        report.push_note(format!(
            "on a real open wal.bin would be truncated from {} to {offset} bytes (torn tail)",
            plan.wal_len
        ));
    }
    if !plan.recovered_ops.is_empty() {
        report.push_note(format!(
            "on a real open {} recovered quarantine entries would be appended to wal.bin",
            plan.recovered_ops.len()
        ));
    }
}
