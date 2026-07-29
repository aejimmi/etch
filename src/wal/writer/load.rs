//! Mutating load path for [`WalBackend`]: runs the read-only replay core in
//! [`super::replay`], then applies the repairs it deferred.
//!
//! The repairs are exactly the writes that make a load *not* a read — an
//! undecodable snapshot preserved as `snapshot.backup`, a torn WAL tail
//! truncated, and quarantine entries recovered by newly-registered migrations
//! appended to the WAL. [`super::WalBackend::inspect`] runs the same core and
//! reports these instead of performing them.
//!
//! What a load no longer does is delete `wal.prev`. That file can hold ops the
//! committed snapshot does not (a snapshot is encoded from a state captured
//! before the rotation, and in grouped mode a concurrent writer can land an op
//! in `wal.bin` in between), so deleting it on the strength of "we replayed it
//! into memory" makes the durable state a strict subset of the acknowledged
//! one until the next compaction. `wal.prev` is superseded only by a committed
//! snapshot — see [`super::compact`].

use serde::{Serialize, de::DeserializeOwned};
use std::sync::atomic::Ordering;

use super::replay::{self, LoadPlan};
use super::{LoadMode, WalBackend};
use crate::error::Result;
use crate::wal::diff::Replayable;
use crate::wal::report::ReplayReport;

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Load state (snapshot + WAL replay) under the given [`LoadMode`].
    ///
    /// Quarantine is updated in-place on the backend's persistent store.
    /// New ops during replay that target quarantined keys correctly
    /// supersede old quarantine entries.
    pub(super) fn load_reporting(&self, mode: LoadMode) -> Result<(T, ReplayReport)> {
        let mut report = ReplayReport::default();
        if let Some(note) = &self.quarantine_load_note {
            report.push_note(note.clone());
        }

        // Lock the backend quarantine for the whole load. Called once at
        // startup (before anyone else holds a reference), so contention-free.
        let mut q = self.quarantine.lock();

        let (mut state, plan) = replay::replay_dir::<T>(&self.dir, &mut q, &mut report)?;
        self.apply_repairs(&plan, &mut report)?;

        // Persist any quarantine changes (additions, removals, retry drains).
        q.save(&self.dir)?;
        drop(q);

        if mode == LoadMode::Strict
            && let Some(err) = report.strict_error()
        {
            return Err(err);
        }

        state.after_load();
        Ok((state, report))
    }

    /// Execute the repairs the read-only replay deferred.
    fn apply_repairs(&self, plan: &LoadPlan, report: &mut ReplayReport) -> Result<()> {
        if plan.snapshot_discarded {
            let backup = replay::snapshot_backup_path(&self.dir);
            let _ = std::fs::rename(self.snapshot_path(), &backup);
            report.push_note(format!(
                "undecodable snapshot preserved as {}",
                backup.display()
            ));
        }
        if let Some(offset) = plan.truncate_wal_to {
            // Truncate through the live handle so its writer is repositioned
            // to the new end. A static truncate would leave self.wal seeked
            // past the truncation point, producing a sparse hole on the next
            // append that the next boot reads as corruption.
            self.wal.lock().truncate_to(offset)?;
        }
        self.persist_recovered_ops(plan)
    }

    /// Append auto-retry-recovered quarantine ops to the WAL so they survive a
    /// restart without relying on `quarantine.bin`.
    fn persist_recovered_ops(&self, plan: &LoadPlan) -> Result<()> {
        if plan.recovered_ops.is_empty() {
            return Ok(());
        }
        {
            let mut wal = self.wal.lock();
            wal.append(&plan.recovered_ops)?;
            wal.sync()?;
        }
        self.entry_count.fetch_add(1, Ordering::Release);
        Ok(())
    }
}
