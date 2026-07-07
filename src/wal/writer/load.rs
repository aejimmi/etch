//! Load path for [`WalBackend`]: snapshot decode, WAL replay (live +
//! compaction backup), and auto-retry of the quarantine.

use serde::{Serialize, de::DeserializeOwned};
use std::path::Path;
use std::sync::atomic::Ordering;

use super::{
    LoadMode, SNAPSHOT_MAGIC, SNAPSHOT_VERSION_MSGPACK, SNAPSHOT_VERSION_MSGPACK_ZSTD,
    SNAPSHOT_VERSION_RAW, SNAPSHOT_VERSION_ZSTD, WalBackend, guess_current_version,
};
use crate::error::{Error, Result};
use crate::wal::diff::{ReplayContext, ReplayFormat, Replayable};
use crate::wal::format::WalFile;
use crate::wal::migration::{ChainResult, MigrationSet};
use crate::wal::op::Op;
use crate::wal::quarantine::{Quarantine, QuarantineReason, QuarantinedEntry};
use crate::wal::report::{ReplayReport, SchemaDrift, SnapshotStatus};
use crate::wal::snapshot::SnapshotPayload;

impl<T: Replayable + Serialize + DeserializeOwned + Default> WalBackend<T> {
    /// Load state (snapshot + WAL replay) under the given [`LoadMode`].
    ///
    /// Quarantine is updated in-place on the backend's persistent store.
    /// New ops during replay that target quarantined keys correctly
    /// supersede old quarantine entries.
    pub(super) fn load_reporting(&self, mode: LoadMode) -> Result<(T, ReplayReport)> {
        let migrations = T::migrations();
        let mut report = ReplayReport::default();
        if let Some(note) = &self.quarantine_load_note {
            report.push_note(note.clone());
        }

        // Lock the backend quarantine for the whole load. Called once at
        // startup (before anyone else holds a reference), so contention-free.
        let mut q = self.quarantine.lock();

        let mut state = self.load_snapshot_state(&migrations, &mut q, &mut report)?;
        let replayed_prev = self.replay_wal_prev(&mut state, &migrations, &mut q, &mut report);
        self.replay_wal(&mut state, &migrations, &mut q, &mut report)?;

        // Once both WALs (and the snapshot) have replayed, wal.prev has served
        // its purpose. If we crash before this, wal.prev persists for reboot.
        if replayed_prev {
            let _ = std::fs::remove_file(self.wal_prev_path());
        }

        self.auto_retry_quarantine(&mut state, &migrations, &mut q, &mut report)?;

        // Persist any quarantine changes (additions, removals, retry drains).
        q.save(&self.dir)?;
        finalize_quarantine_report(&q, &mut report);
        drop(q);

        if mode == LoadMode::Strict
            && let Some(err) = report.strict_error()
        {
            return Err(err);
        }

        state.after_load();
        Ok((state, report))
    }

    /// Decode the snapshot into state, recording its status and any drift.
    fn load_snapshot_state(
        &self,
        migrations: &MigrationSet,
        q: &mut Quarantine,
        report: &mut ReplayReport,
    ) -> Result<T> {
        let snap_path = self.snapshot_path();
        if !snap_path.exists() {
            report.set_snapshot(SnapshotStatus::Absent);
            return Ok(T::default());
        }
        let bytes = std::fs::read(&snap_path)?;
        if bytes.is_empty() {
            report.set_snapshot(SnapshotStatus::Absent);
            return Ok(T::default());
        }
        match Self::decode_snapshot_into_state(&bytes, migrations, q, report) {
            Ok(s) => {
                report.set_snapshot(SnapshotStatus::Loaded);
                Ok(s)
            }
            Err(e) => {
                // Never silently fall back to default: preserve the file and
                // record the discard. In strict mode the load aborts on this.
                let backup = self.dir.join("snapshot.backup");
                let _ = std::fs::rename(&snap_path, &backup);
                report.set_snapshot(SnapshotStatus::Discarded {
                    reason: format!("{e}; preserved as {}", backup.display()),
                });
                report.push_note(
                    "snapshot discarded; replaying WAL on default state (possible schema drift)"
                        .to_string(),
                );
                Ok(T::default())
            }
        }
    }

    /// Replay `wal.prev` (compaction backup) if present. Returns whether it
    /// was replayed (so the caller can delete it after a clean boot).
    fn replay_wal_prev(
        &self,
        state: &mut T,
        migrations: &MigrationSet,
        q: &mut Quarantine,
        report: &mut ReplayReport,
    ) -> bool {
        let path = self.wal_prev_path();
        if !path.exists() {
            return false;
        }
        match WalFile::iter_entries(&path) {
            Ok((entries, _)) => {
                let format = wal_replay_format(&path);
                let mut ctx = ReplayContext::new(format, migrations, q);
                for ops in &entries {
                    apply_entry(state, ops, &mut ctx);
                }
                report.absorb(&ctx.report);
                true
            }
            Err(e) => {
                report.record_wal_prev_unreadable(format!("wal.prev unreadable ({e}); skipped"));
                false
            }
        }
    }

    /// Replay the live WAL and truncate any torn tail.
    fn replay_wal(
        &self,
        state: &mut T,
        migrations: &MigrationSet,
        q: &mut Quarantine,
        report: &mut ReplayReport,
    ) -> Result<()> {
        let wal_path = self.wal_path();
        let (entries, valid_offset) = WalFile::iter_entries(&wal_path)?;
        let file_len = std::fs::metadata(&wal_path)?.len();
        let format = wal_replay_format(&wal_path);

        let mut ctx = ReplayContext::new(format, migrations, q);
        for ops in &entries {
            apply_entry(state, ops, &mut ctx);
        }
        report.absorb(&ctx.report);

        if valid_offset < file_len {
            // Truncate through the live handle so its writer is repositioned
            // to the new end. The static truncate_at would leave self.wal
            // seeked past the truncation point, producing a sparse hole on the
            // next append that the next boot reads as corruption.
            self.wal.lock().truncate_to(valid_offset)?;
        }
        Ok(())
    }

    /// Auto-retry quarantine drains using the current migration registry.
    /// Recovered ops are applied and appended to the WAL so they survive a
    /// restart without another compaction.
    fn auto_retry_quarantine(
        &self,
        state: &mut T,
        migrations: &MigrationSet,
        q: &mut Quarantine,
        report: &mut ReplayReport,
    ) -> Result<()> {
        if q.is_empty() || !migrations.is_nonempty() {
            return Ok(());
        }
        let (recovered_ops, still_quarantined) = drain_recoverable(q, migrations);
        if recovered_ops.is_empty() {
            return Ok(());
        }
        report.push_note(format!(
            "auto-retry recovered {} quarantined entries with current migrations",
            recovered_ops.len()
        ));

        let mut scratch_q = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, migrations, &mut scratch_q);
        state.apply_with_ctx(&recovered_ops, &mut ctx)?;
        report.absorb(&ctx.report);

        // Persist recovered ops to WAL so they survive restart without
        // relying on quarantine.bin.
        self.wal.lock().append(&recovered_ops)?;
        self.wal.lock().sync()?;
        self.entry_count.fetch_add(1, Ordering::Release);

        q.clear();
        for entry in still_quarantined {
            q.insert(entry);
        }
        Ok(())
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
        report: &mut ReplayReport,
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
                report.set_drift(check_schema_drift::<T>(snap.schema_fingerprint, migrations));
                let mut ctx = ReplayContext::new(ReplayFormat::Versioned, migrations, quarantine);
                let state = T::from_snapshot(snap, &mut ctx)?;
                report.absorb(&ctx.report);
                Ok(state)
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
                    report.set_drift(check_schema_drift::<T>(snap.schema_fingerprint, migrations));
                    let mut ctx =
                        ReplayContext::new(ReplayFormat::Versioned, migrations, quarantine);
                    let state = T::from_snapshot(snap, &mut ctx)?;
                    report.absorb(&ctx.report);
                    Ok(state)
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
}

/// Classify snapshot-fingerprint drift against the current binary.
///
/// A fingerprint mismatch means either:
/// - The developer bumped one or more collection versions (expected — a
///   migration should cover the difference), or
/// - The developer changed a value type without bumping the version
///   (unexpected — the "forgot to bump" bug the shape-aware fingerprint
///   catches).
///
/// We can't tell these apart at runtime, so this is *informational only*:
/// the outcome is recorded in the [`ReplayReport`] and never aborts a load
/// (not even in strict mode). Genuine data loss surfaces independently as
/// skips/quarantine. Note: an old pair-only fingerprint will drift against
/// the newer shape-aware computation — harmless precisely because drift is
/// warning-only.
fn check_schema_drift<T: Replayable>(stored: u64, migrations: &MigrationSet) -> SchemaDrift {
    let current = T::schema_fingerprint();
    if stored == 0 || current == 0 {
        return SchemaDrift::NotChecked; // one side opted out of drift detection
    }
    if stored == current {
        return SchemaDrift::Match;
    }
    SchemaDrift::Detected {
        stored,
        current,
        has_migrations: migrations.is_nonempty(),
    }
}

/// Select the replay value format for a WAL/backup file by its header version.
fn wal_replay_format(path: &Path) -> ReplayFormat {
    match WalFile::version_of(path) {
        Ok(3) => ReplayFormat::LegacyPostcard,
        _ => ReplayFormat::Versioned,
    }
}

/// Apply one WAL entry, recording success or an entry-level failure in the
/// context's report. Per-op skips are recorded inside `apply_with_ctx`.
fn apply_entry<T: Replayable>(state: &mut T, ops: &[Op], ctx: &mut ReplayContext<'_>) {
    match state.apply_with_ctx(ops, ctx) {
        Ok(()) => ctx.report.record_applied_entry(),
        Err(e) => ctx
            .report
            .record_entry_decode(format!("skipped WAL entry: {e}")),
    }
}

/// Migrate every quarantine entry that now has a forward path. Returns the
/// recovered ops (current-version envelopes) and the entries still stuck.
fn drain_recoverable(
    q: &Quarantine,
    migrations: &MigrationSet,
) -> (Vec<Op>, Vec<QuarantinedEntry>) {
    let mut recovered = Vec::new();
    let mut still = Vec::with_capacity(q.len());
    for entry in q.entries().iter().cloned() {
        let from = entry.version;
        let to = guess_current_version(migrations, entry.collection, from);
        if from == to {
            still.push(entry);
            continue;
        }
        match migrations.migrate_chain(entry.collection, from, to, &entry.value) {
            ChainResult::Migrated(new_bytes) => {
                let mut env = Vec::with_capacity(2 + new_bytes.len());
                env.extend_from_slice(&to.to_le_bytes());
                env.extend_from_slice(&new_bytes);
                recovered.push(Op::Put {
                    collection: entry.collection,
                    key: entry.key.clone(),
                    value: env,
                });
            }
            _ => still.push(entry),
        }
    }
    (recovered, still)
}

/// Summarize the settled quarantine into the report (count + by-reason).
fn finalize_quarantine_report(q: &Quarantine, report: &mut ReplayReport) {
    let mut by_reason: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for entry in q.entries() {
        let label = match &entry.reason {
            QuarantineReason::MissingMigration { from, to } => {
                format!(
                    "MissingMigration collection {} {from}->{to}",
                    entry.collection
                )
            }
            QuarantineReason::MigrationFailed { from, to, .. } => {
                format!(
                    "MigrationFailed collection {} {from}->{to}",
                    entry.collection
                )
            }
            QuarantineReason::MigrationPanicked { from, to, .. } => {
                format!(
                    "MigrationPanicked collection {} {from}->{to}",
                    entry.collection
                )
            }
            QuarantineReason::FromFutureVersion { .. } => {
                format!("FromFutureVersion collection {}", entry.collection)
            }
            QuarantineReason::DecodeFailed { .. } => {
                format!("DecodeFailed collection {}", entry.collection)
            }
        };
        *by_reason.entry(label).or_insert(0) += 1;
    }
    report.set_quarantine(q.len() as u64, by_reason);
}
