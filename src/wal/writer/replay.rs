//! The read-only core of a load, shared by [`super::WalBackend::load_with_report`]
//! (which then performs the repairs) and [`super::WalBackend::inspect`] (which
//! only reports them).
//!
//! Everything in this module is a pure read of a store directory: it opens
//! files for reading, decodes, replays into an in-memory `T`, and returns a
//! [`LoadPlan`] describing the repairs a real open *would* perform. Nothing
//! here creates, renames, truncates, appends to, or deletes a file — that
//! separation is what makes a non-mutating `inspect` expressible at all.

use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};

use super::recover::guess_current_version;
use super::{
    SNAPSHOT_MAGIC, SNAPSHOT_VERSION_MSGPACK, SNAPSHOT_VERSION_MSGPACK_ZSTD, SNAPSHOT_VERSION_RAW,
    SNAPSHOT_VERSION_ZSTD,
};
use crate::error::{Error, Result};
use crate::wal::diff::{ReplayContext, ReplayFormat, Replayable};
use crate::wal::format::WalFile;
use crate::wal::migration::{ChainResult, MigrationSet};
use crate::wal::op::Op;
use crate::wal::quarantine::{Quarantine, QuarantineReason, QuarantinedEntry};
use crate::wal::report::{ReplayReport, SchemaDrift, SnapshotStatus};
use crate::wal::snapshot::SnapshotPayload;

/// Committed snapshot file.
pub(super) fn snapshot_path(dir: &Path) -> PathBuf {
    dir.join("snapshot.postcard")
}

/// Where an undecodable snapshot is preserved by a real open.
pub(super) fn snapshot_backup_path(dir: &Path) -> PathBuf {
    dir.join("snapshot.backup")
}

/// Live write-ahead log.
pub(super) fn wal_path(dir: &Path) -> PathBuf {
    dir.join("wal.bin")
}

/// Compaction backup of the previous WAL generation.
pub(super) fn wal_prev_path(dir: &Path) -> PathBuf {
    dir.join("wal.prev")
}

/// Note recorded when the on-disk quarantine file could not be read.
pub(super) fn quarantine_unreadable_note(e: &Error) -> String {
    format!("quarantine file unreadable ({e}); started with an empty quarantine")
}

/// The repairs a mutating load would apply after the read-only replay.
///
/// A real open executes these; [`super::WalBackend::inspect`] reports them.
#[derive(Debug, Default)]
pub(super) struct LoadPlan {
    /// The snapshot failed to decode and must be preserved as
    /// `snapshot.backup` rather than overwritten.
    pub snapshot_discarded: bool,
    /// The live WAL has a torn tail and must be truncated to this offset.
    pub truncate_wal_to: Option<u64>,
    /// Length of `wal.bin` as observed, for reporting the truncation size.
    pub wal_len: u64,
    /// Quarantined entries that the current migration registry can recover.
    /// A real open applies them to state and appends them to the WAL.
    pub recovered_ops: Vec<Op>,
}

/// Replay a store directory into state without writing to it.
///
/// Reads the snapshot, replays `wal.prev` then `wal.bin`, and settles the
/// quarantine (including auto-retry against the current migrations). Counters
/// land in `report`; deferred repairs land in the returned [`LoadPlan`].
pub(super) fn replay_dir<T>(
    dir: &Path,
    q: &mut Quarantine,
    report: &mut ReplayReport,
) -> Result<(T, LoadPlan)>
where
    T: Replayable + Serialize + DeserializeOwned + Default,
{
    let migrations = T::migrations();
    let mut plan = LoadPlan::default();

    let mut state = load_snapshot_state::<T>(dir, &migrations, q, report, &mut plan)?;
    replay_backup_wal(dir, &mut state, &migrations, q, report);
    replay_live_wal(dir, &mut state, &migrations, q, report, &mut plan)?;
    auto_retry_quarantine(&mut state, &migrations, q, report, &mut plan)?;

    finalize_quarantine_report(q, report);
    Ok((state, plan))
}

/// Decode the snapshot into state, recording its status and any drift.
fn load_snapshot_state<T>(
    dir: &Path,
    migrations: &MigrationSet,
    q: &mut Quarantine,
    report: &mut ReplayReport,
    plan: &mut LoadPlan,
) -> Result<T>
where
    T: Replayable + Serialize + DeserializeOwned + Default,
{
    let snap_path = snapshot_path(dir);
    if !snap_path.exists() {
        report.set_snapshot(SnapshotStatus::Absent);
        return Ok(T::default());
    }
    let bytes = std::fs::read(&snap_path)?;
    if bytes.is_empty() {
        report.set_snapshot(SnapshotStatus::Absent);
        return Ok(T::default());
    }
    match decode_snapshot::<T>(&bytes, migrations, q, report) {
        Ok(s) => {
            report.set_snapshot(SnapshotStatus::Loaded);
            Ok(s)
        }
        Err(e) => {
            // Never silently fall back to default: the file is preserved (by
            // the caller, if it is a mutating load) and the discard recorded.
            // In strict mode the load aborts on this.
            plan.snapshot_discarded = true;
            report.set_snapshot(SnapshotStatus::Discarded {
                reason: format!("{e}"),
            });
            report.push_note(
                "snapshot discarded; replaying WAL on default state (possible schema drift)"
                    .to_string(),
            );
            Ok(T::default())
        }
    }
}

/// Replay `wal.prev` (compaction backup) if present.
///
/// `wal.prev` always holds a *strictly older* generation than `wal.bin`, so
/// replaying it before the live WAL is idempotent even when its ops are
/// already in the snapshot.
fn replay_backup_wal<T: Replayable>(
    dir: &Path,
    state: &mut T,
    migrations: &MigrationSet,
    q: &mut Quarantine,
    report: &mut ReplayReport,
) {
    let path = wal_prev_path(dir);
    if !path.exists() {
        return;
    }
    match WalFile::iter_entries(&path) {
        Ok((entries, _)) => {
            let mut ctx = ReplayContext::new(wal_replay_format(&path), migrations, q);
            for ops in &entries {
                apply_entry(state, ops, &mut ctx);
            }
            report.absorb(&ctx.report);
        }
        Err(e) => {
            report.record_wal_prev_unreadable(format!("wal.prev unreadable ({e}); skipped"));
        }
    }
}

/// Replay the live WAL, recording any torn tail for the caller to truncate.
fn replay_live_wal<T: Replayable>(
    dir: &Path,
    state: &mut T,
    migrations: &MigrationSet,
    q: &mut Quarantine,
    report: &mut ReplayReport,
    plan: &mut LoadPlan,
) -> Result<()> {
    let path = wal_path(dir);
    if !path.exists() {
        // Only reachable from `inspect` — a real open creates the WAL first.
        return Ok(());
    }
    let (entries, valid_offset) = WalFile::iter_entries(&path)?;
    plan.wal_len = std::fs::metadata(&path)?.len();

    let mut ctx = ReplayContext::new(wal_replay_format(&path), migrations, q);
    for ops in &entries {
        apply_entry(state, ops, &mut ctx);
    }
    report.absorb(&ctx.report);

    if valid_offset < plan.wal_len {
        plan.truncate_wal_to = Some(valid_offset);
    }
    Ok(())
}

/// Auto-retry quarantine drains using the current migration registry.
/// Recovered ops are applied to state here; persisting them to the WAL is the
/// mutating caller's job.
fn auto_retry_quarantine<T: Replayable>(
    state: &mut T,
    migrations: &MigrationSet,
    q: &mut Quarantine,
    report: &mut ReplayReport,
    plan: &mut LoadPlan,
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

    q.clear();
    for entry in still_quarantined {
        q.insert(entry);
    }
    plan.recovered_ops = recovered_ops;
    Ok(())
}

/// Decode a snapshot into state, dispatching by envelope version.
///
/// v1/v2 (legacy postcard) → decode directly as T.
/// v3/v4 (msgpack SnapshotPayload) → dispatch through T::from_snapshot
/// with per-value migration and quarantine.
fn decode_snapshot<T>(
    bytes: &[u8],
    migrations: &MigrationSet,
    quarantine: &mut Quarantine,
    report: &mut ReplayReport,
) -> Result<T>
where
    T: Replayable + Serialize + DeserializeOwned + Default,
{
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
        SNAPSHOT_VERSION_ZSTD => decode_legacy_zstd(payload),
        SNAPSHOT_VERSION_MSGPACK => decode_versioned::<T>(payload, migrations, quarantine, report),
        SNAPSHOT_VERSION_MSGPACK_ZSTD => {
            decode_versioned_zstd::<T>(payload, migrations, quarantine, report)
        }
        _ => Err(Error::SnapshotVersion {
            version,
            expected: SNAPSHOT_VERSION_MSGPACK,
        }),
    }
}

/// Legacy v2: zstd-compressed postcard payload of T.
fn decode_legacy_zstd<T: DeserializeOwned>(payload: &[u8]) -> Result<T> {
    #[cfg(feature = "compression")]
    {
        let decompressed = zstd::decode_all(payload)?;
        Ok(postcard::from_bytes(&decompressed)?)
    }
    #[cfg(not(feature = "compression"))]
    {
        let _ = payload;
        Err(compression_disabled())
    }
}

/// v3: msgpack-named `SnapshotPayload` with per-value versioning.
fn decode_versioned<T>(
    payload: &[u8],
    migrations: &MigrationSet,
    quarantine: &mut Quarantine,
    report: &mut ReplayReport,
) -> Result<T>
where
    T: Replayable + Serialize + DeserializeOwned + Default,
{
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

/// v4: zstd-compressed [`decode_versioned`] payload.
fn decode_versioned_zstd<T>(
    payload: &[u8],
    migrations: &MigrationSet,
    quarantine: &mut Quarantine,
    report: &mut ReplayReport,
) -> Result<T>
where
    T: Replayable + Serialize + DeserializeOwned + Default,
{
    #[cfg(feature = "compression")]
    {
        let decompressed = zstd::decode_all(payload)?;
        decode_versioned::<T>(&decompressed, migrations, quarantine, report)
    }
    #[cfg(not(feature = "compression"))]
    {
        let _ = (payload, migrations, quarantine, report);
        Err(compression_disabled())
    }
}

/// Error for a compressed snapshot read by a build without the feature.
#[cfg(not(feature = "compression"))]
fn compression_disabled() -> Error {
    Error::invalid(
        "snapshot",
        "snapshot was written with zstd compression; enable the `compression` feature to read it",
    )
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
