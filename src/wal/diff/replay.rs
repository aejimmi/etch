//! Versioned replay machinery: the [`ReplayContext`], the migration- and
//! quarantine-aware `apply_op_versioned_*` helpers, and the snapshot-entry
//! decoder. Split out from the format-agnostic diff primitives in the parent
//! module.

use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::hash::Hash;

use super::{ReplayFormat, split_versioned_value};
use crate::wal::migration::{ChainResult, MigrationSet};
use crate::wal::op::Op;
use crate::wal::quarantine::{Quarantine, QuarantineReason, QuarantinedEntry};
use crate::wal::report::ReplayReport;
use crate::wal::snapshot::SnapshotEntry;

/// Decode a single snapshot entry with migration + quarantine dispatch.
///
/// - `stored_version == current_version`: direct msgpack decode.
/// - `stored_version < current`: run migration chain, then decode.
/// - Any failure: push a `QuarantinedEntry` into `ctx.quarantine`,
///   return `None`.
///
/// This helper is the snapshot-side counterpart to
/// `apply_op_versioned_with_ctx` — same failure taxonomy, same reason
/// translation, different source of bytes.
pub fn load_snapshot_entry<V>(
    entry: &SnapshotEntry,
    collection_id: u8,
    current_version: u16,
    ctx: &mut ReplayContext<'_>,
) -> Option<V>
where
    V: DeserializeOwned,
{
    // Reconstruct the versioned envelope so we can reuse the shared
    // migration/decode path. This keeps WAL and snapshot behavior in sync.
    let mut envelope = Vec::with_capacity(2 + entry.value.len());
    envelope.extend_from_slice(&entry.version.to_le_bytes());
    envelope.extend_from_slice(&entry.value);

    match decode_value_with_migration::<V>(
        &envelope,
        ReplayFormat::Versioned,
        collection_id,
        current_version,
        ctx.migrations,
    ) {
        Ok(v) => Some(v),
        Err(e) => {
            let future = quarantine_value(
                ctx.quarantine,
                collection_id,
                &entry.key,
                &envelope,
                ReplayFormat::Versioned,
                current_version,
                e,
            );
            ctx.report.record_value_decode();
            if let Some((stored, current)) = future {
                ctx.report.note_future_version(stored, current);
            }
            None
        }
    }
}

/// Context threaded through `apply_with_ctx` for versioned replay with
/// migration and quarantine support.
///
/// The quarantine collects values that cannot be migrated to the current
/// schema. It is the caller's responsibility to persist quarantine after
/// replay completes.
///
/// `report` accumulates a structured account of every recoverable event
/// (value/key-decode skips, unknown collections, successful applies) so the
/// caller can surface exactly what happened instead of relying on log
/// scraping. See [`ReplayReport`].
pub struct ReplayContext<'a> {
    pub format: ReplayFormat,
    pub migrations: &'a MigrationSet,
    pub quarantine: &'a mut Quarantine,
    /// Per-phase replay accounting. Owned so the 3-arg [`ReplayContext::new`]
    /// stays source-compatible; the load path folds each phase's counters
    /// into the top-level [`ReplayReport`] it returns to the caller.
    pub report: ReplayReport,
}

impl<'a> ReplayContext<'a> {
    /// Create a context with a fresh, empty [`ReplayReport`].
    pub fn new(
        format: ReplayFormat,
        migrations: &'a MigrationSet,
        quarantine: &'a mut Quarantine,
    ) -> Self {
        Self {
            format,
            migrations,
            quarantine,
            report: ReplayReport::default(),
        }
    }

    /// Record an op that targeted a collection id the current schema does not
    /// declare. Called from `#[derive(Replayable)]`-generated dispatch.
    pub fn record_unknown_collection(&mut self, id: u8) {
        self.report.record_unknown_collection(id);
    }

    /// Record a value that decoded but whose key could not be converted from
    /// its stored bytes. Called from `#[derive(Replayable)]`-generated
    /// snapshot loading.
    pub fn record_key_decode(&mut self) {
        self.report.record_key_decode();
    }
}

/// Apply a Put or Delete with format awareness (legacy postcard vs versioned msgpack).
///
/// On schema version mismatch, returns `SchemaVersionMismatch` — migration
/// is not attempted. Use `apply_op_versioned_with_ctx` for migration-aware
/// replay.
pub fn apply_op_versioned_with<K, V, F>(
    map: &mut BTreeMap<K, V>,
    op: &Op,
    format: ReplayFormat,
    current_version: u16,
    convert_key: F,
) -> crate::Result<()>
where
    K: Ord,
    V: DeserializeOwned,
    F: Fn(&[u8]) -> crate::Result<K>,
{
    match op {
        Op::Put { key, value, .. } => {
            let k = convert_key(key)?;
            let v: V = decode_value_no_migrate(value, format, current_version)?;
            map.insert(k, v);
        }
        Op::Delete { key, .. } => {
            let k = convert_key(key)?;
            map.remove(&k);
        }
    }
    Ok(())
}

/// HashMap variant of `apply_op_versioned_with`.
pub fn apply_op_versioned_hash_with<K, V, F>(
    map: &mut std::collections::HashMap<K, V>,
    op: &Op,
    format: ReplayFormat,
    current_version: u16,
    convert_key: F,
) -> crate::Result<()>
where
    K: Eq + Hash,
    V: DeserializeOwned,
    F: Fn(&[u8]) -> crate::Result<K>,
{
    match op {
        Op::Put { key, value, .. } => {
            let k = convert_key(key)?;
            let v: V = decode_value_no_migrate(value, format, current_version)?;
            map.insert(k, v);
        }
        Op::Delete { key, .. } => {
            let k = convert_key(key)?;
            map.remove(&k);
        }
    }
    Ok(())
}

/// Apply a Put or Delete with full context: format, migrations, quarantine,
/// and replay accounting.
///
/// Recoverable failures never abort: a value that cannot decode/migrate is
/// added to quarantine, and a value whose key cannot be converted is skipped.
/// Both are recorded in `ctx.report` and the function returns `Ok(())` — the
/// database still starts and every event is inspectable.
///
/// On Delete of a previously quarantined key, the quarantine entry is
/// removed (the user explicitly chose to delete).
pub fn apply_op_versioned_with_ctx<K, V, F>(
    map: &mut BTreeMap<K, V>,
    op: &Op,
    collection_id: u8,
    current_version: u16,
    ctx: &mut ReplayContext<'_>,
    convert_key: F,
) -> crate::Result<()>
where
    K: Ord,
    V: DeserializeOwned,
    F: Fn(&[u8]) -> crate::Result<K>,
{
    match op {
        Op::Put { key, value, .. } => {
            match decode_value_with_migration::<V>(
                value,
                ctx.format,
                collection_id,
                current_version,
                ctx.migrations,
            ) {
                Ok(v) => match convert_key(key) {
                    Ok(k) => {
                        // A new successful write supersedes any quarantine entry.
                        ctx.quarantine.remove_key(collection_id, key);
                        map.insert(k, v);
                        ctx.report.record_applied_op();
                    }
                    Err(_) => ctx.report.record_key_decode(),
                },
                Err(e) => record_quarantine(ctx, collection_id, key, value, current_version, e),
            }
        }
        Op::Delete { key, .. } => match convert_key(key) {
            Ok(k) => {
                // Remove from both the live map and quarantine (explicit deletion).
                map.remove(&k);
                ctx.quarantine.remove_key(collection_id, key);
                ctx.report.record_applied_op();
            }
            Err(_) => ctx.report.record_key_decode(),
        },
    }
    Ok(())
}

/// HashMap variant of `apply_op_versioned_with_ctx`.
pub fn apply_op_versioned_hash_with_ctx<K, V, F>(
    map: &mut std::collections::HashMap<K, V>,
    op: &Op,
    collection_id: u8,
    current_version: u16,
    ctx: &mut ReplayContext<'_>,
    convert_key: F,
) -> crate::Result<()>
where
    K: Eq + Hash,
    V: DeserializeOwned,
    F: Fn(&[u8]) -> crate::Result<K>,
{
    match op {
        Op::Put { key, value, .. } => {
            match decode_value_with_migration::<V>(
                value,
                ctx.format,
                collection_id,
                current_version,
                ctx.migrations,
            ) {
                Ok(v) => match convert_key(key) {
                    Ok(k) => {
                        ctx.quarantine.remove_key(collection_id, key);
                        map.insert(k, v);
                        ctx.report.record_applied_op();
                    }
                    Err(_) => ctx.report.record_key_decode(),
                },
                Err(e) => record_quarantine(ctx, collection_id, key, value, current_version, e),
            }
        }
        Op::Delete { key, .. } => match convert_key(key) {
            Ok(k) => {
                map.remove(&k);
                ctx.quarantine.remove_key(collection_id, key);
                ctx.report.record_applied_op();
            }
            Err(_) => ctx.report.record_key_decode(),
        },
    }
    Ok(())
}

/// Quarantine a value-decode failure and record it in the report.
fn record_quarantine(
    ctx: &mut ReplayContext<'_>,
    collection_id: u8,
    key: &[u8],
    value: &[u8],
    current_version: u16,
    err: crate::Error,
) {
    let future = quarantine_value(
        ctx.quarantine,
        collection_id,
        key,
        value,
        ctx.format,
        current_version,
        err,
    );
    ctx.report.record_value_decode();
    if let Some((stored, current)) = future {
        ctx.report.note_future_version(stored, current);
    }
}

/// Translate a decode/migration error into a quarantine entry.
///
/// Returns `Some((stored, current))` when the failure was a future-version
/// mismatch, so the caller can surface a typed `SchemaVersionMismatch` in
/// strict mode.
fn quarantine_value(
    q: &mut Quarantine,
    collection_id: u8,
    key: &[u8],
    original_bytes: &[u8],
    format: ReplayFormat,
    current_version: u16,
    err: crate::Error,
) -> Option<(u16, u16)> {
    // Recover the stored version if possible. For legacy postcard, it's 0.
    let stored_version = match format {
        ReplayFormat::LegacyPostcard => 0,
        ReplayFormat::Versioned => split_versioned_value(original_bytes)
            .map(|(v, _)| v)
            .unwrap_or(0),
    };

    // The value bytes we preserve are the payload *after* the version tag
    // (for Versioned) or the raw bytes (for LegacyPostcard). This way the
    // bytes can be fed directly to a migration function in `retry_quarantine`.
    let preserved: Vec<u8> = match format {
        ReplayFormat::LegacyPostcard => original_bytes.to_vec(),
        ReplayFormat::Versioned => split_versioned_value(original_bytes)
            .map(|(_, p)| p.to_vec())
            .unwrap_or_else(|_| original_bytes.to_vec()),
    };

    let mut future = None;
    let reason = match err {
        crate::Error::MigrationMissing {
            from_version,
            to_version,
            ..
        } => QuarantineReason::MissingMigration {
            from: from_version,
            to: to_version,
        },
        crate::Error::MigrationFailed {
            from_version,
            to_version,
            reason,
            ..
        } => QuarantineReason::MigrationFailed {
            from: from_version,
            to: to_version,
            reason,
        },
        crate::Error::MigrationPanicked {
            from_version,
            to_version,
            message,
            ..
        } => QuarantineReason::MigrationPanicked {
            from: from_version,
            to: to_version,
            message,
        },
        crate::Error::SchemaVersionMismatch { stored, current } if stored > current => {
            future = Some((stored, current));
            QuarantineReason::FromFutureVersion { stored, current }
        }
        other => QuarantineReason::DecodeFailed {
            reason: other.to_string(),
        },
    };

    q.insert(QuarantinedEntry {
        collection: collection_id,
        key: key.to_vec(),
        version: stored_version,
        value: preserved,
        reason,
    });
    let _ = current_version; // reserved for future diagnostics
    future
}

/// Decode without attempting migration.
fn decode_value_no_migrate<V: DeserializeOwned>(
    bytes: &[u8],
    format: ReplayFormat,
    current_version: u16,
) -> crate::Result<V> {
    match format {
        ReplayFormat::LegacyPostcard => Ok(postcard::from_bytes(bytes)?),
        ReplayFormat::Versioned => {
            let (stored_version, payload) = split_versioned_value(bytes)?;
            if stored_version != current_version {
                return Err(crate::Error::SchemaVersionMismatch {
                    stored: stored_version,
                    current: current_version,
                });
            }
            decode_versioned_payload(payload)
        }
    }
}

/// Decode with migration chain on version mismatch.
fn decode_value_with_migration<V: DeserializeOwned>(
    bytes: &[u8],
    format: ReplayFormat,
    collection_id: u8,
    current_version: u16,
    migrations: &MigrationSet,
) -> crate::Result<V> {
    let (stored_version, payload): (u16, Vec<u8>) = match format {
        // Legacy postcard WAL: treat all values as version 0 (unversioned).
        ReplayFormat::LegacyPostcard => (0, bytes.to_vec()),
        ReplayFormat::Versioned => {
            let (v, p) = split_versioned_value(bytes)?;
            (v, p.to_vec())
        }
    };

    if stored_version == current_version {
        return decode_versioned_payload(&payload);
    }

    // Need migration.
    if stored_version > current_version {
        return Err(crate::Error::SchemaVersionMismatch {
            stored: stored_version,
            current: current_version,
        });
    }

    match migrations.migrate_chain(collection_id, stored_version, current_version, &payload) {
        ChainResult::Migrated(new_bytes) => decode_versioned_payload(&new_bytes),
        ChainResult::Missing { hop } => Err(crate::Error::MigrationMissing {
            collection: collection_id,
            from_version: hop,
            to_version: hop + 1,
        }),
        ChainResult::Failed { hop, error } => Err(crate::Error::MigrationFailed {
            collection: collection_id,
            from_version: hop,
            to_version: hop + 1,
            reason: error.0,
        }),
        ChainResult::Panicked { hop, message } => Err(crate::Error::MigrationPanicked {
            collection: collection_id,
            from_version: hop,
            to_version: hop + 1,
            message,
        }),
    }
}

fn decode_versioned_payload<V: DeserializeOwned>(payload: &[u8]) -> crate::Result<V> {
    rmp_serde::from_slice(payload).map_err(|e| crate::Error::WalCorrupted {
        offset: 0,
        reason: format!("msgpack decode: {e}"),
    })
}
