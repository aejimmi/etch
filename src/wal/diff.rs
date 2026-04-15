//! Replayable trait and helpers for WAL replay.
//!
//! Types that implement `Replayable` can reconstruct their state from
//! a sequence of WAL ops on startup.
//!
//! # Value encoding
//!
//! Which format a value uses depends on the WAL format version it was
//! written to, not on anything embedded in the value bytes themselves.
//! This avoids magic-byte collisions with arbitrary user data.
//!
//! - **WAL v3 (legacy, etchdb < 0.4.0)**: `Op::Put.value` is raw postcard
//!   bytes of the V type. Read-only — no new writes go to v3 WALs.
//! - **WAL v4 (etchdb >= 0.4.0)**: `Op::Put.value` is `[version: u16 LE]
//!   [msgpack_named payload]`. Every value carries its own schema version
//!   tag, allowing per-value migration independently of other collections.
//!
//! The caller (typically `WalBackend`) knows the WAL version at replay
//! time and passes `ReplayFormat` into the apply helpers to select the
//! correct decoder.

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::hash::Hash;

use super::migration::{ChainResult, MigrationSet};
use super::op::Op;
use super::quarantine::{Quarantine, QuarantineReason, QuarantinedEntry};
use super::snapshot::{SnapshotEntry, SnapshotPayload};

/// Which on-disk value format to expect during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayFormat {
    /// Legacy WAL v3: raw postcard bytes, no version envelope.
    LegacyPostcard,
    /// WAL v4+: 2-byte LE version prefix + msgpack_named payload.
    Versioned,
}

/// Encode a value as msgpack (named fields) without a version envelope.
///
/// Used for snapshot entries, where the version is stored separately.
pub fn encode_msgpack_value<V: Serialize>(value: &V) -> crate::Result<Vec<u8>> {
    rmp_serde::to_vec_named(value).map_err(|e| crate::Error::WalCorrupted {
        offset: 0,
        reason: format!("msgpack encode: {e}"),
    })
}

/// Encode a value for the WAL v4 versioned envelope.
///
/// Format: `[version: u16 LE][msgpack_named payload]`.
pub fn encode_versioned_value<V: Serialize>(version: u16, value: &V) -> crate::Result<Vec<u8>> {
    let payload = encode_msgpack_value(value)?;
    let mut out = Vec::with_capacity(2 + payload.len());
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Split a WAL v4 versioned value into `(version, payload)`.
///
/// Returns an error if the value is too short to contain a version tag.
pub fn split_versioned_value(bytes: &[u8]) -> crate::Result<(u16, &[u8])> {
    if bytes.len() < 2 {
        return Err(crate::Error::WalCorrupted {
            offset: 0,
            reason: format!(
                "versioned value too short: got {} bytes, need >=2 for version tag",
                bytes.len()
            ),
        });
    }
    let version = u16::from_le_bytes([bytes[0], bytes[1]]);
    Ok((version, &bytes[2..]))
}

/// A type whose state can be reconstructed by replaying WAL ops.
pub trait Replayable: Clone + Send + Sync + 'static {
    /// Apply ops to reconstruct state during WAL replay.
    ///
    /// Default dispatch assumes values use the WAL v4 versioned envelope.
    /// Callers replaying legacy v3 WALs should use `apply_with_format`.
    fn apply(&mut self, ops: &[Op]) -> crate::Result<()> {
        self.apply_with_format(ops, ReplayFormat::Versioned)
    }

    /// Apply ops with explicit format selection (no migration, no quarantine).
    ///
    /// Provided by `#[derive(Replayable)]`. Manual implementations override
    /// this for basic support. Types that need migration/quarantine should
    /// additionally override `apply_with_ctx`.
    fn apply_with_format(&mut self, ops: &[Op], format: ReplayFormat) -> crate::Result<()>;

    /// Apply ops with full replay context (format, migrations, future:
    /// quarantine).
    ///
    /// Provided by `#[derive(Replayable)]`. The default implementation
    /// delegates to `apply_with_format`, which means manual implementations
    /// get basic format dispatch but no migration support.
    fn apply_with_ctx(&mut self, ops: &[Op], ctx: &mut ReplayContext<'_>) -> crate::Result<()> {
        self.apply_with_format(ops, ctx.format)
    }

    /// Migration registry for this state type.
    ///
    /// Override to register migration functions for schema evolution.
    /// Default: empty — values whose stored version differs from current
    /// are quarantined.
    fn migrations() -> MigrationSet {
        MigrationSet::new()
    }

    /// Schema fingerprint: xxh3 hash of the sorted list of
    /// `(collection_id, version)` pairs declared by the state type.
    ///
    /// The derive macro provides a real implementation. Manual impls can
    /// return 0 to opt out of drift detection.
    ///
    /// Purpose: detect "forgot to bump version" when the DB's recorded
    /// fingerprint differs from the current binary's without a migration
    /// for the difference. The backend warns on load if this happens.
    fn schema_fingerprint() -> u64 {
        0
    }

    /// Serialize the current state to a per-value-versioned snapshot
    /// payload (ESNA v3+ format).
    ///
    /// Default returns `SnapshotNotSupported` — `WalBackend` falls back to
    /// legacy postcard ESNA v1/v2 format in that case. The derive macro
    /// provides a real implementation.
    fn to_snapshot(&self) -> crate::Result<SnapshotPayload> {
        Err(crate::Error::SnapshotNotSupported)
    }

    /// Reconstruct state from a per-value-versioned snapshot payload.
    ///
    /// Entries with stored versions below current are migrated through
    /// the registry. Failures are quarantined in `ctx.quarantine` rather
    /// than aborting the load.
    ///
    /// Default returns `SnapshotNotSupported`. Derive macro overrides.
    fn from_snapshot(payload: SnapshotPayload, ctx: &mut ReplayContext<'_>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let _ = (payload, ctx);
        Err(crate::Error::SnapshotNotSupported)
    }

    /// Called after snapshot deserialization and WAL replay.
    ///
    /// Override this to rebuild secondary indexes or derived state
    /// that is `#[serde(skip)]`'d from snapshots.
    fn after_load(&mut self) {}
}

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
            quarantine_value(
                ctx.quarantine,
                collection_id,
                &entry.key,
                &envelope,
                ReplayFormat::Versioned,
                current_version,
                e,
            );
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
pub struct ReplayContext<'a> {
    pub format: ReplayFormat,
    pub migrations: &'a MigrationSet,
    pub quarantine: &'a mut Quarantine,
}

impl<'a> ReplayContext<'a> {
    pub fn new(
        format: ReplayFormat,
        migrations: &'a MigrationSet,
        quarantine: &'a mut Quarantine,
    ) -> Self {
        Self {
            format,
            migrations,
            quarantine,
        }
    }
}

/// Apply a Put or Delete to a `BTreeMap<String, V>`.
///
/// Converts the `Vec<u8>` key to a `String` via UTF-8 validation.
pub fn apply_op<V: DeserializeOwned>(map: &mut BTreeMap<String, V>, op: &Op) -> crate::Result<()> {
    match op {
        Op::Put { key, value, .. } => {
            let k = String::from_utf8(key.clone()).map_err(|e| crate::Error::WalCorrupted {
                offset: 0,
                reason: format!("invalid UTF-8 key: {e}"),
            })?;
            let v: V = postcard::from_bytes(value)?;
            map.insert(k, v);
        }
        Op::Delete { key, .. } => {
            let k = String::from_utf8(key.clone()).map_err(|e| crate::Error::WalCorrupted {
                offset: 0,
                reason: format!("invalid UTF-8 key: {e}"),
            })?;
            map.remove(&k);
        }
    }
    Ok(())
}

/// Apply a Put or Delete to a `BTreeMap<Vec<u8>, V>`.
///
/// No key conversion — uses raw bytes directly.
pub fn apply_op_bytes<V: DeserializeOwned>(
    map: &mut BTreeMap<Vec<u8>, V>,
    op: &Op,
) -> crate::Result<()> {
    match op {
        Op::Put { key, value, .. } => {
            let v: V = postcard::from_bytes(value)?;
            map.insert(key.clone(), v);
        }
        Op::Delete { key, .. } => {
            map.remove(key);
        }
    }
    Ok(())
}

/// Apply a Put or Delete to a `HashMap<String, V>`.
///
/// Converts the `Vec<u8>` key to a `String` via UTF-8 validation.
pub fn apply_op_hash<V: DeserializeOwned>(
    map: &mut std::collections::HashMap<String, V>,
    op: &Op,
) -> crate::Result<()> {
    match op {
        Op::Put { key, value, .. } => {
            let k = String::from_utf8(key.clone()).map_err(|e| crate::Error::WalCorrupted {
                offset: 0,
                reason: format!("invalid UTF-8 key: {e}"),
            })?;
            let v: V = postcard::from_bytes(value)?;
            map.insert(k, v);
        }
        Op::Delete { key, .. } => {
            let k = String::from_utf8(key.clone()).map_err(|e| crate::Error::WalCorrupted {
                offset: 0,
                reason: format!("invalid UTF-8 key: {e}"),
            })?;
            map.remove(&k);
        }
    }
    Ok(())
}

/// Apply a Put or Delete to a `HashMap<Vec<u8>, V>`.
///
/// No key conversion — uses raw bytes directly.
pub fn apply_op_hash_bytes<V: DeserializeOwned>(
    map: &mut std::collections::HashMap<Vec<u8>, V>,
    op: &Op,
) -> crate::Result<()> {
    match op {
        Op::Put { key, value, .. } => {
            let v: V = postcard::from_bytes(value)?;
            map.insert(key.clone(), v);
        }
        Op::Delete { key, .. } => {
            map.remove(key);
        }
    }
    Ok(())
}

/// Apply a Put or Delete to any map via key conversion function.
///
/// Generic helper for custom key types.
pub fn apply_op_with<K, V, F>(
    map: &mut BTreeMap<K, V>,
    op: &Op,
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
            let v: V = postcard::from_bytes(value)?;
            map.insert(k, v);
        }
        Op::Delete { key, .. } => {
            let k = convert_key(key)?;
            map.remove(&k);
        }
    }
    Ok(())
}

/// Apply a Put or Delete to any HashMap via key conversion function.
pub fn apply_op_hash_with<K, V, F>(
    map: &mut std::collections::HashMap<K, V>,
    op: &Op,
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
            let v: V = postcard::from_bytes(value)?;
            map.insert(k, v);
        }
        Op::Delete { key, .. } => {
            let k = convert_key(key)?;
            map.remove(&k);
        }
    }
    Ok(())
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

/// Apply a Put or Delete with full context: format, migrations, quarantine.
///
/// On decode/migration failure, adds the raw value to quarantine and returns
/// `Ok(())` — the database still starts and the value is preserved.
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
                Ok(v) => {
                    let k = convert_key(key)?;
                    // A new successful write supersedes any quarantined entry.
                    ctx.quarantine.remove_key(collection_id, key);
                    map.insert(k, v);
                }
                Err(e) => {
                    quarantine_value(
                        ctx.quarantine,
                        collection_id,
                        key,
                        value,
                        ctx.format,
                        current_version,
                        e,
                    );
                }
            }
        }
        Op::Delete { key, .. } => {
            // Remove from both the live map and quarantine (explicit deletion).
            let k = convert_key(key)?;
            map.remove(&k);
            ctx.quarantine.remove_key(collection_id, key);
        }
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
                Ok(v) => {
                    let k = convert_key(key)?;
                    ctx.quarantine.remove_key(collection_id, key);
                    map.insert(k, v);
                }
                Err(e) => {
                    quarantine_value(
                        ctx.quarantine,
                        collection_id,
                        key,
                        value,
                        ctx.format,
                        current_version,
                        e,
                    );
                }
            }
        }
        Op::Delete { key, .. } => {
            let k = convert_key(key)?;
            map.remove(&k);
            ctx.quarantine.remove_key(collection_id, key);
        }
    }
    Ok(())
}

/// Translate a decode/migration error into a quarantine entry.
fn quarantine_value(
    q: &mut Quarantine,
    collection_id: u8,
    key: &[u8],
    original_bytes: &[u8],
    format: ReplayFormat,
    current_version: u16,
    err: crate::Error,
) {
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
