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

mod replay;

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::hash::Hash;

use super::migration::MigrationSet;
use super::op::Op;
use super::snapshot::SnapshotPayload;

pub use replay::{
    ReplayContext, apply_op_versioned_hash_with, apply_op_versioned_hash_with_ctx,
    apply_op_versioned_with, apply_op_versioned_with_ctx, load_snapshot_entry,
};

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
