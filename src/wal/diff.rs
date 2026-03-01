//! Replayable trait and helpers for WAL replay.
//!
//! Types that implement `Replayable` can reconstruct their state from
//! a sequence of WAL ops on startup.

use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::hash::Hash;

use super::op::Op;

/// A type whose state can be reconstructed by replaying WAL ops.
pub trait Replayable: Clone + Send + Sync + 'static {
    /// Apply ops to reconstruct state during WAL replay.
    fn apply(&mut self, ops: &[Op]) -> crate::Result<()>;

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
