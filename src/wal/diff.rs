//! Replayable trait and helpers for WAL replay.
//!
//! Types that implement `Replayable` can reconstruct their state from
//! a sequence of WAL ops on startup.

use serde::de::DeserializeOwned;
use std::collections::BTreeMap;

use super::op::Op;

/// A type whose state can be reconstructed by replaying WAL ops.
pub trait Replayable: Clone + Send + Sync + 'static {
    /// Apply ops to reconstruct state during WAL replay.
    fn apply(&mut self, ops: &[Op]) -> crate::Result<()>;
}

/// Apply a Put or Delete to a BTreeMap.
pub fn apply_op<V: DeserializeOwned>(map: &mut BTreeMap<String, V>, op: &Op) -> crate::Result<()> {
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
