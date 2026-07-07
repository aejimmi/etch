//! Per-collection transaction handle for derive macros.
//!
//! `Collection` wraps an overlay, a committed map reference, and a local ops
//! buffer. It provides typed `get` / `put` / `delete` methods that
//! automatically serialize keys and values into WAL ops.

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::diff::encode_versioned_value;
use super::key::EtchKey;
use super::op::Op;
use super::overlay::{MapRead, Overlay};
use crate::error::Result;

/// A per-collection transaction handle.
///
/// Each annotated field in a `#[derive(Transactable)]` struct becomes a
/// `Collection` on the generated transaction type. Ops are buffered locally
/// and collected by `finish_tx`.
pub struct Collection<'a, K: Ord + Clone, V, M> {
    committed: &'a M,
    overlay: Overlay<K, V>,
    ops: Vec<Op>,
    collection_id: u8,
    schema_version: u16,
}

impl<'a, K, V, M> Collection<'a, K, V, M>
where
    K: EtchKey,
    V: Serialize + DeserializeOwned,
    M: MapRead<K, V>,
{
    /// Create a new collection handle for a transaction.
    ///
    /// `schema_version` is the current version for this collection's value
    /// type. Values written through this handle will carry this version tag
    /// in their WAL envelope, enabling per-value migration on replay.
    pub fn new(committed: &'a M, collection_id: u8, schema_version: u16) -> Self {
        Self {
            committed,
            overlay: Overlay::new(),
            ops: Vec::new(),
            collection_id,
            schema_version,
        }
    }

    /// Look up a key (read-your-writes: checks overlay then committed).
    pub fn get(&self, key: &K) -> Option<&V> {
        self.overlay.get(self.committed, key)
    }

    /// Insert or update a key-value pair.
    ///
    /// Serializes `value` into a versioned WAL envelope. Returns
    /// [`crate::Error`] if serialization fails (e.g. a `Serialize` impl that
    /// errors) instead of panicking, so the enclosing `write` closure can
    /// propagate the failure with `?` into [`crate::Store::write`]'s `Result`.
    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        let encoded = encode_versioned_value(self.schema_version, &value)?;
        self.ops.push(Op::Put {
            collection: self.collection_id,
            key: key.to_bytes(),
            value: encoded,
        });
        self.overlay.put(key, value);
        Ok(())
    }

    /// Delete a key. Returns true if the key existed.
    pub fn delete(&mut self, key: &K) -> bool {
        self.ops.push(Op::Delete {
            collection: self.collection_id,
            key: key.to_bytes(),
        });
        self.overlay.delete(key, self.committed)
    }

    /// Check if a key exists in the merged view.
    pub fn contains(&self, key: &K) -> bool {
        self.overlay.contains_key(self.committed, key)
    }

    /// Iterate all values in the merged view.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.overlay.values(self.committed)
    }

    /// Iterate all (key, value) pairs in the merged view.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.overlay.iter(self.committed)
    }

    /// Consume the collection handle, returning ops and overlay.
    pub fn into_parts(self) -> (Vec<Op>, Overlay<K, V>) {
        (self.ops, self.overlay)
    }
}
