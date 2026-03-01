//! Per-collection transaction handle for derive macros.
//!
//! `Collection` wraps an overlay, a committed map reference, and a local ops
//! buffer. It provides typed `get` / `put` / `delete` methods that
//! automatically serialize keys and values into WAL ops.

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::key::EtchKey;
use super::op::Op;
use super::overlay::{MapRead, Overlay};

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
}

impl<'a, K, V, M> Collection<'a, K, V, M>
where
    K: EtchKey,
    V: Serialize + DeserializeOwned,
    M: MapRead<K, V>,
{
    /// Create a new collection handle for a transaction.
    pub fn new(committed: &'a M, collection_id: u8) -> Self {
        Self {
            committed,
            overlay: Overlay::new(),
            ops: Vec::new(),
            collection_id,
        }
    }

    /// Look up a key (read-your-writes: checks overlay then committed).
    pub fn get(&self, key: &K) -> Option<&V> {
        self.overlay.get(self.committed, key)
    }

    /// Insert or update a key-value pair.
    pub fn put(&mut self, key: K, value: V) {
        self.ops.push(Op::Put {
            collection: self.collection_id,
            key: key.to_bytes(),
            value: postcard::to_allocvec(&value).expect("serialize"),
        });
        self.overlay.put(key, value);
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
