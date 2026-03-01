//! Merge transaction overlays into committed state.
//!
//! After a transaction commits, its overlay is applied to the in-memory
//! state in O(changed keys) — no full-state clone needed.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use super::Op;
use super::overlay::Overlay;

/// A type that supports zero-clone transaction capture.
pub trait Transactable: Clone + Send + Sync + 'static {
    type Tx<'a>
    where
        Self: 'a;
    type Overlay;

    fn begin_tx(&self) -> Self::Tx<'_>;
    fn finish_tx(tx: Self::Tx<'_>) -> (Vec<Op>, Self::Overlay);
    fn apply_overlay(&mut self, overlay: Self::Overlay);
}

/// Apply a transaction overlay to a committed BTreeMap.
pub fn apply_overlay_btree<K: Ord + Clone, V>(map: &mut BTreeMap<K, V>, overlay: Overlay<K, V>) {
    for key in overlay.deletes {
        map.remove(&key);
    }
    for (key, value) in overlay.puts {
        map.insert(key, value);
    }
}

/// Apply a transaction overlay to a committed HashMap.
pub fn apply_overlay_hash<K: Eq + Hash + Ord + Clone, V>(
    map: &mut HashMap<K, V>,
    overlay: Overlay<K, V>,
) {
    for key in overlay.deletes {
        map.remove(&key);
    }
    for (key, value) in overlay.puts {
        map.insert(key, value);
    }
}
