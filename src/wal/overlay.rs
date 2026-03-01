//! Per-collection overlay for transaction capture.
//!
//! Provides read-your-writes semantics on top of committed state.
//! `get()` checks deletes → puts → committed, avoiding full-state clones.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;

/// Trait abstracting read access to a committed collection.
///
/// Implemented for both `BTreeMap` and `HashMap`, allowing overlays to
/// work with either backing store. Monomorphization means zero overhead.
pub trait MapRead<K, V> {
    fn get(&self, key: &K) -> Option<&V>;
    fn contains_key(&self, key: &K) -> bool;
    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a;
}

impl<K: Ord, V> MapRead<K, V> for BTreeMap<K, V> {
    fn get(&self, key: &K) -> Option<&V> {
        BTreeMap::get(self, key)
    }

    fn contains_key(&self, key: &K) -> bool {
        BTreeMap::contains_key(self, key)
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        BTreeMap::iter(self)
    }
}

impl<K: Eq + Hash, V> MapRead<K, V> for HashMap<K, V> {
    fn get(&self, key: &K) -> Option<&V> {
        HashMap::get(self, key)
    }

    fn contains_key(&self, key: &K) -> bool {
        HashMap::contains_key(self, key)
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        HashMap::iter(self)
    }
}

/// A lightweight overlay that records puts and deletes against a committed
/// map. All reads see a merged view; writes only touch the overlay.
pub struct Overlay<K: Ord + Clone, V> {
    pub puts: BTreeMap<K, V>,
    pub deletes: BTreeSet<K>,
}

impl<K: Ord + Clone, V> Default for Overlay<K, V> {
    fn default() -> Self {
        Self {
            puts: BTreeMap::new(),
            deletes: BTreeSet::new(),
        }
    }
}

impl<K: Ord + Clone, V> Overlay<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a key with read-your-writes: deletes → puts → committed.
    pub fn get<'a>(&'a self, committed: &'a impl MapRead<K, V>, key: &K) -> Option<&'a V> {
        if self.deletes.contains(key) {
            return None;
        }
        if let Some(v) = self.puts.get(key) {
            return Some(v);
        }
        committed.get(key)
    }

    /// Check if a key exists in the merged view.
    pub fn contains_key(&self, committed: &impl MapRead<K, V>, key: &K) -> bool {
        if self.deletes.contains(key) {
            return false;
        }
        self.puts.contains_key(key) || committed.contains_key(key)
    }

    /// Iterate all values in the merged view.
    ///
    /// Yields committed values (minus deletes, minus overwritten) then overlay puts.
    pub fn values<'a>(&'a self, committed: &'a impl MapRead<K, V>) -> impl Iterator<Item = &'a V> {
        let from_committed = committed.iter().filter_map(move |(k, v)| {
            if self.deletes.contains(k) || self.puts.contains_key(k) {
                None
            } else {
                Some(v)
            }
        });
        let from_puts = self.puts.values();
        from_committed.chain(from_puts)
    }

    /// Iterate all (key, value) pairs in the merged view.
    pub fn iter<'a>(
        &'a self,
        committed: &'a impl MapRead<K, V>,
    ) -> impl Iterator<Item = (&'a K, &'a V)> {
        let from_committed = committed.iter().filter_map(move |(k, v)| {
            if self.deletes.contains(k) || self.puts.contains_key(k) {
                None
            } else {
                Some((k, v))
            }
        });
        let from_puts = self.puts.iter();
        from_committed.chain(from_puts)
    }

    /// Record a put (insert or update).
    pub fn put(&mut self, key: K, value: V) {
        self.deletes.remove(&key);
        self.puts.insert(key, value);
    }

    /// Record a delete. Returns true if the key existed in the merged view.
    pub fn delete(&mut self, key: &K, committed: &impl MapRead<K, V>) -> bool {
        let existed = self.puts.remove(key).is_some() || committed.contains_key(key);
        if committed.contains_key(key) {
            self.deletes.insert(key.clone());
        }
        existed
    }

    /// Retain only entries where `f` returns true.
    ///
    /// Scans committed (minus already deleted) + overlay puts, marking
    /// non-matching entries as deleted and collecting their keys.
    pub fn retain(
        &mut self,
        committed: &impl MapRead<K, V>,
        mut f: impl FnMut(&K, &V) -> bool,
    ) -> Vec<K> {
        let mut removed = Vec::new();

        // Scan committed entries not yet deleted or overwritten.
        for (k, v) in committed.iter() {
            if self.deletes.contains(k) || self.puts.contains_key(k) {
                continue;
            }
            if !f(k, v) {
                self.deletes.insert(k.clone());
                removed.push(k.clone());
            }
        }

        // Scan overlay puts.
        let to_remove: Vec<K> = self
            .puts
            .iter()
            .filter(|(k, v)| !f(k, v))
            .map(|(k, _)| k.clone())
            .collect();
        for k in to_remove {
            self.puts.remove(&k);
            if committed.contains_key(&k) {
                self.deletes.insert(k.clone());
            }
            removed.push(k);
        }

        removed
    }

    /// Returns true if the overlay has no changes.
    pub fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.deletes.is_empty()
    }
}
