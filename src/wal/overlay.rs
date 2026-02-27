//! Per-collection overlay for transaction capture.
//!
//! Provides read-your-writes semantics on top of committed state.
//! `get()` checks deletes → puts → committed, avoiding full-state clones.

use std::collections::{BTreeMap, BTreeSet};

/// A lightweight overlay that records puts and deletes against a committed
/// `BTreeMap`. All reads see a merged view; writes only touch the overlay.
pub struct Overlay<V> {
    pub puts: BTreeMap<String, V>,
    pub deletes: BTreeSet<String>,
}

impl<V> Default for Overlay<V> {
    fn default() -> Self {
        Self {
            puts: BTreeMap::new(),
            deletes: BTreeSet::new(),
        }
    }
}

impl<V> Overlay<V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a key with read-your-writes: deletes → puts → committed.
    pub fn get<'a>(&'a self, committed: &'a BTreeMap<String, V>, key: &str) -> Option<&'a V> {
        if self.deletes.contains(key) {
            return None;
        }
        if let Some(v) = self.puts.get(key) {
            return Some(v);
        }
        committed.get(key)
    }

    /// Check if a key exists in the merged view.
    pub fn contains_key(&self, committed: &BTreeMap<String, V>, key: &str) -> bool {
        if self.deletes.contains(key) {
            return false;
        }
        self.puts.contains_key(key) || committed.contains_key(key)
    }

    /// Iterate all values in the merged view.
    ///
    /// Yields committed values (minus deletes, minus overwritten) then overlay puts.
    pub fn values<'a>(&'a self, committed: &'a BTreeMap<String, V>) -> impl Iterator<Item = &'a V> {
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
        committed: &'a BTreeMap<String, V>,
    ) -> impl Iterator<Item = (&'a str, &'a V)> {
        let from_committed = committed.iter().filter_map(move |(k, v)| {
            if self.deletes.contains(k) || self.puts.contains_key(k) {
                None
            } else {
                Some((k.as_str(), v))
            }
        });
        let from_puts = self.puts.iter().map(|(k, v)| (k.as_str(), v));
        from_committed.chain(from_puts)
    }

    /// Record a put (insert or update).
    pub fn put(&mut self, key: String, value: V) {
        self.deletes.remove(&key);
        self.puts.insert(key, value);
    }

    /// Record a delete. Returns true if the key existed in the merged view.
    pub fn delete(&mut self, key: &str, committed: &BTreeMap<String, V>) -> bool {
        let existed = self.puts.remove(key).is_some() || committed.contains_key(key);
        if committed.contains_key(key) {
            self.deletes.insert(key.to_string());
        }
        existed
    }

    /// Retain only entries where `f` returns true.
    ///
    /// Scans committed (minus already deleted) + overlay puts, marking
    /// non-matching entries as deleted and collecting their keys.
    pub fn retain(
        &mut self,
        committed: &BTreeMap<String, V>,
        mut f: impl FnMut(&str, &V) -> bool,
    ) -> Vec<String> {
        let mut removed = Vec::new();

        // Scan committed entries not yet deleted or overwritten.
        for (k, v) in committed {
            if self.deletes.contains(k) || self.puts.contains_key(k) {
                continue;
            }
            if !f(k, v) {
                self.deletes.insert(k.clone());
                removed.push(k.clone());
            }
        }

        // Scan overlay puts.
        let to_remove: Vec<String> = self
            .puts
            .iter()
            .filter(|(k, v)| !f(k, v))
            .map(|(k, _)| k.clone())
            .collect();
        for k in to_remove {
            self.puts.remove(&k);
            // If committed also has this key, add to deletes so it
            // doesn't leak through after the overlay put is removed.
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
