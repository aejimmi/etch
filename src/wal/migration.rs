//! Schema migration registry and chain execution.
//!
//! When a stored value's schema version differs from the current version,
//! the migration registry looks up a chain of migration functions that
//! transform bytes from the old format to the new format, one hop at a time.
//!
//! Migration functions work on raw serialized bytes, not typed values —
//! this lets chains compose freely (v0 postcard bytes -> v1 msgpack bytes ->
//! v2 msgpack bytes -> ...).
//!
//! # Example
//!
//! ```ignore
//! let migrations = MigrationSet::new()
//!     .add(3, 0, 1, |bytes| {
//!         // v0 (legacy postcard) -> v1 (msgpack)
//!         let old: OldWorkspace = postcard::from_bytes(bytes)?;
//!         let new = Workspace::from(old);
//!         Ok(rmp_serde::to_vec_named(&new)?)
//!     })
//!     .add(3, 1, 2, |bytes| {
//!         // v1 -> v2 (type change)
//!         let old: WorkspaceV1 = rmp_serde::from_slice(bytes)?;
//!         let new = Workspace::from(old);
//!         Ok(rmp_serde::to_vec_named(&new)?)
//!     });
//! ```

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Error returned by a user-supplied migration function.
///
/// The inner message is user-provided and should describe what went wrong
/// (e.g. "missing required field `foo`", "invalid enum variant `Bar`").
#[derive(Debug, Clone)]
pub struct MigrationError(pub String);

impl MigrationError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MigrationError {}

impl From<postcard::Error> for MigrationError {
    fn from(e: postcard::Error) -> Self {
        Self(format!("postcard: {e}"))
    }
}

impl From<rmp_serde::encode::Error> for MigrationError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self(format!("msgpack encode: {e}"))
    }
}

impl From<rmp_serde::decode::Error> for MigrationError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self(format!("msgpack decode: {e}"))
    }
}

/// A single-hop migration function: old bytes in, new bytes out.
pub type MigrationFn =
    Box<dyn Fn(&[u8]) -> Result<Vec<u8>, MigrationError> + Send + Sync + 'static>;

/// Registry of migration functions, keyed by `(collection_id, from_version)`.
///
/// Each entry handles a single-hop migration from version N to version N+1.
/// Chain execution walks the entries for a given collection from the stored
/// version up to the current version.
#[derive(Default)]
pub struct MigrationSet {
    /// (collection_id, from_version) -> migration to (from_version + 1)
    entries: HashMap<(u8, u16), MigrationFn>,
}

impl MigrationSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a migration from `from_version` to `to_version` for the
    /// given collection.
    ///
    /// # Panics
    ///
    /// Panics if `to_version != from_version + 1`. Migrations must be
    /// single-hop — chains are built by registering a function for each hop.
    pub fn add<F>(mut self, collection: u8, from_version: u16, to_version: u16, f: F) -> Self
    where
        F: Fn(&[u8]) -> Result<Vec<u8>, MigrationError> + Send + Sync + 'static,
    {
        assert_eq!(
            to_version,
            from_version + 1,
            "migrations must be single-hop: got {} -> {}",
            from_version,
            to_version
        );
        self.entries.insert((collection, from_version), Box::new(f));
        self
    }

    /// Look up whether a direct `(from -> from+1)` migration is registered
    /// for this collection.
    pub fn has(&self, collection: u8, from_version: u16) -> bool {
        self.entries.contains_key(&(collection, from_version))
    }

    /// Returns true if at least one migration is registered.
    pub fn is_nonempty(&self) -> bool {
        !self.entries.is_empty()
    }

    /// Number of registered migrations.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if no migrations are registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Run the migration chain from `stored` to `current` version.
    ///
    /// Returns the transformed bytes on success, or:
    /// - `ChainResult::Missing { hop }` if a migration is missing at some hop.
    /// - `ChainResult::Failed { hop, error }` if a migration function returned an error.
    /// - `ChainResult::Panicked { hop, message }` if a migration function panicked.
    ///
    /// Each migration is wrapped in `catch_unwind` so a buggy migration
    /// function cannot crash the process.
    pub fn migrate_chain(
        &self,
        collection: u8,
        stored: u16,
        current: u16,
        bytes: &[u8],
    ) -> ChainResult {
        if stored >= current {
            // Can't migrate forward from an equal or newer version.
            return ChainResult::Missing { hop: stored };
        }

        let mut cur_bytes: Vec<u8> = bytes.to_vec();
        for from in stored..current {
            let Some(f) = self.entries.get(&(collection, from)) else {
                return ChainResult::Missing { hop: from };
            };

            let input = cur_bytes;
            let result = catch_unwind(AssertUnwindSafe(|| f(&input)));
            match result {
                Ok(Ok(next)) => cur_bytes = next,
                Ok(Err(e)) => {
                    return ChainResult::Failed {
                        hop: from,
                        error: e,
                    };
                }
                Err(panic_info) => {
                    let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "migration panicked with non-string payload".to_string()
                    };
                    return ChainResult::Panicked {
                        hop: from,
                        message: msg,
                    };
                }
            }
        }

        ChainResult::Migrated(cur_bytes)
    }
}

impl std::fmt::Debug for MigrationSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationSet")
            .field("entries", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Outcome of `MigrationSet::migrate_chain`.
#[derive(Debug)]
pub enum ChainResult {
    /// All hops succeeded. Contains the final migrated bytes.
    Migrated(Vec<u8>),
    /// No migration registered for `(collection, hop)`.
    Missing { hop: u16 },
    /// Migration function returned an error.
    Failed { hop: u16, error: MigrationError },
    /// Migration function panicked. Message extracted from the panic payload.
    Panicked { hop: u16, message: String },
}

#[cfg(test)]
#[path = "migration_test.rs"]
mod migration_test;
