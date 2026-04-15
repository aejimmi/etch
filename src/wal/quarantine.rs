//! Quarantine store for values that cannot be migrated.
//!
//! When a value's stored version cannot be migrated to the current version
//! (missing migration function, migration error, or migration panic), the
//! raw bytes are preserved in quarantine rather than silently dropped.
//!
//! Quarantine entries:
//!
//! - Persist to `quarantine.bin` in the database directory.
//! - Survive snapshot compaction (the quarantine file is independent of
//!   the WAL and snapshot files).
//! - Can be retried via `retry_with_migrations` after the missing migration
//!   is registered in a later release.
//! - Are removed when a new write supersedes the quarantined key.
//! - Are never auto-deleted — only explicit purge removes them.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Filename used to persist quarantine within the database directory.
pub const QUARANTINE_FILE: &str = "quarantine.bin";

/// Magic header for the quarantine file.
const QUARANTINE_MAGIC: &[u8; 4] = b"EQUA";
/// Version of the quarantine file envelope format.
const QUARANTINE_VERSION: u8 = 1;

/// Reason a value ended up in quarantine.
///
/// Stored alongside the raw bytes so the developer can see *why* recovery
/// failed when inspecting the quarantine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QuarantineReason {
    /// Stored version has no migration path to current.
    MissingMigration { from: u16, to: u16 },
    /// Migration function returned an error.
    MigrationFailed { from: u16, to: u16, reason: String },
    /// Migration function panicked.
    MigrationPanicked { from: u16, to: u16, message: String },
    /// Value version is newer than the current binary understands.
    FromFutureVersion { stored: u16, current: u16 },
    /// Decoding produced a corrupted or type-mismatched value.
    DecodeFailed { reason: String },
}

impl std::fmt::Display for QuarantineReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingMigration { from, to } => {
                write!(f, "no migration registered for {from}->{to}")
            }
            Self::MigrationFailed { from, to, reason } => {
                write!(f, "migration {from}->{to} failed: {reason}")
            }
            Self::MigrationPanicked { from, to, message } => {
                write!(f, "migration {from}->{to} panicked: {message}")
            }
            Self::FromFutureVersion { stored, current } => {
                write!(
                    f,
                    "value at version {stored} exceeds current version {current}"
                )
            }
            Self::DecodeFailed { reason } => write!(f, "decode failed: {reason}"),
        }
    }
}

/// A single quarantined entry. The `value` bytes are exactly what was in
/// the WAL or snapshot — no decoding has been performed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QuarantinedEntry {
    pub collection: u8,
    pub key: Vec<u8>,
    pub version: u16,
    pub value: Vec<u8>,
    pub reason: QuarantineReason,
}

impl QuarantinedEntry {
    /// Composite identity for deduplication and lookup.
    pub fn ident(&self) -> (u8, &[u8]) {
        (self.collection, &self.key)
    }
}

/// In-memory quarantine collection with disk persistence.
///
/// Uses postcard internally (not msgpack) because the quarantine envelope
/// format is stable and simple — it stores opaque byte payloads.
#[derive(Debug, Default, Clone)]
pub struct Quarantine {
    entries: Vec<QuarantinedEntry>,
}

impl Quarantine {
    /// Create an empty quarantine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of quarantined entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Borrow all entries for inspection.
    pub fn entries(&self) -> &[QuarantinedEntry] {
        &self.entries
    }

    /// Add an entry. If an entry with the same `(collection, key)` already
    /// exists, it is replaced (the newer quarantine reason wins).
    pub fn insert(&mut self, entry: QuarantinedEntry) {
        let ident = entry.ident();
        if let Some(existing) = self
            .entries
            .iter_mut()
            .find(|e| e.collection == ident.0 && e.key == ident.1)
        {
            *existing = entry;
        } else {
            self.entries.push(entry);
        }
    }

    /// Remove entries matching the predicate. Returns how many were removed.
    pub fn remove_where<F>(&mut self, mut f: F) -> usize
    where
        F: FnMut(&QuarantinedEntry) -> bool,
    {
        let before = self.entries.len();
        self.entries.retain(|e| !f(e));
        before - self.entries.len()
    }

    /// Remove the entry for a given `(collection, key)`. Returns true if
    /// something was removed. Called when a normal write supersedes a
    /// quarantined value.
    pub fn remove_key(&mut self, collection: u8, key: &[u8]) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|e| !(e.collection == collection && e.key == key));
        self.entries.len() < before
    }

    /// Drop all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Load quarantine from disk. Returns an empty quarantine if the file
    /// does not exist.
    pub fn load(dir: &Path) -> Result<Self> {
        let path = dir.join(QUARANTINE_FILE);
        if !path.exists() {
            return Ok(Self::default());
        }

        let bytes = std::fs::read(&path)?;
        if bytes.len() < 5 {
            return Err(Error::QuarantineCorrupted {
                reason: "file too short for envelope".into(),
            });
        }

        if &bytes[..4] != QUARANTINE_MAGIC {
            return Err(Error::QuarantineCorrupted {
                reason: format!(
                    "bad magic: expected {:?}, got {:?}",
                    QUARANTINE_MAGIC,
                    &bytes[..4]
                ),
            });
        }

        let version = bytes[4];
        if version != QUARANTINE_VERSION {
            return Err(Error::QuarantineCorrupted {
                reason: format!("unsupported version {version}, expected {QUARANTINE_VERSION}"),
            });
        }

        let payload = &bytes[5..];
        let entries: Vec<QuarantinedEntry> =
            postcard::from_bytes(payload).map_err(|e| Error::QuarantineCorrupted {
                reason: format!("decode: {e}"),
            })?;

        Ok(Self { entries })
    }

    /// Persist quarantine to disk. Writes atomically via `.tmp` + rename.
    pub fn save(&self, dir: &Path) -> Result<()> {
        let path = dir.join(QUARANTINE_FILE);
        let tmp = path.with_extension("tmp");

        // Empty quarantine → remove the file rather than writing a zero-entry blob.
        if self.entries.is_empty() {
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            return Ok(());
        }

        let payload = postcard::to_allocvec(&self.entries)?;
        let mut bytes = Vec::with_capacity(5 + payload.len());
        bytes.extend_from_slice(QUARANTINE_MAGIC);
        bytes.push(QUARANTINE_VERSION);
        bytes.extend_from_slice(&payload);

        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &path)?;

        // Best-effort directory fsync on unix for durability.
        #[cfg(unix)]
        {
            if let Ok(dir_file) = std::fs::File::open(dir) {
                let _ = dir_file.sync_all();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "quarantine_test.rs"]
mod quarantine_test;
