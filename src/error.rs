//! Error types for etch.

use std::time::Duration;
use thiserror::Error;

/// Store and persistence errors.
///
/// Marked `#[non_exhaustive]`: this is a 0.x library and the variant set
/// will keep growing, so downstream `match` statements must include a
/// wildcard arm and stay forward-compatible across releases.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("postcard error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },

    #[error("{entity} already exists: {id}")]
    AlreadyExists { entity: &'static str, id: String },

    #[error("invalid {field}: {message}")]
    Invalid {
        field: &'static str,
        message: String,
    },

    #[error("WAL corrupted at offset {offset}: {reason}")]
    WalCorrupted { offset: u64, reason: String },

    #[error("snapshot version mismatch: got {version}, expected {expected}")]
    SnapshotVersion { version: u8, expected: u8 },

    #[error("schema version mismatch: stored={stored}, current={current}")]
    SchemaVersionMismatch { stored: u16, current: u16 },

    /// A strict load (`Store::open_wal_strict`) hit recoverable data loss —
    /// values were skipped, quarantined, or the snapshot was discarded.
    ///
    /// The lenient load paths (`Store::open_wal`, `Store::open_wal_with_report`)
    /// never raise this: they come up with a partial state and expose the
    /// detail through [`crate::ReplayReport`]. `summary` is a one-line digest
    /// of that report.
    #[error("replay incurred data loss: {summary}")]
    ReplayLoss {
        /// One-line digest of the [`crate::ReplayReport`] that triggered the abort.
        summary: String,
    },

    #[error("migration missing: collection {collection}, {from_version}->{to_version}")]
    MigrationMissing {
        collection: u8,
        from_version: u16,
        to_version: u16,
    },

    #[error("migration failed: collection {collection}, {from_version}->{to_version}: {reason}")]
    MigrationFailed {
        collection: u8,
        from_version: u16,
        to_version: u16,
        reason: String,
    },

    #[error("migration panicked: collection {collection}, {from_version}->{to_version}: {message}")]
    MigrationPanicked {
        collection: u8,
        from_version: u16,
        to_version: u16,
        message: String,
    },

    #[error("quarantine file corrupted: {reason}")]
    QuarantineCorrupted { reason: String },

    #[error("snapshot not supported: type does not implement versioned snapshot format")]
    SnapshotNotSupported,

    #[error(
        "database at {dir} is already open by another process (holding exclusive lock on .lock)"
    )]
    DatabaseLocked { dir: String },

    /// [`crate::Store::checkpoint_to`] was called on a store whose backend
    /// cannot produce a consistent on-disk copy — an in-memory
    /// [`crate::Store::memory`], a store built on a custom [`crate::Backend`],
    /// or any backend that does not implement the checkpoint hook.
    #[error("checkpoint is only supported on a WAL-backed store")]
    CheckpointUnsupported,

    /// The checkpoint destination already holds a store file. A checkpoint
    /// never merges into an existing store; nothing was written.
    #[error("checkpoint destination {dir} already contains a store file ({file})")]
    CheckpointDestNotEmpty {
        /// Destination directory that was rejected.
        dir: String,
        /// Name of the pre-existing store file that caused the rejection.
        file: String,
    },

    /// The checkpoint destination is structurally unusable — it is the source
    /// directory, nested inside it (or contains it), or is not a directory.
    #[error("invalid checkpoint destination {dir}: {reason}")]
    CheckpointDestInvalid {
        /// Destination directory that was rejected.
        dir: String,
        /// Why the destination cannot be used.
        reason: String,
    },

    /// A lock acquisition exceeded the deadlock-detection budget.
    ///
    /// Returned by the `Result`-bearing write paths (`write`,
    /// `write_durable`, `retry_quarantine`). `Store::read` cannot return
    /// this — it yields a bare guard and panics on timeout instead.
    #[error("lock timed out after {timeout_ms} ms at '{site}' — likely a deadlock: {cause}")]
    LockTimeout {
        /// Call-site label identifying which acquisition timed out.
        site: &'static str,
        /// Configured deadlock budget, in milliseconds.
        timeout_ms: u64,
        /// Human-readable likely cause for this call site.
        cause: &'static str,
    },
}

impl Error {
    pub fn not_found(entity: &'static str, id: impl Into<String>) -> Self {
        Self::NotFound {
            entity,
            id: id.into(),
        }
    }

    pub fn already_exists(entity: &'static str, id: impl Into<String>) -> Self {
        Self::AlreadyExists {
            entity,
            id: id.into(),
        }
    }

    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::Invalid {
            field,
            message: message.into(),
        }
    }

    /// Build a [`Error::LockTimeout`] from a call site, the configured
    /// budget, and a likely-cause description. The budget is reported in
    /// whole milliseconds (sub-millisecond configured values round down;
    /// the effective wait is floored separately in the store).
    #[must_use]
    pub fn lock_timeout(site: &'static str, timeout: Duration, cause: &'static str) -> Self {
        Self::LockTimeout {
            site,
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            cause,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
