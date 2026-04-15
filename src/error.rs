//! Error types for etch.

use thiserror::Error;

/// Store and persistence errors.
#[derive(Debug, Error)]
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
}

pub type Result<T> = std::result::Result<T, Error>;
