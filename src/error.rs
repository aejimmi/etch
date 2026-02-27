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
