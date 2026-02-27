//! WAL operation types — the minimal diff unit.

use serde::{Deserialize, Serialize};

/// A single WAL operation representing one key mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Op {
    /// Insert or replace a value. `value` is postcard-serialized model bytes.
    Put {
        collection: u8,
        key: String,
        value: Vec<u8>,
    },
    /// Remove a key.
    Delete { collection: u8, key: String },
}
