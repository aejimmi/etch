//! WAL operation types — the minimal diff unit.

use serde::{Deserialize, Serialize};

/// A single WAL operation representing one key mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Op {
    /// Insert or replace a value. `value` is postcard-serialized model bytes.
    Put {
        collection: u8,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Remove a key.
    Delete { collection: u8, key: Vec<u8> },
}

impl Op {
    /// Returns the collection id for this op.
    pub fn collection(&self) -> u8 {
        match self {
            Op::Put { collection, .. } | Op::Delete { collection, .. } => *collection,
        }
    }
}
