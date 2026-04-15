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

    /// Returns the key bytes of this op.
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Put { key, .. } | Op::Delete { key, .. } => key,
        }
    }

    /// Returns a human-readable tag for this op's kind.
    pub fn kind(&self) -> &'static str {
        match self {
            Op::Put { .. } => "PUT",
            Op::Delete { .. } => "DELETE",
        }
    }
}

/// Format an op's key for diagnostic output.
///
/// Returns UTF-8 lossy decoding if the key is valid UTF-8 without control
/// characters, otherwise a hex-encoded prefix. Truncates to 32 bytes with
/// an ellipsis suffix for longer keys.
pub fn format_op_key(op: &Op) -> String {
    const MAX_LEN: usize = 32;
    let key = op.key();
    let truncated = key.len() > MAX_LEN;
    let slice = if truncated { &key[..MAX_LEN] } else { key };

    let is_printable = std::str::from_utf8(slice)
        .map(|s| s.chars().all(|c| !c.is_control()))
        .unwrap_or(false);

    if is_printable {
        let s = std::str::from_utf8(slice).unwrap();
        if truncated {
            format!("\"{}...\"", s)
        } else {
            format!("\"{}\"", s)
        }
    } else {
        let hex: String = slice.iter().map(|b| format!("{:02x}", b)).collect();
        if truncated {
            format!("0x{}...", hex)
        } else {
            format!("0x{}", hex)
        }
    }
}

#[cfg(test)]
mod format_key_tests {
    use super::*;

    fn put(key: &[u8]) -> Op {
        Op::Put {
            collection: 0,
            key: key.to_vec(),
            value: vec![],
        }
    }

    #[test]
    fn printable_utf8_is_quoted() {
        assert_eq!(format_op_key(&put(b"hello")), "\"hello\"");
    }

    #[test]
    fn long_printable_truncates_with_ellipsis() {
        let key = "a".repeat(40);
        let out = format_op_key(&put(key.as_bytes()));
        assert_eq!(out, format!("\"{}...\"", "a".repeat(32)));
    }

    #[test]
    fn binary_key_hex_encoded() {
        assert_eq!(format_op_key(&put(&[0xde, 0xad, 0xbe, 0xef])), "0xdeadbeef");
    }

    #[test]
    fn long_binary_key_truncates() {
        let key = vec![0xffu8; 40];
        let out = format_op_key(&put(&key));
        assert!(out.starts_with("0x"));
        assert!(out.ends_with("..."));
        assert_eq!(out.len(), 2 + 64 + 3);
    }

    #[test]
    fn control_chars_fall_back_to_hex() {
        assert_eq!(format_op_key(&put(b"a\x00b")), "0x610062");
    }
}
