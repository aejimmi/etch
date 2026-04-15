//! Structured snapshot format with per-value schema versioning.
//!
//! Unlike the legacy ESNA v1/v2 format (which serializes the whole state
//! with postcard as one opaque blob), the v3+ format preserves per-value
//! version tags so individual values can be migrated on load — matching
//! the versioning discipline already applied to WAL ops.
//!
//! # Layout
//!
//! ```text
//! ESNA (4 bytes) | format_version (1 byte) | payload
//! ```
//!
//! For `format_version >= 3`, `payload` is `rmp_serde::to_vec_named(SnapshotPayload)`
//! (optionally zstd-compressed for v4). This envelope itself is
//! forward-compatible via msgpack-named semantics: new fields can be added
//! to `SnapshotPayload` with `#[serde(default)]` without breaking older
//! readers.
//!
//! Inside `SnapshotPayload`, each collection section carries its own
//! `current_version`. Each entry within a section carries the version at
//! which its value was written. The load path dispatches per-entry:
//! `stored_version == current` → direct decode; otherwise run migration
//! chain; on failure, quarantine the raw bytes.

use serde::{Deserialize, Serialize};

/// Top-level snapshot payload.
///
/// Serialized with `rmp_serde::to_vec_named` so fields can be added over
/// time without breaking older readers.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SnapshotPayload {
    /// Schema fingerprint at the time the snapshot was written. Derived
    /// from the sorted list of `(collection_id, version)` pairs. On read,
    /// mismatch with the current binary's fingerprint triggers a drift
    /// warning (useful for catching "bumped struct, forgot to bump
    /// version" bugs).
    #[serde(default)]
    pub schema_fingerprint: u64,

    /// Per-collection sections. Order matches declaration order in the
    /// derived state type, but readers should key by `collection_id`.
    #[serde(default)]
    pub collections: Vec<CollectionSection>,
}

/// One collection's contribution to the snapshot.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CollectionSection {
    /// Collection id declared by `#[etch(collection = N)]`.
    pub collection_id: u8,
    /// Schema version at the time the snapshot was written. Used only
    /// diagnostically — each entry also carries its own `version` tag,
    /// and that is what drives migration dispatch.
    pub current_version: u16,
    /// The entries in this collection, each independently versioned.
    #[serde(default)]
    pub entries: Vec<SnapshotEntry>,
}

/// A single key-value entry with an inline schema version.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SnapshotEntry {
    /// Serialized key bytes (via `EtchKey::to_bytes`).
    pub key: Vec<u8>,
    /// Schema version at which `value` was encoded.
    pub version: u16,
    /// Value bytes — msgpack_named payload (no version prefix; the
    /// prefix lives in the `version` field above).
    pub value: Vec<u8>,
}
