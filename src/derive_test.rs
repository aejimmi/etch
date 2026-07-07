//! Tests for etchdb-derive macros, Collection, and Op helpers.
//!
//! Shared state-type fixtures live here; the scenario tests that exercise
//! them are split across the child modules below.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use crate::wal::Op;
use crate::{Replayable, Transactable};

#[path = "derive_test/collection.rs"]
mod collection;
#[path = "derive_test/crud.rs"]
mod crud;
#[path = "derive_test/keys.rs"]
mod keys;
#[path = "derive_test/migration.rs"]
mod migration;
#[path = "derive_test/quarantine.rs"]
mod quarantine;
#[path = "derive_test/schema.rs"]
mod schema;

// ---- BTreeMap-based state (most common) ----

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct User {
    name: String,
    email: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct AppState {
    #[etch(collection = 0)]
    users: BTreeMap<String, User>,
    #[etch(collection = 1)]
    counters: BTreeMap<String, u32>,
}

// ---- HashMap-based state ----

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct HashState {
    #[etch(collection = 0)]
    items: HashMap<String, String>,
}

// ---- Mixed BTreeMap + HashMap ----

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct MixedState {
    #[etch(collection = 0)]
    ordered: BTreeMap<String, String>,
    #[etch(collection = 1)]
    fast: HashMap<String, u64>,
}

// ---- Non-String key types ----

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct IntKeyState {
    #[etch(collection = 0)]
    items: BTreeMap<u64, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct HashIntKeyState {
    #[etch(collection = 0)]
    items: HashMap<u32, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct MixedKeyState {
    #[etch(collection = 0)]
    by_name: BTreeMap<String, String>,
    #[etch(collection = 1)]
    by_id: BTreeMap<u64, String>,
}

// ---- Schema evolution tests ----

/// "Old" struct — what was deployed before.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ItemV1 {
    name: String,
    count: u32,
}

/// "New" struct — added optional fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ItemV2 {
    name: String,
    count: u32,
    label: Option<String>,
    description: Option<String>,
}

/// State type using the v2 struct.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct SchemaState {
    #[etch(collection = 0)]
    items: BTreeMap<String, ItemV2>,
}

// ---- Compacted v1 data upgraded to v2 via snapshot migration ----
//
// The killer scenario: data is written with schema v1, a snapshot is
// forced (so WAL is empty), then we reopen with schema v2 and a migration.
// If per-value snapshot versioning works, the migrated value is present.
// If it doesn't work (the failure mode the old design had), the value is
// silently lost on first compaction.

/// v1 shape of the item type. Separate state type so we can write with v1
/// semantics, close, and reopen with v2.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct CompactV1Item {
    payload: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct CompactStateV1 {
    #[etch(collection = 0, version = 1)]
    items: BTreeMap<String, CompactV1Item>,
}

/// v2 shape: renamed `payload` to `body` and added a numeric `weight`.
/// The rename means msgpack-named auto-decode would drop the old value
/// for `payload` — a migration is required to preserve it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct CompactV2Item {
    #[serde(default)]
    body: String,
    #[serde(default)]
    weight: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Transactable)]
struct CompactStateV2 {
    #[etch(collection = 0, version = 2)]
    items: BTreeMap<String, CompactV2Item>,
}

/// Manual Replayable impl so we can register the migration.
impl crate::Replayable for CompactStateV2 {
    fn apply_with_format(&mut self, ops: &[Op], format: crate::ReplayFormat) -> crate::Result<()> {
        for op in ops {
            if op.collection() == 0 {
                let _ =
                    crate::wal::apply_op_versioned_with(&mut self.items, op, format, 2, |bytes| {
                        <String as crate::EtchKey>::from_bytes(bytes)
                    });
            }
        }
        Ok(())
    }

    fn apply_with_ctx(
        &mut self,
        ops: &[Op],
        ctx: &mut crate::ReplayContext<'_>,
    ) -> crate::Result<()> {
        for op in ops {
            if op.collection() == 0 {
                let _ = crate::wal::apply_op_versioned_with_ctx(
                    &mut self.items,
                    op,
                    0,
                    2,
                    ctx,
                    <String as crate::EtchKey>::from_bytes,
                );
            }
        }
        Ok(())
    }

    fn migrations() -> crate::MigrationSet {
        crate::MigrationSet::new().add(0, 1, 2, |bytes| {
            let old: CompactV1Item = rmp_serde::from_slice(bytes)?;
            let new = CompactV2Item {
                body: old.payload,
                weight: 0,
            };
            Ok(rmp_serde::to_vec_named(&new)?)
        })
    }

    fn to_snapshot(&self) -> crate::Result<crate::wal::SnapshotPayload> {
        let mut entries = Vec::new();
        for (k, v) in &self.items {
            entries.push(crate::wal::SnapshotEntry {
                key: crate::EtchKey::to_bytes(k),
                version: 2,
                value: crate::wal::encode_msgpack_value(v)?,
            });
        }
        Ok(crate::wal::SnapshotPayload {
            schema_fingerprint: 0,
            collections: vec![crate::wal::CollectionSection {
                collection_id: 0,
                current_version: 2,
                entries,
            }],
        })
    }

    fn from_snapshot(
        payload: crate::wal::SnapshotPayload,
        ctx: &mut crate::ReplayContext<'_>,
    ) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state = Self::default();
        for section in &payload.collections {
            if section.collection_id == 0 {
                for entry in &section.entries {
                    if let Some(v) =
                        crate::wal::load_snapshot_entry::<CompactV2Item>(entry, 0, 2, ctx)
                        && let Ok(k) = <String as crate::EtchKey>::from_bytes(&entry.key)
                    {
                        state.items.insert(k, v);
                    }
                }
            }
        }
        Ok(state)
    }
}

// ---- Migration: v1 -> v2 single hop ----

/// v1 item: just name and count.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigItemV1 {
    name: String,
    count: u32,
}

/// v2 item: added `label` and `priority` fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct MigItemV2 {
    name: String,
    count: u32,
    label: Option<String>,
    priority: u8,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct MigStateV2 {
    #[etch(collection = 0, version = 2)]
    items: BTreeMap<String, MigItemV2>,
}

// ---- Migration: v1 -> v3 chain (two hops) ----

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct MigItemV3 {
    name: String,
    count: u32,
    label: Option<String>,
    priority: u8,
    /// v3 added a `tags` field.
    tags: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct MigStateV3 {
    #[etch(collection = 0, version = 3)]
    items: BTreeMap<String, MigItemV3>,
}
