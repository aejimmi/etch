//! Tests for etchdb-derive macros, Collection, and Op helpers.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use crate::store::Store;
use crate::wal::{Collection, Op, WalBackend};
use crate::{Replayable, Transactable};

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

#[test]
fn derive_btree_write_read() {
    let store = Store::<AppState>::memory();

    store
        .write(|tx| {
            tx.users.put(
                "alice".into(),
                User {
                    name: "Alice".into(),
                    email: "alice@example.com".into(),
                },
            );
            tx.counters.put("logins".into(), 1);
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.users.get("alice").unwrap().name, "Alice");
    assert_eq!(*state.counters.get("logins").unwrap(), 1);
}

#[test]
fn derive_btree_get_in_tx() {
    let store = Store::<AppState>::memory();

    store
        .write(|tx| {
            tx.users.put(
                "bob".into(),
                User {
                    name: "Bob".into(),
                    email: "bob@example.com".into(),
                },
            );
            // Read-your-writes: should see the value we just put.
            let bob = tx.users.get(&"bob".into()).unwrap();
            assert_eq!(bob.name, "Bob");
            Ok(())
        })
        .unwrap();
}

#[test]
fn derive_btree_delete() {
    let store = Store::<AppState>::memory();

    store
        .write(|tx| {
            tx.users.put(
                "carol".into(),
                User {
                    name: "Carol".into(),
                    email: "carol@example.com".into(),
                },
            );
            Ok(())
        })
        .unwrap();

    store
        .write(|tx| {
            let existed = tx.users.delete(&"carol".into());
            assert!(existed);
            assert!(tx.users.get(&"carol".into()).is_none());
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert!(!state.users.contains_key("carol"));
}

#[test]
fn derive_btree_contains() {
    let store = Store::<AppState>::memory();

    store
        .write(|tx| {
            tx.users.put(
                "dave".into(),
                User {
                    name: "Dave".into(),
                    email: "dave@example.com".into(),
                },
            );
            assert!(tx.users.contains(&"dave".into()));
            assert!(!tx.users.contains(&"nobody".into()));
            Ok(())
        })
        .unwrap();
}

#[test]
fn derive_btree_multi_collection_tx() {
    let store = Store::<AppState>::memory();

    store
        .write(|tx| {
            tx.users.put(
                "eve".into(),
                User {
                    name: "Eve".into(),
                    email: "eve@example.com".into(),
                },
            );
            tx.counters.put("signups".into(), 42);
            // Can read from one collection while writing to another.
            let count = tx.counters.get(&"signups".into()).copied().unwrap_or(0);
            assert_eq!(count, 42);
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.users.len(), 1);
    assert_eq!(*state.counters.get("signups").unwrap(), 42);
}

#[test]
fn derive_btree_wal_persistence() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::<AppState, WalBackend<AppState>>::open_wal(dir.path().into()).unwrap();
        store
            .write(|tx| {
                tx.users.put(
                    "frank".into(),
                    User {
                        name: "Frank".into(),
                        email: "frank@example.com".into(),
                    },
                );
                Ok(())
            })
            .unwrap();
    }

    // Reopen and verify state was replayed.
    let store = Store::<AppState, WalBackend<AppState>>::open_wal(dir.path().into()).unwrap();
    let state = store.read();
    assert_eq!(state.users.get("frank").unwrap().name, "Frank");
}

// ---- HashMap-based state ----

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct HashState {
    #[etch(collection = 0)]
    items: HashMap<String, String>,
}

#[test]
fn derive_hashmap_write_read() {
    let store = Store::<HashState>::memory();

    store
        .write(|tx| {
            tx.items.put("key1".into(), "value1".into());
            tx.items.put("key2".into(), "value2".into());
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get("key1").unwrap(), "value1");
    assert_eq!(state.items.get("key2").unwrap(), "value2");
}

#[test]
fn derive_hashmap_delete() {
    let store = Store::<HashState>::memory();

    store
        .write(|tx| {
            tx.items.put("gone".into(), "soon".into());
            Ok(())
        })
        .unwrap();

    store
        .write(|tx| {
            tx.items.delete(&"gone".into());
            assert!(tx.items.get(&"gone".into()).is_none());
            Ok(())
        })
        .unwrap();

    assert!(!store.read().items.contains_key("gone"));
}

// ---- Mixed BTreeMap + HashMap ----

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct MixedState {
    #[etch(collection = 0)]
    ordered: BTreeMap<String, String>,
    #[etch(collection = 1)]
    fast: HashMap<String, u64>,
}

#[test]
fn derive_mixed_collections() {
    let store = Store::<MixedState>::memory();

    store
        .write(|tx| {
            tx.ordered.put("a".into(), "alpha".into());
            tx.fast.put("x".into(), 99);
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.ordered.get("a").unwrap(), "alpha");
    assert_eq!(*state.fast.get("x").unwrap(), 99);
}

#[test]
fn derive_iter_values() {
    let store = Store::<AppState>::memory();

    store
        .write(|tx| {
            tx.users.put(
                "a".into(),
                User {
                    name: "A".into(),
                    email: "a@a.com".into(),
                },
            );
            tx.users.put(
                "b".into(),
                User {
                    name: "B".into(),
                    email: "b@b.com".into(),
                },
            );
            let names: Vec<&str> = tx.users.values().map(|u| u.name.as_str()).collect();
            assert_eq!(names.len(), 2);
            Ok(())
        })
        .unwrap();
}

// ---- Op::collection() ----

#[test]
fn op_collection_put() {
    let op = Op::Put {
        collection: 7,
        key: b"k".to_vec(),
        value: vec![],
    };
    assert_eq!(op.collection(), 7);
}

#[test]
fn op_collection_delete() {
    let op = Op::Delete {
        collection: 3,
        key: b"k".to_vec(),
    };
    assert_eq!(op.collection(), 3);
}

// ---- Collection::into_parts ----

#[test]
fn collection_into_parts_returns_ops_and_overlay() {
    let committed: BTreeMap<String, String> = BTreeMap::new();
    let mut col: Collection<String, String, _> = Collection::new(&committed, 5, 1);

    col.put("x".into(), "val".into());
    col.put("y".into(), "val2".into());

    let (ops, overlay) = col.into_parts();
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[0].collection(), 5);
    assert_eq!(ops[1].collection(), 5);
    assert_eq!(overlay.puts.len(), 2);
}

// ---- Collection::iter ----

#[test]
fn collection_iter_merges_committed_and_overlay() {
    let mut committed: BTreeMap<String, String> = BTreeMap::new();
    committed.insert("a".into(), "1".into());
    committed.insert("b".into(), "2".into());

    let mut col: Collection<String, String, _> = Collection::new(&committed, 0, 1);
    col.put("c".into(), "3".into());

    let pairs: Vec<_> = col.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(pairs.len(), 3);
    assert!(pairs.contains(&("a".into(), "1".into())));
    assert!(pairs.contains(&("b".into(), "2".into())));
    assert!(pairs.contains(&("c".into(), "3".into())));
}

#[test]
fn collection_iter_excludes_deleted() {
    let mut committed: BTreeMap<String, String> = BTreeMap::new();
    committed.insert("a".into(), "1".into());
    committed.insert("b".into(), "2".into());

    let mut col: Collection<String, String, _> = Collection::new(&committed, 0, 1);
    col.delete(&"a".into());

    let keys: Vec<_> = col.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec!["b".to_string()]);
}

// ---- Derive: rollback on error ----

#[test]
fn derive_rollback_on_error() {
    let store = Store::<AppState>::memory();

    store
        .write(|tx| {
            tx.users.put(
                "alice".into(),
                User {
                    name: "Alice".into(),
                    email: "alice@example.com".into(),
                },
            );
            Ok(())
        })
        .unwrap();

    // This write returns an error — state should NOT change.
    let result: crate::Result<()> = store.write(|tx| {
        tx.users.put(
            "bob".into(),
            User {
                name: "Bob".into(),
                email: "bob@example.com".into(),
            },
        );
        tx.users.delete(&"alice".into());
        Err(crate::Error::invalid("test", "intentional rollback"))
    });
    assert!(result.is_err());

    let state = store.read();
    assert!(
        state.users.contains_key("alice"),
        "alice should survive rollback"
    );
    assert!(
        !state.users.contains_key("bob"),
        "bob should not exist after rollback"
    );
}

// ---- Derive: Replayable routes ops to correct collection ----

#[test]
fn derive_replayable_routes_by_collection_id() {
    let mut state = AppState::default();

    let ops = vec![
        Op::Put {
            collection: 0, // users
            key: b"alice".to_vec(),
            value: crate::encode_versioned_value(
                1,
                &User {
                    name: "Alice".into(),
                    email: "a@a.com".into(),
                },
            )
            .unwrap(),
        },
        Op::Put {
            collection: 1, // counters
            key: b"logins".to_vec(),
            value: crate::encode_versioned_value(1, &42u32).unwrap(),
        },
    ];

    crate::wal::Replayable::apply(&mut state, &ops).unwrap();

    assert_eq!(state.users.get("alice").unwrap().name, "Alice");
    assert_eq!(*state.counters.get("logins").unwrap(), 42);
}

#[test]
fn derive_replayable_ignores_unknown_collection() {
    let mut state = AppState::default();

    let ops = vec![Op::Put {
        collection: 99, // not mapped
        key: b"foo".to_vec(),
        value: vec![1, 2, 3],
    }];

    // Should not panic or error — unknown collections are silently skipped.
    crate::wal::Replayable::apply(&mut state, &ops).unwrap();
    assert!(state.users.is_empty());
    assert!(state.counters.is_empty());
}

// ---- Collection: delete nonexistent key ----

#[test]
fn collection_delete_nonexistent_returns_false() {
    let committed: BTreeMap<String, String> = BTreeMap::new();
    let mut col: Collection<String, String, _> = Collection::new(&committed, 0, 1);

    let existed = col.delete(&"nope".into());
    assert!(!existed);
    // Op is still emitted (WAL records the intent).
    let (ops, _) = col.into_parts();
    assert_eq!(ops.len(), 1);
}

// ---- Non-String key types ----

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct IntKeyState {
    #[etch(collection = 0)]
    items: BTreeMap<u64, String>,
}

#[test]
fn derive_btree_u64_key_write_read() {
    let store = Store::<IntKeyState>::memory();

    store
        .write(|tx| {
            tx.items.put(42, "forty-two".into());
            tx.items.put(100, "hundred".into());
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get(&42).unwrap(), "forty-two");
    assert_eq!(state.items.get(&100).unwrap(), "hundred");
}

#[test]
fn derive_btree_u64_key_wal_persistence() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store =
            Store::<IntKeyState, WalBackend<IntKeyState>>::open_wal(dir.path().into()).unwrap();
        store
            .write(|tx| {
                tx.items.put(1, "one".into());
                tx.items.put(999, "nine-nine-nine".into());
                Ok(())
            })
            .unwrap();
    }

    let store = Store::<IntKeyState, WalBackend<IntKeyState>>::open_wal(dir.path().into()).unwrap();
    let state = store.read();
    assert_eq!(state.items.get(&1).unwrap(), "one");
    assert_eq!(state.items.get(&999).unwrap(), "nine-nine-nine");
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct HashIntKeyState {
    #[etch(collection = 0)]
    items: HashMap<u32, String>,
}

#[test]
fn derive_hashmap_u32_key_write_read() {
    let store = Store::<HashIntKeyState>::memory();

    store
        .write(|tx| {
            tx.items.put(7, "seven".into());
            tx.items.put(256, "two-fifty-six".into());
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get(&7).unwrap(), "seven");
    assert_eq!(state.items.get(&256).unwrap(), "two-fifty-six");
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct MixedKeyState {
    #[etch(collection = 0)]
    by_name: BTreeMap<String, String>,
    #[etch(collection = 1)]
    by_id: BTreeMap<u64, String>,
}

#[test]
fn derive_mixed_string_and_u64_keys() {
    let store = Store::<MixedKeyState>::memory();

    store
        .write(|tx| {
            tx.by_name.put("alice".into(), "Alice".into());
            tx.by_id.put(1, "first".into());
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.by_name.get("alice").unwrap(), "Alice");
    assert_eq!(state.by_id.get(&1).unwrap(), "first");
}

#[test]
fn derive_mixed_keys_wal_persistence() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store =
            Store::<MixedKeyState, WalBackend<MixedKeyState>>::open_wal(dir.path().into()).unwrap();
        store
            .write(|tx| {
                tx.by_name.put("bob".into(), "Bob".into());
                tx.by_id.put(42, "the answer".into());
                Ok(())
            })
            .unwrap();
    }

    let store =
        Store::<MixedKeyState, WalBackend<MixedKeyState>>::open_wal(dir.path().into()).unwrap();
    let state = store.read();
    assert_eq!(state.by_name.get("bob").unwrap(), "Bob");
    assert_eq!(state.by_id.get(&42).unwrap(), "the answer");
}

// ---- Collection with HashMap committed ----

#[test]
fn collection_with_hashmap_committed() {
    let mut committed: HashMap<String, u64> = HashMap::new();
    committed.insert("x".into(), 10);

    let mut col: Collection<String, u64, _> = Collection::new(&committed, 0, 1);
    assert_eq!(*col.get(&"x".into()).unwrap(), 10);

    col.put("y".into(), 20);
    assert_eq!(*col.get(&"y".into()).unwrap(), 20);
    assert!(col.contains(&"x".into()));
    assert!(col.contains(&"y".into()));
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

/// postcard is positional: old bytes can't deserialize into new struct.
#[test]
fn test_postcard_old_bytes_fail_new_struct() {
    let v1 = ItemV1 {
        name: "widget".into(),
        count: 42,
    };
    let v1_bytes = postcard::to_allocvec(&v1).unwrap();

    let v2 = ItemV2 {
        name: "widget".into(),
        count: 42,
        label: None,
        description: None,
    };
    let v2_bytes = postcard::to_allocvec(&v2).unwrap();

    // v1 has fewer bytes — reading as v2 hits EOF.
    assert!(v1_bytes.len() < v2_bytes.len());
    assert!(postcard::from_bytes::<ItemV2>(&v1_bytes).is_err());
    // v2 -> v1 is fine (extra bytes ignored).
    assert!(postcard::from_bytes::<ItemV1>(&v2_bytes).is_ok());
}

/// State type using the v2 struct.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct SchemaState {
    #[etch(collection = 0)]
    items: BTreeMap<String, ItemV2>,
}

/// WAL with old-schema entries should open successfully, skipping bad entries.
#[test]
fn test_wal_skips_old_schema_entries() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path();

    // Step 1: Write one good v2 entry, then inject a v1 entry into the WAL.
    {
        let store =
            Store::<SchemaState, WalBackend<SchemaState>>::open_wal(state_dir.to_path_buf())
                .unwrap();

        store
            .write(|tx| {
                tx.items.put(
                    "good".into(),
                    ItemV2 {
                        name: "good".into(),
                        count: 1,
                        label: None,
                        description: None,
                    },
                );
                Ok(())
            })
            .unwrap();
        drop(store);

        // Append a v1-serialized entry directly to the WAL file.
        let v1_bytes = postcard::to_allocvec(&ItemV1 {
            name: "old".into(),
            count: 99,
        })
        .unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"old".to_vec(),
            value: v1_bytes,
        }];
        // WAL entry format: len(u32 LE) | payload | xxh3(u64 LE)
        use std::io::Write;
        let payload = postcard::to_allocvec(&ops).unwrap();
        let hash = xxhash_rust::xxh3::xxh3_64(&payload);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(state_dir.join("wal.bin"))
            .unwrap();
        f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&payload).unwrap();
        f.write_all(&hash.to_le_bytes()).unwrap();
        f.sync_all().unwrap();
    }

    // Step 2: Re-open — should succeed, with the good entry intact.
    let store = Store::<SchemaState, WalBackend<SchemaState>>::open_wal(state_dir.to_path_buf())
        .expect("WAL open must not crash on schema mismatch");

    let state = store.read();
    assert!(
        state.items.contains_key("good"),
        "valid entry should survive"
    );
    assert!(
        !state.items.contains_key("old"),
        "old-schema entry should be skipped"
    );
}

// ---- Retry quarantine after registering a missing migration ----

#[test]
fn test_retry_quarantine_after_fix() {
    use crate::IncrementalSave;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().to_path_buf();

    // Phase A: write a v1 value and compact. The snapshot holds a v1 item.
    {
        let store =
            Store::<CompactStateV1, WalBackend<CompactStateV1>>::open_wal(state_dir.clone())
                .unwrap();
        store
            .write(|tx| {
                tx.items.put(
                    "quarantinee".into(),
                    CompactV1Item {
                        payload: "preserve-me".into(),
                    },
                );
                Ok(())
            })
            .unwrap();
        let live = store.read().clone();
        store.backend().snapshot(&live).unwrap();
    }

    // Phase B: simulate a binary WITHOUT migration 1->2 registered.
    // We reuse CompactStateV2 but with a local override by creating a new
    // type that has NO migrations. For simplicity, use BrokenV2State.

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Transactable)]
    struct BrokenV2State {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, CompactV2Item>,
    }

    impl crate::Replayable for BrokenV2State {
        fn apply_with_format(
            &mut self,
            ops: &[Op],
            format: crate::ReplayFormat,
        ) -> crate::Result<()> {
            for op in ops {
                if op.collection() == 0 {
                    let _ = crate::apply_op_versioned_with(
                        &mut self.items,
                        op,
                        format,
                        2,
                        <String as crate::EtchKey>::from_bytes,
                    );
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
                    let _ = crate::apply_op_versioned_with_ctx(
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

        // NO migrations registered.

        fn to_snapshot(&self) -> crate::Result<crate::SnapshotPayload> {
            let mut entries = Vec::new();
            for (k, v) in &self.items {
                entries.push(crate::SnapshotEntry {
                    key: crate::EtchKey::to_bytes(k),
                    version: 2,
                    value: crate::encode_msgpack_value(v)?,
                });
            }
            Ok(crate::SnapshotPayload {
                schema_fingerprint: 0,
                collections: vec![crate::CollectionSection {
                    collection_id: 0,
                    current_version: 2,
                    entries,
                }],
            })
        }

        fn from_snapshot(
            payload: crate::SnapshotPayload,
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
                            crate::load_snapshot_entry::<CompactV2Item>(entry, 0, 2, ctx)
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

    {
        let store =
            Store::<BrokenV2State, WalBackend<BrokenV2State>>::open_wal(state_dir.clone()).unwrap();
        // Migration missing → snapshot entry quarantined, not lost.
        assert!(store.read().items.is_empty(), "value not yet in live state");
        let quarantined = store.quarantined();
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].key, b"quarantinee");
    }

    // Phase C: ship the fix — CompactStateV2 has the 1->2 migration.
    // Reopen, retry_quarantine, value is recovered.
    {
        let store =
            Store::<CompactStateV2, WalBackend<CompactStateV2>>::open_wal(state_dir.clone())
                .unwrap();

        // Quarantine file persisted from phase B. On open_wal, the backend
        // loads quarantine.bin and *also* replays; the snapshot values
        // migrate successfully this time (migration is registered), so the
        // live state already has the value.
        //
        // Note: quarantine.bin may still hold the old entry (persisted from
        // phase B's close). retry_quarantine cleans it up.
        let recovered = store.retry_quarantine().unwrap();
        let _ = recovered; // may be 0 if replay already migrated from snapshot

        let state = store.read();
        let got = state.items.get("quarantinee").expect("recovered");
        assert_eq!(got.body, "preserve-me");
    }
}

// ---- Snapshot roundtrip with derive-generated to_snapshot/from_snapshot ----

/// Verify a store can write via Collection, force compaction, reopen, and
/// still see the data — exercising the new versioned snapshot format.
#[test]
fn test_snapshot_roundtrip_through_derive() {
    use crate::IncrementalSave;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().to_path_buf();

    // Populate and compact.
    {
        let store = Store::<AppState, WalBackend<AppState>>::open_wal(state_dir.clone()).unwrap();
        store
            .write(|tx| {
                tx.users.put(
                    "alice".into(),
                    User {
                        name: "Alice".into(),
                        email: "a@a.com".into(),
                    },
                );
                tx.counters.put("logins".into(), 7);
                Ok(())
            })
            .unwrap();

        // Force snapshot via backend.
        let live = store.read().clone();
        store.backend().snapshot(&live).unwrap();
    }

    // Reopen and verify.
    {
        let store = Store::<AppState, WalBackend<AppState>>::open_wal(state_dir.clone()).unwrap();
        let state = store.read();
        assert_eq!(state.users.get("alice").unwrap().name, "Alice");
        assert_eq!(*state.counters.get("logins").unwrap(), 7);
    }
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
                let _ = crate::apply_op_versioned_with(&mut self.items, op, format, 2, |bytes| {
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
                let _ = crate::apply_op_versioned_with_ctx(
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

    fn to_snapshot(&self) -> crate::Result<crate::SnapshotPayload> {
        let mut entries = Vec::new();
        for (k, v) in &self.items {
            entries.push(crate::SnapshotEntry {
                key: crate::EtchKey::to_bytes(k),
                version: 2,
                value: crate::encode_msgpack_value(v)?,
            });
        }
        Ok(crate::SnapshotPayload {
            schema_fingerprint: 0,
            collections: vec![crate::CollectionSection {
                collection_id: 0,
                current_version: 2,
                entries,
            }],
        })
    }

    fn from_snapshot(
        payload: crate::SnapshotPayload,
        ctx: &mut crate::ReplayContext<'_>,
    ) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state = Self::default();
        for section in &payload.collections {
            if section.collection_id == 0 {
                for entry in &section.entries {
                    if let Some(v) = crate::load_snapshot_entry::<CompactV2Item>(entry, 0, 2, ctx)
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

/// Write v1 data, compact (so data is only in snapshot, WAL is empty),
/// reopen as v2 with migration. Per-value snapshot versioning must
/// surface the migrated value.
#[test]
fn test_compacted_v1_plus_no_wal_upgrade_to_v2() {
    use crate::IncrementalSave;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().to_path_buf();

    // Phase 1: write with v1 schema and force compaction.
    {
        let store =
            Store::<CompactStateV1, WalBackend<CompactStateV1>>::open_wal(state_dir.clone())
                .unwrap();
        store
            .write(|tx| {
                tx.items.put(
                    "x".into(),
                    CompactV1Item {
                        payload: "the-original-data".into(),
                    },
                );
                tx.items.put(
                    "y".into(),
                    CompactV1Item {
                        payload: "more-data".into(),
                    },
                );
                Ok(())
            })
            .unwrap();
        let live = store.read().clone();
        store.backend().snapshot(&live).unwrap();
    }

    // Phase 2: reopen as v2 with migration registered. Snapshot-only data
    // should survive the upgrade because each value carries its own version.
    {
        let store =
            Store::<CompactStateV2, WalBackend<CompactStateV2>>::open_wal(state_dir.clone())
                .unwrap();
        let state = store.read();
        let x = state
            .items
            .get("x")
            .expect("snapshot-migrated value present");
        assert_eq!(x.body, "the-original-data");
        assert_eq!(x.weight, 0);
        let y = state
            .items
            .get("y")
            .expect("snapshot-migrated value present");
        assert_eq!(y.body, "more-data");
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

/// When we upgrade from v1 to v2 with a migration registered, the
/// migration chain runs per-op and produces correct v2 values.
#[test]
fn test_migrate_wal_v1_to_v2() {
    // Build a v1 op as if it had been written by an older binary.
    // (We still write with msgpack + envelope — the "old" part is the V type.)
    let v1 = MigItemV1 {
        name: "widget".into(),
        count: 42,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes()); // version 1
    envelope.extend_from_slice(&v1_bytes);

    let ops = vec![Op::Put {
        collection: 0,
        key: b"widget".to_vec(),
        value: envelope,
    }];

    // Without a migration, the v1 op should fail with SchemaVersionMismatch.
    let migrations_empty = crate::MigrationSet::new();
    let mut quarantine = crate::Quarantine::new();
    let mut ctx = crate::ReplayContext::new(
        crate::ReplayFormat::Versioned,
        &migrations_empty,
        &mut quarantine,
    );
    let mut state = MigStateV2::default();
    // apply_with_ctx routes internally and eprintln-skips on error, so state is empty.
    crate::Replayable::apply_with_ctx(&mut state, &ops, &mut ctx).unwrap();
    assert!(
        state.items.is_empty(),
        "no migration registered -> value should be skipped"
    );

    // With a registered migration 1 -> 2, the value arrives migrated.
    let migrations = crate::MigrationSet::new().add(0, 1, 2, |bytes| {
        let old: MigItemV1 = rmp_serde::from_slice(bytes)?;
        let new = MigItemV2 {
            name: old.name,
            count: old.count,
            label: None,
            priority: 0,
        };
        Ok(rmp_serde::to_vec_named(&new)?)
    });
    let mut quarantine = crate::Quarantine::new();
    let mut ctx =
        crate::ReplayContext::new(crate::ReplayFormat::Versioned, &migrations, &mut quarantine);
    let mut state = MigStateV2::default();
    crate::Replayable::apply_with_ctx(&mut state, &ops, &mut ctx).unwrap();

    let got = state.items.get("widget").expect("migrated value present");
    assert_eq!(got.name, "widget");
    assert_eq!(got.count, 42);
    assert_eq!(got.label, None);
    assert_eq!(got.priority, 0);
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

/// User skips v2 entirely — upgrades directly from v1 to v3. The migration
/// chain must run both 1->2 and 2->3 hops to reach the current version.
#[test]
fn test_chain_migrate_v1_to_v3() {
    let v1 = MigItemV1 {
        name: "thingy".into(),
        count: 7,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes());
    envelope.extend_from_slice(&v1_bytes);

    let ops = vec![Op::Put {
        collection: 0,
        key: b"thingy".to_vec(),
        value: envelope,
    }];

    // Register both hops.
    let migrations = crate::MigrationSet::new()
        .add(0, 1, 2, |bytes| {
            let old: MigItemV1 = rmp_serde::from_slice(bytes)?;
            let v2 = MigItemV2 {
                name: old.name,
                count: old.count,
                label: None,
                priority: 0,
            };
            Ok(rmp_serde::to_vec_named(&v2)?)
        })
        .add(0, 2, 3, |bytes| {
            let old: MigItemV2 = rmp_serde::from_slice(bytes)?;
            let v3 = MigItemV3 {
                name: old.name,
                count: old.count,
                label: old.label,
                priority: old.priority,
                tags: vec![],
            };
            Ok(rmp_serde::to_vec_named(&v3)?)
        });

    let mut quarantine = crate::Quarantine::new();
    let mut ctx =
        crate::ReplayContext::new(crate::ReplayFormat::Versioned, &migrations, &mut quarantine);
    let mut state = MigStateV3::default();
    crate::Replayable::apply_with_ctx(&mut state, &ops, &mut ctx).unwrap();

    let got = state
        .items
        .get("thingy")
        .expect("chain-migrated value present");
    assert_eq!(got.name, "thingy");
    assert_eq!(got.count, 7);
    assert_eq!(got.tags, Vec::<String>::new());
}

/// Skip v2 migration, only register 2->3. Value at v1 should be skipped
/// (no chain possible) without crashing.
#[test]
fn test_skip_v2_no_migration_skips_value() {
    let v1 = MigItemV1 {
        name: "thingy".into(),
        count: 7,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes());
    envelope.extend_from_slice(&v1_bytes);

    let ops = vec![Op::Put {
        collection: 0,
        key: b"thingy".to_vec(),
        value: envelope,
    }];

    // Only 2->3 registered — no path from 1.
    let migrations = crate::MigrationSet::new().add(0, 2, 3, |bytes| Ok(bytes.to_vec()));

    let mut quarantine = crate::Quarantine::new();
    let mut ctx =
        crate::ReplayContext::new(crate::ReplayFormat::Versioned, &migrations, &mut quarantine);
    let mut state = MigStateV3::default();
    crate::Replayable::apply_with_ctx(&mut state, &ops, &mut ctx).unwrap();

    // Value is NOT lost — it's quarantined with the reason recorded.
    assert!(state.items.is_empty());
    assert_eq!(quarantine.len(), 1);
    let entry = &quarantine.entries()[0];
    assert_eq!(entry.collection, 0);
    assert_eq!(entry.key, b"thingy");
    assert_eq!(entry.version, 1);
    assert!(matches!(
        entry.reason,
        crate::QuarantineReason::MissingMigration { from: 1, to: 2 }
    ));
}

// ---- Write supersedes quarantined key ----

#[test]
fn test_write_supersedes_quarantined_key() {
    // First seed the quarantine with a v1 value that can't migrate.
    let v1 = MigItemV1 {
        name: "oldie".into(),
        count: 99,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes());
    envelope.extend_from_slice(&v1_bytes);
    let quarantining_op = Op::Put {
        collection: 0,
        key: b"oldie".to_vec(),
        value: envelope,
    };

    let migrations_empty = crate::MigrationSet::new();
    let mut quarantine = crate::Quarantine::new();
    let mut ctx = crate::ReplayContext::new(
        crate::ReplayFormat::Versioned,
        &migrations_empty,
        &mut quarantine,
    );
    let mut state = MigStateV3::default();
    crate::Replayable::apply_with_ctx(&mut state, &[quarantining_op], &mut ctx).unwrap();
    assert_eq!(quarantine.len(), 1, "sanity: quarantine populated");

    // Now simulate a new write to the same key (current v3 format).
    let v3 = MigItemV3 {
        name: "new-value".into(),
        count: 1,
        label: None,
        priority: 0,
        tags: vec![],
    };
    let fresh = Op::Put {
        collection: 0,
        key: b"oldie".to_vec(),
        value: crate::encode_versioned_value(3, &v3).unwrap(),
    };
    let mut ctx = crate::ReplayContext::new(
        crate::ReplayFormat::Versioned,
        &migrations_empty,
        &mut quarantine,
    );
    crate::Replayable::apply_with_ctx(&mut state, &[fresh], &mut ctx).unwrap();

    // New value in state, quarantine entry gone.
    assert_eq!(state.items.get("oldie").unwrap().name, "new-value");
    assert!(quarantine.is_empty(), "write supersedes quarantine entry");
}

// ---- Delete removes quarantine entry ----

#[test]
fn test_delete_removes_quarantined_entry() {
    let v1 = MigItemV1 {
        name: "goner".into(),
        count: 1,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes());
    envelope.extend_from_slice(&v1_bytes);

    let migrations_empty = crate::MigrationSet::new();
    let mut quarantine = crate::Quarantine::new();
    let mut state = MigStateV3::default();

    // Quarantine populated.
    {
        let mut ctx = crate::ReplayContext::new(
            crate::ReplayFormat::Versioned,
            &migrations_empty,
            &mut quarantine,
        );
        crate::Replayable::apply_with_ctx(
            &mut state,
            &[Op::Put {
                collection: 0,
                key: b"goner".to_vec(),
                value: envelope,
            }],
            &mut ctx,
        )
        .unwrap();
    }
    assert_eq!(quarantine.len(), 1);

    // Delete removes quarantine entry too.
    {
        let mut ctx = crate::ReplayContext::new(
            crate::ReplayFormat::Versioned,
            &migrations_empty,
            &mut quarantine,
        );
        crate::Replayable::apply_with_ctx(
            &mut state,
            &[Op::Delete {
                collection: 0,
                key: b"goner".to_vec(),
            }],
            &mut ctx,
        )
        .unwrap();
    }
    assert!(quarantine.is_empty());
}

/// Verify that diagnostic helpers expose op metadata needed for good error
/// messages (field name is baked into the derive macro at compile time, so
/// we test the runtime-accessible pieces here).
#[test]
fn test_op_diagnostic_accessors() {
    let put = Op::Put {
        collection: 7,
        key: b"workspace-acme".to_vec(),
        value: vec![],
    };
    assert_eq!(put.collection(), 7);
    assert_eq!(put.key(), b"workspace-acme");
    assert_eq!(put.kind(), "PUT");

    let del = Op::Delete {
        collection: 3,
        key: b"u-42".to_vec(),
    };
    assert_eq!(del.kind(), "DELETE");
    assert_eq!(crate::format_op_key(&del), "\"u-42\"");

    // Binary keys fall back to hex so control characters don't corrupt logs.
    let binary = Op::Put {
        collection: 0,
        key: vec![0x00, 0xff, 0x01],
        value: vec![],
    };
    assert_eq!(crate::format_op_key(&binary), "0x00ff01");
}
