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
    let mut col: Collection<String, String, _> = Collection::new(&committed, 5);

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

    let mut col: Collection<String, String, _> = Collection::new(&committed, 0);
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

    let mut col: Collection<String, String, _> = Collection::new(&committed, 0);
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
            value: postcard::to_allocvec(&User {
                name: "Alice".into(),
                email: "a@a.com".into(),
            })
            .unwrap(),
        },
        Op::Put {
            collection: 1, // counters
            key: b"logins".to_vec(),
            value: postcard::to_allocvec(&42u32).unwrap(),
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
    let mut col: Collection<String, String, _> = Collection::new(&committed, 0);

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

    let mut col: Collection<String, u64, _> = Collection::new(&committed, 0);
    assert_eq!(*col.get(&"x".into()).unwrap(), 10);

    col.put("y".into(), 20);
    assert_eq!(*col.get(&"y".into()).unwrap(), 20);
    assert!(col.contains(&"x".into()));
    assert!(col.contains(&"y".into()));
}
