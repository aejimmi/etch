//! Basic derive-driven CRUD across BTreeMap, HashMap, and mixed states.

use super::*;
use crate::store::Store;
use crate::wal::WalBackend;

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
            )?;
            tx.counters.put("logins".into(), 1)?;
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
            )?;
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
            )?;
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
            )?;
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
            )?;
            tx.counters.put("signups".into(), 42)?;
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
                )?;
                Ok(())
            })
            .unwrap();
    }

    // Reopen and verify state was replayed.
    let store = Store::<AppState, WalBackend<AppState>>::open_wal(dir.path().into()).unwrap();
    let state = store.read();
    assert_eq!(state.users.get("frank").unwrap().name, "Frank");
}

#[test]
fn derive_hashmap_write_read() {
    let store = Store::<HashState>::memory();

    store
        .write(|tx| {
            tx.items.put("key1".into(), "value1".into())?;
            tx.items.put("key2".into(), "value2".into())?;
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
            tx.items.put("gone".into(), "soon".into())?;
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

#[test]
fn derive_mixed_collections() {
    let store = Store::<MixedState>::memory();

    store
        .write(|tx| {
            tx.ordered.put("a".into(), "alpha".into())?;
            tx.fast.put("x".into(), 99)?;
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
            )?;
            tx.users.put(
                "b".into(),
                User {
                    name: "B".into(),
                    email: "b@b.com".into(),
                },
            )?;
            let names: Vec<&str> = tx.users.values().map(|u| u.name.as_str()).collect();
            assert_eq!(names.len(), 2);
            Ok(())
        })
        .unwrap();
}
