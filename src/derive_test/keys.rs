//! Derive support for non-String key types (u64/u32) and mixed key states.

use super::*;
use crate::store::Store;
use crate::wal::WalBackend;

#[test]
fn derive_btree_u64_key_write_read() {
    let store = Store::<IntKeyState>::memory();

    store
        .write(|tx| {
            tx.items.put(42, "forty-two".into())?;
            tx.items.put(100, "hundred".into())?;
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
                tx.items.put(1, "one".into())?;
                tx.items.put(999, "nine-nine-nine".into())?;
                Ok(())
            })
            .unwrap();
    }

    let store = Store::<IntKeyState, WalBackend<IntKeyState>>::open_wal(dir.path().into()).unwrap();
    let state = store.read();
    assert_eq!(state.items.get(&1).unwrap(), "one");
    assert_eq!(state.items.get(&999).unwrap(), "nine-nine-nine");
}

#[test]
fn derive_hashmap_u32_key_write_read() {
    let store = Store::<HashIntKeyState>::memory();

    store
        .write(|tx| {
            tx.items.put(7, "seven".into())?;
            tx.items.put(256, "two-fifty-six".into())?;
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get(&7).unwrap(), "seven");
    assert_eq!(state.items.get(&256).unwrap(), "two-fifty-six");
}

#[test]
fn derive_mixed_string_and_u64_keys() {
    let store = Store::<MixedKeyState>::memory();

    store
        .write(|tx| {
            tx.by_name.put("alice".into(), "Alice".into())?;
            tx.by_id.put(1, "first".into())?;
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
                tx.by_name.put("bob".into(), "Bob".into())?;
                tx.by_id.put(42, "the answer".into())?;
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
