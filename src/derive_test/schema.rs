//! Rollback, op routing, postcard schema incompatibility, WAL skip of
//! old-schema entries, and the derive-generated snapshot roundtrip.

use super::*;
use crate::store::Store;
use crate::wal::WalBackend;

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
            )?;
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
        )?;
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

#[test]
fn derive_replayable_routes_by_collection_id() {
    let mut state = AppState::default();

    let ops = vec![
        Op::Put {
            collection: 0, // users
            key: b"alice".to_vec(),
            value: crate::wal::encode_versioned_value(
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
            value: crate::wal::encode_versioned_value(1, &42u32).unwrap(),
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
                )?;
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
                )?;
                tx.counters.put("logins".into(), 7)?;
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
