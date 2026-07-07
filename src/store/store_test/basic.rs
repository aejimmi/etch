use super::*;
use tempfile::TempDir;

#[test]
fn memory_store_read_write() {
    let store = Store::<TestState>::memory();

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get("a").unwrap(), "1");
    assert_eq!(state.items.len(), 1);
}

#[test]
fn write_rollback_on_error() {
    let store = Store::<TestState>::memory();

    store
        .write(|tx| {
            tx.insert("good", "data");
            Ok(())
        })
        .unwrap();

    // This write should fail and not affect state
    let result: std::result::Result<(), Error> =
        store.write(|_tx| Err(Error::invalid("test", "forced error")));
    assert!(result.is_err());

    let state = store.read();
    assert!(state.items.contains_key("good"));
    assert!(!state.items.contains_key("bad"));
}

#[test]
fn concurrent_reads() {
    let store = Store::<TestState>::memory();
    store
        .write(|tx| {
            tx.insert("x", "1");
            Ok(())
        })
        .unwrap();

    // Multiple concurrent read guards
    let r1 = store.read();
    let r2 = store.read();
    assert_eq!(r1.items.get("x"), r2.items.get("x"));
}

// ---------------------------------------------------------------------------
// Immediate-mode auto-compaction (WAL bounded on the synchronous write path)
// ---------------------------------------------------------------------------

#[test]
fn immediate_mode_auto_compacts_over_threshold() {
    let dir = TempDir::new().unwrap();
    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    store.set_snapshot_threshold(5);

    let snap = dir.path().join("snapshot.postcard");
    assert!(
        !snap.exists(),
        "no snapshot before the threshold is crossed"
    );

    for i in 0..6 {
        store
            .write(|tx| {
                tx.insert(&format!("k{i}"), &format!("v{i}"));
                Ok(())
            })
            .unwrap();
    }

    // Crossing the threshold triggers inline compaction on the immediate path
    // (previously compaction only ran in the grouped flusher, so an
    // immediate-mode WAL grew unbounded).
    assert!(
        snap.exists(),
        "immediate-mode write should compact once past the threshold"
    );
    assert!(
        !store.backend().should_snapshot(),
        "WAL entry count must reset after compaction"
    );

    // Data survives a reopen from the freshly compacted snapshot.
    drop(store);
    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    assert_eq!(store.read().items.get("k0").unwrap(), "v0");
    assert_eq!(store.read().items.get("k5").unwrap(), "v5");
    assert_eq!(store.read().items.len(), 6);
}
