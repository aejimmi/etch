use super::*;
use tempfile::TempDir;

// =========================================================================
// End-to-end WAL store tests
// =========================================================================

#[test]
fn wal_store_write_close_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        store
            .write(|tx| {
                tx.insert("a", "1");
                tx.insert("b", "2");
                Ok(())
            })
            .unwrap();
    }

    {
        let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        let state = store.read();
        assert_eq!(state.items.get("a").unwrap(), "1");
        assert_eq!(state.items.get("b").unwrap(), "2");
        assert_eq!(state.items.len(), 2);
    }
}

#[test]
fn wal_store_multiple_writes_persist() {
    let dir = TempDir::new().unwrap();

    {
        let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        for i in 0..10 {
            store
                .write(|tx| {
                    tx.insert(&format!("k{i}"), &format!("v{i}"));
                    Ok(())
                })
                .unwrap();
        }
    }

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = store.read();
    assert_eq!(state.items.len(), 10);
    for i in 0..10 {
        assert_eq!(state.items.get(&format!("k{i}")).unwrap(), &format!("v{i}"));
    }
}

#[test]
fn wal_store_rollback_does_not_persist() {
    let dir = TempDir::new().unwrap();

    {
        let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        store
            .write(|tx| {
                tx.insert("good", "yes");
                Ok(())
            })
            .unwrap();

        let _ = store.write(|tx| {
            tx.insert("bad", "no");
            Err::<(), _>(Error::invalid("test", "nope"))
        });
    }

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = store.read();
    assert_eq!(state.items.get("good").unwrap(), "yes");
    assert!(!state.items.contains_key("bad"));
}

#[test]
fn wal_store_write_durable_persists() {
    let dir = TempDir::new().unwrap();

    {
        let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        store
            .write_durable(|tx| {
                tx.insert("critical", "data");
                Ok(())
            })
            .unwrap();
    }

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = store.read();
    assert_eq!(state.items.get("critical").unwrap(), "data");
}

#[test]
fn wal_store_open_empty_dir() {
    let dir = TempDir::new().unwrap();
    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    assert!(store.read().items.is_empty());
}

#[test]
fn wal_store_backend_accessible() {
    let dir = TempDir::new().unwrap();
    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let _backend = store.backend();
}

// =========================================================================
// WAL store + grouped flush
// =========================================================================

#[test]
fn wal_store_grouped_write_persists() {
    let dir = TempDir::new().unwrap();

    {
        let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_millis(50),
        });

        for i in 0..10 {
            store
                .write(|tx| {
                    tx.insert(&format!("k{i}"), &format!("v{i}"));
                    Ok(())
                })
                .unwrap();
        }

        std::thread::sleep(Duration::from_millis(200));
        store.flush().unwrap();
    }

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = store.read();
    assert_eq!(state.items.len(), 10);
    assert_eq!(state.items.get("k9").unwrap(), "v9");
}

#[test]
fn wal_store_grouped_flusher_processes_ops() {
    let dir = TempDir::new().unwrap();

    let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped {
        interval: Duration::from_millis(30),
    });

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    std::thread::sleep(Duration::from_millis(100));

    assert_eq!(store.read().items.get("a").unwrap(), "1");

    store.flush().unwrap();
}

#[test]
fn wal_store_grouped_write_durable_drains_pending() {
    let dir = TempDir::new().unwrap();

    {
        let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_secs(10),
        });

        store
            .write(|tx| {
                tx.insert("buffered", "yes");
                Ok(())
            })
            .unwrap();

        store
            .write_durable(|tx| {
                tx.insert("durable", "yes");
                Ok(())
            })
            .unwrap();
    }

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = store.read();
    assert_eq!(state.items.get("buffered").unwrap(), "yes");
    assert_eq!(state.items.get("durable").unwrap(), "yes");
}

#[test]
fn wal_store_grouped_flush_persists_across_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_millis(50),
        });

        for i in 0..20 {
            store
                .write(|tx| {
                    tx.insert(&format!("k{i}"), &format!("v{i}"));
                    Ok(())
                })
                .unwrap();
        }

        store.flush().unwrap();
    }

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = store.read();
    assert_eq!(state.items.len(), 20);
}

#[test]
fn set_flush_policy_to_immediate() {
    let (mut store, _inner) = grouped_store(Duration::from_millis(50));

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    store.set_flush_policy(FlushPolicy::Immediate);

    store
        .write(|tx| {
            tx.insert("b", "2");
            Ok(())
        })
        .unwrap();

    assert_eq!(store.read().items.get("b").unwrap(), "2");
}

#[test]
fn flush_noop_in_immediate_mode() {
    let store = Store::<TestState>::memory();
    store.flush().unwrap();
}

#[test]
fn flush_noop_when_already_flushed() {
    let (store, _inner) = grouped_store(Duration::from_millis(50));

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    store.flush().unwrap();
    store.flush().unwrap();
}

#[test]
fn close_shuts_down_flusher() {
    let backend = CountingBackend::new();
    let inner = Arc::clone(&backend.inner);
    let mut store = Store::with_backend(backend).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped {
        interval: Duration::from_secs(5),
    });

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    store.close().unwrap();

    let state = inner.state.lock().clone();
    assert_eq!(
        state.items.get("a").unwrap(),
        "1",
        "close must flush pending writes"
    );
}

#[test]
fn wal_store_grouped_empty_write_noop() {
    let dir = TempDir::new().unwrap();

    let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped {
        interval: Duration::from_millis(50),
    });

    store.write(|_tx| Ok(())).unwrap();

    store.flush().unwrap();
    assert!(store.read().items.is_empty());
}

#[test]
fn wal_store_write_durable_empty_ops() {
    let dir = TempDir::new().unwrap();

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();

    store.write_durable(|_tx| Ok(())).unwrap();

    assert!(store.read().items.is_empty());
}

#[test]
fn wal_store_grouped_close_flushes() {
    let dir = TempDir::new().unwrap();

    {
        let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_secs(10),
        });

        store
            .write(|tx| {
                tx.insert("k", "v");
                Ok(())
            })
            .unwrap();

        store.close().unwrap();
    }

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    assert_eq!(store.read().items.get("k").unwrap(), "v");
}
