use super::*;
use tempfile::TempDir;

// =========================================================================
// Group commit unit tests
// =========================================================================

#[test]
fn group_commit_coalesces_writes() {
    let (store, inner) = grouped_store(Duration::from_millis(200));

    for i in 0..100 {
        store
            .write(|tx| {
                tx.insert(&format!("k{i}"), &format!("v{i}"));
                Ok(())
            })
            .unwrap();
    }

    std::thread::sleep(Duration::from_millis(400));

    let saves = inner.save_count.load(Ordering::Acquire);
    assert!(saves < 50, "expected coalesced saves (<50), got {saves}");

    let state = inner.state.lock().clone();
    assert_eq!(state.items.len(), 100);
    assert_eq!(state.items.get("k99").unwrap(), "v99");
}

#[test]
fn write_durable_bypasses_grouping() {
    let (store, inner) = grouped_store(Duration::from_secs(10));

    store
        .write_durable(|tx| {
            tx.insert("critical", "yes");
            Ok(())
        })
        .unwrap();

    assert_eq!(
        inner.save_count.load(Ordering::Acquire),
        1,
        "write_durable must save synchronously"
    );

    let state = inner.state.lock().clone();
    assert_eq!(state.items.get("critical").unwrap(), "yes");
}

#[test]
fn group_commit_error_propagation() {
    let backend = CountingBackend::new();
    let inner = Arc::clone(&backend.inner);
    let mut store = Store::with_backend(backend).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped {
        interval: Duration::from_millis(50),
    });

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    *inner.fail_next.lock() = Some(Error::Io(std::io::Error::other("disk full")));

    std::thread::sleep(Duration::from_millis(500));

    let result = store.write(|tx| {
        tx.insert("b", "2");
        Ok(())
    });
    assert!(result.is_err(), "write should propagate flusher error");

    store
        .write(|tx| {
            tx.insert("c", "3");
            Ok(())
        })
        .unwrap();
}

#[test]
fn group_commit_clean_shutdown() {
    let backend = CountingBackend::new();
    let inner = Arc::clone(&backend.inner);
    let mut store = Store::with_backend(backend).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped {
        interval: Duration::from_secs(5),
    });

    store
        .write(|tx| {
            tx.insert("k", "v");
            Ok(())
        })
        .unwrap();

    drop(store);

    let state = inner.state.lock().clone();
    assert_eq!(
        state.items.get("k").unwrap(),
        "v",
        "dirty state must be flushed on drop"
    );
}

#[test]
fn flush_forces_immediate_persist() {
    let (store, inner) = grouped_store(Duration::from_secs(5));

    store
        .write(|tx| {
            tx.insert("x", "1");
            Ok(())
        })
        .unwrap();

    store.flush().unwrap();

    let state = inner.state.lock().clone();
    assert_eq!(state.items.get("x").unwrap(), "1");
}

#[test]
fn flush_error_returns_latest() {
    let backend = CountingBackend::new();
    let inner = Arc::clone(&backend.inner);
    let mut store = Store::with_backend(backend).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped {
        interval: Duration::from_millis(50),
    });

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    *inner.fail_next.lock() = Some(Error::Io(std::io::Error::other("oops")));

    std::thread::sleep(Duration::from_millis(200));

    assert!(
        store.flush_error().is_some(),
        "flush_error should return the flusher's error"
    );
    assert!(
        store.flush_error().is_none(),
        "flush_error should be consumed after first call"
    );
}

#[test]
fn group_commit_file_backed_persists() {
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
    }

    {
        let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        let state = store.read();
        assert_eq!(state.items.len(), 10);
        assert_eq!(state.items.get("k9").unwrap(), "v9");
    }
}

#[test]
fn group_commit_concurrent_writers() {
    let (store, inner) = grouped_store(Duration::from_millis(50));

    let store = Arc::new(store);
    let mut handles = Vec::new();

    for t in 0..20 {
        let s = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            for i in 0..50 {
                s.write(|tx| {
                    tx.insert(&format!("t{t}_k{i}"), &format!("v{i}"));
                    Ok(())
                })
                .unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    store.flush().unwrap();

    let state = inner.state.lock().clone();
    assert_eq!(
        state.items.len(),
        1000,
        "all 20*50=1000 items must be present"
    );
}

#[test]
fn group_commit_mixed_write_and_durable() {
    let (store, inner) = grouped_store(Duration::from_millis(100));

    let store = Arc::new(store);
    let mut handles = Vec::new();

    for t in 0..10 {
        let s = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            for i in 0..20 {
                s.write(|tx| {
                    tx.insert(&format!("w{t}_{i}"), "regular");
                    Ok(())
                })
                .unwrap();
            }
        }));
    }

    for t in 0..5 {
        let s = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            for i in 0..10 {
                s.write_durable(|tx| {
                    tx.insert(&format!("d{t}_{i}"), "durable");
                    Ok(())
                })
                .unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    store.flush().unwrap();

    let state = inner.state.lock().clone();
    let expected = 10 * 20 + 5 * 10;
    assert_eq!(state.items.len(), expected);
}
