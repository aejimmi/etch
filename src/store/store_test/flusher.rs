use super::*;
use crate::backend::NullBackend;
use std::time::Instant;

// =========================================================================
// Flusher WAL branch tests
// =========================================================================

#[test]
fn flusher_wal_triggers_snapshot() {
    let (store, mock) = mock_wal_store(3, Duration::from_millis(30));

    for i in 0..5 {
        store
            .write(|tx| {
                tx.insert(&format!("k{i}"), &format!("v{i}"));
                Ok(())
            })
            .unwrap();
    }

    std::thread::sleep(Duration::from_millis(200));
    store.flush().unwrap();

    assert!(
        mock.snapshot_count.load(Ordering::Acquire) >= 1,
        "snapshot should have been triggered"
    );
}

#[test]
fn flusher_wal_save_ops_error_propagates() {
    let (store, mock) = mock_wal_store(1000, Duration::from_millis(30));

    mock.fail_save_ops.store(true, Ordering::Release);

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    std::thread::sleep(Duration::from_millis(200));

    let result = store.write(|tx| {
        tx.insert("b", "2");
        Ok(())
    });
    assert!(
        result.is_err(),
        "write should propagate flusher save_ops error"
    );
}

#[test]
fn flusher_wal_sync_error_propagates() {
    let (store, mock) = mock_wal_store(1000, Duration::from_millis(30));

    mock.fail_sync.store(true, Ordering::Release);

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    std::thread::sleep(Duration::from_millis(200));

    let result = store.write(|tx| {
        tx.insert("b", "2");
        Ok(())
    });
    assert!(result.is_err(), "write should propagate flusher sync error");
}

#[test]
fn flush_discovers_flusher_wal_error() {
    let (store, mock) = mock_wal_store(1000, Duration::from_millis(30));

    mock.fail_sync.store(true, Ordering::Release);

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    let result = store.flush();
    assert!(result.is_err(), "flush should return flusher error");
}

// =========================================================================
// Flush post-loop error detection
// =========================================================================

/// After a successful flush, flush_error() is None and a second flush()
/// is a no-op (gen already caught up).
#[test]
fn flush_no_error_after_success() {
    let (store, _mock) = mock_wal_store(1000, Duration::from_millis(30));

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    store.flush().unwrap();
    assert!(store.flush_error().is_none());

    // Second flush is a no-op since gen is already caught up.
    store.flush().unwrap();
}

// =========================================================================
// Backend edge case tests
// =========================================================================

#[test]
fn null_backend_load_via_with_backend() {
    let store: Store<TestState, NullBackend> = Store::with_backend(NullBackend).unwrap();
    assert!(store.read().items.is_empty());
}

#[test]
fn last_flush_error_peek_is_non_consuming() {
    let backend = CountingBackend::new();
    let inner = Arc::clone(&backend.inner);
    let mut store = Store::with_backend(backend).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped {
        interval: Duration::from_millis(50),
    });

    // Arm the failure BEFORE the write so the queued op deterministically
    // fails when the flusher drains it. (Arming after the write races the
    // 50ms flusher, which under load can drain the op successfully before
    // the failure is set, leaving no error to observe.) Nothing is stashed
    // yet, so this write does not consume a phantom error.
    *inner.fail_next.lock() = Some(Error::Io(std::io::Error::other("boom")));
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    // Poll for the flusher to attempt and fail, rather than a fixed sleep.
    let deadline = Instant::now() + Duration::from_secs(5);
    while store.last_flush_error().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    // Peek is non-consuming: the recorded error survives repeated polls.
    assert!(
        store.last_flush_error().is_some(),
        "peek must observe error"
    );
    assert!(
        store.last_flush_error().is_some(),
        "peek must not consume the error"
    );

    // The consuming accessor clears it; the peek then reads clean.
    assert!(
        store.flush_error().is_some(),
        "consuming accessor returns it"
    );
    assert!(
        store.last_flush_error().is_none(),
        "acknowledged error is cleared"
    );
}
