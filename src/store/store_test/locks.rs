use super::*;

// =========================================================================
// Lock-deadlock detector tests
//
// The store bounds every lock acquisition with a configurable budget.
// `read()` panics on timeout (bare guard, no error channel); the
// `Result`-bearing write paths return `Error::LockTimeout`. Tests use a
// sub-second budget to stay fast, and hand-off holder threads via a
// channel so nothing is leaked.
// =========================================================================

use crate::store::lock::{
    STATE_LOCK_DEADLOCK_TIMEOUT, read_or_panic, try_lock_gate_for, try_read_for,
};

/// Spawn a thread that holds a guard until signalled, returning the
/// stop-sender and join handle so the caller can shut it down cleanly.
fn hold_write_guard(
    lock: Arc<parking_lot::RwLock<u32>>,
) -> (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let _g = lock.write();
        let _ = rx.recv();
    });
    std::thread::sleep(Duration::from_millis(50));
    (tx, handle)
}

/// A writer holding the lock indefinitely makes `read_or_panic` panic
/// with the read-side diagnostic rather than hang.
#[test]
fn read_or_panic_panics_when_writer_holds_lock() {
    let lock: Arc<parking_lot::RwLock<u32>> = Arc::new(parking_lot::RwLock::new(0));
    let (tx, holder) = hold_write_guard(Arc::clone(&lock));

    let reader = Arc::clone(&lock);
    let panicked = std::thread::spawn(move || {
        let _g = read_or_panic(&reader, "test/read", Duration::from_millis(150));
    })
    .join();
    assert!(
        panicked.is_err(),
        "read_or_panic must panic when a writer holds the lock"
    );

    let _ = tx.send(());
    let _ = holder.join();
}

/// A writer holding the lock makes `try_read_for` return
/// `Error::LockTimeout` (used by the write paths' committed-state borrow).
#[test]
fn try_read_for_errors_when_writer_holds_lock() {
    let lock: Arc<parking_lot::RwLock<u32>> = Arc::new(parking_lot::RwLock::new(0));
    let (tx, holder) = hold_write_guard(Arc::clone(&lock));

    let err = try_read_for(&lock, "test/read", Duration::from_millis(150)).unwrap_err();
    assert!(
        matches!(err, Error::LockTimeout { .. }),
        "expected LockTimeout, got {err:?}"
    );

    let _ = tx.send(());
    let _ = holder.join();
}

/// The write-serialization gate reports a reentrancy hint when it times
/// out (the common cause is write()-in-write() on one thread).
#[test]
fn try_lock_gate_for_reports_reentrancy_cause() {
    let gate = parking_lot::Mutex::new(());
    let _held = gate.lock();

    let err = try_lock_gate_for(&gate, "write", Duration::from_millis(50)).unwrap_err();
    match err {
        Error::LockTimeout { site, cause, .. } => {
            assert_eq!(site, "write");
            assert!(
                cause.contains("reentrant"),
                "gate cause should name reentrancy, got: {cause}"
            );
        }
        other => panic!("expected LockTimeout, got {other:?}"),
    }
}

/// HIGH-severity regression: `write()` called from inside another
/// `write()` closure on the same thread must return `Error::LockTimeout`
/// at the budget instead of self-deadlocking on the write gate forever.
#[test]
fn write_in_write_returns_lock_timeout_not_hang() {
    let store: Arc<Store<TestState>> = Arc::new(Store::memory());
    store.set_lock_deadlock_timeout(Duration::from_millis(100));

    let inner = Arc::clone(&store);
    let started = std::time::Instant::now();
    let result = store.write(|tx| {
        tx.insert("outer", "1");
        // Reentrant write on the same thread — the gate is already held
        // by the outer write, so this must time out, not hang.
        inner.write(|tx2| {
            tx2.insert("inner", "2");
            Ok(())
        })
    });

    assert!(
        matches!(result, Err(Error::LockTimeout { .. })),
        "reentrant write must return LockTimeout, got {result:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "must fail fast at the budget, not hang"
    );
}

/// `retry_quarantine` is a write, and takes the write gate for its whole
/// duration.
///
/// Proving that directly is awkward — the gate is private and the method is
/// usually a no-op — so this leans on the gate being non-reentrant: calling
/// `retry_quarantine` from inside a `write()` closure on the same thread must
/// time out on the gate rather than sail through and append to `wal.bin`.
/// Before the fix it took no gate at all and returned `Ok(0)` here, which is
/// exactly what let it append into a concurrent `checkpoint_to`'s copy window.
#[test]
fn retry_quarantine_in_write_returns_lock_timeout_not_ok() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<WalStore> = Arc::new(Store::open_wal(dir.path().to_path_buf()).unwrap());
    store.set_lock_deadlock_timeout(Duration::from_millis(100));

    let inner = Arc::clone(&store);
    let started = std::time::Instant::now();
    let result = store.write(|tx| {
        tx.insert("outer", "1");
        inner.retry_quarantine()
    });

    assert!(
        matches!(result, Err(Error::LockTimeout { site, .. }) if site == "retry_quarantine"),
        "retry_quarantine must contend on the write gate, got {result:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "must fail fast at the budget, not hang"
    );
}

/// `set_lock_deadlock_timeout` takes effect on the next `read()`:
/// lowering the budget makes a contended read panic near the configured
/// value instead of the default 30 s. Reaches into the private `state`
/// field to hold the inner write guard directly (bypassing the
/// `Transactable` bound on `write`).
#[test]
fn set_lock_deadlock_timeout_changes_read_budget() {
    let store: Arc<Store<u32>> = Arc::new(Store::memory());
    assert_eq!(
        store.lock_deadlock_timeout(),
        STATE_LOCK_DEADLOCK_TIMEOUT,
        "default should match the const"
    );
    store.set_lock_deadlock_timeout(Duration::from_millis(150));
    assert_eq!(
        store.lock_deadlock_timeout(),
        Duration::from_millis(150),
        "setter must round-trip"
    );

    let state_holder = Arc::clone(&store.state);
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let _g = state_holder.write();
        let _ = rx.recv();
    });
    std::thread::sleep(Duration::from_millis(50));

    let started = std::time::Instant::now();
    let reader = Arc::clone(&store);
    let panicked = std::thread::spawn(move || {
        let _r = reader.read();
    })
    .join();
    let elapsed = started.elapsed();

    assert!(panicked.is_err(), "read must panic when budget exceeded");
    assert!(
        elapsed < Duration::from_secs(2),
        "read must panic near the configured budget, not the default 30 s; took {elapsed:?}"
    );

    let _ = tx.send(());
    let _ = holder.join();
}

/// A sub-millisecond configured budget is floored to `MIN_LOCK_WAIT`
/// rather than truncated to zero, so an uncontended read still succeeds
/// instead of panicking instantly.
#[test]
fn sub_millisecond_budget_does_not_instant_timeout() {
    let store: Store<u32> = Store::memory();
    store.set_lock_deadlock_timeout(Duration::from_micros(200));
    assert_eq!(
        store.lock_deadlock_timeout(),
        Duration::from_micros(200),
        "sub-ms budget must round-trip with microsecond precision"
    );
    // Uncontended: must return a guard, not panic on a truncated-to-zero
    // budget.
    assert_eq!(*store.read(), 0);
}

/// `read_with` scopes the guard: it runs the closure and releases the
/// read lock, so a subsequent `write()` on the same store does not
/// deadlock.
#[test]
fn read_with_scopes_guard_and_allows_later_write() {
    let store: Store<TestState> = Store::memory();
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    let value = store.read_with(|state| state.items.get("a").cloned());
    assert_eq!(value.as_deref(), Some("1"));

    // The read guard from read_with is already dropped, so this write
    // proceeds without contention.
    store
        .write(|tx| {
            tx.insert("b", "2");
            Ok(())
        })
        .unwrap();
    assert_eq!(store.read_with(|s| s.items.len()), 2);
}

/// Lock helpers return their guard when the lock is free, even under a
/// sub-second budget.
#[test]
fn lock_helpers_succeed_when_free() {
    let lock: parking_lot::RwLock<u32> = parking_lot::RwLock::new(42);
    {
        let g = read_or_panic(&lock, "test/read-ok", Duration::from_millis(50));
        assert_eq!(*g, 42);
    }
    {
        let mut g =
            crate::store::lock::try_write_for(&lock, "test/write-ok", Duration::from_millis(50))
                .unwrap();
        *g = 100;
    }
    assert_eq!(
        *try_read_for(&lock, "test/read2", Duration::from_millis(50)).unwrap(),
        100
    );
}
