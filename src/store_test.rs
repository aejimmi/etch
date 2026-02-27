use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tempfile::TempDir;

use crate::backend::{Backend, PostcardBackend};
use crate::error::Error;
use crate::store::{FlushPolicy, Store};
use crate::wal::{Diffable, Op};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct TestState {
    #[serde(default)]
    items: BTreeMap<String, String>,
}

impl Diffable for TestState {
    fn diff(before: &Self, after: &Self) -> Vec<Op> {
        let mut ops = Vec::new();
        // Deleted keys.
        for k in before.items.keys() {
            if !after.items.contains_key(k) {
                ops.push(Op::Delete {
                    collection: 0,
                    key: k.clone(),
                });
            }
        }
        // Added or changed keys.
        for (k, v) in &after.items {
            match before.items.get(k) {
                Some(bv) if bv == v => {}
                _ => {
                    ops.push(Op::Put {
                        collection: 0,
                        key: k.clone(),
                        value: postcard::to_allocvec(v).unwrap(),
                    });
                }
            }
        }
        ops
    }

    fn apply(&mut self, ops: &[Op]) -> crate::error::Result<()> {
        for op in ops {
            match op {
                Op::Put { key, value, .. } => {
                    let v: String = postcard::from_bytes(value)?;
                    self.items.insert(key.clone(), v);
                }
                Op::Delete { key, .. } => {
                    self.items.remove(key);
                }
            }
        }
        Ok(())
    }
}

type FileStore = Store<TestState, PostcardBackend>;

#[test]
fn memory_store_read_write() {
    let store = Store::<TestState>::memory();

    store
        .write(|s| {
            s.items.insert("a".into(), "1".into());
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get("a").unwrap(), "1");
    assert_eq!(state.items.len(), 1);
}

#[test]
fn file_store_persists_across_opens() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.postcard");

    // Write
    {
        let store: FileStore = Store::open_postcard(path.clone()).unwrap();
        store
            .write(|s: &mut TestState| {
                s.items.insert("key".into(), "value".into());
                Ok(())
            })
            .unwrap();
    }

    // Re-open and verify
    {
        let store: FileStore = Store::open_postcard(path).unwrap();
        let state = store.read();
        assert_eq!(state.items.get("key").unwrap(), "value");
    }
}

#[test]
fn write_rollback_on_error() {
    let store = Store::<TestState>::memory();

    store
        .write(|s| {
            s.items.insert("good".into(), "data".into());
            Ok(())
        })
        .unwrap();

    // This write should fail and not affect state
    let result: std::result::Result<(), Error> = store.write(|s| {
        s.items.insert("bad".into(), "data".into());
        Err(Error::invalid("test", "forced error"))
    });
    assert!(result.is_err());

    let state = store.read();
    assert!(state.items.contains_key("good"));
    assert!(!state.items.contains_key("bad"));
}

#[test]
fn open_missing_file_returns_default() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("nonexistent.postcard");

    let store: FileStore = Store::open_postcard(path).unwrap();
    assert!(store.read().items.is_empty());
}

#[test]
fn backup_file_created_on_write() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.postcard");
    let bak = dir.path().join("state.bak");

    let store: FileStore = Store::open_postcard(path.clone()).unwrap();

    // First write — no backup yet (no prior file)
    store
        .write(|s: &mut TestState| {
            s.items.insert("v1".into(), "1".into());
            Ok(())
        })
        .unwrap();

    // Second write — should create backup of first state
    store
        .write(|s: &mut TestState| {
            s.items.insert("v2".into(), "2".into());
            Ok(())
        })
        .unwrap();

    assert!(bak.exists(), "backup file should exist after second write");

    // Backup should contain v1 state (before second write)
    let bak_bytes = std::fs::read(&bak).unwrap();
    let bak_state: TestState = postcard::from_bytes(&bak_bytes).unwrap();
    assert!(bak_state.items.contains_key("v1"));
    assert!(!bak_state.items.contains_key("v2"));
}

#[test]
fn recovery_from_backup() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.postcard");
    let bak = dir.path().join("state.bak");

    // Create a backup file directly (simulating crash after persist)
    let state = TestState {
        items: BTreeMap::from([("recovered".into(), "yes".into())]),
    };
    std::fs::write(&bak, postcard::to_allocvec(&state).unwrap()).unwrap();

    // Open with missing main file — should load backup
    let store: FileStore = Store::open_postcard(path).unwrap();
    assert_eq!(store.read().items.get("recovered").unwrap(), "yes");
}

#[test]
fn tmp_file_cleaned_on_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.postcard");
    let tmp = dir.path().join("state.tmp");

    // Create main file and stale tmp (valid empty postcard state)
    let empty = postcard::to_allocvec(&TestState::default()).unwrap();
    std::fs::write(&path, &empty).unwrap();
    std::fs::write(&tmp, b"stale").unwrap();

    let _store: FileStore = Store::open_postcard(path).unwrap();
    assert!(!tmp.exists(), "stale tmp should be cleaned up on open");
}

#[test]
fn concurrent_reads() {
    let store = Store::<TestState>::memory();
    store
        .write(|s| {
            s.items.insert("x".into(), "1".into());
            Ok(())
        })
        .unwrap();

    // Multiple concurrent read guards
    let r1 = store.read();
    let r2 = store.read();
    assert_eq!(r1.items.get("x"), r2.items.get("x"));
}

// =========================================================================
// Mock backend for group commit tests
// =========================================================================

/// Shared counting state, held behind Arc so CountingBackend is Clone-friendly.
struct CountingInner {
    save_count: AtomicU64,
    state: parking_lot::Mutex<TestState>,
    fail_next: parking_lot::Mutex<Option<Error>>,
}

/// Backend that counts save calls and stores latest state in memory.
struct CountingBackend {
    inner: Arc<CountingInner>,
}

#[allow(dead_code)]
impl CountingBackend {
    fn new() -> Self {
        Self {
            inner: Arc::new(CountingInner {
                save_count: AtomicU64::new(0),
                state: parking_lot::Mutex::new(TestState::default()),
                fail_next: parking_lot::Mutex::new(None),
            }),
        }
    }

    fn saves(&self) -> u64 {
        self.inner.save_count.load(Ordering::Acquire)
    }

    fn persisted_state(&self) -> TestState {
        self.inner.state.lock().clone()
    }

    fn set_fail_next(&self, err: Error) {
        *self.inner.fail_next.lock() = Some(err);
    }
}

impl Backend<TestState> for CountingBackend {
    fn load(&self) -> crate::error::Result<TestState> {
        Ok(self.inner.state.lock().clone())
    }

    fn save(&self, state: &TestState) -> crate::error::Result<()> {
        if let Some(err) = self.inner.fail_next.lock().take() {
            return Err(err);
        }
        *self.inner.state.lock() = state.clone();
        self.inner.save_count.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

/// Helper to create a grouped store with a counting backend.
fn grouped_store(interval: Duration) -> (Store<TestState, CountingBackend>, Arc<CountingInner>) {
    let backend = CountingBackend::new();
    let inner = Arc::clone(&backend.inner);
    let mut store = Store::with_backend(backend).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped { interval });
    (store, inner)
}

// =========================================================================
// Group commit unit tests
// =========================================================================

#[test]
fn group_commit_coalesces_writes() {
    let (store, inner) = grouped_store(Duration::from_millis(200));

    // Rapid-fire 100 writes — should coalesce into very few fsyncs.
    for i in 0..100 {
        store
            .write(|s| {
                s.items.insert(format!("k{i}"), format!("v{i}"));
                Ok(())
            })
            .unwrap();
    }

    // Wait for the flusher to persist.
    std::thread::sleep(Duration::from_millis(400));

    let saves = inner.save_count.load(Ordering::Acquire);
    // With a mock backend (no real fsync), saves happen faster, but still
    // far fewer than 100. Real backends with 6-8ms fsync coalesce much harder.
    assert!(saves < 50, "expected coalesced saves (<50), got {saves}");

    // Verify final state is complete.
    let state = inner.state.lock().clone();
    assert_eq!(state.items.len(), 100);
    assert_eq!(state.items.get("k99").unwrap(), "v99");
}

#[test]
fn write_durable_bypasses_grouping() {
    let (store, inner) = grouped_store(Duration::from_secs(10));

    // write_durable should persist immediately, even with long interval.
    store
        .write_durable(|s| {
            s.items.insert("critical".into(), "yes".into());
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

    // First write succeeds in-memory.
    store
        .write(|s| {
            s.items.insert("a".into(), "1".into());
            Ok(())
        })
        .unwrap();

    // Inject an error into the backend.
    *inner.fail_next.lock() = Some(Error::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        "disk full",
    )));

    // Wait for flusher to hit the error.
    std::thread::sleep(Duration::from_millis(200));

    // Next write should return the flusher's error.
    let result = store.write(|s| {
        s.items.insert("b".into(), "2".into());
        Ok(())
    });
    assert!(result.is_err(), "write should propagate flusher error");

    // Subsequent write should succeed (error was consumed).
    store
        .write(|s| {
            s.items.insert("c".into(), "3".into());
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

    // Write but don't wait for flush.
    store
        .write(|s| {
            s.items.insert("k".into(), "v".into());
            Ok(())
        })
        .unwrap();

    // Drop triggers shutdown + final flush.
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
        .write(|s| {
            s.items.insert("x".into(), "1".into());
            Ok(())
        })
        .unwrap();

    // flush() should persist immediately.
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
        .write(|s| {
            s.items.insert("a".into(), "1".into());
            Ok(())
        })
        .unwrap();

    *inner.fail_next.lock() = Some(Error::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        "oops",
    )));

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
    let path = dir.path().join("state.postcard");

    // Write with grouped policy, then drop.
    {
        let mut store: FileStore = Store::open_postcard(path.clone()).unwrap();
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_millis(50),
        });

        for i in 0..10 {
            store
                .write(|s| {
                    s.items.insert(format!("k{i}"), format!("v{i}"));
                    Ok(())
                })
                .unwrap();
        }
    } // drop flushes

    // Re-open and verify everything persisted.
    {
        let store: FileStore = Store::open_postcard(path).unwrap();
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
                s.write(|state| {
                    state.items.insert(format!("t{t}_k{i}"), format!("v{i}"));
                    Ok(())
                })
                .unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Flush to ensure everything persisted.
    // Need to get past the Arc — use the flush method directly.
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

    // Regular writers.
    for t in 0..10 {
        let s = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            for i in 0..20 {
                s.write(|state| {
                    state.items.insert(format!("w{t}_{i}"), "regular".into());
                    Ok(())
                })
                .unwrap();
            }
        }));
    }

    // Durable writers.
    for t in 0..5 {
        let s = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            for i in 0..10 {
                s.write_durable(|state| {
                    state.items.insert(format!("d{t}_{i}"), "durable".into());
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
    let expected = 10 * 20 + 5 * 10; // 250
    assert_eq!(state.items.len(), expected);
}
