use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tempfile::TempDir;

use crate::backend::{Backend, NullBackend, PostcardBackend};
use crate::error::Error;
use crate::store::{FlushPolicy, Store};
use crate::wal::{
    IncrementalSave, Op, Overlay, Replayable, Transactable, WalBackend, apply_overlay_map,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct TestState {
    #[serde(default)]
    items: BTreeMap<String, String>,
}

impl Replayable for TestState {
    fn apply(&mut self, ops: &[Op]) -> crate::error::Result<()> {
        for op in ops {
            crate::wal::apply_op(&mut self.items, op)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Transactable
// ---------------------------------------------------------------------------

struct TestTx<'a> {
    committed: &'a TestState,
    items: Overlay<String>,
    ops: Vec<Op>,
}

struct TestOverlay {
    items: Overlay<String>,
}

impl<'a> TestTx<'a> {
    fn insert(&mut self, key: &str, value: &str) {
        self.ops.push(Op::Put {
            collection: 0,
            key: key.to_string(),
            value: postcard::to_allocvec(&value.to_string()).unwrap(),
        });
        self.items.put(key.to_string(), value.to_string());
    }

    #[allow(dead_code)]
    fn get(&self, key: &str) -> Option<&String> {
        self.items.get(&self.committed.items, key)
    }
}

impl Transactable for TestState {
    type Tx<'a> = TestTx<'a>;
    type Overlay = TestOverlay;

    fn begin_tx(&self) -> TestTx<'_> {
        TestTx {
            committed: self,
            items: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: TestTx<'_>) -> (Vec<Op>, TestOverlay) {
        (tx.ops, TestOverlay { items: tx.items })
    }

    fn apply_overlay(&mut self, overlay: TestOverlay) {
        apply_overlay_map(&mut self.items, overlay.items);
    }
}

type FileStore = Store<TestState, PostcardBackend>;
type WalStore = Store<TestState, WalBackend<TestState>>;

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
fn file_store_persists_across_opens() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.postcard");

    // Write
    {
        let store: FileStore = Store::open_postcard(path.clone()).unwrap();
        store
            .write(|tx| {
                tx.insert("key", "value");
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
        .write(|tx| {
            tx.insert("v1", "1");
            Ok(())
        })
        .unwrap();

    // Second write — should create backup of first state
    store
        .write(|tx| {
            tx.insert("v2", "2");
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
            .write(|tx| {
                tx.insert(&format!("k{i}"), &format!("v{i}"));
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

    // First write succeeds in-memory.
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    // Inject an error into the backend.
    *inner.fail_next.lock() = Some(Error::Io(std::io::Error::other("disk full")));

    // Wait for flusher to hit the error.
    std::thread::sleep(Duration::from_millis(500));

    // Next write should return the flusher's error.
    let result = store.write(|tx| {
        tx.insert("b", "2");
        Ok(())
    });
    assert!(result.is_err(), "write should propagate flusher error");

    // Subsequent write should succeed (error was consumed).
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

    // Write but don't wait for flush.
    store
        .write(|tx| {
            tx.insert("k", "v");
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
        .write(|tx| {
            tx.insert("x", "1");
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
    let path = dir.path().join("state.postcard");

    // Write with grouped policy, then drop.
    {
        let mut store: FileStore = Store::open_postcard(path.clone()).unwrap();
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

    // Flush to ensure everything persisted.
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
                s.write(|tx| {
                    tx.insert(&format!("w{t}_{i}"), "regular");
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
    let expected = 10 * 20 + 5 * 10; // 250
    assert_eq!(state.items.len(), expected);
}

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

    // Reopen and verify state survived.
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
    // Verify backend() returns a reference.
    let _backend = store.backend();
}

// =========================================================================
// WAL store + grouped flush (covers WAL flusher_loop + grouped write paths)
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

        // Wait for the grouped flusher to run and persist WAL ops.
        std::thread::sleep(Duration::from_millis(200));
        store.flush().unwrap();
    }

    // Reopen and verify.
    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = store.read();
    assert_eq!(state.items.len(), 10);
    assert_eq!(state.items.get("k9").unwrap(), "v9");
}

#[test]
fn wal_store_grouped_flusher_processes_ops() {
    // Ensure the flusher loop WAL branch actually fires by writing,
    // then waiting for the flusher interval to elapse.
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

    // Let the flusher tick and process the pending ops.
    std::thread::sleep(Duration::from_millis(100));

    // Verify data is readable (flusher synced the WAL).
    assert_eq!(store.read().items.get("a").unwrap(), "1");

    // flush should be a no-op now (already flushed).
    store.flush().unwrap();
}

#[test]
fn wal_store_grouped_write_durable_drains_pending() {
    let dir = TempDir::new().unwrap();

    {
        let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_secs(10), // long interval — won't auto-flush
        });

        // Buffer some writes (grouped, not flushed yet).
        store
            .write(|tx| {
                tx.insert("buffered", "yes");
                Ok(())
            })
            .unwrap();

        // write_durable should drain pending + fsync immediately.
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

    // Reopen and verify everything survived.
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

    // Switch back to immediate mode.
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
    // flush() on an immediate-mode store should be a no-op.
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
    // Second flush — already caught up, should return immediately.
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
fn postcard_backend_path_accessor() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.postcard");

    let store: FileStore = Store::open_postcard(path.clone()).unwrap();
    assert_eq!(store.backend().path(), path);
}

#[test]
fn wal_store_grouped_empty_write_noop() {
    // Exercises the WAL grouped path where ops is empty (line 219).
    let dir = TempDir::new().unwrap();

    let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    store.set_flush_policy(FlushPolicy::Grouped {
        interval: Duration::from_millis(50),
    });

    // A write that produces no ops (no mutations in the tx).
    store
        .write(|_tx| {
            // No inserts — empty ops vec.
            Ok(())
        })
        .unwrap();

    store.flush().unwrap();
    assert!(store.read().items.is_empty());
}

#[test]
fn wal_store_write_durable_empty_ops() {
    // Exercises the WAL write_durable path where ops is empty (line 275).
    let dir = TempDir::new().unwrap();

    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();

    store
        .write_durable(|_tx| {
            // No mutations — empty ops.
            Ok(())
        })
        .unwrap();

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

    // Data should have been flushed on close.
    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    assert_eq!(store.read().items.get("k").unwrap(), "v");
}

// =========================================================================
// Mock IncrementalSave for testing flusher WAL code paths
// =========================================================================

struct MockIncremental {
    fail_save_ops: AtomicBool,
    fail_sync: AtomicBool,
    entry_count: AtomicU64,
    snapshot_threshold: u64,
    snapshot_count: AtomicU64,
}

impl MockIncremental {
    fn new(snapshot_threshold: u64) -> Self {
        Self {
            fail_save_ops: AtomicBool::new(false),
            fail_sync: AtomicBool::new(false),
            entry_count: AtomicU64::new(0),
            snapshot_threshold,
            snapshot_count: AtomicU64::new(0),
        }
    }
}

impl IncrementalSave<TestState> for MockIncremental {
    fn save_ops(&self, ops: &[Op]) -> crate::error::Result<()> {
        if self.fail_save_ops.load(Ordering::Acquire) {
            return Err(Error::Io(std::io::Error::other("mock save_ops failure")));
        }
        if !ops.is_empty() {
            self.entry_count.fetch_add(1, Ordering::Release);
        }
        Ok(())
    }

    fn sync(&self) -> crate::error::Result<()> {
        if self.fail_sync.load(Ordering::Acquire) {
            return Err(Error::Io(std::io::Error::other("mock sync failure")));
        }
        Ok(())
    }

    fn should_snapshot(&self) -> bool {
        self.entry_count.load(Ordering::Acquire) >= self.snapshot_threshold
    }

    fn snapshot(&self, _state: &TestState) -> crate::error::Result<()> {
        self.snapshot_count.fetch_add(1, Ordering::Release);
        self.entry_count.store(0, Ordering::Release);
        Ok(())
    }
}

/// Helper: build a grouped store with a mock IncrementalSave.
/// Since store_test is a child module of store, we can access private fields.
fn mock_wal_store(
    threshold: u64,
    interval: Duration,
) -> (Store<TestState, CountingBackend>, Arc<MockIncremental>) {
    let backend = CountingBackend::new();
    let mock = Arc::new(MockIncremental::new(threshold));

    let mut store = Store {
        state: Arc::new(parking_lot::RwLock::new(TestState::default())),
        write_gate: parking_lot::Mutex::new(()),
        backend: Arc::new(backend),
        incremental: Some(Arc::clone(&mock) as Arc<dyn IncrementalSave<TestState>>),
        shared: None,
        flusher: None,
    };
    store.set_flush_policy(FlushPolicy::Grouped { interval });

    (store, mock)
}

// =========================================================================
// Flusher WAL branch tests (store.rs lines 421-448)
// =========================================================================

#[test]
fn flusher_wal_triggers_snapshot() {
    // Covers flusher_loop WAL snapshot compaction (lines 440-441).
    let (store, mock) = mock_wal_store(3, Duration::from_millis(30));

    for i in 0..5 {
        store
            .write(|tx| {
                tx.insert(&format!("k{i}"), &format!("v{i}"));
                Ok(())
            })
            .unwrap();
    }

    // Wait for flusher to process ops and trigger snapshot.
    std::thread::sleep(Duration::from_millis(200));
    store.flush().unwrap();

    assert!(
        mock.snapshot_count.load(Ordering::Acquire) >= 1,
        "snapshot should have been triggered"
    );
}

#[test]
fn flusher_wal_save_ops_error_propagates() {
    // Covers flusher_loop WAL save_ops error (lines 430-431, 435).
    let (store, mock) = mock_wal_store(1000, Duration::from_millis(30));

    // Set up failure before writes.
    mock.fail_save_ops.store(true, Ordering::Release);

    // write() just buffers ops — doesn't call save_ops directly.
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    // Wait for flusher to attempt save_ops and fail.
    std::thread::sleep(Duration::from_millis(200));

    // Next write should see the propagated error.
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
    // Covers flusher_loop WAL sync error (line 446).
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
    // Covers flush() error-during-wait path (line 372).
    let (store, mock) = mock_wal_store(1000, Duration::from_millis(30));

    mock.fail_sync.store(true, Ordering::Release);

    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    // flush() should loop, discover the flusher error, and return it.
    let result = store.flush();
    assert!(result.is_err(), "flush should return flusher error");
}

// =========================================================================
// Backend edge case tests
// =========================================================================

#[test]
fn null_backend_load_via_with_backend() {
    // Covers NullBackend::load (backend.rs lines 94-96).
    let store: Store<TestState, NullBackend> = Store::with_backend(NullBackend).unwrap();
    assert!(store.read().items.is_empty());
}

#[test]
fn postcard_backend_load_non_notfound_error() {
    // Covers backend.rs line 60 — I/O error that isn't NotFound.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.postcard");

    // Create a directory at the file path — reading it will fail with a non-NotFound error.
    std::fs::create_dir(&path).unwrap();

    let backend = PostcardBackend::new(path).unwrap();
    let result: crate::error::Result<TestState> = backend.load();
    assert!(result.is_err());

    match result.unwrap_err() {
        Error::Io(e) => assert_ne!(e.kind(), std::io::ErrorKind::NotFound),
        other => panic!("expected Io error, got: {other}"),
    }
}
