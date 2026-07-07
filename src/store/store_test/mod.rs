use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::backend::Backend;
use crate::error::Error;
use crate::store::{FlushPolicy, Store};
use crate::wal::{
    IncrementalSave, Op, Overlay, Replayable, Transactable, WalBackend, apply_overlay_btree,
};

mod basic;
mod concurrency;
mod enospc;
mod flusher;
mod grouped;
mod locks;
mod wal;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct TestState {
    #[serde(default)]
    items: BTreeMap<String, String>,
}

impl Replayable for TestState {
    fn apply_with_format(
        &mut self,
        ops: &[Op],
        _format: crate::wal::ReplayFormat,
    ) -> crate::error::Result<()> {
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
    items: Overlay<String, String>,
    ops: Vec<Op>,
}

struct TestOverlay {
    items: Overlay<String, String>,
}

impl<'a> TestTx<'a> {
    fn insert(&mut self, key: &str, value: &str) {
        self.ops.push(Op::Put {
            collection: 0,
            key: key.as_bytes().to_vec(),
            value: postcard::to_allocvec(&value.to_string()).unwrap(),
        });
        self.items.put(key.to_string(), value.to_string());
    }

    #[allow(dead_code)]
    fn get(&self, key: &str) -> Option<&String> {
        self.items.get(&self.committed.items, &key.to_string())
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
        apply_overlay_btree(&mut self.items, overlay.items);
    }
}

type WalStore = Store<TestState, WalBackend<TestState>>;

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
        lock_deadlock_timeout_us: std::sync::atomic::AtomicU64::new(super::duration_to_us(
            super::STATE_LOCK_DEADLOCK_TIMEOUT,
        )),
        replay_report: crate::ReplayReport::default(),
    };
    store.set_flush_policy(FlushPolicy::Grouped { interval });

    (store, mock)
}
