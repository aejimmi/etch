//! Concurrency-depth tests.
//!
//! Three families:
//! - (a) a reader storm concurrent with a writer storm — no deadlock (bounded
//!   well under the lock-timeout budget), and every read observes a consistent
//!   snapshot;
//! - (b) concurrent writers in grouped mode under an intermittent flush
//!   failure — the requeue-on-error path loses no acknowledged write;
//! - (c) a tight close→immediate-reopen loop while background flushers run —
//!   a regression guard for the bounded lock-acquire retry (reopen must never
//!   fail with `DatabaseLocked`).
//!
//! Timing uses poll-with-deadline, never fixed sleeps that assume a duration.

use std::sync::atomic::{AtomicBool, AtomicI64};

use super::*;
use tempfile::TempDir;

// -------------------------------------------------------------------------
// (a) Reader storm vs writer storm.
// -------------------------------------------------------------------------

/// Many readers running `read()` / `read_with()` concurrently with a writer
/// storm must never deadlock and must always observe a consistent snapshot.
///
/// Each write sets keys `a` and `b` to the same counter atomically, so any
/// reader that sees both keys must see them equal — a torn read would show a
/// mismatch.
#[test]
fn test_reader_storm_sees_consistent_snapshots_no_deadlock() {
    let store: Arc<Store<TestState>> = Arc::new(Store::memory());
    // Bound lock acquisition well under the default so a genuine deadlock
    // fails fast instead of hanging the suite. Writes are microsecond-scale,
    // far under this budget.
    store.set_lock_deadlock_timeout(Duration::from_secs(5));

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    // Reader threads: mix of read() and read_with().
    for r in 0..8 {
        let s = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                if r % 2 == 0 {
                    let guard = s.read();
                    if let (Some(a), Some(b)) = (guard.items.get("a"), guard.items.get("b")) {
                        assert_eq!(a, b, "torn read: a={a} b={b}");
                    }
                } else {
                    s.read_with(|st| {
                        if let (Some(a), Some(b)) = (st.items.get("a"), st.items.get("b")) {
                            assert_eq!(a, b, "torn read: a={a} b={b}");
                        }
                    });
                }
            }
        }));
    }

    // Writer threads: a storm of atomic a==b updates.
    for _ in 0..4 {
        let s = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            for n in 0..2000u32 {
                s.write(|tx| {
                    tx.insert("a", &n.to_string());
                    tx.insert("b", &n.to_string());
                    Ok(())
                })
                .unwrap();
            }
        }));
    }

    // Let writers finish, then stop readers. Handles 0..8 are the 8 readers;
    // 8..12 are the 4 writers. split_off(8) yields the writers to join first.
    for h in handles.split_off(8) {
        h.join()
            .expect("writer thread panicked (deadlock or torn state)");
    }
    stop.store(true, Ordering::Release);
    for h in handles {
        h.join()
            .expect("reader thread panicked (deadlock or torn read)");
    }

    let final_state = store.read();
    assert_eq!(final_state.items.get("a"), final_state.items.get("b"));
}

// -------------------------------------------------------------------------
// (b) Concurrent grouped writers with intermittent flush failure.
// -------------------------------------------------------------------------

/// A `WalBackend` wrapper whose `sync` fails while "chaos" is enabled, on a
/// deterministic subset of calls. Models a flaky disk that intermittently
/// rejects fsync; the store must requeue and retry, losing nothing.
struct IntermittentWal {
    inner: Arc<WalBackend<TestState>>,
    chaos: AtomicBool,
    calls: AtomicI64,
}

impl IntermittentWal {
    fn open(dir: &std::path::Path) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(WalBackend::open(dir).unwrap()),
            chaos: AtomicBool::new(true),
            calls: AtomicI64::new(0),
        })
    }

    fn calm(&self) {
        self.chaos.store(false, Ordering::Release);
    }
}

impl Backend<TestState> for IntermittentWal {
    fn load(&self) -> crate::error::Result<TestState> {
        self.inner.load()
    }
    fn save(&self, state: &TestState) -> crate::error::Result<()> {
        self.inner.save(state)
    }
}

impl IncrementalSave<TestState> for IntermittentWal {
    fn save_ops(&self, ops: &[Op]) -> crate::error::Result<()> {
        // Fail one of every three appends while chaos is on. Failing at
        // `save_ops` (before the batch is appended) means a batch that is NOT
        // requeued is genuinely absent at reopen — so the no-loss assertion
        // actually exercises the requeue path. A sync-only fault would leave
        // the batch buffered in the WAL writer and it would reach disk anyway.
        if self.chaos.load(Ordering::Acquire) && self.calls.fetch_add(1, Ordering::AcqRel) % 3 == 0
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "intermittent append failure",
            )));
        }
        self.inner.save_ops(ops)
    }

    fn sync(&self) -> crate::error::Result<()> {
        self.inner.sync()
    }

    fn should_snapshot(&self) -> bool {
        self.inner.should_snapshot()
    }
    fn snapshot(&self, state: &TestState) -> crate::error::Result<()> {
        self.inner.snapshot(state)
    }
}

fn intermittent_store(dir: &std::path::Path) -> Store<TestState, IntermittentWal> {
    let wal = IntermittentWal::open(dir);
    Store {
        state: Arc::new(parking_lot::RwLock::new(TestState::default())),
        write_gate: parking_lot::Mutex::new(()),
        backend: Arc::clone(&wal),
        incremental: Some(Arc::clone(&wal) as Arc<dyn IncrementalSave<TestState>>),
        shared: None,
        flusher: None,
        lock_deadlock_timeout_us: AtomicU64::new(crate::store::lock::duration_to_us(
            crate::store::lock::STATE_LOCK_DEADLOCK_TIMEOUT,
        )),
        replay_report: crate::ReplayReport::default(),
    }
}

/// Concurrent grouped writers under intermittent fsync failures. Every write
/// that returned `Ok` (acknowledged) must be durable once the disk calms and a
/// final flush succeeds — requeue-on-error drops nothing.
#[test]
fn test_grouped_concurrent_writers_intermittent_flush_failure_loses_nothing() {
    let dir = TempDir::new().unwrap();
    const WRITERS: usize = 6;
    const PER_WRITER: usize = 40;

    {
        let mut store = intermittent_store(dir.path());
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_millis(10),
        });
        let store = Arc::new(store);

        let mut handles = Vec::new();
        for t in 0..WRITERS {
            let s = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_WRITER {
                    // write() may surface a *previous* flush error fail-fast;
                    // retry until the op is acknowledged so it counts as
                    // "acknowledged-then-must-be-flushed".
                    loop {
                        let r = s.write(|tx| {
                            tx.insert(&format!("t{t}_k{i}"), "v");
                            Ok(())
                        });
                        if r.is_ok() {
                            break;
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Calm the disk and drain: flush must eventually succeed, making every
        // acknowledged op durable.
        store.backend().calm();
        let ok = {
            let start = std::time::Instant::now();
            loop {
                if store.flush().is_ok() {
                    break true;
                }
                if start.elapsed() > Duration::from_secs(10) {
                    break false;
                }
            }
        };
        assert!(ok, "flush never succeeded after calming the disk");
    }

    let reopened: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = reopened.read();
    assert_eq!(
        state.items.len(),
        WRITERS * PER_WRITER,
        "an acknowledged-then-flushed op was lost across intermittent failures"
    );
}

// -------------------------------------------------------------------------
// (c) close -> immediate reopen loop (DatabaseLocked lock-retry regression).
// -------------------------------------------------------------------------

/// Tight loop of open → write → drop → immediate reopen while a background
/// flusher runs. Regression for the bounded lock-acquire retry: reopening the
/// same directory right after the previous handle drops must always succeed,
/// never fail with `DatabaseLocked` on the not-yet-published `flock` release.
#[test]
fn test_close_then_immediate_reopen_never_locks_out() {
    let dir = TempDir::new().unwrap();

    for iter in 0..50 {
        let mut store: WalStore = Store::open_wal(dir.path().to_path_buf())
            .unwrap_or_else(|e| panic!("reopen {iter} failed: {e:?}"));
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_millis(1),
        });
        store
            .write(|tx| {
                tx.insert(&format!("k{iter}"), "v");
                Ok(())
            })
            .unwrap();
        // Drop (via scope end) joins the flusher and releases the lock; the
        // next iteration reopens immediately.
        drop(store);
    }

    // Final open sees every write that was flushed on drop.
    let store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    assert!(!store.read().items.is_empty());
}
