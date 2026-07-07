//! ENOSPC / short-write behaviour.
//!
//! Scope: a true filesystem `ENOSPC` needs a special filesystem, so these
//! tests use a fault-injecting `IncrementalSave`/`Backend` wrapper as the
//! accepted unit-level stand-in. The fault engages at an op/sync boundary
//! (returning a disk-full `io::Error`), so the WAL bytes are never *partially*
//! written by this stand-in — the genuine torn-write case is covered by the
//! crash-injection and parser-fuzz tests. What these tests assert is the
//! *surfacing* and *recovery* contract:
//!
//! - immediate mode: a failing write returns the error from `write()`;
//! - grouped mode: the error is visible via `last_flush_error` and returned by
//!   `flush()`, and the failing batch is retried (not dropped);
//! - after the fault, a reopen recovers the last consistent prefix — never a
//!   mis-replayed or half-applied state.

use std::sync::atomic::AtomicI64;

use super::*;
use tempfile::TempDir;

/// A `WalBackend` wrapper that fails `save_ops` / `sync` once a per-operation
/// budget is exhausted, modelling a disk-full backend.
struct FaultyWal {
    inner: Arc<WalBackend<TestState>>,
    /// Successful `save_ops` calls remaining before the fault engages.
    save_ops_budget: AtomicI64,
    /// Successful `sync` calls remaining before the fault engages.
    sync_budget: AtomicI64,
}

impl FaultyWal {
    fn open(dir: &std::path::Path) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(WalBackend::open(dir).unwrap()),
            save_ops_budget: AtomicI64::new(i64::MAX),
            sync_budget: AtomicI64::new(i64::MAX),
        })
    }

    fn fail_save_ops_after(&self, n: i64) {
        self.save_ops_budget.store(n, Ordering::Release);
    }

    fn fail_sync_after(&self, n: i64) {
        self.sync_budget.store(n, Ordering::Release);
    }

    /// Restore both budgets so subsequent operations succeed (models the disk
    /// having room again).
    fn heal(&self) {
        self.save_ops_budget.store(i64::MAX, Ordering::Release);
        self.sync_budget.store(i64::MAX, Ordering::Release);
    }

    fn disk_full() -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "disk full (ENOSPC stand-in)",
        ))
    }

    fn consume(budget: &AtomicI64) -> bool {
        budget.fetch_sub(1, Ordering::AcqRel) > 0
    }
}

impl Backend<TestState> for FaultyWal {
    fn load(&self) -> crate::error::Result<TestState> {
        self.inner.load()
    }

    fn save(&self, state: &TestState) -> crate::error::Result<()> {
        self.inner.save(state)
    }
}

impl IncrementalSave<TestState> for FaultyWal {
    fn save_ops(&self, ops: &[Op]) -> crate::error::Result<()> {
        if !Self::consume(&self.save_ops_budget) {
            return Err(Self::disk_full());
        }
        self.inner.save_ops(ops)
    }

    fn sync(&self) -> crate::error::Result<()> {
        if !Self::consume(&self.sync_budget) {
            return Err(Self::disk_full());
        }
        self.inner.sync()
    }

    fn should_snapshot(&self) -> bool {
        self.inner.should_snapshot()
    }

    fn snapshot(&self, state: &TestState) -> crate::error::Result<()> {
        self.inner.snapshot(state)
    }
}

/// Build a store over a fault-injecting WAL wrapper for `dir`.
fn faulty_store(dir: &std::path::Path) -> Store<TestState, FaultyWal> {
    let faulty = FaultyWal::open(dir);
    Store {
        state: Arc::new(parking_lot::RwLock::new(TestState::default())),
        write_gate: parking_lot::Mutex::new(()),
        backend: Arc::clone(&faulty),
        incremental: Some(Arc::clone(&faulty) as Arc<dyn IncrementalSave<TestState>>),
        shared: None,
        flusher: None,
        lock_deadlock_timeout_us: AtomicU64::new(crate::store::lock::duration_to_us(
            crate::store::lock::STATE_LOCK_DEADLOCK_TIMEOUT,
        )),
        replay_report: crate::ReplayReport::default(),
    }
}

fn is_disk_full(err: &Error) -> bool {
    matches!(err, Error::Io(e) if e.kind() == std::io::ErrorKind::WriteZero)
}

/// Poll a predicate until it holds or the deadline passes — no fixed sleeps.
fn poll_until(mut cond: impl FnMut() -> bool, budget: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < budget {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    cond()
}

// -------------------------------------------------------------------------
// Immediate mode.
// -------------------------------------------------------------------------

/// Immediate mode: once the disk is full, `write()` surfaces the error; the
/// three acknowledged writes before the fault survive a reopen, and the failed
/// write is absent — no half-applied state.
#[test]
fn test_immediate_write_surfaces_disk_full_and_reopen_recovers_prefix() {
    let dir = TempDir::new().unwrap();

    {
        let store = faulty_store(dir.path());
        store.backend().fail_save_ops_after(3);

        for i in 0..3 {
            store
                .write(|tx| {
                    tx.insert(&format!("k{i}"), &format!("v{i}"));
                    Ok(())
                })
                .unwrap();
        }

        let err = store
            .write(|tx| {
                tx.insert("k3", "v3");
                Ok(())
            })
            .expect_err("write must surface the disk-full error");
        assert!(
            is_disk_full(&err),
            "expected disk-full io error, got {err:?}"
        );

        // In-memory state must not contain the failed write (write() returns
        // before the overlay merge on a WAL error).
        assert!(!store.read().items.contains_key("k3"));
    } // store dropped -> WalBackend lock released

    let reopened: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = reopened.read();
    assert_eq!(
        state.items.len(),
        3,
        "only acknowledged writes must persist"
    );
    for i in 0..3 {
        assert_eq!(state.items.get(&format!("k{i}")).unwrap(), &format!("v{i}"));
    }
    assert!(!state.items.contains_key("k3"));
}

/// Immediate mode: a `sync` failure is surfaced by `write()` too.
#[test]
fn test_immediate_write_surfaces_sync_failure() {
    let dir = TempDir::new().unwrap();
    let store = faulty_store(dir.path());
    store.backend().fail_sync_after(0);

    let err = store
        .write(|tx| {
            tx.insert("k", "v");
            Ok(())
        })
        .expect_err("a sync failure must surface from write()");
    assert!(
        is_disk_full(&err),
        "expected disk-full io error, got {err:?}"
    );
}

// -------------------------------------------------------------------------
// Grouped mode.
// -------------------------------------------------------------------------

/// Grouped mode: a background flush failure is visible via `last_flush_error`
/// (non-consuming) and returned by `flush()`. After the disk recovers, the
/// requeued ops are retried and a reopen shows every write — the failed batch
/// was retried, not dropped.
///
/// The fault engages at `save_ops`, NOT `sync`, on purpose: a `save_ops` that
/// fails before delegating never appends the batch to the WAL's `BufWriter`, so
/// a batch that was *not* requeued is genuinely absent at reopen. (A sync-only
/// fault would leave the already-appended bytes buffered, and they would reach
/// disk on any later successful sync or on writer drop — making the reopen
/// assertion pass even if requeue were a no-op.)
#[test]
fn test_grouped_flush_failure_surfaces_and_recovers() {
    let dir = TempDir::new().unwrap();

    {
        let mut store = faulty_store(dir.path());
        store.set_flush_policy(FlushPolicy::Grouped {
            interval: Duration::from_millis(20),
        });
        // Fail the flusher's append immediately — nothing reaches the WAL until
        // the batch is retried after the disk recovers.
        store.backend().fail_save_ops_after(0);

        for i in 0..8 {
            // A grouped write() fail-fasts any stashed flusher error (consuming
            // it) before buffering; retry so the op is definitely acknowledged.
            while store
                .write(|tx| {
                    tx.insert(&format!("k{i}"), &format!("v{i}"));
                    Ok(())
                })
                .is_err()
            {}
        }

        // The flusher must record the disk-full error; observe it without
        // consuming.
        assert!(
            poll_until(
                || store.last_flush_error().is_some(),
                Duration::from_secs(3)
            ),
            "grouped flush failure must surface via last_flush_error"
        );
        let seen = store.last_flush_error().unwrap();
        assert!(seen.contains("disk full"), "unexpected error text: {seen}");

        // flush() returns the error (and consumes it).
        let flush_err = store
            .flush()
            .expect_err("flush must return the flush error");
        assert!(is_disk_full(&flush_err));

        // Disk recovers: the requeued ops must flush cleanly now.
        store.backend().heal();
        assert!(
            poll_until(|| store.flush().is_ok(), Duration::from_secs(3)),
            "after healing, flush must eventually succeed (ops were requeued)"
        );
    }

    let reopened: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
    let state = reopened.read();
    assert_eq!(
        state.items.len(),
        8,
        "every write must persist after retry — requeue dropped nothing"
    );
    for i in 0..8 {
        assert_eq!(state.items.get(&format!("k{i}")).unwrap(), &format!("v{i}"));
    }
}
