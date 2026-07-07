//! Core persistence engine.
//!
//! Generic `Store<T, B>` that holds state in memory behind an `RwLock` and
//! delegates persistence to a `Backend`. Reads are zero-copy borrows; writes
//! use transaction capture (overlay + ops) so the state lock is only held
//! briefly during merge.
//!
//! # Reads unblocked during persistence
//!
//! No persistence path holds the state lock across the disk write. The WAL
//! paths persist owned ops (captured before any lock is taken). The
//! non-WAL path snapshots the state under a short lock and then serializes
//! and fsyncs the snapshot with no lock held. A slow disk therefore never
//! stalls readers or the next writer waiting on the state lock — it only
//! occupies the write-serialization gate.
//!
//! # Flush Policies
//!
//! - **Immediate** (default): every write fsyncs before returning, and the
//!   WAL is compacted inline once it crosses the snapshot threshold.
//! - **Grouped**: writes are coalesced; a background thread fsyncs at most
//!   every `interval`. Only the latest state is persisted — intermediate
//!   mutations are folded in. See [`FlushPolicy::Grouped`] for the exact
//!   durability contract (an acknowledged write is durable only after the
//!   next flush).

mod flush;
mod lock;
mod persist;

use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Serialize, de::DeserializeOwned};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::backend::{Backend, NullBackend};
use crate::error::{Error, Result};
use crate::wal::{IncrementalSave, Op, ReplayReport, Replayable, WalBackend};

pub use flush::FlushPolicy;
pub use lock::Ref;
use lock::{STATE_LOCK_DEADLOCK_TIMEOUT, duration_to_us, read_or_panic, try_write_for};

/// Shared state between the store and the flusher thread.
struct FlushShared<T, B: Backend<T>> {
    state: Arc<RwLock<T>>,
    backend: Arc<B>,
    /// Optional incremental saver (WAL). When present, the flusher syncs the
    /// WAL instead of doing full backend.save().
    incremental: Option<Arc<dyn IncrementalSave<T>>>,
    /// Pending ops buffer for WAL grouped mode. Writers push ops here; the
    /// flusher drains and writes them to the WAL file in bulk, then fsyncs.
    /// This avoids per-write WAL mutex acquisition and BufWriter I/O.
    pending_ops: Mutex<Vec<Vec<Op>>>,
    gen_written: AtomicU64,
    gen_flushed: AtomicU64,
    notify: Condvar,
    notify_mu: Mutex<()>,
    last_error: Mutex<Option<Error>>,
    shutdown: AtomicBool,
}

/// Background flusher state (only present in Grouped mode).
struct FlushState {
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// Persistent state store.
///
/// Holds `T` in memory behind a read-write lock. On write, mutations execute
/// against a transaction overlay that captures ops directly. The overlay is
/// merged into state in O(changed keys).
///
/// A separate `Mutex` serializes writers so the `RwLock` write-lock is held
/// only for the final overlay merge (~microseconds), keeping reads unblocked
/// during persistence.
pub struct Store<T, B: Backend<T> = NullBackend> {
    state: Arc<RwLock<T>>,
    write_gate: Mutex<()>,
    backend: Arc<B>,
    /// Optional incremental save (WAL). When present, write() diffs and
    /// appends ops instead of full backend.save().
    incremental: Option<Arc<dyn IncrementalSave<T>>>,
    /// Shared state with flusher thread. None = Immediate mode.
    shared: Option<Arc<FlushShared<T, B>>>,
    /// Flusher thread handle. None = Immediate mode or not yet started.
    flusher: Option<FlushState>,
    /// Deadlock-detector budget in microseconds. On timeout, `read()`
    /// panics and the `Result`-bearing write paths return
    /// [`Error::LockTimeout`]. Stored as microseconds (not milliseconds)
    /// so a sub-millisecond configured value keeps its precision instead
    /// of truncating to zero. Defaults to [`STATE_LOCK_DEADLOCK_TIMEOUT`]
    /// (30 s); operators can tune via [`Store::set_lock_deadlock_timeout`]
    /// for high-contention workloads (raise the budget) or fast-fail test
    /// environments (lower the budget).
    lock_deadlock_timeout_us: AtomicU64,
    /// Structured account of the load that populated this store. Empty for
    /// in-memory / non-WAL stores. Set once at open; see
    /// [`Store::replay_report`].
    replay_report: ReplayReport,
}

impl<T: Default> Store<T, NullBackend> {
    /// In-memory only store (for tests).
    pub fn memory() -> Self {
        Self {
            state: Arc::new(RwLock::new(T::default())),
            write_gate: Mutex::new(()),
            backend: Arc::new(NullBackend),
            incremental: None,
            shared: None,
            flusher: None,
            lock_deadlock_timeout_us: AtomicU64::new(duration_to_us(STATE_LOCK_DEADLOCK_TIMEOUT)),
            replay_report: ReplayReport::default(),
        }
    }
}

impl<T: Replayable + Serialize + DeserializeOwned + Default> Store<T, WalBackend<T>> {
    /// Open store with WAL backend. Immediate mode (every write fsyncs WAL).
    ///
    /// Lenient: replay data loss (skips, quarantine, snapshot discard) is
    /// recorded, not raised — the store still comes up. Inspect what happened
    /// via [`Store::replay_report`], or use [`Store::open_wal_with_report`] to
    /// receive the report directly. For a fail-fast open, see
    /// [`Store::open_wal_strict`].
    pub fn open_wal(dir: PathBuf) -> Result<Self> {
        Self::open_wal_with_report(dir).map(|(store, _)| store)
    }

    /// Like [`Store::open_wal`], but also returns the [`ReplayReport`] for the
    /// load (the same value later reachable via [`Store::replay_report`]).
    pub fn open_wal_with_report(dir: PathBuf) -> Result<(Self, ReplayReport)> {
        let backend = WalBackend::open(&dir)?;
        let (state, report) = backend.load_with_report()?;
        Ok((Self::from_wal_parts(state, backend, report.clone()), report))
    }

    /// Open store with WAL backend in **strict** mode: any replay data loss
    /// (skip, quarantine, or snapshot discard) aborts the open with a typed
    /// error ([`Error::SchemaVersionMismatch`] when a version mismatch is the
    /// cause, otherwise [`Error::ReplayLoss`]).
    ///
    /// Use this when a partial state is unacceptable and you would rather fail
    /// loudly than come up with missing data. The default [`Store::open_wal`]
    /// stays lenient for compatibility.
    pub fn open_wal_strict(dir: PathBuf) -> Result<Self> {
        let backend = WalBackend::open(&dir)?;
        let (state, report) = backend.load_strict()?;
        Ok(Self::from_wal_parts(state, backend, report))
    }

    /// Assemble a WAL-backed store from a completed load.
    fn from_wal_parts(state: T, backend: WalBackend<T>, report: ReplayReport) -> Self {
        let backend = Arc::new(backend);
        let incremental: Arc<dyn IncrementalSave<T>> = Arc::clone(&backend) as _;
        Self {
            state: Arc::new(RwLock::new(state)),
            write_gate: Mutex::new(()),
            backend,
            incremental: Some(incremental),
            shared: None,
            flusher: None,
            lock_deadlock_timeout_us: AtomicU64::new(duration_to_us(STATE_LOCK_DEADLOCK_TIMEOUT)),
            replay_report: report,
        }
    }

    /// Set the WAL-entry count at which a full snapshot is taken.
    ///
    /// Defaults to `DEFAULT_SNAPSHOT_THRESHOLD`. A high-churn store (the
    /// corpus tier under heavy ingest) raises this so it stops rewriting
    /// the entire snapshot every few hundred records; a size-based WAL
    /// watchdog bounds growth instead. Atomic — safe to call after open.
    pub fn set_snapshot_threshold(&self, threshold: u64) {
        self.backend.set_snapshot_threshold(threshold);
    }

    /// Snapshot of currently quarantined entries (clones).
    ///
    /// Use this to surface schema-drift failures to operators. Entries
    /// persist across restarts until `purge_quarantine` is called or a
    /// normal write supersedes them.
    pub fn quarantined(&self) -> Vec<crate::QuarantinedEntry> {
        self.backend.quarantined()
    }

    /// Drop all quarantined entries. Explicit — never called automatically.
    pub fn purge_quarantine(&self) -> Result<()> {
        self.backend.purge_quarantine()
    }

    /// Retry migration on all quarantined entries with the current
    /// `T::migrations()` registry.
    ///
    /// For each entry that successfully migrates, the recovered value is
    /// written back through the normal WAL path (so it becomes visible to
    /// readers and will survive the next snapshot). Entries that still
    /// fail remain in quarantine.
    ///
    /// Returns the number of entries successfully recovered.
    pub fn retry_quarantine(&self) -> Result<usize> {
        let recovered_ops = self.backend.retry_quarantine()?;
        if recovered_ops.is_empty() {
            return Ok(0);
        }
        let n = recovered_ops.len();

        // Append the synthetic Put ops to the WAL and apply in-memory so
        // subsequent reads see the recovered values.
        if let Some(ref inc) = self.incremental {
            inc.save_ops(&recovered_ops)?;
            inc.sync()?;
        }
        let migrations = T::migrations();
        let mut quarantine_scratch = crate::Quarantine::new();
        let mut ctx = crate::ReplayContext::new(
            crate::ReplayFormat::Versioned,
            &migrations,
            &mut quarantine_scratch,
        );
        {
            let mut state = try_write_for(
                &self.state,
                "retry_quarantine",
                self.lock_deadlock_timeout(),
            )?;
            state.apply_with_ctx(&recovered_ops, &mut ctx)?;
        }
        Ok(n)
    }
}

impl<T: Clone, B: Backend<T>> Store<T, B> {
    /// Create a store from an existing backend.
    pub fn with_backend(backend: B) -> Result<Self>
    where
        T: DeserializeOwned,
    {
        let state = backend.load()?;
        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            write_gate: Mutex::new(()),
            backend: Arc::new(backend),
            incremental: None,
            shared: None,
            flusher: None,
            lock_deadlock_timeout_us: AtomicU64::new(duration_to_us(STATE_LOCK_DEADLOCK_TIMEOUT)),
            replay_report: ReplayReport::default(),
        })
    }

    /// Zero-copy shared read. Multiple readers can hold the returned
    /// [`Ref`] concurrently.
    ///
    /// # Deadlock hazard
    ///
    /// The returned [`Ref`] holds a read lock until it is dropped. Do
    /// **not** keep it alive across a call into [`Self::write`] /
    /// [`Self::write_durable`] (the `RwLock` is not reentrant — the write
    /// would wait on your own read guard) or across an `.await` point in
    /// async code. For scoped access that makes this unrepresentable, use
    /// [`Self::read_with`]. On lock-acquisition timeout this panics with a
    /// deadlock diagnostic; the `Result`-bearing write paths return
    /// [`Error::LockTimeout`] instead.
    pub fn read(&self) -> Ref<'_, T> {
        Ref(read_or_panic(
            &self.state,
            "Store::read",
            self.lock_deadlock_timeout(),
        ))
    }

    /// Scoped zero-copy read: runs `f` with a shared borrow of the state
    /// and drops the read guard before returning.
    ///
    /// Prefer this over [`Self::read`] when the state is only needed for
    /// the duration of a computation. Because the guard cannot escape the
    /// closure, it is *impossible* to accidentally hold the read lock
    /// across a later call into [`Self::write`] (a self-deadlock) or
    /// across an `.await` point. Panics on lock-acquisition timeout,
    /// exactly like [`Self::read`].
    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = read_or_panic(
            &self.state,
            "Store::read_with",
            self.lock_deadlock_timeout(),
        );
        f(&guard)
    }

    /// Current deadlock-detector budget. See
    /// [`Self::set_lock_deadlock_timeout`].
    #[must_use]
    pub fn lock_deadlock_timeout(&self) -> Duration {
        Duration::from_micros(self.lock_deadlock_timeout_us.load(Ordering::Acquire))
    }

    /// Set the deadlock-detector budget. On timeout, `read()` panics and
    /// the `Result`-bearing write paths return [`Error::LockTimeout`].
    ///
    /// Default is 30 s — orders of magnitude past any legitimate
    /// lock-hold duration. Lower it (e.g. 1–5 s) in test environments
    /// where you want a hung suite to fail fast. Raise it for workloads
    /// with genuinely long-running write closures (e.g. bulk imports
    /// under a single transaction).
    ///
    /// The value is stored with microsecond precision; the effective wait
    /// is floored at 1 ms so a sub-millisecond budget never becomes an
    /// instant false-positive timeout.
    ///
    /// Atomic — the next lock acquisition picks up the new value.
    /// In-flight `try_*_for` calls finish on the old budget.
    pub fn set_lock_deadlock_timeout(&self, timeout: Duration) {
        self.lock_deadlock_timeout_us
            .store(duration_to_us(timeout), Ordering::Release);
    }

    /// Take the last background-flush error, clearing it.
    ///
    /// Consuming accessor: returns the typed [`Error`] and resets the health
    /// state to clean. [`Store::write`] and [`Store::flush`] call this
    /// internally, so an error observed by a write is not also returned here.
    /// To poll health without disturbing it, use [`Store::last_flush_error`].
    /// Always `None` outside Grouped mode.
    #[must_use]
    pub fn flush_error(&self) -> Option<Error> {
        self.shared
            .as_ref()
            .and_then(|s| s.last_error.lock().take())
    }

    /// Peek the last background-flush error without clearing it.
    ///
    /// Non-consuming health probe for Grouped mode: returns the error's
    /// display string while leaving it set, so a monitoring loop can poll
    /// divergence without issuing a write. [`Error`] is not `Clone` (it wraps
    /// [`std::io::Error`]), so the typed value is only available via the
    /// consuming [`Store::flush_error`].
    ///
    /// The recorded error persists until a consumer acknowledges it via
    /// [`Store::flush_error`], [`Store::write`], or [`Store::flush`] (a
    /// recovered background retry advances the durability watermark but does
    /// not clear the error on its own). `None` therefore means "no
    /// unacknowledged flush error" (or the store is not in Grouped mode).
    #[must_use]
    pub fn last_flush_error(&self) -> Option<String> {
        self.shared
            .as_ref()
            .and_then(|s| s.last_error.lock().as_ref().map(|e| e.to_string()))
    }

    /// Returns a reference to the backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// The [`ReplayReport`] from the load that populated this store.
    ///
    /// For a WAL-backed store this reflects what happened during the last
    /// open (applied/skipped/quarantined counts, snapshot status, schema
    /// drift). For in-memory or non-WAL stores it is empty. Consult
    /// [`ReplayReport::has_loss`] to branch on whether the load was clean.
    pub fn replay_report(&self) -> &ReplayReport {
        &self.replay_report
    }
}

#[cfg(test)]
mod store_test;
