//! Core persistence engine.
//!
//! Generic `Store<T, B>` that holds state in memory behind an `RwLock` and
//! delegates persistence to a `Backend`. Reads are zero-copy borrows; writes
//! use transaction capture (overlay + ops) so the read lock is only held
//! briefly during merge.
//!
//! # Flush Policies
//!
//! - **Immediate** (default): every write fsyncs before returning.
//! - **Grouped**: writes are coalesced; a background thread fsyncs at most
//!   every `interval`. Only the latest state is persisted — intermediate
//!   mutations are folded in.

use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard};
use serde::{Serialize, de::DeserializeOwned};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::backend::{Backend, NullBackend, PostcardBackend};
use crate::error::{Error, Result};
use crate::wal::{IncrementalSave, Op, Replayable, Transactable, WalBackend};

/// Controls how writes are persisted to disk.
#[derive(Debug, Clone)]
pub enum FlushPolicy {
    /// Every write fsyncs immediately (current behavior, default).
    Immediate,
    /// Writes are coalesced; a background thread fsyncs at most every `interval`.
    Grouped { interval: Duration },
}

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
}

/// Shared read guard — holds read lock, provides zero-copy access.
pub struct Ref<'a, T>(RwLockReadGuard<'a, T>);

impl<'a, T> std::ops::Deref for Ref<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Serialize + DeserializeOwned + Clone + Default> Store<T, PostcardBackend> {
    /// Open store from disk using the postcard binary backend.
    /// Falls back to `.bak`, then `T::default()`.
    pub fn open_postcard(path: PathBuf) -> Result<Self> {
        let backend = PostcardBackend::new(path)?;
        let state = backend.load()?;
        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            write_gate: Mutex::new(()),
            backend: Arc::new(backend),
            incremental: None,
            shared: None,
            flusher: None,
        })
    }
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
        }
    }
}

impl<T: Replayable + Serialize + DeserializeOwned + Default> Store<T, WalBackend<T>> {
    /// Open store with WAL backend. Immediate mode (every write fsyncs WAL).
    pub fn open_wal(dir: PathBuf) -> Result<Self> {
        let backend = WalBackend::open(&dir)?;
        let state = backend.load()?;
        let backend = Arc::new(backend);
        let incremental: Arc<dyn IncrementalSave<T>> = Arc::clone(&backend) as _;
        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            write_gate: Mutex::new(()),
            backend,
            incremental: Some(incremental),
            shared: None,
            flusher: None,
        })
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
        })
    }

    /// Zero-copy shared read. Multiple readers can hold this concurrently.
    pub fn read(&self) -> Ref<'_, T> {
        Ref(self.state.read())
    }

    /// Returns the last background flush error, if any.
    pub fn flush_error(&self) -> Option<Error> {
        self.shared
            .as_ref()
            .and_then(|s| s.last_error.lock().take())
    }

    /// Returns a reference to the backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }
}

// Write methods — zero-clone transaction capture.
impl<T: Transactable, B: Backend<T>> Store<T, B> {
    /// Atomic write via transaction capture.
    ///
    /// Borrows committed state via a read lock, executes mutations against an
    /// overlay that captures ops directly, then merges the overlay into state.
    /// O(changed keys), not O(total entries).
    ///
    /// **With WAL (Immediate)**: begin_tx → mutate → finish → append ops → fsync → merge.
    /// **With WAL (Grouped)**: begin_tx → mutate → finish → buffer ops → merge → bump gen.
    /// **Without WAL**: falls through to overlay merge only (no persistence).
    pub fn write<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R>,
    {
        let _gate = self.write_gate.lock();

        // Fail-fast on grouped flusher error.
        if let Some(ref shared) = self.shared
            && let Some(err) = shared.last_error.lock().take()
        {
            return Err(err);
        }

        // Borrow committed state via read lock — no clone.
        let state_guard = self.state.read();
        let mut tx = state_guard.begin_tx();
        let result = f(&mut tx)?;
        let (ops, overlay) = T::finish_tx(tx);
        drop(state_guard); // release read lock before write lock

        // Persist, then merge overlay into in-memory state.
        if let Some(ref inc) = self.incremental {
            // WAL path: append ops, then sync or buffer.
            if !ops.is_empty() {
                match &self.shared {
                    None => {
                        inc.save_ops(&ops)?;
                        inc.sync()?;
                    }
                    Some(shared) => {
                        shared.pending_ops.lock().push(ops);
                    }
                }
            }
        } else {
            // Non-WAL path: merge overlay first, then persist full state.
            self.state.write().apply_overlay(overlay);
            match &self.shared {
                None => {
                    self.backend.save(&self.state.read())?;
                }
                Some(shared) => {
                    shared.gen_written.fetch_add(1, Ordering::Release);
                    shared.notify.notify_one();
                }
            }
            return Ok(result);
        }

        self.state.write().apply_overlay(overlay);

        if let Some(ref shared) = self.shared {
            shared.gen_written.fetch_add(1, Ordering::Release);
            shared.notify.notify_one();
        }

        Ok(result)
    }

    /// Atomic write with guaranteed immediate persistence.
    ///
    /// Same as `write()` but forces an immediate fsync regardless of flush
    /// policy. Use for critical writes that must survive a crash.
    pub fn write_durable<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R>,
    {
        let _gate = self.write_gate.lock();

        let state_guard = self.state.read();
        let mut tx = state_guard.begin_tx();
        let result = f(&mut tx)?;
        let (ops, overlay) = T::finish_tx(tx);
        drop(state_guard);

        if let Some(ref inc) = self.incremental {
            // WAL path: drain pending ops, append ours, fsync.
            if let Some(ref shared) = self.shared {
                let batched: Vec<Vec<Op>> = {
                    let mut pending = shared.pending_ops.lock();
                    std::mem::take(&mut *pending)
                };
                for batch in &batched {
                    inc.save_ops(batch)?;
                }
            }

            if !ops.is_empty() {
                inc.save_ops(&ops)?;
            }
            inc.sync()?;
        } else {
            // Non-WAL path: merge overlay, then full persist.
            self.state.write().apply_overlay(overlay);
            self.backend.save(&self.state.read())?;
            if let Some(ref shared) = self.shared {
                let generation = shared.gen_written.fetch_add(1, Ordering::Release) + 1;
                shared.gen_flushed.store(generation, Ordering::Release);
            }
            return Ok(result);
        }

        self.state.write().apply_overlay(overlay);

        if let Some(ref shared) = self.shared {
            let generation = shared.gen_written.fetch_add(1, Ordering::Release) + 1;
            shared.gen_flushed.store(generation, Ordering::Release);
        }

        Ok(result)
    }
}

impl<T: Clone + Send + Sync + 'static, B: Backend<T> + Send + Sync + 'static> Store<T, B> {
    /// Set flush policy. Must be called before first write.
    /// Starts background flusher thread for Grouped policy.
    pub fn set_flush_policy(&mut self, policy: FlushPolicy) {
        // Shut down existing flusher if any.
        self.shutdown_flusher();

        match policy {
            FlushPolicy::Immediate => {
                self.shared = None;
                self.flusher = None;
            }
            FlushPolicy::Grouped { interval } => {
                let shared = Arc::new(FlushShared {
                    state: Arc::clone(&self.state),
                    backend: Arc::clone(&self.backend),
                    incremental: self.incremental.clone(),
                    pending_ops: Mutex::new(Vec::new()),
                    gen_written: AtomicU64::new(0),
                    gen_flushed: AtomicU64::new(0),
                    notify: Condvar::new(),
                    notify_mu: Mutex::new(()),
                    last_error: Mutex::new(None),
                    shutdown: AtomicBool::new(false),
                });

                let thread_shared = Arc::clone(&shared);
                let handle = std::thread::Builder::new()
                    .name("store-flusher".into())
                    .spawn(move || flusher_loop(&thread_shared, interval))
                    .expect("failed to spawn flusher thread");

                self.shared = Some(shared);
                self.flusher = Some(FlushState {
                    handle: Mutex::new(Some(handle)),
                });
            }
        }
    }

    /// Flush dirty state now and wait for completion.
    ///
    /// In grouped mode, wakes the flusher thread and spins until it catches
    /// up with the current generation. In immediate mode, this is a no-op
    /// since writes are already persisted synchronously.
    pub fn flush(&self) -> Result<()> {
        let Some(ref shared) = self.shared else {
            return Ok(());
        };

        let target_gen = shared.gen_written.load(Ordering::Acquire);
        if target_gen == shared.gen_flushed.load(Ordering::Acquire) {
            return Ok(());
        }

        // Wake the flusher repeatedly until it catches up.
        let start = std::time::Instant::now();
        loop {
            shared.notify.notify_one();

            if shared.gen_flushed.load(Ordering::Acquire) >= target_gen {
                break;
            }

            if start.elapsed() > std::time::Duration::from_secs(5) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "flush timed out waiting for flusher",
                )));
            }

            // Check for flusher error.
            if let Some(err) = shared.last_error.lock().take() {
                return Err(err);
            }

            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Check for any error that occurred during the flush.
        if let Some(err) = shared.last_error.lock().take() {
            return Err(err);
        }
        Ok(())
    }

    /// Shut down the flusher thread gracefully.
    pub fn close(&mut self) -> Result<()> {
        self.shutdown_flusher();
        Ok(())
    }

    fn shutdown_flusher(&mut self) {
        if let Some(ref shared) = self.shared {
            shared.shutdown.store(true, Ordering::Release);
            shared.notify.notify_one();
        }
        if let Some(ref flusher) = self.flusher
            && let Some(handle) = flusher.handle.lock().take()
        {
            let _ = handle.join();
        }
    }
}

/// Flusher thread main loop.
///
/// With WAL: sync WAL (fsync buffered entries) + check snapshot threshold.
/// Without WAL: clone state + full backend.save().
fn flusher_loop<T: Clone, B: Backend<T>>(shared: &FlushShared<T, B>, interval: Duration) {
    loop {
        {
            let mut guard = shared.notify_mu.lock();
            shared.notify.wait_for(&mut guard, interval);
        }

        let should_shutdown = shared.shutdown.load(Ordering::Acquire);

        let current_gen = shared.gen_written.load(Ordering::Acquire);
        let flushed_gen = shared.gen_flushed.load(Ordering::Acquire);

        if current_gen != flushed_gen {
            let result = if let Some(ref inc) = shared.incremental {
                // WAL mode: drain buffered ops, write to WAL, fsync once.
                let batched: Vec<Vec<Op>> = {
                    let mut pending = shared.pending_ops.lock();
                    std::mem::take(&mut *pending)
                };
                let mut write_err = None;
                for ops in &batched {
                    if let Err(e) = inc.save_ops(ops) {
                        write_err = Some(e);
                        break;
                    }
                }
                match write_err {
                    Some(e) => Err(e),
                    None => match inc.sync() {
                        Ok(()) => {
                            // Check if we should compact.
                            if inc.should_snapshot() {
                                let snapshot = shared.state.read().clone();
                                inc.snapshot(&snapshot)
                            } else {
                                Ok(())
                            }
                        }
                        Err(e) => Err(e),
                    },
                }
            } else {
                // Non-WAL: full state clone + serialize.
                let snapshot = shared.state.read().clone();
                shared.backend.save(&snapshot)
            };

            match result {
                Ok(()) => {
                    shared.gen_flushed.store(current_gen, Ordering::Release);
                }
                Err(e) => {
                    *shared.last_error.lock() = Some(e);
                }
            }
        }

        if should_shutdown {
            break;
        }
    }
}

impl<T, B: Backend<T>> Drop for Store<T, B> {
    fn drop(&mut self) {
        if let Some(ref shared) = self.shared {
            shared.shutdown.store(true, Ordering::Release);
            shared.notify.notify_one();
        }
        if let Some(ref flusher) = self.flusher
            && let Some(handle) = flusher.handle.lock().take()
        {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
#[path = "store_test.rs"]
mod store_test;
