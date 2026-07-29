//! Grouped-flush policy: the background flusher thread, its batching and
//! retry machinery, the durability watermark, and lifecycle (`set_flush_policy`,
//! `flush`, `close`, and `Drop`).

use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::backend::Backend;
use crate::error::{Error, Result};
use crate::wal::{IncrementalSave, Op};

use super::{FlushShared, FlushState, Store};

/// Controls how writes are persisted to disk.
#[derive(Debug, Clone)]
pub enum FlushPolicy {
    /// Every write fsyncs immediately (current behavior, default).
    Immediate,
    /// Writes are coalesced; a background thread fsyncs at most every
    /// `interval`.
    ///
    /// # Durability contract
    ///
    /// A write acknowledged by [`Store::write`] (i.e. `Ok(_)`) is applied to
    /// in-memory state immediately but is **not yet durable**: its ops sit in
    /// a pending buffer until the flusher fsyncs them, which happens at most
    /// `interval` later (or when you call [`Store::flush`], or on a
    /// [`Store::write_durable`]). If the process crashes in that window, the
    /// acknowledged-but-unflushed writes are lost.
    ///
    /// If a background flush fails, the error is stored and surfaced two ways:
    /// the next [`Store::write`] / [`Store::flush`] returns it (and clears it),
    /// and [`Store::last_flush_error`] exposes it to a poller that is not
    /// issuing a write. The failing batch of ops is **kept queued and retried
    /// on the next tick** (WAL ops are idempotent on replay) rather than being
    /// dropped, so a transient failure still lands its data; the recorded
    /// error stays visible until a consumer acknowledges it. For writes that
    /// must be durable before returning, use [`Store::write_durable`].
    Grouped { interval: Duration },
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
/// With WAL: fsync buffered ops (requeued on error), advance the durability
/// watermark, then compact if over threshold. Without WAL: clone state +
/// full `backend.save()`.
///
/// Compaction is deliberately handled *after* and independently of the
/// durability watermark: WAL ops become durable as soon as the fsync
/// succeeds, so a later snapshot failure must not stall `gen_flushed` or
/// block [`Store::flush`].
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
            match flush_pending(shared) {
                Ok(()) => {
                    // Monotonic: never regress under a race with
                    // write_durable's own watermark advance. A recovered
                    // retry advances the watermark but leaves any recorded
                    // error set until a consumer acknowledges it via
                    // write() / flush() / flush_error() — the failure did
                    // happen, so it stays visible until observed.
                    shared.gen_flushed.fetch_max(current_gen, Ordering::AcqRel);
                }
                Err(e) => {
                    *shared.last_error.lock() = Some(e);
                }
            }
        }

        maybe_compact_bg(shared);

        if should_shutdown {
            break;
        }
    }
}

/// Persist buffered work for one flusher tick. WAL: drain + fsync the pending
/// op-batches (requeued on error). Non-WAL: clone state + full save.
fn flush_pending<T: Clone, B: Backend<T>>(shared: &FlushShared<T, B>) -> Result<()> {
    match shared.incremental {
        Some(ref inc) => {
            let batched: Vec<Vec<Op>> = std::mem::take(&mut *shared.pending_ops.lock());
            flush_pending_batches(shared, inc, batched)
        }
        None => {
            let snapshot = shared.state.read().clone();
            shared.backend.save(&snapshot)
        }
    }
}

/// Compact the WAL from the background flusher if it is over threshold.
/// Independent of the durability watermark; records a snapshot failure into
/// `last_error` without stalling `gen_flushed`.
///
/// Goes through `compact_if_needed` rather than a bare `should_snapshot()` +
/// `snapshot()` pair so it cannot interleave with a foreground compaction
/// started by `write_durable` on another thread: the threshold re-check and
/// the state clone both happen inside the backend's compaction exclusion.
fn maybe_compact_bg<T: Clone, B: Backend<T>>(shared: &FlushShared<T, B>) {
    let Some(ref inc) = shared.incremental else {
        return;
    };
    if !inc.should_snapshot() {
        return;
    }
    let mut state_fn = || -> Result<T> { Ok(shared.state.read().clone()) };
    if let Err(e) = inc.compact_if_needed(&mut state_fn) {
        *shared.last_error.lock() = Some(e);
    }
}

/// Append and fsync a drained set of pending op-batches. On any error the
/// batches are pushed back to the front of `pending_ops` so the next flush
/// retries them — WAL ops replay idempotently, so a duplicated append is
/// harmless and this trades a little WAL growth for no silent op loss.
pub(super) fn flush_pending_batches<T, B: Backend<T>>(
    shared: &FlushShared<T, B>,
    inc: &Arc<dyn IncrementalSave<T>>,
    batched: Vec<Vec<Op>>,
) -> Result<()> {
    for batch in &batched {
        if let Err(e) = inc.save_ops(batch) {
            requeue_front(shared, batched);
            return Err(e);
        }
    }
    if let Err(e) = inc.sync() {
        requeue_front(shared, batched);
        return Err(e);
    }
    Ok(())
}

/// Push drained batches back ahead of any newly-arrived ops, preserving order.
fn requeue_front<T, B: Backend<T>>(shared: &FlushShared<T, B>, mut batched: Vec<Vec<Op>>) {
    let mut pending = shared.pending_ops.lock();
    batched.append(&mut pending);
    *pending = batched;
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
