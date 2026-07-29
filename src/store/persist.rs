//! Write paths: transaction capture, overlay merge, and the immediate/
//! grouped/non-WAL persistence branches for [`Store::write`] and
//! [`Store::write_durable`].

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::backend::Backend;
use crate::error::Result;
use crate::wal::{IncrementalSave, Op, Transactable};

use super::flush::flush_pending_batches;
use super::lock::{try_lock_gate_for, try_read_for, try_write_for};
use super::{FlushShared, Store};

// Write methods — zero-clone transaction capture.
//
// `T: Clone` is required so the non-WAL persistence path and immediate-mode
// WAL compaction can snapshot state under a short lock and serialize/fsync it
// with no lock held (see the module-level "Reads unblocked during
// persistence" note). Every derived or hand-written `Transactable` state in
// practice is `Clone` already.
impl<T: Transactable + Clone, B: Backend<T>> Store<T, B> {
    /// Atomic write via transaction capture.
    ///
    /// Borrows committed state via a read lock, executes mutations against an
    /// overlay that captures ops directly, then merges the overlay into state.
    /// O(changed keys), not O(total entries).
    ///
    /// **With WAL (Immediate)**: begin_tx → mutate → finish → append ops →
    /// fsync → merge → compact if over threshold.
    /// **With WAL (Grouped)**: begin_tx → mutate → finish → buffer ops → merge
    /// → bump gen. The ops are fsynced later by the flusher; see the
    /// durability contract on [`FlushPolicy::Grouped`] — an acknowledged write
    /// is durable only after the next flush.
    /// **Without WAL**: overlay merge, then (if a real backend) a full
    /// snapshot serialized outside the state lock.
    ///
    /// In Grouped mode, a returned `Err` may be a *previous* background-flush
    /// failure surfaced fail-fast at the start of this call (acknowledged
    /// writes since the last flush may be lost). Poll
    /// [`Store::last_flush_error`] to observe divergence without writing.
    ///
    /// [`FlushPolicy::Grouped`]: super::FlushPolicy::Grouped
    pub fn write<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R>,
    {
        let _gate = try_lock_gate_for(&self.write_gate, "write", self.lock_deadlock_timeout())?;

        // Fail-fast on a stashed grouped-flusher error (consumes it).
        if let Some(ref shared) = self.shared
            && let Some(err) = shared.last_error.lock().take()
        {
            return Err(err);
        }

        let (ops, overlay, result) = self.capture_tx(f)?;

        match self.incremental {
            Some(ref inc) => {
                if !ops.is_empty() {
                    match &self.shared {
                        None => {
                            inc.save_ops(&ops)?;
                            inc.sync()?;
                        }
                        Some(shared) => shared.pending_ops.lock().push(ops),
                    }
                }
                self.merge_overlay(overlay)?;
                match &self.shared {
                    None => self.maybe_compact()?,
                    Some(shared) => Self::mark_written(shared),
                }
            }
            None => match &self.shared {
                None => {
                    // Snapshot under a short lock, serialize + fsync unlocked.
                    let snapshot = self.merge_and_snapshot(overlay)?;
                    self.backend.save(&snapshot)?;
                }
                Some(shared) => {
                    self.merge_overlay(overlay)?;
                    Self::mark_written(shared);
                }
            },
        }
        Ok(result)
    }

    /// Run `f` against a fresh transaction under a short read borrow, then
    /// release the borrow and hand back the captured ops, overlay, and result.
    fn capture_tx<F, R>(&self, f: F) -> Result<(Vec<Op>, T::Overlay, R)>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R>,
    {
        let state_guard =
            try_read_for(&self.state, "write/begin_tx", self.lock_deadlock_timeout())?;
        let mut tx = state_guard.begin_tx();
        let result = f(&mut tx)?;
        let (ops, overlay) = T::finish_tx(tx);
        drop(state_guard);
        Ok((ops, overlay, result))
    }

    /// Merge a captured overlay into committed state under a short write lock.
    fn merge_overlay(&self, overlay: T::Overlay) -> Result<()> {
        try_write_for(&self.state, "write/merge", self.lock_deadlock_timeout())?
            .apply_overlay(overlay);
        Ok(())
    }

    /// Merge the overlay and clone the resulting state under a single short
    /// write lock. The caller serializes and fsyncs the clone with no lock
    /// held, so a slow disk never stalls readers or the next writer.
    fn merge_and_snapshot(&self, overlay: T::Overlay) -> Result<T> {
        let mut guard = try_write_for(&self.state, "write/snapshot", self.lock_deadlock_timeout())?;
        guard.apply_overlay(overlay);
        Ok(guard.clone())
    }

    /// Record a grouped-mode write and wake the flusher.
    fn mark_written(shared: &FlushShared<T, B>) {
        shared.gen_written.fetch_add(1, Ordering::Release);
        shared.notify.notify_one();
    }

    /// Atomic write with guaranteed immediate persistence.
    ///
    /// Same as `write()` but forces an immediate fsync regardless of flush
    /// policy. Use for critical writes that must survive a crash.
    pub fn write_durable<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R>,
    {
        let _gate = try_lock_gate_for(
            &self.write_gate,
            "write_durable",
            self.lock_deadlock_timeout(),
        )?;

        let (ops, overlay, result) = self.capture_tx(f)?;

        match self.incremental {
            Some(ref inc) => {
                self.append_durable(inc, ops)?;
                self.merge_overlay(overlay)?;
                self.maybe_compact()?;
            }
            None => {
                // Non-WAL: snapshot under a short lock, persist unlocked.
                let snapshot = self.merge_and_snapshot(overlay)?;
                self.backend.save(&snapshot)?;
            }
        }

        if let Some(ref shared) = self.shared {
            Self::mark_flushed(shared);
        }
        Ok(result)
    }
}

// Persistence helpers shared by the transactional write paths and by
// `Store::retry_quarantine`, which is not a transaction but must reach the WAL
// through the same gated, order-preserving route.
//
// `T: Clone` is the only bound these need — `maybe_compact` clones state for
// the snapshot — so `retry_quarantine` can use them without requiring
// `Transactable`.
impl<T: Clone, B: Backend<T>> Store<T, B> {
    /// Append `ops` to the WAL and fsync them.
    ///
    /// In Grouped mode the pending buffer is drained and flushed together with
    /// `ops` as one batch, so an op appended here can never be ordered *ahead*
    /// of an earlier-acknowledged write that supersedes it. On error the whole
    /// batch is requeued for retry (WAL replay is idempotent) rather than lost.
    /// In Immediate mode nothing is buffered, so this appends and fsyncs
    /// directly.
    ///
    /// The caller must hold the write gate: this is the only thing that keeps
    /// the append out of a concurrent [`Store::checkpoint_to`]'s copy window.
    pub(super) fn append_durable(
        &self,
        inc: &Arc<dyn IncrementalSave<T>>,
        ops: Vec<Op>,
    ) -> Result<()> {
        let Some(shared) = &self.shared else {
            if !ops.is_empty() {
                inc.save_ops(&ops)?;
            }
            return inc.sync();
        };
        let mut batched = std::mem::take(&mut *shared.pending_ops.lock());
        if !ops.is_empty() {
            batched.push(ops);
        }
        flush_pending_batches(shared, inc, batched)
    }

    /// WAL compaction from the write path: once the WAL crosses the snapshot
    /// threshold, write a fresh snapshot (resetting the WAL) with no state
    /// lock held across the disk write. Cheap when under threshold — a single
    /// atomic compare.
    ///
    /// The state clone happens inside the backend's compaction exclusion, not
    /// here, so this can never race the grouped flusher's compaction into a
    /// shared `snapshot.tmp` or a doubled `wal.prev` rotation.
    pub(super) fn maybe_compact(&self) -> Result<()> {
        let Some(ref inc) = self.incremental else {
            return Ok(());
        };
        if !inc.should_snapshot() {
            return Ok(());
        }
        let timeout = self.lock_deadlock_timeout();
        let state = &self.state;
        let mut state_fn =
            move || -> Result<T> { Ok(try_read_for(state, "write/compact", timeout)?.clone()) };
        inc.compact_if_needed(&mut state_fn).map(|_| ())
    }

    /// Record a write that is already durable (write_durable). Advances the
    /// flushed watermark monotonically so it can never regress under a race
    /// with the flusher thread.
    pub(super) fn mark_flushed(shared: &FlushShared<T, B>) {
        let generation = shared.gen_written.fetch_add(1, Ordering::AcqRel) + 1;
        shared.gen_flushed.fetch_max(generation, Ordering::AcqRel);
    }
}
