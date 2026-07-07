//! Lock-acquisition helpers with a deadlock-detector budget, plus the
//! zero-copy read guard [`Ref`].
//!
//! Every state-lock acquisition in the store is bounded by a configurable
//! timeout so a genuine deadlock fails fast (a panic on the bare-guard
//! `read()` path, a typed [`Error::LockTimeout`] on the `Result`-bearing
//! write paths) instead of hanging forever.

use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::time::Duration;

use crate::error::{Error, Result};

/// Hard ceiling on how long lock acquisition will wait before it is
/// treated as a deadlock. Applies symmetrically to both `write()` and
/// `read()` — the legitimate worst case for either is a long-running
/// grouped flush holding the lock for tens of milliseconds; 30 s is
/// orders of magnitude past that and almost certainly indicates a
/// deadlock.
///
/// On timeout the `Result`-bearing write paths return
/// [`Error::LockTimeout`], while [`crate::store::Store::read`] panics (it
/// yields a bare [`Ref`] and has no error channel). Either way the process
/// fails fast instead of hanging forever. Real incident: openbinary's
/// `pool.recover()` held a read guard while looping over Running jobs
/// and calling `state.fail_job(...)` — the write waited forever for the
/// read guard to drop. Pre-fix the symptom was an hour-long startup
/// hang with no log line; post-fix the same bug surfaces at the budget
/// with the call site named.
pub(crate) const STATE_LOCK_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Minimum effective lock-wait budget. A sub-millisecond configured
/// timeout is clamped up to this floor so a near-zero value can never
/// turn the deadlock detector into an instant false positive on a lock
/// that is merely momentarily contended. See
/// [`crate::store::Store::set_lock_deadlock_timeout`].
const MIN_LOCK_WAIT: Duration = Duration::from_millis(1);

/// Likely cause reported when a write guard times out.
const WRITE_TIMEOUT_CAUSE: &str = "a guard from read() is held across this write(), or a \
     long-running operation holds the state lock on another thread";
/// Likely cause reported when a read guard times out inside a write path.
const READ_TIMEOUT_CAUSE: &str = "a writer is holding the state lock (long-running write \
     closure, or a guard held across an .await point)";
/// Likely cause reported when the write-serialization gate times out.
const GATE_TIMEOUT_CAUSE: &str = "write() (or write_durable()) was likely called from inside \
     another write closure on the same thread — the write gate is not reentrant";

/// Clamp a configured budget to [`MIN_LOCK_WAIT`] so sub-millisecond
/// values never truncate to an instant timeout.
#[must_use]
fn effective_timeout(configured: Duration) -> Duration {
    configured.max(MIN_LOCK_WAIT)
}

/// Convert a duration to whole microseconds for atomic storage,
/// saturating rather than truncating on overflow.
#[must_use]
pub(crate) fn duration_to_us(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// Acquire a write guard, or return [`Error::LockTimeout`] if the lock
/// is not available within the budget. Used by the `Result`-returning
/// write paths, which surface the timeout instead of hanging.
pub(crate) fn try_write_for<'a, T>(
    lock: &'a RwLock<T>,
    site: &'static str,
    timeout: Duration,
) -> Result<parking_lot::RwLockWriteGuard<'a, T>> {
    lock.try_write_for(effective_timeout(timeout))
        .ok_or_else(|| Error::lock_timeout(site, timeout, WRITE_TIMEOUT_CAUSE))
}

/// Acquire a read guard, or return [`Error::LockTimeout`]. Used by the
/// `Result`-returning write paths for their brief committed-state borrow.
pub(crate) fn try_read_for<'a, T>(
    lock: &'a RwLock<T>,
    site: &'static str,
    timeout: Duration,
) -> Result<parking_lot::RwLockReadGuard<'a, T>> {
    lock.try_read_for(effective_timeout(timeout))
        .ok_or_else(|| Error::lock_timeout(site, timeout, READ_TIMEOUT_CAUSE))
}

/// Acquire the write-serialization gate, or return
/// [`Error::LockTimeout`]. The gate is a plain (non-reentrant) mutex, so
/// the dominant real cause of a timeout here is calling `write()` from
/// inside another write closure on the same thread — a self-deadlock
/// that would otherwise hang forever.
pub(crate) fn try_lock_gate_for<'a>(
    gate: &'a Mutex<()>,
    site: &'static str,
    timeout: Duration,
) -> Result<parking_lot::MutexGuard<'a, ()>> {
    gate.try_lock_for(effective_timeout(timeout))
        .ok_or_else(|| Error::lock_timeout(site, timeout, GATE_TIMEOUT_CAUSE))
}

/// Acquire a read guard for [`crate::store::Store::read`], panicking on
/// timeout. `read()` returns a bare [`Ref`] with no error channel, so a
/// deadlock here can only be surfaced as a panic (the alternative is an
/// unbounded hang). The message names real causes only — `parking_lot` has
/// no lock poisoning, and an unwinding panic always releases its guards, so
/// a held-but-abandoned guard is not among them. See
/// [`STATE_LOCK_DEADLOCK_TIMEOUT`].
#[track_caller]
pub(crate) fn read_or_panic<'a, T>(
    lock: &'a RwLock<T>,
    site: &'static str,
    timeout: Duration,
) -> parking_lot::RwLockReadGuard<'a, T> {
    lock.try_read_for(effective_timeout(timeout))
        .unwrap_or_else(|| {
            panic!(
                "etchdb: read lock acquisition timed out after {timeout:?} at {site} \
                 — likely a deadlock. A writer is holding the state lock and never \
                 released it. Real causes: a guard from read() held across a call \
                 into write(); a guard held across an .await point in async code; \
                 or a genuinely long-running operation under the write lock. Check \
                 what most recently acquired the write lock on another thread."
            )
        })
}

/// Shared read guard — holds the read lock, provides zero-copy access.
///
/// # Deadlock hazard
///
/// This guard holds the state read lock for as long as it is alive.
/// Holding it across a call into [`crate::store::Store::write`] /
/// [`crate::store::Store::write_durable`] deadlocks the store (the
/// `RwLock` is not reentrant — the write waits on your own read guard),
/// and holding it across an `.await` point in async code lets the guard
/// straddle a task yield and block a writer. Drop it as soon as the borrow
/// is done, or prefer [`crate::store::Store::read_with`] for scoped access
/// that makes the mistake unrepresentable. See
/// [`crate::store::Store::read`].
pub struct Ref<'a, T>(pub(crate) RwLockReadGuard<'a, T>);

impl<'a, T> std::ops::Deref for Ref<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
