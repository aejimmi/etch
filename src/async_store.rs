//! Async wrapper for `Store`.
//!
//! Write and open operations run on Tokio's blocking pool via
//! [`tokio::task::spawn_blocking`], so they never block a runtime worker and
//! work on both multi-thread and current-thread runtimes. Reads stay
//! synchronous (a sub-microsecond `RwLock` acquisition).

use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::backend::Backend;
use crate::error::Result;
use crate::store::{Ref, Store};
use crate::wal::{Replayable, Transactable, WalBackend};

/// Async-friendly wrapper around `Store`.
///
/// Wraps a `Store` in an `Arc` and offloads blocking work (transaction
/// commit, fsync, open) to [`tokio::task::spawn_blocking`], so the calling
/// task yields instead of monopolizing a worker. Clone is cheap (an `Arc`
/// bump).
///
/// # Reads and `.await`
///
/// [`AsyncStore::read`] is intentionally synchronous and cheap. The [`Ref`]
/// it returns holds the state read lock; like [`Store::read`], it must **not**
/// be held across an `.await` point — doing so straddles a task yield and can
/// block a writer for the duration of the suspension. Read the value out (or
/// clone it) and drop the [`Ref`] before awaiting.
pub struct AsyncStore<T, B: Backend<T>> {
    inner: Arc<Store<T, B>>,
}

impl<T, B: Backend<T>> Clone for AsyncStore<T, B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Run a blocking closure on the Tokio blocking pool, propagating a panic in
/// the closure as a panic on the caller (rather than swallowing it). The
/// handle is always awaited, so a `JoinError` can only be a panic.
async fn run_blocking<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(r) => r,
        Err(join_err) => std::panic::resume_unwind(join_err.into_panic()),
    }
}

impl<T, B: Backend<T>> AsyncStore<T, B> {
    /// Wrap an existing `Store` for async use.
    pub fn from_store(store: Store<T, B>) -> Self {
        Self {
            inner: Arc::new(store),
        }
    }

    /// Access the underlying store reference.
    pub fn store(&self) -> &Store<T, B> {
        &self.inner
    }
}

impl<T: Replayable + Serialize + DeserializeOwned + Default + Send + Sync + 'static>
    AsyncStore<T, WalBackend<T>>
{
    /// Open a WAL-backed store for async use.
    ///
    /// Runs `Store::open_wal` on the blocking pool. Lenient: replay loss is
    /// recorded, not raised — inspect it via `store().replay_report()`.
    pub async fn open_wal(dir: PathBuf) -> Result<Self> {
        run_blocking(move || Store::open_wal(dir).map(Self::from_store)).await
    }

    /// Async [`Store::open_wal_with_report`]. Returns the store and the
    /// [`crate::ReplayReport`] for the load.
    pub async fn open_wal_with_report(dir: PathBuf) -> Result<(Self, crate::ReplayReport)> {
        run_blocking(move || {
            Store::open_wal_with_report(dir).map(|(s, r)| (Self::from_store(s), r))
        })
        .await
    }

    /// Async [`Store::open_wal_strict`]. Aborts on any replay data loss.
    pub async fn open_wal_strict(dir: PathBuf) -> Result<Self> {
        run_blocking(move || Store::open_wal_strict(dir).map(Self::from_store)).await
    }
}

impl<T: Clone, B: Backend<T>> AsyncStore<T, B> {
    /// Zero-copy read — synchronous, no async overhead.
    ///
    /// The returned [`Ref`] holds the state read lock; do not hold it across
    /// an `.await` point (see the type-level docs).
    pub fn read(&self) -> Ref<'_, T> {
        self.inner.read()
    }
}

impl<T: Transactable + Clone + Send + Sync + 'static, B: Backend<T> + Send + Sync + 'static>
    AsyncStore<T, B>
{
    /// Async atomic write via transaction capture.
    ///
    /// The closure and its result cross a thread boundary onto the blocking
    /// pool, hence the `Send + 'static` bounds. The calling task yields for
    /// the duration of the commit instead of blocking a worker.
    pub async fn write<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.write(f)).await
    }

    /// Async atomic write with guaranteed immediate persistence.
    pub async fn write_durable<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.write_durable(f)).await
    }
}

impl<T: Clone + Send + Sync + 'static, B: Backend<T> + Send + Sync + 'static> AsyncStore<T, B> {
    /// Async flush — forces immediate persistence of buffered writes.
    pub async fn flush(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.flush()).await
    }
}
