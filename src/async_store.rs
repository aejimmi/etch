//! Async wrapper for `Store`.
//!
//! Uses `tokio::task::block_in_place` for write operations so they don't
//! block the async runtime. Reads are synchronous (sub-microsecond RwLock).
//! Requires a multi-threaded tokio runtime.

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
/// Wraps a `Store` in an `Arc` and uses `block_in_place` for writes
/// so they run on the current thread without blocking async tasks.
/// Clone is cheap (Arc bump).
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
    /// Wraps `Store::open_wal` in `block_in_place` so it can be called from
    /// an async context without blocking the runtime.
    pub async fn open_wal(dir: PathBuf) -> Result<Self> {
        tokio::task::block_in_place(move || Store::open_wal(dir).map(Self::from_store))
    }
}

impl<T: Clone, B: Backend<T>> AsyncStore<T, B> {
    /// Zero-copy read — synchronous, no async overhead.
    pub fn read(&self) -> Ref<'_, T> {
        self.inner.read()
    }
}

impl<T: Transactable, B: Backend<T>> AsyncStore<T, B> {
    /// Async atomic write via transaction capture.
    ///
    /// Uses `block_in_place` to run the write on the current thread.
    pub async fn write<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R>,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::block_in_place(move || inner.write(f))
    }

    /// Async atomic write with guaranteed immediate persistence.
    pub async fn write_durable<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&mut T::Tx<'a>) -> Result<R>,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::block_in_place(move || inner.write_durable(f))
    }
}

impl<T: Clone + Send + Sync + 'static, B: Backend<T> + Send + Sync + 'static> AsyncStore<T, B> {
    /// Async flush — forces immediate persistence of buffered writes.
    pub async fn flush(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        tokio::task::block_in_place(move || inner.flush())
    }
}
