//! Persistence backend trait and built-in implementations.
//!
//! The `Backend` trait decouples the `Store` concurrency engine from the
//! serialization format and storage medium.

use crate::error::Result;

/// Persistence backend — how state gets to/from durable storage.
pub trait Backend<T>: Send + Sync {
    /// Load state from storage. Returns `T::default()` equivalent when empty.
    fn load(&self) -> Result<T>;

    /// Persist state to storage. Must be atomic or crash-safe.
    fn save(&self, state: &T) -> Result<()>;
}

/// No-op backend for in-memory stores.
pub struct NullBackend;

impl<T: Default> Backend<T> for NullBackend {
    fn load(&self) -> Result<T> {
        Ok(T::default())
    }

    fn save(&self, _state: &T) -> Result<()> {
        Ok(())
    }
}
