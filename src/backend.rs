//! Persistence backend trait and built-in implementations.
//!
//! The `Backend` trait decouples the `Store` concurrency engine from the
//! serialization format and storage medium.

use serde::{de::DeserializeOwned, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::Result;

/// Persistence backend — how state gets to/from durable storage.
pub trait Backend<T>: Send + Sync {
    /// Load state from storage. Returns `T::default()` equivalent when empty.
    fn load(&self) -> Result<T>;

    /// Persist state to storage. Must be atomic or crash-safe.
    fn save(&self, state: &T) -> Result<()>;
}

/// Binary postcard backend with atomic rename and backup.
///
/// Write path: serialize → tmp file → fsync → hard-link backup → rename → fsync dir.
pub struct PostcardBackend {
    path: PathBuf,
}

impl PostcardBackend {
    pub fn new(path: PathBuf) -> Result<Self> {
        let tmp = path.with_extension("tmp");
        let _ = std::fs::remove_file(&tmp);
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn try_load_backup(path: &Path) -> Result<Vec<u8>> {
        let bak = path.with_extension("bak");
        match std::fs::read(&bak) {
            Ok(bytes) => Ok(bytes),
            Err(_) => Ok(Vec::new()),
        }
    }
}

impl<T: Serialize + DeserializeOwned + Default> Backend<T> for PostcardBackend {
    fn load(&self) -> Result<T> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(postcard::from_bytes(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let bytes = Self::try_load_backup(&self.path)?;
                if bytes.is_empty() {
                    Ok(T::default())
                } else {
                    Ok(postcard::from_bytes(&bytes)?)
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    fn save(&self, state: &T) -> Result<()> {
        let tmp = self.path.with_extension("tmp");
        let bytes = postcard::to_allocvec(state)?;

        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }

        let bak = self.path.with_extension("bak");
        let _ = std::fs::remove_file(&bak);
        let _ = std::fs::hard_link(&self.path, &bak);

        std::fs::rename(&tmp, &self.path)?;

        if let Some(parent) = self.path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
        {
            let _ = dir.sync_all();
        }

        Ok(())
    }
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
