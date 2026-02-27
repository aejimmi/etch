//! etch — lightweight embedded store with Write-Ahead Log.
//!
//! In-memory state backed by a Write-Ahead Log (WAL) with postcard binary format.
//! Reads are zero-copy borrows via `RwLock`. Writes are atomic and crash-safe.
//!
//! # Architecture
//!
//! ```text
//! Store<T, B>
//!   ├── RwLock<T>        ← in-memory state, zero-copy reads
//!   ├── Mutex<()>        ← serializes writers (reads unblocked during persist)
//!   └── Backend<T>       ← pluggable: NullBackend, PostcardBackend, WalBackend
//! ```
//!
//! # Quick start
//!
//! ```rust
//! use etch::{Store, Diffable, Op};
//! use serde::{Serialize, Deserialize};
//! use std::collections::BTreeMap;
//!
//! #[derive(Debug, Clone, Default, Serialize, Deserialize)]
//! struct AppState {
//!     items: BTreeMap<String, String>,
//! }
//!
//! impl Diffable for AppState {
//!     fn diff(before: &Self, after: &Self) -> Vec<Op> {
//!         let mut ops = Vec::new();
//!         etch::diff_map(&before.items, &after.items, 0, &mut ops);
//!         ops
//!     }
//!     fn apply(&mut self, ops: &[Op]) -> etch::Result<()> {
//!         for op in ops {
//!             etch::apply_op(&mut self.items, op)?;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! let store = Store::<AppState>::memory();
//!
//! store.write(|state| {
//!     state.items.insert("key".into(), "value".into());
//!     Ok(())
//! }).unwrap();
//!
//! let state = store.read();
//! assert_eq!(state.items["key"], "value");
//! ```

pub mod backend;
pub mod error;
pub mod store;
pub mod wal;

pub use backend::{Backend, NullBackend, PostcardBackend};
pub use error::{Error, Result};
pub use store::{FlushPolicy, Ref, Store};
pub use wal::{
    Diffable, IncrementalSave, Op, Overlay, Transactable, WalBackend, apply_op, apply_overlay_map,
    diff_map,
};
