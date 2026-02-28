//! etch — a fast, embedded database for Rust.
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
//! use etch::{Store, Replayable, Op, Overlay, Transactable};
//! use serde::{Serialize, Deserialize};
//! use std::collections::BTreeMap;
//!
//! #[derive(Debug, Clone, Default, Serialize, Deserialize)]
//! struct AppState {
//!     items: BTreeMap<String, Item>,
//! }
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Item { name: String }
//!
//! const ITEMS: u8 = 0;
//!
//! impl Replayable for AppState {
//!     fn apply(&mut self, ops: &[Op]) -> etch::Result<()> {
//!         for op in ops { etch::apply_op(&mut self.items, op)?; }
//!         Ok(())
//!     }
//! }
//!
//! struct AppTx<'a> {
//!     committed: &'a AppState,
//!     items: Overlay<Item>,
//!     ops: Vec<Op>,
//! }
//!
//! impl<'a> AppTx<'a> {
//!     fn insert(&mut self, key: &str, item: Item) {
//!         self.ops.push(Op::Put {
//!             collection: ITEMS,
//!             key: key.to_string(),
//!             value: postcard::to_allocvec(&item).expect("serialize"),
//!         });
//!         self.items.put(key.to_string(), item);
//!     }
//! }
//!
//! struct AppOverlay { items: Overlay<Item> }
//!
//! impl Transactable for AppState {
//!     type Tx<'a> = AppTx<'a>;
//!     type Overlay = AppOverlay;
//!     fn begin_tx(&self) -> AppTx<'_> {
//!         AppTx { committed: self, items: Overlay::new(), ops: Vec::new() }
//!     }
//!     fn finish_tx(tx: AppTx<'_>) -> (Vec<Op>, AppOverlay) {
//!         (tx.ops, AppOverlay { items: tx.items })
//!     }
//!     fn apply_overlay(&mut self, overlay: AppOverlay) {
//!         etch::apply_overlay_map(&mut self.items, overlay.items);
//!     }
//! }
//!
//! let store = Store::<AppState>::memory();
//!
//! store.write(|tx| {
//!     tx.insert("k1", Item { name: "first".into() });
//!     Ok(())
//! }).unwrap();
//!
//! let state = store.read();
//! assert_eq!(state.items["k1"].name, "first");
//! ```

pub mod backend;
pub mod error;
pub mod store;
pub mod wal;

pub use backend::{Backend, NullBackend, PostcardBackend};
pub use error::{Error, Result};
pub use store::{FlushPolicy, Ref, Store};
pub use wal::{
    IncrementalSave, Op, Overlay, Replayable, Transactable, WalBackend, apply_op, apply_overlay_map,
};

#[cfg(test)]
#[path = "error_test.rs"]
mod error_test;
