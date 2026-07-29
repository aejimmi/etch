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
//!   └── Backend<T>       ← pluggable: WalBackend, NullBackend, or bring your own
//! ```
//!
//! # Quick start
//!
//! ```rust
//! use etchdb::{Store, Replayable, Op, Overlay, Transactable, apply_overlay_btree};
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
//!     fn apply_with_format(
//!         &mut self,
//!         ops: &[Op],
//!         _format: etchdb::ReplayFormat,
//!     ) -> etchdb::Result<()> {
//!         for op in ops { etchdb::apply_op(&mut self.items, op)?; }
//!         Ok(())
//!     }
//! }
//!
//! struct AppTx<'a> {
//!     committed: &'a AppState,
//!     items: Overlay<String, Item>,
//!     ops: Vec<Op>,
//! }
//!
//! impl<'a> AppTx<'a> {
//!     fn insert(&mut self, key: &str, item: Item) {
//!         self.ops.push(Op::Put {
//!             collection: ITEMS,
//!             key: key.as_bytes().to_vec(),
//!             value: postcard::to_allocvec(&item).expect("serialize"),
//!         });
//!         self.items.put(key.to_string(), item);
//!     }
//! }
//!
//! struct AppOverlay { items: Overlay<String, Item> }
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
//!         etchdb::apply_overlay_btree(&mut self.items, overlay.items);
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

// Allow the crate to refer to itself as `etchdb` in generated code from derive macros.
extern crate self as etchdb;

#[cfg(feature = "async")]
pub mod async_store;
pub mod backend;
pub mod error;
pub mod store;
pub mod wal;

#[cfg(feature = "async")]
pub use async_store::AsyncStore;
pub use backend::{Backend, NullBackend};
pub use error::{Error, Result};
/// Re-export derive macros so users can `use etchdb::{Replayable, Transactable}` for both
/// the trait and the derive macro (same pattern as serde).
pub use etchdb_derive::{Replayable, Transactable};
pub use store::{CheckpointReport, FlushPolicy, Ref, Store};
/// High-level API surface. The low-level WAL replay/format primitives
/// (`apply_op_versioned_*`, `encode_*`, `split_versioned_value`,
/// `SnapshotPayload`, …) are intentionally **not** re-exported here as of
/// 0.5.0 — reach them through the documented [`mod@wal`] module. The derive
/// macro emits fully-qualified `::etchdb::wal::…` paths to them, so generated
/// code keeps working without the flat re-export.
pub use wal::{
    ChainResult, Collection, EtchKey, IncrementalSave, LoadMode, MapRead, MigrationError,
    MigrationFn, MigrationSet, Op, Overlay, Quarantine, QuarantineReason, QuarantinedEntry,
    ReplayContext, ReplayFormat, ReplayReport, Replayable, SchemaDrift, SnapshotStatus,
    Transactable, WalBackend, apply_op, apply_op_hash, apply_overlay_btree, apply_overlay_hash,
};

#[cfg(test)]
#[path = "error_test.rs"]
mod error_test;

#[cfg(all(test, feature = "async"))]
#[path = "async_store_test.rs"]
mod async_store_test;

#[cfg(test)]
#[path = "derive_test.rs"]
mod derive_test;

#[cfg(test)]
#[path = "derive_generic_test.rs"]
mod derive_generic_test;

#[cfg(test)]
#[path = "migration_scenarios_test.rs"]
mod migration_scenarios_test;

#[cfg(test)]
#[path = "replay_report_test.rs"]
mod replay_report_test;
