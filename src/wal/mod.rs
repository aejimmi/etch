//! Write-Ahead Log (WAL) for incremental persistence.
//!
//! Instead of serializing the full state on every write, the WAL appends
//! only the diff (changed keys) to an append-only log file. Periodic
//! snapshots compact the WAL.
//!
//! # Transaction Capture
//!
//! The `overlay` + `merge` modules provide a zero-clone write path:
//! mutations execute against an overlay that borrows committed state, emitting
//! ops directly. On commit, the overlay is merged into state in O(changed keys).

pub mod collection;
mod diff;
mod format;
pub mod key;
pub mod merge;
mod op;
pub mod overlay;
mod writer;

pub use collection::Collection;
pub use diff::{
    Replayable, apply_op, apply_op_bytes, apply_op_hash, apply_op_hash_bytes, apply_op_hash_with,
    apply_op_with,
};
pub use key::EtchKey;
pub use merge::{Transactable, apply_overlay_btree, apply_overlay_hash};
pub use op::Op;
pub use overlay::{MapRead, Overlay};
pub use writer::{IncrementalSave, WalBackend};

#[cfg(test)]
#[path = "format_test.rs"]
mod format_test;

#[cfg(test)]
#[path = "overlay_test.rs"]
mod overlay_test;

#[cfg(test)]
#[path = "writer_test.rs"]
mod writer_test;

#[cfg(test)]
#[path = "diff_test.rs"]
mod diff_test;

#[cfg(test)]
#[path = "merge_test.rs"]
mod merge_test;

#[cfg(test)]
#[path = "key_test.rs"]
mod key_test;
