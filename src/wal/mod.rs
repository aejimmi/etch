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

mod diff;
mod format;
pub mod merge;
mod op;
pub mod overlay;
mod writer;

pub use diff::{Diffable, apply_op, diff_map};
pub use merge::{Transactable, apply_overlay_map};
pub use op::Op;
pub use overlay::Overlay;
pub use writer::{IncrementalSave, WalBackend};

#[cfg(test)]
#[path = "format_test.rs"]
mod format_test;

#[cfg(test)]
#[path = "overlay_test.rs"]
mod overlay_test;
