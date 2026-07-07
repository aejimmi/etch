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
pub mod migration;
mod op;
pub mod overlay;
pub mod quarantine;
pub mod report;
pub mod snapshot;
mod writer;

pub use collection::Collection;
pub use diff::{
    ReplayContext, ReplayFormat, Replayable, apply_op, apply_op_bytes, apply_op_hash,
    apply_op_hash_bytes, apply_op_hash_with, apply_op_versioned_hash_with,
    apply_op_versioned_hash_with_ctx, apply_op_versioned_with, apply_op_versioned_with_ctx,
    apply_op_with, encode_msgpack_value, encode_versioned_value, load_snapshot_entry,
    split_versioned_value,
};
pub use key::EtchKey;
pub use merge::{Transactable, apply_overlay_btree, apply_overlay_hash};
pub use migration::{ChainResult, MigrationError, MigrationFn, MigrationSet};
pub use op::{Op, format_op_key};
pub use overlay::{MapRead, Overlay};
pub use quarantine::{QUARANTINE_FILE, Quarantine, QuarantineReason, QuarantinedEntry};
pub use report::{ReplayReport, SchemaDrift, SnapshotStatus};
pub use snapshot::{CollectionSection, SnapshotEntry, SnapshotPayload};
pub use writer::{IncrementalSave, LoadMode, WalBackend};

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

#[cfg(test)]
#[path = "fuzz_test.rs"]
mod fuzz_test;
