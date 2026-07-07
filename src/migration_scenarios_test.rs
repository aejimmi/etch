//! Comprehensive migration and schema-evolution scenario tests.
//!
//! Each scenario tests a specific shape of schema change, organized by
//! category. The goal is to prove the migration machinery behaves
//! correctly across the full matrix of things that can change between
//! releases — not just the happy path.
//!
//! Scenarios are grouped:
//!
//! - `auto`      — msgpack named mode should handle with NO migration.
//! - `explicit`  — requires a user-provided migration function.
//! - `failure`   — failure modes: panic, error, unknown variant, etc.
//! - `integration` — multi-collection, mixed-WAL, legacy-read, etc.
//!
//! Each scenario uses its own types inside a submodule so there's zero
//! cross-contamination between tests.

pub(crate) use crate::wal::{
    CollectionSection, SnapshotEntry, SnapshotPayload, apply_op_versioned_with,
    apply_op_versioned_with_ctx, encode_msgpack_value, encode_versioned_value, load_snapshot_entry,
};
pub(crate) use crate::{
    EtchKey, IncrementalSave, MigrationSet, Op, Quarantine, QuarantineReason, ReplayContext,
    ReplayFormat, Replayable, Store, Transactable, WalBackend,
};
pub(crate) use serde::{Deserialize, Serialize};
pub(crate) use std::collections::BTreeMap;

#[path = "migration_scenarios_test/auto.rs"]
mod auto;
#[path = "migration_scenarios_test/explicit.rs"]
mod explicit;
#[path = "migration_scenarios_test/failure.rs"]
mod failure;
#[path = "migration_scenarios_test/integration1.rs"]
mod integration1;
#[path = "migration_scenarios_test/integration2.rs"]
mod integration2;
#[path = "migration_scenarios_test/integration3.rs"]
mod integration3;
#[path = "migration_scenarios_test/integration4.rs"]
mod integration4;
