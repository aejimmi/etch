//! Acceptance tests for the replay-observability overhaul.
//!
//! Covers: per-class skip accounting in the [`ReplayReport`], strict-mode
//! aborts, lenient-default loading, shape-aware schema fingerprints, and
//! backward compatibility of old pair-only fingerprints.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::wal::{
    CollectionSection, SnapshotEntry, SnapshotPayload, encode_msgpack_value,
    encode_versioned_value, split_versioned_value,
};
use crate::{
    Error, IncrementalSave, Op, Replayable, SchemaDrift, SnapshotStatus, Store, WalBackend,
};

// -------------------------------------------------------------------------
// Fixtures
// -------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Item {
    n: u32,
}

/// State declaring collection 0 at version 2 with NO migration — a v1 value
/// therefore has no forward path and lands in quarantine.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct StateV2 {
    #[etch(collection = 0, version = 2)]
    items: BTreeMap<String, Item>,
}

use crate::Transactable;

/// Write a v3 (msgpack, uncompressed) snapshot file directly.
fn write_snapshot(dir: &std::path::Path, payload: &SnapshotPayload) {
    std::fs::create_dir_all(dir).unwrap();
    let encoded = rmp_serde::to_vec_named(payload).unwrap();
    let mut out = vec![b'E', b'S', b'N', b'A', 3u8];
    out.extend_from_slice(&encoded);
    std::fs::write(dir.join("snapshot.postcard"), out).unwrap();
}

/// A snapshot holding one v1 `Item` for collection 0, current_version 1.
fn v1_item_snapshot(key: &[u8], version: u16) -> SnapshotPayload {
    let envelope = encode_versioned_value(version, &Item { n: 42 }).unwrap();
    let (_v, payload) = split_versioned_value(&envelope).unwrap();
    SnapshotPayload {
        schema_fingerprint: 0,
        collections: vec![CollectionSection {
            collection_id: 0,
            current_version: version,
            entries: vec![SnapshotEntry {
                key: key.to_vec(),
                version,
                value: payload.to_vec(),
            }],
        }],
    }
}

/// Append a raw WAL entry (len | payload | xxh3) to an existing `wal.bin`.
fn append_wal_entry(dir: &std::path::Path, ops: &[Op]) {
    use std::io::Write;
    let payload = postcard::to_allocvec(&ops).unwrap();
    let hash = xxhash_rust::xxh3::xxh3_64(&payload);
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(dir.join("wal.bin"))
        .unwrap();
    f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
    f.write_all(&payload).unwrap();
    f.write_all(&hash.to_le_bytes()).unwrap();
    f.sync_all().unwrap();
}

/// Create `wal.bin` with a valid v4 header by briefly opening the backend.
fn init_wal(dir: &std::path::Path) {
    let _ = Store::<StateV2, WalBackend<StateV2>>::open_wal(dir.to_path_buf()).unwrap();
}

// -------------------------------------------------------------------------
// Lenient default still loads, with the loss now visible
// -------------------------------------------------------------------------

#[test]
fn test_lenient_open_loads_and_reports_quarantine() {
    let dir = tempfile::tempdir().unwrap();
    write_snapshot(dir.path(), &v1_item_snapshot(b"k", 1));

    let store = Store::<StateV2, WalBackend<StateV2>>::open_wal(dir.path().to_path_buf()).unwrap();
    // Value could not migrate (no 1->2) → quarantined, but the store is up.
    assert!(store.read().items.is_empty());

    let report = store.replay_report();
    assert_eq!(report.quarantined(), 1);
    assert_eq!(report.value_decode_skipped(), 1);
    assert!(report.has_loss());
    assert_eq!(report.snapshot(), &SnapshotStatus::Loaded);
    assert!(!report.quarantine_by_reason().is_empty());
}

#[test]
fn test_open_wal_with_report_returns_same_report() {
    let dir = tempfile::tempdir().unwrap();
    write_snapshot(dir.path(), &v1_item_snapshot(b"k", 1));

    let (store, report) =
        Store::<StateV2, WalBackend<StateV2>>::open_wal_with_report(dir.path().to_path_buf())
            .unwrap();
    assert_eq!(report.quarantined(), 1);
    assert_eq!(store.replay_report().quarantined(), report.quarantined());
}

#[test]
fn test_clean_open_reports_no_loss() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store =
            Store::<StateV2, WalBackend<StateV2>>::open_wal(dir.path().to_path_buf()).unwrap();
        store
            .write(|tx| {
                tx.items.put("a".into(), Item { n: 1 })?;
                Ok(())
            })
            .unwrap();
    }
    let store = Store::<StateV2, WalBackend<StateV2>>::open_wal(dir.path().to_path_buf()).unwrap();
    let report = store.replay_report();
    assert!(!report.has_loss());
    assert_eq!(report.quarantined(), 0);
    assert!(report.applied_ops() >= 1);
    assert_eq!(store.read().items.get("a").map(|i| i.n), Some(1));
}

// -------------------------------------------------------------------------
// Strict mode aborts on each loss class
// -------------------------------------------------------------------------

#[test]
fn test_strict_aborts_on_quarantine_with_replay_loss() {
    let dir = tempfile::tempdir().unwrap();
    write_snapshot(dir.path(), &v1_item_snapshot(b"k", 1));

    match Store::<StateV2, WalBackend<StateV2>>::open_wal_strict(dir.path().to_path_buf()) {
        Err(Error::ReplayLoss { summary }) => {
            assert!(summary.contains("quarantined"), "got: {summary}");
        }
        Err(other) => panic!("expected ReplayLoss, got {other:?}"),
        Ok(_) => panic!("quarantine must abort strict load"),
    }
}

#[test]
fn test_strict_aborts_on_future_version_with_schema_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    // Value tagged version 9, current schema is version 2 → future version.
    write_snapshot(dir.path(), &v1_item_snapshot(b"k", 9));

    match Store::<StateV2, WalBackend<StateV2>>::open_wal_strict(dir.path().to_path_buf()) {
        Err(Error::SchemaVersionMismatch { stored, current }) => {
            assert_eq!(stored, 9);
            assert_eq!(current, 2);
        }
        Err(other) => panic!("expected SchemaVersionMismatch, got {other:?}"),
        Ok(_) => panic!("future version must abort strict load"),
    }
}

#[test]
fn test_strict_aborts_on_unknown_collection() {
    let dir = tempfile::tempdir().unwrap();
    init_wal(dir.path());
    // Op for collection 5 — StateV2 only declares collection 0.
    append_wal_entry(
        dir.path(),
        &[Op::Put {
            collection: 5,
            key: b"orphan".to_vec(),
            value: encode_versioned_value(2, &Item { n: 1 }).unwrap(),
        }],
    );

    match Store::<StateV2, WalBackend<StateV2>>::open_wal_strict(dir.path().to_path_buf()) {
        Err(Error::ReplayLoss { .. }) => {}
        Err(other) => panic!("expected ReplayLoss, got {other:?}"),
        Ok(_) => panic!("unknown collection must abort strict load"),
    }
}

#[test]
fn test_strict_aborts_on_discarded_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path()).unwrap();
    // A non-envelope snapshot fails to decode → discarded.
    std::fs::write(dir.path().join("snapshot.postcard"), b"not a snapshot").unwrap();

    match Store::<StateV2, WalBackend<StateV2>>::open_wal_strict(dir.path().to_path_buf()) {
        Err(Error::ReplayLoss { .. }) => {}
        Err(other) => panic!("expected ReplayLoss, got {other:?}"),
        Ok(_) => panic!("discarded snapshot must abort strict load"),
    }
}

// -------------------------------------------------------------------------
// Per-class skip accounting in lenient mode
// -------------------------------------------------------------------------

#[test]
fn test_report_counts_unknown_collection() {
    let dir = tempfile::tempdir().unwrap();
    init_wal(dir.path());
    append_wal_entry(
        dir.path(),
        &[Op::Put {
            collection: 5,
            key: b"orphan".to_vec(),
            value: encode_versioned_value(2, &Item { n: 1 }).unwrap(),
        }],
    );

    let store = Store::<StateV2, WalBackend<StateV2>>::open_wal(dir.path().to_path_buf()).unwrap();
    let report = store.replay_report();
    assert_eq!(report.unknown_collection_skipped(), 1);
    assert_eq!(report.unknown_collections().get(&5), Some(&1));
    assert!(report.has_loss());
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct U64State {
    #[etch(collection = 0, version = 1)]
    items: BTreeMap<u64, u32>,
}

#[test]
fn test_report_counts_key_decode() {
    let dir = tempfile::tempdir().unwrap();
    let _ = Store::<U64State, WalBackend<U64State>>::open_wal(dir.path().to_path_buf()).unwrap();
    // Value decodes fine (u32 @ v1); the 3-byte key is invalid for a u64 key.
    append_wal_entry(
        dir.path(),
        &[Op::Put {
            collection: 0,
            key: vec![1, 2, 3],
            value: encode_versioned_value(1, &7u32).unwrap(),
        }],
    );

    let store =
        Store::<U64State, WalBackend<U64State>>::open_wal(dir.path().to_path_buf()).unwrap();
    let report = store.replay_report();
    assert_eq!(report.key_decode_skipped(), 1);
    assert_eq!(
        report.value_decode_skipped(),
        0,
        "value decoded; only key failed"
    );
    assert!(store.read().items.is_empty());
    assert!(report.has_loss());
}

#[test]
fn test_report_snapshot_discarded_status() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(dir.path().join("snapshot.postcard"), b"not a snapshot").unwrap();

    let store = Store::<StateV2, WalBackend<StateV2>>::open_wal(dir.path().to_path_buf()).unwrap();
    let report = store.replay_report();
    match report.snapshot() {
        SnapshotStatus::Discarded { reason } => {
            // The status carries why the snapshot was rejected. Where the file
            // went is a repair the load performed, so it is a note — that
            // split is what lets `WalBackend::inspect` report an identical
            // status without pretending it moved anything.
            assert!(reason.contains("snapshot envelope"), "got: {reason}");
        }
        other => panic!("expected Discarded, got {other:?}"),
    }
    assert!(
        report
            .notes()
            .iter()
            .any(|n| n.contains("preserved as") && n.contains("snapshot.backup")),
        "load must record where it preserved the snapshot: {:?}",
        report.notes()
    );
    assert!(report.has_loss());
    assert!(
        dir.path().join("snapshot.backup").exists(),
        "discarded snapshot preserved as backup"
    );
}

// -------------------------------------------------------------------------
// Shape-aware fingerprint fires on un-bumped type change
// -------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Widget {
    x: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Gadget {
    x: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct WidgetState {
    #[etch(collection = 0, version = 1)]
    items: BTreeMap<String, Widget>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct GadgetState {
    #[etch(collection = 0, version = 1)]
    items: BTreeMap<String, Gadget>,
}

#[test]
fn test_fingerprint_differs_for_unbumped_type_change() {
    // Same collection id AND same version, different value type — the
    // "forgot to bump the version" shape. The shape-aware fingerprint flips.
    assert_ne!(
        WidgetState::schema_fingerprint(),
        GadgetState::schema_fingerprint(),
        "value-type change must change the fingerprint even at the same version"
    );
}

#[test]
fn test_reopen_with_changed_type_detects_drift_without_loss() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().to_path_buf();

    // Write + compact with WidgetState.
    {
        let store =
            Store::<WidgetState, WalBackend<WidgetState>>::open_wal(state_dir.clone()).unwrap();
        store
            .write(|tx| {
                tx.items.put("k".into(), Widget { x: 7 })?;
                Ok(())
            })
            .unwrap();
        let live = store.read().clone();
        store.backend().snapshot(&live).unwrap();
    }

    // Reopen with GadgetState (msgpack-compatible shape, same version).
    let store = Store::<GadgetState, WalBackend<GadgetState>>::open_wal(state_dir).unwrap();
    let report = store.replay_report();
    match report.schema_drift() {
        SchemaDrift::Detected { has_migrations, .. } => {
            assert!(!has_migrations);
        }
        other => panic!("expected Detected drift, got {other:?}"),
    }
    // Drift is informational: the value decoded cleanly, so there is no loss.
    assert!(!report.has_loss());
    assert_eq!(store.read().items.get("k").map(|g| g.x), Some(7));
}

// -------------------------------------------------------------------------
// Backward compat: an OLD pair-only fingerprint must not spuriously fail
// -------------------------------------------------------------------------

/// Reproduce the pre-0.5.0 pair-only fingerprint algorithm.
fn old_pair_fingerprint(collection: u8, version: u16) -> u64 {
    let mut bytes = vec![collection];
    bytes.extend_from_slice(&version.to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&bytes)
}

#[test]
fn test_old_pair_fingerprint_drifts_but_does_not_fail_strict() {
    let dir = tempfile::tempdir().unwrap();
    // Snapshot with a legacy pair-only fingerprint and a cleanly-decodable
    // value for the current (shape-aware) schema.
    let payload = SnapshotPayload {
        schema_fingerprint: old_pair_fingerprint(0, 1),
        collections: vec![CollectionSection {
            collection_id: 0,
            current_version: 1,
            entries: vec![SnapshotEntry {
                key: b"k".to_vec(),
                version: 1,
                value: encode_msgpack_value(&Gadget { x: 5 }).unwrap(),
            }],
        }],
    };
    write_snapshot(dir.path(), &payload);

    // Strict open MUST succeed: drift is detected but there is no data loss,
    // so an old fingerprint cannot spuriously abort.
    let store =
        Store::<GadgetState, WalBackend<GadgetState>>::open_wal_strict(dir.path().to_path_buf())
            .expect("old pair-only fingerprint must not fail a clean strict load");

    assert_eq!(store.read().items.get("k").map(|g| g.x), Some(5));
    assert!(matches!(
        store.replay_report().schema_drift(),
        SchemaDrift::Detected { .. }
    ));
    assert!(!store.replay_report().has_loss());
}
