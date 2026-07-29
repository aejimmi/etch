//! Unit tests for `ReplayReport` accounting.

use super::*;
use crate::error::Error;

#[test]
fn test_default_report_is_clean() {
    let r = ReplayReport::default();
    assert!(!r.has_loss());
    assert_eq!(r.total_skipped(), 0);
    assert_eq!(r.applied_ops(), 0);
    assert!(r.strict_error().is_none());
    assert_eq!(r.snapshot(), &SnapshotStatus::Absent);
    assert_eq!(r.schema_drift(), &SchemaDrift::NotChecked);
}

#[test]
fn test_records_each_skip_class() {
    let mut r = ReplayReport::default();
    r.record_value_decode();
    r.record_value_decode();
    r.record_key_decode();
    r.record_unknown_collection(7);
    r.record_unknown_collection(7);
    r.record_unknown_collection(9);
    r.record_entry_decode("bad entry");
    r.record_wal_prev_unreadable("torn prev");

    assert_eq!(r.value_decode_skipped(), 2);
    assert_eq!(r.key_decode_skipped(), 1);
    assert_eq!(r.unknown_collection_skipped(), 3);
    assert_eq!(r.unknown_collections().get(&7), Some(&2));
    assert_eq!(r.unknown_collections().get(&9), Some(&1));
    assert_eq!(r.entry_decode_skipped(), 1);
    assert_eq!(r.wal_prev_unreadable(), 1);
    assert_eq!(r.total_skipped(), 2 + 1 + 3 + 1 + 1);
    assert!(r.has_loss());
}

#[test]
fn test_applied_counts_are_not_loss() {
    let mut r = ReplayReport::default();
    r.record_applied_entry();
    r.record_applied_op();
    r.record_applied_op();
    assert_eq!(r.applied_entries(), 1);
    assert_eq!(r.applied_ops(), 2);
    assert!(!r.has_loss(), "clean applies are not loss");
    assert!(r.strict_error().is_none());
}

#[test]
fn test_quarantine_counts_as_loss() {
    let mut r = ReplayReport::default();
    r.set_quarantine(3, std::collections::BTreeMap::new());
    assert_eq!(r.quarantined(), 3);
    assert!(r.has_loss());
}

#[test]
fn test_snapshot_discard_is_loss_but_absent_and_loaded_are_not() {
    let mut r = ReplayReport::default();
    r.set_snapshot(SnapshotStatus::Loaded);
    assert!(!r.has_loss());
    r.set_snapshot(SnapshotStatus::Discarded {
        reason: "corrupt".into(),
    });
    assert!(r.has_loss());
}

#[test]
fn test_drift_alone_is_not_loss() {
    let mut r = ReplayReport::default();
    r.set_drift(SchemaDrift::Detected {
        stored: 1,
        current: 2,
        has_migrations: false,
    });
    // Drift is informational only — no skips/quarantine means no loss.
    assert!(!r.has_loss());
    assert!(r.strict_error().is_none());
}

#[test]
fn test_strict_error_prefers_schema_version_mismatch() {
    let mut r = ReplayReport::default();
    r.record_value_decode();
    r.note_future_version(5, 1);
    match r.strict_error() {
        Some(Error::SchemaVersionMismatch { stored, current }) => {
            assert_eq!(stored, 5);
            assert_eq!(current, 1);
        }
        other => panic!("expected SchemaVersionMismatch, got {other:?}"),
    }
}

#[test]
fn test_strict_error_falls_back_to_replay_loss() {
    let mut r = ReplayReport::default();
    r.record_key_decode();
    match r.strict_error() {
        Some(Error::ReplayLoss { summary }) => {
            assert!(summary.contains("key-decode"), "got: {summary}");
        }
        other => panic!("expected ReplayLoss, got {other:?}"),
    }
}

#[test]
fn test_absorb_sums_op_counters_but_not_snapshot() {
    let mut top = ReplayReport::default();
    top.set_snapshot(SnapshotStatus::Loaded);

    let mut phase = ReplayReport::default();
    phase.record_applied_entry();
    phase.record_applied_op();
    phase.record_value_decode();
    phase.record_unknown_collection(3);
    phase.set_snapshot(SnapshotStatus::Discarded {
        reason: "ignored".into(),
    });

    top.absorb(&phase);
    assert_eq!(top.applied_entries(), 1);
    assert_eq!(top.applied_ops(), 1);
    assert_eq!(top.value_decode_skipped(), 1);
    assert_eq!(top.unknown_collections().get(&3), Some(&1));
    // Snapshot status is owned by the top-level load, not absorbed.
    assert_eq!(top.snapshot(), &SnapshotStatus::Loaded);
}

#[test]
fn test_notes_are_bounded() {
    let mut r = ReplayReport::default();
    for i in 0..(MAX_NOTES + 50) {
        r.push_note(format!("note {i}"));
    }
    assert_eq!(r.notes().len(), MAX_NOTES + 1, "capped plus elision marker");
    assert!(r.notes().last().unwrap().contains("elided"));
}

#[test]
fn test_summary_mentions_active_skip_classes() {
    let mut r = ReplayReport::default();
    r.record_applied_entry();
    r.set_quarantine(2, std::collections::BTreeMap::new());
    r.record_value_decode();
    let s = r.summary();
    assert!(s.contains("2 quarantined"), "got: {s}");
    assert!(s.contains("value-decode"), "got: {s}");
}

// -------------------------------------------------------------------------
// Serialization — a report must be able to cross a process boundary
// -------------------------------------------------------------------------

/// Every public counter survives a JSON round-trip, so a health check that
/// inspects a store in one process can hand its verdict to another without a
/// hand-rolled mirror type.
#[test]
fn test_report_round_trips_through_json() {
    let mut r = ReplayReport::default();
    r.record_applied_entry();
    r.record_applied_op();
    r.record_applied_op();
    r.record_value_decode();
    r.record_key_decode();
    r.record_unknown_collection(7);
    r.record_entry_decode("bad entry");
    r.record_wal_prev_unreadable("torn prev");
    r.set_snapshot(SnapshotStatus::Discarded {
        reason: "bad envelope".into(),
    });
    r.set_drift(SchemaDrift::Detected {
        stored: 11,
        current: 12,
        has_migrations: true,
    });
    r.set_quarantine(
        2,
        std::collections::BTreeMap::from([("MissingMigration collection 0 1->2".to_string(), 2)]),
    );

    let json = serde_json::to_string(&r).expect("report must serialize");
    let back: ReplayReport = serde_json::from_str(&json).expect("report must deserialize");

    assert_eq!(back, r, "round-trip changed the report");
    assert_eq!(back.applied_entries(), 1);
    assert_eq!(back.applied_ops(), 2);
    assert_eq!(back.value_decode_skipped(), 1);
    assert_eq!(back.key_decode_skipped(), 1);
    assert_eq!(back.unknown_collection_skipped(), 1);
    assert_eq!(back.unknown_collections().get(&7), Some(&1));
    assert_eq!(back.entry_decode_skipped(), 1);
    assert_eq!(back.wal_prev_unreadable(), 1);
    assert_eq!(back.quarantined(), 2);
    assert_eq!(back.quarantine_by_reason().len(), 1);
    assert_eq!(back.notes().len(), r.notes().len());
    assert!(back.has_loss());
    assert_eq!(back.summary(), r.summary());
}

/// Every public counter is present as a field in the serialized output — a
/// consumer reading the JSON directly sees the whole account.
#[test]
fn test_report_json_carries_every_counter() {
    let value = serde_json::to_value(ReplayReport::default()).unwrap();
    let object = value.as_object().expect("report serializes as an object");
    for field in [
        "applied_entries",
        "applied_ops",
        "value_decode_skipped",
        "key_decode_skipped",
        "entry_decode_skipped",
        "wal_prev_unreadable",
        "unknown_collections",
        "quarantined",
        "quarantine_by_reason",
        "snapshot",
        "drift",
        "notes",
    ] {
        assert!(
            object.contains_key(field),
            "missing field {field} in {value}"
        );
    }
}

/// The nested verdict enums serialize on their own too — a consumer may want
/// only the snapshot status.
#[test]
fn test_snapshot_status_and_drift_serialize() {
    let status = SnapshotStatus::Discarded {
        reason: "torn".into(),
    };
    let back: SnapshotStatus =
        serde_json::from_str(&serde_json::to_string(&status).unwrap()).unwrap();
    assert_eq!(back, status);

    let drift = SchemaDrift::Detected {
        stored: 1,
        current: 2,
        has_migrations: false,
    };
    let back: SchemaDrift = serde_json::from_str(&serde_json::to_string(&drift).unwrap()).unwrap();
    assert_eq!(back, drift);
}
