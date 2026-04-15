//! Tests for the quarantine store.

use super::*;
use tempfile::TempDir;

fn entry(collection: u8, key: &[u8], version: u16, value: &[u8]) -> QuarantinedEntry {
    QuarantinedEntry {
        collection,
        key: key.to_vec(),
        version,
        value: value.to_vec(),
        reason: QuarantineReason::MissingMigration {
            from: version,
            to: version + 1,
        },
    }
}

#[test]
fn empty_quarantine_loads_empty_when_file_absent() {
    let dir = TempDir::new().unwrap();
    let q = Quarantine::load(dir.path()).unwrap();
    assert!(q.is_empty());
}

#[test]
fn save_and_load_roundtrip() {
    let dir = TempDir::new().unwrap();
    let mut q = Quarantine::new();
    q.insert(entry(3, b"acme", 0, &[1, 2, 3]));
    q.insert(entry(3, b"beta", 1, &[4, 5]));
    q.save(dir.path()).unwrap();

    let loaded = Quarantine::load(dir.path()).unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded.entries()[0].key, b"acme");
    assert_eq!(loaded.entries()[1].key, b"beta");
}

#[test]
fn insert_dedupes_by_collection_and_key() {
    let mut q = Quarantine::new();
    q.insert(entry(3, b"acme", 0, &[1]));
    q.insert(entry(3, b"acme", 1, &[2, 3])); // same (collection, key), newer
    assert_eq!(q.len(), 1);
    assert_eq!(q.entries()[0].version, 1);
    assert_eq!(q.entries()[0].value, &[2, 3]);
}

#[test]
fn different_collections_coexist() {
    let mut q = Quarantine::new();
    q.insert(entry(3, b"x", 0, &[1]));
    q.insert(entry(5, b"x", 0, &[2]));
    assert_eq!(q.len(), 2);
}

#[test]
fn remove_key_deletes_single_entry() {
    let mut q = Quarantine::new();
    q.insert(entry(3, b"a", 0, &[]));
    q.insert(entry(3, b"b", 0, &[]));
    assert!(q.remove_key(3, b"a"));
    assert_eq!(q.len(), 1);
    assert_eq!(q.entries()[0].key, b"b");
    assert!(!q.remove_key(3, b"a")); // already gone
}

#[test]
fn remove_where_uses_predicate() {
    let mut q = Quarantine::new();
    q.insert(entry(3, b"a", 0, &[]));
    q.insert(entry(3, b"b", 1, &[]));
    q.insert(entry(3, b"c", 2, &[]));
    let removed = q.remove_where(|e| e.version >= 1);
    assert_eq!(removed, 2);
    assert_eq!(q.len(), 1);
    assert_eq!(q.entries()[0].key, b"a");
}

#[test]
fn save_empty_removes_existing_file() {
    let dir = TempDir::new().unwrap();
    let mut q = Quarantine::new();
    q.insert(entry(3, b"x", 0, &[1]));
    q.save(dir.path()).unwrap();
    assert!(dir.path().join(QUARANTINE_FILE).exists());

    q.clear();
    q.save(dir.path()).unwrap();
    assert!(!dir.path().join(QUARANTINE_FILE).exists());
}

#[test]
fn load_rejects_bad_magic() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join(QUARANTINE_FILE);
    std::fs::write(&path, b"XXXX\x01payload").unwrap();

    match Quarantine::load(dir.path()) {
        Err(crate::Error::QuarantineCorrupted { reason }) => {
            assert!(reason.contains("bad magic"), "got: {reason}");
        }
        other => panic!("expected QuarantineCorrupted, got {:?}", other),
    }
}

#[test]
fn load_rejects_unsupported_version() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join(QUARANTINE_FILE);
    std::fs::write(&path, b"EQUA\x99payload").unwrap();

    match Quarantine::load(dir.path()) {
        Err(crate::Error::QuarantineCorrupted { reason }) => {
            assert!(reason.contains("unsupported version"), "got: {reason}");
        }
        other => panic!("expected QuarantineCorrupted, got {:?}", other),
    }
}

#[test]
fn save_survives_crash_between_tmp_and_rename() {
    // We can't actually simulate a crash here, but we can verify:
    // 1. A .tmp file doesn't leak after a successful save.
    // 2. The final file exists and has the right contents.
    let dir = TempDir::new().unwrap();
    let mut q = Quarantine::new();
    q.insert(entry(3, b"x", 0, &[1]));
    q.save(dir.path()).unwrap();

    assert!(dir.path().join(QUARANTINE_FILE).exists());
    assert!(!dir.path().join("quarantine.tmp").exists());
}

#[test]
fn quarantine_reason_display() {
    let r = QuarantineReason::MissingMigration { from: 1, to: 2 };
    assert_eq!(format!("{r}"), "no migration registered for 1->2");

    let r = QuarantineReason::MigrationFailed {
        from: 2,
        to: 3,
        reason: "bad field".into(),
    };
    assert_eq!(format!("{r}"), "migration 2->3 failed: bad field");
}
