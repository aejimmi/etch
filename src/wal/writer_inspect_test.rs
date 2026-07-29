//! `WalBackend::inspect` (R3) — a load that reports and writes nothing.
//!
//! The central invariant is byte-identity: hash every file in a fixture
//! directory, inspect it, hash again. Anything `inspect` writes shows up as a
//! changed hash or a new entry, including files a load would create as a side
//! effect (`.lock`, `wal.bin`, `snapshot.backup`).

use super::*;

use std::collections::BTreeMap as Map;
use std::path::{Path, PathBuf};

use crate::wal::report::{ReplayReport, SnapshotStatus};
use crate::wal::{Quarantine, QuarantineReason, QuarantinedEntry};

/// Content hash of every file in a directory, keyed by file name.
fn dir_digest(dir: &Path) -> Map<String, u64> {
    let mut out = Map::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_type().unwrap().is_file() {
            continue;
        }
        let bytes = std::fs::read(entry.path()).unwrap();
        out.insert(
            entry.file_name().to_string_lossy().into_owned(),
            xxhash_rust::xxh3::xxh3_64(&bytes),
        );
    }
    out
}

/// Copy a fixture directory so a mutating load can be compared against a
/// non-mutating inspect of the same bytes.
fn clone_dir(src: &Path) -> tempfile::TempDir {
    let dst = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), dst.path().join(entry.file_name())).unwrap();
        }
    }
    dst
}

/// The counters a load and an inspect must agree on, as a comparable tuple.
fn counters(r: &ReplayReport) -> (u64, u64, u64, u64, u64, u64, u64, u64) {
    (
        r.applied_entries(),
        r.applied_ops(),
        r.value_decode_skipped(),
        r.key_decode_skipped(),
        r.unknown_collection_skipped(),
        r.entry_decode_skipped(),
        r.wal_prev_unreadable(),
        r.quarantined(),
    )
}

/// Snapshot-status verdict without its free-form reason string.
fn verdict(r: &ReplayReport) -> &'static str {
    match r.snapshot() {
        SnapshotStatus::Absent => "absent",
        SnapshotStatus::Loaded => "loaded",
        SnapshotStatus::Discarded { .. } => "discarded",
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A clean store with a snapshot, a `wal.prev`, and live WAL entries.
fn fixture_clean() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(u64::MAX);

    let mut state = State::default();
    for key in ["a", "b"] {
        state.items.insert(key.into(), key.into());
        backend.save_ops(&[put_op(key, key)]).unwrap();
    }
    backend.sync().unwrap();
    backend.snapshot(&state).unwrap(); // creates wal.prev

    backend.save_ops(&[put_op("c", "c")]).unwrap();
    backend.sync().unwrap();
    drop(backend);
    std::fs::remove_file(dir.path().join(".lock")).unwrap();
    dir
}

/// A store whose live WAL has a torn tail.
fn fixture_torn_tail() -> tempfile::TempDir {
    let dir = fixture_clean();
    let wal = dir.path().join("wal.bin");
    let mut bytes = std::fs::read(&wal).unwrap();
    let len = bytes.len();
    bytes[len - 1] ^= 0xFF; // break the trailing hash
    std::fs::write(&wal, &bytes).unwrap();
    dir
}

/// A store whose snapshot cannot be decoded.
fn fixture_bad_snapshot() -> tempfile::TempDir {
    let dir = fixture_clean();
    std::fs::write(dir.path().join("snapshot.postcard"), b"not a snapshot").unwrap();
    dir
}

/// A store with a persisted, unrecoverable quarantine.
fn fixture_quarantined() -> tempfile::TempDir {
    let dir = fixture_clean();
    let mut q = Quarantine::new();
    q.insert(QuarantinedEntry {
        collection: 0,
        key: b"stuck".to_vec(),
        version: 1,
        value: vec![7, 7, 7],
        reason: QuarantineReason::MissingMigration { from: 1, to: 2 },
    });
    q.save(dir.path()).unwrap();
    dir
}

fn fixtures() -> Vec<(&'static str, tempfile::TempDir)> {
    vec![
        ("clean", fixture_clean()),
        ("torn-tail", fixture_torn_tail()),
        ("bad-snapshot", fixture_bad_snapshot()),
        ("quarantined", fixture_quarantined()),
    ]
}

// ---------------------------------------------------------------------------
// Byte identity
// ---------------------------------------------------------------------------

/// Every fixture is byte-identical before and after `inspect`, and no new
/// file appears — no `.lock`, no `snapshot.backup`, no created `wal.bin`.
#[test]
fn test_inspect_leaves_directory_byte_identical() {
    for (name, dir) in fixtures() {
        let before = dir_digest(dir.path());
        let _ = WalBackend::<State>::inspect(dir.path()).unwrap();
        let after = dir_digest(dir.path());

        assert_eq!(
            before, after,
            "fixture {name}: inspect mutated the directory"
        );
        assert!(
            !dir.path().join(".lock").exists(),
            "fixture {name}: inspect created a lock file"
        );
        assert!(
            !dir.path().join("snapshot.backup").exists(),
            "fixture {name}: inspect renamed the snapshot"
        );
        assert!(
            dir.path().join("wal.prev").exists(),
            "fixture {name}: inspect deleted wal.prev"
        );
    }
}

/// A directory holding only a snapshot must not gain a `wal.bin`.
#[test]
fn test_inspect_does_not_create_missing_wal() {
    let dir = fixture_clean();
    std::fs::remove_file(dir.path().join("wal.bin")).unwrap();
    std::fs::remove_file(dir.path().join("wal.prev")).unwrap();

    let report = WalBackend::<State>::inspect(dir.path()).unwrap();
    assert_eq!(*report.snapshot(), SnapshotStatus::Loaded);
    assert!(
        !dir.path().join("wal.bin").exists(),
        "inspect created the WAL file"
    );
}

/// Running `inspect` twice yields identical reports — there is no state
/// carried between runs because nothing is written.
#[test]
fn test_inspect_is_repeatable() {
    for (name, dir) in fixtures() {
        let first = WalBackend::<State>::inspect(dir.path()).unwrap();
        let second = WalBackend::<State>::inspect(dir.path()).unwrap();
        assert_eq!(first, second, "fixture {name}: inspect was not repeatable");
    }
}

// ---------------------------------------------------------------------------
// Same verdict as a real load
// ---------------------------------------------------------------------------

/// `inspect` and `load_with_report` reach the same verdict on every fixture —
/// same counters, same snapshot status — with different side effects.
#[test]
fn test_inspect_matches_load_counts() {
    for (name, dir) in fixtures() {
        let inspected = WalBackend::<State>::inspect(dir.path()).unwrap();

        // Load a copy: a load mutates, so it cannot run on the fixture that
        // the byte-identity assertions rely on.
        let copy = clone_dir(dir.path());
        let backend = WalBackend::<State>::open(copy.path()).unwrap();
        let (_state, loaded) = backend.load_with_report().unwrap();

        assert_eq!(
            counters(&inspected),
            counters(&loaded),
            "fixture {name}: inspect and load disagree ({} vs {})",
            inspected.summary(),
            loaded.summary()
        );
        assert_eq!(
            verdict(&inspected),
            verdict(&loaded),
            "fixture {name}: snapshot verdict differs"
        );
        assert_eq!(
            inspected.has_loss(),
            loaded.has_loss(),
            "fixture {name}: loss verdict differs"
        );
    }
}

/// A load repairs; an inspect says the repair *would* happen.
#[test]
fn test_inspect_reports_deferred_repairs() {
    let torn = fixture_torn_tail();
    let report = WalBackend::<State>::inspect(torn.path()).unwrap();
    assert!(
        report
            .notes()
            .iter()
            .any(|n| n.contains("would be truncated")),
        "torn tail not reported: {:?}",
        report.notes()
    );

    let bad = fixture_bad_snapshot();
    let report = WalBackend::<State>::inspect(bad.path()).unwrap();
    assert!(
        matches!(report.snapshot(), SnapshotStatus::Discarded { .. }),
        "undecodable snapshot must be reported as discarded"
    );
    assert!(
        report
            .notes()
            .iter()
            .any(|n| n.contains("would be preserved as")),
        "snapshot preservation not reported: {:?}",
        report.notes()
    );
    assert!(
        !bad.path().join("snapshot.backup").exists(),
        "inspect must not perform the rename it describes"
    );
}

/// A persisted quarantine is a state, not a load failure: `inspect` reports
/// the count and returns `Ok`, where a strict open would fail forever.
#[test]
fn test_inspect_reports_quarantine_without_error() {
    let dir = fixture_quarantined();
    let report = WalBackend::<State>::inspect(dir.path()).unwrap();

    assert_eq!(report.quarantined(), 1);
    assert!(report.has_loss(), "a persisted quarantine counts as loss");
    assert_eq!(
        report.quarantine_by_reason().values().copied().sum::<u64>(),
        1
    );

    // The same directory refuses a strict open — that is exactly why inspect
    // exists as a separate verification surface.
    let copy = clone_dir(dir.path());
    let backend = WalBackend::<State>::open(copy.path()).unwrap();
    assert!(
        backend.load_strict().is_err(),
        "strict open of a quarantined store is expected to fail"
    );
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

/// A missing directory is an IO error, not a silent empty report.
#[test]
fn test_inspect_missing_directory_errors() {
    let dir = tempfile::tempdir().unwrap();
    let missing: PathBuf = dir.path().join("nope");
    let err = WalBackend::<State>::inspect(&missing).unwrap_err();
    assert!(
        matches!(err, crate::Error::Io(_)),
        "expected an IO error, got {err:?}"
    );
}

/// An empty directory reports an absent snapshot and a clean load.
#[test]
fn test_inspect_empty_directory_reports_absent() {
    let dir = tempfile::tempdir().unwrap();
    let report = WalBackend::<State>::inspect(dir.path()).unwrap();
    assert_eq!(*report.snapshot(), SnapshotStatus::Absent);
    assert!(!report.has_loss());
    assert_eq!(report.applied_entries(), 0);
    assert!(dir_digest(dir.path()).is_empty(), "inspect created files");
}

/// A WAL header the build cannot read aborts an inspect the same way it
/// aborts an open.
#[test]
fn test_inspect_rejects_unreadable_wal_header() {
    let dir = tempfile::tempdir().unwrap();
    let mut header = Vec::new();
    header.extend_from_slice(b"EWAL");
    header.push(99); // future version
    header.extend_from_slice(&[0u8; 3]);
    header.extend_from_slice(&0u64.to_le_bytes());
    std::fs::write(dir.path().join("wal.bin"), &header).unwrap();

    let err = WalBackend::<State>::inspect(dir.path()).unwrap_err();
    assert!(
        matches!(err, crate::Error::WalCorrupted { .. }),
        "expected WalCorrupted, got {err:?}"
    );
}

/// An unreadable `quarantine.bin` is a note, not a failure — mirroring
/// `WalBackend::open`.
#[test]
fn test_inspect_notes_unreadable_quarantine() {
    let dir = fixture_clean();
    std::fs::write(dir.path().join("quarantine.bin"), b"junk").unwrap();

    let report = WalBackend::<State>::inspect(dir.path()).unwrap();
    assert!(
        report
            .notes()
            .iter()
            .any(|n| n.contains("quarantine file unreadable")),
        "unreadable quarantine not noted: {:?}",
        report.notes()
    );
}
