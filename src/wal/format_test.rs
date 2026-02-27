//! Tests for the WAL binary format.

use std::io::Write;

use super::format::WalFile;
use super::op::Op;

#[test]
fn write_read_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops1 = vec![Op::Put {
        collection: 0,
        key: "u1".into(),
        value: vec![1, 2, 3],
    }];
    let ops2 = vec![
        Op::Delete {
            collection: 1,
            key: "s1".into(),
        },
        Op::Put {
            collection: 0,
            key: "u2".into(),
            value: vec![4, 5],
        },
    ];

    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.append(&ops1).unwrap();
        wal.append(&ops2).unwrap();
        wal.sync().unwrap();
    }

    let (entries, _valid_offset) = WalFile::iter_entries(&path).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], ops1);
    assert_eq!(entries[1], ops2);
}

#[test]
fn empty_wal_has_no_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.sync().unwrap();
    }

    let (entries, valid_offset) = WalFile::iter_entries(&path).unwrap();
    assert!(entries.is_empty());
    assert_eq!(valid_offset, 16); // Header only.
}

#[test]
fn corruption_detected_via_hash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops1 = vec![Op::Put {
        collection: 0,
        key: "u1".into(),
        value: vec![1, 2, 3],
    }];
    let ops2 = vec![Op::Put {
        collection: 0,
        key: "u2".into(),
        value: vec![4, 5, 6],
    }];

    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.append(&ops1).unwrap();
        wal.append(&ops2).unwrap();
        wal.sync().unwrap();
    }

    // Corrupt a byte in the second entry's payload.
    {
        let mut data = std::fs::read(&path).unwrap();
        // Flip a byte well into the second entry.
        let corrupt_pos = data.len() - 10;
        data[corrupt_pos] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();
    }

    let (entries, _valid_offset) = WalFile::iter_entries(&path).unwrap();
    // First entry should survive, second should be detected as corrupt.
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], ops1);
}

#[test]
fn partial_write_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops = vec![Op::Put {
        collection: 0,
        key: "u1".into(),
        value: vec![1, 2, 3],
    }];

    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.append(&ops).unwrap();
        wal.sync().unwrap();
    }

    // Append a partial entry (just the length, no payload/hash).
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&42u32.to_le_bytes()).unwrap();
        f.sync_all().unwrap();
    }

    let (entries, _valid_offset) = WalFile::iter_entries(&path).unwrap();
    assert_eq!(entries.len(), 1); // Only the complete entry.
    assert_eq!(entries[0], ops);
}

#[test]
fn reset_clears_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops = vec![Op::Put {
        collection: 0,
        key: "u1".into(),
        value: vec![1],
    }];

    let mut wal = WalFile::open(&path).unwrap();
    wal.append(&ops).unwrap();
    wal.sync().unwrap();

    wal.reset().unwrap();

    let (entries, _) = WalFile::iter_entries(&path).unwrap();
    assert!(entries.is_empty());
    assert_eq!(wal.offset(), 16); // Header only.
}

#[test]
fn reopen_appends_at_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops1 = vec![Op::Put {
        collection: 0,
        key: "u1".into(),
        value: vec![1],
    }];
    let ops2 = vec![Op::Put {
        collection: 0,
        key: "u2".into(),
        value: vec![2],
    }];

    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.append(&ops1).unwrap();
        wal.sync().unwrap();
    }

    // Reopen and append.
    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.append(&ops2).unwrap();
        wal.sync().unwrap();
    }

    let (entries, _) = WalFile::iter_entries(&path).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], ops1);
    assert_eq!(entries[1], ops2);
}

#[test]
fn empty_ops_not_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let mut wal = WalFile::open(&path).unwrap();
    wal.append(&[]).unwrap();
    wal.sync().unwrap();

    assert_eq!(wal.offset(), 16); // No entries written.
}
