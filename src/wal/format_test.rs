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
        key: b"u1".to_vec(),
        value: vec![1, 2, 3],
    }];
    let ops2 = vec![
        Op::Delete {
            collection: 1,
            key: b"s1".to_vec(),
        },
        Op::Put {
            collection: 0,
            key: b"u2".to_vec(),
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
        key: b"u1".to_vec(),
        value: vec![1, 2, 3],
    }];
    let ops2 = vec![Op::Put {
        collection: 0,
        key: b"u2".to_vec(),
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
        let corrupt_pos = data.len() - 10;
        data[corrupt_pos] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();
    }

    let (entries, _valid_offset) = WalFile::iter_entries(&path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], ops1);
}

#[test]
fn partial_write_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops = vec![Op::Put {
        collection: 0,
        key: b"u1".to_vec(),
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
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], ops);
}

#[test]
fn reset_clears_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops = vec![Op::Put {
        collection: 0,
        key: b"u1".to_vec(),
        value: vec![1],
    }];

    let mut wal = WalFile::open(&path).unwrap();
    wal.append(&ops).unwrap();
    wal.sync().unwrap();

    wal.reset().unwrap();

    let (entries, _) = WalFile::iter_entries(&path).unwrap();
    assert!(entries.is_empty());
    assert_eq!(wal.offset(), 16);
}

#[test]
fn reopen_appends_at_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops1 = vec![Op::Put {
        collection: 0,
        key: b"u1".to_vec(),
        value: vec![1],
    }];
    let ops2 = vec![Op::Put {
        collection: 0,
        key: b"u2".to_vec(),
        value: vec![2],
    }];

    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.append(&ops1).unwrap();
        wal.sync().unwrap();
    }

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

    assert_eq!(wal.offset(), 16);
}

#[test]
fn bad_magic_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    std::fs::write(
        &path,
        b"BAAD\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
    )
    .unwrap();

    let result = WalFile::iter_entries(&path);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("bad magic"),
        "expected bad magic error, got: {err}"
    );
}

#[test]
fn bad_version_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    std::fs::write(
        &path,
        b"EWAL\x63\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
    )
    .unwrap();

    let result = WalFile::iter_entries(&path);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("unsupported version"),
        "expected version error, got: {err}"
    );
}

#[test]
fn file_too_short_for_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    std::fs::write(&path, b"EWAL").unwrap();

    let result = WalFile::iter_entries(&path);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("too short"),
        "expected too-short error, got: {err}"
    );
}

#[test]
fn truncate_at_removes_trailing_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    let ops1 = vec![Op::Put {
        collection: 0,
        key: b"u1".to_vec(),
        value: vec![1],
    }];
    let ops2 = vec![Op::Put {
        collection: 0,
        key: b"u2".to_vec(),
        value: vec![2],
    }];

    let offset_after_first;
    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.append(&ops1).unwrap();
        wal.sync().unwrap();
        offset_after_first = wal.offset();

        wal.append(&ops2).unwrap();
        wal.sync().unwrap();
    }

    let (entries, _) = WalFile::iter_entries(&path).unwrap();
    assert_eq!(entries.len(), 2);

    WalFile::truncate_at(&path, offset_after_first).unwrap();

    let (entries, _) = WalFile::iter_entries(&path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], ops1);
}

#[test]
fn partial_length_at_end_of_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.sync().unwrap();
    }

    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&[0x01, 0x00]).unwrap();
        f.sync_all().unwrap();
    }

    let (entries, valid_offset) = WalFile::iter_entries(&path).unwrap();
    assert!(entries.is_empty());
    assert_eq!(valid_offset, 16);
}

#[test]
fn valid_hash_but_invalid_postcard() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.bin");

    {
        let mut wal = WalFile::open(&path).unwrap();
        wal.sync().unwrap();
    }

    {
        let payload = b"definitely not valid postcard";
        let len = payload.len() as u32;
        let hash = xxhash_rust::xxh3::xxh3_64(payload);

        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&len.to_le_bytes()).unwrap();
        f.write_all(payload).unwrap();
        f.write_all(&hash.to_le_bytes()).unwrap();
        f.sync_all().unwrap();
    }

    let (entries, valid_offset) = WalFile::iter_entries(&path).unwrap();
    assert!(entries.is_empty());
    assert_eq!(valid_offset, 16);
}
