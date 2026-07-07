use super::*;

// -------------------------------------------------------------------------
// Snapshot envelope format
// -------------------------------------------------------------------------

#[test]
fn snapshot_has_magic_and_version() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();

    let mut state = State::default();
    state.items.insert("a".into(), "alpha".into());
    backend.save(&state).unwrap();

    let bytes = std::fs::read(dir.path().join("snapshot.postcard")).unwrap();
    assert!(bytes.len() >= 5, "snapshot too short");
    assert_eq!(&bytes[..4], b"ESNA", "missing snapshot magic");

    #[cfg(feature = "compression")]
    assert_eq!(bytes[4], 2, "expected zstd snapshot version");
    #[cfg(not(feature = "compression"))]
    assert_eq!(bytes[4], 1, "expected raw snapshot version");
}

#[test]
fn snapshot_roundtrip_with_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let state = {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();

        let mut state = State::default();
        state.items.insert("x".into(), "10".into());
        state.items.insert("y".into(), "20".into());
        backend.save(&state).unwrap();
        state
    };

    let backend2 = WalBackend::<State>::open(dir.path()).unwrap();
    let loaded = backend2.load().unwrap();
    assert_eq!(loaded, state);
}

#[test]
fn snapshot_version_mismatch_returns_error() {
    let dir = tempfile::tempdir().unwrap();

    // Write a snapshot with version 99.
    let state = State::default();
    let payload = postcard::to_allocvec(&state).unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"ESNA");
    bytes.push(99); // bad version
    bytes.extend_from_slice(&payload);

    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(dir.path().join("snapshot.postcard"), &bytes).unwrap();

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    // New behavior: unreadable snapshots are preserved as .backup and load
    // returns default state rather than erroring. This preserves user data
    // that would otherwise be destroyed by the next compaction.
    let loaded = backend.load().unwrap();
    assert!(loaded.items.is_empty(), "fell back to default state");
    assert!(
        dir.path().join("snapshot.backup").exists(),
        "original snapshot preserved as .backup"
    );
}

/// A zstd-compressed snapshot (version 2) without the compression feature
/// is preserved as .backup and falls back to default state.
#[cfg(not(feature = "compression"))]
#[test]
fn zstd_snapshot_without_feature_returns_error() {
    let dir = tempfile::tempdir().unwrap();

    let state = State::default();
    let payload = postcard::to_allocvec(&state).unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"ESNA");
    bytes.push(2); // zstd version
    bytes.extend_from_slice(&payload);

    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(dir.path().join("snapshot.postcard"), &bytes).unwrap();

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let loaded = backend.load().unwrap();
    assert!(loaded.items.is_empty());
    assert!(dir.path().join("snapshot.backup").exists());
}

/// Raw v1 snapshots are always readable regardless of compression feature.
#[test]
fn raw_v1_snapshot_readable_with_any_feature() {
    let dir = tempfile::tempdir().unwrap();

    let mut state = State::default();
    state.items.insert("raw".into(), "value".into());
    let payload = postcard::to_allocvec(&state).unwrap();

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"ESNA");
    bytes.push(1); // v1 raw
    bytes.extend_from_slice(&payload);

    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(dir.path().join("snapshot.postcard"), &bytes).unwrap();

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let loaded = backend.load().unwrap();
    assert_eq!(loaded.items.get("raw").unwrap(), "value");
}

/// Snapshot file without ESNA magic header returns error.
#[test]
fn snapshot_missing_envelope_returns_error() {
    let dir = tempfile::tempdir().unwrap();

    // Write raw postcard bytes (no ESNA header) as snapshot.
    let state = State::default();
    let raw = postcard::to_allocvec(&state).unwrap();
    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(dir.path().join("snapshot.postcard"), &raw).unwrap();

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    // New behavior: missing envelope is treated as corrupt snapshot —
    // preserved as .backup, load returns default state.
    let loaded = backend.load().unwrap();
    assert!(loaded.items.is_empty());
    assert!(dir.path().join("snapshot.backup").exists());
}

/// When compression is enabled, snapshot write+read roundtrips through zstd.
#[cfg(feature = "compression")]
#[test]
fn compressed_snapshot_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();

    let mut state = State::default();
    for i in 0..100 {
        state.items.insert(format!("key_{i}"), format!("value_{i}"));
    }
    backend.save(&state).unwrap();

    // Verify it wrote version 2.
    let bytes = std::fs::read(dir.path().join("snapshot.postcard")).unwrap();
    assert_eq!(bytes[4], 2);

    // Release the exclusive `.lock` before reopening the same directory.
    drop(backend);

    // Verify roundtrip.
    let backend2 = WalBackend::<State>::open(dir.path()).unwrap();
    let loaded = backend2.load().unwrap();
    assert_eq!(loaded, state);
}

/// Compressed snapshots should be smaller than raw for repetitive data.
#[cfg(feature = "compression")]
#[test]
fn compressed_snapshot_is_smaller() {
    let dir_raw = tempfile::tempdir().unwrap();
    let dir_compressed = tempfile::tempdir().unwrap();

    let mut state = State::default();
    for i in 0..500 {
        state
            .items
            .insert(format!("document_{i}"), format!("content_value_{i}"));
    }

    // Write raw v1 snapshot manually.
    let payload = postcard::to_allocvec(&state).unwrap();
    let mut raw_bytes = Vec::new();
    raw_bytes.extend_from_slice(b"ESNA");
    raw_bytes.push(1);
    raw_bytes.extend_from_slice(&payload);
    std::fs::write(dir_raw.path().join("snapshot.postcard"), &raw_bytes).unwrap();

    // Write compressed snapshot via backend.
    let backend = WalBackend::<State>::open(dir_compressed.path()).unwrap();
    backend.save(&state).unwrap();

    let raw_size = std::fs::metadata(dir_raw.path().join("snapshot.postcard"))
        .unwrap()
        .len();
    let compressed_size = std::fs::metadata(dir_compressed.path().join("snapshot.postcard"))
        .unwrap()
        .len();

    assert!(
        compressed_size < raw_size,
        "compressed ({compressed_size}) should be smaller than raw ({raw_size})"
    );
}
