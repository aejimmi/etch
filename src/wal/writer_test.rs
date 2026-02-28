//! Tests for WalBackend + IncrementalSave.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::diff::Replayable;
use super::op::Op;
use super::writer::{IncrementalSave, WalBackend};
use crate::backend::Backend;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct State {
    items: BTreeMap<String, String>,
}

impl Replayable for State {
    fn apply(&mut self, ops: &[Op]) -> crate::Result<()> {
        for op in ops {
            crate::wal::apply_op(&mut self.items, op)?;
        }
        Ok(())
    }
}

fn put_op(key: &str, value: &str) -> Op {
    Op::Put {
        collection: 0,
        key: key.to_string(),
        value: postcard::to_allocvec(&value.to_string()).unwrap(),
    }
}

fn del_op(key: &str) -> Op {
    Op::Delete {
        collection: 0,
        key: key.to_string(),
    }
}

// -------------------------------------------------------------------------
// WalBackend as Backend<T>
// -------------------------------------------------------------------------

#[test]
fn load_returns_default_on_empty_dir() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(state, State::default());
}

#[test]
fn save_and_load_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();

    let mut state = State::default();
    state.items.insert("a".into(), "alpha".into());
    state.items.insert("b".into(), "beta".into());

    backend.save(&state).unwrap();

    // Reopen and load.
    let backend2 = WalBackend::<State>::open(dir.path()).unwrap();
    let loaded = backend2.load().unwrap();
    assert_eq!(loaded, state);
}

#[test]
fn save_resets_wal() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();

    // Append some ops.
    backend.save_ops(&[put_op("a", "1")]).unwrap();
    backend.sync().unwrap();

    // Full save should reset WAL.
    let state = backend.load().unwrap();
    backend.save(&state).unwrap();

    // WAL should have no entries after save.
    let wal_path = dir.path().join("wal.bin");
    let (entries, _) = super::format::WalFile::iter_entries(&wal_path).unwrap();
    assert!(entries.is_empty(), "WAL should be empty after save");
}

// -------------------------------------------------------------------------
// IncrementalSave
// -------------------------------------------------------------------------

#[test]
fn save_ops_and_replay() {
    let dir = tempfile::tempdir().unwrap();

    // Write ops incrementally.
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend.save_ops(&[put_op("x", "10")]).unwrap();
        backend
            .save_ops(&[put_op("y", "20"), put_op("z", "30")])
            .unwrap();
        backend.sync().unwrap();
    }

    // Reopen — load should replay WAL.
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(state.items.get("x").unwrap(), "10");
    assert_eq!(state.items.get("y").unwrap(), "20");
    assert_eq!(state.items.get("z").unwrap(), "30");
}

#[test]
fn save_ops_empty_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.save_ops(&[]).unwrap();
    backend.sync().unwrap();
    assert!(!backend.should_snapshot());
}

#[test]
fn save_ops_with_deletes() {
    let dir = tempfile::tempdir().unwrap();

    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend
            .save_ops(&[put_op("a", "1"), put_op("b", "2")])
            .unwrap();
        backend.save_ops(&[del_op("a")]).unwrap();
        backend.sync().unwrap();
    }

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert!(!state.items.contains_key("a"));
    assert_eq!(state.items.get("b").unwrap(), "2");
}

// -------------------------------------------------------------------------
// Snapshot threshold
// -------------------------------------------------------------------------

#[test]
fn should_snapshot_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(3);

    assert!(!backend.should_snapshot());

    backend.save_ops(&[put_op("a", "1")]).unwrap();
    assert!(!backend.should_snapshot());

    backend.save_ops(&[put_op("b", "2")]).unwrap();
    assert!(!backend.should_snapshot());

    backend.save_ops(&[put_op("c", "3")]).unwrap();
    backend.sync().unwrap();
    assert!(backend.should_snapshot(), "should snapshot after 3 entries");
}

#[test]
fn snapshot_compacts_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(2);

    backend.save_ops(&[put_op("a", "1")]).unwrap();
    backend.save_ops(&[put_op("b", "2")]).unwrap();
    backend.sync().unwrap();
    assert!(backend.should_snapshot());

    // Snapshot compacts.
    let state = backend.load().unwrap();
    backend.snapshot(&state).unwrap();

    assert!(
        !backend.should_snapshot(),
        "should_snapshot must be false after compaction"
    );

    // WAL should be empty.
    let wal_path = dir.path().join("wal.bin");
    let (entries, _) = super::format::WalFile::iter_entries(&wal_path).unwrap();
    assert!(entries.is_empty());

    // State should survive reload.
    let backend2 = WalBackend::<State>::open(dir.path()).unwrap();
    let reloaded = backend2.load().unwrap();
    assert_eq!(reloaded.items.get("a").unwrap(), "1");
    assert_eq!(reloaded.items.get("b").unwrap(), "2");
}

// -------------------------------------------------------------------------
// Snapshot + WAL combined replay
// -------------------------------------------------------------------------

#[test]
fn snapshot_plus_wal_replay() {
    let dir = tempfile::tempdir().unwrap();

    // Write snapshot with initial data.
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        let mut state = State::default();
        state.items.insert("snap".into(), "data".into());
        backend.save(&state).unwrap();
    }

    // Append WAL ops on top of snapshot.
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend.save_ops(&[put_op("wal", "entry")]).unwrap();
        backend.sync().unwrap();
    }

    // Reload — should have both snapshot and WAL data.
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(state.items.get("snap").unwrap(), "data");
    assert_eq!(state.items.get("wal").unwrap(), "entry");
}

#[test]
fn wal_overrides_snapshot_keys() {
    let dir = tempfile::tempdir().unwrap();

    // Snapshot with key "a" = "old".
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        let mut state = State::default();
        state.items.insert("a".into(), "old".into());
        backend.save(&state).unwrap();
    }

    // WAL updates "a" = "new".
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend.save_ops(&[put_op("a", "new")]).unwrap();
        backend.sync().unwrap();
    }

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(state.items.get("a").unwrap(), "new");
}

// -------------------------------------------------------------------------
// Directory creation
// -------------------------------------------------------------------------

#[test]
fn creates_dir_if_missing() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("a").join("b").join("c");
    let backend = WalBackend::<State>::open(&nested).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(state, State::default());
}

// -------------------------------------------------------------------------
// Entry count tracking across reopens
// -------------------------------------------------------------------------

#[test]
fn entry_count_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let mut backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend.set_snapshot_threshold(5);
        backend.save_ops(&[put_op("a", "1")]).unwrap();
        backend.save_ops(&[put_op("b", "2")]).unwrap();
        backend.sync().unwrap();
    }

    // Reopen — entry count should be restored from WAL file.
    let mut backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(3);

    // 2 existing + 1 new = 3 → should_snapshot.
    backend.save_ops(&[put_op("c", "3")]).unwrap();
    backend.sync().unwrap();
    assert!(backend.should_snapshot());
}

// -------------------------------------------------------------------------
// Edge cases: empty snapshot, corruption recovery
// -------------------------------------------------------------------------

#[test]
fn load_with_empty_snapshot_file() {
    let dir = tempfile::tempdir().unwrap();

    // Create an empty snapshot file (0 bytes).
    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(dir.path().join("snapshot.postcard"), b"").unwrap();

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(state, State::default());
}

#[test]
fn load_truncates_corrupt_wal_tail() {
    let dir = tempfile::tempdir().unwrap();

    // Write valid ops, then corrupt the WAL tail.
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend.save_ops(&[put_op("a", "1")]).unwrap();
        backend.save_ops(&[put_op("b", "2")]).unwrap();
        backend.sync().unwrap();
    }

    // Corrupt the last few bytes of the WAL.
    let wal_path = dir.path().join("wal.bin");
    {
        let mut data = std::fs::read(&wal_path).unwrap();
        let len = data.len();
        data[len - 2] ^= 0xFF;
        data[len - 1] ^= 0xFF;
        std::fs::write(&wal_path, &data).unwrap();
    }

    // Load should recover first entry, truncate corrupt tail.
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(state.items.get("a").unwrap(), "1");
    // Second entry was corrupt — should be gone.
    assert!(!state.items.contains_key("b"));

    // WAL file should have been truncated.
    let file_len = std::fs::metadata(&wal_path).unwrap().len();
    let (entries, valid_offset) = super::format::WalFile::iter_entries(&wal_path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(file_len, valid_offset);
}
