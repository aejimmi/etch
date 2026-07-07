//! Durability / crash-safety tests for the WAL backend.
//!
//! Two families of tests live here:
//!
//! 1. A deterministic regression for the stale-write-offset bug: a load-time
//!    truncation must reposition the live writer so a subsequent append does
//!    not leave a sparse hole (which the next boot would read as corruption).
//!
//! 2. A crash-injection harness: a child process writes acknowledged
//!    (fsync'd) entries and is aborted mid-compaction at deterministic points
//!    via the `ETCHDB_CRASH_POINT` hook. The parent reopens the store and
//!    asserts that no acknowledged write was lost — specifically covering the
//!    compaction window between `wal.prev` creation and the snapshot rename.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::backend::Backend;
use crate::wal::{IncrementalSave, Op, ReplayFormat, Replayable, WalBackend, apply_op};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct State {
    items: BTreeMap<String, String>,
}

impl Replayable for State {
    fn apply_with_format(&mut self, ops: &[Op], _format: ReplayFormat) -> crate::Result<()> {
        for op in ops {
            apply_op(&mut self.items, op)?;
        }
        Ok(())
    }
}

fn put_op(key: &str, value: &str) -> Op {
    Op::Put {
        collection: 0,
        key: key.as_bytes().to_vec(),
        value: postcard::to_allocvec(&value.to_string()).unwrap(),
    }
}

// -------------------------------------------------------------------------
// Regression: stale write offset after load-time truncation (window 2)
// -------------------------------------------------------------------------

/// A torn WAL tail is truncated at load; a NEW acknowledged append on the
/// same backend must land at the truncated end (not past a sparse hole) and
/// survive the next reopen.
#[test]
fn test_append_after_load_truncation_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    // Two acknowledged ops, then corrupt the second entry's tail.
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend.save_ops(&[put_op("a", "1")]).unwrap();
        backend.save_ops(&[put_op("b", "2")]).unwrap();
        backend.sync().unwrap();
    }
    let wal_path = dir.path().join("wal.bin");
    {
        let mut data = std::fs::read(&wal_path).unwrap();
        let len = data.len();
        data[len - 1] ^= 0xFF; // corrupt the last entry's hash
        std::fs::write(&wal_path, &data).unwrap();
    }

    // Open + load truncates the torn tail. Then append a NEW op through the
    // same live backend and fsync it (acknowledged).
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        let state = backend.load().unwrap();
        assert_eq!(state.items.get("a").map(String::as_str), Some("1"));
        assert!(!state.items.contains_key("b"), "torn entry must be dropped");

        backend.save_ops(&[put_op("c", "3")]).unwrap();
        backend.sync().unwrap();
    }

    // Reopen: the post-truncation append must survive — no sparse hole.
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(state.items.get("a").map(String::as_str), Some("1"));
    assert_eq!(
        state.items.get("c").map(String::as_str),
        Some("3"),
        "acknowledged write after load-time truncation was lost"
    );
}

// -------------------------------------------------------------------------
// Crash-injection harness (window 1: compaction crash window)
// -------------------------------------------------------------------------

/// Number of acknowledged (fsync'd) writes each child performs.
const ACK_COUNT: usize = 24;

/// Deterministic crash points inside `write_snapshot`/`rotate_to_backup`,
/// straddling the compaction window between wal.prev creation and the
/// snapshot rename.
const CRASH_POINTS: &[&str] = &[
    "post_wal_prev_dir_fsync",   // wal.prev durable, before live WAL reset
    "post_reset_pre_rename",     // live WAL reset, before snapshot rename (bug #1)
    "post_rename_pre_dir_fsync", // snapshot renamed, before final dir fsync
];

/// libtest filter name for [`crash_child_worker`], derived from the current
/// module path (crate prefix stripped, which the filter does not want).
fn child_test_name() -> String {
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    format!("{module}::crash_child_worker")
}

/// Child body: write `ACK_COUNT` acknowledged ops, then trigger a compaction
/// that aborts mid-way at the requested crash point.
///
/// A no-op during a normal `cargo test` run — only the parent, which sets the
/// `ETCHDB_CRASH_DIR` / `ETCHDB_CRASH_POINT` env vars, drives the crash path.
#[test]
fn crash_child_worker() {
    let dir = match (
        std::env::var("ETCHDB_CRASH_DIR"),
        std::env::var("ETCHDB_CRASH_POINT"),
    ) {
        (Ok(d), Ok(_)) => d,
        _ => return,
    };

    let backend = WalBackend::<State>::open(&dir).unwrap();
    // Never auto-compact from save_ops; we drive the single compaction below.
    backend.set_snapshot_threshold(u64::MAX);

    let mut state = State::default();
    for i in 0..ACK_COUNT {
        let key = format!("k{i}");
        let value = format!("v{i}");
        state.items.insert(key.clone(), value.clone());
        backend.save_ops(&[put_op(&key, &value)]).unwrap();
    }
    // Acknowledge: flush + fsync. These writes MUST survive the crash.
    backend.sync().unwrap();

    // Trigger the compaction. The crash point aborts the process here without
    // returning; reaching the line below means the point never fired.
    backend.snapshot(&state).unwrap();

    // Clean exit signals "did not crash" — the parent asserts a crash.
    std::process::exit(0);
}

/// Spawn a child per crash point, let it die mid-compaction, then reopen the
/// store and assert every acknowledged write survived.
#[test]
fn test_crash_during_compaction_preserves_acked_writes() {
    let child = child_test_name();
    let exe = std::env::current_exe().unwrap();

    for point in CRASH_POINTS {
        let dir = tempfile::tempdir().unwrap();

        let status = std::process::Command::new(&exe)
            .arg("--exact")
            .arg(&child)
            .env("ETCHDB_CRASH_DIR", dir.path())
            .env("ETCHDB_CRASH_POINT", point)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();

        assert!(
            !status.success(),
            "child was expected to crash at {point} but exited cleanly"
        );

        // Reopen and verify durability of every acknowledged write.
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        let state = backend.load().unwrap();
        for i in 0..ACK_COUNT {
            let key = format!("k{i}");
            assert_eq!(
                state.items.get(&key).map(String::as_str),
                Some(format!("v{i}").as_str()),
                "key {key} lost after crash at {point}"
            );
        }
    }
}
