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

/// Deterministic crash points inside `write_snapshot`/`rotate_to_backup`, one
/// per phase of a compaction. A compaction commits the snapshot first and
/// rotates the WAL second, so the windows are:
///
/// 1. temp snapshot fsync'd, not yet renamed — old snapshot + full WAL;
/// 2. renamed, directory not yet fsync'd;
/// 3. snapshot committed, WAL not yet rotated — new snapshot + full WAL;
/// 4. wal.prev durable, live WAL not yet reset — both copies present;
/// 5. live WAL reset — new snapshot + wal.prev.
const CRASH_POINTS: &[&str] = &[
    "post_snapshot_tmp_fsync",         // (1) temp durable, before rename
    "post_rename_pre_dir_fsync",       // (2) renamed, before dir fsync
    "post_snapshot_commit_pre_rotate", // (3) snapshot committed, before rotate
    "post_wal_prev_dir_fsync",         // (4) wal.prev durable, before WAL reset
    "post_wal_reset",                  // (5) live WAL reset
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

/// Spawn a child at the given crash point and assert it aborted.
fn spawn_crashing_child(dir: &std::path::Path, point: &str) {
    let exe = std::env::current_exe().unwrap();
    let status = std::process::Command::new(&exe)
        .arg("--exact")
        .arg(child_test_name())
        .env("ETCHDB_CRASH_DIR", dir)
        .env("ETCHDB_CRASH_POINT", point)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "child was expected to crash at {point} but exited cleanly"
    );
}

/// Assert every acknowledged write is readable from `dir`.
fn assert_all_acked_present(dir: &std::path::Path, context: &str) {
    let backend = WalBackend::<State>::open(dir).unwrap();
    let state = backend.load().unwrap();
    for i in 0..ACK_COUNT {
        let key = format!("k{i}");
        assert_eq!(
            state.items.get(&key).map(String::as_str),
            Some(format!("v{i}").as_str()),
            "key {key} lost {context}"
        );
    }
}

/// Spawn a child per crash point, let it die mid-compaction, then reopen the
/// store and assert every acknowledged write survived.
#[test]
fn test_crash_during_compaction_preserves_acked_writes() {
    for point in CRASH_POINTS {
        let dir = tempfile::tempdir().unwrap();
        spawn_crashing_child(dir.path(), point);
        assert_all_acked_present(dir.path(), &format!("after crash at {point}"));
    }
}

/// Crash mid-compaction, boot, then die immediately after the boot completes
/// and boot again. Every op acknowledged before the *first* crash must still
/// be there.
///
/// This is the two-crash shape of P3: recovery used to happen only in memory
/// (the boot replayed `wal.prev` and then deleted it), so the durable state
/// after the first boot was the pre-compaction snapshot plus an empty WAL and
/// the second kill lost everything the backup held. A backup is now superseded
/// only by a committed snapshot, so the second boot recovers identically.
#[test]
fn test_two_crashes_around_a_boot_preserve_acked_writes() {
    for point in CRASH_POINTS {
        let dir = tempfile::tempdir().unwrap();
        spawn_crashing_child(dir.path(), point);

        // First boot: recovers, then "dies" — the backend is dropped without
        // writing anything, exactly like a kill right after startup.
        assert_all_acked_present(dir.path(), &format!("on the first boot after {point}"));

        // Second boot: the durable state must be unchanged by the first.
        assert_all_acked_present(dir.path(), &format!("on the second boot after {point}"));
    }
}

// -------------------------------------------------------------------------
// inspect() must never mutate a directory frozen mid-compaction-crash
// -------------------------------------------------------------------------

/// Content hash (raw bytes) of every regular file directly inside `dir`.
fn dir_digest(dir: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut out = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            out.insert(
                entry.file_name().to_string_lossy().into_owned(),
                std::fs::read(entry.path()).unwrap(),
            );
        }
    }
    out
}

/// `inspect` must leave a directory frozen mid-compaction-crash byte
/// identical, for every one of the five windows a compaction can be killed
/// in — not just the two the hand-built fixtures in
/// `writer_inspect_test.rs` construct (a torn tail, an undecodable
/// snapshot). The regression tests above only verify content correctness
/// via a real, mutating open (`assert_all_acked_present`); this is the
/// non-mutating verification surface actually being non-mutating on the
/// same crashed artifacts.
#[test]
fn test_inspect_after_compaction_crash_is_byte_identical() {
    for point in CRASH_POINTS {
        let dir = tempfile::tempdir().unwrap();
        spawn_crashing_child(dir.path(), point);

        // The crashed child's own `open()` created `.lock` before it ever
        // reached the crash point (and a dead process leaves the file on
        // disk, only its advisory hold releases) — so `.lock` existing here
        // is expected. The byte-identity check below is what actually
        // proves `inspect` did not rewrite it (or anything else).
        let before = dir_digest(dir.path());
        let first = WalBackend::<State>::inspect(dir.path())
            .unwrap_or_else(|e| panic!("inspect failed after crash at {point}: {e}"));
        let after = dir_digest(dir.path());
        assert_eq!(
            before, after,
            "inspect mutated the directory after a crash at {point}"
        );

        // Repeatable...
        let second = WalBackend::<State>::inspect(dir.path()).unwrap();
        assert_eq!(
            first, second,
            "inspect was not repeatable after a crash at {point}"
        );

        // ...and agrees with what a real (mutating) open finds, once we're
        // done asserting byte-identity and can afford to actually open it.
        let opened = WalBackend::<State>::open(dir.path()).unwrap();
        let (_state, opened_report) = opened.load_with_report().unwrap();
        assert_eq!(
            first.has_loss(),
            opened_report.has_loss(),
            "inspect and a real open disagree on loss after a crash at {point}"
        );
    }
}
