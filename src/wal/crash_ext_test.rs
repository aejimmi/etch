//! Extended crash-injection durability tests (wave 2).
//!
//! The wave-1 harness in `crash_test.rs` covers the *compaction* window
//! (`wal.prev` creation → snapshot rename). This file broadens the coverage to
//! three more crash windows, each driven by a re-exec'd child that aborts at a
//! deterministic `ETCHDB_CRASH_POINT` (see `maybe_crash` / `crash_armed` in
//! `format.rs`). `ETCHDB_CRASH_SKIP` lets a child acknowledge several writes
//! before the crash so the point fires on a *specific* later operation.
//!
//! - `wal_torn_entry` — abort mid-append, after the length+payload are on disk
//!   but before the trailing hash. Invariant: the torn tail is rejected on the
//!   next boot, never replayed as a valid entry; every fsync'd write survives.
//! - `post_wal_sync` — abort immediately after `sync()`'s `sync_all`, before
//!   the caller's `write()` returns. Invariant (durability): the fsync'd writes
//!   ARE recovered.
//! - `quarantine_pre_rename` — abort during `Quarantine::save`, after the tmp
//!   file is fsync'd but before the rename. Invariant (atomicity): reopen sees
//!   the previous committed quarantine or none — never a torn file.
//!
//! `crash_ext_child_worker` is a no-op under a normal `cargo test` run; it only
//! acts when the parent sets the `ETCHDB_CRASH_*` env vars and re-execs it.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::backend::Backend;
use crate::wal::{IncrementalSave, Op, ReplayFormat, Replayable, WalBackend, apply_op};
use crate::{Quarantine, QuarantineReason, QuarantinedEntry};

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

/// Acknowledged (fsync'd) writes laid down before the crash in the torn/sync
/// scenarios.
const ACK_COUNT: usize = 16;

// -------------------------------------------------------------------------
// Child worker — dispatches by crash point.
// -------------------------------------------------------------------------

/// Child entry point. Selects a scenario body from `ETCHDB_CRASH_POINT` and
/// runs it; the crash point aborts mid-way. Reaching `exit(0)` means the point
/// never fired (the parent asserts a crash).
#[test]
fn crash_ext_child_worker() {
    let (dir, point) = match (
        std::env::var("ETCHDB_CRASH_DIR"),
        std::env::var("ETCHDB_CRASH_POINT"),
    ) {
        (Ok(d), Ok(p)) => (d, p),
        _ => return,
    };

    match point.as_str() {
        "wal_torn_entry" => run_torn_append_child(&dir),
        "post_wal_sync" => run_post_sync_child(&dir),
        "quarantine_pre_rename" => run_quarantine_child(&dir),
        _ => return, // A point owned by another worker.
    }

    std::process::exit(0);
}

/// Append `ACK_COUNT` acknowledged ops, then append one more that aborts
/// mid-write (torn: length+payload flushed, hash not). Skip budget lets the
/// acknowledged appends through.
fn run_torn_append_child(dir: &str) {
    let backend = WalBackend::<State>::open(dir).unwrap();
    backend.set_snapshot_threshold(u64::MAX);

    for i in 0..ACK_COUNT {
        backend
            .save_ops(&[put_op(&format!("k{i}"), &format!("v{i}"))])
            .unwrap();
    }
    backend.sync().unwrap();

    // This append aborts after the payload is flushed, before the hash.
    backend
        .save_ops(&[put_op("torn", "should-not-survive")])
        .unwrap();

    std::process::exit(0);
}

/// Append `ACK_COUNT` ops, then a single `sync()` that aborts right after
/// `sync_all` — the ops are durable, so all must be recovered.
fn run_post_sync_child(dir: &str) {
    let backend = WalBackend::<State>::open(dir).unwrap();
    backend.set_snapshot_threshold(u64::MAX);

    for i in 0..ACK_COUNT {
        backend
            .save_ops(&[put_op(&format!("k{i}"), &format!("v{i}"))])
            .unwrap();
    }
    // sync() fsyncs then aborts (skip=0). Every appended op is durable.
    backend.sync().unwrap();

    std::process::exit(0);
}

/// Commit an "old" quarantine (entry A), then save a "new" one (A + B) that
/// aborts after the tmp fsync, before the rename. Skip=1 lets the first save
/// commit.
fn run_quarantine_child(dir: &str) {
    let dir = Path::new(dir);
    let mut q = Quarantine::new();
    q.insert(quarantined("old"));
    q.save(dir).unwrap();

    q.insert(quarantined("new"));
    q.save(dir).unwrap(); // aborts before rename

    std::process::exit(0);
}

fn quarantined(key: &str) -> QuarantinedEntry {
    QuarantinedEntry {
        collection: 0,
        key: key.as_bytes().to_vec(),
        version: 1,
        value: vec![1, 2, 3],
        reason: QuarantineReason::MissingMigration { from: 1, to: 2 },
    }
}

// -------------------------------------------------------------------------
// Parent tests.
// -------------------------------------------------------------------------

/// libtest `--exact` filter name for [`crash_ext_child_worker`].
fn child_test_name() -> String {
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    format!("{module}::crash_ext_child_worker")
}

/// Spawn the child with the given crash point/skip; assert it aborted.
fn spawn_crashing_child(dir: &Path, point: &str, skip: u64) {
    let exe = std::env::current_exe().unwrap();
    let status = std::process::Command::new(&exe)
        .arg("--exact")
        .arg(child_test_name())
        .env("ETCHDB_CRASH_DIR", dir)
        .env("ETCHDB_CRASH_POINT", point)
        .env("ETCHDB_CRASH_SKIP", skip.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "child was expected to crash at {point} but exited cleanly"
    );
}

/// (a) A crash that leaves a torn entry (payload on disk, hash missing) must
/// never replay that entry, and must lose no acknowledged write.
#[test]
fn test_crash_mid_append_never_replays_torn_entry() {
    let dir = tempfile::tempdir().unwrap();
    spawn_crashing_child(dir.path(), "wal_torn_entry", ACK_COUNT as u64);

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();

    for i in 0..ACK_COUNT {
        assert_eq!(
            state.items.get(&format!("k{i}")).map(String::as_str),
            Some(format!("v{i}").as_str()),
            "acknowledged write k{i} lost after torn-append crash"
        );
    }
    assert!(
        !state.items.contains_key("torn"),
        "a torn (hash-less) entry must never be replayed as valid"
    );

    // The load truncated the torn tail; a fresh acknowledged append must land
    // cleanly and survive a second reopen (no sparse hole).
    backend.save_ops(&[put_op("after", "ok")]).unwrap();
    backend.sync().unwrap();
    drop(backend);

    let reopened = WalBackend::<State>::open(dir.path()).unwrap();
    let state2 = reopened.load().unwrap();
    assert_eq!(state2.items.get("after").map(String::as_str), Some("ok"));
    assert!(!state2.items.contains_key("torn"));
}

/// (b) A crash immediately after `sync_all` (before the caller returns) must
/// recover every fsync'd write — the durability guarantee.
#[test]
fn test_crash_after_fsync_recovers_durable_write() {
    let dir = tempfile::tempdir().unwrap();
    spawn_crashing_child(dir.path(), "post_wal_sync", 0);

    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();

    for i in 0..ACK_COUNT {
        assert_eq!(
            state.items.get(&format!("k{i}")).map(String::as_str),
            Some(format!("v{i}").as_str()),
            "fsync'd write k{i} was NOT recovered after post-sync crash"
        );
    }
}

/// (c) A crash during `Quarantine::save` (tmp fsync'd, before rename) must
/// leave the previously committed quarantine intact — never a torn file, never
/// the half-written new version.
#[test]
fn test_crash_during_quarantine_save_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    spawn_crashing_child(dir.path(), "quarantine_pre_rename", 1);

    // The real quarantine file must decode cleanly and hold exactly the old
    // committed set — never the aborted "new" version, never a torn blob.
    let q = Quarantine::load(dir.path()).expect("quarantine file must not be torn");
    let keys: Vec<&[u8]> = q.entries().iter().map(|e| e.key.as_slice()).collect();
    assert_eq!(
        keys,
        vec![b"old".as_slice()],
        "reopen must see the old quarantine, not the aborted new one"
    );

    // An orphaned quarantine.tmp must not break a normal backend open.
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let reloaded = backend.quarantined();
    assert_eq!(reloaded.len(), 1);
    assert_eq!(reloaded[0].key, b"old");
}
