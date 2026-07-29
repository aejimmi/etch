//! `Store::checkpoint_to` (R2) — a consistent copy of a live store.
//!
//! The threat is a silent partial copy: one that opens cleanly while missing
//! writes the caller was told succeeded. The central test therefore runs a
//! writer thread against the checkpoint and asserts, over a recorded
//! committed-set, that every acknowledged key survives in the copy and no
//! never-written key appears in it.

use super::*;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::store::FlushPolicy;
use crate::wal::{
    IncrementalSave, Op, Overlay, ReplayFormat, Replayable, Transactable, WalBackend, apply_op,
    apply_overlay_btree,
};

// ---------------------------------------------------------------------------
// State under test
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct CkState {
    items: BTreeMap<String, String>,
}

impl Replayable for CkState {
    fn apply_with_format(&mut self, ops: &[Op], _format: ReplayFormat) -> Result<()> {
        for op in ops {
            apply_op(&mut self.items, op)?;
        }
        Ok(())
    }
}

struct CkTx<'a> {
    #[allow(dead_code)]
    committed: &'a CkState,
    items: Overlay<String, String>,
    ops: Vec<Op>,
}

struct CkOverlay {
    items: Overlay<String, String>,
}

impl CkTx<'_> {
    fn insert(&mut self, key: &str, value: &str) {
        self.ops.push(Op::Put {
            collection: 0,
            key: key.as_bytes().to_vec(),
            value: postcard::to_allocvec(&value.to_string()).unwrap(),
        });
        self.items.put(key.to_string(), value.to_string());
    }
}

impl Transactable for CkState {
    type Tx<'a> = CkTx<'a>;
    type Overlay = CkOverlay;

    fn begin_tx(&self) -> CkTx<'_> {
        CkTx {
            committed: self,
            items: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: CkTx<'_>) -> (Vec<Op>, CkOverlay) {
        (tx.ops, CkOverlay { items: tx.items })
    }

    fn apply_overlay(&mut self, overlay: CkOverlay) {
        apply_overlay_btree(&mut self.items, overlay.items);
    }
}

type CkStore = Store<CkState, WalBackend<CkState>>;

/// Deterministic value for a key, so the copy can be checked for the *exact*
/// committed value rather than mere presence.
fn value_for(key: &str) -> String {
    format!("v-{key}")
}

/// Open a store rooted at `dir` with the given policy.
fn open_store(dir: &Path, policy: FlushPolicy) -> CkStore {
    let mut store: CkStore = Store::open_wal(dir.to_path_buf()).unwrap();
    store.set_snapshot_threshold(8);
    store.set_flush_policy(policy);
    store
}

// ---------------------------------------------------------------------------
// The core guarantee
// ---------------------------------------------------------------------------

/// With a writer thread issuing `write` and `write_durable` for the duration
/// of the checkpoint, a store reopened from `dest` contains every key whose
/// write returned `Ok` before `checkpoint_to` was called — with the exact
/// committed value — and no key that was never written.
#[test]
fn test_checkpoint_under_write_load_keeps_every_committed_write() {
    for (label, policy) in [
        ("immediate", FlushPolicy::Immediate),
        (
            "grouped",
            FlushPolicy::Grouped {
                interval: Duration::from_millis(2),
            },
        ),
    ] {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let dest = dst.path().join("copy");

        let store = Arc::new(open_store(src.path(), policy));
        let acked: Arc<parking_lot::Mutex<BTreeSet<String>>> = Arc::default();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Fixed seed: a deterministic LCG picks the key sequence, so a
        // failure reproduces exactly.
        let writer = {
            let store = Arc::clone(&store);
            let acked = Arc::clone(&acked);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut seed: u64 = 0x5eed_1234;
                let mut n = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let key = format!("k{}", seed % 500);
                    let value = value_for(&key);
                    let durable = n.is_multiple_of(3);
                    let result = if durable {
                        store.write_durable(|tx| {
                            tx.insert(&key, &value);
                            Ok(())
                        })
                    } else {
                        store.write(|tx| {
                            tx.insert(&key, &value);
                            Ok(())
                        })
                    };
                    result.unwrap();
                    acked.lock().insert(key);
                    n += 1;
                }
            })
        };

        // Let the writer get ahead, then freeze the committed set as of now.
        std::thread::sleep(Duration::from_millis(40));
        let committed_before: BTreeSet<String> = acked.lock().clone();
        assert!(
            !committed_before.is_empty(),
            "{label}: writer produced nothing to check"
        );

        let report = store.checkpoint_to(&dest).unwrap();

        stop.store(true, std::sync::atomic::Ordering::Release);
        writer.join().unwrap();
        let ever_written: BTreeSet<String> = acked.lock().clone();
        drop(store);

        assert!(report.bytes() > 0, "{label}: checkpoint copied nothing");

        // The copy verifies without being perturbed.
        let verdict = WalBackend::<CkState>::inspect(&dest).unwrap();
        assert!(
            !verdict.has_loss(),
            "{label}: copy does not load cleanly: {}",
            verdict.summary()
        );

        let copy: CkStore = Store::open_wal(dest.clone()).unwrap();
        let state = copy.read();
        for key in &committed_before {
            assert_eq!(
                state.items.get(key),
                Some(&value_for(key)),
                "{label}: acknowledged write {key} missing from the checkpoint"
            );
        }
        for key in state.items.keys() {
            assert!(
                ever_written.contains(key),
                "{label}: checkpoint contains {key}, which was never written"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// File set
// ---------------------------------------------------------------------------

/// `.lock` is never copied, and `wal.prev` always is when the source has one.
#[test]
fn test_checkpoint_file_set() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let dest = dst.path().join("copy");

    let store = open_store(src.path(), FlushPolicy::Immediate);
    for i in 0..20 {
        store
            .write(|tx| {
                tx.insert(&format!("k{i}"), &value_for(&format!("k{i}")));
                Ok(())
            })
            .unwrap();
    }
    assert!(
        src.path().join(".lock").exists(),
        "source must have a lock file for this test to mean anything"
    );

    let report = store.checkpoint_to(&dest).unwrap();

    assert!(
        !dest.join(".lock").exists(),
        "a lock file must never be copied"
    );
    assert!(
        !report.files().iter().any(|f| f == ".lock"),
        "report must not claim a lock file"
    );
    assert!(dest.join("snapshot.postcard").exists());
    assert!(dest.join("wal.bin").exists());
    assert!(
        src.path().join("wal.prev").exists(),
        "the forced snapshot must have produced a wal.prev"
    );
    assert!(
        dest.join("wal.prev").exists(),
        "wal.prev must be copied whenever the source has one"
    );
    assert_eq!(
        report.bytes(),
        report
            .files()
            .iter()
            .map(|f| std::fs::metadata(dest.join(f)).unwrap().len())
            .sum::<u64>(),
        "reported byte count must match the copied files"
    );
}

/// `quarantine.bin` and `snapshot.backup` are copied when present.
#[test]
fn test_checkpoint_copies_optional_files() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let dest = dst.path().join("copy");

    let store = open_store(src.path(), FlushPolicy::Immediate);
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();
    // Fabricate the two optional files the copy set must pick up.
    std::fs::write(src.path().join("snapshot.backup"), b"old snapshot").unwrap();
    let mut q = crate::Quarantine::new();
    q.insert(crate::QuarantinedEntry {
        collection: 0,
        key: b"stuck".to_vec(),
        version: 1,
        value: vec![1],
        reason: crate::QuarantineReason::MissingMigration { from: 1, to: 2 },
    });
    q.save(src.path()).unwrap();

    let report = store.checkpoint_to(&dest).unwrap();
    assert!(dest.join("snapshot.backup").exists());
    assert!(dest.join("quarantine.bin").exists());
    assert!(report.files().iter().any(|f| f == "quarantine.bin"));
}

/// The destination is created if absent and, on Unix, inherits the source
/// directory's mode — a copy of a private store is not world-readable.
#[test]
fn test_checkpoint_creates_dest_with_source_permissions() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let dest = dst.path().join("nested").join("copy");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(src.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    let store = open_store(src.path(), FlushPolicy::Immediate);
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();
    let _ = store.checkpoint_to(&dest).unwrap();

    assert!(dest.is_dir(), "destination must be created");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let src_mode = std::fs::metadata(src.path()).unwrap().permissions().mode() & 0o777;
        let dst_mode = std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
        assert_eq!(dst_mode, src_mode, "destination mode must match the source");
    }
}

/// A checkpoint of a store with nothing new to compact skips the forced
/// snapshot and says so.
#[test]
fn test_checkpoint_skips_snapshot_when_wal_is_empty() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let store = open_store(src.path(), FlushPolicy::Immediate);
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    let first = store.checkpoint_to(dst.path().join("one")).unwrap();
    assert!(first.snapshot_forced(), "first checkpoint must compact");

    let second = store.checkpoint_to(dst.path().join("two")).unwrap();
    assert!(
        !second.snapshot_forced(),
        "a back-to-back checkpoint has nothing to compact"
    );
    let copy: CkStore = Store::open_wal(dst.path().join("two")).unwrap();
    assert_eq!(copy.read().items.get("a").map(String::as_str), Some("1"));
}

// ---------------------------------------------------------------------------
// Rejected destinations
// ---------------------------------------------------------------------------

/// Checkpointing into a directory that already holds a store file returns a
/// typed error and writes nothing — not even the forced snapshot.
#[test]
fn test_checkpoint_refuses_dest_holding_a_store_file() {
    for existing in ["snapshot.postcard", "wal.bin", "quarantine.bin"] {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(dst.path().join(existing), b"occupied").unwrap();

        let store = open_store(src.path(), FlushPolicy::Immediate);
        store
            .write(|tx| {
                tx.insert("a", "1");
                Ok(())
            })
            .unwrap();
        let before: Vec<_> = std::fs::read_dir(src.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();

        let err = store.checkpoint_to(dst.path()).unwrap_err();
        match err {
            Error::CheckpointDestNotEmpty { ref file, .. } => assert_eq!(file, existing),
            other => panic!("expected CheckpointDestNotEmpty for {existing}, got {other:?}"),
        }

        let after: Vec<_> = std::fs::read_dir(src.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            before.len(),
            after.len(),
            "a rejected checkpoint must not write to the source"
        );
        assert_eq!(
            std::fs::read(dst.path().join(existing)).unwrap(),
            b"occupied",
            "a rejected checkpoint must not write to the destination"
        );
    }
}

/// A destination inside the source (or equal to it, or containing it) is
/// structurally impossible and is refused.
#[test]
fn test_checkpoint_refuses_nested_destination() {
    let src = tempfile::tempdir().unwrap();
    let store = open_store(src.path(), FlushPolicy::Immediate);
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    for dest in [
        src.path().to_path_buf(),
        src.path().join("inner"),
        src.path().join("a").join("b"),
        src.path().parent().unwrap().to_path_buf(),
    ] {
        let err = store.checkpoint_to(&dest).unwrap_err();
        assert!(
            matches!(err, Error::CheckpointDestInvalid { .. }),
            "expected CheckpointDestInvalid for {}, got {err:?}",
            dest.display()
        );
    }
}

/// A destination path that exists as a file is refused.
#[test]
fn test_checkpoint_refuses_file_destination() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let dest = dst.path().join("a-file");
    std::fs::write(&dest, b"not a directory").unwrap();

    let store = open_store(src.path(), FlushPolicy::Immediate);
    let err = store.checkpoint_to(&dest).unwrap_err();
    assert!(
        matches!(err, Error::CheckpointDestInvalid { .. }),
        "expected CheckpointDestInvalid, got {err:?}"
    );
}

/// An in-memory store has nothing on disk to copy.
#[test]
fn test_checkpoint_on_memory_store_is_unsupported() {
    let dst = tempfile::tempdir().unwrap();
    let store = Store::<CkState>::memory();
    let err = store.checkpoint_to(dst.path().join("copy")).unwrap_err();
    assert!(
        matches!(err, Error::CheckpointUnsupported),
        "expected CheckpointUnsupported, got {err:?}"
    );
    assert!(
        !dst.path().join("copy").exists(),
        "an unsupported checkpoint must create nothing"
    );
}

// ---------------------------------------------------------------------------
// Crash injection
// ---------------------------------------------------------------------------

/// Crash points straddling every phase of a checkpoint.
const CHECKPOINT_CRASH_POINTS: &[&str] = &[
    "checkpoint_pre_snapshot",
    "checkpoint_post_snapshot",
    "checkpoint_mid_copy",
    "checkpoint_post_copy",
];

/// Acknowledged writes the child lays down before checkpointing.
const CHILD_ACK_COUNT: usize = 20;

/// libtest `--exact` filter name for [`checkpoint_crash_child_worker`].
fn child_test_name() -> String {
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    format!("{module}::checkpoint_crash_child_worker")
}

/// Child body: acknowledge `CHILD_ACK_COUNT` writes, then checkpoint into a
/// sibling directory and die at the requested phase.
///
/// A no-op under a normal `cargo test` run.
#[test]
fn checkpoint_crash_child_worker() {
    let (root, point) = match (
        std::env::var("ETCHDB_CRASH_DIR"),
        std::env::var("ETCHDB_CRASH_POINT"),
    ) {
        (Ok(d), Ok(p)) => (d, p),
        _ => return,
    };
    if !CHECKPOINT_CRASH_POINTS.contains(&point.as_str()) {
        return; // A point owned by another worker.
    }

    let root = Path::new(&root);
    let store = open_store(&root.join("src"), FlushPolicy::Immediate);
    for i in 0..CHILD_ACK_COUNT {
        let key = format!("k{i}");
        store
            .write_durable(|tx| {
                tx.insert(&key, &value_for(&key));
                Ok(())
            })
            .unwrap();
    }

    // Aborts inside; reaching the next line means the point never fired.
    let _ = store.checkpoint_to(root.join("dst")).unwrap();
    std::process::exit(0);
}

/// A crash at any checkpoint phase leaves the *source* intact: it reopens
/// with a clean report and every acknowledged key readable.
#[test]
fn test_crash_during_checkpoint_leaves_source_intact() {
    let exe = std::env::current_exe().unwrap();
    let child = child_test_name();

    for point in CHECKPOINT_CRASH_POINTS {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("src")).unwrap();

        let status = std::process::Command::new(&exe)
            .arg("--exact")
            .arg(&child)
            .env("ETCHDB_CRASH_DIR", root.path())
            .env("ETCHDB_CRASH_POINT", point)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(
            !status.success(),
            "child was expected to crash at {point} but exited cleanly"
        );

        let src = root.path().join("src");
        let (store, report) = Store::<CkState, WalBackend<CkState>>::open_wal_with_report(src)
            .unwrap_or_else(|e| panic!("source unreadable after crash at {point}: {e}"));
        assert!(
            !report.has_loss(),
            "crash at {point} left the source damaged: {}",
            report.summary()
        );
        let state = store.read();
        for i in 0..CHILD_ACK_COUNT {
            let key = format!("k{i}");
            assert_eq!(
                state.items.get(&key),
                Some(&value_for(&key)),
                "crash at {point} lost acknowledged key {key} from the source"
            );
        }
    }
}

/// Content hash (raw bytes) of every regular file directly inside `dir`.
fn dir_digest(dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
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

/// `inspect` on the source — and, once it exists, the partially-written
/// destination — must never mutate either directory, at every one of the
/// four phases a checkpoint can be killed in.
/// `test_crash_during_checkpoint_leaves_source_intact` above already checks
/// content correctness via a real (mutating) open; this checks the
/// non-mutating verification surface QA is supposed to use on a frozen
/// crash artifact actually holds its own promise in every one of those same
/// windows, not just the two the byte-identity fixtures in
/// `writer_inspect_test.rs` happen to construct by hand.
#[test]
fn test_inspect_after_checkpoint_crash_is_byte_identical() {
    let exe = std::env::current_exe().unwrap();
    let child = child_test_name();

    for point in CHECKPOINT_CRASH_POINTS {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("src")).unwrap();

        let status = std::process::Command::new(&exe)
            .arg("--exact")
            .arg(&child)
            .env("ETCHDB_CRASH_DIR", root.path())
            .env("ETCHDB_CRASH_POINT", point)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(
            !status.success(),
            "child was expected to crash at {point} but exited cleanly"
        );

        // The crashed child's own `open()` of `src` created `.lock` before
        // it ever reached the crash point (a dead process leaves the file on
        // disk; only its advisory hold releases), so `.lock` existing here
        // is expected. Byte-identity is what proves `inspect` did not
        // rewrite it or anything else — `checkpoint_to` never copies
        // `.lock` into `dst` in the first place, so no such file should
        // exist there at all regardless of `inspect`.
        let src = root.path().join("src");
        let src_before = dir_digest(&src);
        let _src_report = WalBackend::<CkState>::inspect(&src)
            .unwrap_or_else(|e| panic!("inspect(src) failed after crash at {point}: {e}"));
        let src_after = dir_digest(&src);
        assert_eq!(
            src_before, src_after,
            "inspect mutated the source after a crash at {point}"
        );

        // The destination only exists from `checkpoint_mid_copy` onward
        // (`prepare_dest` runs after `checkpoint_post_snapshot`); where it
        // exists, `inspect` must be just as inert on the half-written copy.
        let dst = root.path().join("dst");
        if dst.exists() {
            let dst_before = dir_digest(&dst);
            let _dst_report = WalBackend::<CkState>::inspect(&dst).unwrap_or_else(|e| {
                panic!("inspect(dst) failed on a partial checkpoint copy at {point}: {e}")
            });
            let dst_after = dir_digest(&dst);
            assert_eq!(
                dst_before, dst_after,
                "inspect mutated the partial destination after a crash at {point}"
            );
            assert!(
                !dst.join(".lock").exists(),
                "checkpoint_to must never copy .lock into the destination"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Quarantined rows
// ---------------------------------------------------------------------------

/// A checkpoint of a store with a persisted quarantine copies
/// `quarantine.bin`, and `inspect` reports the copy identically to the
/// source — same quarantine count, same verdict, same notes.
#[test]
fn test_checkpoint_with_quarantine_matches_source_inspect() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let dest = dst.path().join("copy");

    let store = open_store(src.path(), FlushPolicy::Immediate);
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    let mut q = crate::Quarantine::new();
    q.insert(crate::QuarantinedEntry {
        collection: 0,
        key: b"stuck".to_vec(),
        version: 1,
        value: vec![9, 9],
        reason: crate::QuarantineReason::MissingMigration { from: 1, to: 2 },
    });
    q.save(src.path()).unwrap();

    let report = store.checkpoint_to(&dest).unwrap();
    assert!(dest.join("quarantine.bin").exists());
    assert!(report.files().iter().any(|f| f == "quarantine.bin"));

    let source_verdict = WalBackend::<CkState>::inspect(src.path()).unwrap();
    let dest_verdict = WalBackend::<CkState>::inspect(&dest).unwrap();

    assert_eq!(source_verdict.quarantined(), 1);
    assert!(source_verdict.has_loss());
    assert_eq!(
        source_verdict, dest_verdict,
        "a checkpoint copy must inspect identically to its source, quarantine included"
    );
}

// ---------------------------------------------------------------------------
// Empty store
// ---------------------------------------------------------------------------

/// A never-written store has no snapshot yet, so the "nothing to compact"
/// short-circuit (empty WAL *and* an already-committed snapshot) does not
/// apply on the first checkpoint — it still forces one, of the empty
/// default state. A second, back-to-back checkpoint then hits the
/// `snapshot_forced() == false` path, pinning both halves of that branch for
/// a store that never had a single write, not just one with prior data
/// (`test_checkpoint_skips_snapshot_when_wal_is_empty` above).
#[test]
fn test_checkpoint_of_never_written_store() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let store = open_store(src.path(), FlushPolicy::Immediate);
    assert!(
        !src.path().join("snapshot.postcard").exists(),
        "a never-written store has no snapshot yet"
    );

    let first = store.checkpoint_to(dst.path().join("one")).unwrap();
    assert!(
        first.snapshot_forced(),
        "no committed snapshot exists yet, so the first checkpoint must still force one"
    );
    let copy_one: CkStore = Store::open_wal(dst.path().join("one")).unwrap();
    assert!(copy_one.read().items.is_empty());
    assert!(!copy_one.replay_report().has_loss());

    let second = store.checkpoint_to(dst.path().join("two")).unwrap();
    assert!(
        !second.snapshot_forced(),
        "empty WAL plus an already-committed snapshot: nothing left to compact"
    );
    let copy_two: CkStore = Store::open_wal(dst.path().join("two")).unwrap();
    assert!(copy_two.read().items.is_empty());
    assert!(!copy_two.replay_report().has_loss());
}

// ---------------------------------------------------------------------------
// Repeated / concurrent checkpoints against the same store
// ---------------------------------------------------------------------------

/// A second, real checkpoint into the same destination a first checkpoint
/// already populated is refused — not just a destination fabricated by hand
/// ahead of time (`test_checkpoint_refuses_dest_holding_a_store_file`
/// above) — and the refusal writes nothing into either directory.
#[test]
fn test_second_real_checkpoint_to_same_dest_is_refused() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let dest = dst.path().join("copy");

    let store = open_store(src.path(), FlushPolicy::Immediate);
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    let first = store.checkpoint_to(&dest).unwrap();
    assert!(first.bytes() > 0);

    store
        .write(|tx| {
            tx.insert("b", "2");
            Ok(())
        })
        .unwrap();

    let err = store.checkpoint_to(&dest).unwrap_err();
    assert!(
        matches!(err, Error::CheckpointDestNotEmpty { .. }),
        "expected CheckpointDestNotEmpty, got {err:?}"
    );

    let copy: CkStore = Store::open_wal(dest.clone()).unwrap();
    assert_eq!(copy.read().items.get("a").map(String::as_str), Some("1"));
    assert!(
        !copy.read().items.contains_key("b"),
        "a refused second checkpoint must not have written the new key into dest"
    );
}

/// Two threads calling `checkpoint_to` on the same store, into two different
/// destinations, at the same time: the write gate serializes them, and both
/// destinations end up a fully consistent copy.
#[test]
fn test_concurrent_checkpoints_to_different_dests_both_succeed() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let store = Arc::new(open_store(src.path(), FlushPolicy::Immediate));
    for i in 0..30 {
        let key = format!("k{i}");
        store
            .write(|tx| {
                tx.insert(&key, &value_for(&key));
                Ok(())
            })
            .unwrap();
    }

    let dest_a = dst.path().join("a");
    let dest_b = dst.path().join("b");
    let barrier = Arc::new(std::sync::Barrier::new(2));

    let handle_a = {
        let store = Arc::clone(&store);
        let dest = dest_a.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            store.checkpoint_to(&dest)
        })
    };
    let handle_b = {
        let store = Arc::clone(&store);
        let dest = dest_b.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            store.checkpoint_to(&dest)
        })
    };

    let report_a = handle_a.join().unwrap().unwrap();
    let report_b = handle_b.join().unwrap().unwrap();
    assert!(report_a.bytes() > 0);
    assert!(report_b.bytes() > 0);

    for dest in [dest_a, dest_b] {
        let copy: CkStore = Store::open_wal(dest.clone()).unwrap();
        assert!(
            !copy.replay_report().has_loss(),
            "{}: copy does not load cleanly",
            dest.display()
        );
        for i in 0..30 {
            let key = format!("k{i}");
            assert_eq!(
                copy.read().items.get(&key),
                Some(&value_for(&key)),
                "{}: missing key {key}",
                dest.display()
            );
        }
    }
}

/// Two threads racing `checkpoint_to` on the same store into the *same*
/// destination: the write gate makes the race deterministic rather than a
/// true filesystem race — one call runs to completion before the other's
/// `validate_dest` can observe the directory, so exactly one succeeds and
/// the other is refused with `CheckpointDestNotEmpty`, never a partial
/// write or a panic.
#[test]
fn test_concurrent_checkpoints_to_same_dest_exactly_one_succeeds() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let dest = dst.path().join("copy");

    let store = Arc::new(open_store(src.path(), FlushPolicy::Immediate));
    store
        .write(|tx| {
            tx.insert("a", "1");
            Ok(())
        })
        .unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let store = Arc::clone(&store);
        let dest = dest.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            store.checkpoint_to(&dest)
        }));
    }
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let ok_count = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        ok_count, 1,
        "exactly one of two concurrent checkpoints to the same dest must succeed: {results:?}"
    );
    for r in &results {
        if let Err(e) = r {
            assert!(
                matches!(e, Error::CheckpointDestNotEmpty { .. }),
                "expected CheckpointDestNotEmpty for the loser, got {e:?}"
            );
        }
    }

    let copy: CkStore = Store::open_wal(dest).unwrap();
    assert_eq!(copy.read().items.get("a").map(String::as_str), Some("1"));
}

/// Repeated checkpoints against a store under real multi-writer, grouped-mode
/// hammering: four writer threads, an explicit-`flush()` thread, and six
/// checkpoints taken back-to-back while all of that is in flight. Every
/// checkpoint must copy something and reopen clean — a stronger version of
/// the single-writer property test above, aimed at races that only show up
/// with more than one concurrent writer.
#[test]
fn test_repeated_checkpoints_under_multi_writer_grouped_load() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let store = Arc::new(open_store(
        src.path(),
        FlushPolicy::Grouped {
            interval: Duration::from_millis(1),
        },
    ));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writers: Vec<_> = (0..4)
        .map(|w| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut n = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    let key = format!("w{w}k{}", n % 50);
                    let value = value_for(&key);
                    store
                        .write(|tx| {
                            tx.insert(&key, &value);
                            Ok(())
                        })
                        .unwrap();
                    n += 1;
                }
            })
        })
        .collect();

    let flusher = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                store.flush().unwrap();
            }
        })
    };

    for i in 0..6 {
        std::thread::sleep(Duration::from_millis(15));
        let dest = dst.path().join(format!("cp{i}"));
        let report = store.checkpoint_to(&dest).unwrap();
        assert!(report.bytes() > 0, "checkpoint {i} copied nothing");

        let verdict = WalBackend::<CkState>::inspect(&dest).unwrap();
        assert!(
            !verdict.has_loss(),
            "checkpoint {i}: copy does not load cleanly: {}",
            verdict.summary()
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Release);
    for w in writers {
        w.join().unwrap();
    }
    flusher.join().unwrap();
}

// ---------------------------------------------------------------------------
// CLOSED: this hole no longer exists on the public API surface.
// `Store::retry_quarantine` used to call `IncrementalSave::save_ops` + `sync`
// directly on the backend with no write gate held, so it could append to
// `wal.bin` while `checkpoint_to` was mid-copy on another thread. It now takes
// the write gate for its whole duration and reaches the WAL through
// `Store::append_durable` — the same route `write_durable` uses — so the
// append is serialized against the checkpoint's copy window and ordered
// correctly behind grouped-mode pending ops. See
// `src/store/mod.rs::Store::retry_quarantine`.
//
// The test below is kept as defense in depth. It reaches *past* the public API
// to drive the raw `save_ops` + `sync` pair concurrently with `checkpoint_to`,
// which is no longer something the store itself does, and pins the underlying
// robustness property that made the old hole survivable: a torn last WAL entry
// — the worst this race can produce, since `WalFile::iter_entries` truncates
// at the last valid checksum rather than treating a torn tail as corruption —
// does not take the rest of the copy down with it. A regression that makes the
// destination fail to reopen turns this test red.
// ---------------------------------------------------------------------------

/// A raw, ungated WAL appender racing `checkpoint_to`'s copy window leaves the
/// destination loadable. `Store::retry_quarantine` no longer appends this way,
/// so this is a property of the copy, not of any current caller.
#[test]
fn test_unguarded_wal_append_race_during_checkpoint_copy_stays_loadable() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let store = Arc::new(open_store(src.path(), FlushPolicy::Immediate));
    for i in 0..50 {
        let key = format!("k{i}");
        store
            .write(|tx| {
                tx.insert(&key, &value_for(&key));
                Ok(())
            })
            .unwrap();
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // What `Store::retry_quarantine` used to do with the recovered ops:
    // append straight to the backend with no write gate held.
    let racer = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                let key = format!("racer{n}");
                let op = Op::Put {
                    collection: 0,
                    key: key.as_bytes().to_vec(),
                    value: postcard::to_allocvec(&value_for(&key)).unwrap(),
                };
                // Best-effort: a failure here (e.g. a torn write
                // mid-checkpoint-copy) is not itself the property under
                // test, so it is not asserted on.
                let _ = store.backend().save_ops(&[op]);
                let _ = store.backend().sync();
                n += 1;
            }
        })
    };

    std::thread::sleep(Duration::from_millis(2));
    for i in 0..10 {
        let dest = dst.path().join(format!("race{i}"));
        let report = store
            .checkpoint_to(&dest)
            .unwrap_or_else(|e| panic!("checkpoint_to must not itself fail: {e}"));
        assert!(report.bytes() > 0);

        let opened = Store::<CkState, WalBackend<CkState>>::open_wal(dest.clone());
        assert!(
            opened.is_ok(),
            "checkpoint {i}: destination corrupted by the raw append race: {:?}",
            opened.err()
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Release);
    racer.join().unwrap();
}

// ---------------------------------------------------------------------------
// Cross-filesystem destination — not portable, gated behind an env var
// ---------------------------------------------------------------------------

/// `tempfile::tempdir()` for both `src` and `dst` typically land on the same
/// filesystem, and there is no portable way to allocate a guaranteed-
/// different one in CI. This is a manual/local check: point
/// `ETCHDB_QA_XDEV_DIR` at a directory known to be on a different
/// filesystem/mount than `std::env::temp_dir()` (a second disk, a tmpfs vs a
/// real disk, a network mount) and run with `--ignored`.
#[test]
#[ignore = "requires ETCHDB_QA_XDEV_DIR on a different filesystem than the temp dir; not portable across hosts/CI"]
fn test_checkpoint_across_filesystems() {
    let Ok(xdev) = std::env::var("ETCHDB_QA_XDEV_DIR") else {
        return;
    };
    let src = tempfile::tempdir().unwrap();
    let dest_root = tempfile::tempdir_in(&xdev).unwrap();
    let dest = dest_root.path().join("copy");

    let store = open_store(src.path(), FlushPolicy::Immediate);
    for i in 0..20 {
        let key = format!("k{i}");
        store
            .write(|tx| {
                tx.insert(&key, &value_for(&key));
                Ok(())
            })
            .unwrap();
    }

    let report = store.checkpoint_to(&dest).unwrap();
    assert!(report.bytes() > 0);

    let copy: CkStore = Store::open_wal(dest).unwrap();
    for i in 0..20 {
        let key = format!("k{i}");
        assert_eq!(copy.read().items.get(&key), Some(&value_for(&key)));
    }
}
