//! Compaction serialization (R1) and `wal.prev` retention (R4).
//!
//! Two compaction paths exist — the foreground write path and the grouped
//! flusher — and neither used to exclude the other. These tests drive them
//! against each other rather than asserting the exclusion in prose, and pin
//! the retention rule that a backup is superseded only by a committed
//! snapshot.

use super::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::store::{FlushPolicy, Store};
use crate::wal::{Overlay, Transactable, apply_overlay_btree};

// ---------------------------------------------------------------------------
// A Transactable wrapper so these tests can drive a real Store.
// ---------------------------------------------------------------------------

pub(super) struct Tx<'a> {
    #[allow(dead_code)]
    committed: &'a State,
    items: Overlay<String, String>,
    ops: Vec<Op>,
}

pub(super) struct StateOverlay {
    items: Overlay<String, String>,
}

impl Tx<'_> {
    fn insert(&mut self, key: &str, value: &str) {
        self.ops.push(put_op(key, value));
        self.items.put(key.to_string(), value.to_string());
    }
}

impl Transactable for State {
    type Tx<'a> = Tx<'a>;
    type Overlay = StateOverlay;

    fn begin_tx(&self) -> Tx<'_> {
        Tx {
            committed: self,
            items: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: Tx<'_>) -> (Vec<Op>, StateOverlay) {
        (tx.ops, StateOverlay { items: tx.items })
    }

    fn apply_overlay(&mut self, overlay: StateOverlay) {
        apply_overlay_btree(&mut self.items, overlay.items);
    }
}

type WalStore = Store<State, WalBackend<State>>;

/// Read every entry of a WAL file, or `None` if it is absent.
fn wal_entries(path: &std::path::Path) -> Option<Vec<Vec<Op>>> {
    if !path.exists() {
        return None;
    }
    crate::wal::format::WalFile::iter_entries(path)
        .ok()
        .map(|(entries, _)| entries)
}

/// Every key a WAL file's ops put, flattened.
fn wal_keys(path: &std::path::Path) -> Vec<String> {
    wal_entries(path)
        .unwrap_or_default()
        .iter()
        .flatten()
        .map(|op| String::from_utf8_lossy(op.key()).into_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// R1: the two compaction paths are mutually exclusive
// ---------------------------------------------------------------------------

/// The headline race: a `write_durable` on one thread and the grouped flusher
/// on another, both above the snapshot threshold, for many iterations. Every
/// acknowledged op must be present after reopen and the snapshot must decode
/// on every iteration.
#[test]
fn test_concurrent_compaction_paths_keep_every_acked_write() {
    const ITERATIONS: usize = 40;
    const WRITERS: usize = 4;
    const PER_WRITER: usize = 12;

    for iteration in 0..ITERATIONS {
        let dir = tempfile::tempdir().unwrap();
        let overlaps;
        {
            let mut store: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
            // Low threshold + a fast flusher tick: both paths see
            // `should_snapshot()` constantly.
            store.set_snapshot_threshold(2);
            store.set_flush_policy(FlushPolicy::Grouped {
                interval: std::time::Duration::from_micros(200),
            });
            let store = Arc::new(store);

            let mut handles = Vec::new();
            for w in 0..WRITERS {
                let s = Arc::clone(&store);
                handles.push(std::thread::spawn(move || {
                    for i in 0..PER_WRITER {
                        let key = format!("w{w}k{i}");
                        s.write_durable(|tx| {
                            tx.insert(&key, &key);
                            Ok(())
                        })
                        .unwrap();
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            store.flush().unwrap();
            overlaps = store.backend().snapshot_overlaps();
        }

        assert_eq!(
            overlaps, 0,
            "iteration {iteration}: two snapshot bodies overlapped"
        );

        // The committed snapshot must decode — a shared snapshot.tmp with two
        // writers produces a spliced file that does not.
        let snapshot = dir.path().join("snapshot.postcard");
        assert!(snapshot.exists(), "iteration {iteration}: no snapshot");

        let reopened: WalStore = Store::open_wal(dir.path().to_path_buf()).unwrap();
        assert!(
            !matches!(
                reopened.replay_report().snapshot(),
                crate::SnapshotStatus::Discarded { .. }
            ),
            "iteration {iteration}: snapshot failed to decode: {}",
            reopened.replay_report().summary()
        );
        let state = reopened.read();
        for w in 0..WRITERS {
            for i in 0..PER_WRITER {
                let key = format!("w{w}k{i}");
                assert_eq!(
                    state.items.get(&key).map(String::as_str),
                    Some(key.as_str()),
                    "iteration {iteration}: acknowledged write {key} lost"
                );
            }
        }
    }
}

/// A second concurrent `write_snapshot` must never observe a half-written
/// temp file. The exclusion itself is instrumented: entering a snapshot body
/// while another is in flight records an overlap.
#[test]
fn test_write_snapshot_never_overlaps_itself() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(WalBackend::<State>::open(dir.path()).unwrap());
    backend.set_snapshot_threshold(u64::MAX);

    let mut state = State::default();
    for i in 0..200 {
        state.items.insert(format!("k{i}"), format!("v{i}"));
    }
    let state = Arc::new(state);

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..6 {
        let b = Arc::clone(&backend);
        let s = Arc::clone(&state);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                b.snapshot(&s).unwrap();
            }
        }));
    }
    std::thread::sleep(std::time::Duration::from_millis(120));
    stop.store(true, Ordering::Release);
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        backend.snapshot_overlaps(),
        0,
        "concurrent snapshot bodies overlapped — snapshot.tmp had two writers"
    );
    assert!(
        backend.snapshot_writes() > 1,
        "test did not actually exercise concurrent snapshots"
    );
    // The last committed snapshot still decodes. Release the directory lock
    // first — a second backend on the same directory would be refused.
    drop(backend);
    let reloaded = WalBackend::<State>::open(dir.path()).unwrap().load();
    assert!(reloaded.is_ok(), "snapshot corrupted by concurrent writers");
}

/// A caller that queues behind another compaction re-checks the threshold
/// under the exclusion, and does not immediately compact again.
#[test]
fn test_compact_if_needed_rechecks_threshold_under_exclusion() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(2);

    let mut state = State::default();
    for i in 0..3 {
        let key = format!("k{i}");
        state.items.insert(key.clone(), key.clone());
        backend.save_ops(&[put_op(&key, &key)]).unwrap();
    }
    backend.sync().unwrap();
    assert!(backend.should_snapshot());

    let mut state_fn = || Ok(state.clone());
    assert!(
        backend.compact_if_needed(&mut state_fn).unwrap(),
        "first call is over threshold and must compact"
    );
    assert_eq!(backend.snapshot_writes(), 1);

    // Second call: the WAL was reset by the first, so nothing to do.
    assert!(
        !backend.compact_if_needed(&mut state_fn).unwrap(),
        "queued caller must re-check the threshold, not compact again"
    );
    assert_eq!(
        backend.snapshot_writes(),
        1,
        "no second snapshot body should have run"
    );
}

// ---------------------------------------------------------------------------
// R4: wal.prev holds the immediately preceding generation
// ---------------------------------------------------------------------------

/// After two back-to-back compactions `wal.prev` holds the ops of the
/// generation between them — never an empty WAL, which is what a second
/// interleaved rotation used to leave behind.
#[test]
fn test_wal_prev_holds_preceding_generation_after_two_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(u64::MAX);
    let wal_prev = dir.path().join("wal.prev");

    let mut state = State::default();
    for key in ["gen1a", "gen1b"] {
        state.items.insert(key.into(), key.into());
        backend.save_ops(&[put_op(key, key)]).unwrap();
    }
    backend.sync().unwrap();
    backend.snapshot(&state).unwrap();
    assert_eq!(wal_keys(&wal_prev), vec!["gen1a", "gen1b"]);

    for key in ["gen2a", "gen2b", "gen2c"] {
        state.items.insert(key.into(), key.into());
        backend.save_ops(&[put_op(key, key)]).unwrap();
    }
    backend.sync().unwrap();
    backend.snapshot(&state).unwrap();

    assert_eq!(
        wal_keys(&wal_prev),
        vec!["gen2a", "gen2b", "gen2c"],
        "wal.prev must hold the immediately preceding generation"
    );
    assert!(
        !wal_keys(&wal_prev).is_empty(),
        "wal.prev must never be a copy of an already-reset WAL"
    );
}

/// The snapshot is committed before the WAL rotates, so a `wal.prev` is only
/// ever overwritten once a snapshot containing its ops is durable.
#[test]
fn test_snapshot_is_committed_before_wal_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(u64::MAX);

    let mut state = State::default();
    state.items.insert("a".into(), "1".into());
    backend.save_ops(&[put_op("a", "1")]).unwrap();
    backend.sync().unwrap();
    backend.snapshot(&state).unwrap();

    // Snapshot exists and the live WAL is empty: the rotation ran after the
    // rename, not before it.
    assert!(dir.path().join("snapshot.postcard").exists());
    assert!(
        wal_entries(&dir.path().join("wal.bin")).unwrap().is_empty(),
        "live WAL must be reset by the rotation"
    );
    assert!(
        !dir.path().join("snapshot.tmp").exists(),
        "temp snapshot must be renamed away, not left behind"
    );
}

/// A load no longer deletes `wal.prev`: the backup outlives every boot until
/// the next compaction supersedes it, and repeated boots stay idempotent.
#[test]
fn test_wal_prev_survives_boots_until_next_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let wal_prev = dir.path().join("wal.prev");

    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend.set_snapshot_threshold(u64::MAX);
        let mut state = State::default();
        for key in ["a", "b"] {
            state.items.insert(key.into(), key.into());
            backend.save_ops(&[put_op(key, key)]).unwrap();
        }
        backend.sync().unwrap();
        backend.snapshot(&state).unwrap();
    }
    assert!(wal_prev.exists(), "compaction creates wal.prev");
    let before = std::fs::read(&wal_prev).unwrap();

    // Three boots in a row: the backup must still be there, byte-identical,
    // and replay must stay idempotent.
    for boot in 0..3 {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        let state = backend.load().unwrap();
        assert_eq!(state.items.get("a").map(String::as_str), Some("a"));
        assert_eq!(state.items.get("b").map(String::as_str), Some("b"));
        assert_eq!(
            state.items.len(),
            2,
            "boot {boot}: replay was not idempotent"
        );
        assert!(wal_prev.exists(), "boot {boot}: wal.prev was deleted");
        assert_eq!(
            std::fs::read(&wal_prev).unwrap(),
            before,
            "boot {boot}: wal.prev was rewritten"
        );
    }

    // The next compaction is what supersedes it — and the file does not grow
    // without bound, it is replaced.
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(u64::MAX);
    let mut state = backend.load().unwrap();
    state.items.insert("c".into(), "c".into());
    backend.save_ops(&[put_op("c", "c")]).unwrap();
    backend.sync().unwrap();
    backend.snapshot(&state).unwrap();

    assert_eq!(
        wal_keys(&wal_prev),
        vec!["c"],
        "wal.prev is replaced by the next generation, not appended to"
    );
}

/// `wal.prev` replay is idempotent against a snapshot that already contains
/// its ops — including a delete that a naive re-application would resurrect.
#[test]
fn test_wal_prev_replay_is_idempotent_over_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    backend.set_snapshot_threshold(u64::MAX);

    let mut state = State::default();
    state.items.insert("keep".into(), "v1".into());
    state.items.insert("gone".into(), "v1".into());
    backend
        .save_ops(&[put_op("keep", "v1"), put_op("gone", "v1")])
        .unwrap();
    state.items.remove("gone");
    backend.save_ops(&[del_op("gone")]).unwrap();
    backend.sync().unwrap();
    backend.snapshot(&state).unwrap();
    drop(backend);

    let reopened = WalBackend::<State>::open(dir.path()).unwrap();
    let loaded = reopened.load().unwrap();
    assert_eq!(loaded.items.get("keep").map(String::as_str), Some("v1"));
    assert!(
        !loaded.items.contains_key("gone"),
        "wal.prev replay resurrected a deleted key"
    );
    assert_eq!(loaded, state);
}

/// The P3 regression, deterministically.
///
/// A snapshot is encoded from a state captured before the WAL rotates, so an
/// op can reach `wal.bin` after that capture and be absent from the committed
/// snapshot (in grouped mode, the window between a writer's WAL append and
/// its overlay merge). The rotation then leaves that op in `wal.prev` and
/// nowhere else. A boot that deletes `wal.prev` once it has replayed it into
/// *memory* makes the durable state a strict subset of the acknowledged one —
/// the very next kill loses the op. Two boots with no compaction between them
/// must not lose it.
#[test]
fn test_op_only_in_wal_prev_survives_two_boots() {
    let dir = tempfile::tempdir().unwrap();

    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        backend.set_snapshot_threshold(u64::MAX);

        let mut state = State::default();
        for key in ["a", "b"] {
            state.items.insert(key.into(), key.into());
            backend.save_ops(&[put_op(key, key)]).unwrap();
        }
        backend.sync().unwrap();
        backend.snapshot(&state).unwrap();

        // "late" is acknowledged into wal.bin, but the state handed to the
        // next compaction predates it.
        backend.save_ops(&[put_op("late", "late")]).unwrap();
        backend.sync().unwrap();
        backend.snapshot(&state).unwrap();
    }

    // First boot: recovered from wal.prev, which is now its only copy.
    {
        let backend = WalBackend::<State>::open(dir.path()).unwrap();
        let state = backend.load().unwrap();
        assert_eq!(
            state.items.get("late").map(String::as_str),
            Some("late"),
            "wal.prev replay must recover the acknowledged op"
        );
    }

    // Second boot, with nothing in between — a kill right after startup.
    let backend = WalBackend::<State>::open(dir.path()).unwrap();
    let state = backend.load().unwrap();
    assert_eq!(
        state.items.get("late").map(String::as_str),
        Some("late"),
        "the first boot deleted the only durable copy of an acknowledged op"
    );
    assert_eq!(state.items.get("a").map(String::as_str), Some("a"));
    assert_eq!(state.items.get("b").map(String::as_str), Some("b"));
}
