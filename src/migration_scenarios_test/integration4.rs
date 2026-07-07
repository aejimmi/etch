use super::*;

/// wal.prev safety net — the core scenarios for the compaction backup file.
mod integration_wal_prev_safety_net {
    use super::*;
    use crate::IncrementalSave;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Item {
        n: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct State {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, Item>,
    }

    fn wal_prev_exists(dir: &std::path::Path) -> bool {
        dir.join("wal.prev").exists()
    }

    #[test]
    fn wal_prev_created_on_compaction_deleted_on_next_boot() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        {
            let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
            store
                .write(|tx| {
                    tx.items.put("a".into(), Item { n: 1 })?;
                    tx.items.put("b".into(), Item { n: 2 })?;
                    Ok(())
                })
                .unwrap();
            let live = store.read().clone();
            store.backend().snapshot(&live).unwrap();
            assert!(
                wal_prev_exists(&state_dir),
                "wal.prev present immediately after compaction"
            );
        }

        {
            let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
            let state = store.read();
            assert_eq!(state.items.get("a").map(|v| v.n), Some(1));
            assert_eq!(state.items.get("b").map(|v| v.n), Some(2));
            drop(state);
            assert!(
                !wal_prev_exists(&state_dir),
                "wal.prev removed after successful boot"
            );
        }
    }

    /// Crash AFTER wal rotate but BEFORE snapshot commit. Next boot must
    /// recover via wal.prev (old snapshot + wal.prev = full state).
    #[test]
    fn crash_between_wal_rotate_and_snapshot_commit_recovers_via_wal_prev() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        {
            let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
            store
                .write(|tx| {
                    tx.items.put("precompact".into(), Item { n: 42 })?;
                    Ok(())
                })
                .unwrap();
        }

        // Simulate crash: copy wal.bin → wal.prev, reset wal.bin to empty
        // header, don't write snapshot.
        let wal_bin = state_dir.join("wal.bin");
        let wal_prev = state_dir.join("wal.prev");
        std::fs::copy(&wal_bin, &wal_prev).unwrap();
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&wal_bin)
                .unwrap();
            f.write_all(b"EWAL").unwrap();
            f.write_all(&[4]).unwrap();
            f.write_all(&[0u8; 3]).unwrap();
            f.write_all(&0u64.to_le_bytes()).unwrap();
            f.sync_all().unwrap();
        }
        assert!(!state_dir.join("snapshot.postcard").exists());
        assert!(wal_prev.exists());

        let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
        assert_eq!(
            store.read().items.get("precompact").map(|v| v.n),
            Some(42),
            "wal.prev replay recovers pre-compaction data"
        );
        assert!(
            !wal_prev_exists(&state_dir),
            "wal.prev cleaned up on successful boot"
        );
    }

    /// After a real compaction, crash before wal.prev deletion, then reopen.
    /// New snapshot + wal.prev exist; replay must be idempotent.
    #[test]
    fn idempotent_wal_prev_replay_after_crash_between_compaction_and_boot() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        {
            let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
            store
                .write(|tx| {
                    tx.items.put("alpha".into(), Item { n: 10 })?;
                    tx.items.put("beta".into(), Item { n: 20 })?;
                    Ok(())
                })
                .unwrap();
            let live = store.read().clone();
            store.backend().snapshot(&live).unwrap();
        }

        assert!(wal_prev_exists(&state_dir));
        let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
        let state = store.read();
        assert_eq!(state.items.get("alpha").map(|v| v.n), Some(10));
        assert_eq!(state.items.get("beta").map(|v| v.n), Some(20));
        assert_eq!(state.items.len(), 2);
        drop(state);
        assert!(!wal_prev_exists(&state_dir));
    }

    /// Corrupt snapshot + valid wal.prev: the key recovery case. Snapshot
    /// is mangled; wal.prev holds full recovery material.
    #[test]
    fn corrupted_snapshot_recovers_via_wal_prev() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        {
            let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
            store
                .write(|tx| {
                    tx.items.put("x".into(), Item { n: 100 })?;
                    tx.items.put("y".into(), Item { n: 200 })?;
                    Ok(())
                })
                .unwrap();
            let live = store.read().clone();
            store.backend().snapshot(&live).unwrap();
        }

        std::fs::write(state_dir.join("snapshot.postcard"), b"not a valid snapshot").unwrap();

        let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
        let state = store.read();
        assert_eq!(state.items.get("x").map(|v| v.n), Some(100));
        assert_eq!(state.items.get("y").map(|v| v.n), Some(200));
        drop(state);
        assert!(
            state_dir.join("snapshot.backup").exists(),
            "corrupt snapshot preserved as backup"
        );
    }

    /// Corrupt wal.prev: skipped with warning; other sources still work.
    #[test]
    fn corrupted_wal_prev_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        {
            let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
            store
                .write(|tx| {
                    tx.items.put("k".into(), Item { n: 1 })?;
                    Ok(())
                })
                .unwrap();
            let live = store.read().clone();
            store.backend().snapshot(&live).unwrap();
        }

        std::fs::write(state_dir.join("wal.prev"), b"garbage").unwrap();

        let store = Store::<State, WalBackend<State>>::open_wal(state_dir).unwrap();
        assert_eq!(store.read().items.get("k").map(|v| v.n), Some(1));
    }

    /// Consecutive compactions: wal.prev is OVERWRITTEN each time, not appended.
    #[test]
    fn wal_prev_overwritten_across_compactions() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();

        store
            .write(|tx| {
                tx.items.put("first".into(), Item { n: 1 })?;
                Ok(())
            })
            .unwrap();
        store.backend().snapshot(&store.read().clone()).unwrap();
        let size_1 = std::fs::metadata(state_dir.join("wal.prev")).unwrap().len();

        store
            .write(|tx| {
                tx.items.put("second".into(), Item { n: 2 })?;
                tx.items.put("third".into(), Item { n: 3 })?;
                Ok(())
            })
            .unwrap();
        store.backend().snapshot(&store.read().clone()).unwrap();
        let size_2 = std::fs::metadata(state_dir.join("wal.prev")).unwrap().len();

        assert_ne!(size_1, size_2, "wal.prev was overwritten, not appended");
    }
}

/// Verify on-disk WAL header version is 4 after a fresh open with 0.4.0.
mod integration_wal_header_v4 {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Item {
        x: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct State {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, Item>,
    }

    #[test]
    fn fresh_wal_writes_version_4_header() {
        let dir = tempfile::tempdir().unwrap();
        let _store = Store::<State, WalBackend<State>>::open_wal(dir.path().to_path_buf()).unwrap();

        let wal_bytes = std::fs::read(dir.path().join("wal.bin")).unwrap();
        assert_eq!(&wal_bytes[..4], b"EWAL", "magic preserved");
        assert_eq!(wal_bytes[4], 4, "new WALs are version 4");
    }

    /// A hand-crafted v3 WAL (legacy) must still open and replay.
    #[test]
    fn legacy_v3_wal_still_opens() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();

        // Write a minimal valid v3 WAL header (16 bytes).
        let mut header = Vec::new();
        header.extend_from_slice(b"EWAL");
        header.push(3); // version 3
        header.extend_from_slice(&[0u8; 3]); // reserved
        header.extend_from_slice(&0u64.to_le_bytes()); // snapshot_seq
        std::fs::write(dir.path().join("wal.bin"), &header).unwrap();

        // Open should succeed, reading the v3 header without complaint.
        let store = Store::<State, WalBackend<State>>::open_wal(dir.path().to_path_buf()).unwrap();
        assert!(store.read().items.is_empty());
    }

    /// A future WAL version (v99) must be rejected with a clear error,
    /// NOT opened with empty state (which would destroy data on compaction).
    #[test]
    fn future_wal_version_rejected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();

        let mut header = Vec::new();
        header.extend_from_slice(b"EWAL");
        header.push(99); // future version
        header.extend_from_slice(&[0u8; 3]);
        header.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(dir.path().join("wal.bin"), &header).unwrap();

        let result = Store::<State, WalBackend<State>>::open_wal(dir.path().to_path_buf());
        assert!(result.is_err(), "future WAL version must refuse to open");
    }
}

/// Ten-hop migration chain — stress the chain walker.
mod integration_deep_chain {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Counter {
        n: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct State {
        #[etch(collection = 0, version = 10)]
        items: BTreeMap<String, Counter>,
    }

    #[test]
    fn ten_hop_chain_accumulates_correctly() {
        // Each migration increments `n`. Starting from v1 with n=0,
        // after 9 hops to v10, n should be 9.
        let envelope = encode_versioned_value(1, &Counter { n: 0 }).unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: envelope,
        }];

        let mut migrations = MigrationSet::new();
        for from in 1u16..10 {
            migrations = migrations.add(0, from, from + 1, move |bytes| {
                let mut c: Counter = rmp_serde::from_slice(bytes)?;
                c.n += 1;
                Ok(rmp_serde::to_vec_named(&c)?)
            });
        }

        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = State::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert_eq!(state.items.get("k").unwrap().n, 9);
        assert!(quarantine.is_empty());
    }
}

mod integration_schema_fingerprint {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Item {
        x: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateA {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, Item>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateB {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, Item>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateC {
        #[etch(collection = 0, version = 1)]
        a: BTreeMap<String, Item>,
        #[etch(collection = 1, version = 1)]
        b: BTreeMap<String, Item>,
    }

    #[test]
    fn fingerprint_is_deterministic_and_nonzero() {
        let fp1 = StateA::schema_fingerprint();
        let fp2 = StateA::schema_fingerprint();
        assert_eq!(fp1, fp2);
        assert_ne!(fp1, 0);
    }

    #[test]
    fn fingerprint_changes_when_version_bumped() {
        assert_ne!(StateA::schema_fingerprint(), StateB::schema_fingerprint());
    }

    #[test]
    fn fingerprint_changes_when_collection_added() {
        assert_ne!(StateA::schema_fingerprint(), StateC::schema_fingerprint());
    }

    #[test]
    fn fingerprint_embedded_in_snapshot() {
        let state = StateA::default();
        let payload = state.to_snapshot().unwrap();
        assert_eq!(payload.schema_fingerprint, StateA::schema_fingerprint());
    }

    #[test]
    fn drift_detected_on_reopen_with_different_version() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        // Write with StateA and compact.
        {
            let store = Store::<StateA, WalBackend<StateA>>::open_wal(state_dir.clone()).unwrap();
            store
                .write(|tx| {
                    tx.items.put("k".into(), Item { x: 1 })?;
                    Ok(())
                })
                .unwrap();
            let live = store.read().clone();
            store.backend().snapshot(&live).unwrap();
        }

        // Reopen with StateB (same shape, different version). Fingerprint
        // mismatches; drift is recorded in the report; load still succeeds.
        // No migration registered → entry quarantined.
        let store = Store::<StateB, WalBackend<StateB>>::open_wal(state_dir).unwrap();
        assert_eq!(store.quarantined().len(), 1);
        assert!(store.read().items.is_empty());

        // The report now makes the drift and the loss programmatically
        // visible instead of only printing them.
        let report = store.replay_report();
        assert_eq!(report.quarantined(), 1);
        assert!(report.has_loss());
        assert!(
            matches!(report.schema_drift(), crate::SchemaDrift::Detected { .. }),
            "version bump must surface as detected drift"
        );
    }
}
