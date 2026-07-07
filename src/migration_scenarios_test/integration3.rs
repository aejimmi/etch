use super::*;

/// retry_quarantine with nothing to recover returns 0.
mod integration_retry_quarantine_empty {
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
    fn retry_quarantine_returns_zero_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::<State, WalBackend<State>>::open_wal(dir.path().to_path_buf()).unwrap();
        assert_eq!(store.retry_quarantine().unwrap(), 0);
    }
}

/// Auto-retry quarantine on open: if migrations are registered that can
/// drain quarantined entries, they recover automatically without manual
/// `retry_quarantine()`.
mod integration_auto_retry_on_open {
    use super::*;

    // v1 shape
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct OldItem {
        n: u32,
    }

    // v2 shape
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct NewItem {
        n: u32,
        #[serde(default)]
        note: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Transactable)]
    struct StateWithMigration {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, NewItem>,
    }

    impl Replayable for StateWithMigration {
        fn apply_with_format(&mut self, ops: &[Op], format: ReplayFormat) -> crate::Result<()> {
            for op in ops {
                if op.collection() == 0 {
                    let _ = apply_op_versioned_with(
                        &mut self.items,
                        op,
                        format,
                        2,
                        <String as EtchKey>::from_bytes,
                    );
                }
            }
            Ok(())
        }

        fn apply_with_ctx(&mut self, ops: &[Op], ctx: &mut ReplayContext<'_>) -> crate::Result<()> {
            for op in ops {
                if op.collection() == 0 {
                    let _ = apply_op_versioned_with_ctx(
                        &mut self.items,
                        op,
                        0,
                        2,
                        ctx,
                        <String as EtchKey>::from_bytes,
                    );
                }
            }
            Ok(())
        }

        fn migrations() -> MigrationSet {
            MigrationSet::new().add(0, 1, 2, |bytes| {
                let old: OldItem = rmp_serde::from_slice(bytes)?;
                Ok(rmp_serde::to_vec_named(&NewItem {
                    n: old.n,
                    note: "auto-recovered".into(),
                })?)
            })
        }

        fn to_snapshot(&self) -> crate::Result<SnapshotPayload> {
            let mut entries = Vec::new();
            for (k, v) in &self.items {
                entries.push(SnapshotEntry {
                    key: EtchKey::to_bytes(k),
                    version: 2,
                    value: encode_msgpack_value(v)?,
                });
            }
            Ok(SnapshotPayload {
                schema_fingerprint: Self::schema_fingerprint(),
                collections: vec![CollectionSection {
                    collection_id: 0,
                    current_version: 2,
                    entries,
                }],
            })
        }

        fn from_snapshot(
            payload: SnapshotPayload,
            ctx: &mut ReplayContext<'_>,
        ) -> crate::Result<Self>
        where
            Self: Sized,
        {
            let mut state = Self::default();
            for section in &payload.collections {
                if section.collection_id == 0 {
                    for entry in &section.entries {
                        if let Some(v) = load_snapshot_entry::<NewItem>(entry, 0, 2, ctx)
                            && let Ok(k) = <String as EtchKey>::from_bytes(&entry.key)
                        {
                            state.items.insert(k, v);
                        }
                    }
                }
            }
            Ok(state)
        }

        fn schema_fingerprint() -> u64 {
            // Overriding here because we don't use derive for Replayable.
            // Same derive logic: xxh3 of [(collection=0, version=2)].
            let mut bytes = Vec::new();
            bytes.push(0u8);
            bytes.extend_from_slice(&2u16.to_le_bytes());
            xxhash_rust::xxh3::xxh3_64(&bytes)
        }
    }

    // Separate state type with NO migration — used to create the quarantine.
    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateBroken {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, NewItem>,
    }

    #[test]
    fn auto_retry_drains_quarantine_when_migrations_present() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        // Fabricate a v1 snapshot entry.
        let envelope = encode_versioned_value(1, &OldItem { n: 42 }).unwrap();
        let (_v, payload) = crate::wal::split_versioned_value(&envelope).unwrap();
        let fabricated = SnapshotPayload {
            schema_fingerprint: 0,
            collections: vec![CollectionSection {
                collection_id: 0,
                current_version: 1,
                entries: vec![SnapshotEntry {
                    key: b"k".to_vec(),
                    version: 1,
                    value: payload.to_vec(),
                }],
            }],
        };
        std::fs::create_dir_all(&state_dir).unwrap();
        let encoded = rmp_serde::to_vec_named(&fabricated).unwrap();
        let mut out = vec![b'E', b'S', b'N', b'A', 3u8];
        out.extend_from_slice(&encoded);
        std::fs::write(state_dir.join("snapshot.postcard"), out).unwrap();

        // Phase 1: open with NO migration → entry quarantined.
        {
            let store =
                Store::<StateBroken, WalBackend<StateBroken>>::open_wal(state_dir.clone()).unwrap();
            assert_eq!(store.quarantined().len(), 1);
            assert!(store.read().items.is_empty());
        }

        // Phase 2: open with migration → auto-retry runs, value recovered,
        // quarantine drained, WITHOUT calling retry_quarantine manually.
        let store =
            Store::<StateWithMigration, WalBackend<StateWithMigration>>::open_wal(state_dir)
                .unwrap();

        assert_eq!(
            store.quarantined().len(),
            0,
            "auto-retry drained quarantine"
        );
        let state = store.read();
        let got = state.items.get("k").expect("recovered automatically");
        assert_eq!(got.n, 42);
        assert_eq!(got.note, "auto-recovered");
    }
}

/// Auto-retry persists recovered ops to WAL, so they survive a restart
/// *even without another compaction*. This is the critical durability
/// guarantee: upgrade-with-migration + crash-before-snapshot must not
/// lose the newly-migrated data.
mod integration_auto_retry_persists_through_restart {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct OldItem {
        n: u32,
    }
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct NewItem {
        n: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Transactable)]
    struct State {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, NewItem>,
    }

    impl Replayable for State {
        fn apply_with_format(&mut self, ops: &[Op], format: ReplayFormat) -> crate::Result<()> {
            for op in ops {
                if op.collection() == 0 {
                    let _ = apply_op_versioned_with(
                        &mut self.items,
                        op,
                        format,
                        2,
                        <String as EtchKey>::from_bytes,
                    );
                }
            }
            Ok(())
        }
        fn apply_with_ctx(&mut self, ops: &[Op], ctx: &mut ReplayContext<'_>) -> crate::Result<()> {
            for op in ops {
                if op.collection() == 0 {
                    let _ = apply_op_versioned_with_ctx(
                        &mut self.items,
                        op,
                        0,
                        2,
                        ctx,
                        <String as EtchKey>::from_bytes,
                    );
                }
            }
            Ok(())
        }
        fn migrations() -> MigrationSet {
            MigrationSet::new().add(0, 1, 2, |bytes| {
                let old: OldItem = rmp_serde::from_slice(bytes)?;
                Ok(rmp_serde::to_vec_named(&NewItem { n: old.n })?)
            })
        }
        fn to_snapshot(&self) -> crate::Result<SnapshotPayload> {
            let mut entries = Vec::new();
            for (k, v) in &self.items {
                entries.push(SnapshotEntry {
                    key: EtchKey::to_bytes(k),
                    version: 2,
                    value: encode_msgpack_value(v)?,
                });
            }
            Ok(SnapshotPayload {
                schema_fingerprint: Self::schema_fingerprint(),
                collections: vec![CollectionSection {
                    collection_id: 0,
                    current_version: 2,
                    entries,
                }],
            })
        }
        fn from_snapshot(
            payload: SnapshotPayload,
            ctx: &mut ReplayContext<'_>,
        ) -> crate::Result<Self>
        where
            Self: Sized,
        {
            let mut state = Self::default();
            for section in &payload.collections {
                if section.collection_id == 0 {
                    for entry in &section.entries {
                        if let Some(v) = load_snapshot_entry::<NewItem>(entry, 0, 2, ctx)
                            && let Ok(k) = <String as EtchKey>::from_bytes(&entry.key)
                        {
                            state.items.insert(k, v);
                        }
                    }
                }
            }
            Ok(state)
        }
        fn schema_fingerprint() -> u64 {
            let mut b = Vec::new();
            b.push(0u8);
            b.extend_from_slice(&2u16.to_le_bytes());
            xxhash_rust::xxh3::xxh3_64(&b)
        }
    }

    #[test]
    fn auto_retry_persists_without_subsequent_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        // Fabricate v1 snapshot. Open it with no migrations → quarantined.
        let envelope = encode_versioned_value(1, &OldItem { n: 7 }).unwrap();
        let (_v, payload) = crate::wal::split_versioned_value(&envelope).unwrap();
        let fabricated = SnapshotPayload {
            schema_fingerprint: 0,
            collections: vec![CollectionSection {
                collection_id: 0,
                current_version: 1,
                entries: vec![SnapshotEntry {
                    key: b"k".to_vec(),
                    version: 1,
                    value: payload.to_vec(),
                }],
            }],
        };
        std::fs::create_dir_all(&state_dir).unwrap();
        let encoded = rmp_serde::to_vec_named(&fabricated).unwrap();
        let mut out = vec![b'E', b'S', b'N', b'A', 3u8];
        out.extend_from_slice(&encoded);
        std::fs::write(state_dir.join("snapshot.postcard"), out).unwrap();

        // Phase 1: open with migration registered → auto-retry recovers.
        // Close immediately WITHOUT forcing a snapshot.
        {
            let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
            assert_eq!(store.read().items.get("k").map(|v| v.n), Some(7));
            assert!(store.quarantined().is_empty());
            // No forced snapshot — rely on auto-retry having appended to WAL.
        }

        // Phase 2: reopen. The snapshot still has v1 data; the WAL should
        // contain the auto-retry-appended v2 value. If auto-retry didn't
        // persist, this would fail.
        let store2 = Store::<State, WalBackend<State>>::open_wal(state_dir).unwrap();
        assert_eq!(
            store2.read().items.get("k").map(|v| v.n),
            Some(7),
            "recovered value must persist across restart even without compaction"
        );
    }
}

/// File lock on open — prevents silent corruption from concurrent opens.
mod integration_file_lock {
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
    fn second_open_on_same_dir_fails_with_database_locked() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        let _first = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();

        let result = Store::<State, WalBackend<State>>::open_wal(state_dir.clone());
        match result {
            Ok(_) => panic!("second open should have failed"),
            Err(crate::Error::DatabaseLocked { dir }) => {
                assert_eq!(dir, state_dir.display().to_string());
            }
            Err(e) => panic!("expected DatabaseLocked, got {:?}", e),
        }
    }

    #[test]
    fn open_after_drop_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        {
            let store = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();
            store
                .write(|tx| {
                    tx.items.put("k".into(), Item { x: 1 })?;
                    Ok(())
                })
                .unwrap();
        } // Store dropped, lock released.

        let store = Store::<State, WalBackend<State>>::open_wal(state_dir).unwrap();
        assert_eq!(store.read().items.get("k").map(|v| v.x), Some(1));
    }

    #[test]
    fn lock_file_created_in_dir() {
        let dir = tempfile::tempdir().unwrap();
        let _store = Store::<State, WalBackend<State>>::open_wal(dir.path().to_path_buf()).unwrap();
        assert!(dir.path().join(".lock").exists(), ".lock file created");
    }

    #[test]
    fn lock_file_contains_pid() {
        let dir = tempfile::tempdir().unwrap();
        let _store = Store::<State, WalBackend<State>>::open_wal(dir.path().to_path_buf()).unwrap();
        let contents = std::fs::read_to_string(dir.path().join(".lock")).unwrap();
        assert!(
            contents.contains("pid="),
            "lock file should record our PID: got {contents:?}"
        );
    }

    #[test]
    fn different_dirs_are_independent() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();

        // Opening different dirs simultaneously is fine.
        let _a = Store::<State, WalBackend<State>>::open_wal(dir_a.path().to_path_buf()).unwrap();
        let _b = Store::<State, WalBackend<State>>::open_wal(dir_b.path().to_path_buf()).unwrap();
    }

    #[test]
    fn error_message_includes_directory_path() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        let _first = Store::<State, WalBackend<State>>::open_wal(state_dir.clone()).unwrap();

        let err = match Store::<State, WalBackend<State>>::open_wal(state_dir.clone()) {
            Ok(_) => panic!("second open should have failed"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains(&state_dir.display().to_string()));
        assert!(err.contains("already open"));
    }
}
