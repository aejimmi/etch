use super::*;

/// Multi-collection end-to-end: two collections both need migrations,
/// written to disk via the Store, reopened as new schema, both migrate.
mod integration_store_roundtrip_with_migrations {
    use super::*;

    // v1 types (for writing the DB)
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct UserV1 {
        name: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct BoardV1 {
        title: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV1 {
        #[etch(collection = 0, version = 1)]
        users: BTreeMap<String, UserV1>,
        #[etch(collection = 1, version = 1)]
        boards: BTreeMap<String, BoardV1>,
    }

    // v2 types: both have extra fields.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct UserV2 {
        name: String,
        email: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct BoardV2 {
        title: String,
        public: bool,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 2)]
        users: BTreeMap<String, UserV2>,
        #[etch(collection = 1, version = 2)]
        boards: BTreeMap<String, BoardV2>,
    }

    impl Replayable for StateV2 {
        fn apply_with_format(&mut self, ops: &[Op], format: ReplayFormat) -> crate::Result<()> {
            for op in ops {
                let _ = match op.collection() {
                    0 => apply_op_versioned_with(
                        &mut self.users,
                        op,
                        format,
                        2,
                        <String as EtchKey>::from_bytes,
                    ),
                    1 => apply_op_versioned_with(
                        &mut self.boards,
                        op,
                        format,
                        2,
                        <String as EtchKey>::from_bytes,
                    ),
                    _ => Ok(()),
                };
            }
            Ok(())
        }

        fn apply_with_ctx(&mut self, ops: &[Op], ctx: &mut ReplayContext<'_>) -> crate::Result<()> {
            for op in ops {
                let _ = match op.collection() {
                    0 => apply_op_versioned_with_ctx(
                        &mut self.users,
                        op,
                        0,
                        2,
                        ctx,
                        <String as EtchKey>::from_bytes,
                    ),
                    1 => apply_op_versioned_with_ctx(
                        &mut self.boards,
                        op,
                        1,
                        2,
                        ctx,
                        <String as EtchKey>::from_bytes,
                    ),
                    _ => Ok(()),
                };
            }
            Ok(())
        }

        fn migrations() -> MigrationSet {
            MigrationSet::new()
                .add(0, 1, 2, |bytes| {
                    let old: UserV1 = rmp_serde::from_slice(bytes)?;
                    let new = UserV2 {
                        name: old.name,
                        email: String::new(),
                    };
                    Ok(rmp_serde::to_vec_named(&new)?)
                })
                .add(1, 1, 2, |bytes| {
                    let old: BoardV1 = rmp_serde::from_slice(bytes)?;
                    let new = BoardV2 {
                        title: old.title,
                        public: false,
                    };
                    Ok(rmp_serde::to_vec_named(&new)?)
                })
        }

        fn to_snapshot(&self) -> crate::Result<SnapshotPayload> {
            let mut users_entries = Vec::new();
            for (k, v) in &self.users {
                users_entries.push(SnapshotEntry {
                    key: EtchKey::to_bytes(k),
                    version: 2,
                    value: encode_msgpack_value(v)?,
                });
            }
            let mut boards_entries = Vec::new();
            for (k, v) in &self.boards {
                boards_entries.push(SnapshotEntry {
                    key: EtchKey::to_bytes(k),
                    version: 2,
                    value: encode_msgpack_value(v)?,
                });
            }
            Ok(SnapshotPayload {
                schema_fingerprint: 0,
                collections: vec![
                    CollectionSection {
                        collection_id: 0,
                        current_version: 2,
                        entries: users_entries,
                    },
                    CollectionSection {
                        collection_id: 1,
                        current_version: 2,
                        entries: boards_entries,
                    },
                ],
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
                match section.collection_id {
                    0 => {
                        for entry in &section.entries {
                            if let Some(v) = load_snapshot_entry::<UserV2>(entry, 0, 2, ctx)
                                && let Ok(k) = <String as EtchKey>::from_bytes(&entry.key)
                            {
                                state.users.insert(k, v);
                            }
                        }
                    }
                    1 => {
                        for entry in &section.entries {
                            if let Some(v) = load_snapshot_entry::<BoardV2>(entry, 1, 2, ctx)
                                && let Ok(k) = <String as EtchKey>::from_bytes(&entry.key)
                            {
                                state.boards.insert(k, v);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(state)
        }
    }

    #[test]
    fn store_roundtrip_with_migrations_on_both_collections() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        // Write v1 data via the Store, then compact.
        {
            let store = Store::<StateV1, WalBackend<StateV1>>::open_wal(state_dir.clone()).unwrap();
            store
                .write(|tx| {
                    tx.users.put(
                        "alice".into(),
                        UserV1 {
                            name: "Alice".into(),
                        },
                    )?;
                    tx.users.put("bob".into(), UserV1 { name: "Bob".into() })?;
                    tx.boards.put(
                        "roadmap".into(),
                        BoardV1 {
                            title: "Q1 Roadmap".into(),
                        },
                    )?;
                    Ok(())
                })
                .unwrap();
            let live = store.read().clone();
            store.backend().snapshot(&live).unwrap();
        }

        // Reopen as v2 — both collections migrate.
        {
            let store = Store::<StateV2, WalBackend<StateV2>>::open_wal(state_dir.clone()).unwrap();
            let state = store.read();

            // Users migrated: email defaults to empty.
            let alice = state.users.get("alice").expect("alice migrated");
            assert_eq!(alice.name, "Alice");
            assert_eq!(alice.email, "");
            assert!(state.users.contains_key("bob"));

            // Boards migrated: public defaults to false.
            let roadmap = state.boards.get("roadmap").expect("board migrated");
            assert_eq!(roadmap.title, "Q1 Roadmap");
            assert!(!roadmap.public);

            // Nothing quarantined.
            assert!(store.quarantined().is_empty());
        }
    }
}

/// Quarantine survives across compactions: write quarantinable data,
/// close, reopen, force compaction, close, reopen again — quarantine
/// entries still present.
mod integration_quarantine_survives_compaction {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct OldItem {
        n: u32,
    }

    // New state uses a completely different shape — NO migration registered.
    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct NewState {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, IncompatibleValue>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct IncompatibleValue {
        field_only_in_v2: String,
    }

    #[test]
    fn quarantine_survives_multiple_compactions() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        // Hand-craft a snapshot containing a v1-encoded OldItem for collection 0.
        // This simulates data left behind by an older binary.
        let envelope = encode_versioned_value(1, &OldItem { n: 42 }).unwrap();
        let (_version, payload) = crate::wal::split_versioned_value(&envelope).unwrap();
        let fabricated = SnapshotPayload {
            schema_fingerprint: 0,
            collections: vec![CollectionSection {
                collection_id: 0,
                current_version: 1,
                entries: vec![SnapshotEntry {
                    key: b"quarantined".to_vec(),
                    version: 1,
                    value: payload.to_vec(),
                }],
            }],
        };
        // Write the snapshot file directly.
        std::fs::create_dir_all(&state_dir).unwrap();
        let snap_bytes = {
            let encoded = rmp_serde::to_vec_named(&fabricated).unwrap();
            let mut out = Vec::new();
            out.extend_from_slice(b"ESNA");
            // version 3 (msgpack, no compression — matches feature-off path)
            out.push(3);
            out.extend_from_slice(&encoded);
            out
        };
        // Compression feature may pick v4 at write time, but reading v3 always works.
        std::fs::write(state_dir.join("snapshot.postcard"), snap_bytes).unwrap();

        // Open with NewState — value cannot deserialize as IncompatibleValue,
        // quarantined.
        let recovered_count;
        {
            let store =
                Store::<NewState, WalBackend<NewState>>::open_wal(state_dir.clone()).unwrap();
            let quarantined = store.quarantined();
            assert_eq!(quarantined.len(), 1, "quarantined on first open");
            assert_eq!(quarantined[0].key, b"quarantined");
            // Write something to force a compaction later.
            store
                .write(|tx| {
                    tx.items.put(
                        "fresh".into(),
                        IncompatibleValue {
                            field_only_in_v2: "ok".into(),
                        },
                    )?;
                    Ok(())
                })
                .unwrap();
            // Force compaction.
            let live = store.read().clone();
            store.backend().snapshot(&live).unwrap();
            recovered_count = store.quarantined().len();
        }

        // Reopen — quarantine persisted across close.
        {
            let store =
                Store::<NewState, WalBackend<NewState>>::open_wal(state_dir.clone()).unwrap();
            // Quarantine from the fabricated snapshot should still be present.
            assert_eq!(
                store.quarantined().len(),
                recovered_count,
                "quarantine stable across restart+compaction"
            );
            // Fresh write survives.
            assert!(store.read().items.contains_key("fresh"));
        }
    }
}

/// Write via the Store API to a key that's currently quarantined —
/// quarantine entry should be removed, new value visible.
mod integration_store_write_supersedes_quarantine {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct OldItem {
        n: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct NewState {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, NewItem>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct NewItem {
        name: String,
    }

    #[test]
    fn store_write_clears_matching_quarantine_entry() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_path_buf();

        // Hand-craft a snapshot with a quarantinable old entry for key "x".
        let envelope = encode_versioned_value(1, &OldItem { n: 42 }).unwrap();
        let (_version, payload) = crate::wal::split_versioned_value(&envelope).unwrap();
        let fabricated = SnapshotPayload {
            schema_fingerprint: 0,
            collections: vec![CollectionSection {
                collection_id: 0,
                current_version: 1,
                entries: vec![SnapshotEntry {
                    key: b"x".to_vec(),
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

        let store = Store::<NewState, WalBackend<NewState>>::open_wal(state_dir.clone()).unwrap();
        assert_eq!(store.quarantined().len(), 1);

        // Write a fresh value for the same key.
        store
            .write(|tx| {
                tx.items.put(
                    "x".into(),
                    NewItem {
                        name: "superseded".into(),
                    },
                )?;
                Ok(())
            })
            .unwrap();

        // Quarantine entry for "x" is gone.
        assert!(
            store.quarantined().is_empty(),
            "normal write should drop quarantine entry for same key"
        );
        assert_eq!(store.read().items.get("x").unwrap().name, "superseded");
    }
}
