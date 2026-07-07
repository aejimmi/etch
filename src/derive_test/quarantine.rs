//! Quarantine retry after registering a missing migration, and the
//! compacted-v1-with-empty-WAL upgrade to v2 via snapshot migration.

use super::*;
use crate::store::Store;
use crate::wal::WalBackend;

#[test]
fn test_retry_quarantine_after_fix() {
    use crate::IncrementalSave;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().to_path_buf();

    // Phase A: write a v1 value and compact. The snapshot holds a v1 item.
    {
        let store =
            Store::<CompactStateV1, WalBackend<CompactStateV1>>::open_wal(state_dir.clone())
                .unwrap();
        store
            .write(|tx| {
                tx.items.put(
                    "quarantinee".into(),
                    CompactV1Item {
                        payload: "preserve-me".into(),
                    },
                )?;
                Ok(())
            })
            .unwrap();
        let live = store.read().clone();
        store.backend().snapshot(&live).unwrap();
    }

    // Phase B: simulate a binary WITHOUT migration 1->2 registered.
    // We reuse CompactStateV2 but with a local override by creating a new
    // type that has NO migrations. For simplicity, use BrokenV2State.

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Transactable)]
    struct BrokenV2State {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, CompactV2Item>,
    }

    impl crate::Replayable for BrokenV2State {
        fn apply_with_format(
            &mut self,
            ops: &[Op],
            format: crate::ReplayFormat,
        ) -> crate::Result<()> {
            for op in ops {
                if op.collection() == 0 {
                    let _ = crate::wal::apply_op_versioned_with(
                        &mut self.items,
                        op,
                        format,
                        2,
                        <String as crate::EtchKey>::from_bytes,
                    );
                }
            }
            Ok(())
        }

        fn apply_with_ctx(
            &mut self,
            ops: &[Op],
            ctx: &mut crate::ReplayContext<'_>,
        ) -> crate::Result<()> {
            for op in ops {
                if op.collection() == 0 {
                    let _ = crate::wal::apply_op_versioned_with_ctx(
                        &mut self.items,
                        op,
                        0,
                        2,
                        ctx,
                        <String as crate::EtchKey>::from_bytes,
                    );
                }
            }
            Ok(())
        }

        // NO migrations registered.

        fn to_snapshot(&self) -> crate::Result<crate::wal::SnapshotPayload> {
            let mut entries = Vec::new();
            for (k, v) in &self.items {
                entries.push(crate::wal::SnapshotEntry {
                    key: crate::EtchKey::to_bytes(k),
                    version: 2,
                    value: crate::wal::encode_msgpack_value(v)?,
                });
            }
            Ok(crate::wal::SnapshotPayload {
                schema_fingerprint: 0,
                collections: vec![crate::wal::CollectionSection {
                    collection_id: 0,
                    current_version: 2,
                    entries,
                }],
            })
        }

        fn from_snapshot(
            payload: crate::wal::SnapshotPayload,
            ctx: &mut crate::ReplayContext<'_>,
        ) -> crate::Result<Self>
        where
            Self: Sized,
        {
            let mut state = Self::default();
            for section in &payload.collections {
                if section.collection_id == 0 {
                    for entry in &section.entries {
                        if let Some(v) =
                            crate::wal::load_snapshot_entry::<CompactV2Item>(entry, 0, 2, ctx)
                            && let Ok(k) = <String as crate::EtchKey>::from_bytes(&entry.key)
                        {
                            state.items.insert(k, v);
                        }
                    }
                }
            }
            Ok(state)
        }
    }

    {
        let store =
            Store::<BrokenV2State, WalBackend<BrokenV2State>>::open_wal(state_dir.clone()).unwrap();
        // Migration missing → snapshot entry quarantined, not lost.
        assert!(store.read().items.is_empty(), "value not yet in live state");
        let quarantined = store.quarantined();
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].key, b"quarantinee");
    }

    // Phase C: ship the fix — CompactStateV2 has the 1->2 migration.
    // Reopen, retry_quarantine, value is recovered.
    {
        let store =
            Store::<CompactStateV2, WalBackend<CompactStateV2>>::open_wal(state_dir.clone())
                .unwrap();

        // Quarantine file persisted from phase B. On open_wal, the backend
        // loads quarantine.bin and *also* replays; the snapshot values
        // migrate successfully this time (migration is registered), so the
        // live state already has the value.
        //
        // Note: quarantine.bin may still hold the old entry (persisted from
        // phase B's close). retry_quarantine cleans it up.
        let recovered = store.retry_quarantine().unwrap();
        let _ = recovered; // may be 0 if replay already migrated from snapshot

        let state = store.read();
        let got = state.items.get("quarantinee").expect("recovered");
        assert_eq!(got.body, "preserve-me");
    }
}

/// Write v1 data, compact (so data is only in snapshot, WAL is empty),
/// reopen as v2 with migration. Per-value snapshot versioning must
/// surface the migrated value.
#[test]
fn test_compacted_v1_plus_no_wal_upgrade_to_v2() {
    use crate::IncrementalSave;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().to_path_buf();

    // Phase 1: write with v1 schema and force compaction.
    {
        let store =
            Store::<CompactStateV1, WalBackend<CompactStateV1>>::open_wal(state_dir.clone())
                .unwrap();
        store
            .write(|tx| {
                tx.items.put(
                    "x".into(),
                    CompactV1Item {
                        payload: "the-original-data".into(),
                    },
                )?;
                tx.items.put(
                    "y".into(),
                    CompactV1Item {
                        payload: "more-data".into(),
                    },
                )?;
                Ok(())
            })
            .unwrap();
        let live = store.read().clone();
        store.backend().snapshot(&live).unwrap();
    }

    // Phase 2: reopen as v2 with migration registered. Snapshot-only data
    // should survive the upgrade because each value carries its own version.
    {
        let store =
            Store::<CompactStateV2, WalBackend<CompactStateV2>>::open_wal(state_dir.clone())
                .unwrap();
        let state = store.read();
        let x = state
            .items
            .get("x")
            .expect("snapshot-migrated value present");
        assert_eq!(x.body, "the-original-data");
        assert_eq!(x.weight, 0);
        let y = state
            .items
            .get("y")
            .expect("snapshot-migrated value present");
        assert_eq!(y.body, "more-data");
    }
}
