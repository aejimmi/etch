use super::*;

// =============================================================================
//   integration/ — multi-collection, mixed-WAL, end-to-end scenarios.
// =============================================================================

/// Collection isolation: A changes schema, B doesn't. A's migration doesn't
/// affect B, and B's unchanged values load normally.
mod integration_collection_isolation {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct AV1 {
        x: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct AV2 {
        x: u32,
        y: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct B {
        s: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct State {
        #[etch(collection = 0, version = 2)]
        a: BTreeMap<String, AV2>,
        #[etch(collection = 1, version = 1)]
        b: BTreeMap<String, B>,
    }

    #[test]
    fn migration_on_a_does_not_affect_b() {
        let ops = vec![
            Op::Put {
                collection: 0,
                key: b"x".to_vec(),
                value: encode_versioned_value(1, &AV1 { x: 10 }).unwrap(),
            },
            Op::Put {
                collection: 1,
                key: b"y".to_vec(),
                value: encode_versioned_value(1, &B { s: "hi".into() }).unwrap(),
            },
        ];

        let migrations = MigrationSet::new().add(0, 1, 2, |bytes| {
            #[derive(Deserialize)]
            struct V1 {
                x: u32,
            }
            #[derive(Serialize)]
            struct V2 {
                x: u32,
                y: u32,
            }
            let v: V1 = rmp_serde::from_slice(bytes)?;
            Ok(rmp_serde::to_vec_named(&V2 { x: v.x, y: 0 })?)
        });

        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = State::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        // A was migrated.
        assert_eq!(state.a.get("x").unwrap().x, 10);
        // B loaded directly, no migration involved.
        assert_eq!(state.b.get("y").unwrap().s, "hi");
        assert!(quarantine.is_empty());
    }
}

/// Mixed WAL: some ops are at current version (direct decode), others are
/// at older versions (migration), interleaved.
mod integration_mixed_wal {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct V1 {
        n: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct V2 {
        n: u32,
        extra: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, V2>,
    }

    #[test]
    fn interleaved_versions_all_decode_correctly() {
        let ops = vec![
            Op::Put {
                collection: 0,
                key: b"old1".to_vec(),
                value: encode_versioned_value(1, &V1 { n: 1 }).unwrap(),
            },
            Op::Put {
                collection: 0,
                key: b"new1".to_vec(),
                value: encode_versioned_value(
                    2,
                    &V2 {
                        n: 100,
                        extra: "current".into(),
                    },
                )
                .unwrap(),
            },
            Op::Put {
                collection: 0,
                key: b"old2".to_vec(),
                value: encode_versioned_value(1, &V1 { n: 2 }).unwrap(),
            },
            Op::Put {
                collection: 0,
                key: b"new2".to_vec(),
                value: encode_versioned_value(
                    2,
                    &V2 {
                        n: 200,
                        extra: "also current".into(),
                    },
                )
                .unwrap(),
            },
        ];

        let migrations = MigrationSet::new().add(0, 1, 2, |bytes| {
            #[derive(Deserialize)]
            struct V1Local {
                n: u32,
            }
            #[derive(Serialize)]
            struct V2Local {
                n: u32,
                extra: String,
            }
            let v: V1Local = rmp_serde::from_slice(bytes)?;
            Ok(rmp_serde::to_vec_named(&V2Local {
                n: v.n,
                extra: "from migration".into(),
            })?)
        });

        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert_eq!(state.items.get("old1").unwrap().n, 1);
        assert_eq!(state.items.get("old1").unwrap().extra, "from migration");
        assert_eq!(state.items.get("new1").unwrap().n, 100);
        assert_eq!(state.items.get("new1").unwrap().extra, "current");
        assert_eq!(state.items.get("old2").unwrap().n, 2);
        assert_eq!(state.items.get("new2").unwrap().extra, "also current");
        assert!(quarantine.is_empty());
    }
}

/// Legacy WAL v3 path: values are raw postcard (no version envelope).
/// When we switch context to LegacyPostcard, values should decode correctly.
mod integration_legacy_postcard_wal {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Item {
        name: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct State {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, Item>,
    }

    #[test]
    fn legacy_postcard_values_decode_without_envelope() {
        // Simulate a v0.3.x WAL entry: raw postcard, no version prefix.
        let raw = postcard::to_allocvec(&Item {
            name: "legacy".into(),
        })
        .unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: raw,
        }];

        let mut state = State::default();
        // Use the format-only path since there's no version envelope to migrate through.
        state
            .apply_with_format(&ops, ReplayFormat::LegacyPostcard)
            .unwrap();

        assert_eq!(state.items.get("k").unwrap().name, "legacy");
    }
}

/// Empty collection in snapshot round-trips cleanly.
mod integration_empty_collection {
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
    fn empty_collection_snapshot_roundtrip() {
        let empty = State::default();
        let payload = empty.to_snapshot().unwrap();
        // Envelope present, zero entries.
        assert_eq!(payload.collections.len(), 1);
        assert_eq!(payload.collections[0].entries.len(), 0);

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let loaded = State::from_snapshot(payload, &mut ctx).unwrap();
        assert!(loaded.items.is_empty());
        assert!(quarantine.is_empty());
    }
}

/// Unknown collection id in snapshot (e.g. a collection was removed from
/// the state struct in the new release). Should be silently skipped, not
/// fail the load.
mod integration_unknown_collection_in_snapshot {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Item {
        x: u32,
    }

    // State only declares collection 0 — but the snapshot we'll feed it has
    // data for collection 99.
    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct State {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, Item>,
    }

    #[test]
    fn unknown_collection_is_ignored() {
        let payload = SnapshotPayload {
            schema_fingerprint: 0,
            collections: vec![
                // Known collection — should load.
                CollectionSection {
                    collection_id: 0,
                    current_version: 1,
                    entries: vec![SnapshotEntry {
                        key: b"a".to_vec(),
                        version: 1,
                        value: encode_msgpack_value(&Item { x: 1 }).unwrap(),
                    }],
                },
                // Unknown (removed) collection — should be ignored.
                CollectionSection {
                    collection_id: 99,
                    current_version: 1,
                    entries: vec![SnapshotEntry {
                        key: b"orphan".to_vec(),
                        version: 1,
                        value: vec![0xde, 0xad, 0xbe, 0xef],
                    }],
                },
            ],
        };

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let state = State::from_snapshot(payload, &mut ctx).unwrap();

        assert_eq!(state.items.get("a").unwrap().x, 1);
        // Unknown collection ignored, nothing quarantined.
        assert!(quarantine.is_empty());
    }
}
