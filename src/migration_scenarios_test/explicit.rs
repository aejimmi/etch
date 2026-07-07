use super::*;

// =============================================================================
//   explicit/ — changes that need a user-provided migration.
// =============================================================================

/// Field renamed without `#[serde(alias)]` AND no `#[serde(default)]` on the
/// new field: decode fails, value quarantined. This documents the safer
/// default: msgpack requires all fields unless explicitly defaulted, so a
/// forgotten alias surfaces as a quarantine entry rather than silent
/// data loss.
mod explicit_rename_without_alias_quarantines {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        name: String,
        importance: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV2 {
        name: String,
        priority: u32, // renamed from `importance`, no alias, no default
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, ItemV2>,
    }

    #[test]
    fn renamed_field_without_alias_goes_to_quarantine() {
        let envelope = encode_versioned_value(
            1,
            &ItemV1 {
                name: "x".into(),
                importance: 99,
            },
        )
        .unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"x".to_vec(),
            value: envelope,
        }];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert!(state.items.is_empty(), "decode fails, not silently loaded");
        assert_eq!(quarantine.len(), 1);
        match &quarantine.entries()[0].reason {
            QuarantineReason::DecodeFailed { reason } => {
                assert!(reason.contains("missing field"), "got: {reason}");
            }
            other => panic!("expected DecodeFailed, got {:?}", other),
        }
    }
}

/// Field renamed WITH `#[serde(default)]` on the new field: decode
/// succeeds, but the old field's data is silently dropped. This is the
/// genuinely silent case — document that defaults + rename without alias
/// is a footgun that preserves the value shape while losing content.
mod explicit_rename_with_default_drops_data_silently {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        name: String,
        importance: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV2 {
        name: String,
        #[serde(default)]
        priority: u32, // default makes decode succeed; importance data is lost
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, ItemV2>,
    }

    #[test]
    fn rename_with_default_loses_old_data_without_warning() {
        let envelope = encode_versioned_value(
            1,
            &ItemV1 {
                name: "x".into(),
                importance: 99,
            },
        )
        .unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"x".to_vec(),
            value: envelope,
        }];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        let got = state.items.get("x").expect("decode succeeded");
        assert_eq!(got.name, "x");
        assert_eq!(got.priority, 0, "old importance data silently lost");
        assert!(
            quarantine.is_empty(),
            "no quarantine because decode 'succeeded' — footgun"
        );
    }
}

/// Remove an enum variant. Old data for the removed variant can't be
/// decoded and must be quarantined (or migrated).
mod explicit_enum_variant_removal {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum StatusV1 {
        Active,
        Suspended,
        Archived, // removed in V2
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
    enum StatusV2 {
        #[default]
        Active,
        Suspended,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, StatusV2>,
    }

    #[test]
    fn removed_variant_with_no_migration_quarantines() {
        let ops = vec![
            Op::Put {
                collection: 0,
                key: b"ok".to_vec(),
                value: encode_versioned_value(1, &StatusV1::Active).unwrap(),
            },
            Op::Put {
                collection: 0,
                key: b"archived".to_vec(),
                value: encode_versioned_value(1, &StatusV1::Archived).unwrap(),
            },
        ];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        // Good variant loads; removed variant is quarantined.
        assert_eq!(state.items.get("ok"), Some(&StatusV2::Active));
        assert!(!state.items.contains_key("archived"));
        assert_eq!(quarantine.len(), 1);
        assert_eq!(quarantine.entries()[0].key, b"archived");
    }
}

/// Enum variant renamed with `#[serde(alias)]` — old variant name still reads.
mod auto_enum_variant_rename_with_alias {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum StatusV1 {
        Active,
        Dormant, // v1 name
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
    enum StatusV2 {
        #[default]
        Active,
        #[serde(alias = "Dormant")]
        Idle, // renamed, alias preserves old name
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, StatusV2>,
    }

    #[test]
    fn renamed_variant_with_alias_reads_old_name() {
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: encode_versioned_value(1, &StatusV1::Dormant).unwrap(),
        }];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert_eq!(state.items.get("k"), Some(&StatusV2::Idle));
        assert!(quarantine.is_empty());
    }
}

/// Four-hop migration chain: v1 -> v2 -> v3 -> v4 -> v5.
mod explicit_four_hop_chain {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        value: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV5 {
        value: u32,
        doubled: u32,
        tripled: u32,
        quadrupled: u32,
        quintupled: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV5 {
        #[etch(collection = 0, version = 5)]
        items: BTreeMap<String, ItemV5>,
    }

    #[test]
    fn four_hop_chain_runs_all_hops() {
        let v1 = ItemV1 { value: 10 };
        let envelope = encode_versioned_value(1, &v1).unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: envelope,
        }];

        // Each migration adds one more field.
        let migrations = MigrationSet::new()
            .add(0, 1, 2, |bytes| {
                #[derive(Deserialize)]
                struct V1 {
                    value: u32,
                }
                #[derive(Serialize)]
                struct V2 {
                    value: u32,
                    doubled: u32,
                }
                let v: V1 = rmp_serde::from_slice(bytes)?;
                let doubled = v.value * 2;
                Ok(rmp_serde::to_vec_named(&V2 {
                    value: v.value,
                    doubled,
                })?)
            })
            .add(0, 2, 3, |bytes| {
                #[derive(Deserialize)]
                struct V2 {
                    value: u32,
                    doubled: u32,
                }
                #[derive(Serialize)]
                struct V3 {
                    value: u32,
                    doubled: u32,
                    tripled: u32,
                }
                let v: V2 = rmp_serde::from_slice(bytes)?;
                let tripled = v.value * 3;
                Ok(rmp_serde::to_vec_named(&V3 {
                    value: v.value,
                    doubled: v.doubled,
                    tripled,
                })?)
            })
            .add(0, 3, 4, |bytes| {
                #[derive(Deserialize)]
                struct V3 {
                    value: u32,
                    doubled: u32,
                    tripled: u32,
                }
                #[derive(Serialize)]
                struct V4 {
                    value: u32,
                    doubled: u32,
                    tripled: u32,
                    quadrupled: u32,
                }
                let v: V3 = rmp_serde::from_slice(bytes)?;
                let quadrupled = v.value * 4;
                Ok(rmp_serde::to_vec_named(&V4 {
                    value: v.value,
                    doubled: v.doubled,
                    tripled: v.tripled,
                    quadrupled,
                })?)
            })
            .add(0, 4, 5, |bytes| {
                #[derive(Deserialize)]
                struct V4 {
                    value: u32,
                    doubled: u32,
                    tripled: u32,
                    quadrupled: u32,
                }
                #[derive(Serialize)]
                struct V5 {
                    value: u32,
                    doubled: u32,
                    tripled: u32,
                    quadrupled: u32,
                    quintupled: u32,
                }
                let v: V4 = rmp_serde::from_slice(bytes)?;
                let quintupled = v.value * 5;
                Ok(rmp_serde::to_vec_named(&V5 {
                    value: v.value,
                    doubled: v.doubled,
                    tripled: v.tripled,
                    quadrupled: v.quadrupled,
                    quintupled,
                })?)
            });

        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV5::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        let got = state.items.get("k").expect("chain-migrated");
        assert_eq!(got.value, 10);
        assert_eq!(got.doubled, 20);
        assert_eq!(got.tripled, 30);
        assert_eq!(got.quadrupled, 40);
        assert_eq!(got.quintupled, 50);
        assert!(quarantine.is_empty());
    }
}
