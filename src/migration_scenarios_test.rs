//! Comprehensive migration and schema-evolution scenario tests.
//!
//! Each scenario tests a specific shape of schema change, organized by
//! category. The goal is to prove the migration machinery behaves
//! correctly across the full matrix of things that can change between
//! releases — not just the happy path.
//!
//! Scenarios are grouped:
//!
//! - `auto`      — msgpack named mode should handle with NO migration.
//! - `explicit`  — requires a user-provided migration function.
//! - `failure`   — failure modes: panic, error, unknown variant, etc.
//! - `integration` — multi-collection, mixed-WAL, legacy-read, etc.
//!
//! Each scenario uses its own types inside a submodule so there's zero
//! cross-contamination between tests.

use crate::{
    CollectionSection, EtchKey, IncrementalSave, MigrationSet, Op, Quarantine, QuarantineReason,
    ReplayContext, ReplayFormat, Replayable, SnapshotEntry, SnapshotPayload, Store, Transactable,
    WalBackend, apply_op_versioned_with, apply_op_versioned_with_ctx, encode_msgpack_value,
    encode_versioned_value, load_snapshot_entry,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// =============================================================================
//   auto/ — changes msgpack handles automatically, no migration needed.
// =============================================================================

/// Add an `Option<T>` field to an existing type. Old values load with None.
mod auto_field_add_optional {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        name: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
    struct ItemV2 {
        name: String,
        #[serde(default)]
        nickname: Option<String>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, ItemV2>,
    }

    #[test]
    fn old_value_loads_with_new_field_defaulted() {
        let old = ItemV1 {
            name: "acme".into(),
        };
        let envelope = encode_versioned_value(1, &old).unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"acme".to_vec(),
            value: envelope,
        }];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        let got = state.items.get("acme").expect("old value loads");
        assert_eq!(got.name, "acme");
        assert_eq!(got.nickname, None, "new field defaults");
        assert!(quarantine.is_empty(), "no migration needed");
    }
}

/// Remove a field from an existing type. Old values load without it.
mod auto_field_remove {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        name: String,
        deprecated_field: Vec<String>,
        count: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
    struct ItemV2 {
        name: String,
        count: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, ItemV2>,
    }

    #[test]
    fn removed_field_is_ignored_on_read() {
        let old = ItemV1 {
            name: "widget".into(),
            deprecated_field: vec!["a".into(), "b".into(), "c".into()],
            count: 42,
        };
        let envelope = encode_versioned_value(1, &old).unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"widget".to_vec(),
            value: envelope,
        }];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        let got = state.items.get("widget").expect("loads");
        assert_eq!(got.name, "widget");
        assert_eq!(got.count, 42);
        assert!(quarantine.is_empty());
    }
}

/// Reorder fields within a struct. Msgpack named mode matches by name, not position.
mod auto_field_reorder {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        alpha: String,
        beta: u32,
        gamma: bool,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV2 {
        gamma: bool,
        alpha: String,
        beta: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, ItemV2>,
    }

    #[test]
    fn reordered_fields_decode_correctly() {
        let old = ItemV1 {
            alpha: "hi".into(),
            beta: 99,
            gamma: true,
        };
        let envelope = encode_versioned_value(1, &old).unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: envelope,
        }];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        let got = state.items.get("k").unwrap();
        assert_eq!(got.alpha, "hi");
        assert_eq!(got.beta, 99);
        assert!(got.gamma);
        assert!(quarantine.is_empty());
    }
}

/// Rename a field using `#[serde(alias)]`. Old data reads through the alias.
mod auto_field_rename_with_alias {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        old_name: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV2 {
        #[serde(alias = "old_name")]
        new_name: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, ItemV2>,
    }

    #[test]
    fn alias_reads_old_field_name() {
        let old = ItemV1 {
            old_name: "preserved".into(),
        };
        let envelope = encode_versioned_value(1, &old).unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: envelope,
        }];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert_eq!(state.items.get("k").unwrap().new_name, "preserved");
        assert!(quarantine.is_empty());
    }
}

/// Add an enum variant. Existing data (other variants) still loads.
mod auto_enum_variant_add {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum ColorV1 {
        Red,
        Green,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
    enum ColorV2 {
        #[default]
        Red,
        Green,
        Blue,
        Purple,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, ColorV2>,
    }

    #[test]
    fn existing_variants_still_load() {
        let envelope_red = encode_versioned_value(1, &ColorV1::Red).unwrap();
        let envelope_green = encode_versioned_value(1, &ColorV1::Green).unwrap();
        let ops = vec![
            Op::Put {
                collection: 0,
                key: b"r".to_vec(),
                value: envelope_red,
            },
            Op::Put {
                collection: 0,
                key: b"g".to_vec(),
                value: envelope_green,
            },
        ];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert_eq!(state.items.get("r"), Some(&ColorV2::Red));
        assert_eq!(state.items.get("g"), Some(&ColorV2::Green));
        assert!(quarantine.is_empty());
    }
}

/// Reorder enum variants. The SILENT CORRUPTION case under postcard that
/// msgpack named-encoding protects us against. This test is the
/// single most important guarantee the whole 0.4.0 change buys.
mod auto_enum_variant_reorder {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum ShapeV1 {
        Rectangle,
        Circle,
        Diamond,
        Triangle,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
    enum ShapeV2 {
        #[default]
        Rectangle,
        // Note: old `Circle` at index 1 is still present by name; new variants inserted at index 2+.
        Square,
        Circle,
        Pentagon,
        Diamond,
        Triangle,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, ShapeV2>,
    }

    #[test]
    fn reorder_does_not_silently_corrupt() {
        // Under postcard, V1::Circle (index 1) would have silently decoded
        // as V2::Square (new index 1). msgpack by name prevents this.
        let ops = vec![
            Op::Put {
                collection: 0,
                key: b"rect".to_vec(),
                value: encode_versioned_value(1, &ShapeV1::Rectangle).unwrap(),
            },
            Op::Put {
                collection: 0,
                key: b"circle".to_vec(),
                value: encode_versioned_value(1, &ShapeV1::Circle).unwrap(),
            },
            Op::Put {
                collection: 0,
                key: b"diamond".to_vec(),
                value: encode_versioned_value(1, &ShapeV1::Diamond).unwrap(),
            },
            Op::Put {
                collection: 0,
                key: b"triangle".to_vec(),
                value: encode_versioned_value(1, &ShapeV1::Triangle).unwrap(),
            },
        ];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        // Each name maps to the same-named variant in V2, regardless of new index.
        assert_eq!(state.items.get("rect"), Some(&ShapeV2::Rectangle));
        assert_eq!(state.items.get("circle"), Some(&ShapeV2::Circle));
        assert_eq!(state.items.get("diamond"), Some(&ShapeV2::Diamond));
        assert_eq!(state.items.get("triangle"), Some(&ShapeV2::Triangle));
        assert!(quarantine.is_empty());
    }
}

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

// =============================================================================
//   failure/ — migration failure modes.
// =============================================================================

/// Migration returns an error. Entry quarantined with MigrationFailed reason.
mod failure_migration_returns_error {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        value: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV2 {
        value: u32,
        derived: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, ItemV2>,
    }

    #[test]
    fn migration_error_is_recorded_in_quarantine_reason() {
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: encode_versioned_value(1, &ItemV1 { value: 5 }).unwrap(),
        }];

        let migrations = MigrationSet::new().add(0, 1, 2, |_bytes| {
            Err(crate::MigrationError::new("cannot compute derived field"))
        });

        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert!(state.items.is_empty());
        assert_eq!(quarantine.len(), 1);
        let entry = &quarantine.entries()[0];
        match &entry.reason {
            QuarantineReason::MigrationFailed { from, to, reason } => {
                assert_eq!(*from, 1);
                assert_eq!(*to, 2);
                assert!(reason.contains("cannot compute"), "got: {reason}");
            }
            other => panic!("expected MigrationFailed, got {:?}", other),
        }
    }
}

/// Migration panics. catch_unwind recovers; entry quarantined with MigrationPanicked.
mod failure_migration_panics {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV1 {
        value: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct ItemV2 {
        value: u32,
        extra: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV2 {
        #[etch(collection = 0, version = 2)]
        items: BTreeMap<String, ItemV2>,
    }

    #[test]
    fn migration_panic_is_quarantined_not_crashed() {
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: encode_versioned_value(1, &ItemV1 { value: 5 }).unwrap(),
        }];

        let migrations = MigrationSet::new().add(0, 1, 2, |_| panic!("boom in migration"));

        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV2::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert!(state.items.is_empty());
        assert_eq!(quarantine.len(), 1);
        match &quarantine.entries()[0].reason {
            QuarantineReason::MigrationPanicked { from, to, message } => {
                assert_eq!(*from, 1);
                assert_eq!(*to, 2);
                assert!(message.contains("boom"), "got: {message}");
            }
            other => panic!("expected MigrationPanicked, got {:?}", other),
        }
    }
}

/// Value stored at version N+1 opened by binary that only knows version N.
/// Should quarantine with `FromFutureVersion` reason.
mod failure_future_version {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Item {
        value: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct CurrentState {
        // Current binary is at version 1.
        #[etch(collection = 0, version = 1)]
        items: BTreeMap<String, Item>,
    }

    #[test]
    fn future_version_is_quarantined() {
        // Pretend a newer binary wrote a value tagged as version 5.
        let envelope = encode_versioned_value(5, &Item { value: 42 }).unwrap();
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: envelope,
        }];

        let migrations = MigrationSet::new();
        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = CurrentState::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert!(state.items.is_empty());
        assert_eq!(quarantine.len(), 1);
        match &quarantine.entries()[0].reason {
            QuarantineReason::FromFutureVersion { stored, current } => {
                assert_eq!(*stored, 5);
                assert_eq!(*current, 1);
            }
            other => panic!("expected FromFutureVersion, got {:?}", other),
        }
    }
}

/// Migration chain 1 -> 3, but only 2 -> 3 is registered. Value at v1
/// has no path forward. Should quarantine with MissingMigration at hop 1.
mod failure_chain_gap {
    use super::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct V1 {
        a: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct V3 {
        a: u32,
        b: u32,
        c: u32,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
    struct StateV3 {
        #[etch(collection = 0, version = 3)]
        items: BTreeMap<String, V3>,
    }

    #[test]
    fn gap_in_chain_quarantines_with_correct_hop() {
        let ops = vec![Op::Put {
            collection: 0,
            key: b"k".to_vec(),
            value: encode_versioned_value(1, &V1 { a: 5 }).unwrap(),
        }];

        // Only 2->3 registered — no path from 1.
        let migrations = MigrationSet::new().add(0, 2, 3, |bytes| Ok(bytes.to_vec()));

        let mut quarantine = Quarantine::new();
        let mut ctx = ReplayContext::new(ReplayFormat::Versioned, &migrations, &mut quarantine);
        let mut state = StateV3::default();
        state.apply_with_ctx(&ops, &mut ctx).unwrap();

        assert!(state.items.is_empty());
        assert_eq!(quarantine.len(), 1);
        match &quarantine.entries()[0].reason {
            QuarantineReason::MissingMigration { from, to } => {
                assert_eq!(*from, 1, "reports the missing hop");
                assert_eq!(*to, 2);
            }
            other => panic!("expected MissingMigration, got {:?}", other),
        }
    }
}

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
                    );
                    tx.users.put("bob".into(), UserV1 { name: "Bob".into() });
                    tx.boards.put(
                        "roadmap".into(),
                        BoardV1 {
                            title: "Q1 Roadmap".into(),
                        },
                    );
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
        let (_version, payload) = crate::split_versioned_value(&envelope).unwrap();
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
                    );
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
        let (_version, payload) = crate::split_versioned_value(&envelope).unwrap();
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
                );
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
        let (_v, payload) = crate::split_versioned_value(&envelope).unwrap();
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
        let (_v, payload) = crate::split_versioned_value(&envelope).unwrap();
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
                    tx.items.put("k".into(), Item { x: 1 });
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
                    tx.items.put("a".into(), Item { n: 1 });
                    tx.items.put("b".into(), Item { n: 2 });
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
                    tx.items.put("precompact".into(), Item { n: 42 });
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
                    tx.items.put("alpha".into(), Item { n: 10 });
                    tx.items.put("beta".into(), Item { n: 20 });
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
                    tx.items.put("x".into(), Item { n: 100 });
                    tx.items.put("y".into(), Item { n: 200 });
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
                    tx.items.put("k".into(), Item { n: 1 });
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
                tx.items.put("first".into(), Item { n: 1 });
                Ok(())
            })
            .unwrap();
        store.backend().snapshot(&store.read().clone()).unwrap();
        let size_1 = std::fs::metadata(state_dir.join("wal.prev")).unwrap().len();

        store
            .write(|tx| {
                tx.items.put("second".into(), Item { n: 2 });
                tx.items.put("third".into(), Item { n: 3 });
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
                    tx.items.put("k".into(), Item { x: 1 });
                    Ok(())
                })
                .unwrap();
            let live = store.read().clone();
            store.backend().snapshot(&live).unwrap();
        }

        // Reopen with StateB (same shape, different version). Fingerprint
        // mismatches; drift warning fires (stderr); load still succeeds.
        // No migration registered → entry quarantined.
        let store = Store::<StateB, WalBackend<StateB>>::open_wal(state_dir).unwrap();
        assert_eq!(store.quarantined().len(), 1);
        assert!(store.read().items.is_empty());
    }
}
