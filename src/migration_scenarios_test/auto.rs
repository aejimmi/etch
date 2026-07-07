use super::*;

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

        // Report counts must corroborate the state: exactly one op applied,
        // nothing skipped or quarantined.
        assert_eq!(ctx.report.applied_ops(), 1, "exactly one op applied");
        assert_eq!(
            ctx.report.total_skipped(),
            0,
            "clean auto-migration skips nothing"
        );

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

        // The anti-silent-corruption guarantee, sharpened: all four values were
        // applied (by name) and NONE were skipped — a positional decode that
        // mismatched would show up as a value-decode skip here.
        assert_eq!(ctx.report.applied_ops(), 4, "all four values applied");
        assert_eq!(
            ctx.report.total_skipped(),
            0,
            "name-based decode never skips"
        );

        // Each name maps to the same-named variant in V2, regardless of new index.
        assert_eq!(state.items.get("rect"), Some(&ShapeV2::Rectangle));
        assert_eq!(state.items.get("circle"), Some(&ShapeV2::Circle));
        assert_eq!(state.items.get("diamond"), Some(&ShapeV2::Diamond));
        assert_eq!(state.items.get("triangle"), Some(&ShapeV2::Triangle));
        assert!(quarantine.is_empty());
    }
}
