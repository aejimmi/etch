use super::*;

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

        // Report corroborates the quarantine: nothing applied, and the
        // future-version value is counted as a value-decode skip.
        assert_eq!(
            ctx.report.applied_ops(),
            0,
            "future-version value is not applied"
        );
        assert_eq!(
            ctx.report.value_decode_skipped(),
            1,
            "quarantined future-version value counts as a value-decode skip"
        );

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
