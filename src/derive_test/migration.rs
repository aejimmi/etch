//! Migration chains (single hop, two-hop chain, skipped hop) plus write- and
//! delete-supersede-quarantine semantics.

use super::*;

/// When we upgrade from v1 to v2 with a migration registered, the
/// migration chain runs per-op and produces correct v2 values.
#[test]
fn test_migrate_wal_v1_to_v2() {
    // Build a v1 op as if it had been written by an older binary.
    // (We still write with msgpack + envelope — the "old" part is the V type.)
    let v1 = MigItemV1 {
        name: "widget".into(),
        count: 42,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes()); // version 1
    envelope.extend_from_slice(&v1_bytes);

    let ops = vec![Op::Put {
        collection: 0,
        key: b"widget".to_vec(),
        value: envelope,
    }];

    // Without a migration, the v1 op should fail with SchemaVersionMismatch.
    let migrations_empty = crate::MigrationSet::new();
    let mut quarantine = crate::Quarantine::new();
    let mut ctx = crate::ReplayContext::new(
        crate::ReplayFormat::Versioned,
        &migrations_empty,
        &mut quarantine,
    );
    let mut state = MigStateV2::default();
    // apply_with_ctx routes internally and eprintln-skips on error, so state is empty.
    crate::Replayable::apply_with_ctx(&mut state, &ops, &mut ctx).unwrap();
    assert!(
        state.items.is_empty(),
        "no migration registered -> value should be skipped"
    );

    // With a registered migration 1 -> 2, the value arrives migrated.
    let migrations = crate::MigrationSet::new().add(0, 1, 2, |bytes| {
        let old: MigItemV1 = rmp_serde::from_slice(bytes)?;
        let new = MigItemV2 {
            name: old.name,
            count: old.count,
            label: None,
            priority: 0,
        };
        Ok(rmp_serde::to_vec_named(&new)?)
    });
    let mut quarantine = crate::Quarantine::new();
    let mut ctx =
        crate::ReplayContext::new(crate::ReplayFormat::Versioned, &migrations, &mut quarantine);
    let mut state = MigStateV2::default();
    crate::Replayable::apply_with_ctx(&mut state, &ops, &mut ctx).unwrap();

    let got = state.items.get("widget").expect("migrated value present");
    assert_eq!(got.name, "widget");
    assert_eq!(got.count, 42);
    assert_eq!(got.label, None);
    assert_eq!(got.priority, 0);
}

/// User skips v2 entirely — upgrades directly from v1 to v3. The migration
/// chain must run both 1->2 and 2->3 hops to reach the current version.
#[test]
fn test_chain_migrate_v1_to_v3() {
    let v1 = MigItemV1 {
        name: "thingy".into(),
        count: 7,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes());
    envelope.extend_from_slice(&v1_bytes);

    let ops = vec![Op::Put {
        collection: 0,
        key: b"thingy".to_vec(),
        value: envelope,
    }];

    // Register both hops.
    let migrations = crate::MigrationSet::new()
        .add(0, 1, 2, |bytes| {
            let old: MigItemV1 = rmp_serde::from_slice(bytes)?;
            let v2 = MigItemV2 {
                name: old.name,
                count: old.count,
                label: None,
                priority: 0,
            };
            Ok(rmp_serde::to_vec_named(&v2)?)
        })
        .add(0, 2, 3, |bytes| {
            let old: MigItemV2 = rmp_serde::from_slice(bytes)?;
            let v3 = MigItemV3 {
                name: old.name,
                count: old.count,
                label: old.label,
                priority: old.priority,
                tags: vec![],
            };
            Ok(rmp_serde::to_vec_named(&v3)?)
        });

    let mut quarantine = crate::Quarantine::new();
    let mut ctx =
        crate::ReplayContext::new(crate::ReplayFormat::Versioned, &migrations, &mut quarantine);
    let mut state = MigStateV3::default();
    crate::Replayable::apply_with_ctx(&mut state, &ops, &mut ctx).unwrap();

    let got = state
        .items
        .get("thingy")
        .expect("chain-migrated value present");
    assert_eq!(got.name, "thingy");
    assert_eq!(got.count, 7);
    assert_eq!(got.tags, Vec::<String>::new());
}

/// Skip v2 migration, only register 2->3. Value at v1 should be skipped
/// (no chain possible) without crashing.
#[test]
fn test_skip_v2_no_migration_skips_value() {
    let v1 = MigItemV1 {
        name: "thingy".into(),
        count: 7,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes());
    envelope.extend_from_slice(&v1_bytes);

    let ops = vec![Op::Put {
        collection: 0,
        key: b"thingy".to_vec(),
        value: envelope,
    }];

    // Only 2->3 registered — no path from 1.
    let migrations = crate::MigrationSet::new().add(0, 2, 3, |bytes| Ok(bytes.to_vec()));

    let mut quarantine = crate::Quarantine::new();
    let mut ctx =
        crate::ReplayContext::new(crate::ReplayFormat::Versioned, &migrations, &mut quarantine);
    let mut state = MigStateV3::default();
    crate::Replayable::apply_with_ctx(&mut state, &ops, &mut ctx).unwrap();

    // Value is NOT lost — it's quarantined with the reason recorded.
    assert!(state.items.is_empty());
    assert_eq!(quarantine.len(), 1);
    let entry = &quarantine.entries()[0];
    assert_eq!(entry.collection, 0);
    assert_eq!(entry.key, b"thingy");
    assert_eq!(entry.version, 1);
    assert!(matches!(
        entry.reason,
        crate::QuarantineReason::MissingMigration { from: 1, to: 2 }
    ));
}

#[test]
fn test_write_supersedes_quarantined_key() {
    // First seed the quarantine with a v1 value that can't migrate.
    let v1 = MigItemV1 {
        name: "oldie".into(),
        count: 99,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes());
    envelope.extend_from_slice(&v1_bytes);
    let quarantining_op = Op::Put {
        collection: 0,
        key: b"oldie".to_vec(),
        value: envelope,
    };

    let migrations_empty = crate::MigrationSet::new();
    let mut quarantine = crate::Quarantine::new();
    let mut ctx = crate::ReplayContext::new(
        crate::ReplayFormat::Versioned,
        &migrations_empty,
        &mut quarantine,
    );
    let mut state = MigStateV3::default();
    crate::Replayable::apply_with_ctx(&mut state, &[quarantining_op], &mut ctx).unwrap();
    assert_eq!(quarantine.len(), 1, "sanity: quarantine populated");

    // Now simulate a new write to the same key (current v3 format).
    let v3 = MigItemV3 {
        name: "new-value".into(),
        count: 1,
        label: None,
        priority: 0,
        tags: vec![],
    };
    let fresh = Op::Put {
        collection: 0,
        key: b"oldie".to_vec(),
        value: crate::wal::encode_versioned_value(3, &v3).unwrap(),
    };
    let mut ctx = crate::ReplayContext::new(
        crate::ReplayFormat::Versioned,
        &migrations_empty,
        &mut quarantine,
    );
    crate::Replayable::apply_with_ctx(&mut state, &[fresh], &mut ctx).unwrap();

    // New value in state, quarantine entry gone.
    assert_eq!(state.items.get("oldie").unwrap().name, "new-value");
    assert!(quarantine.is_empty(), "write supersedes quarantine entry");
}

#[test]
fn test_delete_removes_quarantined_entry() {
    let v1 = MigItemV1 {
        name: "goner".into(),
        count: 1,
    };
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&1u16.to_le_bytes());
    envelope.extend_from_slice(&v1_bytes);

    let migrations_empty = crate::MigrationSet::new();
    let mut quarantine = crate::Quarantine::new();
    let mut state = MigStateV3::default();

    // Quarantine populated.
    {
        let mut ctx = crate::ReplayContext::new(
            crate::ReplayFormat::Versioned,
            &migrations_empty,
            &mut quarantine,
        );
        crate::Replayable::apply_with_ctx(
            &mut state,
            &[Op::Put {
                collection: 0,
                key: b"goner".to_vec(),
                value: envelope,
            }],
            &mut ctx,
        )
        .unwrap();
    }
    assert_eq!(quarantine.len(), 1);

    // Delete removes quarantine entry too.
    {
        let mut ctx = crate::ReplayContext::new(
            crate::ReplayFormat::Versioned,
            &migrations_empty,
            &mut quarantine,
        );
        crate::Replayable::apply_with_ctx(
            &mut state,
            &[Op::Delete {
                collection: 0,
                key: b"goner".to_vec(),
            }],
            &mut ctx,
        )
        .unwrap();
    }
    assert!(quarantine.is_empty());
}
