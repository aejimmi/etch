//! Tests for MigrationSet and chain execution.

use super::*;

#[test]
fn single_hop_migrate_succeeds() {
    let set = MigrationSet::new().add(0, 1, 2, |bytes| {
        let mut out = bytes.to_vec();
        out.push(b'!');
        Ok(out)
    });

    match set.migrate_chain(0, 1, 2, b"hello") {
        ChainResult::Migrated(out) => assert_eq!(out, b"hello!"),
        other => panic!("expected Migrated, got {:?}", other),
    }
}

#[test]
fn chain_walks_multiple_hops() {
    let set = MigrationSet::new()
        .add(0, 1, 2, |bytes| {
            let mut out = bytes.to_vec();
            out.push(b'a');
            Ok(out)
        })
        .add(0, 2, 3, |bytes| {
            let mut out = bytes.to_vec();
            out.push(b'b');
            Ok(out)
        })
        .add(0, 3, 4, |bytes| {
            let mut out = bytes.to_vec();
            out.push(b'c');
            Ok(out)
        });

    match set.migrate_chain(0, 1, 4, b"x") {
        ChainResult::Migrated(out) => assert_eq!(out, b"xabc"),
        other => panic!("expected Migrated, got {:?}", other),
    }
}

#[test]
fn missing_hop_reports_which_version() {
    let set = MigrationSet::new().add(0, 1, 2, |b| Ok(b.to_vec()));
    // missing 2->3
    match set.migrate_chain(0, 1, 3, b"x") {
        ChainResult::Missing { hop } => assert_eq!(hop, 2),
        other => panic!("expected Missing, got {:?}", other),
    }
}

#[test]
fn missing_at_start_reports_start_hop() {
    let set = MigrationSet::new();
    match set.migrate_chain(0, 1, 2, b"x") {
        ChainResult::Missing { hop } => assert_eq!(hop, 1),
        other => panic!("expected Missing, got {:?}", other),
    }
}

#[test]
fn migrations_for_different_collections_are_independent() {
    let set = MigrationSet::new()
        .add(0, 1, 2, |b| Ok([b, b"-col0"].concat()))
        .add(1, 1, 2, |b| Ok([b, b"-col1"].concat()));

    match set.migrate_chain(0, 1, 2, b"x") {
        ChainResult::Migrated(out) => assert_eq!(out, b"x-col0"),
        other => panic!("expected Migrated, got {:?}", other),
    }
    match set.migrate_chain(1, 1, 2, b"x") {
        ChainResult::Migrated(out) => assert_eq!(out, b"x-col1"),
        other => panic!("expected Migrated, got {:?}", other),
    }
}

#[test]
fn migration_error_propagated_with_hop() {
    let set = MigrationSet::new()
        .add(0, 1, 2, |b| Ok(b.to_vec()))
        .add(0, 2, 3, |_| Err(MigrationError::new("bad data at hop 2")));

    match set.migrate_chain(0, 1, 3, b"x") {
        ChainResult::Failed { hop, error } => {
            assert_eq!(hop, 2);
            assert_eq!(error.0, "bad data at hop 2");
        }
        other => panic!("expected Failed, got {:?}", other),
    }
}

#[test]
fn migration_panic_is_caught_and_reported() {
    let set = MigrationSet::new().add(0, 1, 2, |_| panic!("boom at hop 1"));

    match set.migrate_chain(0, 1, 2, b"x") {
        ChainResult::Panicked { hop, message } => {
            assert_eq!(hop, 1);
            assert!(message.contains("boom at hop 1"), "got: {message}");
        }
        other => panic!("expected Panicked, got {:?}", other),
    }
}

#[test]
fn forward_migration_from_equal_version_is_missing() {
    let set = MigrationSet::new();
    match set.migrate_chain(0, 5, 5, b"x") {
        ChainResult::Missing { hop } => assert_eq!(hop, 5),
        other => panic!("expected Missing for no-op range, got {:?}", other),
    }
}

#[test]
fn backward_migration_is_missing() {
    let set = MigrationSet::new();
    match set.migrate_chain(0, 5, 3, b"x") {
        ChainResult::Missing { hop } => assert_eq!(hop, 5),
        other => panic!("expected Missing, got {:?}", other),
    }
}

#[test]
fn has_reports_registration_correctly() {
    let set = MigrationSet::new().add(3, 0, 1, |b| Ok(b.to_vec()));
    assert!(set.has(3, 0));
    assert!(!set.has(3, 1));
    assert!(!set.has(4, 0));
}

#[test]
#[should_panic(expected = "migrations must be single-hop")]
fn multi_hop_registration_panics() {
    MigrationSet::new().add(0, 1, 3, |b| Ok(b.to_vec()));
}
