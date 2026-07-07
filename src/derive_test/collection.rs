//! `Collection` overlay behavior and `Op` accessor/diagnostic helpers.

use super::*;
use crate::wal::Collection;

// ---- Op::collection() ----

#[test]
fn op_collection_put() {
    let op = Op::Put {
        collection: 7,
        key: b"k".to_vec(),
        value: vec![],
    };
    assert_eq!(op.collection(), 7);
}

#[test]
fn op_collection_delete() {
    let op = Op::Delete {
        collection: 3,
        key: b"k".to_vec(),
    };
    assert_eq!(op.collection(), 3);
}

// ---- Collection::into_parts ----

#[test]
fn collection_into_parts_returns_ops_and_overlay() {
    let committed: BTreeMap<String, String> = BTreeMap::new();
    let mut col: Collection<String, String, _> = Collection::new(&committed, 5, 1);

    col.put("x".into(), "val".into()).unwrap();
    col.put("y".into(), "val2".into()).unwrap();

    let (ops, overlay) = col.into_parts();
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[0].collection(), 5);
    assert_eq!(ops[1].collection(), 5);
    assert_eq!(overlay.puts.len(), 2);
}

// ---- Collection::iter ----

#[test]
fn collection_iter_merges_committed_and_overlay() {
    let mut committed: BTreeMap<String, String> = BTreeMap::new();
    committed.insert("a".into(), "1".into());
    committed.insert("b".into(), "2".into());

    let mut col: Collection<String, String, _> = Collection::new(&committed, 0, 1);
    col.put("c".into(), "3".into()).unwrap();

    let pairs: Vec<_> = col.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(pairs.len(), 3);
    assert!(pairs.contains(&("a".into(), "1".into())));
    assert!(pairs.contains(&("b".into(), "2".into())));
    assert!(pairs.contains(&("c".into(), "3".into())));
}

#[test]
fn collection_iter_excludes_deleted() {
    let mut committed: BTreeMap<String, String> = BTreeMap::new();
    committed.insert("a".into(), "1".into());
    committed.insert("b".into(), "2".into());

    let mut col: Collection<String, String, _> = Collection::new(&committed, 0, 1);
    col.delete(&"a".into());

    let keys: Vec<_> = col.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec!["b".to_string()]);
}

// ---- Collection: delete nonexistent key ----

#[test]
fn collection_delete_nonexistent_returns_false() {
    let committed: BTreeMap<String, String> = BTreeMap::new();
    let mut col: Collection<String, String, _> = Collection::new(&committed, 0, 1);

    let existed = col.delete(&"nope".into());
    assert!(!existed);
    // Op is still emitted (WAL records the intent).
    let (ops, _) = col.into_parts();
    assert_eq!(ops.len(), 1);
}

// ---- Collection with HashMap committed ----

#[test]
fn collection_with_hashmap_committed() {
    let mut committed: HashMap<String, u64> = HashMap::new();
    committed.insert("x".into(), 10);

    let mut col: Collection<String, u64, _> = Collection::new(&committed, 0, 1);
    assert_eq!(*col.get(&"x".into()).unwrap(), 10);

    col.put("y".into(), 20).unwrap();
    assert_eq!(*col.get(&"y".into()).unwrap(), 20);
    assert!(col.contains(&"x".into()));
    assert!(col.contains(&"y".into()));
}

/// Verify that diagnostic helpers expose op metadata needed for good error
/// messages (field name is baked into the derive macro at compile time, so
/// we test the runtime-accessible pieces here).
#[test]
fn test_op_diagnostic_accessors() {
    let put = Op::Put {
        collection: 7,
        key: b"workspace-acme".to_vec(),
        value: vec![],
    };
    assert_eq!(put.collection(), 7);
    assert_eq!(put.key(), b"workspace-acme");
    assert_eq!(put.kind(), "PUT");

    let del = Op::Delete {
        collection: 3,
        key: b"u-42".to_vec(),
    };
    assert_eq!(del.kind(), "DELETE");
    assert_eq!(crate::wal::format_op_key(&del), "\"u-42\"");

    // Binary keys fall back to hex so control characters don't corrupt logs.
    let binary = Op::Put {
        collection: 0,
        key: vec![0x00, 0xff, 0x01],
        value: vec![],
    };
    assert_eq!(crate::wal::format_op_key(&binary), "0x00ff01");
}
