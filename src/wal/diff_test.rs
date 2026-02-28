//! Tests for apply_op and Replayable.

use std::collections::BTreeMap;

use super::diff::apply_op;
use super::op::Op;

fn put_op(key: &str, value: &str) -> Op {
    Op::Put {
        collection: 0,
        key: key.to_string(),
        value: postcard::to_allocvec(&value.to_string()).unwrap(),
    }
}

fn del_op(key: &str) -> Op {
    Op::Delete {
        collection: 0,
        key: key.to_string(),
    }
}

#[test]
fn apply_put_inserts_into_map() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    apply_op(&mut map, &put_op("k", "v")).unwrap();
    assert_eq!(map.get("k").unwrap(), "v");
}

#[test]
fn apply_put_overwrites_existing() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    map.insert("k".into(), "old".into());
    apply_op(&mut map, &put_op("k", "new")).unwrap();
    assert_eq!(map.get("k").unwrap(), "new");
}

#[test]
fn apply_delete_removes_key() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    map.insert("k".into(), "v".into());
    apply_op(&mut map, &del_op("k")).unwrap();
    assert!(!map.contains_key("k"));
}

#[test]
fn apply_delete_missing_key_is_ok() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    apply_op(&mut map, &del_op("nope")).unwrap();
    assert!(map.is_empty());
}

#[test]
fn apply_sequence_put_delete_put() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    apply_op(&mut map, &put_op("a", "1")).unwrap();
    apply_op(&mut map, &put_op("b", "2")).unwrap();
    apply_op(&mut map, &del_op("a")).unwrap();
    apply_op(&mut map, &put_op("c", "3")).unwrap();

    assert!(!map.contains_key("a"));
    assert_eq!(map.get("b").unwrap(), "2");
    assert_eq!(map.get("c").unwrap(), "3");
}

#[test]
fn apply_put_different_collections_same_key() {
    // apply_op ignores collection field for the BTreeMap helper,
    // so same key with different collections overwrites.
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let op1 = Op::Put {
        collection: 0,
        key: "k".into(),
        value: postcard::to_allocvec(&"from_0".to_string()).unwrap(),
    };
    let op2 = Op::Put {
        collection: 1,
        key: "k".into(),
        value: postcard::to_allocvec(&"from_1".to_string()).unwrap(),
    };
    apply_op(&mut map, &op1).unwrap();
    apply_op(&mut map, &op2).unwrap();
    assert_eq!(map.get("k").unwrap(), "from_1");
}

#[test]
fn apply_put_bad_payload_returns_error() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let op = Op::Put {
        collection: 0,
        key: "k".into(),
        value: vec![0xFF, 0xFE, 0xFD], // invalid postcard for String
    };
    assert!(apply_op(&mut map, &op).is_err());
}
