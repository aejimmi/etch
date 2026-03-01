//! Tests for apply_op and Replayable.

use std::collections::{BTreeMap, HashMap};

use super::diff::{
    apply_op, apply_op_bytes, apply_op_hash, apply_op_hash_bytes, apply_op_hash_with, apply_op_with,
};
use super::op::Op;

fn put_op(key: &str, value: &str) -> Op {
    Op::Put {
        collection: 0,
        key: key.as_bytes().to_vec(),
        value: postcard::to_allocvec(&value.to_string()).unwrap(),
    }
}

fn del_op(key: &str) -> Op {
    Op::Delete {
        collection: 0,
        key: key.as_bytes().to_vec(),
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
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let op1 = Op::Put {
        collection: 0,
        key: b"k".to_vec(),
        value: postcard::to_allocvec(&"from_0".to_string()).unwrap(),
    };
    let op2 = Op::Put {
        collection: 1,
        key: b"k".to_vec(),
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
        key: b"k".to_vec(),
        value: vec![0xFF, 0xFE, 0xFD],
    };
    assert!(apply_op(&mut map, &op).is_err());
}

#[test]
fn apply_op_bytes_put_and_delete() {
    let mut map: BTreeMap<Vec<u8>, String> = BTreeMap::new();
    let op = Op::Put {
        collection: 0,
        key: b"key1".to_vec(),
        value: postcard::to_allocvec(&"val1".to_string()).unwrap(),
    };
    apply_op_bytes(&mut map, &op).unwrap();
    assert_eq!(map.get(b"key1".as_slice()).unwrap(), "val1");

    let del = Op::Delete {
        collection: 0,
        key: b"key1".to_vec(),
    };
    apply_op_bytes(&mut map, &del).unwrap();
    assert!(map.is_empty());
}

#[test]
fn apply_op_invalid_utf8_key_errors() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let op = Op::Put {
        collection: 0,
        key: vec![0xFF, 0xFE],
        value: postcard::to_allocvec(&"v".to_string()).unwrap(),
    };
    let result = apply_op(&mut map, &op);
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("invalid UTF-8"),
        "should report UTF-8 error"
    );
}

// ---- apply_op_hash (HashMap<String, V>) ----

#[test]
fn apply_op_hash_put_and_delete() {
    let mut map: HashMap<String, String> = HashMap::new();
    apply_op_hash(&mut map, &put_op("a", "1")).unwrap();
    apply_op_hash(&mut map, &put_op("b", "2")).unwrap();
    assert_eq!(map.get("a").unwrap(), "1");
    assert_eq!(map.get("b").unwrap(), "2");

    apply_op_hash(&mut map, &del_op("a")).unwrap();
    assert!(!map.contains_key("a"));
    assert_eq!(map.len(), 1);
}

#[test]
fn apply_op_hash_overwrites_existing() {
    let mut map: HashMap<String, String> = HashMap::new();
    apply_op_hash(&mut map, &put_op("k", "old")).unwrap();
    apply_op_hash(&mut map, &put_op("k", "new")).unwrap();
    assert_eq!(map.get("k").unwrap(), "new");
}

#[test]
fn apply_op_hash_invalid_utf8() {
    let mut map: HashMap<String, String> = HashMap::new();
    let op = Op::Put {
        collection: 0,
        key: vec![0xFF],
        value: postcard::to_allocvec(&"v".to_string()).unwrap(),
    };
    assert!(apply_op_hash(&mut map, &op).is_err());
}

#[test]
fn apply_op_hash_delete_invalid_utf8() {
    let mut map: HashMap<String, String> = HashMap::new();
    let op = Op::Delete {
        collection: 0,
        key: vec![0xFF],
    };
    assert!(apply_op_hash(&mut map, &op).is_err());
}

// ---- apply_op_hash_bytes (HashMap<Vec<u8>, V>) ----

#[test]
fn apply_op_hash_bytes_put_and_delete() {
    let mut map: HashMap<Vec<u8>, String> = HashMap::new();
    let op = Op::Put {
        collection: 0,
        key: vec![1, 2, 3],
        value: postcard::to_allocvec(&"val".to_string()).unwrap(),
    };
    apply_op_hash_bytes(&mut map, &op).unwrap();
    assert_eq!(map.get(&vec![1, 2, 3]).unwrap(), "val");

    let del = Op::Delete {
        collection: 0,
        key: vec![1, 2, 3],
    };
    apply_op_hash_bytes(&mut map, &del).unwrap();
    assert!(map.is_empty());
}

#[test]
fn apply_op_hash_bytes_delete_missing_is_ok() {
    let mut map: HashMap<Vec<u8>, String> = HashMap::new();
    let del = Op::Delete {
        collection: 0,
        key: vec![99],
    };
    apply_op_hash_bytes(&mut map, &del).unwrap();
}

// ---- apply_op_with (BTreeMap with custom key converter) ----

#[test]
fn apply_op_with_custom_key() {
    let mut map: BTreeMap<u32, String> = BTreeMap::new();
    let op = Op::Put {
        collection: 0,
        key: 42u32.to_le_bytes().to_vec(),
        value: postcard::to_allocvec(&"answer".to_string()).unwrap(),
    };
    apply_op_with(&mut map, &op, |bytes| {
        let arr: [u8; 4] = bytes.try_into().map_err(|_| crate::Error::WalCorrupted {
            offset: 0,
            reason: "bad key length".into(),
        })?;
        Ok(u32::from_le_bytes(arr))
    })
    .unwrap();
    assert_eq!(map.get(&42).unwrap(), "answer");
}

#[test]
fn apply_op_with_delete() {
    let mut map: BTreeMap<u32, String> = BTreeMap::new();
    map.insert(7, "seven".into());

    let del = Op::Delete {
        collection: 0,
        key: 7u32.to_le_bytes().to_vec(),
    };
    apply_op_with(&mut map, &del, |bytes| {
        let arr: [u8; 4] = bytes.try_into().map_err(|_| crate::Error::WalCorrupted {
            offset: 0,
            reason: "bad key length".into(),
        })?;
        Ok(u32::from_le_bytes(arr))
    })
    .unwrap();
    assert!(map.is_empty());
}

#[test]
fn apply_op_with_converter_error() {
    let mut map: BTreeMap<u32, String> = BTreeMap::new();
    let op = Op::Put {
        collection: 0,
        key: vec![1], // too short for u32
        value: postcard::to_allocvec(&"v".to_string()).unwrap(),
    };
    let result = apply_op_with(&mut map, &op, |bytes| {
        let arr: [u8; 4] = bytes.try_into().map_err(|_| crate::Error::WalCorrupted {
            offset: 0,
            reason: "bad key length".into(),
        })?;
        Ok(u32::from_le_bytes(arr))
    });
    assert!(result.is_err());
}

// ---- apply_op Delete with invalid UTF-8 (BTreeMap) ----

#[test]
fn apply_op_delete_invalid_utf8() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let op = Op::Delete {
        collection: 0,
        key: vec![0xFF, 0xFE],
    };
    let result = apply_op(&mut map, &op);
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("invalid UTF-8"),
        "should report UTF-8 error for Delete"
    );
}

// ---- apply_op_hash_with (HashMap with custom key converter) ----

#[test]
fn apply_op_hash_with_custom_key() {
    let mut map: HashMap<u64, String> = HashMap::new();
    let op = Op::Put {
        collection: 0,
        key: 100u64.to_le_bytes().to_vec(),
        value: postcard::to_allocvec(&"hundred".to_string()).unwrap(),
    };
    apply_op_hash_with(&mut map, &op, |bytes| {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| crate::Error::WalCorrupted {
            offset: 0,
            reason: "bad key length".into(),
        })?;
        Ok(u64::from_le_bytes(arr))
    })
    .unwrap();
    assert_eq!(map.get(&100).unwrap(), "hundred");
}

#[test]
fn apply_op_hash_with_delete() {
    let mut map: HashMap<u64, String> = HashMap::new();
    map.insert(5, "five".into());

    let del = Op::Delete {
        collection: 0,
        key: 5u64.to_le_bytes().to_vec(),
    };
    apply_op_hash_with(&mut map, &del, |bytes| {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| crate::Error::WalCorrupted {
            offset: 0,
            reason: "bad key length".into(),
        })?;
        Ok(u64::from_le_bytes(arr))
    })
    .unwrap();
    assert!(map.is_empty());
}
