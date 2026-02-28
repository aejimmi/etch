//! Tests for apply_overlay_map.

use std::collections::BTreeMap;

use super::merge::apply_overlay_map;
use super::overlay::Overlay;

#[test]
fn apply_puts_into_empty_map() {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let mut overlay = Overlay::new();
    overlay.put("a".into(), "alpha".into());
    overlay.put("b".into(), "beta".into());

    apply_overlay_map(&mut map, overlay);

    assert_eq!(map.get("a").unwrap(), "alpha");
    assert_eq!(map.get("b").unwrap(), "beta");
    assert_eq!(map.len(), 2);
}

#[test]
fn apply_deletes_from_map() {
    let mut map: BTreeMap<String, String> = BTreeMap::from([
        ("a".into(), "alpha".into()),
        ("b".into(), "beta".into()),
        ("c".into(), "gamma".into()),
    ]);

    let mut overlay = Overlay::<String>::new();
    overlay.deletes.insert("b".into());

    apply_overlay_map(&mut map, overlay);

    assert_eq!(map.len(), 2);
    assert!(!map.contains_key("b"));
    assert_eq!(map.get("a").unwrap(), "alpha");
}

#[test]
fn apply_puts_and_deletes_combined() {
    let mut map: BTreeMap<String, String> =
        BTreeMap::from([("a".into(), "alpha".into()), ("b".into(), "beta".into())]);

    let mut overlay = Overlay::new();
    overlay.deletes.insert("a".into());
    overlay.put("c".into(), "gamma".into());
    overlay.put("b".into(), "BETA".into()); // override

    apply_overlay_map(&mut map, overlay);

    assert!(!map.contains_key("a"));
    assert_eq!(map.get("b").unwrap(), "BETA");
    assert_eq!(map.get("c").unwrap(), "gamma");
    assert_eq!(map.len(), 2);
}

#[test]
fn apply_empty_overlay_is_noop() {
    let mut map: BTreeMap<String, String> = BTreeMap::from([("a".into(), "alpha".into())]);
    let original = map.clone();

    let overlay = Overlay::<String>::new();
    apply_overlay_map(&mut map, overlay);

    assert_eq!(map, original);
}

#[test]
fn apply_delete_nonexistent_key_is_ok() {
    let mut map: BTreeMap<String, String> = BTreeMap::from([("a".into(), "alpha".into())]);

    let mut overlay = Overlay::<String>::new();
    overlay.deletes.insert("z".into());

    apply_overlay_map(&mut map, overlay);

    assert_eq!(map.len(), 1);
    assert_eq!(map.get("a").unwrap(), "alpha");
}
