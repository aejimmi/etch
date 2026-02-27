//! Tests for Overlay<V>.

use std::collections::BTreeMap;

use super::overlay::Overlay;

fn committed() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("a".into(), "alpha".into()),
        ("b".into(), "beta".into()),
        ("c".into(), "gamma".into()),
    ])
}

#[test]
fn get_falls_through_to_committed() {
    let c = committed();
    let ov = Overlay::<String>::new();
    assert_eq!(ov.get(&c, "a").unwrap(), "alpha");
    assert!(ov.get(&c, "z").is_none());
}

#[test]
fn get_returns_overlay_put() {
    let c = committed();
    let mut ov = Overlay::new();
    ov.put("a".into(), "ALPHA".into());
    assert_eq!(ov.get(&c, "a").unwrap(), "ALPHA");
}

#[test]
fn get_returns_none_for_deleted() {
    let c = committed();
    let mut ov = Overlay::<String>::new();
    ov.delete("b", &c);
    assert!(ov.get(&c, "b").is_none());
}

#[test]
fn contains_key_merged() {
    let c = committed();
    let mut ov = Overlay::new();
    ov.put("new".into(), "value".into());
    ov.delete("a", &c);
    assert!(!ov.contains_key(&c, "a"));
    assert!(ov.contains_key(&c, "b"));
    assert!(ov.contains_key(&c, "new"));
    assert!(!ov.contains_key(&c, "z"));
}

#[test]
fn values_merges_committed_and_overlay() {
    let c = committed();
    let mut ov = Overlay::new();
    ov.put("a".into(), "ALPHA".into()); // override
    ov.delete("c", &c); // delete
    ov.put("d".into(), "delta".into()); // new

    let mut vals: Vec<_> = ov.values(&c).cloned().collect();
    vals.sort();
    assert_eq!(vals, vec!["ALPHA", "beta", "delta"]);
}

#[test]
fn retain_marks_deletes_and_returns_keys() {
    let c = committed();
    let mut ov = Overlay::new();
    ov.put("d".into(), "delta".into());

    // Keep only values starting with 'a' or 'd'
    let removed = ov.retain(&c, |_, v| v.starts_with('a') || v.starts_with('d'));
    let mut removed_sorted = removed.clone();
    removed_sorted.sort();
    assert_eq!(removed_sorted, vec!["b", "c"]);

    // Verify merged view
    let mut vals: Vec<_> = ov.values(&c).cloned().collect();
    vals.sort();
    assert_eq!(vals, vec!["alpha", "delta"]);
}

#[test]
fn delete_then_put_same_key() {
    let c = committed();
    let mut ov = Overlay::new();
    ov.delete("a", &c);
    assert!(ov.get(&c, "a").is_none());

    ov.put("a".into(), "resurrected".into());
    assert_eq!(ov.get(&c, "a").unwrap(), "resurrected");
}

#[test]
fn iter_merges_both() {
    let c = committed();
    let mut ov = Overlay::new();
    ov.put("b".into(), "BETA".into());
    ov.delete("c", &c);

    let mut pairs: Vec<_> = ov
        .iter(&c)
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![("a".into(), "alpha".into()), ("b".into(), "BETA".into()),]
    );
}

#[test]
fn is_empty_on_fresh_overlay() {
    let ov = Overlay::<String>::new();
    assert!(ov.is_empty());
}

#[test]
fn retain_removes_overlay_put_that_shadows_committed() {
    // Bug scenario: committed has "a", tx updates "a" (overlay put),
    // retain removes it — committed "a" must NOT leak through.
    let c = committed();
    let mut ov = Overlay::new();
    ov.put("a".into(), "ALPHA_UPDATED".into()); // shadows committed "a"

    // Retain only values starting with 'b' — should remove "ALPHA_UPDATED"
    let removed = ov.retain(&c, |_, v| v.starts_with('b'));

    // "a" should be gone from the merged view (not fall through to committed)
    assert!(ov.get(&c, "a").is_none());
    // "b" (committed) and nothing else should remain
    let vals: Vec<_> = ov.values(&c).cloned().collect();
    assert_eq!(vals, vec!["beta"]);
    // "a", "c", and the overlay "a" should all be in removed
    assert!(removed.contains(&"a".to_string()));
    assert!(removed.contains(&"c".to_string()));
}
