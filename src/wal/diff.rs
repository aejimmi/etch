//! Diffable trait and helpers for computing minimal BTreeMap diffs.
//!
//! Computes the minimal set of `Op`s to transform `before` into `after`
//! by walking each BTreeMap in key order. O(changed keys), not O(total).

use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;

use super::op::Op;

/// A type that can produce diffs and apply them.
pub trait Diffable: Clone + Send + Sync + 'static {
    /// Compute the ops needed to go from `before` to `after`.
    fn diff(before: &Self, after: &Self) -> Vec<Op>;

    /// Apply ops to mutate `self` into the post-diff state.
    fn apply(&mut self, ops: &[Op]) -> crate::Result<()>;
}

/// Diff a single BTreeMap field, emitting Put/Delete ops.
pub fn diff_map<V: Serialize + PartialEq>(
    before: &BTreeMap<String, V>,
    after: &BTreeMap<String, V>,
    collection: u8,
    ops: &mut Vec<Op>,
) {
    // Walk both iterators in key order.
    let mut b_iter = before.iter().peekable();
    let mut a_iter = after.iter().peekable();

    loop {
        match (b_iter.peek(), a_iter.peek()) {
            (None, None) => break,
            (Some(_), None) => {
                // Remaining keys in before → deleted.
                for (k, _) in b_iter {
                    ops.push(Op::Delete {
                        collection,
                        key: k.clone(),
                    });
                }
                break;
            }
            (None, Some(_)) => {
                // Remaining keys in after → inserted.
                for (k, v) in a_iter {
                    ops.push(Op::Put {
                        collection,
                        key: k.clone(),
                        value: postcard::to_allocvec(v).expect("postcard serialize"),
                    });
                }
                break;
            }
            (Some((bk, _)), Some((ak, _))) => {
                use std::cmp::Ordering;
                match bk.cmp(ak) {
                    Ordering::Less => {
                        // Key exists in before but not after → deleted.
                        let (k, _) = b_iter.next().unwrap();
                        ops.push(Op::Delete {
                            collection,
                            key: k.clone(),
                        });
                    }
                    Ordering::Greater => {
                        // Key exists in after but not before → inserted.
                        let (k, v) = a_iter.next().unwrap();
                        ops.push(Op::Put {
                            collection,
                            key: k.clone(),
                            value: postcard::to_allocvec(v).expect("postcard serialize"),
                        });
                    }
                    Ordering::Equal => {
                        let (_, bv) = b_iter.next().unwrap();
                        let (k, av) = a_iter.next().unwrap();
                        if bv != av {
                            ops.push(Op::Put {
                                collection,
                                key: k.clone(),
                                value: postcard::to_allocvec(av).expect("postcard serialize"),
                            });
                        }
                    }
                }
            }
        }
    }
}

/// Apply a Put or Delete to a BTreeMap.
pub fn apply_op<V: DeserializeOwned>(map: &mut BTreeMap<String, V>, op: &Op) -> crate::Result<()> {
    match op {
        Op::Put { key, value, .. } => {
            let v: V = postcard::from_bytes(value)?;
            map.insert(key.clone(), v);
        }
        Op::Delete { key, .. } => {
            map.remove(key);
        }
    }
    Ok(())
}
