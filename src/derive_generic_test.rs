//! Compile + smoke test: `#[etch]` on a *generic* struct.
//!
//! Exercises the derive's generic propagation — the generated `Tx` / `Overlay`
//! structs and the `Replayable` / `Transactable` impls must carry the input's
//! generics and the implied bounds (`V: Serialize + DeserializeOwned`,
//! `Self: Default`) or this file would not compile.

use crate::Store;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// The framework bounds (`Clone + Send + Sync + 'static`) come from the
// `Transactable` supertrait and must be supplied by the user's value type;
// the derive contributes the field-implied `Serialize + DeserializeOwned` and
// `Self: Default` bounds automatically.
#[derive(Clone, Default, Serialize, Deserialize, crate::Replayable, crate::Transactable)]
struct GenericState<V>
where
    V: Clone + Default + Send + Sync + 'static,
{
    #[etch(collection = 0)]
    items: BTreeMap<String, V>,
}

#[test]
fn test_generic_etch_struct_writes_and_reads() {
    let store = Store::<GenericState<u64>>::memory();
    store
        .write(|tx| {
            tx.items.put("a".into(), 7u64)?;
            tx.items.put("b".into(), 9u64)?;
            Ok(())
        })
        .unwrap();

    let state = store.read();
    assert_eq!(state.items.get("a"), Some(&7));
    assert_eq!(state.items.get("b"), Some(&9));
}

#[test]
fn test_generic_etch_struct_second_instantiation() {
    // A second, distinct type argument proves the impls are genuinely generic.
    let store = Store::<GenericState<String>>::memory();
    store
        .write(|tx| {
            tx.items.put("k".into(), "value".to_string())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store.read().items.get("k").map(String::as_str),
        Some("value")
    );
}
