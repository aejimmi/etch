//! Hello etch — derive macro edition.
//!
//! Same todo list as `hello.rs` but using derive macros.
//! Compare: 20 lines of schema vs ~60 lines manual.
//!
//! Run with: `cargo run --example hello_derive`

use etchdb::{Replayable, Store, Transactable};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// Schema — derive generates Replayable, Transactable, Tx, and Overlay.

#[derive(Debug, Clone, Default, Serialize, Deserialize, Replayable, Transactable)]
struct Todos {
    #[etch(collection = 0)]
    items: BTreeMap<String, Todo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Todo {
    title: String,
    done: bool,
}

fn main() -> etchdb::Result<()> {
    let store = Store::<Todos>::memory();

    store.write(|tx| {
        tx.items.put(
            "1".into(),
            Todo {
                title: "Learn etch".into(),
                done: false,
            },
        );
        tx.items.put(
            "2".into(),
            Todo {
                title: "Build something".into(),
                done: false,
            },
        );
        Ok(())
    })?;

    let state = store.read();
    println!("Todos ({} items):", state.items.len());
    for (id, todo) in state.items.iter() {
        let check = if todo.done { "x" } else { " " };
        println!("  [{check}] {id}: {}", todo.title);
    }
    drop(state);

    // Update: read-then-write using Collection methods.
    store.write(|tx| {
        if let Some(t) = tx.items.get(&"1".into()).cloned() {
            tx.items.put("1".into(), Todo { done: true, ..t });
        }
        Ok(())
    })?;

    let state = store.read();
    println!("\nAfter completing \"Learn etch\":");
    for (id, todo) in state.items.iter() {
        let check = if todo.done { "x" } else { " " };
        println!("  [{check}] {id}: {}", todo.title);
    }

    Ok(())
}
