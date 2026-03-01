//! Hello etch — the simplest possible example.
//!
//! An in-memory todo list.
//!
//! Run with: `cargo run --example hello`

use etchdb::{Op, Overlay, Store, Transactable, apply_overlay_btree};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// Schema

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Todos {
    items: BTreeMap<String, Todo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Todo {
    title: String,
    done: bool,
}

// Collection tag — identifies which BTreeMap an op belongs to.
// One const per collection. Only matters when your state has multiple maps.
const ITEMS: u8 = 0;

// Transactable — defines the write API for your state.
// Each method updates the in-memory overlay AND emits an Op for persistence.

struct TodoTx<'a> {
    committed: &'a Todos,
    items: Overlay<String, Todo>,
    ops: Vec<Op>,
}

struct TodoOverlay {
    items: Overlay<String, Todo>,
}

impl<'a> TodoTx<'a> {
    fn insert(&mut self, id: &str, todo: Todo) {
        self.ops.push(Op::Put {
            collection: ITEMS,
            key: id.as_bytes().to_vec(),
            value: postcard::to_allocvec(&todo).expect("serialize"),
        });
        self.items.put(id.to_string(), todo);
    }

    fn update(&mut self, id: &str, f: impl FnOnce(&mut Todo)) {
        if let Some(t) = self.items.get(&self.committed.items, &id.to_string()) {
            let mut t = t.clone();
            f(&mut t);
            self.ops.push(Op::Put {
                collection: ITEMS,
                key: id.as_bytes().to_vec(),
                value: postcard::to_allocvec(&t).expect("serialize"),
            });
            self.items.put(id.to_string(), t);
        }
    }
}

impl Transactable for Todos {
    type Tx<'a> = TodoTx<'a>;
    type Overlay = TodoOverlay;

    fn begin_tx(&self) -> TodoTx<'_> {
        TodoTx {
            committed: self,
            items: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: TodoTx<'_>) -> (Vec<Op>, TodoOverlay) {
        (tx.ops, TodoOverlay { items: tx.items })
    }

    fn apply_overlay(&mut self, overlay: TodoOverlay) {
        apply_overlay_btree(&mut self.items, overlay.items);
    }
}

fn main() -> etchdb::Result<()> {
    let store = Store::<Todos>::memory();

    store.write(|tx| {
        tx.insert(
            "1",
            Todo {
                title: "Learn etch".into(),
                done: false,
            },
        );
        tx.insert(
            "2",
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
    drop(state); // release read lock before writing

    store.write(|tx| {
        tx.update("1", |t| t.done = true);
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
