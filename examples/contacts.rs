//! Persistent contacts book — file-backed with WAL.
//!
//! Demonstrates CRUD with crash-safe persistence.
//! Data survives process restarts — run it twice to see.
//!
//! Run with: `cargo run --example contacts`

use etchdb::{Op, Overlay, Replayable, Store, Transactable, WalBackend, apply_overlay_btree};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

// Schema

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ContactBook {
    people: BTreeMap<String, Person>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Person {
    name: String,
    email: String,
    phone: String,
}

// Collection tag — identifies which BTreeMap an op belongs to.
const PEOPLE: u8 = 0;

// Replayable — tells etch how to reconstruct state from WAL ops on startup.
// One line per collection: route ops to the right BTreeMap.

impl Replayable for ContactBook {
    fn apply(&mut self, ops: &[Op]) -> etchdb::Result<()> {
        for op in ops {
            etchdb::apply_op(&mut self.people, op)?;
        }
        Ok(())
    }
}

// Transactable — defines the write API for your state.
// Each method updates the in-memory overlay AND emits an Op for persistence.

struct ContactTx<'a> {
    committed: &'a ContactBook,
    people: Overlay<String, Person>,
    ops: Vec<Op>,
}

struct ContactOverlay {
    people: Overlay<String, Person>,
}

impl<'a> ContactTx<'a> {
    fn insert(&mut self, id: &str, person: Person) {
        self.ops.push(Op::Put {
            collection: PEOPLE,
            key: id.as_bytes().to_vec(),
            value: postcard::to_allocvec(&person).expect("serialize"),
        });
        self.people.put(id.to_string(), person);
    }

    fn update(&mut self, id: &str, f: impl FnOnce(&mut Person)) {
        if let Some(p) = self.people.get(&self.committed.people, &id.to_string()) {
            let mut p = p.clone();
            f(&mut p);
            self.ops.push(Op::Put {
                collection: PEOPLE,
                key: id.as_bytes().to_vec(),
                value: postcard::to_allocvec(&p).expect("serialize"),
            });
            self.people.put(id.to_string(), p);
        }
    }

    fn delete(&mut self, id: &str) {
        self.ops.push(Op::Delete {
            collection: PEOPLE,
            key: id.as_bytes().to_vec(),
        });
        self.people.delete(&id.to_string(), &self.committed.people);
    }
}

impl Transactable for ContactBook {
    type Tx<'a> = ContactTx<'a>;
    type Overlay = ContactOverlay;

    fn begin_tx(&self) -> ContactTx<'_> {
        ContactTx {
            committed: self,
            people: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: ContactTx<'_>) -> (Vec<Op>, ContactOverlay) {
        (tx.ops, ContactOverlay { people: tx.people })
    }

    fn apply_overlay(&mut self, overlay: ContactOverlay) {
        apply_overlay_btree(&mut self.people, overlay.people);
    }
}

fn main() -> etchdb::Result<()> {
    let dir = PathBuf::from("data/contacts");
    std::fs::create_dir_all(&dir)?;

    let store = Store::<ContactBook, WalBackend<ContactBook>>::open_wal(dir.clone())?;
    println!(
        "Opened contact book ({} existing contacts)\n",
        store.read().people.len()
    );

    // Create
    store.write(|tx| {
        tx.insert(
            "alice",
            Person {
                name: "Alice Park".into(),
                email: "alice@example.com".into(),
                phone: "555-0101".into(),
            },
        );
        tx.insert(
            "bob",
            Person {
                name: "Bob Chen".into(),
                email: "bob@example.com".into(),
                phone: "555-0102".into(),
            },
        );
        tx.insert(
            "carol",
            Person {
                name: "Carol Diaz".into(),
                email: "carol@example.com".into(),
                phone: "555-0103".into(),
            },
        );
        Ok(())
    })?;
    println!("After adding 3 contacts:");
    print_contacts(&store);

    // Read
    let state = store.read();
    let alice = &state.people["alice"];
    println!("Lookup alice: {} <{}>\n", alice.name, alice.email);
    drop(state);

    // Update
    store.write(|tx| {
        tx.update("bob", |p| p.phone = "555-9999".into());
        Ok(())
    })?;
    println!("After updating Bob's phone:");
    print_contacts(&store);

    // Delete
    store.write(|tx| {
        tx.delete("carol");
        Ok(())
    })?;
    println!("After removing Carol:");
    print_contacts(&store);

    // Persistence — drop and reopen from disk.
    drop(store);
    let store = Store::<ContactBook, WalBackend<ContactBook>>::open_wal(dir)?;
    println!("Reopened from disk:");
    print_contacts(&store);

    Ok(())
}

fn print_contacts(store: &Store<ContactBook, WalBackend<ContactBook>>) {
    let state = store.read();
    for (id, p) in state.people.iter() {
        println!("  {id}: {} <{}> {}", p.name, p.email, p.phone);
    }
    println!();
}
