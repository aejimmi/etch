//! Performance benchmarks for etch.
//!
//! Not a usage example — see `hello.rs` and `contacts.rs` for that.
//!
//! Run with: `cargo bench`

use etch::{Op, Overlay, Replayable, Store, Transactable, WalBackend};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Instant;

// Schema

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    users: BTreeMap<String, User>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
    email: String,
    role: String,
    active: bool,
}

// Collection tag — identifies which BTreeMap an op belongs to.
const USERS: u8 = 0;

// Replayable — tells etch how to reconstruct state from WAL ops on startup.

impl Replayable for State {
    fn apply(&mut self, ops: &[Op]) -> etch::Result<()> {
        for op in ops {
            etch::apply_op(&mut self.users, op)?;
        }
        Ok(())
    }
}

// Transactable — defines the write API for your state.

struct StateTx<'a> {
    committed: &'a State,
    users: Overlay<User>,
    ops: Vec<Op>,
}

struct StateOverlay {
    users: Overlay<User>,
}

impl<'a> StateTx<'a> {
    fn get(&self, id: &str) -> Option<&User> {
        self.users.get(&self.committed.users, id)
    }

    fn insert(&mut self, user: User) {
        self.ops.push(Op::Put {
            collection: USERS,
            key: user.id.clone(),
            value: postcard::to_allocvec(&user).expect("serialize"),
        });
        self.users.put(user.id.clone(), user);
    }

    fn update(&mut self, id: &str, f: impl FnOnce(&mut User)) {
        if let Some(u) = self.users.get(&self.committed.users, id) {
            let mut u = u.clone();
            f(&mut u);
            self.ops.push(Op::Put {
                collection: USERS,
                key: id.to_string(),
                value: postcard::to_allocvec(&u).expect("serialize"),
            });
            self.users.put(id.to_string(), u);
        }
    }
}

impl Transactable for State {
    type Tx<'a> = StateTx<'a>;
    type Overlay = StateOverlay;

    fn begin_tx(&self) -> StateTx<'_> {
        StateTx {
            committed: self,
            users: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: StateTx<'_>) -> (Vec<Op>, StateOverlay) {
        (tx.ops, StateOverlay { users: tx.users })
    }

    fn apply_overlay(&mut self, overlay: StateOverlay) {
        etch::apply_overlay_map(&mut self.users, overlay.users);
    }
}

// Benchmark harness

fn make_user(i: usize) -> User {
    User {
        id: format!("u_{i}"),
        name: format!("User {i}"),
        email: format!("user{i}@example.com"),
        role: "editor".into(),
        active: true,
    }
}

fn bench(name: &str, iterations: u64, mut f: impl FnMut()) {
    // Warmup.
    for _ in 0..std::cmp::min(100, iterations) {
        f();
    }

    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = start.elapsed();
    let ops_sec = iterations as f64 / elapsed.as_secs_f64();
    println!("  {name:<30} {ops_sec:>12.0} ops/sec  ({elapsed:.2?})");
}

fn main() -> etch::Result<()> {
    println!("etch benchmark (run with --release)\n");

    let seed_size: usize = 1_000;

    // Seed a store.
    let store = Store::<State>::memory();
    store.write(|tx| {
        for i in 0..seed_size {
            tx.insert(make_user(i));
        }
        Ok(())
    })?;

    // Reads
    println!("Reads ({seed_size} records):");
    bench("read (RwLock + BTreeMap get)", 5_000_000, || {
        let state = store.read();
        std::hint::black_box(state.users.get("u_500"));
    });

    // Writes
    println!("\nWrites ({seed_size} records):");
    {
        let mut i = seed_size + 10_000;
        bench("insert", 100_000, || {
            store
                .write(|tx| {
                    tx.insert(make_user(i));
                    i += 1;
                    Ok(())
                })
                .unwrap();
        });

        let mut j = 0usize;
        bench("update", 100_000, || {
            store
                .write(|tx| {
                    let key = format!("u_{}", j % seed_size);
                    tx.update(&key, |u| u.name = format!("Updated {j}"));
                    j += 1;
                    Ok(())
                })
                .unwrap();
        });

        let mut k = 0usize;
        bench("read-then-write", 100_000, || {
            store
                .write(|tx| {
                    let key = format!("u_{}", k % seed_size);
                    std::hint::black_box(tx.get(&key));
                    tx.update(&key, |u| u.active = !u.active);
                    k += 1;
                    Ok(())
                })
                .unwrap();
        });
    }

    // WAL-backed
    println!("\nWAL-backed store (fsync per write call):");
    for (batch_size, batches) in [(1_000usize, 100u64), (100_000, 100), (1_000_000, 10)] {
        let dir = tempfile::tempdir()?;
        let wal_store = Store::<State, WalBackend<State>>::open_wal(dir.path().into())?;

        let mut n = 0usize;
        let start = Instant::now();
        for _ in 0..batches {
            wal_store.write(|tx| {
                for _ in 0..batch_size {
                    tx.insert(make_user(n));
                    n += 1;
                }
                Ok(())
            })?;
        }
        let elapsed = start.elapsed();
        let total = batches * batch_size as u64;
        let recs_sec = total as f64 / elapsed.as_secs_f64();
        let label = format!("{batch_size} inserts per fsync");
        println!(
            "  {label:<30} {recs_sec:>12.0} recs/sec ({total} records, {batches} fsyncs, {elapsed:.2?})"
        );

        bench("read after WAL writes", 2_000_000, || {
            let state = wal_store.read();
            std::hint::black_box(state.users.get("u_0"));
        });

        // Reopen and measure load time.
        let record_count = wal_store.read().users.len();
        let wal_dir = dir.path().to_path_buf();
        drop(wal_store);
        let start = Instant::now();
        let reopened = Store::<State, WalBackend<State>>::open_wal(wal_dir)?;
        let load_time = start.elapsed();
        assert_eq!(reopened.read().users.len(), record_count);
        println!(
            "  {:<30} {:>9} records  ({load_time:.2?})\n",
            "WAL reload", record_count
        );
    }

    println!("Done.");
    Ok(())
}
