//! Benchmark for etch store operations.
//!
//! Run with: `cargo run --example bench --release`

use etch::{Diffable, Op, Overlay, Store, Transactable, WalBackend};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    users: BTreeMap<String, User>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct User {
    id: String,
    name: String,
    email: String,
    role: String,
    active: bool,
}

const USERS: u8 = 0;

// ---------------------------------------------------------------------------
// Diffable (clone+diff path — used by store.write())
// ---------------------------------------------------------------------------

impl Diffable for State {
    fn diff(before: &Self, after: &Self) -> Vec<Op> {
        let mut ops = Vec::new();
        etch::diff_map(&before.users, &after.users, USERS, &mut ops);
        ops
    }

    fn apply(&mut self, ops: &[Op]) -> etch::Result<()> {
        for op in ops {
            etch::apply_op(&mut self.users, op)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Transactable (zero-clone path — used by store.write_tx())
// ---------------------------------------------------------------------------

struct StateTx<'a> {
    committed: &'a State,
    users: Overlay<User>,
    ops: Vec<Op>,
}

struct StateTxOverlay {
    users: Overlay<User>,
}

impl<'a> StateTx<'a> {
    fn get_user(&self, id: &str) -> Option<&User> {
        self.users.get(&self.committed.users, id)
    }

    fn insert_user(&mut self, user: User) {
        self.ops.push(Op::Put {
            collection: USERS,
            key: user.id.clone(),
            value: postcard::to_allocvec(&user).expect("serialize"),
        });
        self.users.put(user.id.clone(), user);
    }

    fn update_user(&mut self, id: &str, mut f: impl FnMut(&mut User)) {
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
    type Overlay = StateTxOverlay;

    fn begin_tx(&self) -> StateTx<'_> {
        StateTx {
            committed: self,
            users: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: StateTx<'_>) -> (Vec<Op>, StateTxOverlay) {
        (tx.ops, StateTxOverlay { users: tx.users })
    }

    fn apply_overlay(&mut self, overlay: StateTxOverlay) {
        etch::apply_overlay_map(&mut self.users, overlay.users);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_user(i: usize) -> User {
    User {
        id: format!("u_{i}"),
        name: format!("User {i}"),
        email: format!("user{i}@example.com"),
        role: "editor".into(),
        active: true,
    }
}

fn bench<F: FnMut() -> etch::Result<()>>(name: &str, iterations: u64, mut f: F) -> Duration {
    // Warmup.
    for _ in 0..std::cmp::min(100, iterations) {
        f().unwrap();
    }

    let start = Instant::now();
    for _ in 0..iterations {
        f().unwrap();
    }
    let elapsed = start.elapsed();
    let ops_sec = iterations as f64 / elapsed.as_secs_f64();
    println!("  {name:<30} {ops_sec:>12.0} ops/sec  ({elapsed:.2?})");
    elapsed
}

fn main() {
    println!("etch benchmark (run with --release)\n");

    let seed_size: usize = 1_000;

    // Seed a store.
    let store = Store::<State>::memory();
    for i in 0..seed_size {
        store
            .write_tx(|tx| {
                tx.insert_user(make_user(i));
                Ok(())
            })
            .unwrap();
    }

    // --- Reads ---
    println!("Reads ({seed_size} records):");
    bench("read (RwLock + BTreeMap get)", 5_000_000, || {
        let state = store.read();
        std::hint::black_box(state.users.get("u_500"));
        Ok(())
    });

    // --- Zero-clone writes (write_tx) ---
    println!("\nWrites — zero-clone tx path ({seed_size} records):");
    {
        let mut i = seed_size + 10_000;
        bench("insert (write_tx)", 100_000, || {
            store.write_tx(|tx| {
                tx.insert_user(make_user(i));
                i += 1;
                Ok(())
            })
        });

        let mut j = 0u64;
        bench("update (write_tx)", 100_000, || {
            store.write_tx(|tx| {
                let key = format!("u_{}", j % seed_size as u64);
                tx.update_user(&key, |u| u.name = format!("Updated {j}"));
                j += 1;
                Ok(())
            })
        });

        let mut k = 0u64;
        bench("read-then-write (write_tx)", 100_000, || {
            store.write_tx(|tx| {
                let key = format!("u_{}", k % seed_size as u64);
                let _user = tx.get_user(&key);
                tx.update_user(&key, |u| u.active = !u.active);
                k += 1;
                Ok(())
            })
        });
    }

    // --- WAL-backed ---
    println!("\nWAL-backed store (fsync per write_tx call):");
    {
        for (batch_size, batches) in [(1_000usize, 100u64), (100_000, 100), (1_000_000, 10)] {
            let dir = tempfile::tempdir().unwrap();
            let store3 = Store::<State, WalBackend<State>>::open_wal(dir.path().into()).unwrap();
            let start = Instant::now();
            for _ in 0..batches {
                store3
                    .write_tx(|tx| {
                        for _ in 0..batch_size {
                            let i = tx.committed.users.len() + tx.users.puts.len();
                            tx.insert_user(make_user(i));
                        }
                        Ok(())
                    })
                    .unwrap();
            }
            let elapsed = start.elapsed();
            let total_records = batches * batch_size as u64;
            let recs_sec = total_records as f64 / elapsed.as_secs_f64();
            let label = format!("{batch_size} inserts per fsync");
            println!(
                "  {label:<30} {recs_sec:>12.0} recs/sec ({total_records} records, {batches} fsyncs, {elapsed:.2?})"
            );

            // Read performance after bulk load.
            bench("read after WAL writes", 2_000_000, || {
                let state = store3.read();
                std::hint::black_box(state.users.get("u_0"));
                Ok(())
            });

            // Reopen and measure load time.
            let total_records = store3.read().users.len();
            let wal_dir = dir.path().to_path_buf();
            drop(store3);
            let start = Instant::now();
            let store4 = Store::<State, WalBackend<State>>::open_wal(wal_dir).unwrap();
            let load_time = start.elapsed();
            let count = store4.read().users.len();
            assert_eq!(count, total_records);
            println!("  {:<30} {:>9} records  ({load_time:.2?})\n", "WAL reload", count);
        }
    }

    println!("\nDone.");
}
