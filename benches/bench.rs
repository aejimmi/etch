//! Performance benchmarks for etch.
//!
//! Not a usage example — see `hello.rs` and `contacts.rs` for that.
//!
//! Run with: `cargo bench`

#[cfg(feature = "async")]
use etchdb::AsyncStore;
use etchdb::{
    Op, Overlay, Replayable, Store, Transactable, WalBackend, apply_overlay_btree,
    apply_overlay_hash,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

// Schema — BTreeMap-based state

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

const USERS: u8 = 0;

impl Replayable for State {
    fn apply(&mut self, ops: &[Op]) -> etchdb::Result<()> {
        for op in ops {
            etchdb::apply_op(&mut self.users, op)?;
        }
        Ok(())
    }
}

struct StateTx<'a> {
    committed: &'a State,
    users: Overlay<String, User>,
    ops: Vec<Op>,
}

struct StateOverlay {
    users: Overlay<String, User>,
}

impl<'a> StateTx<'a> {
    fn get(&self, id: &str) -> Option<&User> {
        self.users.get(&self.committed.users, &id.to_string())
    }

    fn insert(&mut self, user: User) {
        self.ops.push(Op::Put {
            collection: USERS,
            key: user.id.as_bytes().to_vec(),
            value: postcard::to_allocvec(&user).expect("serialize"),
        });
        self.users.put(user.id.clone(), user);
    }

    fn update(&mut self, id: &str, f: impl FnOnce(&mut User)) {
        if let Some(u) = self.users.get(&self.committed.users, &id.to_string()) {
            let mut u = u.clone();
            f(&mut u);
            self.ops.push(Op::Put {
                collection: USERS,
                key: id.as_bytes().to_vec(),
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
        apply_overlay_btree(&mut self.users, overlay.users);
    }
}

// Schema — HashMap-based state

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HashState {
    users: HashMap<String, User>,
}

impl Replayable for HashState {
    fn apply(&mut self, ops: &[Op]) -> etchdb::Result<()> {
        for op in ops {
            etchdb::apply_op_hash(&mut self.users, op)?;
        }
        Ok(())
    }
}

struct HashStateTx<'a> {
    #[allow(dead_code)]
    committed: &'a HashState,
    users: Overlay<String, User>,
    ops: Vec<Op>,
}

struct HashStateOverlay {
    users: Overlay<String, User>,
}

impl<'a> HashStateTx<'a> {
    fn insert(&mut self, user: User) {
        self.ops.push(Op::Put {
            collection: USERS,
            key: user.id.as_bytes().to_vec(),
            value: postcard::to_allocvec(&user).expect("serialize"),
        });
        self.users.put(user.id.clone(), user);
    }
}

impl Transactable for HashState {
    type Tx<'a> = HashStateTx<'a>;
    type Overlay = HashStateOverlay;

    fn begin_tx(&self) -> HashStateTx<'_> {
        HashStateTx {
            committed: self,
            users: Overlay::new(),
            ops: Vec::new(),
        }
    }

    fn finish_tx(tx: HashStateTx<'_>) -> (Vec<Op>, HashStateOverlay) {
        (tx.ops, HashStateOverlay { users: tx.users })
    }

    fn apply_overlay(&mut self, overlay: HashStateOverlay) {
        apply_overlay_hash(&mut self.users, overlay.users);
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
    println!("  {name:<40} {ops_sec:>12.0} ops/sec  ({elapsed:.2?})");
}

fn main() -> etchdb::Result<()> {
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

    // =====================================================================
    // Reads
    // =====================================================================
    println!("Reads ({seed_size} records):");
    bench("read (RwLock + BTreeMap get)", 5_000_000, || {
        let state = store.read();
        std::hint::black_box(state.users.get("u_500"));
    });

    // =====================================================================
    // Writes (in-memory)
    // =====================================================================
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

    // =====================================================================
    // Key format overhead (Step 1 — Vec<u8> keys)
    // =====================================================================
    println!("\nKey format overhead:");
    {
        let n = 100_000u64;
        bench("Op::Put with into_bytes() key", n, || {
            let key = format!("u_{}", 42);
            std::hint::black_box(Op::Put {
                collection: 0,
                key: key.into_bytes(),
                value: vec![1, 2, 3],
            });
        });

        bench("Op::Put with as_bytes().to_vec() key", n, || {
            let key = "u_42";
            std::hint::black_box(Op::Put {
                collection: 0,
                key: key.as_bytes().to_vec(),
                value: vec![1, 2, 3],
            });
        });

        // apply_op with UTF-8 conversion
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        let op = Op::Put {
            collection: 0,
            key: b"bench_key".to_vec(),
            value: postcard::to_allocvec(&"bench_val".to_string()).unwrap(),
        };
        bench("apply_op (Vec<u8> → String)", n, || {
            etchdb::apply_op(&mut map, &op).unwrap();
        });

        // apply_op_bytes — no conversion
        let mut bmap: BTreeMap<Vec<u8>, String> = BTreeMap::new();
        bench("apply_op_bytes (no conversion)", n, || {
            etchdb::apply_op_bytes(&mut bmap, &op).unwrap();
        });
    }

    // =====================================================================
    // HashMap vs BTreeMap overlay (Step 2)
    // =====================================================================
    println!("\nHashMap vs BTreeMap overlay ({seed_size} records):");
    {
        // Build committed maps.
        let btree: BTreeMap<String, User> = (0..seed_size)
            .map(|i| (format!("u_{i}"), make_user(i)))
            .collect();
        let hashmap: HashMap<String, User> = (0..seed_size)
            .map(|i| (format!("u_{i}"), make_user(i)))
            .collect();

        let n = 500_000u64;

        bench("BTreeMap overlay get", n, || {
            let ov = Overlay::<String, User>::new();
            std::hint::black_box(ov.get(&btree, &"u_500".to_string()));
        });

        bench("HashMap overlay get", n, || {
            let ov = Overlay::<String, User>::new();
            std::hint::black_box(ov.get(&hashmap, &"u_500".to_string()));
        });

        // Merge benchmarks
        let n_merge = 10_000u64;
        bench("BTreeMap merge (overlay → committed)", n_merge, || {
            let mut target = btree.clone();
            let mut ov = Overlay::new();
            ov.put("u_0".into(), make_user(9999));
            ov.deletes.insert("u_999".into());
            apply_overlay_btree(&mut target, ov);
            std::hint::black_box(&target);
        });

        bench("HashMap merge (overlay → committed)", n_merge, || {
            let mut target = hashmap.clone();
            let mut ov = Overlay::new();
            ov.put("u_0".into(), make_user(9999));
            ov.deletes.insert("u_999".into());
            apply_overlay_hash(&mut target, ov);
            std::hint::black_box(&target);
        });

        // Full write cycle comparison
        let btree_store = Store::<State>::memory();
        btree_store.write(|tx| {
            for i in 0..seed_size {
                tx.insert(make_user(i));
            }
            Ok(())
        })?;

        let hash_store = Store::<HashState>::memory();
        hash_store.write(|tx| {
            for i in 0..seed_size {
                tx.insert(make_user(i));
            }
            Ok(())
        })?;

        let n_write = 50_000u64;
        let mut bi = seed_size + 100_000;
        bench("BTreeMap full write cycle", n_write, || {
            btree_store
                .write(|tx| {
                    tx.insert(make_user(bi));
                    bi += 1;
                    Ok(())
                })
                .unwrap();
        });

        let mut hi = seed_size + 100_000;
        bench("HashMap full write cycle", n_write, || {
            hash_store
                .write(|tx| {
                    tx.insert(make_user(hi));
                    hi += 1;
                    Ok(())
                })
                .unwrap();
        });
    }

    // =====================================================================
    // WAL-backed
    // =====================================================================
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
            "  {label:<40} {recs_sec:>12.0} recs/sec ({total} records, {batches} fsyncs, {elapsed:.2?})"
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
            "  {:<40} {:>9} records  ({load_time:.2?})\n",
            "WAL reload", record_count
        );
    }

    // =====================================================================
    // Async overhead (Step 5)
    // =====================================================================
    #[cfg(feature = "async")]
    {
        println!("Async vs sync overhead:");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let sync_store = Store::<State>::memory();
        sync_store.write(|tx| {
            for i in 0..seed_size {
                tx.insert(make_user(i));
            }
            Ok(())
        })?;
        let async_store = AsyncStore::from_store(Store::<State>::memory());
        rt.block_on(async {
            async_store
                .write(|tx| {
                    for i in 0..seed_size {
                        tx.insert(make_user(i));
                    }
                    Ok(())
                })
                .await
                .unwrap();
        });

        let n = 50_000u64;
        let mut si = seed_size + 200_000;
        bench("sync write (baseline)", n, || {
            sync_store
                .write(|tx| {
                    tx.insert(make_user(si));
                    si += 1;
                    Ok(())
                })
                .unwrap();
        });

        let mut ai = seed_size + 200_000;
        bench("async write (block_in_place)", n, || {
            rt.block_on(async {
                async_store
                    .write(|tx| {
                        tx.insert(make_user(ai));
                        ai += 1;
                        Ok(())
                    })
                    .await
                    .unwrap();
            });
        });
    }

    println!("Done.");
    Ok(())
}
