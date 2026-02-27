<p align="center">
  <img src="assets/etch-banner.png" alt="etch" />
</p>

# etch

A fast, embedded persistent store for Rust. No SQL. No ORM. No bloat.

## Why

We needed embedded storage for [Tell](https://tell.rs) and Turso/libsql was bloating our binary and dependency tree for what was essentially CRUD on structured data. So we built etch — 5 dependencies, no C code, no build scripts.

## What it is

In-memory state with durable file-backed persistence. Reads are direct struct access behind an `RwLock`. Writes are atomic and crash-safe. Reads never block on writes.

WAL with postcard binary serialization, two flush modes (immediate fsync, grouped batching).

## What it is not

Not a database. No SQL, no joins, no replication. It stores your Rust structs to disk and gets out of the way. If you need more, use SQLite.

## Quick start

```rust
use etch::{Store, Diffable, Op};
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AppState {
    items: BTreeMap<String, String>,
}

impl Diffable for AppState {
    fn diff(before: &Self, after: &Self) -> Vec<Op> {
        let mut ops = Vec::new();
        etch::diff_map(&before.items, &after.items, 0, &mut ops);
        ops
    }
    fn apply(&mut self, ops: &[Op]) -> etch::Result<()> {
        for op in ops {
            etch::apply_op(&mut self.items, op)?;
        }
        Ok(())
    }
}

// In-memory (tests)
let store = Store::<AppState>::memory();

// File-backed with WAL (production)
// let store = Store::<AppState, _>::open_wal("data/".into()).unwrap();

store.write(|state| {
    state.items.insert("key".into(), "value".into());
    Ok(())
}).unwrap();

let state = store.read();
assert_eq!(state.items["key"], "value");
```

## Features

- **Zero-copy reads** — `store.read()` returns an `RwLockReadGuard`, no cloning
- **Crash-safe writes** — WAL with xxh3 integrity, automatic corruption recovery
- **Snapshot compaction** — WAL auto-compacts after a configurable threshold
- **Two flush modes** — immediate fsync or grouped batching for throughput
- **Transaction capture** — `Overlay` + `Transactable` for zero-clone write paths
- **Pluggable backends** — `WalBackend`, `PostcardBackend`, `NullBackend`, or bring your own

## Performance

Apple M4 Pro, `--release`, 1,000 records seeded. Run yourself: `cargo run --example bench --release`

| Path | Operation | ops/sec |
|---|---|---|
| In-memory | Read (RwLock + BTreeMap) | 31,721,955 |
| In-memory | Insert (zero-clone tx) | 2,328,562 |
| In-memory | Update (zero-clone tx) | 2,005,985 |
| In-memory | Read-then-write (zero-clone tx) | 1,832,342 |
| WAL | 1K inserts per fsync | 218,870 recs/sec |
| WAL | 100K inserts per fsync | 1,528,951 recs/sec |
| WAL | 1M inserts per fsync | 1,682,808 recs/sec |
| WAL | Read (10M records) | 36,449,154 |
| WAL | Reload 10M from disk | 3.89s |

WAL write throughput plateaus at ~1.7M recs/sec — the ceiling is postcard serialization + BTreeMap insertion, not fsync. Batch your inserts into a single `write_tx` call. Use `FlushPolicy::Grouped` to decouple write latency from fsync entirely.

## License

MIT
