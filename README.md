<p align="center">
  <img src="assets/etch-banner.png" alt="etch" />
</p>

# etch

A fast, embedded persistent store for Rust. 5 dependencies. No C code. No build scripts.

We built etch for [Tell](https://tell.rs) after Turso/libsql bloated our binary and dependency tree for what was essentially CRUD on structured data. Your Rust structs live in memory, reads are direct field access, and a WAL keeps everything crash-safe on disk.

## What it is

- In-memory state with durable file-backed persistence
- Reads are direct struct access behind an `RwLock` — no deserialization, no disk I/O
- Writes are atomic and crash-safe via WAL with xxh3 integrity checksums
- 1.7M durable writes/sec, 32M reads/sec
- 5 dependencies, pure Rust, compiles in seconds

## What it is not

- Not a database — no SQL, no query engine, no joins
- Not for data larger than memory — your entire state lives in a struct
- No replication, no networking, no multi-process access
- No schema migrations — you own your types, you own your versioning

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

- **Snapshot compaction** — WAL auto-compacts after a configurable threshold
- **Two flush modes** — immediate fsync or grouped batching for throughput
- **Zero-clone writes** — `Overlay` + `Transactable` captures changes without cloning state
- **Pluggable backends** — `WalBackend`, `PostcardBackend`, `NullBackend`, or bring your own
- **Corruption recovery** — truncates incomplete WAL entries, keeps valid prefix

## Performance

Apple M4 Pro, `--release`. Run yourself: `cargo run --example bench --release`

| Operation | throughput |
|---|---|
| Read | 32M ops/sec |
| Insert (in-memory) | 2.3M ops/sec |
| Update (in-memory) | 2.0M ops/sec |
| WAL write (1K batch) | 219K recs/sec |
| WAL write (100K batch) | 1.5M recs/sec |
| WAL write (1M batch) | 1.7M recs/sec |
| WAL reload (10M records) | 3.9s |

WAL throughput plateaus at ~1.7M recs/sec — the ceiling is serialization + BTreeMap insertion, not fsync. Batch inserts into a single `write_tx` call for maximum throughput.

## License

MIT
