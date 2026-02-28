<p align="center">
  <img src="assets/etch-banner.png" alt="etch" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/tests-97_passed-brightgreen" alt="tests" />
  <img src="https://img.shields.io/badge/coverage-95.4%25-brightgreen" alt="coverage" />
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="license" />
  <img src="https://img.shields.io/badge/rust-2024_edition-orange?logo=rust" alt="rust edition" />
</p>

# etch

A fast, embedded database for Rust. 5 dependencies. No C code. No build scripts.

Etch is an embedded object-store database. Your Rust structs live in memory, reads are direct field access through an `RwLock`, and a WAL keeps everything crash-safe on disk. No SQL, no query engine — just your types, persisted and durable.

If you have structured application state and you're using SQLite or Turso for what's essentially a persistent `BTreeMap`, you're paying for a query engine you never query. Etch gives you a durable `RwLock<YourStruct>` instead.

## What it is

- An embedded database — durable, crash-safe storage and retrieval of structured data
- Reads are direct struct access behind an `RwLock` — no deserialization, no disk I/O
- Writes are atomic and crash-safe via WAL with xxh3 integrity checksums
- 1.7M durable writes/sec, 79M reads/sec
- 5 dependencies, pure Rust, compiles in seconds
- Rust-only by design — your data is your types. If you want language-agnostic access, use [Turso](https://turso.tech). If you want zero-overhead typed access from Rust, use etch.

## What it is not

- Not a SQL database — no query language, no query engine, no joins
- Data must fit in memory — your entire state lives in a struct
- Single-process — no replication, no networking, no multi-process access
- No schema migrations — you own your types, you own your versioning

## Quick start

```rust
// Open a file-backed store (or Store::<Music>::memory() for tests)
let store = Store::<Music, WalBackend<Music>>::open_wal("data/".into()).unwrap();

// Write
store.write(|tx| {
    tx.add("radiohead", Artist { name: "Radiohead".into(), genre: "alt rock".into() });
    tx.add("coltrane", Artist { name: "John Coltrane".into(), genre: "jazz".into() });
    Ok(())
}).unwrap();

// Read — direct struct access, no deserialization
let state = store.read();
assert_eq!(state.artists["coltrane"].name, "John Coltrane");
```

You define your schema as a Rust struct, then implement two traits:

- **`Replayable`** — one method. Tells etch how to reconstruct state from WAL ops on startup.
- **`Transactable`** — defines your transaction type with insert/update/delete methods.

See the full examples:

| Example | What it shows |
|---|---|
| [`hello`](examples/hello.rs) | In-memory todo list — minimal setup |
| [`contacts`](examples/contacts.rs) | Persistent contacts book — CRUD with WAL that survives restarts |

```sh
cargo run --example hello
cargo run --example contacts
```

## Features

- **Snapshot compaction** — WAL auto-compacts after a configurable threshold
- **Two flush modes** — immediate fsync or grouped batching for throughput
- **Zero-clone writes** — `Overlay` + `Transactable` captures changes without cloning state
- **Pluggable backends** — `WalBackend`, `PostcardBackend`, `NullBackend`, or bring your own
- **Corruption recovery** — truncates incomplete WAL entries, keeps valid prefix

## Performance

Apple M4 Pro, `--release`. Run yourself: `cargo bench`

| Operation | throughput |
|---|---|
| Read | 79M ops/sec |
| Insert (in-memory) | 2.4M ops/sec |
| Update (in-memory) | 2.2M ops/sec |
| WAL write (1K batch) | 220K recs/sec |
| WAL write (100K batch) | 1.7M recs/sec |
| WAL write (1M batch) | 1.7M recs/sec |
| WAL reload (10M records) | 3.8s |

WAL throughput plateaus at ~1.7M recs/sec — the ceiling is serialization + BTreeMap insertion, not fsync. Batch inserts into a single `write` call for maximum throughput.

## License

MIT
