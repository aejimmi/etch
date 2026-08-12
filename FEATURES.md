# Features

## Schema

- Derive macros — `#[derive(Replayable, Transactable)]` generates ~60 lines of boilerplate per state type from annotated fields.
- `#[etch(collection = N)]` — tag a `BTreeMap` or `HashMap` field as a persisted collection.
- `#[etch(version = V)]` — tag a collection's value type with a schema version; drives per-value migration on load.
- BTreeMap and HashMap collections — both supported out of the box via the same derive.
- Compile-time duplicate collection id check — invalid schemas fail to build.
- Typed transaction handle — `tx.field.get/put/delete` with compile-time key and value types.
- EtchKey trait — use `String`, `Vec<u8>`, `u8`..`u64`, `i8`..`i64`, `Ipv4Addr`, `Ipv6Addr`, `IpAddr`, or `(A, B)` tuples as collection keys.

## Storage

- Write-Ahead Log — appends only the diff per write, keeping the full state in memory.
- Snapshot compaction — WAL auto-compacts after a configurable threshold (default 1000 entries).
- Zstd compression — optional snapshot compression behind the `compression` feature.
- Versioned snapshot envelope — per-value schema versions preserved through compaction.
- MessagePack value encoding — named-field msgpack for forward-compatible value evolution.
- Exclusive database lock — `.lock` file with OS advisory lock; a second process opening the same directory gets a clear `DatabaseLocked` error with the holder's PID.
- Pluggable backends — `WalBackend` for durable storage, `NullBackend` for in-memory, or bring your own.

## Durability

- Immediate flush — every write fsyncs the WAL before returning (default).
- Grouped flush — background thread fsyncs at most every configured interval; writes coalesce for throughput.
- `write_durable` — force immediate fsync for critical writes regardless of flush policy.
- Atomic snapshot rotation — old WAL preserved as `wal.prev` until the new snapshot is confirmed loadable, so a crash mid-compaction recovers fully.
- xxh3 integrity checksums — every WAL entry is hashed; corrupt tails are detected and truncated.
- Corruption recovery — incomplete WAL entries are truncated at the last valid offset; valid prefix survives.
- Unreadable snapshot fallback — if the snapshot file is corrupt, it is preserved as `snapshot.backup` and the store rebuilds from WAL.
- `Store::checkpoint_to` — consistent copy of a live store; writes and compaction are held for the duration, so no acknowledged write is missing from the copy (a plain `cp` races compaction and can lose everything since the last snapshot).
- `CheckpointReport` — files, bytes, whether a snapshot was forced, and elapsed time; metadata only, safe to log.
- `WalBackend::inspect` — replay a store directory without writing a byte to it, returning the same `ReplayReport` a real open would, so a checkpoint or backup can be verified before it is needed.
- Serializable reports — `ReplayReport` and `CheckpointReport` are serde types, so a health check can hand its verdict to a supervisor or log pipeline.

## Schema Evolution

- Migration registry — register single-hop migration functions per `(collection, from_version)` via `MigrationSet`.
- Migration chains — multi-version evolution composes from single-hop functions (`v0 → v1 → v2 → ...`) walked automatically on load.
- Panic-safe migrations — each migration runs inside `catch_unwind`; a buggy migration cannot crash the process.
- Schema fingerprint drift warning — on load, a changed `(collection_id, version)` fingerprint without a matching migration prints a prominent warning (catches "bumped struct, forgot to bump version" bugs).
- Legacy format support — reads WAL v3 (postcard) and snapshot v1/v2 from etchdb < 0.4.0; writes v4/v3-msgpack.

## Quarantine

- Quarantine store — values that fail to migrate are preserved as raw bytes in `quarantine.bin` rather than silently dropped.
- Reason tracking — each quarantined entry records why recovery failed: missing migration, migration error, migration panic, future version, or decode failure.
- `store.quarantined()` — inspect all quarantined entries with reason, collection, key, and version.
- `store.retry_quarantine()` — retry migration for all quarantined entries against the current registry; recovered entries are written to the WAL and become visible to readers.
- Auto-retry on load — if migrations are registered that can drain existing quarantine entries, recovery happens at startup without explicit user action.
- `store.purge_quarantine()` — explicit drop of all quarantined entries (never automatic).
- Write supersedes quarantine — a normal write to a quarantined key removes the stale quarantine entry.
- Startup quarantine report — on load, a summary of quarantine contents is printed, grouped by failure mode.

## Async

- `AsyncStore` — wraps `Store` for use from tokio runtimes (behind `async` feature).
- `AsyncStore::open_wal` — open a WAL-backed store directly from an async context.
- Async `write` / `write_durable` / `flush` — all block via `tokio::task::block_in_place` to avoid blocking the reactor.

## Performance

- 79M reads per second — direct struct field access through `RwLock`, no deserialization.
- 2.4M in-memory inserts per second, 2.2M updates per second.
- 1.7M durable writes per second (100K per WAL commit, Apple M4 Pro, `--release`).
- WAL reload of 10M records in 3.8 seconds.
- Zero-clone writes — transaction overlay captures changes without cloning committed state; merge is O(changed keys), not O(total entries).
- Reads unblocked during persistence — the write lock is held only for the brief overlay merge.

## Dependencies

- 7 runtime dependencies, pure Rust, no C code, no build scripts.
- Compiles in seconds on a clean target.
