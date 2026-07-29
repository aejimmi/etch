# Changelog

## v0.6.0

The backup release: copy a live store safely, and prove the copy is whole before you need it.

New:
- checkpoint: Store::checkpoint_to writes a consistent copy of a live store — blocks writes and compaction during the copy, so no acknowledged write is missing (a plain cp races compaction and can silently lose everything since the last snapshot)
- checkpoint: returns a CheckpointReport — files, bytes, whether a snapshot was forced, elapsed; metadata only, safe to log
- inspect: WalBackend::inspect replays a store directory without writing a byte to it and returns the same ReplayReport a real open would — verify a checkpoint or backup before you need it
- report: ReplayReport and CheckpointReport are serde-serializable, so a health check can hand its verdict to a supervisor or log pipeline
- migration: MigrationSet::hops() enumerates registered migrations, so tests can require a fixture per hop

Fix:
- wal: compaction commits the new snapshot before rotating the WAL — the old order had a crash window where a generation of acknowledged ops existed nowhere on disk
- wal: load no longer deletes wal.prev after replay; only the next committed snapshot supersedes it
- wal: foreground and background compaction share one exclusion — no more interleaved temp snapshots or double rotation
- store: retry_quarantine is a real write — gated, durable, ordered behind pending ops, so it can't race a checkpoint or vanish on crash

## v0.5.0

New:
- report: ReplayReport records everything the loader skipped, quarantined, or discarded — exact counts per anomaly class, snapshot status, schema drift, and a one-line summary(); replaces 13 eprintln! sites on the load path
- report: Store::open_wal_with_report returns the report alongside the store; Store::replay_report() keeps the most recent load's report queryable for the store's lifetime
- strict: Store::open_wal_strict aborts the load with Error::ReplayLoss instead of silently skipping past recoverable data loss; LoadMode selects lenient or strict at the backend level
- lock: deadlock detector on the state lock — write() and write_durable() return Error::LockTimeout after a 30s budget (tunable via set_lock_deadlock_timeout), read() panics with call-site diagnostics instead of hanging forever
- store: read_with() closure-scoped read access — the guard can't leak past the closure, so it can't starve writers
- store: last_flush_error() exposes the most recent grouped-mode background flush failure without issuing a write
- store: Immediate-mode writes now auto-compact at the snapshot threshold (previously grouped mode only)
- derive: schema fingerprint is shape-aware — key/value type tokens feed the hash, so swapping a field's value type flips the fingerprint even without a version bump
- test: crash-injection harness — child processes abort at deterministic points via ETCHDB_CRASH_POINT; parent asserts no acknowledged write is lost

Fix:
- wal: compaction fsyncs the directory in the correct order around the snapshot rename, closing a crash window where acknowledged writes could vanish after WAL reset
- wal: load-time truncation of a torn tail now re-seeks the live writer — a subsequent append no longer leaves a sparse hole that the next boot read as corruption
- wal: quarantine writes go through tmp file + fsync + rename, so a torn quarantine write can't corrupt previously preserved values
- store: non-WAL save clones state and persists outside the lock instead of holding it across serialization and IO
- store: grouped-mode flush failures requeue the batched ops for retry instead of dropping them; a recovered retry advances the durability watermark
- wal: opening a directory retries the exclusive lock briefly before reporting DatabaseLocked, so closing a store and immediately reopening it no longer races the lock release under load
- key: decoding a corrupt tuple key with an oversized length prefix returns an error instead of panicking on integer overflow

Breaking:
- collection: Collection::put now returns Result — write closures must propagate it (tx.items.put(k, v)?)
- store: write() and write_durable() require T: Clone (backs the non-WAL persistence path and immediate-mode compaction)
- async: AsyncStore closures require Send + 'static — work now runs on tokio's blocking pool via spawn_blocking instead of block_in_place
- lib: 16 WAL internals moved off the crate root into etchdb::wal:: (SnapshotEntry, SnapshotPayload, CollectionSection, apply_op_with, apply_op_bytes, encode_msgpack_value, format_op_key, load_snapshot_entry, split_versioned_value, and the versioned apply_op variants)
- load: recoverable load anomalies no longer print to stderr — consumers that watched eprintln! output must read store.replay_report() (or open with open_wal_strict to fail hard)

## v0.4.0

New:
- migrations: register single-hop migration functions that compose into chains on load; each runs inside catch_unwind so a buggy one can't crash the process
- schema: per-value version tag via #[etch(version = V)] — values migrate independently as schemas evolve
- quarantine: values that can't migrate are preserved with a reason; retry after shipping the missing migration, or drop explicitly
- drift: schema fingerprint mismatch on load prints a prominent warning — catches "bumped struct, forgot to bump version" bugs
- lock: exclusive .lock file prevents two processes from opening the same directory; second opener gets DatabaseLocked with the holder's PID
- snapshot: new envelope with per-value version tags, serialized as named msgpack so adding fields doesn't break older readers
- compaction: crash-safe rotation — old WAL preserved as wal.prev until the new snapshot is confirmed readable on next boot

Breaking:
- wal: format bumped from v3 to v4; legacy v3 WALs still read, new writes are v4
- replayable: apply_with_format now required; manual trait impls must accept a ReplayFormat argument
- deps: rmp-serde added as a required dependency

## v0.3.3

- workspace: repo converted to a Cargo workspace with shared [workspace.package] metadata (version, edition, license, repository)
- derive: etchdb-derive had shipped stuck at 0.3.0 while the main crate advanced — version now workspace-inherited and pinned exactly (=0.3.3) so both crates release in lockstep
- release: RELEASE.md added with the two-crate publish order

## v0.3.2

- key: EtchKey impls for Ipv4Addr, Ipv6Addr, and IpAddr with discriminant-tagged encoding
- key: generic (A, B) tuple key impl with length-prefixed encoding

## v0.3.1

- async: AsyncStore::open_wal for opening WAL-backed stores directly from async contexts

## v0.3.0

New:
- derive: Replayable and Transactable derive macros, eliminating ~60 lines of boilerplate per state type
- derive: Collection transaction handle with typed get/put/delete and automatic op buffering
- async: AsyncStore wrapper using block_in_place for tokio runtimes (optional, behind async feature)
- key: EtchKey trait with impls for String, Vec<u8>, u8, u16, u32, u64, i8, i16, i32, i64
- overlay: generic over key type via EtchKey, HashMap support alongside BTreeMap via MapRead trait
- diff: apply_op_with and apply_op_hash_with for generic key types, plus bytes variants
- writer: versioned snapshot envelope with optional zstd compression (behind compression feature)
- writer: clear error message when opening zstd snapshot without compression feature

Breaking:
- op: Op keys changed from String to Vec<u8>
- overlay: Overlay<V> is now Overlay<K, V> requiring an Ord + Clone key type
- merge: apply_overlay_map removed, use apply_overlay_btree instead
- wal: WAL format bumped to version 3
- deps: tokio moved to optional dependency behind async feature

## v0.2.2

- backend: remove PostcardBackend and Store::open_postcard — WAL is the only file-backed path
- store: group_commit test migrated from PostcardBackend to WalBackend

## v0.2.1

- crate: renamed to etchdb (etch was taken on crates.io)
- crate: description updated to match README tagline

## v0.2.0

New:
- store: unified write path — Diffable removed, write() now uses zero-clone transaction capture (Transactable)
- store: Replayable trait replaces Diffable for WAL replay
- examples: hello (in-memory todos) and contacts (persistent CRUD with WAL)
- bench: moved to benches/ with cargo bench support, no extra dependencies

Breaking:
- store: write() now takes a transaction closure, not a mutable state closure
- store: write_tx() renamed to write(), write_tx_durable() renamed to write_durable()
- wal: Diffable trait and diff_map() removed from public API
