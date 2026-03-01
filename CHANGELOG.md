# Changelog

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
