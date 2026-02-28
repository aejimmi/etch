# Changelog

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
