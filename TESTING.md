# Testing

## Running

```sh
cargo test --workspace                 # default features
cargo test --workspace --all-features  # + async (tokio) and compression (zstd) tests
```

## Conventions

- Tests live in `*_test.rs` files next to the code under test — never inline
  `#[cfg(test)] mod` blocks in source files.
- Crate-root test files are wired up in `src/lib.rs` via
  `#[cfg(test)] #[path = "foo_test.rs"] mod foo_test;`; `wal/` test modules are
  declared the same way in `src/wal/mod.rs`.
- Test files start with `use super::*;` (or explicit `crate::` imports).
- Names follow `test_<what>_<scenario>`, e.g.
  `test_append_after_load_truncation_survives_reopen`.
- Filesystem fixtures use `tempfile::TempDir` — no fixed paths, no leftovers.
- Both success and error paths are covered; error tests assert the variant,
  not just `is_err()`.

## Crash-injection harness

`src/wal/crash_test.rs` verifies that acknowledged (fsync'd) writes survive a
hard kill mid-compaction:

- Instrumentation points in the WAL call `maybe_crash("<label>")`
  (`src/wal/format.rs`). When a process runs with
  `ETCHDB_CRASH_POINT=<label>`, reaching that point calls
  `std::process::abort()` — no unwinding, no destructors, no buffer flushes,
  modelling SIGKILL / power loss. The hook compiles to nothing outside test
  builds.
- The parent test re-execs the test binary as a child (selecting the
  `crash_child_worker` test with `--exact`), passing `ETCHDB_CRASH_DIR` and
  `ETCHDB_CRASH_POINT`. The child writes acknowledged ops, triggers a
  compaction, and dies at the requested point.
- The parent asserts the child crashed, reopens the store, and verifies every
  acknowledged write survived — covering the window between `wal.prev`
  creation and the snapshot rename.

`crash_child_worker` is a no-op in a normal `cargo test` run; it only acts
when the env vars are set.

## Coverage

```sh
cargo llvm-cov --all --ignore-filename-regex '_test\.rs$|examples/'
```

## Benchmarks

```sh
cargo bench
```

See `benches/bench.rs`; results feed the performance table in README.md.
