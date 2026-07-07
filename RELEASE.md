# Release checklist

Shipping skills stage changes but never commit — the user reviews, commits, and tags manually.

## 1. Documentation

1. `/etchdb:features` — populate FEATURES.md from current code
2. `/etchdb:changelog` — generate changelog from staged/committed changes
3. `/etchdb:readme` — write or update README.md (badges, version in install snippet, feature list)
4. `/etchdb:doc-check` — validate documentation completeness

## 2. Gates

All must pass before bumping the version:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo build --examples --all-features
```

## 3. Version bump

`/etchdb:release` runs the full gate and stops before commit. The version lives in
two places in the root `Cargo.toml`, and they must move in lockstep:

- `[workspace.package] version` — shared by `etchdb` and `etchdb-derive`
- `[workspace.dependencies] etchdb-derive = { path = "derive", version = "=X.Y.Z" }` — the exact pin

Breaking API changes bump the minor version under 0.x semver (0.4 -> 0.5).

## 4. Commit and tag (manual)

The user commits the release and tags it `vX.Y.Z`.

## 5. Publish

`etchdb` depends on the exact-pinned `etchdb-derive`, so the derive crate must be
on crates.io first:

```sh
cargo publish -p etchdb-derive
# wait for the crates.io index to pick it up, then:
cargo publish -p etchdb
```
