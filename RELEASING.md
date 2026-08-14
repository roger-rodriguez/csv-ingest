# Publishing this crate

This is the process for releasing a new version of this crate to crates.io.

## 0. Preflight

Update Rust:

```bash
rustup update stable
```

## 1. Update `crates/csv-ingest/Cargo.toml`

```toml
[package]
name = "crate-name"
version = "X.X.X"
```

## 2. Lint, test, and docs

```bash
cargo fmt --all
cargo clippy -p csv_ingest --all-targets --all-features -- -D warnings
cargo test -p csv_ingest --all-targets --all-features --locked
cargo doc -p csv_ingest --no-deps --all-features
```

## 3. Package locally

```bash
cargo package -p csv_ingest --locked
```

## 4. Dry-run publish

```bash
cargo publish -p csv_ingest --dry-run
```

## 5. Publish for real

```bash
cargo publish -p csv_ingest
```

## 6. Post-publish

```bash
git tag v0.X.0
git push --tags
```

## If something goes wrong

- Yank a bad release:

```bash
cargo yank --vers 0.X.0 crate-name
```

- Fix, bump the version, and publish again.
