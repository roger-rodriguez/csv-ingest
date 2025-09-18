# Publishing this crate

This is the process for releasing a new version of this crate to crates.io.

## 0. Preflight

Update Rust:

```bash
rustup update stable
```

## 1. Update Cargo.toml

```toml
[package]
name = "crate-name"
version = "X.X.X"
```

## 2. Lint, test, and docs

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps
```

## 3. Package locally

```bash
cargo package
```

## 4. Dry-run publish

```bash
cargo publish --dry-run
```

## 5. Publish for real

```bash
cargo publish
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
