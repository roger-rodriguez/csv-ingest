# Releasing `csv_ingest`

Only `crates/csv-ingest` is published. Workspace packages under `tools/` must
remain unpublished and must not appear in the packaged crate.

## 1. Prepare a release pull request

1. Start from current `main` on a dedicated release branch.
2. Update the version in `crates/csv-ingest/Cargo.toml`.
3. Refresh both `Cargo.lock` and `tools/csv-ingest-fuzz/Cargo.lock`.
4. Move the release notes from `[Unreleased]` into a dated section in
   `CHANGELOG.md`, including migration instructions for breaking changes.
5. Run the complete release validation below.
6. Open a pull request and merge it only after required CI passes.

## 2. Validate the release

Update stable, then run the supported minimum and current stable toolchains
explicitly:

```bash
rustup update stable
cargo +1.82.0 test -p csv_ingest --all-targets --all-features --locked --no-fail-fast
cargo +stable fmt --all -- --check
cargo +stable clippy --workspace --all-targets --all-features -- -D warnings
cargo +stable test --workspace --all-targets --all-features --locked --no-fail-fast
cargo +stable doc -p csv_ingest --no-deps --all-features
```

Run the same coverage thresholds enforced by CI:

```bash
cargo +stable llvm-cov \
  --package csv_ingest \
  --all-features \
  --all-targets \
  --ignore-filename-regex '(^|/)(bin|examples)/' \
  --fail-under-lines 95 \
  --fail-under-file-lines 90 \
  --summary-only
```

Verify the exact crate contents and the crates.io upload without publishing:

```bash
cargo +stable package -p csv_ingest --locked
cargo +stable publish -p csv_ingest --locked --dry-run
```

Inspect `cargo package -p csv_ingest --locked --list` and confirm that no
workspace tools, fuzz targets, generated data, or build artifacts are included.

## 3. Publish the merged commit

Publishing is irreversible. Obtain explicit maintainer confirmation immediately
before this section.

1. Switch to `main`, pull with `--ff-only`, and verify a clean working tree.
2. Confirm the release pull request and current `main` CI are green.
3. Confirm the manifest and package both report the intended version.
4. Publish the exact merged commit:

```bash
cargo +stable publish -p csv_ingest --locked
```

5. Tag that same commit and create the GitHub release from the matching changelog
   section:

```bash
git tag -a vX.Y.Z -m "csv_ingest vX.Y.Z"
git push origin vX.Y.Z
gh release create vX.Y.Z --verify-tag --title "csv_ingest vX.Y.Z" --notes-file <release-notes-file>
```

6. Verify the new version on crates.io and confirm its generated documentation
   builds successfully on docs.rs.

## Recovery

Published crate contents cannot be replaced. If a release is unusable, yank it,
fix the problem, increment the version, and publish a new release:

```bash
cargo yank --vers X.Y.Z csv_ingest
```
