# Releasing `csv_ingest`

Only `crates/csv-ingest` is published. Workspace packages under `tools/` must
remain unpublished and must not appear in the packaged crate.

Publishing uses crates.io Trusted Publishing through
`.github/workflows/release.yml`; no long-lived crates.io token is stored in
GitHub. After publishing succeeds, the workflow tags the published commit and
creates a GitHub release from the matching changelog section. The crates.io
publisher configuration must match this repository, the `release.yml` workflow
filename, and the `release` GitHub environment. Protect that environment with
an explicit approval before deployment.

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
workspace tools, tests, proptest regressions, fuzz targets, generated data, or
build artifacts are included.

## 3. Publish the merged commit

Publishing is irreversible. Approval of the `release` GitHub environment is the
final maintainer confirmation immediately before the publish job receives a
short-lived crates.io token.

1. Switch to `main`, pull with `--ff-only`, and verify a clean working tree.
2. Confirm the release pull request and current `main` CI are green.
3. Confirm the manifest and package both report the intended version.
4. In GitHub Actions, run **Publish to crates.io** from `main` and enter the
   manifest version without a `v` prefix. The workflow verifies the branch,
   version, lockfile, and package before requesting publishing approval.
5. Review the pending `release` environment deployment and approve it only when
   the validated commit and version are correct.
6. Wait for the complete workflow to succeed. After crates.io publishing, it
   creates an annotated `vX.Y.Z` tag on the exact published commit and a GitHub
   release from that version's `CHANGELOG.md` section.
7. Verify the new version on crates.io and confirm its generated documentation
   builds successfully on docs.rs.

## Recovery

Published crate contents cannot be replaced. If a release is unusable, yank it,
fix the problem, increment the version, and publish a new release:

```bash
cargo yank --vers X.Y.Z csv_ingest
```

If crates.io publishing succeeds but the GitHub release job fails, use **Re-run
failed jobs** in GitHub Actions. The job safely reuses a tag only when it points
to the exact published commit and treats an existing GitHub release as success.
