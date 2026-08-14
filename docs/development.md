# Development

The repository is a Cargo workspace:

- `crates/csv-ingest`: the only publishable crate;
- `tools/csv-ingest-tools`: unpublished generator and benchmark binaries;
- `tools/csv-ingest-fuzz`: an isolated, unpublished nightly fuzz workspace.

The minimum supported Rust version is 1.82. CI also tests the latest stable
toolchain. The MSRV may be raised when a newer compiler enables a materially
simpler or faster implementation; compatibility shims are not added solely to
retain older compiler support.

## Standard checks

```bash
cargo +1.82.0 test -p csv_ingest --all-targets --all-features --locked --no-fail-fast
cargo +stable fmt --all -- --check
cargo +stable clippy --workspace --all-targets --all-features -- -D warnings
cargo +stable test --workspace --all-targets --all-features --locked --no-fail-fast
cargo +stable doc -p csv_ingest --no-deps --all-features
```

See [RELEASING.md](../RELEASING.md) for the complete release gate and publishing
boundary.

## Coverage

CI requires at least 95% line coverage across the library and 90% for each
library source file. Development binaries and examples are excluded.

With `cargo-llvm-cov` 0.8.6 or newer:

```bash
cargo llvm-cov \
  --package csv_ingest \
  --all-features \
  --all-targets \
  --ignore-filename-regex '(^|/)(bin|examples)/' \
  --fail-under-lines 95 \
  --fail-under-file-lines 90 \
  --summary-only
```

## Property tests

The fast-local property suite generates supported unquoted CSV fixtures and
compares results with the streaming parser across 1, 2, and 8 workers:

```bash
cargo test --features fast_local fast::property_tests::
```

Proptest shrinks mismatches and records seeds under `proptest-regressions/`.
Retain those files as deterministic regression cases.

## Fuzzing

The nightly cargo-fuzz workspace exercises the public fast-local API without
adding dependencies or latency to normal workspace and pull-request checks.

```bash
cargo install cargo-fuzz --version 0.13.2 --locked
cargo +nightly fuzz build --fuzz-dir tools/csv-ingest-fuzz fast_local
mkdir -p tools/csv-ingest-fuzz/target/smoke-corpus
cargo +nightly fuzz run --fuzz-dir tools/csv-ingest-fuzz fast_local \
  tools/csv-ingest-fuzz/target/smoke-corpus \
  tools/csv-ingest-fuzz/corpus/fast_local -- \
  -max_total_time=60 \
  -timeout=10 \
  -max_len=1048576
```

The first two fuzz bytes select input transformations and parser options. The
remaining bytes are written to a temporary file and passed to the public API.
The first corpus path is writable, while the second contains deterministic
checked-in seeds.

Failures are written below `tools/csv-ingest-fuzz/artifacts/`. Minimize a case
before diagnosis:

```bash
cargo +nightly fuzz tmin --fuzz-dir tools/csv-ingest-fuzz \
  fast_local tools/csv-ingest-fuzz/artifacts/fast_local/<artifact>
```

After fixing a failure, retain the minimized input as a deterministic core
regression test. A bounded fuzz run can find crashes, panics, hangs, and
sanitizer findings; it cannot prove the absence of all undefined behavior.
