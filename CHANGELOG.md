# Changelog

All notable changes to `csv_ingest` are documented here. The project follows
Semantic Versioning, with changes to the minor version representing breaking
releases while the crate remains below 1.0.

## [Unreleased]

## [0.2.2] - 2026-08-14

### Changed

- Reduced the crates.io archive by excluding repository-only integration tests
  and proptest regression artifacts from the published package.
- Added a CI package-content gate to prevent those test artifacts from being
  published again.

This patch release has no public API or runtime behavior changes.

## [0.2.1] - 2026-08-14

### Changed

- Refreshed the locked fast-local dependencies to `memchr` 2.8.3 and
  `memmap2` 0.9.11 while retaining Rust 1.82 support.
- Release publishing now uses crates.io Trusted Publishing with a protected
  GitHub environment instead of a long-lived registry token.

This patch release has no public API changes.

## [0.2.0] - 2026-08-14

### Added

- Added `CsvParser`, a record-oriented API that resolves required columns once
  and supports parser-owned or caller-owned reusable `ByteRecord` storage.
- Added `CsvOptions` and the `CsvHeaderMode`, `CsvTerminator`, and `CsvTrim`
  types as a shared dialect contract for streaming and fast-local parsing.
- Added typed errors for ragged rows, missing fields, unsupported dialects,
  compression metadata conflicts, malformed encoding, and CSV syntax errors.
- Added `summarize_csv_stream` and `summarize_csv_path` convenience APIs.
- Added deterministic property tests, a coverage gate, and an isolated
  cargo-fuzz harness for the public fast-local API.
- Declared Rust 1.82 as the minimum supported Rust version and added CI coverage
  for both Rust 1.82 and current stable Rust.

### Changed

- Replaced `process_csv_stream(reader, required_headers)` with
  `summarize_csv_stream(reader, required_headers, options)`.
- Changed the default row-width behavior from flexible to strict. Set
  `CsvOptions::flexible` to `true` to accept ragged records.
- Changed `CsvIngestSummary::row_count` from `usize` to `u64`.
- Changed `build_csv_reader` to return `CsvResult` and a lifetime-aware boxed
  reader. Compression conflicts and unsupported encodings now fail explicitly.
- Changed non-UTF-8 transcoding to reject malformed input by default. Set
  `CsvMeta::decode_policy` to `DecodePolicy::Replace` to restore replacement
  character behavior.
- Changed `fast_local_process` to accept shared `CsvOptions` instead of separate
  delimiter and line-break arguments. It now returns `CsvResult`, applies a
  global row limit, validates row widths, and produces worker-independent CRCs.
- Moved the publishable crate to `crates/csv-ingest` and development binaries to
  the unpublished `tools/csv-ingest-tools` workspace package.

### Fixed

- Fixed fast-local handling of CRLF, final unterminated records, empty and
  header-only files, missing required fields, global row numbers, and UTF-8 BOMs.
- Fast-local parsing now rejects quoted input it cannot safely represent instead
  of silently returning incorrect results.
- Compression detection now applies deterministic metadata precedence and
  rejects contradictory gzip and zstd signals.

## Migrating from 0.1.x

### Stream summaries

Replace `process_csv_stream` with `summarize_csv_stream` and pass explicit CSV
options:

```rust
use csv_ingest::{summarize_csv_stream, CsvOptions};

let options = CsvOptions {
    flexible: true, // Preserves the 0.1.x ragged-row behavior when required.
    ..CsvOptions::default()
};
let summary = summarize_csv_stream(reader, &["sku"], &options).await?;
```

Use `CsvParser::from_reader` when records, rather than only a summary, are
needed. Header indices are resolved once and each record remains byte-oriented.

### Transport setup

`build_csv_reader` is now fallible, so propagate or handle its result:

```rust
let (reader, normalized_meta) = build_csv_reader(raw, meta)?;
```

`CsvMeta` has a new `decode_policy` field. Prefer struct update syntax with
`..CsvMeta::default()`. To retain lossy transcoding explicitly:

```rust
use csv_ingest::{CsvMeta, DecodePolicy};

let meta = CsvMeta {
    decode_policy: DecodePolicy::Replace,
    ..CsvMeta::default()
};
```

### Fast-local parsing

Move delimiter and terminator settings into `CsvOptions` and account for the
typed result:

```rust
use csv_ingest::{fast_local_process, CsvOptions, CsvTerminator};

let options = CsvOptions {
    delimiter: b';',
    terminator: CsvTerminator::Any(b'\n'),
    quoting: false,
    ..CsvOptions::default()
};
let (summary, crc) =
    fast_local_process(path, &["sku"], &options, true, None)?;
```

The fast-local path supports only unquoted records. Leave quoting enabled to
detect and reject quote bytes, or disable it only when quote bytes are ordinary
data in the input dialect.

[Unreleased]: https://github.com/roger-rodriguez/csv-ingest/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/roger-rodriguez/csv-ingest/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/roger-rodriguez/csv-ingest/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/roger-rodriguez/csv-ingest/compare/v0.1.1...v0.2.0
