# 📄 csv_ingest

[![CI](https://github.com/roger-rodriguez/csv-ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/roger-rodriguez/csv-ingest/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/csv_ingest.svg)](https://github.com/roger-rodriguez/csv-ingest/blob/main/LICENSE)

----

Rust Library for parsing CSV files from local files or any async source (`AsyncRead`). It focuses on high throughput, low memory, and correctness by default.

## ✨ Features

- Automatic decompression (gzip, zstd) via content‑encoding, content‑type, or file extension
- Optional transcoding to UTF‑8 using `encoding_rs`, with malformed input rejected by default
- Streaming CSV parsing using `csv_async` (no full‑file buffering)
- Header validation to ensure required columns exist
- Optional fast local mode (mmap + parallel memchr) for uncompressed UTF‑8 CSVs

## 🚀 Quickstart

```shell
cargo add csv_ingest
```

If you need to parse from a remote source, construct an `AsyncRead` in your app (e.g., a `reqwest` byte stream) and pass it to `build_csv_reader`/`process_csv_stream`.

```rust
// pseudo
let (reader, meta) = build_csv_reader(remote_async_read, CsvMeta { content_type, content_encoding, name_hint, ..Default::default() })?;
let options = CsvOptions::default();
let summary = process_csv_stream(reader, &["sku"], &options).await?;
```

```rs
// Stream & validate; returns headers + row_count
async fn process_csv_stream<R: AsyncRead + Unpin + Send + 'static>(
  reader: R,
  required_headers: &[&str],
  options: &CsvOptions,
) -> CsvResult<CsvIngestSummary>;
```

Minimal example (local file):

```rs
use csv_ingest::{reader_from_path, process_csv_stream, CsvOptions};
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (reader, _meta) = reader_from_path(Path::new("./data/sample.csv.gz")).await?;
    let required = ["sku"]; // repeat in the slice for multiple required headers
    let summary = process_csv_stream(reader, &required, &CsvOptions::default()).await?;
    println!("rows={}, headers={:?}", summary.row_count, summary.headers);
    Ok(())
}
```

## 🧑‍💻 Usage

### 📦 What this library returns (data shape)

- CsvIngestSummary: returned by `process_csv_stream(...)`
  - `row_count: usize`
  - `headers: Vec<String>` (exact header strings from the first row)
- Streaming rows (when you iterate): `csv_async::ByteRecord`
  - Access by index: `record.get(idx) -> Option<&[u8]>`
  - Decode only if needed: `std::str::from_utf8(bytes)` or parse to numbers as required
  - You typically resolve header indices once, then read those fields per row
- Remote vs local: identical shapes; only the reader source differs
- Fast‑local (feature `fast_local`): internal path optimized for local uncompressed CSVs
  - Library returns the same `CsvIngestSummary` (and the bench can print an optional CRC for verification)
  - Assumptions are listed below; use the streaming API when those don’t hold

### Shared CSV options

`CsvOptions` is the parsing contract for both streaming and fast-local parsing. Its defaults are:

- comma delimiter
- `\r`, `\n`, or `\r\n` record terminators
- first record is a header
- fixed-width rows (ragged rows are rejected)
- standard double-quote handling with no backslash escape
- no whitespace trimming
- a leading UTF-8 BOM is stripped

Set `headers` to `CsvHeaderMode::Absent` to count every record as data; named required-header validation is then unavailable. Set `flexible` to `true` to permit ragged rows, although rows must still contain every required column. Invalid delimiter, quote, escape, and terminator combinations return `CsvIngestError::UnsupportedOptions` before parsing.

Compression, content type, filename hints, and character transcoding remain separate transport concerns in `CsvMeta`.
Compression detection uses the first available signal in this order:
`content_encoding`, a compression-specific `content_type`, then the extension in
`name_hint`. Values are matched case-insensitively, and content-type parameters
are ignored. Disagreement between gzip and zstd signals is rejected, as are
unsupported or stacked content encodings. A plain content type such as
`text/csv` describes the underlying media and does not prevent extension fallback.

When `charset` is not UTF-8, transcoding rejects malformed or incomplete byte sequences by default.
Callers that explicitly want replacement characters can set `decode_policy` to
`DecodePolicy::Replace`; each malformed sequence is then replaced with `U+FFFD`.

```rs
use csv_ingest::{CsvMeta, DecodePolicy};

let meta = CsvMeta {
    charset: encoding_rs::SHIFT_JIS,
    decode_policy: DecodePolicy::Replace,
    ..CsvMeta::default()
};
```

### 🌊 Streaming (recommended default)

Works for local files, gzip/zstd, and remote streams (HTTP via reqwest, etc.). You provide an `AsyncRead` and process `ByteRecord`s, decoding only when needed.

```rs
use csv_ingest::reader_from_path;
use csv_async::{AsyncReaderBuilder, ByteRecord};
use std::path::Path;

# #[tokio::main]
# async fn main() -> anyhow::Result<()> {
let (reader, _meta) = reader_from_path(Path::new("data/your.csv.gz")).await?;
let mut rdr = AsyncReaderBuilder::new()
    .has_headers(true)
    .buffer_capacity(1 << 20)
    .create_reader(reader);

let headers = rdr.headers().await?.clone();
let required = ["sku", "col1", "col2"];
let idxs: Vec<usize> = required.iter()
    .map(|h| headers.iter().position(|x| x == *h).ok_or_else(|| anyhow::anyhow!("missing {h}")))
    .collect::<anyhow::Result<_>>()?;

let mut rec = ByteRecord::new();
while rdr.read_byte_record(&mut rec).await? {
    let sku = rec.get(idxs[0]).unwrap(); // &[u8]
    // decode only if needed:
    // let sku_str = std::str::from_utf8(sku)?;
}
# Ok(()) }
```

### ⚡️ Fast local mode (optional)

For local, uncompressed, UTF‑8 CSVs you control, enable the `fast_local` feature and use `--fast-local` in the bench. This path maps the file, splits by newline per core, and scans with `memchr`, validating required-field availability without allocating row records.

Assumptions:

- Quoted records are unsupported: with quoting enabled, the configured quote byte is rejected instead of being silently misparsed
- No embedded newlines inside fields
- Single-byte delimiter and terminator configuration
- Named required columns require `CsvHeaderMode::Present`

Use `--verify --limit` to validate on a global row sample when benchmarking. Verification hashes every field in row order and produces the same digest regardless of fast-local worker count.

## 🛠️ CLI (dev helpers)

This repo ships two binaries to generate synthetic CSV data and measure throughput.

```bash
# Build release binaries (enable fast_local for the optional mmap path)
cargo build --release --bins
cargo build --release --bins --features fast_local

# Generate 100M rows and compress
./target/release/gen --rows 100000000 --with-header | gzip -c > data/100m.csv.gz
./target/release/gen --rows 100000000 --with-header | zstd -T0 -q -o data/100m.csv.zst

# Run the bench (gzip / zstd / verify subset)
./target/release/bench --path data/100m.csv.gz --required sku
./target/release/bench --path data/100m.csv.zst --required sku
./target/release/bench --path data/100m.csv.gz --required sku --verify --limit 1000000

# Fast local path (uncompressed UTF‑8 CSVs)
./target/release/bench --path data/100m.csv --required sku --fast-local
./target/release/bench --path data/100m.csv --required sku --fast-local --verify --limit 1000000
```

Flags:

- `--required <col>`: specify one or more required headers (repeatable)
- `--verify`: strict checks + CRC32 across fields (catches subtle differences)
- `--limit <N>`: limit processed rows (useful with `--verify`)
- `--fast-local` (requires `--features fast_local`): mmap + parallel scanning for local, uncompressed UTF‑8 CSVs

## 📈 Generating large datasets

```bash
# 1 billion rows (uncompressed)
./target/release/gen --rows 1000000000 --with-header > data/1b.csv

# gzip
./target/release/gen --rows 1000000000 --with-header | gzip -c > data/1b.csv.gz

# zstd (often faster to read back)
./target/release/gen --rows 1000000000 --with-header | zstd -T0 -q -o data/1b.csv.zst

# sanity checks
wc -l data/1b.csv           # expect 1,000,000,001 (includes header)
./target/release/bench --path data/1b.csv.gz --required sku --verify --limit 1000000
```

## 🧪 Notes on performance

- Gzip is typically the bottleneck; prefer zstd or uncompressed for peak throughput
- With flexible rows and verification disabled, put required columns early so fast-local can stop field scanning after the last required column
- Build with native CPU flags and release optimizations (already configured)

## ✅ Test coverage

CI enforces at least 95% line coverage across the library and at least 90% for each library source file. Development binaries and examples are excluded from the metric because they are benchmark tooling rather than the published parser.

With `cargo-llvm-cov` 0.8.6 or newer installed, run the same gate locally:

```bash
cargo llvm-cov --all-features --all-targets \
  --ignore-filename-regex '(^|/)(bin|examples)/' \
  --fail-under-lines 95 \
  --fail-under-file-lines 90 \
  --summary-only
```

## 📄 License

MIT
