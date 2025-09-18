# 📄 csv_ingest

[![CI](https://github.com/roger-rodriguez/csv-ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/roger-rodriguez/csv-ingest/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/csv_ingest.svg)](https://github.com/roger-rodriguez/csv-ingest/blob/main/LICENSE)

----

Rust Library for parsing CSV files from local files or any async source (`AsyncRead`). It focuses on high throughput, low memory, and correctness by default.

## ✨ Features

- Automatic decompression (gzip, zstd) via content‑encoding, content‑type, or file extension
- Optional transcoding to UTF‑8 using `encoding_rs`
- Streaming CSV parsing using `csv_async` (no full‑file buffering)
- Header validation to ensure required columns exist
- Optional fast local mode (mmap + parallel memchr) for uncompressed UTF‑8 CSVs

## 🚀 Quickstart

```toml
# Cargo.toml
[dependencies]
csv_ingest = { git = "https://github.com/roger-rodriguez/csv-ingest" }
```

If you need to parse from a remote source, construct an `AsyncRead` in your app (e.g., a `reqwest` byte stream) and pass it to `build_csv_reader`/`process_csv_stream`.

```rust
// pseudo
let (reader, meta) = build_csv_reader(remote_async_read, CsvMeta { content_type, content_encoding, name_hint, ..Default::default() });
let summary = process_csv_stream(reader, &["sku"]).await?;
```

```rs
// Stream & validate; returns headers + row_count
async fn process_csv_stream<R: AsyncRead + Unpin + Send + 'static>(
  reader: R,
  required_headers: &[&str],
) -> Result<CsvIngestSummary, anyhow::Error>;
```

Minimal example (local file):

```rs
use csv_ingest::{reader_from_path, process_csv_stream};
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (reader, _meta) = reader_from_path(Path::new("./data/sample.csv.gz")).await?;
    let required = ["sku"]; // repeat in the slice for multiple required headers
    let summary = process_csv_stream(reader, &required).await?;
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

For local, uncompressed, UTF‑8 CSVs you control, enable the `fast_local` feature and use `--fast-local` in the bench. This path maps the file, splits by newline per core, and scans with `memchr`, extracting only required fields.

Assumptions:

- No embedded newlines inside fields (simple quoting only)
- Single‑byte delimiter (default `,`)
- Header is first line

Use `--verify --limit` to validate on a sample when benchmarking.

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
- Put required columns early; the fast‑local path short‑circuits after the last required column
- Build with native CPU flags and release optimizations (already configured)

## 📄 License

MIT
