# csv_ingest

[![CI](https://github.com/roger-rodriguez/csv-ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/roger-rodriguez/csv-ingest/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/csv_ingest.svg)](https://crates.io/crates/csv_ingest)
[![Documentation](https://docs.rs/csv_ingest/badge.svg)](https://docs.rs/csv_ingest)
[![License](https://img.shields.io/crates/l/csv_ingest.svg)](https://github.com/roger-rodriguez/csv-ingest/blob/main/LICENSE)

Fast, byte-oriented CSV parsing for local files and asynchronous streams.

- Stream records without loading the full file into memory.
- Read plain, gzip, or zstd input from a path or any Tokio `AsyncRead`.
- Validate required headers and row widths while parsing.
- Transcode non-UTF-8 input with strict error handling by default.
- Opt into a parallel mmap path for uncompressed local files.

Requires Rust 1.82 or newer.

## Install

```bash
cargo add csv_ingest
cargo add tokio --features macros,rt-multi-thread
```

The second command adds the Tokio runtime used by the async examples. Enable
the specialized local parser when needed:

```bash
cargo add csv_ingest --features fast_local
```

## Choose an API

| Need | API |
| --- | --- |
| Count rows and validate headers | `summarize_csv_path` or `summarize_csv_stream` |
| Process every record | `CsvParser` |
| Maximize throughput for a compatible local file | `fast_local_process` |

Start with the streaming APIs. They support compression, transcoding, standard
CSV quoting, local paths, and remote readers. Use fast-local only for
uncompressed UTF-8 files that contain no quoted records or embedded newlines.

## Quick start

### Summarize a file

```rust
use csv_ingest::{summarize_csv_path, CsvOptions};
use std::error::Error;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (summary, _meta) = summarize_csv_path(
        Path::new("data/sample.csv.gz"),
        &["sku"],
        &CsvOptions::default(),
    )
    .await?;

    println!("rows={}, headers={:?}", summary.row_count, summary.headers);
    Ok(())
}
```

### Process records

`CsvParser` resolves required columns once and keeps fields as bytes so callers
only decode or parse the values they need.

```rust
use csv_ingest::{CsvOptions, CsvParser};
use std::error::Error;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (mut parser, _meta) = CsvParser::from_path(
        Path::new("data/sample.csv.zst"),
        &["sku"],
        &CsvOptions::default(),
    )
    .await?;
    let sku_index = parser.header_index("sku").expect("required header");

    while let Some(record) = parser.next_record().await? {
        let sku = record.get(sku_index).expect("validated field");
        // Use sku as &[u8], or decode it only when needed.
    }
    Ok(())
}
```

Any Tokio `AsyncRead` can be passed to `CsvParser::from_reader` or
`summarize_csv_stream`, including a reader backed by an HTTP or S3 response.

### Use the fast-local path

```rust
use csv_ingest::{fast_local_process, CsvOptions};
use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let (summary, _crc) = fast_local_process(
        Path::new("data/sample.csv"),
        &["sku"],
        &CsvOptions::default(),
        false,
        None,
    )?;
    println!("rows={}", summary.row_count);
    Ok(())
}
```

Fast-local maps the file and parses chunks in parallel. It deliberately rejects
quoted input rather than risk returning incorrect records.

## Important defaults

`CsvOptions::default()` uses:

- a comma delimiter and CR/LF record terminators;
- a header row;
- fixed-width records, with ragged rows rejected;
- standard double-quote handling;
- no whitespace trimming;
- UTF-8 BOM removal.

Set `flexible: true` to accept ragged rows. Non-UTF-8 transcoding rejects
malformed input unless `DecodePolicy::Replace` is selected explicitly. All
public parsing paths return typed `CsvIngestError` variants.

## More documentation

- [Usage and configuration](https://github.com/roger-rodriguez/csv-ingest/blob/main/docs/usage.md)
- [Benchmarking and large datasets](https://github.com/roger-rodriguez/csv-ingest/blob/main/docs/benchmarking.md)
- [Development, tests, coverage, and fuzzing](https://github.com/roger-rodriguez/csv-ingest/blob/main/docs/development.md)
- [API documentation](https://docs.rs/csv_ingest/latest/csv_ingest/)
- [Changelog and migration guides](https://github.com/roger-rodriguez/csv-ingest/blob/main/CHANGELOG.md)

## License

MIT
