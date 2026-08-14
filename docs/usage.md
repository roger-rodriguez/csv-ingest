# Usage and configuration

The streaming parser is the default choice. It supports local paths, arbitrary
Tokio `AsyncRead` implementations, gzip and zstd decompression, character
transcoding, and standard CSV quoting. The fast-local parser is a specialized
path for compatible uncompressed files.

## Data types

- `CsvParser` reads records and reuses parser-owned or caller-owned storage.
- `ByteRecord` exposes fields as byte slices with `record.get(index)`.
- `CsvIngestSummary` contains `row_count: u64` and `headers: Vec<String>`.
- `CsvResult<T>` uses typed `CsvIngestError` variants for parsing, transport,
  dialect, and encoding failures.

Local and remote readers produce the same record and summary types.

## CSV options

`CsvOptions` is shared by streaming and fast-local parsing. Its defaults are:

- comma delimiter;
- `\r`, `\n`, or `\r\n` record terminators;
- first record is a header;
- fixed-width records;
- standard double-quote handling with no backslash escape;
- no whitespace trimming;
- leading UTF-8 BOM removal.

Set `headers` to `CsvHeaderMode::Absent` to count every record as data. Named
required-header validation is unavailable in that mode. Set `flexible` to
`true` to permit ragged rows; records must still contain every required column.
Invalid delimiter, quote, escape, and terminator combinations fail before
parsing with `CsvIngestError::UnsupportedDialect`.

## Streaming records

`CsvParser` resolves headers and required-column indices during construction.
Fields remain bytes unless the caller chooses to decode them.

```rust
use csv_ingest::{ByteRecord, CsvOptions, CsvParser};
use std::error::Error;
use std::path::Path;

async fn process_file() -> Result<(), Box<dyn Error>> {
    let (mut parser, _meta) = CsvParser::from_path(
        Path::new("data/your.csv.gz"),
        &["sku", "value"],
        &CsvOptions::default(),
    )
    .await?;
    let sku_index = parser.header_index("sku").expect("required header");

    let mut record = ByteRecord::new();
    while parser.read_record(&mut record).await? {
        let sku = record.get(sku_index).expect("validated field");
        // let sku = std::str::from_utf8(sku)?;
    }
    Ok(())
}
```

Use `next_record()` for parser-owned record storage or `read_record()` to reuse
a caller-owned `ByteRecord`.

## Remote readers

Pass any compatible Tokio `AsyncRead` to `CsvParser::from_reader` or
`summarize_csv_stream`. If transport metadata indicates compression or a
non-UTF-8 charset, normalize the source with `build_csv_reader` first:

```rust
use csv_ingest::{build_csv_reader, CsvMeta, CsvOptions, CsvParser};
use std::error::Error;
use tokio::io::AsyncRead;

async fn process_remote<R>(remote_reader: R) -> Result<(), Box<dyn Error>>
where
    R: AsyncRead + Unpin + Send,
{
    let meta = CsvMeta {
        content_type: "application/gzip".into(),
        name_hint: "rows.csv.gz".into(),
        ..CsvMeta::default()
    };
    let (reader, _normalized_meta) = build_csv_reader(remote_reader, meta)?;
    let mut parser =
        CsvParser::from_reader(reader, &["sku"], &CsvOptions::default()).await?;

    while let Some(record) = parser.next_record().await? {
        // Process record.
    }
    Ok(())
}
```

Compression signals are evaluated in this order:

1. `content_encoding`
2. a compression-specific `content_type`
3. the extension in `name_hint`

Matching is case-insensitive and content-type parameters are ignored.
Contradictory gzip and zstd signals, stacked encodings, and unsupported content
encodings return typed errors.

## Character transcoding

UTF-8 input passes through without transcoding. For another character encoding,
set `CsvMeta::charset`. Malformed or incomplete input is rejected by default.
Lossy replacement is an explicit opt-in:

```bash
cargo add encoding_rs
```

```rust
use csv_ingest::{CsvMeta, DecodePolicy};

let meta = CsvMeta {
    charset: encoding_rs::SHIFT_JIS,
    decode_policy: DecodePolicy::Replace,
    ..CsvMeta::default()
};
```

## Fast-local parsing

Enable the feature with `cargo add csv_ingest --features fast_local`.

The fast-local path uses mmap, parallel chunking, and byte scanning. It is
intended for local, uncompressed UTF-8 files with:

- no quoted records;
- no embedded newlines in fields;
- single-byte delimiters and terminators;
- a header row when named required columns are used.

```rust
use csv_ingest::{fast_local_process, CsvOptions};
use std::error::Error;
use std::path::Path;

fn process_local() -> Result<(), Box<dyn Error>> {
    let (summary, crc) = fast_local_process(
        Path::new("data/your.csv"),
        &["sku"],
        &CsvOptions::default(),
        true,
        Some(1_000_000),
    )?;
    Ok(())
}
```

With quoting enabled, the configured quote byte is rejected. Disable quoting
only when quote bytes are ordinary data in the selected dialect. The optional
CRC hashes every field in row order and is independent of worker count.
