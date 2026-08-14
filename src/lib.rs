//! Streaming CSV ingestion with optional fast local path.
//!
//! - Streaming path: works with local files and gzip/zstd.
//! - Fast local path: feature `fast_local`, uncompressed local UTF-8 only.
//!
//! Data shape:
//! - `CsvIngestSummary { row_count, headers }`
//! - Streaming rows: `csv_async::ByteRecord` (access with `get(idx) -> Option<&[u8]>`)
#![cfg_attr(docsrs, feature(doc_cfg))]
//
mod codec;
#[cfg(feature = "fast_local")]
mod fast;
mod io;
mod options;

pub use crate::codec::{DecodePolicy, TranscodingError};
#[cfg(feature = "fast_local")]
pub use crate::fast::fast_local_process;
pub use crate::io::{build_csv_reader, reader_from_path, CsvMeta};
pub use crate::options::{CsvHeaderMode, CsvOptions, CsvTerminator, CsvTrim};

use csv_async::{AsyncReaderBuilder, ByteRecord};
use thiserror::Error;
use tokio::io::AsyncRead;

/// Result summary (keep it simple/minimal)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvIngestSummary {
    pub row_count: usize,
    pub headers: Vec<String>,
}

/// Error type returned by this crate when not using `anyhow`.
#[derive(Debug, Error)]
pub enum CsvIngestError {
    #[error("Missing required header: {0}")]
    MissingHeader(String),
    #[error("Row {row} is missing required field: {header}")]
    MissingRequiredField { row: usize, header: String },
    #[error("Unsupported CSV options: {0}")]
    UnsupportedOptions(String),
    #[error("Unsupported Content-Encoding: {0}")]
    UnsupportedContentEncoding(String),
    #[error("Stacked Content-Encoding values are unsupported: {0}")]
    UnsupportedStackedContentEncoding(String),
    #[error(
        "Conflicting compression metadata: {higher_source} indicates {higher}, but {lower_source} indicates {lower}"
    )]
    ConflictingCompressionMetadata {
        higher_source: &'static str,
        higher: &'static str,
        lower_source: &'static str,
        lower: &'static str,
    },
    #[error(transparent)]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Csv(#[from] csv_async::Error),
}

pub type CsvResult<T> = std::result::Result<T, CsvIngestError>;

/// Parse a CSV stream using the shared dialect options and required-header validation.
pub async fn process_csv_stream<R>(
    reader: R,
    required_headers: &[&str],
    options: &CsvOptions,
) -> CsvResult<CsvIngestSummary>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    if options.headers == CsvHeaderMode::Absent && !required_headers.is_empty() {
        return Err(CsvIngestError::UnsupportedOptions(
            "required headers cannot be validated when headers are absent".to_string(),
        ));
    }

    let mut builder = AsyncReaderBuilder::new();
    options.configure_reader(&mut builder)?;
    // Larger internal buffer reduces syscalls and allocator churn.
    builder.buffer_capacity(1 << 20);
    let mut rdr = builder.create_reader(reader);

    let headers = if options.headers == CsvHeaderMode::Present {
        rdr.byte_headers().await?.clone()
    } else {
        ByteRecord::new()
    };
    let required_indices = required_headers
        .iter()
        .map(|req_h| {
            headers
                .iter()
                .position(|h| h == req_h.as_bytes())
                .ok_or_else(|| CsvIngestError::MissingHeader(req_h.to_string()))
        })
        .collect::<CsvResult<Vec<_>>>()?;

    let mut row_count = 0usize;
    // Use ByteRecord to avoid per-row UTF-8 decoding; decode only when needed
    let mut record = ByteRecord::new();

    while rdr.read_byte_record(&mut record).await? {
        row_count += 1;

        for (i, &idx) in required_indices.iter().enumerate() {
            if record.get(idx).is_none() {
                return Err(CsvIngestError::MissingRequiredField {
                    row: row_count,
                    header: required_headers[i].to_string(),
                });
            }
        }
    }

    Ok(CsvIngestSummary {
        row_count,
        headers: headers
            .iter()
            .map(std::str::from_utf8)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(str::to_string)
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn reports_a_missing_required_header() {
        let error = process_csv_stream(
            Cursor::new(b"sku,value\nA,1\n"),
            &["missing"],
            &CsvOptions::default(),
        )
        .await
        .expect_err("missing header must fail");

        assert!(matches!(
            error,
            CsvIngestError::MissingHeader(header) if header == "missing"
        ));
    }

    #[tokio::test]
    async fn reports_a_row_missing_a_required_field() {
        let options = CsvOptions {
            flexible: true,
            ..CsvOptions::default()
        };
        let error = process_csv_stream(Cursor::new(b"sku,value\nA\n"), &["value"], &options)
            .await
            .expect_err("short row must fail");

        assert!(matches!(
            error,
            CsvIngestError::MissingRequiredField { row: 1, header } if header == "value"
        ));
    }

    #[tokio::test]
    async fn strict_rows_must_match_the_header_width() {
        let error = process_csv_stream(
            Cursor::new(b"sku,value\nA\n"),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect_err("ragged row must fail by default");

        assert!(matches!(error, CsvIngestError::Csv(_)));
    }

    #[tokio::test]
    async fn empty_and_header_only_inputs_have_no_rows() {
        let empty = process_csv_stream(Cursor::new(b""), &[], &CsvOptions::default())
            .await
            .expect("parse empty input");
        let header_only = process_csv_stream(
            Cursor::new(b"sku,value\n"),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect("parse header-only input");

        assert_eq!(
            empty,
            CsvIngestSummary {
                row_count: 0,
                headers: vec![]
            }
        );
        assert_eq!(header_only.row_count, 0);
        assert_eq!(header_only.headers, ["sku", "value"]);
    }

    #[tokio::test]
    async fn headerless_input_counts_the_first_record_as_data() {
        let options = CsvOptions {
            headers: CsvHeaderMode::Absent,
            ..CsvOptions::default()
        };
        let summary = process_csv_stream(Cursor::new(b"A,1\nB,2\n"), &[], &options)
            .await
            .expect("parse headerless input");

        assert_eq!(
            summary,
            CsvIngestSummary {
                row_count: 2,
                headers: vec![]
            }
        );
    }

    #[tokio::test]
    async fn required_headers_need_a_header_record() {
        let options = CsvOptions {
            headers: CsvHeaderMode::Absent,
            ..CsvOptions::default()
        };
        let error = process_csv_stream(Cursor::new(b"A,1\n"), &["sku"], &options)
            .await
            .expect_err("required headers without a header row must fail");

        assert!(matches!(error, CsvIngestError::UnsupportedOptions(_)));
    }

    #[tokio::test]
    async fn delimiter_terminator_trimming_quotes_and_bom_follow_options() {
        let options = CsvOptions {
            delimiter: b';',
            terminator: CsvTerminator::Any(b'$'),
            trim: CsvTrim::All,
            ..CsvOptions::default()
        };
        let summary = process_csv_stream(
            Cursor::new(b"\xef\xbb\xbf sku ; value $A;'quoted;value'$"),
            &["sku"],
            &CsvOptions {
                quote: b'\'',
                ..options
            },
        )
        .await
        .expect("parse configured dialect");

        assert_eq!(summary.row_count, 1);
        assert_eq!(summary.headers, ["sku", "value"]);
    }

    #[tokio::test]
    async fn header_and_field_only_trimming_modes_are_supported() {
        let header_trim = CsvOptions {
            trim: CsvTrim::Headers,
            ..CsvOptions::default()
        };
        let summary = process_csv_stream(
            Cursor::new(b" sku , value \n A , 1 \n"),
            &["sku"],
            &header_trim,
        )
        .await
        .expect("trim headers");
        assert_eq!(summary.headers, ["sku", "value"]);

        let field_trim = CsvOptions {
            trim: CsvTrim::Fields,
            ..CsvOptions::default()
        };
        let summary =
            process_csv_stream(Cursor::new(b"sku,value\n A , 1 \n"), &["sku"], &field_trim)
                .await
                .expect("trim fields");
        assert_eq!(summary.row_count, 1);
    }

    #[tokio::test]
    async fn invalid_utf8_headers_return_a_typed_error() {
        let error = process_csv_stream(
            Cursor::new(b"sku,\xff\nA,1\n"),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect_err("invalid UTF-8 header must fail");

        assert!(matches!(error, CsvIngestError::InvalidUtf8(_)));
    }
}
