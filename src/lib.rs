//! Streaming CSV ingestion with optional fast local path.
//!
//! - Streaming path: works with local files and gzip/zstd.
//! - Fast local path: feature `fast_local`, uncompressed local UTF-8 only.
//!
//! Data shape:
//! - `CsvIngestSummary { row_count, headers }`
//! - Streaming rows: [`ByteRecord`] (access with `get(idx) -> Option<&[u8]>`)
#![cfg_attr(docsrs, feature(doc_cfg))]
//
mod codec;
#[cfg(feature = "fast_local")]
mod fast;
mod io;
mod options;
mod parser;

pub use crate::codec::{DecodePolicy, TranscodingError};
#[cfg(feature = "fast_local")]
pub use crate::fast::fast_local_process;
pub use crate::io::{build_csv_reader, reader_from_path, BoxedCsvReader, CsvMeta};
pub use crate::options::{CsvHeaderMode, CsvOptions, CsvTerminator, CsvTrim};
pub use crate::parser::{summarize_csv_path, summarize_csv_stream, CsvParser};
pub use csv_async::ByteRecord;

use thiserror::Error;

/// A count-and-header summary of a parsed CSV stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvIngestSummary {
    pub row_count: u64,
    pub headers: Vec<String>,
}

/// Error returned by every public parsing path in this crate.
#[derive(Debug, Error)]
pub enum CsvIngestError {
    #[error("Missing required header: {0}")]
    MissingHeader(String),
    #[error("Row {row} is missing required field: {header}")]
    MissingRequiredField { row: u64, header: String },
    /// A fixed-width parser encountered a record with a different width.
    /// `row` is absent only when the underlying parser provides no position.
    #[error("Ragged row: got {actual} fields, expected {expected}")]
    RaggedRow {
        row: Option<u64>,
        expected: u64,
        actual: u64,
    },
    /// The selected parser cannot represent the configured CSV dialect.
    #[error("Unsupported CSV dialect: {0}")]
    UnsupportedDialect(String),
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
    /// A byte header could not be represented by the string-based summary API.
    #[error(transparent)]
    InvalidUtf8(#[from] std::str::Utf8Error),
    /// A string-oriented `csv_async` operation encountered invalid UTF-8.
    #[error("Invalid UTF-8 CSV field: {0}")]
    InvalidCsvUtf8(#[source] csv_async::Error),
    /// Input declared as a non-UTF-8 encoding contained malformed bytes.
    #[error("Invalid encoded input: {0}")]
    InvalidEncoding(#[source] std::io::Error),
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),
    /// A CSV parser failure that is neither I/O nor a ragged record.
    #[error("CSV syntax error: {0}")]
    CsvSyntax(#[source] csv_async::Error),
    #[cfg(feature = "fast_local")]
    #[error("Fast-local parser worker panicked")]
    FastLocalWorkerPanicked,
}

pub type CsvResult<T> = std::result::Result<T, CsvIngestError>;

impl From<std::io::Error> for CsvIngestError {
    fn from(error: std::io::Error) -> Self {
        if error
            .get_ref()
            .is_some_and(|source| source.is::<TranscodingError>())
        {
            Self::InvalidEncoding(error)
        } else {
            Self::Io(error)
        }
    }
}

impl From<csv_async::Error> for CsvIngestError {
    fn from(error: csv_async::Error) -> Self {
        if let csv_async::ErrorKind::UnequalLengths {
            pos,
            expected_len,
            len,
        } = error.kind()
        {
            return Self::RaggedRow {
                row: pos.as_ref().map(csv_async::Position::record),
                expected: *expected_len,
                actual: *len,
            };
        }

        if matches!(error.kind(), csv_async::ErrorKind::Utf8 { .. }) {
            return Self::InvalidCsvUtf8(error);
        }

        if error.is_io_error() {
            if let csv_async::ErrorKind::Io(error) = error.into_kind() {
                return error.into();
            }
            unreachable!("is_io_error guarantees an I/O error kind");
        }

        Self::CsvSyntax(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn reports_a_missing_required_header() {
        let error = summarize_csv_stream(
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
        let error = summarize_csv_stream(Cursor::new(b"sku,value\nA\n"), &["value"], &options)
            .await
            .expect_err("short row must fail");

        assert!(matches!(
            error,
            CsvIngestError::MissingRequiredField { row: 1, header } if header == "value"
        ));
    }

    #[tokio::test]
    async fn strict_rows_must_match_the_header_width() {
        let error = summarize_csv_stream(
            Cursor::new(b"sku,value\nA\n"),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect_err("ragged row must fail by default");

        assert!(matches!(
            error,
            CsvIngestError::RaggedRow {
                row: Some(1),
                expected: 2,
                actual: 1
            }
        ));
    }

    #[tokio::test]
    async fn empty_and_header_only_inputs_have_no_rows() {
        let empty = summarize_csv_stream(Cursor::new(b""), &[], &CsvOptions::default())
            .await
            .expect("parse empty input");
        let header_only = summarize_csv_stream(
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
        let summary = summarize_csv_stream(Cursor::new(b"A,1\nB,2\n"), &[], &options)
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
        let error = summarize_csv_stream(Cursor::new(b"A,1\n"), &["sku"], &options)
            .await
            .expect_err("required headers without a header row must fail");

        assert!(matches!(error, CsvIngestError::UnsupportedDialect(_)));
    }

    #[tokio::test]
    async fn delimiter_terminator_trimming_quotes_and_bom_follow_options() {
        let options = CsvOptions {
            delimiter: b';',
            terminator: CsvTerminator::Any(b'$'),
            trim: CsvTrim::All,
            ..CsvOptions::default()
        };
        let summary = summarize_csv_stream(
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
        let summary = summarize_csv_stream(
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
            summarize_csv_stream(Cursor::new(b"sku,value\n A , 1 \n"), &["sku"], &field_trim)
                .await
                .expect("trim fields");
        assert_eq!(summary.row_count, 1);
    }

    #[tokio::test]
    async fn invalid_utf8_headers_return_a_typed_error() {
        let error = summarize_csv_stream(
            Cursor::new(b"sku,\xff\nA,1\n"),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect_err("invalid UTF-8 header must fail");

        assert!(matches!(error, CsvIngestError::InvalidUtf8(_)));
    }

    #[tokio::test]
    async fn csv_utf8_errors_are_not_reported_as_syntax_errors() {
        let mut reader = csv_async::AsyncReader::from_reader(Cursor::new(b"sku,\xff\n"));
        let error = reader
            .headers()
            .await
            .expect_err("string headers must validate UTF-8");
        let error = CsvIngestError::from(error);

        assert!(matches!(error, CsvIngestError::InvalidCsvUtf8(_)));
    }

    #[tokio::test]
    async fn invalid_transcoded_input_is_not_reported_as_generic_io() {
        let meta = CsvMeta {
            charset: encoding_rs::SHIFT_JIS,
            ..CsvMeta::default()
        };
        let (reader, _) = build_csv_reader(Cursor::new(vec![0x82, 0x20]), meta)
            .expect("construct transcoding reader");
        let error = summarize_csv_stream(reader, &[], &CsvOptions::default())
            .await
            .expect_err("malformed encoded input must fail");

        assert!(matches!(error, CsvIngestError::InvalidEncoding(_)));
    }

    #[tokio::test]
    async fn path_io_failures_keep_the_io_variant() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let missing = directory.path().join("missing.csv");
        let error = summarize_csv_path(&missing, &[], &CsvOptions::default())
            .await
            .expect_err("missing path must fail");

        assert!(matches!(
            error,
            CsvIngestError::Io(error) if error.kind() == std::io::ErrorKind::NotFound
        ));
    }
}
