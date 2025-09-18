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

#[cfg(feature = "fast_local")]
pub use crate::fast::fast_local_process;
pub use crate::io::{build_csv_reader, reader_from_path, CsvMeta};

use csv_async::{AsyncReaderBuilder, ByteRecord};
use thiserror::Error;
use tokio::io::AsyncRead;

/// Result summary (keep it simple/minimal)
#[derive(Debug)]
pub struct CsvIngestSummary {
    pub row_count: usize,
    pub headers: Vec<String>,
}

/// Error type returned by this crate when not using `anyhow`.
#[derive(Debug, Error)]
pub enum CsvIngestError {
    #[error("Missing required header: {0}")]
    MissingHeader(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Csv(#[from] csv_async::Error),
}

pub type CsvResult<T> = std::result::Result<T, CsvIngestError>;

/// Streaming parse with required header validation.
/// This mirrors your existing logic as closely as possible.
pub async fn process_csv_stream<R>(
    reader: R,
    required_headers: &[&str],
) -> CsvResult<CsvIngestSummary>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut rdr = AsyncReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        // Larger internal buffer reduces syscalls and allocator churn
        .buffer_capacity(1 << 20) // 1 MiB
        .create_reader(reader);

    let headers = rdr.headers().await?.clone();
    let required_indices = required_headers
        .iter()
        .map(|req_h| {
            headers
                .iter()
                .position(|h| h == *req_h)
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
                return Err(CsvIngestError::MissingHeader(
                    required_headers[i].to_string(),
                ));
            }
        }
    }

    Ok(CsvIngestSummary {
        row_count,
        headers: headers.iter().map(|s| s.to_string()).collect(),
    })
}
