use crate::CsvResult;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::codec::FramedRead;
use tokio_util::io::StreamReader;

use crate::codec::Transcoder;

#[derive(Debug, Clone)]
pub struct CsvMeta {
    /// e.g. "application/gzip" or "text/csv"
    pub content_type: String,
    /// e.g. "gzip", "zstd", "gzip, or empty
    pub content_encoding: String,
    /// just the key/filename (used for extension fallback)
    pub name_hint: String,
    /// Which character encoding to expect (defaults to UTF-8)
    pub charset: &'static encoding_rs::Encoding,
}

impl Default for CsvMeta {
    fn default() -> Self {
        Self {
            content_type: String::new(),
            content_encoding: String::new(),
            name_hint: String::new(),
            charset: encoding_rs::UTF_8,
        }
    }
}

/// From a generic AsyncRead, wrap with optional decompression and UTF-8 transcoding.
/// Returns an AsyncRead suitable for csv_async plus the normalized meta we used.
pub fn build_csv_reader<R>(raw: R, meta: CsvMeta) -> (impl AsyncRead + Unpin + Send, CsvMeta)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    // 1) decompression choice: encoding -> type -> extension
    let normalized_meta = meta.clone();
    let ce = meta.content_encoding.to_ascii_lowercase();
    let ct = meta.content_type.to_ascii_lowercase();

    let is_gzip = ce.split(',').any(|s| s.trim() == "gzip")
        || matches!(ct.as_str(), "application/gzip" | "application/x-gzip")
        || meta.name_hint.ends_with(".gz");

    let is_zstd = ce.split(',').any(|s| s.trim() == "zstd")
        || ct == "application/zstd"
        || meta.name_hint.ends_with(".zst");

    // Use a larger buffer for fewer syscalls (1 MiB)
    let buf = BufReader::with_capacity(1 << 20, raw);
    let decompressed: Box<dyn AsyncRead + Unpin + Send> = if is_gzip {
        Box::new(GzipDecoder::new(buf))
    } else if is_zstd {
        Box::new(ZstdDecoder::new(buf))
    } else {
        Box::new(buf)
    };

    // 2) transcoding to UTF-8 only when charset != UTF-8 to avoid extra copies
    let stream_reader: Box<dyn AsyncRead + Unpin + Send> = if meta.charset == encoding_rs::UTF_8 {
        // No transcoding needed; pass through as bytes
        Box::new(decompressed)
    } else {
        let transcoder = Transcoder::new(meta.charset);
        let framed = FramedRead::new(decompressed, transcoder);
        Box::new(StreamReader::new(framed))
    };

    // 3) return a Tokio AsyncRead
    (stream_reader, normalized_meta)
}

/// Build a reader from a local file path (lightweight meta from extension).
pub async fn reader_from_path(path: &Path) -> CsvResult<(impl AsyncRead + Unpin + Send, CsvMeta)> {
    let file = File::open(path).await?;
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();

    // best-effort content-type/encoding from extension only (minimal change)
    let mut meta = CsvMeta {
        name_hint: name,
        ..Default::default()
    };

    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    match ext {
        "gz" => {
            meta.content_type = "application/gzip".into();
            meta.content_encoding = "gzip".into();
        }
        "zst" => {
            meta.content_type = "application/zstd".into();
            meta.content_encoding = "zstd".into();
        }
        _ => {
            meta.content_type = "text/csv".into();
        }
    }

    Ok(build_csv_reader(file, meta))
}
