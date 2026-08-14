use crate::{CsvResult, DecodePolicy};
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
    /// How malformed encoded input is handled while transcoding (defaults to strict).
    pub decode_policy: DecodePolicy,
}

impl Default for CsvMeta {
    fn default() -> Self {
        Self {
            content_type: String::new(),
            content_encoding: String::new(),
            name_hint: String::new(),
            charset: encoding_rs::UTF_8,
            decode_policy: DecodePolicy::Strict,
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
        let transcoder = Transcoder::new(meta.charset, meta.decode_policy);
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
    use std::io::{self, Cursor};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(bytes).await.expect("write gzip input");
        encoder.shutdown().await.expect("finish gzip stream");
        encoder.into_inner()
    }

    async fn zstd(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = ZstdEncoder::new(Vec::new());
        encoder.write_all(bytes).await.expect("write zstd input");
        encoder.shutdown().await.expect("finish zstd stream");
        encoder.into_inner()
    }

    async fn try_decode(raw: Vec<u8>, meta: CsvMeta) -> io::Result<(Vec<u8>, CsvMeta)> {
        let (mut reader, normalized) = build_csv_reader(Cursor::new(raw), meta);
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).await?;
        Ok((decoded, normalized))
    }

    async fn decode(raw: Vec<u8>, meta: CsvMeta) -> (Vec<u8>, CsvMeta) {
        try_decode(raw, meta).await.expect("read decoded")
    }

    #[tokio::test]
    async fn plain_utf8_passes_through_and_preserves_metadata() {
        let meta = CsvMeta {
            content_type: "text/csv".into(),
            name_hint: "rows.csv".into(),
            ..Default::default()
        };

        let (decoded, normalized) = decode(b"sku\nA\n".to_vec(), meta).await;

        assert_eq!(decoded, b"sku\nA\n");
        assert_eq!(normalized.content_type, "text/csv");
        assert_eq!(normalized.name_hint, "rows.csv");
    }

    #[tokio::test]
    async fn gzip_is_decoded_from_content_encoding() {
        let expected = b"sku,value\nA,1\n";
        let compressed = gzip(expected).await;
        let meta = CsvMeta {
            content_encoding: "gzip".into(),
            ..Default::default()
        };

        let (decoded, _) = decode(compressed, meta).await;

        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn gzip_is_decoded_from_content_type() {
        let expected = b"sku\nA\n";
        let compressed = gzip(expected).await;
        let meta = CsvMeta {
            content_type: "application/x-gzip".into(),
            ..Default::default()
        };

        let (decoded, _) = decode(compressed, meta).await;

        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn zstd_is_decoded_from_content_type() {
        let expected = b"sku,value\nA,1\n";
        let compressed = zstd(expected).await;
        let meta = CsvMeta {
            content_type: "application/zstd".into(),
            ..Default::default()
        };

        let (decoded, _) = decode(compressed, meta).await;

        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn filename_extensions_select_compression() {
        let expected = b"sku\nA\n";
        let gzip_meta = CsvMeta {
            name_hint: "rows.csv.gz".into(),
            ..Default::default()
        };
        let zstd_meta = CsvMeta {
            name_hint: "rows.csv.zst".into(),
            ..Default::default()
        };

        let (gzip_decoded, _) = decode(gzip(expected).await, gzip_meta).await;
        let (zstd_decoded, _) = decode(zstd(expected).await, zstd_meta).await;

        assert_eq!(gzip_decoded, expected);
        assert_eq!(zstd_decoded, expected);
    }

    #[tokio::test]
    async fn non_utf8_input_is_transcoded() {
        let meta = CsvMeta {
            charset: encoding_rs::WINDOWS_1252,
            ..Default::default()
        };

        let (decoded, _) = decode(b"name\ncaf\xe9\n".to_vec(), meta).await;

        assert_eq!(decoded, "name\ncafé\n".as_bytes());
    }

    #[tokio::test]
    async fn malformed_transcoded_input_is_rejected_by_default() {
        let meta = CsvMeta {
            charset: encoding_rs::SHIFT_JIS,
            ..Default::default()
        };

        let error = try_decode(vec![0x82, 0x20], meta)
            .await
            .expect_err("malformed input must fail");
        let typed = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<crate::TranscodingError>())
            .expect("typed transcoding error");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(typed.encoding(), "Shift_JIS");
    }

    #[tokio::test]
    async fn replacement_mode_is_an_explicit_opt_in() {
        let meta = CsvMeta {
            charset: encoding_rs::SHIFT_JIS,
            decode_policy: DecodePolicy::Replace,
            ..Default::default()
        };

        let (decoded, normalized) = decode(vec![0x82, 0x20], meta).await;

        assert_eq!(decoded, "� ".as_bytes());
        assert_eq!(normalized.decode_policy, DecodePolicy::Replace);
    }

    #[tokio::test]
    async fn incomplete_trailing_sequence_is_rejected_at_eof() {
        let meta = CsvMeta {
            charset: encoding_rs::SHIFT_JIS,
            ..Default::default()
        };

        let error = try_decode(vec![0x82], meta)
            .await
            .expect_err("incomplete trailing sequence must fail");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error
            .get_ref()
            .and_then(|source| source.downcast_ref::<crate::TranscodingError>())
            .is_some());
    }

    #[tokio::test]
    async fn reader_from_path_sets_plain_and_zstd_metadata() {
        let plain = tempfile::Builder::new()
            .suffix(".csv")
            .tempfile()
            .expect("create plain fixture");
        std::fs::write(plain.path(), b"sku\nA\n").expect("write plain fixture");

        let zstd_file = tempfile::Builder::new()
            .suffix(".zst")
            .tempfile()
            .expect("create zstd fixture");
        std::fs::write(zstd_file.path(), zstd(b"sku\nA\n").await).expect("write zstd fixture");

        let (mut plain_reader, plain_meta) = reader_from_path(plain.path())
            .await
            .expect("open plain fixture");
        let mut plain_decoded = Vec::new();
        plain_reader
            .read_to_end(&mut plain_decoded)
            .await
            .expect("read plain fixture");

        let (mut zstd_reader, zstd_meta) = reader_from_path(zstd_file.path())
            .await
            .expect("open zstd fixture");
        let mut zstd_decoded = Vec::new();
        zstd_reader
            .read_to_end(&mut zstd_decoded)
            .await
            .expect("read zstd fixture");

        assert_eq!(plain_decoded, b"sku\nA\n");
        assert_eq!(plain_meta.content_type, "text/csv");
        assert_eq!(zstd_decoded, b"sku\nA\n");
        assert_eq!(zstd_meta.content_encoding, "zstd");
    }
}
