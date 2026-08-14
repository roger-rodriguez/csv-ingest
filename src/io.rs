use crate::{CsvIngestError, CsvResult, DecodePolicy};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Compression {
    Gzip,
    Zstd,
}

impl Compression {
    fn name(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        }
    }
}

fn compression_from_content_encoding(value: &str) -> CsvResult<Option<Compression>> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("identity") {
        return Ok(None);
    }
    if value.contains(',') {
        return Err(CsvIngestError::UnsupportedStackedContentEncoding(
            value.to_string(),
        ));
    }
    if value.eq_ignore_ascii_case("gzip") {
        Ok(Some(Compression::Gzip))
    } else if value.eq_ignore_ascii_case("zstd") {
        Ok(Some(Compression::Zstd))
    } else {
        Err(CsvIngestError::UnsupportedContentEncoding(
            value.to_string(),
        ))
    }
}

fn compression_from_content_type(value: &str) -> Option<Compression> {
    let media_type = value.split(';').next().unwrap_or_default().trim();
    if media_type.eq_ignore_ascii_case("application/gzip")
        || media_type.eq_ignore_ascii_case("application/x-gzip")
    {
        Some(Compression::Gzip)
    } else if media_type.eq_ignore_ascii_case("application/zstd") {
        Some(Compression::Zstd)
    } else {
        None
    }
}

fn compression_from_name_hint(value: &str) -> Option<Compression> {
    let extension = Path::new(value).extension()?.to_str()?;
    if extension.eq_ignore_ascii_case("gz") {
        Some(Compression::Gzip)
    } else if extension.eq_ignore_ascii_case("zst") {
        Some(Compression::Zstd)
    } else {
        None
    }
}

fn reject_conflict(
    higher: Compression,
    higher_source: &'static str,
    lower: Option<Compression>,
    lower_source: &'static str,
) -> CsvResult<()> {
    if let Some(lower) = lower {
        if lower != higher {
            return Err(CsvIngestError::ConflictingCompressionMetadata {
                higher_source,
                higher: higher.name(),
                lower_source,
                lower: lower.name(),
            });
        }
    }
    Ok(())
}

fn detect_compression(meta: &CsvMeta) -> CsvResult<Option<Compression>> {
    let content_encoding = compression_from_content_encoding(&meta.content_encoding)?;
    let content_type = compression_from_content_type(&meta.content_type);
    let name_hint = compression_from_name_hint(&meta.name_hint);

    if let Some(compression) = content_encoding {
        reject_conflict(
            compression,
            "Content-Encoding",
            content_type,
            "Content-Type",
        )?;
        reject_conflict(
            compression,
            "Content-Encoding",
            name_hint,
            "filename extension",
        )?;
        return Ok(Some(compression));
    }
    if let Some(compression) = content_type {
        reject_conflict(compression, "Content-Type", name_hint, "filename extension")?;
        return Ok(Some(compression));
    }
    Ok(name_hint)
}

/// Wrap an [`AsyncRead`] with optional decompression and UTF-8 transcoding.
///
/// Compression signals are evaluated in this order: `Content-Encoding`,
/// compression-specific `Content-Type`, then filename extension. Gzip/zstd
/// disagreements and unsupported or stacked content encodings return an error.
pub fn build_csv_reader<R>(
    raw: R,
    meta: CsvMeta,
) -> CsvResult<(Box<dyn AsyncRead + Unpin + Send>, CsvMeta)>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let normalized_meta = meta.clone();
    let compression = detect_compression(&meta)?;

    // Use a larger buffer for fewer syscalls (1 MiB)
    let buf = BufReader::with_capacity(1 << 20, raw);
    let decompressed: Box<dyn AsyncRead + Unpin + Send> = match compression {
        Some(Compression::Gzip) => Box::new(GzipDecoder::new(buf)),
        Some(Compression::Zstd) => Box::new(ZstdDecoder::new(buf)),
        None => Box::new(buf),
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

    Ok((stream_reader, normalized_meta))
}

/// Build a reader from a local file path (lightweight meta from extension).
pub async fn reader_from_path(
    path: &Path,
) -> CsvResult<(Box<dyn AsyncRead + Unpin + Send>, CsvMeta)> {
    let file = File::open(path).await?;
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();

    let meta = CsvMeta {
        name_hint: name,
        ..Default::default()
    };
    build_csv_reader(file, meta)
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

    async fn try_decode(raw: Vec<u8>, meta: CsvMeta) -> CsvResult<(Vec<u8>, CsvMeta)> {
        let (mut reader, normalized) = build_csv_reader(Cursor::new(raw), meta)?;
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
    async fn mixed_case_extensions_select_compression() {
        let expected = b"sku\nA\n";
        let gzip_meta = CsvMeta {
            name_hint: "rows.csv.GZ".into(),
            ..Default::default()
        };
        let zstd_meta = CsvMeta {
            name_hint: "rows.csv.ZsT".into(),
            ..Default::default()
        };

        let (gzip_decoded, _) = decode(gzip(expected).await, gzip_meta).await;
        let (zstd_decoded, _) = decode(zstd(expected).await, zstd_meta).await;

        assert_eq!(gzip_decoded, expected);
        assert_eq!(zstd_decoded, expected);
    }

    #[tokio::test]
    async fn content_type_parameters_and_case_are_normalized() {
        assert_eq!(
            compression_from_content_type(" Application/GZip ; charset=binary"),
            Some(Compression::Gzip)
        );
        assert_eq!(
            compression_from_content_type("APPLICATION/ZSTD; profile=csv"),
            Some(Compression::Zstd)
        );
        assert_eq!(
            compression_from_content_type("text/csv; charset=utf-8"),
            None
        );
    }

    #[tokio::test]
    async fn matching_compression_signals_are_accepted() {
        let expected = b"sku\nA\n";
        let gzip_meta = CsvMeta {
            content_encoding: " GZIP ".into(),
            content_type: "application/x-gzip; version=1".into(),
            name_hint: "rows.csv.GZ".into(),
            ..Default::default()
        };
        let zstd_meta = CsvMeta {
            content_encoding: "zstd".into(),
            content_type: "application/zstd; profile=csv".into(),
            name_hint: "rows.csv.zst".into(),
            ..Default::default()
        };

        assert_eq!(decode(gzip(expected).await, gzip_meta).await.0, expected);
        assert_eq!(decode(zstd(expected).await, zstd_meta).await.0, expected);
    }

    #[tokio::test]
    async fn content_encoding_precedes_plain_lower_priority_metadata() {
        let expected = b"sku\nA\n";
        let meta = CsvMeta {
            content_encoding: "zstd".into(),
            content_type: "text/csv; charset=utf-8".into(),
            name_hint: "rows.csv".into(),
            ..Default::default()
        };

        let (decoded, _) = decode(zstd(expected).await, meta).await;

        assert_eq!(decoded, expected);
    }

    #[test]
    fn conflicting_compression_signals_are_rejected() {
        let cases = [
            CsvMeta {
                content_encoding: "zstd".into(),
                name_hint: "rows.csv.gz".into(),
                ..Default::default()
            },
            CsvMeta {
                content_encoding: "gzip".into(),
                content_type: "application/zstd".into(),
                ..Default::default()
            },
            CsvMeta {
                content_type: "application/gzip".into(),
                name_hint: "rows.csv.zst".into(),
                ..Default::default()
            },
        ];

        for meta in cases {
            let error = detect_compression(&meta).expect_err("conflict must fail");
            assert!(matches!(
                error,
                CsvIngestError::ConflictingCompressionMetadata { .. }
            ));
        }
    }

    #[test]
    fn stacked_and_unsupported_content_encodings_are_rejected() {
        let stacked = CsvMeta {
            content_encoding: "gzip, zstd".into(),
            ..Default::default()
        };
        let unsupported = CsvMeta {
            content_encoding: "br".into(),
            name_hint: "rows.csv.gz".into(),
            ..Default::default()
        };

        assert!(matches!(
            detect_compression(&stacked),
            Err(CsvIngestError::UnsupportedStackedContentEncoding(value))
                if value == "gzip, zstd"
        ));
        assert!(matches!(
            detect_compression(&unsupported),
            Err(CsvIngestError::UnsupportedContentEncoding(value)) if value == "br"
        ));
    }

    #[tokio::test]
    async fn missing_or_plain_metadata_passes_csv_through() {
        let expected = b"sku\nA\n".to_vec();
        let (missing, _) = decode(expected.clone(), CsvMeta::default()).await;
        let (plain, _) = decode(
            expected.clone(),
            CsvMeta {
                content_encoding: "identity".into(),
                content_type: "text/csv; charset=utf-8".into(),
                name_hint: "rows.CSV".into(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(missing, expected);
        assert_eq!(plain, expected);
    }

    #[tokio::test]
    async fn plain_content_type_allows_filename_fallback() {
        let expected = b"sku\nA\n";
        let meta = CsvMeta {
            content_type: "text/csv; charset=utf-8".into(),
            name_hint: "rows.csv.gz".into(),
            ..Default::default()
        };

        let (decoded, _) = decode(gzip(expected).await, meta).await;

        assert_eq!(decoded, expected);
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
        let CsvIngestError::Io(error) = error else {
            panic!("expected I/O error");
        };
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
        let CsvIngestError::Io(error) = error else {
            panic!("expected I/O error");
        };

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error
            .get_ref()
            .and_then(|source| source.downcast_ref::<crate::TranscodingError>())
            .is_some());
    }

    #[tokio::test]
    async fn reader_from_path_uses_filename_metadata_for_plain_and_zstd_files() {
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
        assert!(plain_meta.name_hint.ends_with(".csv"));
        assert!(plain_meta.content_type.is_empty());
        assert_eq!(zstd_decoded, b"sku\nA\n");
        assert!(zstd_meta.name_hint.ends_with(".zst"));
        assert!(zstd_meta.content_encoding.is_empty());
    }
}
