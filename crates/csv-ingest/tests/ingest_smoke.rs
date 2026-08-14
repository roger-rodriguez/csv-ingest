use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
use csv_ingest::{ByteRecord, CsvIngestError, CsvOptions, CsvParser};
use std::io::Cursor;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Copy, Debug)]
enum Compression {
    Gzip,
    Zstd,
}

impl Compression {
    fn suffix(self) -> &'static str {
        match self {
            Self::Gzip => ".csv.gz",
            Self::Zstd => ".csv.zst",
        }
    }
}

async fn encode(contents: &[u8], compression: Compression) -> std::io::Result<Vec<u8>> {
    match compression {
        Compression::Gzip => {
            let mut encoder = GzipEncoder::new(Vec::new());
            encoder.write_all(contents).await?;
            encoder.shutdown().await?;
            Ok(encoder.into_inner())
        }
        Compression::Zstd => {
            let mut encoder = ZstdEncoder::new(Vec::new());
            encoder.write_all(contents).await?;
            encoder.shutdown().await?;
            Ok(encoder.into_inner())
        }
    }
}

async fn compressed_fixture(
    contents: &[u8],
    compression: Compression,
) -> anyhow::Result<NamedTempFile> {
    let file = tempfile::Builder::new()
        .suffix(compression.suffix())
        .tempfile()?;
    std::fs::write(file.path(), encode(contents, compression).await?)?;
    Ok(file)
}

fn assert_fields(record: &ByteRecord, expected: &[&[u8]]) {
    assert_eq!(record.iter().collect::<Vec<_>>(), expected);
}

#[tokio::test]
async fn compressed_local_paths_preserve_complex_csv_records() -> anyhow::Result<()> {
    let contents = b"sku,description\n\
A,\"quoted, delimiter\"\n\
B,\"escaped \"\"quote\"\"\"\n\
C,\"embedded\nnewline\"\n";
    let expected_rows: &[&[&[u8]]] = &[
        &[b"A", b"quoted, delimiter"],
        &[b"B", b"escaped \"quote\""],
        &[b"C", b"embedded\nnewline"],
    ];

    for compression in [Compression::Gzip, Compression::Zstd] {
        let file = compressed_fixture(contents, compression).await?;
        let (mut parser, meta) =
            CsvParser::from_path(file.path(), &["sku", "description"], &CsvOptions::default())
                .await?;

        assert!(meta.name_hint.ends_with(compression.suffix()));
        assert_fields(parser.headers(), &[b"sku", b"description"]);
        for expected in expected_rows {
            let record = parser
                .next_record()
                .await?
                .expect("fixture should contain every expected row");
            assert_fields(record, expected);
        }
        assert!(parser.next_record().await?.is_none());
        assert_eq!(parser.records_read(), expected_rows.len() as u64);
    }

    Ok(())
}

#[tokio::test]
async fn extra_fields_respect_fixed_and_flexible_modes() -> anyhow::Result<()> {
    let contents = b"sku,value\nA,1,extra\n";
    let mut fixed =
        CsvParser::from_reader(Cursor::new(contents), &["sku"], &CsvOptions::default()).await?;
    let error = fixed
        .next_record()
        .await
        .expect_err("extra fields must fail in fixed-width mode");
    assert!(matches!(
        error,
        CsvIngestError::RaggedRow {
            row: Some(1),
            expected: 2,
            actual: 3
        }
    ));

    let options = CsvOptions {
        flexible: true,
        ..CsvOptions::default()
    };
    let mut flexible = CsvParser::from_reader(Cursor::new(contents), &["sku"], &options).await?;
    let record = flexible
        .next_record()
        .await?
        .expect("extra fields must parse in flexible mode");
    assert_fields(record, &[b"A", b"1", b"extra"]);
    assert!(flexible.next_record().await?.is_none());

    Ok(())
}
