#![cfg(feature = "fast_local")]

use csv_ingest::{
    fast_local_process, process_csv_stream, CsvHeaderMode, CsvOptions, CsvTerminator, CsvTrim,
};
use std::io::{Cursor, Write};
use tempfile::NamedTempFile;

async fn parse_both(
    contents: &[u8],
    required_headers: &[&str],
    options: &CsvOptions,
) -> anyhow::Result<(csv_ingest::CsvIngestSummary, csv_ingest::CsvIngestSummary)> {
    let streaming =
        process_csv_stream(Cursor::new(contents.to_vec()), required_headers, options).await?;
    let mut file = NamedTempFile::new()?;
    file.write_all(contents)?;
    let (fast, _) = fast_local_process(file.path(), required_headers, options, false, None)?;
    Ok((streaming, fast))
}

#[tokio::test]
async fn defaults_match_for_bom_and_crlf_input() -> anyhow::Result<()> {
    let (streaming, fast) = parse_both(
        b"\xef\xbb\xbfsku,value\r\nA,1\r\nB,2",
        &["sku"],
        &CsvOptions::default(),
    )
    .await?;

    assert_eq!(streaming, fast);
    assert_eq!(streaming.row_count, 2);
    assert_eq!(streaming.headers, ["sku", "value"]);
    Ok(())
}

#[tokio::test]
async fn empty_input_behavior_matches() -> anyhow::Result<()> {
    let (streaming, fast) = parse_both(b"", &[], &CsvOptions::default()).await?;
    assert_eq!(streaming, fast);

    let streaming =
        process_csv_stream(Cursor::new(Vec::new()), &["sku"], &CsvOptions::default()).await;
    let file = NamedTempFile::new()?;
    let fast = fast_local_process(file.path(), &["sku"], &CsvOptions::default(), false, None);
    assert!(streaming.is_err());
    assert!(fast.is_err());
    Ok(())
}

#[tokio::test]
async fn custom_dialect_and_headerless_mode_match() -> anyhow::Result<()> {
    let custom = CsvOptions {
        delimiter: b';',
        terminator: CsvTerminator::Any(b'$'),
        trim: CsvTrim::All,
        ..CsvOptions::default()
    };
    let (streaming, fast) = parse_both(b" sku ; value $ A ; 1 $ B ; 2 ", &["sku"], &custom).await?;
    assert_eq!(streaming, fast);

    let headerless = CsvOptions {
        headers: CsvHeaderMode::Absent,
        ..CsvOptions::default()
    };
    let (streaming, fast) = parse_both(b"A,1\nB,2", &[], &headerless).await?;
    assert_eq!(streaming, fast);
    assert_eq!(streaming.row_count, 2);
    assert!(streaming.headers.is_empty());
    Ok(())
}

#[tokio::test]
async fn strict_and_flexible_row_width_behavior_matches() -> anyhow::Result<()> {
    let contents = b"sku,value\nA\n";
    let streaming = process_csv_stream(
        Cursor::new(contents.to_vec()),
        &["sku"],
        &CsvOptions::default(),
    )
    .await;
    let mut file = NamedTempFile::new()?;
    file.write_all(contents)?;
    let fast = fast_local_process(file.path(), &["sku"], &CsvOptions::default(), false, None);
    assert!(streaming.is_err());
    assert!(fast.is_err());

    let flexible = CsvOptions {
        flexible: true,
        ..CsvOptions::default()
    };
    let (streaming, fast) = parse_both(contents, &["sku"], &flexible).await?;
    assert_eq!(streaming, fast);
    Ok(())
}

#[tokio::test]
async fn fast_local_rejects_quoted_data_instead_of_disagreeing() -> anyhow::Result<()> {
    let contents = b"sku,value\nA,\"quoted,value\"\n";
    let streaming = process_csv_stream(
        Cursor::new(contents.to_vec()),
        &["sku"],
        &CsvOptions::default(),
    )
    .await?;
    assert_eq!(streaming.row_count, 1);

    let mut file = NamedTempFile::new()?;
    file.write_all(contents)?;
    let error = fast_local_process(file.path(), &["sku"], &CsvOptions::default(), false, None)
        .expect_err("fast-local quoted input must fail explicitly");
    assert!(error.to_string().contains("only unquoted CSV"));
    Ok(())
}
