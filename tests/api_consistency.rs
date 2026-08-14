use csv_ingest::{
    build_csv_reader, summarize_csv_stream, CsvIngestSummary, CsvMeta, CsvOptions, CsvParser,
    CsvResult,
};
use std::io::Cursor;

#[tokio::test]
async fn borrowed_readers_and_u64_counts_are_public_contracts() {
    let bytes = b"sku,value\nA,1\nB,2\n".to_vec();
    let mut parser = CsvParser::from_reader(
        Cursor::new(bytes.as_slice()),
        &["sku"],
        &CsvOptions::default(),
    )
    .await
    .expect("construct parser over borrowed bytes");

    while parser.next_record().await.expect("read record").is_some() {}
    let records_read: u64 = parser.records_read();
    assert_eq!(records_read, 2);

    let borrowed = Cursor::new(bytes.as_slice());
    let (reader, _) =
        build_csv_reader(borrowed, CsvMeta::default()).expect("normalize borrowed reader");
    let result: CsvResult<CsvIngestSummary> =
        summarize_csv_stream(reader, &["sku"], &CsvOptions::default()).await;
    let summary = result.expect("summarize borrowed reader");
    let row_count: u64 = summary.row_count;
    assert_eq!(row_count, 2);
}

#[cfg(feature = "fast_local")]
#[test]
fn fast_local_uses_the_shared_result_and_count_types() {
    use csv_ingest::fast_local_process;
    use std::io::Write;

    let mut file = tempfile::NamedTempFile::new().expect("create fixture");
    file.write_all(b"sku,value\nA,1\n").expect("write fixture");

    let result: CsvResult<(CsvIngestSummary, Option<u32>)> = fast_local_process(
        file.path(),
        &["sku"],
        &CsvOptions::default(),
        false,
        Some(1u64),
    );
    let (summary, _) = result.expect("parse fast-local fixture");
    let row_count: u64 = summary.row_count;
    assert_eq!(row_count, 1);
}
