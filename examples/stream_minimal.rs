use csv_ingest::{process_csv_stream, reader_from_path, CsvOptions};
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = Path::new("./data/sample.csv.gz");
    let (reader, _meta) = reader_from_path(path).await?;

    let summary = process_csv_stream(reader, &["sku"], &CsvOptions::default()).await?;
    println!("rows={}, headers={:?}", summary.row_count, summary.headers);
    Ok(())
}
