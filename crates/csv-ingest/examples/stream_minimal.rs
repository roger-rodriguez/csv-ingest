use csv_ingest::{summarize_csv_path, CsvOptions};
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = Path::new("./data/sample.csv.gz");
    let (summary, _meta) = summarize_csv_path(path, &["sku"], &CsvOptions::default()).await?;
    println!("rows={}, headers={:?}", summary.row_count, summary.headers);
    Ok(())
}
