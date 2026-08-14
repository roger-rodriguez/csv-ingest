use csv_ingest::{ByteRecord, CsvOptions, CsvParser};
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (mut parser, _meta) = CsvParser::from_path(
        Path::new("./data/sample.csv.gz"),
        &["sku"],
        &CsvOptions::default(),
    )
    .await?;
    let sku_index = parser.header_index("sku").expect("required header");
    let mut record = ByteRecord::new();

    while parser.read_record(&mut record).await? {
        let sku = record.get(sku_index).expect("validated required field");
        println!("{}", String::from_utf8_lossy(sku));
    }
    Ok(())
}
