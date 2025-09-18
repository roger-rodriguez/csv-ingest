use csv_async::{AsyncReaderBuilder, ByteRecord};
use csv_ingest::reader_from_path;
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = Path::new("./data/sample.csv.gz");
    let (reader, _meta) = reader_from_path(path).await?;

    let mut rdr = AsyncReaderBuilder::new()
        .has_headers(true)
        .buffer_capacity(1 << 20)
        .create_reader(reader);

    let headers = rdr.headers().await?.clone();
    let required = ["sku"];
    let idxs: Vec<usize> = required
        .iter()
        .map(|h| {
            headers
                .iter()
                .position(|x| x == *h)
                .ok_or_else(|| anyhow::anyhow!("missing {h}"))
        })
        .collect::<anyhow::Result<_>>()?;

    let mut rec = ByteRecord::new();
    while rdr.read_byte_record(&mut rec).await? {
        let sku = rec.get(idxs[0]).unwrap();
        let _sku_str = std::str::from_utf8(sku).unwrap_or("");
    }
    Ok(())
}
