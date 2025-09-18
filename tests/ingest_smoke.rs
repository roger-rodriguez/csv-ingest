use csv_ingest::{process_csv_stream, reader_from_path};
use std::{fs::File, io::Write, path::PathBuf, process::Command};

#[tokio::test]
async fn parses_gzip_and_counts_rows() -> anyhow::Result<()> {
    // Create small CSV
    let dir = tempfile::tempdir()?;
    let csv_path = dir.path().join("tiny.csv");
    let mut f = File::create(&csv_path)?;
    writeln!(f, "sku,col1")?;
    for i in 0..100_000 {
        writeln!(f, "SKU{i:06},{i}")?;
    }

    // gzip it (use system gzip for speed)
    let gz_path: PathBuf = dir.path().join("tiny.csv.gz");
    let status = Command::new("bash")
        .arg("-lc")
        .arg(format!(
            "gzip -c {} > {}",
            csv_path.display(),
            gz_path.display()
        ))
        .status()?;
    assert!(status.success());

    // Parse via library
    let (reader, _meta) = reader_from_path(&gz_path).await?;
    let summary = process_csv_stream(reader, &["sku"]).await?;

    assert_eq!(summary.row_count, 100_000);
    assert_eq!(summary.headers, vec!["sku".to_string(), "col1".to_string()]);
    Ok(())
}
