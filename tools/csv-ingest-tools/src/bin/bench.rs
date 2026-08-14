use clap::{Arg, ArgAction, Command};
use crc32fast::Hasher as Crc32;
use csv_ingest::{
    reader_from_path, summarize_csv_stream, BoxedCsvReader, ByteRecord, CsvMeta, CsvOptions,
    CsvParser,
};
#[cfg(feature = "fast_local")]
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = Command::new("bench")
        .arg(Arg::new("path").long("path").value_parser(clap::value_parser!(PathBuf)))
        .arg(Arg::new("required").long("required").action(clap::ArgAction::Append).required(true))
        .arg(Arg::new("verify").long("verify").help("Enable strict verification: row width checks and CRC32 over fields").action(ArgAction::SetTrue))
        .arg(Arg::new("limit").long("limit").help("Stop after N rows (for faster verify)").value_parser(clap::value_parser!(u64)))
        .arg(Arg::new("fast-local").long("fast-local").help("Use mmap+parallel fast path for local uncompressed UTF-8 files (feature: fast_local)").action(ArgAction::SetTrue))
        .get_matches();

    let required: Vec<String> = matches
        .get_many::<String>("required")
        .unwrap()
        .map(|s| s.to_string())
        .collect();
    let required_refs: Vec<&str> = required.iter().map(|s| s.as_str()).collect();
    let csv_options = CsvOptions::default();

    let start = Instant::now();

    let (reader, _meta): (BoxedCsvReader<'static>, CsvMeta) =
        if let Some(p) = matches.get_one::<PathBuf>("path") {
            #[cfg(feature = "fast_local")]
            if matches.get_flag("fast-local")
                && p.extension().and_then(|s| s.to_str()) == Some("csv")
            {
                // Run fast path and print, then exit early
                let start = Instant::now();
                let (res, crc) = csv_ingest::fast_local_process(
                    Path::new(p),
                    &required_refs,
                    &csv_options,
                    matches.get_flag("verify"),
                    matches.get_one::<u64>("limit").copied(),
                )?;
                let elapsed = start.elapsed().as_secs_f64();
                let rps = (res.row_count as f64) / elapsed;
                if let Some(d) = crc {
                    println!(
                    "source={} rows={} headers={:?} crc=0x{d:08x}\nelapsed={:.1}s rows/sec={:.0}",
                    p.display(), res.row_count, res.headers, elapsed, rps
                );
                } else {
                    println!(
                        "source={} rows={} headers={:?}\nelapsed={:.1}s rows/sec={:.0}",
                        p.display(),
                        res.row_count,
                        res.headers,
                        elapsed,
                        rps
                    );
                }
                return Ok(());
            }
            let (r, m) = reader_from_path(p).await?;
            (r, m)
        } else {
            panic!("Provide --path <file>");
        };

    let (summary, crc) = if matches.get_flag("verify") {
        // Use the public record parser and add a checksum over the selected sample.
        let (summary, crc) = verify_and_count(
            reader,
            &required_refs,
            &csv_options,
            matches.get_one::<u64>("limit").copied(),
        )
        .await?;
        (summary, Some(crc))
    } else {
        (
            summarize_csv_stream(reader, &required_refs, &csv_options).await?,
            None,
        )
    };
    let elapsed = start.elapsed().as_secs_f64();
    let rps = (summary.row_count as f64) / elapsed;

    let src = matches
        .get_one::<PathBuf>("path")
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "(unknown)".to_string());
    if let Some(digest) = crc {
        println!(
            "source={} rows={} headers={:?} crc=0x{digest:08x}\nelapsed={:.1}s rows/sec={:.0}",
            src, summary.row_count, summary.headers, elapsed, rps
        );
    } else {
        println!(
            "source={} rows={} headers={:?}\nelapsed={:.1}s rows/sec={:.0}",
            src, summary.row_count, summary.headers, elapsed, rps
        );
    }
    Ok(())
}

async fn verify_and_count<R: tokio::io::AsyncRead + Unpin + Send>(
    reader: R,
    required_headers: &[&str],
    options: &CsvOptions,
    limit: Option<u64>,
) -> anyhow::Result<(csv_ingest::CsvIngestSummary, u32)> {
    let mut parser = CsvParser::from_reader(reader, required_headers, options).await?;
    let headers = parser
        .headers()
        .iter()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(str::to_string)
        .collect();
    let mut record = ByteRecord::new();
    let mut crc = Crc32::new();
    while limit.is_none_or(|limit| parser.records_read() < limit)
        && parser.read_record(&mut record).await?
    {
        for (fi, field) in record.iter().enumerate() {
            if fi > 0 {
                crc.update(&[0x1f]);
            }
            crc.update(field);
        }
    }

    Ok((
        csv_ingest::CsvIngestSummary {
            row_count: parser.records_read(),
            headers,
        },
        crc.finalize(),
    ))
}

#[cfg(all(test, feature = "fast_local"))]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn streaming_and_fast_verification_match() {
        let contents = b"sku,col1,col2\r\nA,1,x\r\nB,2,y\r\nC,3,z\r\nD,4,w";
        let mut file = NamedTempFile::new().expect("create fixture");
        file.write_all(contents).expect("write fixture");

        let reader = tokio::fs::File::open(file.path())
            .await
            .expect("open streaming fixture");
        let options = CsvOptions::default();
        let (streaming_summary, streaming_crc) =
            verify_and_count(reader, &["sku"], &options, Some(3))
                .await
                .expect("streaming verification");
        let (fast_summary, fast_crc) =
            csv_ingest::fast_local_process(file.path(), &["sku"], &options, true, Some(3))
                .expect("fast verification");

        assert_eq!(streaming_summary.row_count, fast_summary.row_count);
        assert_eq!(streaming_summary.headers, fast_summary.headers);
        assert_eq!(Some(streaming_crc), fast_crc);
    }

    #[tokio::test]
    async fn zero_limit_verifies_zero_rows_in_both_paths() {
        let contents = b"sku,value\nA,1\n";
        let mut file = NamedTempFile::new().expect("create fixture");
        file.write_all(contents).expect("write fixture");

        let reader = tokio::fs::File::open(file.path())
            .await
            .expect("open streaming fixture");
        let options = CsvOptions::default();
        let (streaming_summary, streaming_crc) =
            verify_and_count(reader, &["sku"], &options, Some(0))
                .await
                .expect("streaming verification");
        let (fast_summary, fast_crc) =
            csv_ingest::fast_local_process(file.path(), &["sku"], &options, true, Some(0))
                .expect("fast verification");

        assert_eq!(streaming_summary.row_count, 0);
        assert_eq!(fast_summary.row_count, 0);
        assert_eq!(Some(streaming_crc), fast_crc);
    }
}
