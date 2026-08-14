use clap::{Arg, ArgAction, Command};
use crc32fast::Hasher as Crc32;
use csv_ingest::{
    reader_from_path, summarize_csv_stream, ByteRecord, CsvHeaderMode, CsvOptions, CsvTerminator,
    CsvTrim,
};
#[cfg(feature = "fast_local")]
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::AsyncRead;

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

    let (reader, _meta): (Box<dyn AsyncRead + Unpin + Send>, csv_ingest::CsvMeta) =
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
            (Box::new(r), m)
        } else {
            panic!("Provide --path <file>");
        };

    let (summary, crc) = if matches.get_flag("verify") {
        // Run a stricter verification parser that mirrors summarize_csv_stream but adds checksums
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
    use csv_async::{AsyncReaderBuilder, Terminator, Trim};
    options.validate()?;
    if options.headers == CsvHeaderMode::Absent && !required_headers.is_empty() {
        anyhow::bail!("required headers cannot be validated when headers are absent");
    }
    let mut builder = AsyncReaderBuilder::new();
    builder
        .delimiter(options.delimiter)
        .terminator(match options.terminator {
            CsvTerminator::CrLf => Terminator::CRLF,
            CsvTerminator::Any(byte) => Terminator::Any(byte),
        })
        .has_headers(options.headers == CsvHeaderMode::Present)
        .flexible(options.flexible)
        .trim(match options.trim {
            CsvTrim::None => Trim::None,
            CsvTrim::Headers => Trim::Headers,
            CsvTrim::Fields => Trim::Fields,
            CsvTrim::All => Trim::All,
        })
        .quoting(options.quoting)
        .quote(options.quote)
        .escape(options.escape)
        .double_quote(options.double_quote)
        .buffer_capacity(1 << 20);
    let mut rdr = builder.create_reader(reader);

    let headers = if options.headers == CsvHeaderMode::Present {
        rdr.byte_headers().await?.clone()
    } else {
        ByteRecord::new()
    };
    let mut width = (options.headers == CsvHeaderMode::Present).then_some(headers.len());

    let required_indices = required_headers
        .iter()
        .map(|req_h| {
            headers
                .iter()
                .position(|h| h == req_h.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("Missing required header: '{}'", req_h))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    if limit == Some(0) {
        return Ok((
            csv_ingest::CsvIngestSummary {
                row_count: 0,
                headers: headers
                    .iter()
                    .map(std::str::from_utf8)
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            },
            Crc32::new().finalize(),
        ));
    }

    let mut record = ByteRecord::new();
    let mut row_count: u64 = 0;
    let mut crc = Crc32::new();
    while rdr.read_byte_record(&mut record).await? {
        row_count += 1;
        let expected_width = *width.get_or_insert(record.len());
        if !options.flexible && record.len() != expected_width {
            return Err(anyhow::anyhow!(
                "Row {} width mismatch: got {}, expected {}",
                row_count,
                record.len(),
                expected_width
            ));
        }
        for (i, &idx) in required_indices.iter().enumerate() {
            if record.get(idx).is_none() {
                return Err(anyhow::anyhow!(
                    "Row {} missing required field '{}'",
                    row_count,
                    required_headers[i]
                ));
            }
        }
        // accumulate CRC32 over all fields separated by '\x1f' (unit separator)
        for (fi, field) in record.iter().enumerate() {
            if fi > 0 {
                crc.update(&[0x1f]);
            }
            crc.update(field);
        }
        if let Some(lim) = limit {
            if row_count >= lim {
                break;
            }
        }
    }
    let digest = crc.finalize();
    Ok((
        csv_ingest::CsvIngestSummary {
            row_count,
            headers: headers
                .iter()
                .map(std::str::from_utf8)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(str::to_string)
                .collect(),
        },
        digest,
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
