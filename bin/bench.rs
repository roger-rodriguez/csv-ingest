use clap::{Arg, ArgAction, Command};
use crc32fast::Hasher as Crc32;
use csv_async::ByteRecord;
use csv_ingest::{process_csv_stream, reader_from_path};
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
                    b',',
                    b'\n',
                    &required_refs,
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

    let summary = if matches.get_flag("verify") {
        // Run a stricter verification parser that mirrors process_csv_stream but adds checksums
        verify_and_count(
            reader,
            &required_refs,
            matches.get_one::<u64>("limit").copied(),
        )
        .await?
    } else {
        process_csv_stream(reader, &required_refs).await?
    };
    let elapsed = start.elapsed().as_secs_f64();
    let rps = (summary.row_count as f64) / elapsed;

    let src = matches
        .get_one::<PathBuf>("path")
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "(unknown)".to_string());
    println!(
        "source={} rows={} headers={:?}\nelapsed={:.1}s rows/sec={:.0}",
        src, summary.row_count, summary.headers, elapsed, rps
    );
    Ok(())
}

async fn verify_and_count<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    reader: R,
    required_headers: &[&str],
    limit: Option<u64>,
) -> anyhow::Result<csv_ingest::CsvIngestSummary> {
    use csv_async::AsyncReaderBuilder;
    let mut rdr = AsyncReaderBuilder::new()
        .has_headers(true)
        .flexible(false) // be strict in verify mode
        .buffer_capacity(1 << 20)
        .create_reader(reader);

    let headers = rdr.headers().await?.clone();
    let width = headers.len();

    let required_indices = required_headers
        .iter()
        .map(|req_h| {
            headers
                .iter()
                .position(|h| h == *req_h)
                .ok_or_else(|| anyhow::anyhow!("Missing required header: '{}'", req_h))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut record = ByteRecord::new();
    let mut row_count: u64 = 0;
    let mut crc = Crc32::new();
    while rdr.read_byte_record(&mut record).await? {
        row_count += 1;
        if record.len() != width {
            return Err(anyhow::anyhow!(
                "Row {} width mismatch: got {}, expected {}",
                row_count,
                record.len(),
                width
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
    let _digest = crc.finalize();
    Ok(csv_ingest::CsvIngestSummary {
        row_count: row_count as usize,
        headers: headers.iter().map(|s| s.to_string()).collect(),
    })
}
