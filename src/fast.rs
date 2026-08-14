use crate::CsvIngestSummary;
use anyhow::{anyhow, Result};
use crc32fast::Hasher as Crc32;
use memchr::{memchr, memchr_iter};
use memmap2::MmapOptions;
use std::fs::File;
use std::path::Path;
use std::thread;

const FIELD_SEPARATOR: u8 = 0x1f;
const QUOTE: u8 = b'"';

struct ChunkResult {
    row_count: usize,
    crc: Option<Crc32>,
}

/// Fast local parser for uncompressed UTF-8 CSV files using mmap and parallel chunking.
///
/// This specialized path accepts only unquoted CSV. A quote byte anywhere in the
/// parsed portion of the file is rejected instead of being interpreted incorrectly.
pub fn fast_local_process(
    path: &Path,
    delimiter: u8,
    line_break: u8,
    required_headers: &[&str],
    verify_crc: bool,
    limit_rows: Option<u64>,
) -> Result<(CsvIngestSummary, Option<u32>)> {
    let workers = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    fast_local_process_with_workers(
        path,
        delimiter,
        line_break,
        required_headers,
        verify_crc,
        limit_rows,
        workers,
    )
}

fn fast_local_process_with_workers(
    path: &Path,
    delimiter: u8,
    line_break: u8,
    required_headers: &[&str],
    verify_crc: bool,
    limit_rows: Option<u64>,
    workers: usize,
) -> Result<(CsvIngestSummary, Option<u32>)> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    let len = metadata.len() as usize;
    // SAFETY: the map is read-only and remains bounded by the file length captured above.
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let data: &[u8] = &mmap[..];

    if len == 0 {
        return Ok((
            CsvIngestSummary {
                row_count: 0,
                headers: vec![],
            },
            verify_crc.then_some(0),
        ));
    }

    let header_end = memchr(line_break, data).unwrap_or(len);
    let raw_header = &data[..header_end];
    reject_quotes(raw_header, 0)?;
    let header = strip_trailing_cr(raw_header, line_break);
    let headers = parse_header(header, delimiter)?;

    let required_indices = required_headers
        .iter()
        .map(|required| {
            headers
                .iter()
                .position(|header| header == required)
                .ok_or_else(|| anyhow!("Missing required header: '{required}'"))
        })
        .collect::<Result<Vec<_>>>()?;
    let max_required_index = required_indices.into_iter().max();

    let body_start = (header_end + 1).min(len);
    let body_end = limited_body_end(data, body_start, len, line_break, limit_rows);
    let bounds = chunk_bounds(data, body_start, body_end, line_break, workers);
    let expected_width = headers.len();

    let chunk_results = thread::scope(|scope| -> Result<Vec<ChunkResult>> {
        let mut handles = Vec::with_capacity(bounds.len().saturating_sub(1));
        for window in bounds.windows(2) {
            let start = window[0];
            let end = window[1];
            let slice = &data[start..end];
            handles.push(scope.spawn(move || {
                process_chunk(
                    slice,
                    start,
                    delimiter,
                    line_break,
                    max_required_index,
                    expected_width,
                    verify_crc,
                )
            }));
        }

        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow!("fast-local parser worker panicked"))?
            })
            .collect()
    })?;

    let mut row_count = 0usize;
    let mut combined_crc = verify_crc.then(Crc32::new);
    for result in chunk_results {
        row_count += result.row_count;
        if let (Some(combined), Some(chunk_crc)) = (&mut combined_crc, result.crc) {
            combined.combine(&chunk_crc);
        }
    }

    Ok((
        CsvIngestSummary { row_count, headers },
        combined_crc.map(Crc32::finalize),
    ))
}

fn parse_header(header: &[u8], delimiter: u8) -> Result<Vec<String>> {
    let mut headers = Vec::new();
    let mut start = 0usize;
    for end in memchr_iter(delimiter, header) {
        headers.push(std::str::from_utf8(&header[start..end])?.to_string());
        start = end + 1;
    }
    headers.push(std::str::from_utf8(&header[start..])?.to_string());
    Ok(headers)
}

fn limited_body_end(
    data: &[u8],
    body_start: usize,
    len: usize,
    line_break: u8,
    limit_rows: Option<u64>,
) -> usize {
    let Some(limit) = limit_rows else {
        return len;
    };
    if limit == 0 {
        return body_start;
    }

    let mut rows = 0u64;
    for offset in memchr_iter(line_break, &data[body_start..len]) {
        rows += 1;
        if rows == limit {
            return body_start + offset + 1;
        }
    }

    len
}

fn chunk_bounds(
    data: &[u8],
    body_start: usize,
    body_end: usize,
    line_break: u8,
    requested_workers: usize,
) -> Vec<usize> {
    if body_start == body_end {
        return vec![body_start];
    }

    let body_len = body_end - body_start;
    let workers = requested_workers.max(1).min(body_len);
    let mut bounds = Vec::with_capacity(workers + 1);
    bounds.push(body_start);

    for worker in 1..workers {
        let approximate = body_start + body_len.saturating_mul(worker) / workers;
        let next = memchr(line_break, &data[approximate..body_end])
            .map(|offset| approximate + offset + 1)
            .unwrap_or(body_end);
        if next > *bounds.last().expect("body start is present") && next < body_end {
            bounds.push(next);
        }
    }

    bounds.push(body_end);
    bounds
}

fn process_chunk(
    slice: &[u8],
    absolute_start: usize,
    delimiter: u8,
    line_break: u8,
    max_required_index: Option<usize>,
    expected_width: usize,
    verify_crc: bool,
) -> Result<ChunkResult> {
    reject_quotes(slice, absolute_start)?;

    let mut row_count = 0usize;
    let mut cursor = 0usize;
    let mut crc = verify_crc.then(Crc32::new);

    for newline in memchr_iter(line_break, slice) {
        let row = strip_trailing_cr(&slice[cursor..newline], line_break);
        process_row(
            row,
            delimiter,
            max_required_index,
            expected_width,
            crc.as_mut(),
        )?;
        row_count += 1;
        cursor = newline + 1;
    }

    if cursor < slice.len() {
        let row = strip_trailing_cr(&slice[cursor..], line_break);
        process_row(
            row,
            delimiter,
            max_required_index,
            expected_width,
            crc.as_mut(),
        )?;
        row_count += 1;
    }

    Ok(ChunkResult { row_count, crc })
}

fn process_row(
    row: &[u8],
    delimiter: u8,
    max_required_index: Option<usize>,
    expected_width: usize,
    crc: Option<&mut Crc32>,
) -> Result<()> {
    if let Some(crc) = crc {
        let mut field_start = 0usize;
        let mut field_count = 0usize;
        for field_end in memchr_iter(delimiter, row) {
            if field_count > 0 {
                crc.update(&[FIELD_SEPARATOR]);
            }
            crc.update(&row[field_start..field_end]);
            field_count += 1;
            field_start = field_end + 1;
        }
        if field_count > 0 {
            crc.update(&[FIELD_SEPARATOR]);
        }
        crc.update(&row[field_start..]);
        field_count += 1;

        if field_count != expected_width {
            return Err(anyhow!(
                "row width mismatch: got {field_count}, expected {expected_width}"
            ));
        }
    } else if let Some(max_required_index) = max_required_index {
        let delimiter_count = memchr_iter(delimiter, row).take(max_required_index).count();
        if delimiter_count < max_required_index {
            return Err(anyhow!(
                "row is missing required field at column index {max_required_index}"
            ));
        }
    }

    Ok(())
}

fn reject_quotes(bytes: &[u8], absolute_start: usize) -> Result<()> {
    if let Some(offset) = memchr(QUOTE, bytes) {
        return Err(anyhow!(
            "fast-local supports only unquoted CSV; found a quote byte at offset {}",
            absolute_start + offset
        ));
    }
    Ok(())
}

fn strip_trailing_cr(row: &[u8], line_break: u8) -> &[u8] {
    if line_break == b'\n' && row.last() == Some(&b'\r') {
        &row[..row.len() - 1]
    } else {
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn fixture(contents: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("create fixture");
        file.write_all(contents).expect("write fixture");
        file
    }

    fn expected_crc(fields_by_row: &[&[&[u8]]]) -> u32 {
        let mut crc = Crc32::new();
        for fields in fields_by_row {
            for (index, field) in fields.iter().enumerate() {
                if index > 0 {
                    crc.update(&[FIELD_SEPARATOR]);
                }
                crc.update(field);
            }
        }
        crc.finalize()
    }

    #[test]
    fn limit_is_global_across_workers() {
        let file = fixture(b"sku,value\nA,1\nB,2\nC,3\nD,4\n");
        let (summary, crc) =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], true, Some(2), 8)
                .expect("parse limited fixture");

        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn final_unterminated_row_is_parsed_and_verified() {
        let file = fixture(b"sku,value\nA,1\nB,2");
        let (summary, crc) =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], true, None, 4)
                .expect("parse unterminated fixture");

        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn crlf_is_removed_from_headers_and_fields() {
        let file = fixture(b"sku,value\r\nA,1\r\nB,2\r\n");
        let (summary, crc) =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["value"], true, None, 4)
                .expect("parse CRLF fixture");

        assert_eq!(summary.headers, ["sku", "value"]);
        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn quote_bytes_are_rejected() {
        let file = fixture(b"sku,value\nA,\"quoted,field\"\n");
        let error =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], false, None, 4)
                .expect_err("quoted input must be rejected");

        assert!(error.to_string().contains("only unquoted CSV"));
    }

    #[test]
    fn verification_is_independent_of_worker_count() {
        let file = fixture(b"sku,value\nA,1\nB,2\nC,3\nD,4\nE,5\n");
        let one_worker =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], true, None, 1)
                .expect("parse with one worker");
        let many_workers =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], true, None, 8)
                .expect("parse with many workers");

        assert_eq!(one_worker.0.row_count, many_workers.0.row_count);
        assert_eq!(one_worker.1, many_workers.1);
    }

    #[test]
    fn empty_file_returns_an_empty_summary() {
        let file = fixture(b"");
        let (summary, crc) =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], true, None, 4)
                .expect("parse empty fixture");

        assert!(summary.headers.is_empty());
        assert_eq!(summary.row_count, 0);
        assert_eq!(crc, Some(0));
    }

    #[test]
    fn header_only_file_has_no_body_rows() {
        let file = fixture(b"sku,value\n");
        let (summary, crc) =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], true, None, 4)
                .expect("parse header-only fixture");

        assert_eq!(summary.headers, ["sku", "value"]);
        assert_eq!(summary.row_count, 0);
        assert_eq!(crc, Some(0));
    }

    #[test]
    fn missing_required_header_is_rejected() {
        let file = fixture(b"sku,value\nA,1\n");
        let error =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["missing"], false, None, 2)
                .expect_err("missing header must fail");

        assert!(error.to_string().contains("Missing required header"));
    }

    #[test]
    fn verified_row_width_must_match_the_header() {
        let file = fixture(b"sku,value\nA\n");
        let error =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], true, None, 2)
                .expect_err("short verified row must fail");

        assert!(error.to_string().contains("row width mismatch"));
    }

    #[test]
    fn unverified_row_must_contain_the_last_required_column() {
        let file = fixture(b"sku,value\nA\n");
        let error =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["value"], false, None, 2)
                .expect_err("short row must fail");

        assert!(error.to_string().contains("missing required field"));
    }

    #[test]
    fn a_limit_larger_than_the_file_processes_every_row() {
        let file = fixture(b"sku,value\nA,1\nB,2");
        let (summary, crc) =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], true, Some(100), 4)
                .expect("parse fixture below limit");

        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn quotes_in_the_header_are_rejected() {
        let file = fixture(b"\"sku\",value\nA,1\n");
        let error =
            fast_local_process_with_workers(file.path(), b',', b'\n', &["sku"], false, None, 2)
                .expect_err("quoted header must fail");

        assert!(error.to_string().contains("only unquoted CSV"));
    }
}
