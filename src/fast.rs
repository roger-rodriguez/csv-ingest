use crate::{
    CsvHeaderMode, CsvIngestError, CsvIngestSummary, CsvOptions, CsvResult, CsvTerminator,
};
use crc32fast::Hasher as Crc32;
use memchr::{memchr, memchr2, memchr_iter};
use memmap2::MmapOptions;
use std::fs::File;
use std::path::Path;
use std::thread;

const FIELD_SEPARATOR: u8 = 0x1f;
const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";

struct ChunkResult {
    row_count: u64,
    crc: Option<Crc32>,
}

/// Fast local parser for uncompressed UTF-8 CSV files using mmap and parallel chunking.
///
/// This specialized path accepts only unquoted CSV. A quote byte anywhere in the
/// parsed portion of the file is rejected instead of being interpreted incorrectly.
pub fn fast_local_process(
    path: &Path,
    required_headers: &[&str],
    options: &CsvOptions,
    verify_crc: bool,
    limit_rows: Option<u64>,
) -> CsvResult<(CsvIngestSummary, Option<u32>)> {
    let workers = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    fast_local_process_with_workers(
        path,
        required_headers,
        options,
        verify_crc,
        limit_rows,
        workers,
    )
}

fn fast_local_process_with_workers(
    path: &Path,
    required_headers: &[&str],
    options: &CsvOptions,
    verify_crc: bool,
    limit_rows: Option<u64>,
    workers: usize,
) -> CsvResult<(CsvIngestSummary, Option<u32>)> {
    options.validate()?;
    if options.headers == CsvHeaderMode::Absent && !required_headers.is_empty() {
        return Err(CsvIngestError::UnsupportedDialect(
            "required headers cannot be validated when headers are absent".to_string(),
        ));
    }

    let file = File::open(path)?;
    let metadata = file.metadata()?;
    let len = metadata.len() as usize;
    // SAFETY: the map is read-only and remains bounded by the file length captured above.
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let data: &[u8] = &mmap[..];

    let data_start = usize::from(data.starts_with(UTF8_BOM)) * UTF8_BOM.len();
    if data_start == len {
        if options.headers == CsvHeaderMode::Present {
            if let Some(required) = required_headers.first() {
                return Err(CsvIngestError::MissingHeader((*required).to_string()));
            }
        }
        return Ok((
            CsvIngestSummary {
                row_count: 0,
                headers: vec![],
            },
            verify_crc.then_some(0),
        ));
    }

    let (headers, body_start, expected_width) = if options.headers == CsvHeaderMode::Present {
        let (header_end, body_start) =
            next_record_terminator(data, data_start, len, options.terminator).unwrap_or((len, len));
        let raw_header = &data[data_start..header_end];
        reject_quotes(raw_header, data_start, options)?;
        let headers = parse_header(raw_header, options.delimiter, options.trims_headers())?;
        let expected_width = Some(headers.len());
        (headers, body_start, expected_width)
    } else {
        let expected_width = (!options.flexible).then(|| {
            let first_end = next_record_terminator(data, data_start, len, options.terminator)
                .map(|(record_end, _)| record_end)
                .unwrap_or(len);
            memchr_iter(options.delimiter, &data[data_start..first_end]).count() + 1
        });
        (Vec::new(), data_start, expected_width)
    };

    let required_fields = required_headers
        .iter()
        .map(|required| {
            headers
                .iter()
                .position(|header| header == required)
                .map(|index| (index, (*required).to_string()))
                .ok_or_else(|| CsvIngestError::MissingHeader((*required).to_string()))
        })
        .collect::<CsvResult<Vec<_>>>()?;
    let required_field = required_fields.into_iter().max_by_key(|(index, _)| *index);
    let required_field = required_field
        .as_ref()
        .map(|(index, header)| (*index, header.as_str()));

    let body_end = limited_body_end(data, body_start, len, options.terminator, limit_rows);
    let bounds = chunk_bounds(data, body_start, body_end, options.terminator, workers);

    let chunk_results = thread::scope(|scope| -> CsvResult<Vec<CsvResult<ChunkResult>>> {
        let mut handles = Vec::with_capacity(bounds.len().saturating_sub(1));
        for window in bounds.windows(2) {
            let start = window[0];
            let end = window[1];
            let slice = &data[start..end];
            handles.push(scope.spawn(move || {
                process_chunk(
                    slice,
                    start,
                    options,
                    required_field,
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
                    .map_err(|_| CsvIngestError::FastLocalWorkerPanicked)
            })
            .collect()
    })?;

    let mut row_count = 0u64;
    let mut combined_crc = verify_crc.then(Crc32::new);
    for result in chunk_results {
        let result = result.map_err(|error| offset_row(error, row_count))?;
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

fn parse_header(header: &[u8], delimiter: u8, trim: bool) -> CsvResult<Vec<String>> {
    let mut headers = Vec::new();
    let mut start = 0usize;
    for end in memchr_iter(delimiter, header) {
        let value = trim_ascii_if(&header[start..end], trim);
        headers.push(std::str::from_utf8(value)?.to_string());
        start = end + 1;
    }
    let value = trim_ascii_if(&header[start..], trim);
    headers.push(std::str::from_utf8(value)?.to_string());
    Ok(headers)
}

fn limited_body_end(
    data: &[u8],
    body_start: usize,
    len: usize,
    terminator: CsvTerminator,
    limit_rows: Option<u64>,
) -> usize {
    let Some(limit) = limit_rows else {
        return len;
    };
    if limit == 0 {
        return body_start;
    }

    let mut rows = 0u64;
    let mut cursor = body_start;
    while let Some((_, next_record)) = next_record_terminator(data, cursor, len, terminator) {
        rows += 1;
        if rows == limit {
            return next_record;
        }
        cursor = next_record;
    }

    len
}

fn chunk_bounds(
    data: &[u8],
    body_start: usize,
    body_end: usize,
    terminator: CsvTerminator,
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
        let next = next_record_terminator(data, approximate, body_end, terminator)
            .map(|(_, next)| next)
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
    options: &CsvOptions,
    required_field: Option<(usize, &str)>,
    expected_width: Option<usize>,
    verify_crc: bool,
) -> CsvResult<ChunkResult> {
    reject_quotes(slice, absolute_start, options)?;

    let mut row_count = 0u64;
    let mut cursor = 0usize;
    let mut crc = verify_crc.then(Crc32::new);

    while let Some((record_end, next_record)) =
        next_record_terminator(slice, cursor, slice.len(), options.terminator)
    {
        let row = &slice[cursor..record_end];
        process_row(
            row,
            options,
            required_field,
            expected_width,
            crc.as_mut(),
            row_count + 1,
        )?;
        row_count += 1;
        cursor = next_record;
    }

    if cursor < slice.len() {
        process_row(
            &slice[cursor..],
            options,
            required_field,
            expected_width,
            crc.as_mut(),
            row_count + 1,
        )?;
        row_count += 1;
    }

    Ok(ChunkResult { row_count, crc })
}

fn process_row(
    row: &[u8],
    options: &CsvOptions,
    required_field: Option<(usize, &str)>,
    expected_width: Option<usize>,
    crc: Option<&mut Crc32>,
    row_number: u64,
) -> CsvResult<()> {
    if crc.is_none() && options.flexible {
        if let Some((required_index, required_header)) = required_field {
            let delimiter_count = memchr_iter(options.delimiter, row)
                .take(required_index)
                .count();
            if delimiter_count < required_index {
                return Err(CsvIngestError::MissingRequiredField {
                    row: row_number,
                    header: required_header.to_string(),
                });
            }
        }
        return Ok(());
    }

    let mut crc = crc;
    let mut field_start = 0usize;
    let mut field_count = 0usize;
    for field_end in memchr_iter(options.delimiter, row) {
        if let Some(crc) = crc.as_mut() {
            if field_count > 0 {
                crc.update(&[FIELD_SEPARATOR]);
            }
            crc.update(trim_ascii_if(
                &row[field_start..field_end],
                options.trims_fields(),
            ));
        }
        field_count += 1;
        field_start = field_end + 1;
    }
    if let Some(crc) = crc.as_mut() {
        if field_count > 0 {
            crc.update(&[FIELD_SEPARATOR]);
        }
        crc.update(trim_ascii_if(&row[field_start..], options.trims_fields()));
    }
    field_count += 1;

    if !options.flexible {
        if let Some(expected_width) = expected_width {
            if field_count != expected_width {
                return Err(CsvIngestError::RaggedRow {
                    row: Some(row_number),
                    expected: expected_width as u64,
                    actual: field_count as u64,
                });
            }
        }
    } else if let Some((required_index, required_header)) = required_field {
        if field_count <= required_index {
            return Err(CsvIngestError::MissingRequiredField {
                row: row_number,
                header: required_header.to_string(),
            });
        }
    }

    Ok(())
}

fn reject_quotes(bytes: &[u8], absolute_start: usize, options: &CsvOptions) -> CsvResult<()> {
    if options.quoting {
        if let Some(offset) = memchr(options.quote, bytes) {
            return Err(CsvIngestError::UnsupportedDialect(format!(
                "fast-local supports only unquoted CSV; found quote byte {:?} at offset {}",
                options.quote as char,
                absolute_start + offset
            )));
        }
    }
    Ok(())
}

fn offset_row(error: CsvIngestError, offset: u64) -> CsvIngestError {
    match error {
        CsvIngestError::MissingRequiredField { row, header } => {
            CsvIngestError::MissingRequiredField {
                row: row + offset,
                header,
            }
        }
        CsvIngestError::RaggedRow {
            row: Some(row),
            expected,
            actual,
        } => CsvIngestError::RaggedRow {
            row: Some(row + offset),
            expected,
            actual,
        },
        error => error,
    }
}

fn next_record_terminator(
    data: &[u8],
    start: usize,
    end: usize,
    terminator: CsvTerminator,
) -> Option<(usize, usize)> {
    let record_end = match terminator {
        CsvTerminator::CrLf => memchr2(b'\r', b'\n', &data[start..end])? + start,
        CsvTerminator::Any(byte) => memchr(byte, &data[start..end])? + start,
    };
    let next_record = if terminator == CsvTerminator::CrLf
        && data[record_end] == b'\r'
        && record_end + 1 < end
        && data[record_end + 1] == b'\n'
    {
        record_end + 2
    } else {
        record_end + 1
    };
    Some((record_end, next_record))
}

fn trim_ascii_if(bytes: &[u8], trim: bool) -> &[u8] {
    if !trim {
        return bytes;
    }

    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &bytes[start..end]
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
        let (summary, crc) = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            true,
            Some(2),
            8,
        )
        .expect("parse limited fixture");

        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn final_unterminated_row_is_parsed_and_verified() {
        let file = fixture(b"sku,value\nA,1\nB,2");
        let (summary, crc) = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            true,
            None,
            4,
        )
        .expect("parse unterminated fixture");

        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn crlf_is_removed_from_headers_and_fields() {
        let file = fixture(b"sku,value\r\nA,1\r\nB,2\r\n");
        let (summary, crc) = fast_local_process_with_workers(
            file.path(),
            &["value"],
            &CsvOptions::default(),
            true,
            None,
            4,
        )
        .expect("parse CRLF fixture");

        assert_eq!(summary.headers, ["sku", "value"]);
        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn quote_bytes_are_rejected() {
        let file = fixture(b"sku,value\nA,\"quoted,field\"\n");
        let error = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            false,
            None,
            4,
        )
        .expect_err("quoted input must be rejected");

        assert!(error.to_string().contains("only unquoted CSV"));
    }

    #[test]
    fn verification_is_independent_of_worker_count() {
        let file = fixture(b"sku,value\nA,1\nB,2\nC,3\nD,4\nE,5\n");
        let one_worker = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            true,
            None,
            1,
        )
        .expect("parse with one worker");
        let many_workers = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            true,
            None,
            8,
        )
        .expect("parse with many workers");

        assert_eq!(one_worker.0.row_count, many_workers.0.row_count);
        assert_eq!(one_worker.1, many_workers.1);
    }

    #[test]
    fn empty_file_returns_an_empty_summary() {
        let file = fixture(b"");
        let (summary, crc) = fast_local_process_with_workers(
            file.path(),
            &[],
            &CsvOptions::default(),
            true,
            None,
            4,
        )
        .expect("parse empty fixture");

        assert!(summary.headers.is_empty());
        assert_eq!(summary.row_count, 0);
        assert_eq!(crc, Some(0));
    }

    #[test]
    fn empty_file_cannot_satisfy_a_required_header() {
        let file = fixture(UTF8_BOM);
        let error = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            false,
            None,
            1,
        )
        .expect_err("empty input has no required headers");

        assert!(matches!(
            error,
            CsvIngestError::MissingHeader(header) if header == "sku"
        ));
    }

    #[test]
    fn header_only_file_has_no_body_rows() {
        let file = fixture(b"sku,value\n");
        let (summary, crc) = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            true,
            None,
            4,
        )
        .expect("parse header-only fixture");

        assert_eq!(summary.headers, ["sku", "value"]);
        assert_eq!(summary.row_count, 0);
        assert_eq!(crc, Some(0));
    }

    #[test]
    fn missing_required_header_is_rejected() {
        let file = fixture(b"sku,value\nA,1\n");
        let error = fast_local_process_with_workers(
            file.path(),
            &["missing"],
            &CsvOptions::default(),
            false,
            None,
            2,
        )
        .expect_err("missing header must fail");

        assert!(matches!(
            error,
            CsvIngestError::MissingHeader(header) if header == "missing"
        ));
    }

    #[test]
    fn verified_row_width_must_match_the_header() {
        let file = fixture(b"sku,value\nA\n");
        let error = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            true,
            None,
            2,
        )
        .expect_err("short verified row must fail");

        assert!(matches!(
            error,
            CsvIngestError::RaggedRow {
                row: Some(1),
                expected: 2,
                actual: 1
            }
        ));
    }

    #[test]
    fn parallel_errors_report_the_global_row_number() {
        let file = fixture(b"sku,value\nA,1\nB,2\nC\nD,4\n");
        let error = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            false,
            None,
            4,
        )
        .expect_err("ragged row must fail");

        assert!(matches!(
            error,
            CsvIngestError::RaggedRow { row: Some(3), .. }
        ));
    }

    #[test]
    fn unverified_row_must_contain_the_last_required_column() {
        let file = fixture(b"sku,value\nA\n");
        let options = CsvOptions {
            flexible: true,
            ..CsvOptions::default()
        };
        let error =
            fast_local_process_with_workers(file.path(), &["value"], &options, false, None, 2)
                .expect_err("short row must fail");

        assert!(matches!(
            error,
            CsvIngestError::MissingRequiredField { row: 1, header } if header == "value"
        ));
    }

    #[test]
    fn a_limit_larger_than_the_file_processes_every_row() {
        let file = fixture(b"sku,value\nA,1\nB,2");
        let (summary, crc) = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            true,
            Some(100),
            4,
        )
        .expect("parse fixture below limit");

        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn quotes_in_the_header_are_rejected() {
        let file = fixture(b"\"sku\",value\nA,1\n");
        let error = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            false,
            None,
            2,
        )
        .expect_err("quoted header must fail");

        assert!(error.to_string().contains("only unquoted CSV"));
    }

    #[test]
    fn shared_options_control_delimiters_terminators_trimming_and_bom() {
        let file = fixture(b"\xef\xbb\xbf sku ; value $ A ; 1 $ B ; 2 $");
        let options = CsvOptions {
            delimiter: b';',
            terminator: CsvTerminator::Any(b'$'),
            trim: crate::CsvTrim::All,
            ..CsvOptions::default()
        };
        let (summary, crc) =
            fast_local_process_with_workers(file.path(), &["sku"], &options, true, None, 4)
                .expect("parse configured dialect");

        assert_eq!(summary.headers, ["sku", "value"]);
        assert_eq!(summary.row_count, 2);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"1"], &[b"B", b"2"]])));
    }

    #[test]
    fn crlf_mode_accepts_lone_cr_and_lone_lf_terminators() {
        let file = fixture(b"sku,value\rA,1\nB,2\r\nC,3");
        let (summary, _) = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            false,
            None,
            8,
        )
        .expect("parse mixed CRLF terminators");

        assert_eq!(summary.row_count, 3);
    }

    #[test]
    fn headerless_mode_counts_every_record_and_requires_no_named_headers() {
        let file = fixture(b"A,1\nB,2");
        let options = CsvOptions {
            headers: CsvHeaderMode::Absent,
            ..CsvOptions::default()
        };
        let (summary, _) =
            fast_local_process_with_workers(file.path(), &[], &options, false, None, 4)
                .expect("parse headerless fixture");

        assert_eq!(
            summary,
            CsvIngestSummary {
                row_count: 2,
                headers: vec![]
            }
        );

        let error =
            fast_local_process_with_workers(file.path(), &["sku"], &options, false, None, 1)
                .expect_err("named headers require a header record");
        assert!(error.to_string().contains("headers are absent"));
    }

    #[test]
    fn disabled_quoting_treats_quote_bytes_as_regular_data() {
        let file = fixture(b"sku,value\nA,a\"b\n");
        let options = CsvOptions {
            quoting: false,
            ..CsvOptions::default()
        };
        let (summary, crc) =
            fast_local_process_with_workers(file.path(), &["sku"], &options, true, None, 2)
                .expect("parse literal quote byte");

        assert_eq!(summary.row_count, 1);
        assert_eq!(crc, Some(expected_crc(&[&[b"A", b"a\"b"]])));
    }

    #[test]
    fn invalid_options_fail_before_parsing() {
        let file = fixture(b"sku,value\nA,1\n");
        let options = CsvOptions {
            delimiter: b'\n',
            ..CsvOptions::default()
        };
        let error =
            fast_local_process_with_workers(file.path(), &["sku"], &options, false, None, 1)
                .expect_err("invalid dialect must fail");

        assert!(error.to_string().contains("delimiter"));
    }

    #[test]
    fn invalid_utf8_headers_use_the_shared_encoding_error() {
        let file = fixture(b"sku,\xff\nA,1\n");
        let error = fast_local_process_with_workers(
            file.path(),
            &["sku"],
            &CsvOptions::default(),
            false,
            None,
            1,
        )
        .expect_err("invalid UTF-8 header must fail");

        assert!(matches!(error, CsvIngestError::InvalidUtf8(_)));
    }
}
