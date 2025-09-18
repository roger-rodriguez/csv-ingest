use crate::CsvIngestSummary;
use anyhow::{anyhow, Result};
use crc32fast::Hasher as Crc32;
use memchr::{memchr, memchr_iter};
use memmap2::MmapOptions;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

/// Fast local parser for uncompressed UTF-8 CSV files using mmap and parallel chunking.
/// Assumptions: UTF-8, no embedded newlines in quoted fields.
pub fn fast_local_process(
    path: &Path,
    delimiter: u8,
    line_break: u8,
    required_headers: &[&str],
    verify_crc: bool,
    limit_rows: Option<u64>,
) -> Result<(CsvIngestSummary, Option<u32>)> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    let len = metadata.len() as usize;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let data: &[u8] = &mmap[..];

    if len == 0 {
        return Ok((
            CsvIngestSummary {
                row_count: 0,
                headers: vec![],
            },
            None,
        ));
    }

    // Parse header (bytes until first newline)
    let header_end = memchr(line_break, data).unwrap_or(len);
    let header_slice = &data[..header_end];
    let mut headers: Vec<String> = Vec::new();
    let mut start = 0usize;
    for i in memchr_iter(delimiter, header_slice) {
        let field = &header_slice[start..i];
        headers.push(std::str::from_utf8(field)?.to_string());
        start = i + 1;
    }
    // last field
    if start <= header_slice.len() {
        headers.push(std::str::from_utf8(&header_slice[start..])?.to_string());
    }

    // Map required headers to indices
    let mut required_indices = required_headers
        .iter()
        .map(|h| {
            headers
                .iter()
                .position(|hh| hh == h)
                .ok_or_else(|| anyhow!("Missing required header: '{}'", h))
        })
        .collect::<Result<Vec<_>>>()?;
    required_indices.sort_unstable();

    // Count/process lines in parallel across the body (after header)
    let body_start = (header_end + 1).min(len);
    let cores = num_cpus::get().max(1);
    let approx = len / cores;
    let mut starts = Vec::with_capacity(cores + 1);
    starts.push(body_start);
    let mut pos = body_start + approx;
    while starts.len() < cores {
        if pos >= len {
            break;
        }
        let next = memchr_iter(line_break, &data[pos..])
            .next()
            .map(|off| pos + off + 1)
            .unwrap_or(len);
        starts.push(next.min(len));
        pos = body_start + starts.len() * approx;
    }
    starts.push(len);

    let total = AtomicUsize::new(0);
    let crc_total = AtomicUsize::new(0);
    thread::scope(|s| {
        let total_ref = &total;
        let crc_ref = &crc_total;
        for w in starts.windows(2) {
            let start = w[0];
            let end = w[1];
            let slice = &data[start..end];
            let req = required_indices.clone();
            s.spawn(move || {
                let mut count = 0usize;
                let mut local_crc: Crc32 = Crc32::new();
                let mut cursor = 0usize;
                let mut processed: u64 = 0;
                for nl in memchr_iter(line_break, slice) {
                    let row = &slice[cursor..nl];
                    cursor = nl + 1;
                    count += 1;

                    // Extract only required fields (bytes) by scanning delimiters once
                    if !req.is_empty() || verify_crc {
                        // Pointer through columns
                        let mut col_start = 0usize;
                        let mut col_idx = 0usize;
                        let mut req_it = 0usize;
                        // Sorted indices improve skipping; assume not sorted and restart scan each time
                        for (i, b) in row.iter().enumerate() {
                            if *b == delimiter {
                                if req.contains(&col_idx) && verify_crc {
                                    if req_it > 0 {
                                        local_crc.update(&[0x1f]);
                                    }
                                    local_crc.update(&row[col_start..i]);
                                    req_it += 1;
                                }
                                col_idx += 1;
                                col_start = i + 1;
                                // early-exit if last required column reached
                                if let Some(&last_req) = req.last() {
                                    if col_idx > last_req {
                                        break;
                                    }
                                }
                            }
                        }
                        // last field
                        if req.contains(&col_idx) && verify_crc {
                            if req_it > 0 {
                                local_crc.update(&[0x1f]);
                            }
                            local_crc.update(&row[col_start..]);
                        }
                    }

                    processed += 1;
                    if let Some(lim) = limit_rows {
                        if processed >= lim {
                            break;
                        }
                    }
                }
                total_ref.fetch_add(count, Ordering::Relaxed);
                if verify_crc {
                    let d = local_crc.finalize();
                    crc_ref.fetch_xor(d as usize, Ordering::Relaxed);
                }
            });
        }
    });

    let mut body_rows = total.load(Ordering::Relaxed);
    // If file doesn't end with newline, count the last line if beyond header
    if len > body_start && *data.last().unwrap_or(&line_break) != line_break {
        body_rows += 1;
    }

    if headers.is_empty() {
        return Err(anyhow!("fast path failed to parse header"));
    }

    let crc_opt = if verify_crc {
        Some(crc_total.load(Ordering::Relaxed) as u32)
    } else {
        None
    };
    Ok((
        CsvIngestSummary {
            row_count: body_rows,
            headers,
        },
        crc_opt,
    ))
}
