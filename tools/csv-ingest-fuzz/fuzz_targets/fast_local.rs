#![no_main]

use csv_ingest::{fast_local_process, CsvHeaderMode, CsvOptions, CsvTerminator, CsvTrim};
use libfuzzer_sys::fuzz_target;
use std::io::Write;
use tempfile::NamedTempFile;

fuzz_target!(|data: &[u8]| {
    let transform = data.first().copied().unwrap_or_default();
    let selector = data.get(1).copied().unwrap_or(b'0').wrapping_sub(b'0');
    let payload = data.get(2..).unwrap_or_default();

    let mut contents = transform_input(transform, payload);
    if transform == b'E' {
        contents.clear();
    }

    let Ok(mut file) = NamedTempFile::new() else {
        return;
    };
    if file.write_all(&contents).is_err() {
        return;
    }

    let options = options(selector);
    let required_headers: &[&str] = if options.headers == CsvHeaderMode::Absent {
        &[]
    } else {
        match selector & 0b11 {
            0 => &["sku"],
            1 => &["value"],
            2 => &["missing"],
            _ => &[],
        }
    };
    let limit_rows = if transform.is_ascii_uppercase() {
        None
    } else {
        match transform & 0b11 {
            0 => None,
            1 => Some(0),
            2 => Some(1),
            _ => Some(16),
        }
    };

    let _ = fast_local_process(
        file.path(),
        required_headers,
        &options,
        transform & 0b1000 != 0,
        limit_rows,
    );
});

fn options(selector: u8) -> CsvOptions {
    let (delimiter, terminator) = match selector & 0b11 {
        0 => (b',', CsvTerminator::CrLf),
        1 => (b'\t', CsvTerminator::Any(b'\n')),
        2 => (b';', CsvTerminator::Any(b'|')),
        _ => (0x1f, CsvTerminator::Any(0x1e)),
    };
    let trim = match (selector >> 4) & 0b11 {
        0 => CsvTrim::None,
        1 => CsvTrim::Headers,
        2 => CsvTrim::Fields,
        _ => CsvTrim::All,
    };

    CsvOptions {
        delimiter,
        terminator,
        headers: if selector & 0b100 == 0 {
            CsvHeaderMode::Present
        } else {
            CsvHeaderMode::Absent
        },
        flexible: selector & 0b1000 != 0,
        trim,
        quoting: selector & 0b100_0000 == 0,
        quote: b'"',
        escape: (selector & 0b1000_0000 != 0).then_some(b'\\'),
        double_quote: selector & 0b10_0000 == 0,
    }
}

fn transform_input(transform: u8, payload: &[u8]) -> Vec<u8> {
    let mut contents = if transform == b'T' {
        expand_crlf(payload)
    } else {
        payload.to_vec()
    };

    if transform == b'M' {
        contents.insert(0, 0xff);
    }
    if transform == b'U' {
        while matches!(contents.last(), Some(b'\r' | b'\n')) {
            contents.pop();
        }
    }

    contents
}

fn expand_crlf(payload: &[u8]) -> Vec<u8> {
    let mut contents = Vec::with_capacity(payload.len());
    let mut previous = None;

    for &byte in payload {
        if byte == b'\n' && previous != Some(b'\r') {
            contents.push(b'\r');
        }
        contents.push(byte);
        previous = Some(byte);
    }

    contents
}
