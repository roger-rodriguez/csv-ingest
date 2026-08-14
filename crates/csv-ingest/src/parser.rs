use crate::{
    reader_from_path, BoxedCsvReader, CsvHeaderMode, CsvIngestError, CsvIngestSummary, CsvMeta,
    CsvOptions, CsvResult,
};
use csv_async::{AsyncReader, AsyncReaderBuilder};
use std::collections::HashMap;
use std::path::Path;
use tokio::io::AsyncRead;

/// A streaming, byte-oriented CSV parser.
///
/// The parser resolves headers and required columns during construction. Use
/// [`CsvParser::next_record`] to reuse parser-owned storage or
/// [`CsvParser::read_record`] to reuse a caller-owned [`crate::ByteRecord`].
pub struct CsvParser<R> {
    reader: AsyncReader<R>,
    headers: crate::ByteRecord,
    header_indices: HashMap<Vec<u8>, usize>,
    required_indices: Vec<usize>,
    required_headers: Vec<String>,
    record: crate::ByteRecord,
    records_read: u64,
}

impl<R> CsvParser<R>
where
    R: AsyncRead + Unpin + Send,
{
    /// Construct a parser over an arbitrary asynchronous byte reader.
    ///
    /// `Send + Unpin` are required by `csv_async`'s Tokio reader backend. The
    /// reader does not need to be `'static`, so borrowed readers are accepted.
    pub async fn from_reader(
        reader: R,
        required_headers: &[&str],
        options: &CsvOptions,
    ) -> CsvResult<Self> {
        if options.headers == CsvHeaderMode::Absent && !required_headers.is_empty() {
            return Err(CsvIngestError::UnsupportedDialect(
                "required headers cannot be validated when headers are absent".to_string(),
            ));
        }

        let mut builder = AsyncReaderBuilder::new();
        options.configure_reader(&mut builder)?;
        builder.buffer_capacity(1 << 20);
        let mut reader = builder.create_reader(reader);

        let headers = if options.headers == CsvHeaderMode::Present {
            reader.byte_headers().await?.clone()
        } else {
            crate::ByteRecord::new()
        };
        let mut header_indices = HashMap::with_capacity(headers.len());
        for (index, header) in headers.iter().enumerate() {
            header_indices.entry(header.to_vec()).or_insert(index);
        }
        let required_indices = required_headers
            .iter()
            .map(|header| {
                header_indices
                    .get(header.as_bytes())
                    .copied()
                    .ok_or_else(|| CsvIngestError::MissingHeader((*header).to_string()))
            })
            .collect::<CsvResult<Vec<_>>>()?;

        Ok(Self {
            reader,
            headers,
            header_indices,
            required_indices,
            required_headers: required_headers
                .iter()
                .map(|header| (*header).to_string())
                .collect(),
            record: crate::ByteRecord::new(),
            records_read: 0,
        })
    }

    /// Return the header record, or an empty record in headerless mode.
    pub fn headers(&self) -> &crate::ByteRecord {
        &self.headers
    }

    /// Resolve a byte header to its first column index.
    pub fn header_index(&self, header: impl AsRef<[u8]>) -> Option<usize> {
        self.header_indices.get(header.as_ref()).copied()
    }

    /// Return the required-column indices resolved during construction.
    pub fn required_indices(&self) -> &[usize] {
        &self.required_indices
    }

    /// Return the number of data records read so far.
    pub fn records_read(&self) -> u64 {
        self.records_read
    }

    /// Read the next record into parser-owned reusable storage.
    ///
    /// The returned reference remains valid until the parser is mutably used
    /// again. Fields are available as byte slices through `record.get(index)`.
    pub async fn next_record(&mut self) -> CsvResult<Option<&crate::ByteRecord>> {
        let has_record = read_validated_record(
            &mut self.reader,
            &mut self.record,
            &self.required_indices,
            &self.required_headers,
            &mut self.records_read,
        )
        .await?;
        Ok(has_record.then_some(&self.record))
    }

    /// Read the next record into caller-owned reusable storage.
    pub async fn read_record(&mut self, record: &mut crate::ByteRecord) -> CsvResult<bool> {
        read_validated_record(
            &mut self.reader,
            record,
            &self.required_indices,
            &self.required_headers,
            &mut self.records_read,
        )
        .await
    }

    /// Unwrap the parser and return its underlying reader.
    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }
}

impl CsvParser<BoxedCsvReader<'static>> {
    /// Construct a parser from a local path using the same transport and CSV options.
    pub async fn from_path(
        path: &Path,
        required_headers: &[&str],
        options: &CsvOptions,
    ) -> CsvResult<(Self, CsvMeta)> {
        let (reader, meta) = reader_from_path(path).await?;
        let parser = Self::from_reader(reader, required_headers, options).await?;
        Ok((parser, meta))
    }
}

async fn read_validated_record<R>(
    reader: &mut AsyncReader<R>,
    record: &mut crate::ByteRecord,
    required_indices: &[usize],
    required_headers: &[String],
    records_read: &mut u64,
) -> CsvResult<bool>
where
    R: AsyncRead + Unpin + Send,
{
    let has_record = reader.read_byte_record(record).await.map_err(|error| {
        let error = CsvIngestError::from(error);
        match error {
            CsvIngestError::RaggedRow {
                expected, actual, ..
            } => CsvIngestError::RaggedRow {
                row: Some(*records_read + 1),
                expected,
                actual,
            },
            error => error,
        }
    })?;
    if !has_record {
        return Ok(false);
    }
    *records_read += 1;
    for (&index, header) in required_indices.iter().zip(required_headers) {
        if record.get(index).is_none() {
            return Err(CsvIngestError::MissingRequiredField {
                row: *records_read,
                header: header.clone(),
            });
        }
    }
    Ok(true)
}

async fn finish_summary<R>(mut parser: CsvParser<R>) -> CsvResult<CsvIngestSummary>
where
    R: AsyncRead + Unpin + Send,
{
    let headers = parser
        .headers()
        .iter()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(str::to_string)
        .collect();
    while parser.next_record().await?.is_some() {}
    Ok(CsvIngestSummary {
        row_count: parser.records_read(),
        headers,
    })
}

/// Summarize a CSV stream using the record-oriented parser contract.
pub async fn summarize_csv_stream<R>(
    reader: R,
    required_headers: &[&str],
    options: &CsvOptions,
) -> CsvResult<CsvIngestSummary>
where
    R: AsyncRead + Unpin + Send,
{
    finish_summary(CsvParser::from_reader(reader, required_headers, options).await?).await
}

/// Summarize a local CSV path and return the transport metadata used.
pub async fn summarize_csv_path(
    path: &Path,
    required_headers: &[&str],
    options: &CsvOptions,
) -> CsvResult<(CsvIngestSummary, CsvMeta)> {
    let (parser, meta) = CsvParser::from_path(path, required_headers, options).await?;
    Ok((finish_summary(parser).await?, meta))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn parser_exposes_byte_headers_and_resolves_columns_once() {
        let parser = CsvParser::from_reader(
            Cursor::new(b"sku,value,sku\nA,1,B\n"),
            &["sku", "value"],
            &CsvOptions::default(),
        )
        .await
        .expect("construct parser");

        assert_eq!(
            parser.headers(),
            &crate::ByteRecord::from(vec!["sku", "value", "sku"])
        );
        assert_eq!(parser.header_index(b"sku"), Some(0));
        assert_eq!(parser.header_index("value"), Some(1));
        assert_eq!(parser.header_index(b"missing"), None);
        assert_eq!(parser.required_indices(), [0, 1]);
    }

    #[tokio::test]
    async fn parser_keeps_headers_and_fields_byte_oriented() {
        let mut parser = CsvParser::from_reader(
            Cursor::new(&[b's', b'k', b'u', b',', 0xff, b'\n', b'A', b',', 0xfe, b'\n'][..]),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect("construct byte parser");

        assert_eq!(parser.headers().get(1), Some(&[0xff][..]));
        assert_eq!(parser.header_index([0xff]), Some(1));
        let record = parser
            .next_record()
            .await
            .expect("read byte record")
            .expect("record");
        assert_eq!(record.get(1), Some(&[0xfe][..]));
    }

    #[tokio::test]
    async fn parser_owned_record_storage_is_reused() {
        let mut parser = CsvParser::from_reader(
            Cursor::new(b"sku,value\nLONG-SKU,123456\nB,2\n"),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect("construct parser");

        let first_pointer = parser
            .next_record()
            .await
            .expect("read first")
            .expect("first record")
            .as_slice()
            .as_ptr();
        let second = parser
            .next_record()
            .await
            .expect("read second")
            .expect("second record");

        assert_eq!(second.get(0), Some(&b"B"[..]));
        assert_eq!(second.as_slice().as_ptr(), first_pointer);
        let records_read: u64 = parser.records_read();
        assert_eq!(records_read, 2);
        assert!(parser.next_record().await.expect("read EOF").is_none());
    }

    #[tokio::test]
    async fn caller_owned_record_storage_is_supported() {
        let mut parser = CsvParser::from_reader(
            Cursor::new(b"sku,value\nA,1\nB,2\n"),
            &[],
            &CsvOptions::default(),
        )
        .await
        .expect("construct parser");
        let mut record = crate::ByteRecord::new();

        assert!(parser.read_record(&mut record).await.expect("read first"));
        assert_eq!(record.get(1), Some(&b"1"[..]));
        assert!(parser.read_record(&mut record).await.expect("read second"));
        assert_eq!(record.get(0), Some(&b"B"[..]));
        assert!(!parser.read_record(&mut record).await.expect("read EOF"));
        assert_eq!(parser.records_read(), 2);
    }

    #[tokio::test]
    async fn required_columns_are_validated_during_record_reads() {
        let options = CsvOptions {
            flexible: true,
            ..CsvOptions::default()
        };
        let mut parser =
            CsvParser::from_reader(Cursor::new(b"sku,value\nA\n"), &["value"], &options)
                .await
                .expect("construct parser");

        let error = parser
            .next_record()
            .await
            .expect_err("missing field must fail");

        assert!(matches!(
            error,
            CsvIngestError::MissingRequiredField { row: 1, header } if header == "value"
        ));
    }

    #[tokio::test]
    async fn headerless_ragged_errors_use_one_based_data_row_numbers() {
        let options = CsvOptions {
            headers: CsvHeaderMode::Absent,
            ..CsvOptions::default()
        };
        let mut parser = CsvParser::from_reader(Cursor::new(b"A\nB,C\n"), &[], &options)
            .await
            .expect("construct headerless parser");

        parser
            .next_record()
            .await
            .expect("read first row")
            .expect("first row");
        let error = parser
            .next_record()
            .await
            .expect_err("ragged second row must fail");

        assert!(matches!(
            error,
            CsvIngestError::RaggedRow { row: Some(2), .. }
        ));
    }

    #[tokio::test]
    async fn path_and_reader_constructors_share_parser_behavior() {
        let file = tempfile::Builder::new()
            .suffix(".csv")
            .tempfile()
            .expect("create fixture");
        std::fs::write(file.path(), b"sku,value\nA,1\n").expect("write fixture");

        let (mut path_parser, meta) =
            CsvParser::from_path(file.path(), &["sku"], &CsvOptions::default())
                .await
                .expect("construct path parser");
        let mut reader_parser = CsvParser::from_reader(
            Cursor::new(b"sku,value\nA,1\n"),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect("construct reader parser");

        assert_eq!(path_parser.headers(), reader_parser.headers());
        assert_eq!(
            path_parser.next_record().await.expect("read path record"),
            reader_parser
                .next_record()
                .await
                .expect("read reader record")
        );
        assert!(meta.name_hint.ends_with(".csv"));
    }

    #[tokio::test]
    async fn summary_helpers_use_the_parser_contract() {
        let summary = summarize_csv_stream(
            Cursor::new(b"sku,value\nA,1\nB,2\n"),
            &["sku"],
            &CsvOptions::default(),
        )
        .await
        .expect("summarize reader");
        let file = tempfile::Builder::new()
            .suffix(".csv")
            .tempfile()
            .expect("create fixture");
        std::fs::write(file.path(), b"sku,value\nA,1\nB,2\n").expect("write fixture");
        let (path_summary, _) = summarize_csv_path(file.path(), &["sku"], &CsvOptions::default())
            .await
            .expect("summarize path");
        assert_eq!(summary, path_summary);
        assert_eq!(summary.row_count, 2);
        assert_eq!(summary.headers, ["sku", "value"]);
    }

    #[tokio::test]
    async fn into_inner_returns_the_reader() {
        let parser = CsvParser::from_reader(Cursor::new(b"sku\nA\n"), &[], &CsvOptions::default())
            .await
            .expect("construct parser");

        let _reader = parser.into_inner();
    }
}
