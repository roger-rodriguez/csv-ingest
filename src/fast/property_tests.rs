use super::{fast_local_process_with_workers, FIELD_SEPARATOR};
use crate::{
    ByteRecord, CsvHeaderMode, CsvIngestError, CsvIngestSummary, CsvOptions, CsvParser, CsvResult,
};
use crc32fast::Hasher as Crc32;
use proptest::prelude::*;
use std::io::{Cursor, Write};
use std::sync::OnceLock;
use tempfile::NamedTempFile;

const WORKER_COUNTS: [usize; 3] = [1, 2, 8];

#[derive(Clone, Copy, Debug)]
enum LineEnding {
    Lf,
    CrLf,
}

impl LineEnding {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::Lf => b"\n",
            Self::CrLf => b"\r\n",
        }
    }
}

#[derive(Clone, Debug)]
struct DifferentialCase {
    width: usize,
    rows: Vec<Vec<Vec<u8>>>,
    line_ending: LineEnding,
    final_terminator: bool,
    headers: bool,
    flexible: bool,
    require_last_header: bool,
    bom: bool,
    delimiter: u8,
    limit: Option<u64>,
}

impl DifferentialCase {
    fn options(&self) -> CsvOptions {
        CsvOptions {
            delimiter: self.delimiter,
            headers: if self.headers {
                CsvHeaderMode::Present
            } else {
                CsvHeaderMode::Absent
            },
            flexible: self.flexible,
            quoting: false,
            ..CsvOptions::default()
        }
    }

    fn required_headers(&self) -> Vec<String> {
        if self.headers && self.require_last_header {
            vec![format!("column_{}", self.width - 1)]
        } else {
            Vec::new()
        }
    }

    fn render(&self) -> Vec<u8> {
        let mut records = Vec::with_capacity(self.rows.len() + usize::from(self.headers));
        if self.headers {
            records.push(
                (0..self.width)
                    .map(|index| format!("column_{index}").into_bytes())
                    .collect(),
            );
        }
        records.extend(self.rows.iter().cloned());

        let mut bytes = Vec::new();
        if self.bom {
            bytes.extend_from_slice(b"\xef\xbb\xbf");
        }
        let last_record = records.len().saturating_sub(1);
        for (record_index, record) in records.iter().enumerate() {
            for (field_index, field) in record.iter().enumerate() {
                if field_index > 0 {
                    bytes.push(self.delimiter);
                }
                bytes.extend_from_slice(field);
            }
            if record_index < last_record || self.final_terminator {
                bytes.extend_from_slice(self.line_ending.bytes());
            }
        }
        bytes
    }
}

#[derive(Debug, PartialEq, Eq)]
struct VerifiedOutput {
    summary: CsvIngestSummary,
    crc: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum ErrorFingerprint {
    MissingHeader(String),
    MissingField {
        row: u64,
        header: String,
    },
    RaggedRow {
        row: Option<u64>,
        expected: u64,
        actual: u64,
    },
    Other(String),
}

type Outcome = Result<VerifiedOutput, ErrorFingerprint>;

fn fingerprint(error: CsvIngestError) -> ErrorFingerprint {
    match error {
        CsvIngestError::MissingHeader(header) => ErrorFingerprint::MissingHeader(header),
        CsvIngestError::MissingRequiredField { row, header } => {
            ErrorFingerprint::MissingField { row, header }
        }
        CsvIngestError::RaggedRow {
            row,
            expected,
            actual,
        } => ErrorFingerprint::RaggedRow {
            row,
            expected,
            actual,
        },
        error => ErrorFingerprint::Other(format!("{error:?}")),
    }
}

async fn streaming_outcome(
    bytes: &[u8],
    required_headers: &[&str],
    options: &CsvOptions,
    limit: Option<u64>,
) -> Outcome {
    async fn parse(
        bytes: &[u8],
        required_headers: &[&str],
        options: &CsvOptions,
        limit: Option<u64>,
    ) -> CsvResult<VerifiedOutput> {
        let mut parser =
            CsvParser::from_reader(Cursor::new(bytes), required_headers, options).await?;
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
            for (field_index, field) in record.iter().enumerate() {
                if field_index > 0 {
                    crc.update(&[FIELD_SEPARATOR]);
                }
                crc.update(field);
            }
        }

        Ok(VerifiedOutput {
            summary: CsvIngestSummary {
                row_count: parser.records_read(),
                headers,
            },
            crc: crc.finalize(),
        })
    }

    parse(bytes, required_headers, options, limit)
        .await
        .map_err(fingerprint)
}

fn fast_outcome(
    file: &NamedTempFile,
    required_headers: &[&str],
    options: &CsvOptions,
    limit: Option<u64>,
    workers: usize,
) -> Outcome {
    fast_local_process_with_workers(file.path(), required_headers, options, true, limit, workers)
        .map(|(summary, crc)| VerifiedOutput {
            summary,
            crc: crc.expect("verification requested"),
        })
        .map_err(fingerprint)
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("create property-test runtime"))
}

fn fixture(bytes: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create differential fixture");
    file.write_all(bytes).expect("write differential fixture");
    file.flush().expect("flush differential fixture");
    file
}

fn assert_fixture_matches(
    bytes: &[u8],
    required_headers: &[String],
    options: &CsvOptions,
    limits: &[Option<u64>],
) {
    let file = fixture(bytes);
    let required: Vec<&str> = required_headers.iter().map(String::as_str).collect();

    for &limit in limits {
        let expected = runtime().block_on(streaming_outcome(bytes, &required, options, limit));
        for workers in WORKER_COUNTS {
            let actual = fast_outcome(&file, &required, options, limit, workers);
            assert_eq!(
                actual, expected,
                "fixture={bytes:?}, options={options:?}, limit={limit:?}, workers={workers}"
            );
        }
    }
}

fn field_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        prop::sample::select(
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-".to_vec(),
        ),
        0..=16,
    )
}

fn differential_case_strategy() -> impl Strategy<Value = DifferentialCase> {
    (
        1usize..=12,
        0usize..=18,
        any::<bool>(),
        prop_oneof![Just(LineEnding::Lf), Just(LineEnding::CrLf)],
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        prop::sample::select(vec![b',', b';', b'|', b'\t']),
        prop_oneof![3 => Just(None), 1 => (0u64..=20).prop_map(Some)],
    )
        .prop_flat_map(
            |(
                width,
                row_count,
                ragged,
                line_ending,
                final_terminator,
                headers,
                flexible,
                require_last_header,
                bom,
                delimiter,
                limit,
            )| {
                let row = if ragged {
                    (1usize..=(width + 2))
                        .prop_flat_map(|row_width| {
                            prop::collection::vec(field_strategy(), row_width)
                        })
                        .boxed()
                } else {
                    prop::collection::vec(field_strategy(), width).boxed()
                };

                prop::collection::vec(row, row_count).prop_map(move |rows| DifferentialCase {
                    width,
                    rows,
                    line_ending,
                    final_terminator,
                    headers,
                    flexible,
                    require_last_header,
                    bom,
                    delimiter,
                    limit,
                })
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        max_shrink_iters: 4_096,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_supported_csv_matches_streaming(case in differential_case_strategy()) {
        let bytes = case.render();
        let options = case.options();
        let required_headers = case.required_headers();
        let required: Vec<&str> = required_headers.iter().map(String::as_str).collect();
        let expected = runtime().block_on(streaming_outcome(
            &bytes,
            &required,
            &options,
            case.limit,
        ));
        let file = fixture(&bytes);

        for workers in WORKER_COUNTS {
            let actual = fast_outcome(&file, &required, &options, case.limit, workers);
            prop_assert_eq!(
                &actual,
                &expected,
                "case={:#?}\nfixture={:?}\nworkers={}",
                case,
                bytes,
                workers
            );
        }
    }
}

#[test]
fn explicit_edge_case_matrix_matches_streaming() {
    let default = CsvOptions {
        quoting: false,
        ..CsvOptions::default()
    };
    let no_required = Vec::new();

    for bytes in [
        &b""[..],
        &b"\n"[..],
        &b"a,b\n"[..],
        &b"a,b\n1,2\n"[..],
        &b"a,b\n1,2"[..],
        &b"a,b\r\n1,2\r\n"[..],
        &b"a,b\r\n1,2"[..],
        &b"\n\r\na,b\n1,2\n"[..],
        &b"a,b,c\n,middle,\n"[..],
        &b"a,b\n1\n"[..],
    ] {
        assert_fixture_matches(bytes, &no_required, &default, &[None]);
    }

    let headers: Vec<String> = (0..128).map(|index| format!("column_{index}")).collect();
    let values: Vec<String> = (0..128).map(|index| format!("value_{index}")).collect();
    let wide = format!("{}\n{}\n", headers.join(","), values.join(","));
    assert_fixture_matches(wide.as_bytes(), &no_required, &default, &[None]);

    let limited = b"a,b\n1,2\n3,4\n5,6\n";
    assert_fixture_matches(
        limited,
        &no_required,
        &default,
        &[Some(0), Some(1), Some(2), Some(100)],
    );

    let limited_with_blank_records = b"a,b\n\n1,2\n\n3,4\n";
    assert_fixture_matches(
        limited_with_blank_records,
        &no_required,
        &default,
        &[None, Some(0), Some(1), Some(2), Some(100)],
    );
}
