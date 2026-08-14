use crate::{CsvIngestError, CsvResult};
use csv_async::{AsyncReaderBuilder, Terminator, Trim};

/// How the first CSV record is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsvHeaderMode {
    /// The first record contains column names and is not counted as a data row.
    Present,
    /// Every record is data. Named required-header validation is unavailable.
    Absent,
}

/// How CSV records are terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsvTerminator {
    /// Accept `\r`, `\n`, or `\r\n` as a record terminator.
    CrLf,
    /// Use exactly one byte as the record terminator.
    Any(u8),
}

/// ASCII whitespace trimming applied while parsing byte records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsvTrim {
    None,
    Headers,
    Fields,
    All,
}

/// CSV dialect options shared by streaming and fast-local parsing.
///
/// A leading UTF-8 BOM is always stripped. Character transcoding, compression,
/// content type, and filename hints remain transport concerns configured with
/// [`crate::CsvMeta`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CsvOptions {
    pub delimiter: u8,
    pub terminator: CsvTerminator,
    pub headers: CsvHeaderMode,
    pub flexible: bool,
    pub trim: CsvTrim,
    pub quoting: bool,
    pub quote: u8,
    pub escape: Option<u8>,
    pub double_quote: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            terminator: CsvTerminator::CrLf,
            headers: CsvHeaderMode::Present,
            flexible: false,
            trim: CsvTrim::None,
            quoting: true,
            quote: b'"',
            escape: None,
            double_quote: true,
        }
    }
}

impl CsvOptions {
    /// Validate byte combinations before constructing a parser.
    pub fn validate(&self) -> CsvResult<()> {
        if self.is_terminator(self.delimiter) {
            return Err(CsvIngestError::UnsupportedDialect(
                "delimiter cannot also be a record terminator".to_string(),
            ));
        }

        if self.quoting && (self.quote == self.delimiter || self.is_terminator(self.quote)) {
            return Err(CsvIngestError::UnsupportedDialect(
                "quote cannot also be the delimiter or a record terminator".to_string(),
            ));
        }

        if self
            .escape
            .is_some_and(|escape| escape == self.delimiter || self.is_terminator(escape))
        {
            return Err(CsvIngestError::UnsupportedDialect(
                "escape cannot also be the delimiter or a record terminator".to_string(),
            ));
        }

        Ok(())
    }

    pub(crate) fn configure_reader(&self, builder: &mut AsyncReaderBuilder) -> CsvResult<()> {
        self.validate()?;
        builder
            .delimiter(self.delimiter)
            .terminator(match self.terminator {
                CsvTerminator::CrLf => Terminator::CRLF,
                CsvTerminator::Any(byte) => Terminator::Any(byte),
            })
            .has_headers(self.headers == CsvHeaderMode::Present)
            .flexible(self.flexible)
            .trim(match self.trim {
                CsvTrim::None => Trim::None,
                CsvTrim::Headers => Trim::Headers,
                CsvTrim::Fields => Trim::Fields,
                CsvTrim::All => Trim::All,
            })
            .quoting(self.quoting)
            .quote(self.quote)
            .escape(self.escape)
            .double_quote(self.double_quote);
        Ok(())
    }

    #[cfg(feature = "fast_local")]
    pub(crate) fn trims_headers(self) -> bool {
        matches!(self.trim, CsvTrim::Headers | CsvTrim::All)
    }

    #[cfg(feature = "fast_local")]
    pub(crate) fn trims_fields(self) -> bool {
        matches!(self.trim, CsvTrim::Fields | CsvTrim::All)
    }

    pub(crate) fn is_terminator(self, byte: u8) -> bool {
        match self.terminator {
            CsvTerminator::CrLf => matches!(byte, b'\r' | b'\n'),
            CsvTerminator::Any(terminator) => byte == terminator,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_standard_and_strict() {
        let options = CsvOptions::default();

        assert_eq!(options.delimiter, b',');
        assert_eq!(options.terminator, CsvTerminator::CrLf);
        assert_eq!(options.headers, CsvHeaderMode::Present);
        assert!(!options.flexible);
        assert_eq!(options.trim, CsvTrim::None);
        assert!(options.quoting);
        assert_eq!(options.quote, b'"');
        assert_eq!(options.escape, None);
        assert!(options.double_quote);
        options.validate().expect("default options are valid");
    }

    #[test]
    fn delimiter_cannot_be_a_terminator() {
        let options = CsvOptions {
            delimiter: b'\n',
            ..CsvOptions::default()
        };

        assert!(matches!(
            options.validate(),
            Err(CsvIngestError::UnsupportedDialect(message))
                if message.contains("delimiter")
        ));
    }

    #[test]
    fn active_quote_cannot_conflict_with_csv_separators() {
        let options = CsvOptions {
            quote: b',',
            ..CsvOptions::default()
        };

        assert!(matches!(
            options.validate(),
            Err(CsvIngestError::UnsupportedDialect(message)) if message.contains("quote")
        ));

        CsvOptions {
            quoting: false,
            quote: b',',
            ..CsvOptions::default()
        }
        .validate()
        .expect("disabled quoting makes the quote byte irrelevant");
    }

    #[test]
    fn escape_cannot_conflict_with_csv_separators() {
        let options = CsvOptions {
            escape: Some(b'\r'),
            ..CsvOptions::default()
        };

        assert!(matches!(
            options.validate(),
            Err(CsvIngestError::UnsupportedDialect(message)) if message.contains("escape")
        ));
    }
}
