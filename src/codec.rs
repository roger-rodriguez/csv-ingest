use bytes::{Buf, BytesMut};
use encoding_rs::{CoderResult, DecoderResult};
use std::io;
use thiserror::Error;
use tokio_util::codec::Decoder;

/// Controls how malformed byte sequences are handled while transcoding to UTF-8.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DecodePolicy {
    /// Stop at the first malformed sequence and return a [`TranscodingError`].
    #[default]
    Strict,
    /// Replace malformed sequences with Unicode replacement characters (`U+FFFD`).
    Replace,
}

/// A malformed byte sequence encountered while strictly transcoding to UTF-8.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("malformed {encoding} input")]
pub struct TranscodingError {
    encoding: &'static str,
    malformed_sequence_length: u8,
    bytes_after_malformed: u8,
}

impl TranscodingError {
    /// The name of the source encoding.
    pub fn encoding(&self) -> &'static str {
        self.encoding
    }

    /// The number of bytes in the malformed sequence.
    pub fn malformed_sequence_length(&self) -> u8 {
        self.malformed_sequence_length
    }

    /// The number of bytes consumed after the malformed sequence.
    pub fn bytes_after_malformed(&self) -> u8 {
        self.bytes_after_malformed
    }
}

pub struct Transcoder {
    decoder: encoding_rs::Decoder,
    encoding: &'static encoding_rs::Encoding,
    policy: DecodePolicy,
    output: BytesMut,
    finished: bool,
}

impl Transcoder {
    pub fn new(encoding: &'static encoding_rs::Encoding, policy: DecodePolicy) -> Self {
        Self {
            decoder: encoding.new_decoder(),
            encoding,
            policy,
            output: BytesMut::new(),
            finished: false,
        }
    }

    fn output_len(&self, input_len: usize) -> io::Result<usize> {
        let length = match self.policy {
            DecodePolicy::Strict => self
                .decoder
                .max_utf8_buffer_length_without_replacement(input_len),
            DecodePolicy::Replace => self.decoder.max_utf8_buffer_length(input_len),
        };
        length.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "input is too large to allocate a UTF-8 transcoding buffer",
            )
        })
    }

    fn prepare_output(&mut self, input_len: usize) -> io::Result<()> {
        let output_len = self.output_len(input_len)?;
        self.output.resize(output_len, 0);
        Ok(())
    }

    fn malformed(&self, malformed_sequence_length: u8, bytes_after_malformed: u8) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            TranscodingError {
                encoding: self.encoding.name(),
                malformed_sequence_length,
                bytes_after_malformed,
            },
        )
    }

    fn take_output(&mut self, bytes_written: usize) -> Option<BytesMut> {
        self.output.truncate(bytes_written);
        (!self.output.is_empty()).then(|| self.output.split())
    }
}

impl Decoder for Transcoder {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        self.prepare_output(src.len())?;
        let (result, bytes_read, bytes_written) = match self.policy {
            DecodePolicy::Strict => {
                self.decoder
                    .decode_to_utf8_without_replacement(src, &mut self.output, false)
            }
            DecodePolicy::Replace => {
                let (result, bytes_read, bytes_written, _) =
                    self.decoder.decode_to_utf8(src, &mut self.output, false);
                let result = match result {
                    CoderResult::InputEmpty => DecoderResult::InputEmpty,
                    CoderResult::OutputFull => DecoderResult::OutputFull,
                };
                (result, bytes_read, bytes_written)
            }
        };
        src.advance(bytes_read);
        if let DecoderResult::Malformed(length, after) = result {
            return Err(self.malformed(length, after));
        }

        Ok(self.take_output(bytes_written))
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.finished {
            return Ok(None);
        }

        self.prepare_output(buf.len())?;
        let (result, bytes_read, bytes_written) = match self.policy {
            DecodePolicy::Strict => {
                self.decoder
                    .decode_to_utf8_without_replacement(buf, &mut self.output, true)
            }
            DecodePolicy::Replace => {
                let (result, bytes_read, bytes_written, _) =
                    self.decoder.decode_to_utf8(buf, &mut self.output, true);
                let result = match result {
                    CoderResult::InputEmpty => DecoderResult::InputEmpty,
                    CoderResult::OutputFull => DecoderResult::OutputFull,
                };
                (result, bytes_read, bytes_written)
            }
        };
        buf.advance(bytes_read);
        match result {
            DecoderResult::InputEmpty => self.finished = true,
            DecoderResult::OutputFull => {}
            DecoderResult::Malformed(length, after) => {
                return Err(self.malformed(length, after));
            }
        }

        Ok(self.take_output(bytes_written))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_returns_none_for_empty_input() {
        let mut transcoder = Transcoder::new(encoding_rs::WINDOWS_1252, DecodePolicy::Strict);
        let mut input = BytesMut::new();

        assert!(transcoder.decode(&mut input).expect("decode").is_none());
        assert!(transcoder
            .decode_eof(&mut input)
            .expect("decode EOF")
            .is_none());
    }

    #[test]
    fn decode_transcodes_windows_1252_to_utf8() {
        let mut transcoder = Transcoder::new(encoding_rs::WINDOWS_1252, DecodePolicy::Strict);
        let mut input = BytesMut::from(&b"caf\xe9"[..]);

        let output = transcoder
            .decode(&mut input)
            .expect("decode")
            .expect("decoded bytes");

        assert_eq!(output, "café");
        assert!(input.is_empty());
    }

    #[test]
    fn decode_preserves_state_across_split_multibyte_input() {
        let mut transcoder = Transcoder::new(encoding_rs::SHIFT_JIS, DecodePolicy::Strict);
        let mut first = BytesMut::from(&[0x82][..]);
        let first_output = transcoder
            .decode(&mut first)
            .expect("decode first byte")
            .unwrap_or_default();

        let mut second = BytesMut::from(&[0xa0][..]);
        let second_output = transcoder
            .decode(&mut second)
            .expect("decode second byte")
            .unwrap_or_default();

        let mut output = first_output.to_vec();
        output.extend_from_slice(&second_output);
        assert_eq!(output, "あ".as_bytes());
        assert!(first.is_empty());
        assert!(second.is_empty());
    }

    #[test]
    fn decode_eof_flushes_non_utf8_input() {
        let mut transcoder = Transcoder::new(encoding_rs::UTF_16LE, DecodePolicy::Strict);
        let mut input = BytesMut::from(&[b'H', 0, b'i', 0][..]);

        let output = transcoder
            .decode_eof(&mut input)
            .expect("decode EOF")
            .expect("decoded bytes");

        assert_eq!(output, "Hi");
        assert!(input.is_empty());
    }

    #[test]
    fn strict_decode_returns_typed_error_for_malformed_input() {
        let mut transcoder = Transcoder::new(encoding_rs::SHIFT_JIS, DecodePolicy::Strict);
        let mut input = BytesMut::from(&[0x82, 0x20][..]);

        let error = transcoder
            .decode(&mut input)
            .expect_err("malformed input must fail");
        let typed = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<TranscodingError>())
            .expect("typed transcoding error");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(typed.encoding(), "Shift_JIS");
        assert_eq!(typed.malformed_sequence_length(), 1);
        assert_eq!(typed.bytes_after_malformed(), 0);
    }

    #[test]
    fn strict_decode_eof_rejects_an_incomplete_sequence_already_in_decoder_state() {
        let mut transcoder = Transcoder::new(encoding_rs::SHIFT_JIS, DecodePolicy::Strict);
        let mut input = BytesMut::from(&[0x82][..]);

        assert!(transcoder
            .decode(&mut input)
            .expect("decode lead")
            .is_none());
        assert!(input.is_empty());

        let error = transcoder
            .decode_eof(&mut input)
            .expect_err("incomplete EOF sequence must fail");
        assert!(error
            .get_ref()
            .and_then(|source| source.downcast_ref::<TranscodingError>())
            .is_some());
    }

    #[test]
    fn replacement_policy_replaces_malformed_and_incomplete_sequences() {
        let mut malformed = Transcoder::new(encoding_rs::SHIFT_JIS, DecodePolicy::Replace);
        let mut malformed_input = BytesMut::from(&[0x82, 0x20][..]);
        let malformed_output = malformed
            .decode(&mut malformed_input)
            .expect("decode malformed input")
            .expect("replacement output");
        assert_eq!(malformed_output, "� ");

        let mut incomplete = Transcoder::new(encoding_rs::SHIFT_JIS, DecodePolicy::Replace);
        let mut incomplete_input = BytesMut::from(&[0x82][..]);
        assert!(incomplete
            .decode(&mut incomplete_input)
            .expect("decode lead")
            .is_none());
        let replacement = incomplete
            .decode_eof(&mut incomplete_input)
            .expect("flush replacement")
            .expect("replacement output");
        assert_eq!(replacement, "�");
        assert!(incomplete
            .decode_eof(&mut incomplete_input)
            .expect("finished decoder")
            .is_none());
    }

    #[test]
    fn output_storage_is_reused_across_decode_calls() {
        let mut transcoder = Transcoder::new(encoding_rs::WINDOWS_1252, DecodePolicy::Strict);
        let mut first = BytesMut::from(&b"first"[..]);
        let first_output = transcoder
            .decode(&mut first)
            .expect("decode first")
            .expect("first output");
        let remaining_capacity = transcoder.output.capacity();
        let remaining_pointer = transcoder.output.as_ptr();

        let mut second = BytesMut::from(&b"x"[..]);
        let second_output = transcoder
            .decode(&mut second)
            .expect("decode second")
            .expect("second output");

        assert_eq!(first_output, "first");
        assert_eq!(second_output, "x");
        assert!(remaining_capacity >= second_output.len());
        assert_eq!(second_output.as_ptr(), remaining_pointer);
        assert_eq!(
            transcoder.output.capacity(),
            remaining_capacity - second_output.len()
        );
    }
}
