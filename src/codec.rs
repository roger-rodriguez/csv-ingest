use bytes::{Buf, BytesMut};
use std::io;
use tokio_util::codec::Decoder;

pub struct Transcoder {
    decoder: encoding_rs::Decoder,
}

impl Transcoder {
    pub fn new(encoding: &'static encoding_rs::Encoding) -> Self {
        Self {
            decoder: encoding.new_decoder(),
        }
    }
}

impl Decoder for Transcoder {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut temp_out = vec![
            0;
            self.decoder
                .max_utf8_buffer_length_without_replacement(src.len())
                .unwrap_or_else(|| src.len() * 2)
        ];

        let (_result, bytes_read, bytes_written, _has_errors) =
            self.decoder.decode_to_utf8(src, &mut temp_out, false);

        if bytes_read == 0 && bytes_written == 0 && !src.is_empty() {
            return Ok(None);
        }

        src.advance(bytes_read);
        Ok(Some(BytesMut::from(&temp_out[..bytes_written])))
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        let mut temp_out = vec![
            0;
            self.decoder
                .max_utf8_buffer_length(buf.len())
                .unwrap_or_else(|| buf.len() * 2)
        ];
        let (_result, _bytes_read, bytes_written, _has_errors) =
            self.decoder.decode_to_utf8(buf, &mut temp_out, true);

        buf.clear();

        if bytes_written > 0 {
            Ok(Some(BytesMut::from(&temp_out[..bytes_written])))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_returns_none_for_empty_input() {
        let mut transcoder = Transcoder::new(encoding_rs::WINDOWS_1252);
        let mut input = BytesMut::new();

        assert!(transcoder.decode(&mut input).expect("decode").is_none());
        assert!(transcoder
            .decode_eof(&mut input)
            .expect("decode EOF")
            .is_none());
    }

    #[test]
    fn decode_transcodes_windows_1252_to_utf8() {
        let mut transcoder = Transcoder::new(encoding_rs::WINDOWS_1252);
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
        let mut transcoder = Transcoder::new(encoding_rs::SHIFT_JIS);
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
        let mut transcoder = Transcoder::new(encoding_rs::UTF_16LE);
        let mut input = BytesMut::from(&[b'H', 0, b'i', 0][..]);

        let output = transcoder
            .decode_eof(&mut input)
            .expect("decode EOF")
            .expect("decoded bytes");

        assert_eq!(output, "Hi");
        assert!(input.is_empty());
    }
}
