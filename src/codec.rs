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
