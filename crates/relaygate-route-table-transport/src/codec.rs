use std::io::{self, Write};

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{TransportError, frame::WireFrame};

const MAGIC: [u8; 2] = *b"RT";
const VERSION: u8 = 3;
const HEADER_LEN: usize = 7;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CodecError {
    #[error("frame I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid RouteTable frame magic")]
    InvalidMagic,
    #[error("unsupported RouteTable protocol version {0}")]
    UnsupportedVersion(u8),
    #[error("RouteTable frame length {actual} exceeds configured maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("RouteTable frame length cannot be represented on the wire")]
    LengthOverflow,
    #[error("invalid RouteTable frame payload")]
    InvalidPayload,
}

impl CodecError {
    #[must_use]
    pub(crate) const fn is_io(&self) -> bool {
        matches!(self, Self::Io(_))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FrameCodec {
    max_frame_len: usize,
}

impl FrameCodec {
    pub(crate) const fn new(max_frame_len: usize) -> Self {
        Self { max_frame_len }
    }

    pub(crate) fn validate(&self, frame: &WireFrame) -> Result<(), CodecError> {
        let mut counter = BoundedFrameCounter::new(self.max_frame_len);
        match serde_json::to_writer(&mut counter, frame) {
            Ok(()) => self.validate_payload_len(counter.len),
            Err(_) if counter.exceeded_at.is_some() => Err(CodecError::FrameTooLarge {
                actual: counter.exceeded_at.unwrap_or(self.max_frame_len),
                maximum: self.max_frame_len,
            }),
            Err(_) => Err(CodecError::InvalidPayload),
        }
    }

    fn validate_payload_len(&self, payload_len: usize) -> Result<(), CodecError> {
        if payload_len > self.max_frame_len {
            return Err(CodecError::FrameTooLarge {
                actual: payload_len,
                maximum: self.max_frame_len,
            });
        }
        let _ = u32::try_from(payload_len).map_err(|_| CodecError::LengthOverflow)?;
        Ok(())
    }
}

struct BoundedFrameCounter {
    len: usize,
    maximum: usize,
    exceeded_at: Option<usize>,
}

impl BoundedFrameCounter {
    const fn new(maximum: usize) -> Self {
        Self {
            len: 0,
            maximum,
            exceeded_at: None,
        }
    }
}

impl Write for BoundedFrameCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let attempted = self.len.saturating_add(buffer.len());
        if attempted > self.maximum {
            self.exceeded_at = Some(attempted);
            return Err(io::Error::other(
                "RouteTable frame exceeds configured maximum",
            ));
        }
        self.len = attempted;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Encoder<WireFrame> for FrameCodec {
    type Error = CodecError;

    fn encode(&mut self, frame: WireFrame, destination: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = serde_json::to_vec(&frame).map_err(|_| CodecError::InvalidPayload)?;
        self.validate_payload_len(payload.len())?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| CodecError::LengthOverflow)?;
        destination.reserve(HEADER_LEN + payload.len());
        destination.extend_from_slice(&MAGIC);
        destination.put_u8(VERSION);
        destination.put_u32(payload_len);
        destination.extend_from_slice(&payload);
        Ok(())
    }
}

impl Decoder for FrameCodec {
    type Item = WireFrame;
    type Error = CodecError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if source.len() < HEADER_LEN {
            return Ok(None);
        }
        if source[..2] != MAGIC {
            return Err(CodecError::InvalidMagic);
        }
        let version = source[2];
        if version != VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }
        let payload_len = u32::from_be_bytes([source[3], source[4], source[5], source[6]]) as usize;
        if payload_len > self.max_frame_len {
            return Err(CodecError::FrameTooLarge {
                actual: payload_len,
                maximum: self.max_frame_len,
            });
        }
        let frame_len = HEADER_LEN
            .checked_add(payload_len)
            .ok_or(CodecError::LengthOverflow)?;
        if source.len() < frame_len {
            source.reserve(frame_len - source.len());
            return Ok(None);
        }

        source.advance(HEADER_LEN);
        let payload = source.split_to(payload_len);
        serde_json::from_slice(&payload)
            .map(Some)
            .map_err(|_| CodecError::InvalidPayload)
    }
}

/// A send-side codec failure as the caller's transport error.
pub(crate) fn map_send_codec_error(error: CodecError) -> TransportError {
    match error {
        CodecError::FrameTooLarge { .. } | CodecError::LengthOverflow => {
            TransportError::resource_exhausted(error.to_string())
        }
        _ if error.is_io() => TransportError::unavailable(error.to_string()),
        _ => TransportError::protocol(error.to_string()),
    }
}

/// A receive-side codec failure as the caller's transport error.
pub(crate) fn map_receive_codec_error(error: CodecError) -> TransportError {
    if error.is_io() {
        TransportError::unavailable(error.to_string())
    } else {
        TransportError::protocol(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::frame::{GATEWAY_ROLE, ROUTE_TABLE_ROLE};

    #[test]
    fn round_trip() -> Result<(), CodecError> {
        let mut codec = FrameCodec::new(1024);
        let mut bytes = BytesMut::new();
        codec.encode(
            WireFrame::Welcome {
                role: ROUTE_TABLE_ROLE.to_owned(),
            },
            &mut bytes,
        )?;

        let decoded = codec.decode(&mut bytes)?;
        assert!(matches!(
            decoded,
            Some(WireFrame::Welcome { role }) if role == ROUTE_TABLE_ROLE
        ));
        Ok(())
    }

    #[test]
    fn invalid_payload_errors_do_not_expose_input() -> Result<(), Box<dyn std::error::Error>> {
        let marker = "RT_PRIVATE_INPUT_DO_NOT_LOG";
        let payloads = [
            format!(r#"{{"frame":"{marker}"}}"#),
            format!(r#"{{"frame":"WELCOME","role":"route_table","{marker}":true}}"#),
            format!(r#"{{"frame":"REQUEST","role":"gateway","request_id":"{marker}"}}"#),
        ];
        let mappers: [fn(CodecError) -> TransportError; 2] =
            [map_receive_codec_error, map_send_codec_error];

        for payload in payloads {
            let parser_error = serde_json::from_str::<WireFrame>(&payload)
                .err()
                .ok_or("invalid payload was accepted")?;
            assert!(parser_error.to_string().contains(marker));

            let mut bytes = BytesMut::new();
            bytes.extend_from_slice(&MAGIC);
            bytes.put_u8(VERSION);
            bytes.put_u32(u32::try_from(payload.len())?);
            bytes.extend_from_slice(payload.as_bytes());

            let mut codec = FrameCodec::new(1024);
            let error = codec
                .decode(&mut bytes.clone())
                .err()
                .ok_or("invalid frame was accepted")?;
            assert!(!format!("{error:?}").contains(marker));
            assert_eq!(error.to_string(), "invalid RouteTable frame payload");
            assert!(std::error::Error::source(&error).is_none());

            for map_error in mappers {
                let error = codec
                    .decode(&mut bytes.clone())
                    .err()
                    .ok_or("invalid frame was accepted")?;
                let error = map_error(error);
                assert_eq!(error.code(), crate::ErrorCode::ProtocolError);
                assert_eq!(error.message(), "invalid RouteTable frame payload");
                assert!(!format!("{error:?}").contains(marker));
                assert!(!error.to_string().contains(marker));
            }
        }
        Ok(())
    }

    #[test]
    fn rejects_unsupported_version() -> Result<(), CodecError> {
        let mut codec = FrameCodec::new(1024);
        let mut bytes = BytesMut::new();
        codec.encode(
            WireFrame::Welcome {
                role: GATEWAY_ROLE.to_owned(),
            },
            &mut bytes,
        )?;
        for rejected in [1, 2, VERSION + 1] {
            let mut invalid = bytes.clone();
            invalid[2] = rejected;
            assert!(matches!(
                codec.decode(&mut invalid),
                Err(CodecError::UnsupportedVersion(actual)) if actual == rejected
            ));
        }
        Ok(())
    }

    #[test]
    fn rejects_oversized_declared_frame_before_payload_arrives() {
        let mut codec = FrameCodec::new(8);
        let mut bytes = BytesMut::from(&[MAGIC[0], MAGIC[1], VERSION, 0, 0, 0, 9][..]);

        let error = codec.decode(&mut bytes).err();
        assert!(matches!(
            error,
            Some(CodecError::FrameTooLarge {
                actual: 9,
                maximum: 8
            })
        ));
    }
}
