use bytes::{Bytes, BytesMut};
use relaygate_protocol::{
    BearerToken, BindingId, Destination, ErrorCode, Frame, FrameCodec, MAX_BEARER_TOKEN_BYTES,
    PeerObservation, PipeId, ProtocolError, SessionId,
};
use tokio_util::codec::{Decoder, Encoder};

fn destination() -> Result<Destination, Box<dyn std::error::Error>> {
    Ok("inference/stt.seoul".parse()?)
}

#[test]
fn every_frame_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let session_id = SessionId::new();
    let destination = destination()?;
    let access_token = BearerToken::new("secret")?;
    let binding_id = BindingId::new();
    let pipe_id = PipeId::new(session_id, 42);
    let frames = vec![
        Frame::Hello,
        Frame::Welcome { session_id },
        Frame::SessionRejected {
            code: ErrorCode::Unauthenticated,
            message: "bad token".to_owned(),
        },
        Frame::Publish {
            request_id: 1,
            destination: destination.clone(),
            access_token: access_token.clone(),
        },
        Frame::Published {
            request_id: 1,
            binding_id,
        },
        Frame::PublishFailed {
            request_id: 2,
            code: ErrorCode::Unavailable,
            message: "draining".to_owned(),
        },
        Frame::Unpublish {
            request_id: 3,
            binding_id,
        },
        Frame::Unpublished { request_id: 3 },
        Frame::Dial {
            connection_id: 42,
            destination: destination.clone(),
            access_token,
        },
        Frame::Offer {
            pipe_id,
            binding_id,
            destination,
        },
        Frame::OfferAccepted { pipe_id },
        Frame::OfferRejected {
            pipe_id,
            code: ErrorCode::ResourceExhausted,
            message: "full".to_owned(),
        },
        Frame::Opened { pipe_id },
        Frame::DialFailed {
            connection_id: 42,
            code: ErrorCode::Unavailable,
            observation: PeerObservation::MaybeObserved,
            message: "lost".to_owned(),
        },
        Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"\0binary\xff"),
        },
        Frame::Fin { pipe_id },
        Frame::Close { pipe_id },
        Frame::Reset {
            pipe_id,
            code: ErrorCode::ProtocolError,
            message: "bad state".to_owned(),
        },
        Frame::Ping { nonce: 9 },
        Frame::Pong { nonce: 9 },
        Frame::Cancel { pipe_id },
    ];

    for expected in frames {
        let mut encoded = BytesMut::new();
        FrameCodec::default().encode(expected.clone(), &mut encoded)?;
        let actual = FrameCodec::default().decode(&mut encoded)?;
        assert_eq!(actual, Some(expected));
        assert!(encoded.is_empty());
    }
    Ok(())
}

#[test]
fn fragmented_frame_waits_for_complete_payload() -> Result<(), Box<dyn std::error::Error>> {
    let expected = Frame::Dial {
        connection_id: 7,
        destination: destination()?,
        access_token: BearerToken::new("grant")?,
    };
    let mut encoded = BytesMut::new();
    FrameCodec::default().encode(expected.clone(), &mut encoded)?;
    let split_at = encoded.len().saturating_sub(1);
    let tail = encoded.split_off(split_at);
    let mut codec = FrameCodec::default();
    assert_eq!(codec.decode(&mut encoded)?, None);
    encoded.extend_from_slice(&tail);
    assert_eq!(codec.decode(&mut encoded)?, Some(expected));
    Ok(())
}

#[test]
fn version_two_is_rejected_before_payload_decode() {
    let mut input = BytesMut::from(&b"RG\x02\x01\x00\x00\x00\x00"[..]);

    let error = FrameCodec::default().decode(&mut input);

    assert!(matches!(error, Err(ProtocolError::UnsupportedVersion(2))));
}

#[test]
fn oversized_frame_is_rejected_before_allocation() {
    let mut input = BytesMut::from(&b"RG\x03\x0f\x00\x10\x00\x00"[..]);
    let error = FrameCodec::new(1024).decode(&mut input);
    assert!(error.is_err());
}

#[test]
fn bearer_token_is_bounded_and_redacted() -> Result<(), ProtocolError> {
    let token = BearerToken::new("must-not-appear")?;
    let rendered = format!("{token:?}");
    assert!(!rendered.contains("must-not-appear"));
    assert!(rendered.contains("REDACTED"));
    let frame = Frame::Dial {
        connection_id: 1,
        destination: "inference/stt.seoul"
            .parse()
            .map_err(|_| ProtocolError::InvalidDestination)?,
        access_token: token,
    };
    assert!(!format!("{frame:?}").contains("must-not-appear"));
    assert!(matches!(
        BearerToken::new("x".repeat(MAX_BEARER_TOKEN_BYTES + 1)),
        Err(ProtocolError::FieldTooLong {
            field: "access_token",
            maximum: MAX_BEARER_TOKEN_BYTES,
            ..
        })
    ));
    Ok(())
}

#[test]
fn data_debug_reports_only_payload_length() {
    let frame = Frame::Data {
        pipe_id: PipeId::new(SessionId::new(), 1),
        payload: Bytes::from_static(b"payload-must-not-be-logged"),
    };

    let rendered = format!("{frame:?}");
    assert!(rendered.contains("payload_len: 26"));
    assert!(!rendered.contains("payload-must-not-be-logged"));
}

#[test]
fn data_wire_bytes_are_stable_and_append_after_existing_output() -> Result<(), ProtocolError> {
    let session_id = SessionId::from_uuid(uuid::Uuid::from_bytes([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ]));
    let frame = Frame::Data {
        pipe_id: PipeId::new(session_id, 0x1122_3344_5566_7788),
        payload: Bytes::from_static(&[0, 0x80, 0xfe, 0xff]),
    };
    let mut output = BytesMut::from(&[0xa5, 0x5a][..]);
    FrameCodec::default().encode(frame, &mut output)?;

    assert_eq!(
        &output[..],
        &[
            0xa5, 0x5a, // Existing output.
            b'R', b'G', 3, 15, 0, 0, 0, 28, // Header: 24-byte PipeId + 4 bytes DATA.
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77, 0x88, 0, 0x80, 0xfe, 0xff,
        ]
    );
    Ok(())
}

#[test]
fn empty_data_keeps_its_pipe_id_and_fits_the_exact_limit() -> Result<(), ProtocolError> {
    let expected = Frame::Data {
        pipe_id: PipeId::new(SessionId::new(), 42),
        payload: Bytes::new(),
    };
    let mut codec = FrameCodec::new(24);
    let mut output = BytesMut::new();
    codec.encode(expected.clone(), &mut output)?;
    assert_eq!(output.len(), 32);
    assert_eq!(&output[..8], b"RG\x03\x0f\x00\x00\x00\x18");
    assert_eq!(codec.decode(&mut output)?, Some(expected));
    assert!(output.is_empty());
    Ok(())
}

#[test]
fn data_limit_includes_pipe_id_and_failure_preserves_output() -> Result<(), ProtocolError> {
    let pipe_id = PipeId::new(SessionId::new(), 7);
    for data_len in [0, 8, 65_536] {
        let maximum = 24 + data_len;
        let mut codec = FrameCodec::new(maximum);
        let mut output = BytesMut::from(&b"existing output"[..]);
        codec.encode(
            Frame::Data {
                pipe_id,
                payload: Bytes::from(vec![0x5a; data_len]),
            },
            &mut output,
        )?;
        let before = output.to_vec();
        let pointer = output.as_ptr();
        let capacity = output.capacity();
        let error = codec.encode(
            Frame::Data {
                pipe_id,
                payload: Bytes::from(vec![0x5a; data_len + 1]),
            },
            &mut output,
        );
        assert!(matches!(
            error,
            Err(ProtocolError::FrameTooLarge { actual, maximum: limit })
                if actual == maximum + 1 && limit == maximum
        ));
        assert_eq!(&output[..], &before);
        assert_eq!(output.as_ptr(), pointer);
        assert_eq!(output.capacity(), capacity);
    }

    let mut output = BytesMut::from(&b"unchanged"[..]);
    let error = FrameCodec::new(23).encode(
        Frame::Data {
            pipe_id,
            payload: Bytes::new(),
        },
        &mut output,
    );
    assert!(matches!(
        error,
        Err(ProtocolError::FrameTooLarge {
            actual: 24,
            maximum: 23
        })
    ));
    assert_eq!(&output[..], b"unchanged");
    Ok(())
}

#[test]
fn concatenated_data_and_control_frames_decode_across_fragment_boundaries()
-> Result<(), ProtocolError> {
    let pipe_id = PipeId::new(SessionId::new(), 9);
    let expected = vec![
        Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"\0DATA\xff"),
        },
        Frame::Data {
            pipe_id,
            payload: Bytes::new(),
        },
        Frame::Fin { pipe_id },
        Frame::Ping { nonce: 42 },
    ];
    let mut encoded = BytesMut::new();
    let mut codec = FrameCodec::default();
    for frame in &expected {
        codec.encode(frame.clone(), &mut encoded)?;
    }
    let mut source = BytesMut::new();
    let mut actual = Vec::new();
    for chunk in encoded.chunks(3) {
        source.extend_from_slice(chunk);
        while let Some(frame) = codec.decode(&mut source)? {
            actual.push(frame);
        }
    }
    assert_eq!(actual, expected);
    assert!(source.is_empty());
    Ok(())
}
