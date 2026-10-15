use std::{error::Error as StdError, io::ErrorKind, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{DEFAULT_MAX_FRAME_LEN, Frame, FrameCodec, PipeId, SessionId};
use relaygate_sdk::{
    AccessToken, Config, Error, ErrorCode, ErrorOrigin, PeerObservation, Pipe, Relay, RelayStatus,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
    time::timeout,
};
use tokio_util::codec::Framed;

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

fn assert_invalid_argument(error: &Error) {
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert_eq!(error.origin(), ErrorOrigin::Sdk);
    assert_eq!(error.observation(), PeerObservation::NotObserved);
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_outbound_capacity_is_rejected_before_connecting() -> TestResult {
    timeout(Duration::from_secs(5), async {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        for capacity in [0, Semaphore::MAX_PERMITS + 1] {
            let config = Config::new(format!("tcp://{}", listener.local_addr()?))?
                .with_connect_timeout(Duration::from_millis(20))
                .with_outbound_capacity(capacity);
            let error = match Relay::connect(config).await {
                Ok(relay) => {
                    relay.close();
                    return Err("invalid capacity unexpectedly connected".into());
                }
                Err(error) => error,
            };
            assert!(
                matches!(listener.accept(), Err(error) if error.kind() == ErrorKind::WouldBlock),
                "invalid configuration opened a network connection"
            );
            assert_invalid_argument(&error);
        }
        Ok(())
    })
    .await?
}

async fn echo(pipe: &mut Pipe, payload: &[u8]) -> TestResult {
    pipe.write_all(payload).await?;
    let mut received = vec![0; payload.len()];
    pipe.read_exact(&mut received).await?;
    assert_eq!(received, payload);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reset_message_validation_preserves_pipe_and_sibling_session() -> TestResult {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let gateway = async move {
            let (stream, _) = listener.accept().await?;
            let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            assert!(matches!(transport.next().await, Some(Ok(Frame::Hello))));
            let session_id = SessionId::new();
            transport.send(Frame::Welcome { session_id }).await?;
            let mut pipes = Vec::new();
            let mut resets = 0;
            while let Some(frame) = transport.next().await {
                match frame? {
                    Frame::Dial { connection_id, .. } => {
                        let pipe_id = PipeId::new(session_id, connection_id);
                        pipes.push(pipe_id);
                        transport.send(Frame::Opened { pipe_id }).await?;
                    }
                    Frame::Data { pipe_id, payload } => {
                        assert!(pipes.contains(&pipe_id));
                        transport.send(Frame::Data { pipe_id, payload }).await?;
                    }
                    Frame::Reset {
                        pipe_id, message, ..
                    } => {
                        assert_eq!(pipe_id, pipes[0]);
                        assert_eq!(message.len(), usize::from(u16::MAX));
                        resets += 1;
                    }
                    _ => return Err("unexpected frame".into()),
                }
            }
            assert_eq!(pipes.len(), 2);
            assert_eq!(resets, 1, "invalid and repeated RESET must not be sent");
            Ok::<_, Box<dyn StdError + Send + Sync>>(())
        };
        let client = async {
            let relay = Relay::connect(Config::new(format!("tcp://{address}"))?).await?;
            let token = AccessToken::new("test-token")?;
            let mut first = relay
                .dial("test/reset-first".parse()?, token.clone().into())
                .await?;
            let mut sibling = relay
                .dial("test/reset-sibling".parse()?, token.into())
                .await?;
            let oversized_ascii = "x".repeat(usize::from(u16::MAX) + 1);
            let error = first
                .reset(ErrorCode::Cancelled, oversized_ascii)
                .await
                .err()
                .ok_or("oversized RESET must be rejected")?;
            assert_invalid_argument(&error);
            echo(&mut first, b"original remains usable").await?;
            echo(&mut sibling, b"sibling remains usable").await?;

            let (mut reader, mut writer) = first.into_split();
            let oversized_utf8 = "é".repeat(32768);
            assert_eq!(oversized_utf8.len(), 65536);
            let error = writer
                .reset(ErrorCode::Cancelled, oversized_utf8.clone())
                .await
                .err()
                .ok_or("RESET length is measured in UTF-8 bytes")?;
            assert_invalid_argument(&error);
            writer.write_all(b"split remains usable").await?;
            let mut received = [0; 20];
            reader.read_exact(&mut received).await?;
            assert_eq!(&received, b"split remains usable");

            let maximum_utf8 = format!("{}x", "é".repeat(32767));
            assert_eq!(maximum_utf8.len(), 65535);
            writer.reset(ErrorCode::Cancelled, maximum_utf8).await?;
            writer.reset(ErrorCode::Cancelled, oversized_utf8).await?;
            echo(&mut sibling, b"session survives accepted RESET").await?;
            assert_eq!(relay.status(), RelayStatus::Active);
            assert!(relay.last_error().is_none());
            relay.close();
            Ok::<_, Box<dyn StdError + Send + Sync>>(())
        };
        tokio::try_join!(gateway, client)?;
        Ok(())
    })
    .await?
}
