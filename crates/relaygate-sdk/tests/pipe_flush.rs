use std::{error::Error as StdError, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{DEFAULT_MAX_FRAME_LEN, Frame, FrameCodec, PipeId, SessionId};
use relaygate_sdk::{AccessToken, AccessTokenSource, Config, Pipe, Relay};
use tokio::{io::AsyncWriteExt, net::TcpListener, task::JoinHandle, time::timeout};
use tokio_util::codec::Framed;

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
// More than two DATA frames, while fitting in the default outbound queue.
const PAYLOAD_LEN: usize = 128 * 1024 + 17;

#[derive(Default)]
struct Received {
    payload: Vec<u8>,
    fins: usize,
}

async fn connect_pipe() -> TestResult<(Relay, Pipe, JoinHandle<TestResult<Received>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let gateway = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(transport.next().await, Some(Ok(Frame::Hello))) {
            return Err("expected HELLO".into());
        }
        let session_id = SessionId::new();
        transport.send(Frame::Welcome { session_id }).await?;
        let connection_id = match transport.next().await.ok_or("EOF before DIAL")?? {
            Frame::Dial { connection_id, .. } => connection_id,
            _ => return Err("expected DIAL".into()),
        };
        let expected_pipe_id = PipeId::new(session_id, connection_id);
        transport
            .send(Frame::Opened {
                pipe_id: expected_pipe_id,
            })
            .await?;

        let mut received = Received::default();
        while let Some(frame) = transport.next().await {
            match frame? {
                Frame::Data { pipe_id, payload } => {
                    assert_eq!(pipe_id, expected_pipe_id);
                    assert_eq!(received.fins, 0, "DATA must precede FIN");
                    received.payload.extend_from_slice(&payload);
                }
                Frame::Fin { pipe_id } => {
                    assert_eq!(pipe_id, expected_pipe_id);
                    received.fins += 1;
                }
                _ => return Err("unexpected frame after OPENED".into()),
            }
        }
        Ok(received)
    });

    let relay = Relay::connect(Config::new(format!("tcp://{address}"))?).await?;
    let pipe = relay
        .dial(
            "test/flush-completion".parse()?,
            AccessTokenSource::static_token(AccessToken::new("test-token")?),
        )
        .await?;
    Ok((relay, pipe, gateway))
}

#[tokio::test(flavor = "current_thread")]
async fn flush_completes_before_immediate_relay_close() -> TestResult {
    timeout(TEST_TIMEOUT, async {
        let (relay, mut pipe, gateway) = connect_pipe().await?;
        let payload = vec![0x5a; PAYLOAD_LEN];
        pipe.write_all(&payload).await?;
        pipe.flush().await?;
        // No scheduler yield between successful flush and immediate termination.
        relay.close();

        let received = gateway.await??;
        assert_eq!(received.payload.len(), payload.len());
        assert!(
            received.payload == payload,
            "Gateway received different bytes"
        );
        assert_eq!(received.fins, 0, "flush must keep the write direction open");
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_flushes_data_and_fin_before_immediate_relay_close() -> TestResult {
    timeout(TEST_TIMEOUT, async {
        let (relay, mut pipe, gateway) = connect_pipe().await?;
        let payload = vec![0x3c; PAYLOAD_LEN];
        pipe.write_all(&payload).await?;
        // AsyncWrite shutdown must flush pending DATA without a separate flush.
        pipe.shutdown().await?;
        relay.close();

        let received = gateway.await??;
        assert_eq!(received.payload.len(), payload.len());
        assert!(
            received.payload == payload,
            "Gateway received different bytes"
        );
        assert_eq!(received.fins, 1);
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "current_thread")]
async fn split_write_shutdown_flushes_data_and_fin_before_immediate_relay_close() -> TestResult {
    timeout(TEST_TIMEOUT, async {
        let (relay, pipe, gateway) = connect_pipe().await?;
        let (_reader, mut writer) = pipe.into_split();
        let payload = vec![0xa5; PAYLOAD_LEN];
        writer.write_all(&payload).await?;
        writer.shutdown_write().await?;
        relay.close();

        let received = gateway.await??;
        assert_eq!(received.payload.len(), payload.len());
        assert!(
            received.payload == payload,
            "Gateway received different bytes"
        );
        assert_eq!(received.fins, 1);
        Ok(())
    })
    .await?
}
