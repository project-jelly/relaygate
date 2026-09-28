use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak, atomic::AtomicU64},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{
    BindingId, ErrorCode as WireErrorCode, Frame, FrameCodec, PipeId, SessionId,
};
use relaygate_transport::BoxedIo;
use tokio::{
    io::duplex,
    sync::{Notify, Semaphore, mpsc, watch},
    time::Instant,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use super::handle_relay_frame;
use crate::{
    AccessToken, AccessTokenSource, Config, Destination, ListenerStatus,
    lifetime::RuntimeLifetime,
    listener::{
        ListenerLifecycle, ListenerRuntime, ListenerState, RelayInner, RelaySession, RelayStatus,
    },
    pipe::PipeState,
    resource::RelayResources,
    session::{ReconnectBackoff, SessionLink, session_outbound_channel},
};

use super::super::{Registration, RelayFrameAction, RelaySessionState};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(start_paused = true)]
async fn full_listener_queue_rejects_offer_immediately_and_preserves_session_frames() -> TestResult
{
    let config = Config::new_insecure_for_tests("127.0.0.1:1");
    let limits = config.resource_limits;
    let destination: Destination = "inference/stt.seoul".parse()?;
    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    let (status, _) = watch::channel(ListenerStatus::Active);
    let listener = Arc::new(ListenerState {
        destination: destination.clone(),
        access_token_source: AccessTokenSource::static_token(AccessToken::new("grant")?),
        status,
        incoming_tx,
        incoming_rx: tokio::sync::Mutex::new(incoming_rx),
        initial_deadline: Instant::now() + Duration::from_secs(10),
        runtime: StdMutex::new(ListenerRuntime::new(ListenerLifecycle::Returned)),
        live_pipe_slots: Arc::new(Semaphore::new(1)),
    });
    let session_id = SessionId::new();
    let (current, _) = watch::channel(Some(Arc::new(RelaySession {
        id: session_id,
        next_connection_id: tokio::sync::Mutex::new(1),
        commands: mpsc::channel(1).0,
        cancellations: mpsc::unbounded_channel().0,
        cancel: CancellationToken::new(),
    })));
    let (relay_status, _) = watch::channel(RelayStatus::Active);
    let inner = RelayInner {
        config,
        desired: StdMutex::new(HashMap::from([(
            destination.clone(),
            Arc::clone(&listener),
        )])),
        current,
        status: relay_status,
        last_error: StdMutex::new(None),
        reconcile: Arc::new(Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: Weak::<RuntimeLifetime>::new(),
        resources: RelayResources::new(limits),
        reconnect_degraded: std::sync::atomic::AtomicBool::new(false),
        desired_settlement_calls: AtomicU64::new(0),
        republish_retry_epoch: Arc::new(AtomicU64::new(0)),
        republish_backoff: Arc::new(StdMutex::new(ReconnectBackoff::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
        ))),
    };
    let binding_id = BindingId::new();
    let mut state = RelaySessionState::new();
    state.registrations.insert(
        destination.clone(),
        Registration {
            state: Arc::clone(&listener),
            binding_id,
        },
    );

    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let (queued_pipe, _queued_state) = PipeState::pair(
        PipeId::new(SessionId::new(), 1),
        outbound.clone(),
        1,
        abandoned.clone(),
    );
    listener
        .incoming_tx
        .try_send(queued_pipe)
        .map_err(|_| "failed to fill Listener queue")?;

    let (sdk_io, peer_io) = duplex(4 * 1024);
    let mut transport = Framed::new(Box::new(sdk_io) as BoxedIo, FrameCodec::default());
    let mut peer = Framed::new(Box::new(peer_io) as BoxedIo, FrameCodec::default());
    let cancel = CancellationToken::new();
    let started_at = Instant::now();
    let rejected_pipe_id = PipeId::new(SessionId::new(), 2);

    let action = handle_relay_frame(
        Frame::Offer {
            pipe_id: rejected_pipe_id,
            binding_id,
            destination,
        },
        session_id,
        &mut state,
        &outbound,
        &abandoned,
        &inner,
        &mut SessionLink::new(&mut transport, inner.config.operation_timeout, &cancel),
    )
    .await;
    assert!(matches!(action, RelayFrameAction::Continue));
    assert_eq!(Instant::now(), started_at);
    assert!(matches!(
        peer.next().await.transpose()?,
        Some(Frame::OfferRejected {
            pipe_id,
            code: WireErrorCode::ResourceExhausted,
            ..
        }) if pipe_id == rejected_pipe_id
    ));

    let action = handle_relay_frame(
        Frame::Ping { nonce: 7 },
        session_id,
        &mut state,
        &outbound,
        &abandoned,
        &inner,
        &mut SessionLink::new(&mut transport, inner.config.operation_timeout, &cancel),
    )
    .await;
    assert!(matches!(action, RelayFrameAction::Continue));
    assert_eq!(Instant::now(), started_at);
    assert!(matches!(
        peer.next().await.transpose()?,
        Some(Frame::Pong { nonce: 7 })
    ));

    peer.close().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropped_pipe_keeps_its_entry_until_close_is_sent() -> TestResult {
    let config = Config::new_insecure_for_tests("127.0.0.1:1");
    let limits = config.resource_limits;
    let (current, _) = watch::channel(None);
    let (relay_status, _) = watch::channel(RelayStatus::Active);
    let inner = RelayInner {
        config,
        desired: StdMutex::new(HashMap::new()),
        current,
        status: relay_status,
        last_error: StdMutex::new(None),
        reconcile: Arc::new(Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: Weak::<RuntimeLifetime>::new(),
        resources: RelayResources::new(limits),
        reconnect_degraded: std::sync::atomic::AtomicBool::new(false),
        desired_settlement_calls: AtomicU64::new(0),
        republish_retry_epoch: Arc::new(AtomicU64::new(0)),
        republish_backoff: Arc::new(StdMutex::new(ReconnectBackoff::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
        ))),
    };
    let session_id = SessionId::new();
    let mut state = RelaySessionState::new();
    let (outbound, _outbound_rx) = session_outbound_channel(4);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(session_id, 1);
    let (pipe, pipe_state) = PipeState::pair(pipe_id, outbound.clone(), 4, abandoned.clone());
    state.pipes.insert(
        pipe_id,
        super::super::LivePipe {
            state: pipe_state,
            listener: None,
        },
    );
    let (sdk_io, peer_io) = duplex(4 * 1024);
    let mut transport = Framed::new(Box::new(sdk_io) as BoxedIo, FrameCodec::default());
    let mut peer = Framed::new(Box::new(peer_io) as BoxedIo, FrameCodec::default());
    let cancel = CancellationToken::new();

    drop(pipe);
    assert_eq!(abandoned_rx.try_recv()?, pipe_id);

    for frame in [
        Frame::Fin { pipe_id },
        Frame::Data {
            pipe_id,
            payload: bytes::Bytes::from_static(b"late"),
        },
    ] {
        let action = handle_relay_frame(
            frame,
            session_id,
            &mut state,
            &outbound,
            &abandoned,
            &inner,
            &mut SessionLink::new(&mut transport, inner.config.operation_timeout, &cancel),
        )
        .await;
        assert!(matches!(action, RelayFrameAction::Continue));
    }
    assert!(
        state.pipes.contains_key(&pipe_id),
        "entry must survive until the abandoned lane sends CLOSE"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(1), peer.next())
            .await
            .is_err(),
        "no RESET may be sent for DATA that raced a local close"
    );
    Ok(())
}
