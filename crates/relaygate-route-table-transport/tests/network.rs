mod support;

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use relaygate_route_table::{Destination, LeaseId, RegistrationRevision};
use relaygate_route_table_transport::{
    ErrorCode, GatewayName, RouteTableClient, RouteTableClientConfig,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use uuid::Uuid;

use support::{
    RunningService, TestResult, binding, binding_snapshot, gateway, registration_key, session,
};

const PARSER_MARKER: &str = "RT_PARSER_MARKER_MUST_NOT_ESCAPE";
const PARSER_TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("test log buffer poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn write_raw_frame(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    let length = u32::try_from(payload.len()).map_err(io::Error::other)?;
    stream.write_all(b"RT\x03").await?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(payload).await
}

async fn read_raw_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut header = [0; 7];
    stream.read_exact(&mut header).await?;
    assert_eq!(&header[..3], b"RT\x03");
    let length = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    assert!(length <= 256 * 1024, "test peer sent an oversized frame");
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_hello_logs_redacted_error_and_closes_connection() -> TestResult {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::clone(&logs);
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || CapturedLogs(Arc::clone(&writer)))
        .finish();
    // The current-thread runtime keeps service tasks under this test's subscriber.
    let _subscriber = tracing::subscriber::set_default(subscriber);
    timeout(PARSER_TEST_TIMEOUT, async {
        let service = RunningService::start(Duration::from_secs(5)).await?;
        let result: TestResult<Vec<u8>> = async {
            let mut stream = TcpStream::connect(service.endpoint).await?;
            let payload = format!(r#"{{"frame":"{PARSER_MARKER}"}}"#);
            write_raw_frame(&mut stream, payload.as_bytes()).await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok(response)
        }
        .await;
        service.stop().await?;
        assert!(
            result?.is_empty(),
            "malformed HELLO must close without a response"
        );
        let output = String::from_utf8(
            logs.lock()
                .map_err(|_| io::Error::other("test log buffer poisoned"))?
                .clone(),
        )?;
        assert!(output.contains("route_table.handshake.failed"));
        assert!(output.contains("protocol_error"));
        assert!(
            !output.contains(PARSER_MARKER),
            "parser input leaked in DEBUG log: {output}"
        );
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_request_returns_redacted_protocol_fault() -> TestResult {
    timeout(PARSER_TEST_TIMEOUT, async {
        let service = RunningService::start(Duration::from_secs(5)).await?;
        let result: TestResult<(serde_json::Value, Vec<u8>)> = async {
            let mut stream = TcpStream::connect(service.endpoint).await?;
            write_raw_frame(&mut stream, br#"{"frame":"HELLO","role":"gateway","gateway_name":"gw-a","gateway_id":"00000000-0000-0000-0000-000000000001"}"#).await?;
            let welcome: serde_json::Value = serde_json::from_slice(&read_raw_frame(&mut stream).await?)?;
            assert_eq!(welcome["frame"], "WELCOME");
            let payload = format!(r#"{{"frame":"{PARSER_MARKER}"}}"#);
            write_raw_frame(&mut stream, payload.as_bytes()).await?;
            let fault = serde_json::from_slice(&read_raw_frame(&mut stream).await?)?;
            let mut trailing = Vec::new();
            stream.read_to_end(&mut trailing).await?;
            Ok((fault, trailing))
        }
        .await;
        service.stop().await?;
        let (fault, trailing) = result?;
        assert_eq!(fault["frame"], "PROTOCOL_FAULT");
        assert_eq!(fault["role"], "route_table");
        assert_eq!(fault["code"], "PROTOCOL_ERROR");
        assert!(!fault.to_string().contains(PARSER_MARKER), "parser input leaked in ProtocolFault: {fault}");
        assert_eq!(fault["message"], "invalid RouteTable frame payload");
        assert!(trailing.is_empty(), "ProtocolFault must be followed by EOF");
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_response_returns_redacted_client_transport_error() -> TestResult {
    timeout(PARSER_TEST_TIMEOUT, async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let server = async {
            let (mut stream, _) = listener.accept().await?;
            let hello: serde_json::Value =
                serde_json::from_slice(&read_raw_frame(&mut stream).await?)?;
            assert_eq!(hello["frame"], "HELLO");
            write_raw_frame(&mut stream, br#"{"frame":"WELCOME","role":"route_table"}"#).await?;
            let request: serde_json::Value =
                serde_json::from_slice(&read_raw_frame(&mut stream).await?)?;
            assert_eq!(request["frame"], "REQUEST");
            let payload = format!(r#"{{"frame":"{PARSER_MARKER}"}}"#);
            write_raw_frame(&mut stream, payload.as_bytes()).await?;
            let mut trailing = Vec::new();
            stream.read_to_end(&mut trailing).await?;
            assert!(
                trailing.is_empty(),
                "client must close after a malformed response"
            );
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let client = async {
            let second = Duration::from_secs(1);
            let client = RouteTableClient::connect(
                endpoint,
                GatewayName::new("gw-a")?,
                gateway(1),
                RouteTableClientConfig::new(4, 256 * 1024, second, second, second)?,
            )
            .await?;
            let generation = relaygate_route_table::ShardDirectory::from_json_bytes(
                support::ONE_SHARD_DIRECTORY,
            )?
            .generation();
            let destination = "test/parser-redaction".parse()?;
            let error = client
                .resolve(generation, &destination)
                .await
                .err()
                .ok_or("malformed response succeeded")?;
            let next = client
                .resolve(generation, &destination)
                .await
                .err()
                .ok_or("failed connection remained usable")?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((error, next))
        };
        let ((), (error, next)) = tokio::try_join!(server, client)?;
        assert_eq!(error.code(), ErrorCode::ProtocolError);
        assert!(
            !format!("{error} {error:?}").contains(PARSER_MARKER),
            "parser input leaked in client error: {error}"
        );
        assert_eq!(error.message(), "invalid RouteTable frame payload");
        assert_eq!(next.code(), ErrorCode::Unavailable);
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn full_registration_lifecycle_and_ready_empty_not_found() -> TestResult {
    let service = RunningService::start(Duration::from_secs(5)).await?;
    let gateway_id = gateway(1);
    let relay_session_id = session(11);
    let key = registration_key(gateway_id, relay_session_id)?;
    let client = service.connect("gw-a", gateway_id).await?;
    let destination = "test/11111111-1111-4111-8111-111111111111".parse::<Destination>()?;

    let empty = client.resolve(service.generation, &destination).await.err();
    assert_eq!(empty.map(|error| error.code()), Some(ErrorCode::NotFound));

    let registered = client.register(service.generation, &key).await?;
    assert_eq!(registered.accepted_revision(), None);
    assert!(registered.expires_in() > Duration::ZERO);

    let snapshot = binding_snapshot(
        &destination.to_string(),
        gateway_id,
        relay_session_id,
        binding(111),
    )?;
    let updated = client
        .update(
            service.generation,
            &key,
            registered.lease_id(),
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await?;
    assert_eq!(
        updated.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );

    let bindings = client.resolve(service.generation, &destination).await?;
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings.entries()[0].destination(), &destination);

    let kept_alive = client
        .keep_alive(service.generation, &key, registered.lease_id())
        .await?;
    assert_eq!(
        kept_alive.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );

    client
        .deregister(service.generation, &key, registered.lease_id())
        .await?;
    let removed = client.resolve(service.generation, &destination).await.err();
    assert_eq!(removed.map(|error| error.code()), Some(ErrorCode::NotFound));

    service.stop().await
}

#[tokio::test]
async fn plaintext_admits_unlisted_gateways_but_owner_mismatch_creates_no_state() -> TestResult {
    let service = RunningService::start(Duration::from_secs(5)).await?;

    let unlisted = service.connect("gw-unlisted", gateway(9)).await?;
    drop(unlisted);

    let client_a = service.connect("gw-a", gateway(1)).await?;
    let key_b = registration_key(gateway(2), session(22))?;
    let denied = client_a.register(service.generation, &key_b).await.err();
    assert_eq!(
        denied.map(|error| error.code()),
        Some(ErrorCode::PermissionDenied)
    );

    let client_b = service.connect("gw-b", gateway(2)).await?;
    client_b
        .deregister(
            service.generation,
            &key_b,
            LeaseId::from_uuid(Uuid::from_u128(999)),
        )
        .await?;

    service.stop().await
}

#[tokio::test]
async fn service_loss_is_reported_as_unavailable_without_reconnect() -> TestResult {
    let service = RunningService::start(Duration::from_secs(5)).await?;
    let generation = service.generation;
    let client = service.connect("gw-a", gateway(1)).await?;
    service.stop().await?;

    let error = client
        .resolve(
            generation,
            &"test/11111111-1111-4111-8111-111111111111".parse::<Destination>()?,
        )
        .await
        .err();
    assert_eq!(
        error.map(|error| error.code()),
        Some(ErrorCode::Unavailable)
    );
    Ok(())
}

#[tokio::test]
async fn restart_starts_empty_and_recovers_only_from_a_new_lease_snapshot() -> TestResult {
    let gateway_id = gateway(1);
    let relay_session_id = session(11);
    let key = registration_key(gateway_id, relay_session_id)?;
    let destination = "test/22222222-2222-4222-8222-222222222222".parse::<Destination>()?;
    let snapshot = binding_snapshot(
        &destination.to_string(),
        gateway_id,
        relay_session_id,
        binding(111),
    )?;

    let service_a = RunningService::start(Duration::from_secs(5)).await?;
    let generation = service_a.generation;
    let client_a = service_a.connect("gw-a", gateway_id).await?;
    let registered = client_a.register(generation, &key).await?;
    let old_lease = registered.lease_id();
    client_a
        .update(
            generation,
            &key,
            old_lease,
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await?;
    assert_eq!(client_a.resolve(generation, &destination).await?.len(), 1);
    service_a.stop().await?;

    let service_b = RunningService::start(Duration::from_secs(5)).await?;
    assert_eq!(service_b.generation, generation);
    let client_b = service_b.connect("gw-a", gateway_id).await?;
    let empty = client_b.resolve(generation, &destination).await.err();
    assert_eq!(empty.map(|error| error.code()), Some(ErrorCode::NotFound));

    let stale_keep_alive = client_b.keep_alive(generation, &key, old_lease).await.err();
    assert_eq!(
        stale_keep_alive.map(|error| error.code()),
        Some(ErrorCode::FailedPrecondition)
    );
    let stale_update = client_b
        .update(
            generation,
            &key,
            old_lease,
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await
        .err();
    assert_eq!(
        stale_update.map(|error| error.code()),
        Some(ErrorCode::FailedPrecondition)
    );

    let fresh = client_b.register(generation, &key).await?;
    assert_ne!(fresh.lease_id(), old_lease);
    client_b
        .update(
            generation,
            &key,
            fresh.lease_id(),
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await?;
    let restored = client_b.resolve(generation, &destination).await?;
    assert_eq!(restored.len(), 1);
    assert_eq!(restored.entries()[0].destination(), &destination);
    service_b.stop().await
}

#[tokio::test]
async fn connection_limit_rejects_the_next_handshake_as_resource_exhausted() -> TestResult {
    let service = RunningService::start_with_max_connections(Duration::from_secs(5), 1).await?;
    let first = service.connect("gw-a", gateway(1)).await?;

    let rejected = service.connect("gw-a", gateway(2)).await.err();
    assert_eq!(
        rejected.map(|error| error.code()),
        Some(ErrorCode::ResourceExhausted)
    );

    drop(first);
    service.stop().await
}

#[tokio::test]
async fn shutdown_completes_with_an_unread_handshake_stalled_connection() -> TestResult {
    let service = RunningService::start_with_max_connections(Duration::from_secs(5), 1).await?;
    let _stalled = TcpStream::connect(service.endpoint).await?;

    let rejected = service.connect("gw-a", gateway(2)).await.err();
    assert_eq!(
        rejected.map(|error| error.code()),
        Some(ErrorCode::ResourceExhausted)
    );
    tokio::time::timeout(Duration::from_secs(1), service.stop())
        .await
        .map_err(|_| std::io::Error::other("RouteTable shutdown timed out"))??;
    Ok(())
}

#[tokio::test]
async fn oversized_binding_set_returns_resource_exhausted_and_connection_stays_usable() -> TestResult
{
    let service = RunningService::start_with_limits(Duration::from_secs(30), 8, 1024).await?;
    let gateway_id = gateway(1);
    let client = service.connect("gw-a", gateway_id).await?;
    let generation = service.generation;
    let large_destination = "test/33333333-3333-4333-8333-333333333333".parse::<Destination>()?;

    for index in 0_u128..16 {
        let relay_session_id = session(1_000 + index);
        let key = registration_key(gateway_id, relay_session_id)?;
        let registered = client.register(generation, &key).await?;
        let snapshot = binding_snapshot(
            &large_destination.to_string(),
            gateway_id,
            relay_session_id,
            binding(2_000 + index),
        )?;
        client
            .update(
                generation,
                &key,
                registered.lease_id(),
                RegistrationRevision::FIRST,
                &snapshot,
            )
            .await?;
    }

    let small_destination = "test/44444444-4444-4444-8444-444444444444".parse::<Destination>()?;
    let small_session = session(9_000);
    let small_key = registration_key(gateway_id, small_session)?;
    let small_registration = client.register(generation, &small_key).await?;
    let small_snapshot = binding_snapshot(
        &small_destination.to_string(),
        gateway_id,
        small_session,
        binding(9_001),
    )?;
    client
        .update(
            generation,
            &small_key,
            small_registration.lease_id(),
            RegistrationRevision::FIRST,
            &small_snapshot,
        )
        .await?;

    let oversized = client.resolve(generation, &large_destination).await.err();
    assert_eq!(
        oversized.map(|error| error.code()),
        Some(ErrorCode::ResourceExhausted)
    );
    let small = client.resolve(generation, &small_destination).await?;
    assert_eq!(small.len(), 1);
    assert_eq!(small.entries()[0].destination(), &small_destination);

    service.stop().await
}
