# relaygate-sdk

Public Rust SDK for RelayGate applications.

This crate owns the application-facing `Relay`, `Listener`, `Pipe`,
configuration, resource-limit and status-observation APIs. It also owns managed
reconnect and Listener republish behavior. Gateway state types and raw protocol
watch channels are intentionally not exposed as the public SDK contract.

Use this crate when an application needs to publish a destination with a
`Listener` or dial a destination to obtain a byte-stream `Pipe`.

## Transport and operation tokens

A bare `host:port` or `tls://host:port` uses public CA trust and verifies the
endpoint's DNS/IP identity. A private deployment can replace public CA trust
with a private CA via `Config::with_ca_certificate`. `tcp://host:port`
explicitly selects plaintext; it does not encrypt access tokens or Pipe data,
and a TLS failure never falls back to plaintext.

Every `listen` and `dial` supplies an application-issued operation token.
RelayGate does not issue, refresh, or persist these credentials. Production
applications should use `AccessTokenSource::dynamic` to fetch short-lived token
material from their own backend. The helper below uses an environment variable
only as a compileable stand-in for that backend boundary; do not hard-code raw
tokens in source.

```no_run
use relaygate_sdk::{
    AccessToken, AccessTokenRequest, AccessTokenSource, AccessTokenSourceError,
};

async fn token_from_application_backend(
    _request: AccessTokenRequest,
) -> Result<AccessToken, AccessTokenSourceError> {
    // Replace this stand-in with an authenticated call to your application backend.
    let raw = std::env::var("RELAYGATE_ACCESS_TOKEN")
        .map_err(|_| AccessTokenSourceError)?;
    AccessToken::new(raw).map_err(|_| AccessTokenSourceError)
}

let tokens = AccessTokenSource::dynamic(token_from_application_backend);
# let _ = tokens;
```

## Handling errors

Use `Error::code()` and `Error::origin()` for application decisions.
Origin identifies the boundary observed by the SDK, not the ultimate root cause. `message()`
is diagnostic text, not a value to match. `PeerObservation` remains separate;
`is_retryable()` applies only to a new control operation, never Pipe payloads.

| Origin and code | Application response |
| --- | --- |
| `TokenSource` + `Unavailable`/`DeadlineExceeded` | Restore the application token provider; a returned Listener retries publication. Close it if application policy permanently revoked the grant. |
| `Gateway` + `Unauthenticated` | Issue a valid operation JWT; check profile, key, claims, and expiry. |
| `Gateway` + `PermissionDenied` | Check the token's action, Namespace, Destination scope, and permission count. |
| `Gateway` + `Internal` | Check Gateway diagnostics; internal RouteTable/peer admission authentication failures cannot be repaired by refreshing an application JWT. |
| `Transport` + `Unavailable`/`DeadlineExceeded` | Check Gateway reachability and TLS configuration; the Relay reconnects after an established session ends. |

Gateway authorization failures use SDK-controlled diagnostic messages that also
allow for older Gateways forwarding dependency failures. Raw JWTs
and untrusted Gateway authorization text are not included in those messages.

| Failure | Classification |
| --- | --- |
| Token supply deadline, for both publish and dial | `TokenSource` + `DeadlineExceeded` |
| Waiting for a Relay session | `Transport` + `DeadlineExceeded` |
| Runtime frame/order violation | `Transport` + `ProtocolError` |
| Heartbeat or frame-write deadline | `Transport` + `DeadlineExceeded` |
| TCP EOF or frame I/O failure | `Transport` + `Unavailable` |

An initial `SessionRejected` has `NotObserved` metadata because no session was
admitted. `Unavailable` and `ResourceExhausted` permit a new connection attempt
after backoff. A committed DIAL remains `MaybeObserved` when its result is lost.

## Publish and accept Pipes

`Relay::listen` waits for the initial Gateway-local binding. A returned
`Listener` is active and remains desired while the SDK reconnects. Status
subscriptions coalesce changes, so observers receive the latest state rather
than an audit log of every transition. `last_error()` returns the current
registration failure or the latest Relay session/reconnect failure. The session
cause is recorded before `Reconnecting`; subsequent failed attempts replace it.
Relay recovery and close clear its error. A Listener clears its error when a new
PUBLISH is committed, registration becomes active, or it is closed.
Relay status changes do not notify observers for every failed reconnect attempt,
so read `Relay::last_error()` when inspecting a reconnecting Relay.

```no_run
use relaygate_sdk::{
    AccessToken, AccessTokenRequest, AccessTokenSource, AccessTokenSourceError,
    Config, Destination, ListenerStatus, Relay, RelayStatus,
};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

# async fn token_from_application_backend(
#     _request: AccessTokenRequest,
# ) -> Result<AccessToken, AccessTokenSourceError> {
#     let raw = std::env::var("RELAYGATE_ACCESS_TOKEN")
#         .map_err(|_| AccessTokenSourceError)?;
#     AccessToken::new(raw).map_err(|_| AccessTokenSourceError)
# }
# async fn provider() -> Result<(), Box<dyn std::error::Error>> {
let relay = Relay::connect(Config::new("relaygate.example.com:443")?).await?;
let mut relay_status = relay.subscribe_status();
assert_eq!(relay_status.current(), RelayStatus::Active);
let relay_observer = tokio::spawn(async move {
    while let Some(status) = relay_status.changed().await {
        eprintln!("Relay status: {status:?}");
        if status == RelayStatus::Closed {
            break;
        }
    }
});

let destination: Destination = "inference/stt.seoul".parse()?;
let tokens = AccessTokenSource::dynamic(token_from_application_backend);
let listener = Arc::new(relay.listen(destination, tokens).await?);
let observed_listener = Arc::clone(&listener);
let mut listener_status = listener.subscribe_status();
assert_eq!(listener_status.current(), ListenerStatus::Active);
let listener_observer = tokio::spawn(async move {
    while let Some(status) = listener_status.changed().await {
        eprintln!("Listener status: {status:?}");
        if matches!(status, ListenerStatus::Suspended | ListenerStatus::Blocked) {
            if let Some(error) = observed_listener.last_error() {
                eprintln!("Listener registration error: {:?}/{:?}", error.origin(), error.code());
            }
        }
        if status == ListenerStatus::Closed {
            break;
        }
    }
});

let mut pipe = listener.accept().await?;
let mut request = [0_u8; 4096];
let received = pipe.read(&mut request).await?;
pipe.write_all(&request[..received]).await?;
pipe.shutdown_write().await?;

listener.close().await?;
relay.close();
let _ = listener_observer.await;
let _ = relay_observer.await;
# Ok(())
# }
```

## Dial a destination

`Relay::dial` opens one new opaque byte-stream `Pipe`. A committed dial and
Pipe payloads are never replayed by the SDK. After a session interruption,
`dial` waits for a new session within its operation deadline. `wait_ready`
can be used separately when an application needs transport readiness; it does
not wait for Listener registration and has no built-in deadline.

```no_run
use relaygate_sdk::{
    AccessToken, AccessTokenRequest, AccessTokenSource, AccessTokenSourceError,
    Config, Destination, Error, Relay, RelayStatus,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

# async fn token_from_application_backend(
#     _request: AccessTokenRequest,
# ) -> Result<AccessToken, AccessTokenSourceError> {
#     let raw = std::env::var("RELAYGATE_ACCESS_TOKEN")
#         .map_err(|_| AccessTokenSourceError)?;
#     AccessToken::new(raw).map_err(|_| AccessTokenSourceError)
# }
# async fn dialer() -> Result<(), Box<dyn std::error::Error>> {
let relay = Relay::connect(Config::new("relaygate.example.com:443")?).await?;
let mut statuses = relay.subscribe_status();
assert_eq!(statuses.current(), RelayStatus::Active);
let observer = tokio::spawn(async move {
    while let Some(status) = statuses.changed().await {
        eprintln!("Relay status: {status:?}");
        if status == RelayStatus::Closed {
            break;
        }
    }
});

let destination: Destination = "inference/stt.seoul".parse()?;
let tokens = AccessTokenSource::dynamic(token_from_application_backend);
let mut pipe = relay.dial(destination, tokens).await?;
pipe.write_all(b"transcribe this audio").await?;
pipe.shutdown_write().await?;

let mut response = [0_u8; 4096];
loop {
    match pipe.read(&mut response).await {
        Ok(0) => break,
        Ok(received) => {
            // Process `&response[..received]` according to the application protocol.
        }
        Err(error) => {
            // `Pipe` implements Tokio's `AsyncRead`/`AsyncWrite`; the structured SDK
            // error stays recoverable from the `std::io::Error` payload.
            if let Some(sdk_error) = Error::from_io(&error) {
                eprintln!("dial pipe failed: {:?}", sdk_error.code());
            }
            return Err(error.into());
        }
    }
}
relay.close();
let _ = observer.await;
# Ok(())
# }
```

`RelayStatusSubscription::current` and
`ListenerStatusSubscription::current` consume the current watch version. The
next `changed` call therefore waits for a newer state. A `None` result from
`changed` means the owning Relay or Listener runtime has terminated.

## License

Licensed under the Apache License, Version 2.0. The package includes the
workspace `LICENSE` file through Cargo's `license-file` metadata, so the full
license text is present inside the packaged crate archive.
