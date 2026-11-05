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

Branch on `Error::code()` and `Error::origin()`, not message text:

| TLS failure (`ErrorOrigin::Transport`) | Code | Initial connect |
| --- | --- | --- |
| Certificate trust, name, validity, or peer certificate rejection | `Unauthenticated` | Return immediately |
| TLS protocol or `relaygate/3` ALPN mismatch | `ProtocolError` | Return immediately |
| Network I/O or unclassified TLS failure | `Unavailable` | Retry within connect timeout |

An existing Relay keeps reconnecting with backoff and exposes the latest failure
through `last_error()`. A repaired server certificate can restore the session and
Listeners under the existing trust. Changing SDK trust or client identity requires
a new Config and Relay; certificate files are not automatically reloaded.

Every `listen` and `dial` supplies an application-issued operation token. Create an
`AccessTokenSource` with `AccessTokenSource::dynamic_with_errors(your_async_provider)`;
`your_async_provider` is application code, not an SDK function.

| Provider boundary | Contract |
| --- | --- |
| Input | `AccessTokenRequest` containing the action and exact Destination. |
| Output | `Result<AccessToken, AccessTokenSourceFailure>`. |
| Invocation | Initial operations and Listener republish; distinct operations may call concurrently. |
| Ownership | The application/backend authenticates callers, authorizes grants, issues and refreshes tokens, and chooses any cache policy. |

The [managed Listener example](https://github.com/project-jelly/relaygate/blob/main/crates/relaygate-sdk/examples/managed_listener.rs)
uses a provider that rereads a token file on each request. The application/backend
atomically replaces that file; the SDK does not issue or refresh JWTs.

The backend adapter classifies failures by their meaning:

| `AccessTokenSourceFailure` | Provider condition | SDK behavior |
| --- | --- | --- |
| `Unavailable` | Temporary network/backend failure or rate limit | Retry with backoff |
| `Unauthenticated` | Application authentication must be restored | Return error / block returned Listener |
| `PermissionDenied` | Permission to obtain the operation token was denied | Return error / block returned Listener |

Existing `AccessTokenSource::dynamic` callbacks remain supported. Their
`AccessTokenSourceError` always means a transient failure; use
`dynamic_with_errors` to stop retrying authentication or permission failures.

Map provider failures by meaning, not diagnostic text. Provider response bodies
and credentials are not included in SDK errors. After repairing a permanent
failure, close the blocked Listener and call `listen` with a replacement source.
Sibling Listeners and the Relay remain usable.

## Automatic recovery

| Boundary | SDK behavior |
| --- | --- |
| Initial connection | Retry transient failures with backoff within one `connect_timeout`. |
| Initial listen/dial token supply | Retry transient supplier failures within the original `operation_timeout`. |
| Established session loss | Reconnect and republish returned Listeners using the existing managed backoff. |
| Initial PUBLISH/DIAL result | Return the result; do not resend the wire operation automatically. |
| Returned Listener republish | Retry transient supply/registration failures with the shared timer; permanent failures become `Blocked`. |
| Gateway authentication/authorization rejection or uncertain operation | Return the error; do not replay. |
| Established Pipe | Fail on session loss; never migrate or resend payloads. |

`with_reconnect_backoff` also controls initial connection and token-supply retry
spacing. The original deadline includes every attempt and wait. Dropping the
call stops its retries; closing the Relay interrupts token supply and backoff.
A provider may be called more than once after failures. One successful token
supply is reused by the same dial call.

## Handling errors

Use `Error::code()` and `Error::origin()` for application decisions.
Origin identifies the boundary observed by the SDK, not the ultimate root cause. `message()`
is diagnostic text, not a value to match. `PeerObservation` remains separate;
`is_retryable()` applies only to a new control operation, never Pipe payloads.

| Origin and code | Application response |
| --- | --- |
| `TokenSource` + `Unavailable`/`DeadlineExceeded` | Restore the application token provider; a returned Listener retries publication. |
| `TokenSource` + `Unauthenticated` | Restore application authentication; the SDK does not retry. A returned Listener becomes `Blocked`. |
| `TokenSource` + `PermissionDenied` | Restore permission for the requested action and Destination; the SDK does not retry. A returned Listener becomes `Blocked`. |
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

For a blocked Listener, read its error before closing it. Restore application
access first, then replace only that Listener:

```no_run
use relaygate_sdk::{AccessTokenSource, ErrorCode, ErrorOrigin, Listener, Relay};

# async fn replace_blocked(
#     relay: &Relay, blocked: &Listener, destination: relaygate_sdk::Destination,
#     replacement_source: AccessTokenSource,
# ) -> Result<Listener, Box<dyn std::error::Error>> {
let error = blocked.last_error().ok_or("no registration error")?;
match (error.origin(), error.code()) {
    (ErrorOrigin::TokenSource, ErrorCode::Unauthenticated) => {
        // Restore application authentication before providing replacement_source.
    }
    (ErrorOrigin::TokenSource, ErrorCode::PermissionDenied) => {
        // Restore permission for this action and Destination first.
    }
    _ => return Err(error.into()),
}
blocked.close().await?;
let replacement = relay.listen(destination, replacement_source).await?;
# Ok(replacement)
# }
```

## Pipe I/O completion

| Operation | Successful completion |
| --- | --- |
| `write` / `write_all` | DATA accepted by the bounded session queue. |
| `flush` | Earlier DATA from this Pipe flushed by the local transport writer. |
| `shutdown` / `shutdown_write` | Earlier DATA and one FIN flushed by the local transport writer; the read half stays open. |

These operations do not acknowledge remote application receipt or processing.
`Relay::close` stops the runtime immediately without draining queued data. Await
`flush` or write shutdown first when local write completion is required; use an
application response or acknowledgement when remote completion is required.

`Pipe::reset` and `PipeWriteHalf::reset` accept messages up to 65,535 UTF-8 bytes.
Longer messages return `Sdk` / `InvalidArgument` before changing an open Pipe or
queuing a frame. Resetting an already terminal Pipe remains a successful no-op.

## Readiness and cancellation

| Observation | Meaning |
| --- | --- |
| `Relay::connect` / `Relay::wait_ready` success | The current transport session is ready; Listener republish may still be pending. |
| `Relay::listen` success / Listener `Active` | The current Gateway-local binding is ready. |
| `status()` / subscription `current()` | A snapshot; a disconnect can happen immediately afterward. |
| subscription `changed()` | A newer, coalesced snapshot; intermediate transitions may be skipped. |

`current()` consumes the current subscription version, so the next `changed()`
waits for a newer one. `wait_ready()` has no built-in deadline; wrap it in
`tokio::time::timeout` when needed. `accept()` waits through transient recovery;
a blocked or closed Listener returns an error.

`last_error()` is the latest failure, not a history. Relay errors clear on recovery
or close; Listener errors clear on a new PUBLISH commit, successful registration,
or close. Status notifications do not report every failed reconnect attempt.

| Cancellation or close | Application responsibility |
| --- | --- |
| Drop a connect/listen/dial future, including through `select!` or timeout | Stops local waiting and retries; a committed operation may already have been observed remotely. |
| Interrupt `write_all` | Some bytes may already be accepted; do not retry the entire payload automatically. |
| `Listener::close` | Stops new and unaccepted Pipes; already accepted Pipes remain usable. |
| Drain then `Relay::close` | Stop accepting, allow active Pipe tasks to finish within an application deadline, then stop the runtime. `Relay::close` itself does not drain. |

## Publish and accept Pipes

The application supplies `tokens` from its provider. This example echoes one Pipe;
the managed Listener example adds status observation, Ctrl-C, and bounded task shutdown.

```no_run
use relaygate_sdk::{AccessTokenSource, Config, Destination, Relay};
use tokio::io::{AsyncWriteExt, copy};

async fn serve_one(
    config: Config,
    destination: Destination,
    tokens: AccessTokenSource,
) -> Result<(), Box<dyn std::error::Error>> {
    let relay = Relay::connect(config).await?;
    let listener = relay.listen(destination, tokens).await?;
    eprintln!("Relay: {:?}, Listener: {:?}", relay.status(), listener.status());

    let (mut reader, mut writer) = listener.accept().await?.into_split();
    copy(&mut reader, &mut writer).await?;
    writer.shutdown().await?;
    listener.close().await?;
    relay.close();
    Ok(())
}
```

From a repository checkout, run the complete example with a backend-issued
publish token for `RELAYGATE_DESTINATION`:

```sh
RELAYGATE_ADDR=relaygate.example.com:443 \
RELAYGATE_DESTINATION=examples/echo \
RELAYGATE_PUBLISH_TOKEN_FILE=/run/secrets/publish.jwt \
cargo run -p relaygate-sdk --example managed_listener
```

Set `RELAYGATE_CA_FILE` for a private CA. The existing Compose echo examples use
static tokens as topology fixtures; a static source does not refresh an expired token.

## Dial a destination

`dial` waits for a session within its operation deadline and opens one new Pipe.
Committed operations and Pipe payloads are never replayed by the SDK. This example
uses write shutdown to delimit a request, then reads the application's response.

```no_run
use relaygate_sdk::{AccessTokenSource, Config, Destination, Error, Relay};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn dial_once(
    config: Config,
    destination: Destination,
    tokens: AccessTokenSource,
) -> Result<(), Box<dyn std::error::Error>> {
    let relay = Relay::connect(config).await?;
    let mut pipe = relay.dial(destination, tokens).await?;
    pipe.write_all(b"transcribe this audio").await?;
    pipe.shutdown_write().await?;

    let mut response = [0_u8; 4096];
    loop {
        match pipe.read(&mut response).await {
            Ok(0) => break,
            Ok(received) => {
                // Process `&response[..received]` using the application protocol.
            }
            Err(error) => {
                if let Some(sdk_error) = Error::from_io(&error) {
                    eprintln!("Pipe: {:?}/{:?}", sdk_error.origin(), sdk_error.code());
                }
                return Err(error.into());
            }
        }
    }
    relay.close();
    Ok(())
}
```

## License

Licensed under the Apache License, Version 2.0. The package includes the
workspace `LICENSE` file through Cargo's `license-file` metadata, so the full
license text is present inside the packaged crate archive.
