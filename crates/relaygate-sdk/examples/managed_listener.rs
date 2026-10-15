//! Run: `cargo run -p relaygate-sdk --example managed_listener`
//!
//! Required: RELAYGATE_ADDR, RELAYGATE_DESTINATION, RELAYGATE_PUBLISH_TOKEN_FILE.
//! Optional: RELAYGATE_CA_FILE replaces public CA trust for a private deployment.
//! The application/backend atomically replaces the file with a fresh PUBLISH JWT
//! for this exact Destination. The SDK rereads it on initial publish and republish;
//! this adapter does not issue tokens. File limit: 4,096 bytes including an optional
//! final LF/CRLF. Invalid credentials stop this example; repair them before restart.
//!
//! Ctrl-C stops admission, drains active echoes for up to five seconds, then closes
//! the Relay. Pipe shutdown flushes the local writer; it is not a remote payload ACK.

use std::{error::Error as StdError, io, path::Path, time::Duration};

use relaygate_sdk::{
    AccessAction, AccessToken, AccessTokenRequest, AccessTokenSource, AccessTokenSourceFailure,
    Config, Destination, Error, ListenerStatus, Pipe, Relay, RelayStatus,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    task::{JoinError, JoinSet},
    time::timeout,
};

type ExampleResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;
const MAX_TOKEN_FILE_BYTES: usize = 4_096;
const MAX_ECHO_TASKS: usize = 32;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> ExampleResult {
    let destination: Destination = std::env::var("RELAYGATE_DESTINATION")?.parse()?;
    let token_path = std::env::var_os("RELAYGATE_PUBLISH_TOKEN_FILE")
        .ok_or("RELAYGATE_PUBLISH_TOKEN_FILE is required")?;
    let mut config = Config::new(std::env::var("RELAYGATE_ADDR")?)?;
    if let Some(path) = std::env::var_os("RELAYGATE_CA_FILE") {
        config = config.with_ca_certificate(&tokio::fs::read(path).await?)?;
    }
    let token_destination = destination.clone();
    let source = AccessTokenSource::dynamic_with_errors(move |request| {
        let path = token_path.clone();
        let destination = token_destination.clone();
        async move { publish_token(Path::new(&path), &destination, request).await }
    });
    let relay = Relay::connect(config).await?;
    let listener = relay.listen(destination, source).await?;
    let mut relay_status = relay.subscribe_status();
    let mut listener_status = listener.subscribe_status();
    eprintln!(
        "Relay: {:?}; Listener: {:?}",
        relay_status.current(),
        listener_status.current()
    );

    let mut tasks = JoinSet::new();
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    let result: ExampleResult = loop {
        tokio::select! {
            signal = &mut interrupt => break signal.map_err(Into::into),
            status = relay_status.changed() => {
                eprintln!("Relay: {status:?}");
                if let Some(error) = relay.last_error() {
                    report_sdk_error(&error);
                }
                if matches!(status, Some(RelayStatus::Closed) | None) {
                    break Err("Relay closed".into());
                }
            }
            status = listener_status.changed() => {
                eprintln!("Listener: {status:?}");
                if let Some(error) = listener.last_error() {
                    report_sdk_error(&error);
                }
                if matches!(status, Some(ListenerStatus::Blocked | ListenerStatus::Closed) | None) {
                    break Err("Listener stopped; repair its registration inputs before restarting".into());
                }
            }
            accepted = listener.accept(), if tasks.len() < MAX_ECHO_TASKS => {
                match accepted {
                    Ok(pipe) => { tasks.spawn(echo(pipe)); }
                    Err(error) => {
                        report_sdk_error(&error);
                        break Err(error.into());
                    }
                }
            }
            Some(completed) = tasks.join_next(), if !tasks.is_empty() => report_echo(completed),
        }
    };

    let close_result = listener.close().await;
    if timeout(DRAIN_TIMEOUT, async {
        while let Some(completed) = tasks.join_next().await {
            report_echo(completed);
        }
    })
    .await
    .is_err()
    {
        eprintln!("Echo drain deadline exceeded; cancelling remaining tasks");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    relay.close();
    close_result?;
    result
}

async fn publish_token(
    path: &Path,
    destination: &Destination,
    request: AccessTokenRequest,
) -> Result<AccessToken, AccessTokenSourceFailure> {
    if request.action != AccessAction::Publish || request.destination != *destination {
        return Err(AccessTokenSourceFailure::PermissionDenied);
    }
    let file = File::open(path).await.map_err(token_io_failure)?;
    let mut bytes = Vec::with_capacity(MAX_TOKEN_FILE_BYTES + 1);
    file.take((MAX_TOKEN_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(token_io_failure)?;
    if bytes.len() > MAX_TOKEN_FILE_BYTES {
        return Err(AccessTokenSourceFailure::Unauthenticated);
    }
    let text =
        std::str::from_utf8(&bytes).map_err(|_| AccessTokenSourceFailure::Unauthenticated)?;
    let token = text
        .strip_suffix("\r\n")
        .or_else(|| text.strip_suffix('\n'))
        .unwrap_or(text);
    if token.chars().any(char::is_whitespace) {
        return Err(AccessTokenSourceFailure::Unauthenticated);
    }
    AccessToken::new(token).map_err(|_| AccessTokenSourceFailure::Unauthenticated)
}

fn token_io_failure(error: io::Error) -> AccessTokenSourceFailure {
    match error.kind() {
        io::ErrorKind::PermissionDenied => AccessTokenSourceFailure::PermissionDenied,
        _ => AccessTokenSourceFailure::Unavailable,
    }
}

async fn echo(mut pipe: Pipe) -> io::Result<()> {
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let count = pipe.read(&mut buffer).await?;
        if count == 0 {
            return pipe.shutdown().await;
        }
        pipe.write_all(&buffer[..count]).await?;
    }
}

fn report_sdk_error(error: &Error) {
    eprintln!("SDK error: {:?}/{:?}", error.origin(), error.code());
}

fn report_echo(completed: Result<io::Result<()>, JoinError>) {
    match completed {
        Ok(Ok(())) => {}
        Ok(Err(error)) => match Error::from_io(&error) {
            Some(error) => report_sdk_error(error),
            None => eprintln!("Echo I/O error: {:?}", error.kind()),
        },
        Err(error) => eprintln!("Echo task stopped: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    struct TokenDirectory(PathBuf);

    impl TokenDirectory {
        fn new() -> ExampleResult<Self> {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "relaygate-token-{}-{nonce}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for TokenDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn request(destination: &Destination) -> AccessTokenRequest {
        AccessTokenRequest {
            action: AccessAction::Publish,
            destination: destination.clone(),
        }
    }

    #[tokio::test]
    async fn token_file_is_reread_after_atomic_replacement() -> ExampleResult {
        let directory = TokenDirectory::new()?;
        let path = directory.0.join("token");
        let destination: Destination = "test/echo".parse()?;
        tokio::fs::write(&path, "first-token\n").await?;
        assert_eq!(
            publish_token(&path, &destination, request(&destination)).await?,
            AccessToken::new("first-token")?
        );
        let replacement = directory.0.join("replacement");
        tokio::fs::write(&replacement, "second-token\r\n").await?;
        tokio::fs::rename(replacement, &path).await?;
        assert_eq!(
            publish_token(&path, &destination, request(&destination)).await?,
            AccessToken::new("second-token")?
        );
        Ok(())
    }

    #[tokio::test]
    async fn token_source_checks_scope_before_reading_and_classifies_missing_file() -> ExampleResult
    {
        let directory = TokenDirectory::new()?;
        let path = directory.0.join("missing");
        let destination: Destination = "test/echo".parse()?;
        let mut wrong_action = request(&destination);
        wrong_action.action = AccessAction::Dial;
        for wrong in [wrong_action, request(&"test/other".parse()?)] {
            assert_eq!(
                publish_token(&path, &destination, wrong).await,
                Err(AccessTokenSourceFailure::PermissionDenied)
            );
        }
        assert_eq!(
            publish_token(&path, &destination, request(&destination)).await,
            Err(AccessTokenSourceFailure::Unavailable)
        );
        assert_eq!(
            token_io_failure(io::ErrorKind::PermissionDenied.into()),
            AccessTokenSourceFailure::PermissionDenied
        );
        Ok(())
    }

    #[tokio::test]
    async fn token_file_enforces_size_encoding_and_single_line() -> ExampleResult {
        let directory = TokenDirectory::new()?;
        let path = directory.0.join("token");
        let destination: Destination = "test/echo".parse()?;
        let maximum = "a".repeat(MAX_TOKEN_FILE_BYTES);
        tokio::fs::write(&path, &maximum).await?;
        assert_eq!(
            publish_token(&path, &destination, request(&destination)).await?,
            AccessToken::new(maximum)?
        );
        for invalid in [
            Vec::new(),
            vec![b'a'; MAX_TOKEN_FILE_BYTES + 1],
            vec![0xff],
            b"token\nextra".to_vec(),
            b"token\n\n".to_vec(),
            b"\r\n".to_vec(),
        ] {
            tokio::fs::write(&path, invalid).await?;
            assert_eq!(
                publish_token(&path, &destination, request(&destination)).await,
                Err(AccessTokenSourceFailure::Unauthenticated)
            );
        }
        Ok(())
    }
}
