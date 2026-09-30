use std::time::Duration;

use relaygate_destination::{Destination, DestinationName, Namespace};
use relaygate_protocol::{BearerToken, FrameCodec, MAX_BEARER_TOKEN_BYTES};
use relaygate_sdk::{
    AccessAction, AccessTokenRequest, AccessTokenSource, AccessTokenSourceError,
    AccessTokenSourceFailure, Config, Error, ErrorOrigin, Listener, ListenerStatus, Relay,
    RelayStatus, ResourceLimits,
};
use relaygate_token_issuer::{Action, Permission, TokenIssuer};
use relaygate_transport::{ClientTlsConfig, ServerTlsConfig, TlsErrorKind};

pub fn public_api_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let namespace: Namespace = "example".parse()?;
    let name: DestinationName = "worker.primary".parse()?;
    let destination = Destination::new(namespace, name);

    let _permission = Permission::exact(Action::Dial, &destination);
    let _request = AccessTokenRequest {
        action: AccessAction::Dial,
        destination: destination.clone(),
    };
    let _source = AccessTokenSource::dynamic(|_| async { Err(AccessTokenSourceError) });
    let _classified_source = AccessTokenSource::dynamic_with_errors(|_| async {
        Err(AccessTokenSourceFailure::Unauthenticated)
    });
    let _: fn(&Error) -> ErrorOrigin = Error::origin;
    let io = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
    assert!(Error::from_io(&io).is_none());
    assert!(TlsErrorKind::from_io(&io).is_none());
    let _codec = FrameCodec::default();
    let _token_limit = MAX_BEARER_TOKEN_BYTES;
    let _token = BearerToken::new("fixture-token")?;
    let _relay_status = RelayStatus::Closed;
    let _limits = ResourceLimits::default();
    let _config = Config::new("relaygate.example.com:443")?;

    let _: fn(String) -> Result<ClientTlsConfig, relaygate_transport::TlsConfigError> =
        ClientTlsConfig::with_webpki_roots;
    let _: fn(&[u8], &[u8]) -> Result<ServerTlsConfig, relaygate_transport::TlsConfigError> =
        ServerTlsConfig::server_authenticated;

    Ok(())
}

#[allow(dead_code)]
pub async fn status_api_compile_only(
    relay: &Relay,
    listener: &Listener,
) -> relaygate_sdk::Result<()> {
    let _: RelayStatus = relay.status();
    let _: Option<Error> = relay.last_error();
    let mut relay_status = relay.subscribe_status();
    let _: RelayStatus = relay_status.current();
    let _: Option<RelayStatus> = relay_status.changed().await;
    relay.wait_ready().await?;

    let _: ListenerStatus = listener.status();
    let mut listener_status = listener.subscribe_status();
    let _: ListenerStatus = listener_status.current();
    let _: Option<ListenerStatus> = listener_status.changed().await;

    Ok(())
}

#[allow(dead_code)]
pub async fn application_flow_compile_only(
    config: Config,
    published_destination: Destination,
    dialed_destination: Destination,
    tokens: AccessTokenSource,
) -> relaygate_sdk::Result<()> {
    let relay = Relay::connect(config).await?;
    let listener = relay.listen(published_destination, tokens.clone()).await?;
    let mut pipe = relay.dial(dialed_destination, tokens).await?;

    pipe.close().await?;
    listener.close().await?;
    relay.close();
    Ok(())
}

#[allow(dead_code)]
pub fn application_token_backend_compile_only(
    private_key_pem: &[u8],
    action: Action,
    destination: &Destination,
) -> Result<String, relaygate_token_issuer::TokenIssuerError> {
    let issuer = TokenIssuer::from_es256_pem(
        "https://issuer.example.com",
        "relaygate",
        "current-key",
        private_key_pem,
    )?;
    Ok(issuer
        .issue_exact(action, destination, Duration::from_secs(60))?
        .into_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packaged_public_api_is_usable() -> Result<(), Box<dyn std::error::Error>> {
        public_api_smoke()
    }
}
