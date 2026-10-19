use std::{env, fs, time::Duration};

use anyhow::{Context, Result};
use relaygate_gateway::{GatewayConfig, GatewayPeerConfig, GatewayRoutingConfig};
use relaygate_route_table::{GatewayLocator, ShardDirectory};
use relaygate_route_table_transport::{GatewayName, RouteTableClientConfig};
use relaygate_transport::{ClientTlsConfig, ServerTlsConfig};

use super::{
    InternalTransport, internal_transport, load_internal_tls, optional_duration_millis,
    optional_env, optional_usize,
};

const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:27420";
const DEFAULT_PEER_BIND_ADDRESS: &str = "0.0.0.0:27421";
const DEFAULT_RT_CLIENT_QUEUE_CAPACITY: usize = 128;
const DEFAULT_RT_MAX_FRAME_LEN: usize = 1024 * 1024;
const DEFAULT_RT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_RT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_RT_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

const DISTRIBUTED_ENVIRONMENT: [&str; 4] = [
    "RELAYGATE_RT_SHARD_DIRECTORY_PATH",
    "RELAYGATE_GATEWAY_NAME",
    "RELAYGATE_GATEWAY_LOCATOR",
    "RELAYGATE_PEER_BIND_ADDR",
];

pub(crate) struct DistributedGatewayConfig {
    pub(crate) peer_bind_address: String,
    pub(crate) routing: GatewayRoutingConfig,
    pub(crate) peer: GatewayPeerConfig,
    pub(crate) insecure_transport: bool,
}

pub(crate) struct GatewayRuntimeConfig {
    pub(crate) bind_address: String,
    pub(crate) gateway: GatewayConfig,
    pub(crate) distributed: Option<DistributedGatewayConfig>,
    pub(crate) stats_interval: Option<Duration>,
}

impl GatewayRuntimeConfig {
    pub(crate) fn from_env() -> Result<Self> {
        super::transport::reject_removed_flags()?;
        // Validated up front so a bad mode fails local-only boots too.
        let transport = internal_transport()?;
        let bind_address =
            optional_env("RELAYGATE_BIND_ADDR")?.unwrap_or_else(|| DEFAULT_BIND_ADDRESS.to_owned());
        let mut gateway = super::authorization::apply_from_env()?;
        if super::transport::sdk_tls_enabled()? {
            let certificate_path = env::var("RELAYGATE_SDK_TLS_CERT_PATH")
                .context("RELAYGATE_SDK_TLS_CERT_PATH is required for Gateway mode")?;
            let private_key_path = env::var("RELAYGATE_SDK_TLS_KEY_PATH")
                .context("RELAYGATE_SDK_TLS_KEY_PATH is required for Gateway mode")?;
            let tls = ServerTlsConfig::server_authenticated(
                &fs::read(&certificate_path).with_context(|| {
                    format!("failed to read SDK TLS certificate at {certificate_path:?}")
                })?,
                &fs::read(&private_key_path).with_context(|| {
                    format!("failed to read SDK TLS private key at {private_key_path:?}")
                })?,
            )?;
            gateway = gateway.with_sdk_tls(tls);
        }
        gateway = override_with(
            gateway,
            optional_usize("RELAYGATE_WRITER_QUEUE_CAPACITY")?,
            GatewayConfig::with_writer_queue_capacity,
        );
        gateway = override_with(
            gateway,
            optional_usize("RELAYGATE_MAX_FRAME_LEN")?,
            GatewayConfig::with_max_frame_len,
        );
        gateway = override_with(
            gateway,
            optional_usize("RELAYGATE_MAX_SESSIONS")?,
            GatewayConfig::with_max_sessions,
        );
        gateway = override_with(
            gateway,
            optional_usize("RELAYGATE_MAX_PENDING_HANDSHAKES")?,
            GatewayConfig::with_max_pending_handshakes,
        );
        let (default_rate, default_burst) = gateway.sdk_connection_rate_limit();
        gateway = gateway.with_sdk_connection_rate_limit(
            optional_usize("RELAYGATE_SDK_CONNECTION_RATE_PER_SECOND")?.unwrap_or(default_rate),
            optional_usize("RELAYGATE_SDK_CONNECTION_BURST")?.unwrap_or(default_burst),
        );
        gateway = override_with(
            gateway,
            optional_usize("RELAYGATE_MAX_BINDINGS")?,
            GatewayConfig::with_max_bindings,
        );
        let (rate, burst) = gateway.control_rate_limit();
        gateway = gateway.with_control_rate_limit(
            optional_usize("RELAYGATE_CONTROL_RATE_PER_SECOND")?.unwrap_or(rate),
            optional_usize("RELAYGATE_CONTROL_BURST")?.unwrap_or(burst),
        );
        let (rate, burst) = gateway.session_control_rate_limit();
        gateway = gateway.with_session_control_rate_limit(
            optional_usize("RELAYGATE_SESSION_CONTROL_RATE_PER_SECOND")?.unwrap_or(rate),
            optional_usize("RELAYGATE_SESSION_CONTROL_BURST")?.unwrap_or(burst),
        );
        gateway = override_with(
            gateway,
            optional_usize("RELAYGATE_MAX_PENDING_OFFERS")?,
            GatewayConfig::with_max_pending_offers,
        );
        gateway = override_with(
            gateway,
            optional_usize("RELAYGATE_MAX_REMOTE_DIAL_ATTEMPTS")?,
            GatewayConfig::with_max_remote_dial_attempts,
        );
        gateway = override_with(
            gateway,
            optional_usize("RELAYGATE_MAX_LIVE_PIPES")?,
            GatewayConfig::with_max_live_pipes,
        );
        gateway = override_with(
            gateway,
            optional_duration_millis("RELAYGATE_OFFER_TIMEOUT_MS")?,
            GatewayConfig::with_offer_timeout,
        );
        gateway = override_with(
            gateway,
            optional_duration_millis("RELAYGATE_DRAIN_TIMEOUT_MS")?,
            GatewayConfig::with_drain_timeout,
        );
        let heartbeat_idle = optional_duration_millis("RELAYGATE_SDK_HEARTBEAT_IDLE_MS")?;
        let heartbeat_response = optional_duration_millis("RELAYGATE_SDK_HEARTBEAT_TIMEOUT_MS")?;
        if heartbeat_idle.is_some() || heartbeat_response.is_some() {
            let default_idle = gateway.heartbeat_idle_interval();
            let default_response = gateway.heartbeat_response_timeout();
            gateway = gateway.with_heartbeat(
                heartbeat_idle.unwrap_or(default_idle),
                heartbeat_response.unwrap_or(default_response),
            );
        }

        Ok(Self {
            bind_address,
            gateway,
            distributed: distributed_from_env(transport)?,
            stats_interval: optional_duration_millis("RELAYGATE_STATS_INTERVAL_MS")?,
        })
    }
}

/// Applies one optional environment override through its builder method,
/// leaving the current value in place when the variable is unset.
fn override_with<T>(
    config: GatewayConfig,
    value: Option<T>,
    apply: impl FnOnce(GatewayConfig, T) -> GatewayConfig,
) -> GatewayConfig {
    match value {
        Some(value) => apply(config, value),
        None => config,
    }
}

fn distributed_from_env(transport: InternalTransport) -> Result<Option<DistributedGatewayConfig>> {
    if !DISTRIBUTED_ENVIRONMENT
        .iter()
        .any(|name| env::var_os(name).is_some())
    {
        return Ok(None);
    }
    let insecure = transport == InternalTransport::Plaintext;

    let directory_path = env::var("RELAYGATE_RT_SHARD_DIRECTORY_PATH")
        .context("RELAYGATE_RT_SHARD_DIRECTORY_PATH is required for distributed Gateway mode")?;
    let directory =
        ShardDirectory::from_json_bytes(fs::read(&directory_path).with_context(|| {
            format!("failed to read ShardDirectory artifact at {directory_path:?}")
        })?)?;
    let gateway_name_value = env::var("RELAYGATE_GATEWAY_NAME")
        .context("RELAYGATE_GATEWAY_NAME is required for distributed Gateway mode")?;
    let gateway_name = GatewayName::new(gateway_name_value.clone())?;
    let gateway_locator = GatewayLocator::new(
        env::var("RELAYGATE_GATEWAY_LOCATOR")
            .context("RELAYGATE_GATEWAY_LOCATOR is required for distributed Gateway mode")?,
    )?;
    let mut peer = GatewayPeerConfig::new(gateway_name_value)?;
    let peer_heartbeat_idle = optional_duration_millis("RELAYGATE_PEER_HEARTBEAT_IDLE_MS")?
        .unwrap_or_else(|| peer.heartbeat_idle_interval());
    let peer_heartbeat_response = optional_duration_millis("RELAYGATE_PEER_HEARTBEAT_TIMEOUT_MS")?
        .unwrap_or_else(|| peer.heartbeat_response_timeout());
    let peer_idle_retirement = optional_duration_millis("RELAYGATE_PEER_IDLE_TIMEOUT_MS")?
        .unwrap_or_else(|| peer.idle_retirement_timeout());
    peer = peer.with_liveness(
        peer_heartbeat_idle,
        peer_heartbeat_response,
        peer_idle_retirement,
    );
    let client = RouteTableClientConfig::new(
        DEFAULT_RT_CLIENT_QUEUE_CAPACITY,
        DEFAULT_RT_MAX_FRAME_LEN,
        DEFAULT_RT_CONNECT_TIMEOUT,
        DEFAULT_RT_HANDSHAKE_TIMEOUT,
        DEFAULT_RT_REQUEST_TIMEOUT,
    )?;
    let mut routing = GatewayRoutingConfig::new(directory, gateway_name, gateway_locator, client);
    if !insecure {
        let material = load_internal_tls()?;
        let peer_server_name = env::var("RELAYGATE_PEER_TLS_SERVER_NAME")
            .context("RELAYGATE_PEER_TLS_SERVER_NAME is required")?;
        let route_table_server_name = env::var("RELAYGATE_RT_TLS_SERVER_NAME")
            .context("RELAYGATE_RT_TLS_SERVER_NAME is required")?;
        peer = peer.with_tls(
            ClientTlsConfig::mutually_authenticated(
                peer_server_name.clone(),
                &material.ca,
                &material.certificate,
                &material.private_key,
            )?,
            ServerTlsConfig::mutually_authenticated(
                peer_server_name,
                &material.ca,
                &material.certificate,
                &material.private_key,
            )?,
        );
        routing = routing.with_tls(ClientTlsConfig::mutually_authenticated(
            route_table_server_name,
            &material.ca,
            &material.certificate,
            &material.private_key,
        )?);
    }
    Ok(Some(DistributedGatewayConfig {
        peer_bind_address: optional_env("RELAYGATE_PEER_BIND_ADDR")?
            .unwrap_or_else(|| DEFAULT_PEER_BIND_ADDRESS.to_owned()),
        routing,
        peer,
        insecure_transport: insecure,
    }))
}
