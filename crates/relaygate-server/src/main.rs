mod config;
mod metrics;
mod runtime;

use std::{env, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use relaygate_gateway::{check, check_plaintext};
use relaygate_transport::ClientTlsConfig;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const DEFAULT_CHECK_DEADLINE: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;
    match command()? {
        Command::Serve(RuntimeRole::Gateway) => {
            let config = config::GatewayRuntimeConfig::from_env()?;
            let metrics = metrics::MetricsRuntime::install(metrics::MetricsRole::Gateway)?;
            let shutdown = process_shutdown();
            runtime::gateway::serve(config, shutdown, metrics).await
        }
        Command::Serve(RuntimeRole::RouteTable) => {
            let config = config::RouteTableRuntimeConfig::from_env()?;
            let _metrics = metrics::MetricsRuntime::install(metrics::MetricsRole::RouteTable)?;
            let shutdown = process_shutdown();
            runtime::route_table::serve(config, shutdown).await
        }
        Command::CheckGateway { address } => {
            config::reject_removed_flags()?;
            if !config::sdk_tls_enabled()? {
                check_plaintext(address, DEFAULT_CHECK_DEADLINE)
                    .await
                    .context("Gateway SDK admission readiness check failed")
            } else {
                let server_name = env::var("RELAYGATE_SDK_TLS_SERVER_NAME")
                    .context("RELAYGATE_SDK_TLS_SERVER_NAME is required for Gateway checks")?;
                let tls = match env::var("RELAYGATE_SDK_TLS_CA_PATH") {
                    Ok(ca_path) => ClientTlsConfig::server_authenticated(
                        server_name,
                        &std::fs::read(&ca_path)
                            .with_context(|| format!("failed to read SDK TLS CA at {ca_path:?}"))?,
                    )?,
                    Err(env::VarError::NotPresent) => {
                        ClientTlsConfig::with_webpki_roots(server_name)?
                    }
                    Err(error) => {
                        return Err(error)
                            .context("RELAYGATE_SDK_TLS_CA_PATH is not valid Unicode");
                    }
                };
                check(address, &tls, DEFAULT_CHECK_DEADLINE)
                    .await
                    .context("Gateway SDK admission readiness check failed")
            }
        }
    }
}

fn process_shutdown() -> CancellationToken {
    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        signal_shutdown.cancel();
    });
    shutdown
}

fn command() -> Result<Command> {
    let mut args = env::args().skip(1);
    match args.next() {
        None => Ok(Command::Serve(RuntimeRole::Gateway)),
        Some(value) if value == "gateway" => {
            reject_extra_arguments(&mut args, "usage: relaygate-server gateway")?;
            Ok(Command::Serve(RuntimeRole::Gateway))
        }
        Some(value) if value == "route-table" => {
            reject_extra_arguments(&mut args, "usage: relaygate-server route-table")?;
            Ok(Command::Serve(RuntimeRole::RouteTable))
        }
        Some(value) if value == "check" => {
            let Some(address) = args.next() else {
                bail!("usage: relaygate-server check <address>");
            };
            reject_extra_arguments(&mut args, "usage: relaygate-server check <address>")?;
            Ok(Command::CheckGateway { address })
        }
        Some(value) => bail!(
            "unknown command {value:?}; expected `gateway`, `route-table`, or `check <address>`"
        ),
    }
}

fn reject_extra_arguments(
    args: &mut impl Iterator<Item = String>,
    usage: &'static str,
) -> Result<()> {
    if args.next().is_some() {
        bail!(usage);
    }
    Ok(())
}

fn init_tracing() -> Result<()> {
    let filter = match env::var("RELAYGATE_LOG") {
        Ok(value) => {
            EnvFilter::try_new(value).context("RELAYGATE_LOG is not a valid log filter")?
        }
        Err(_) => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };
    match parse_log_format(env::var("RELAYGATE_LOG_FORMAT").ok())? {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .try_init()
            .map_err(|error| anyhow!("failed to initialize tracing: {error}")),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_env_filter(filter)
            .with_target(false)
            .try_init()
            .map_err(|error| anyhow!("failed to initialize tracing: {error}")),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum LogFormat {
    #[default]
    Text,
    Json,
}

fn parse_log_format(value: Option<String>) -> Result<LogFormat> {
    match value.as_deref().unwrap_or("text") {
        "text" => Ok(LogFormat::Text),
        "json" => Ok(LogFormat::Json),
        other => bail!("RELAYGATE_LOG_FORMAT must be `text` or `json`, got {other:?}"),
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::warn!(
                                component = "server",
                                event = "server.signal_listener_failed",
                                signal = "SIGINT",
                                %error,
                                "failed to listen for Ctrl-C"
                            );
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!(
                    component = "server",
                    event = "server.signal_listener_failed",
                    signal = "SIGTERM",
                    %error,
                    "failed to listen for SIGTERM"
                );
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(
            component = "server",
            event = "server.signal_listener_failed",
            signal = "SIGINT",
            %error,
            "failed to listen for Ctrl-C"
        );
    }
}

enum Command {
    Serve(RuntimeRole),
    CheckGateway { address: String },
}

enum RuntimeRole {
    Gateway,
    RouteTable,
}

#[cfg(test)]
mod tests {
    use super::{LogFormat, parse_log_format};

    #[test]
    fn log_format_accepts_text_and_json_only() {
        assert_eq!(parse_log_format(None).unwrap_or_default(), LogFormat::Text);
        assert_eq!(
            parse_log_format(Some("json".to_owned())).unwrap_or_default(),
            LogFormat::Json
        );
        assert!(parse_log_format(Some("xml".to_owned())).is_err());
    }
}
