#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

mod access_token;
mod config;
mod error;
mod lifetime;
mod listener;
mod observability;
mod pipe;
mod resource;
mod retry;
mod session;
mod transport;

pub use access_token::{
    AccessAction, AccessToken, AccessTokenError, AccessTokenRequest, AccessTokenSource,
    AccessTokenSourceError, AccessTokenSourceFailure,
};
pub use config::{Config, ResourceLimits};
pub use error::{Error, ErrorCode, ErrorOrigin, PeerObservation, Result};
pub use listener::{
    Listener, ListenerStatus, ListenerStatusSubscription, Relay, RelayStatus,
    RelayStatusSubscription,
};
pub use pipe::{Pipe, PipeReadHalf, PipeWriteHalf};
pub use relaygate_destination::{Destination, DestinationName, Namespace};
pub use relaygate_transport::{ClientTlsConfig, TlsConfigError};
pub use transport::GatewayTransportConfig;
