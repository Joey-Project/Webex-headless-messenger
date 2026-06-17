//! Async Rust bindings for headless Webex Messaging automation.
//!
//! The crate intentionally wraps the public Webex REST and OAuth Integration
//! surfaces first. Webhook support is optional, and WebSocket/Mercury realtime
//! support is documented as experimental because Cisco exposes it through the
//! JavaScript SDK rather than as a stable public Rust protocol.

#![forbid(unsafe_code)]

pub mod auth;
pub mod client;
pub mod error;
pub mod pagination;
pub mod realtime;
pub mod rooms;
pub mod sidecar;
pub mod types;
pub mod webhooks;

pub use auth::{
    AccessTokenProvider, DEFAULT_MESSAGING_SCOPES, DeviceAuthorization, DeviceTokenStatus,
    MANAGEMENT_SCOPES, MemoryTokenStore, OAuthClient, OAuthConfig, PkceCodeChallengeMethod,
    RefreshingTokenProvider, StaticTokenProvider, TokenSet, TokenStore,
};
pub use client::{ClientBuilder, WebexClient};
pub use error::{ApiError, Error, Result};
pub use pagination::Page;
pub use realtime::{MessagePoller, PollingConfig};
pub use rooms::room_id_candidates_from_link;
pub use sidecar::SidecarEvent;

#[cfg(feature = "experimental-websocket")]
pub mod experimental_websocket {
    //! Placeholder for future WebSocket support.
    //!
    //! Cisco documents Webex messaging WebSocket listening through the official
    //! JavaScript SDK `listen()` APIs, backed by the SDK's Mercury connection.
    //! This crate does not yet implement that private protocol directly.

    /// Marker type for applications that want to gate experimental integrations.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct MercuryWebsocketNotImplemented;
}
