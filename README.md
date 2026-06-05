# webex-headless-messenger

Async Rust bindings for headless Webex Messaging automation.

This crate targets a dedicated Webex user account, often called a generic
account, authorized through a Webex OAuth Integration. It wraps the public Webex
REST API for reading spaces, reading and sending messages, replying in threads,
listing memberships, and optionally managing webhooks.

## Scope

Implemented in the first slice:

- OAuth Integration helpers for Authorization Code, Device Grant, and refresh
  token flows.
- Token provider abstraction for fixed tokens and refreshable token storage.
- Typed REST client for:
  - `GET /v1/people/me`
  - `GET/POST/PUT/DELETE /v1/rooms`
  - `GET/POST/PUT/DELETE /v1/messages`
  - `GET /v1/messages/direct`
  - `GET/POST/PUT/DELETE /v1/memberships`
  - `GET/POST/PUT/DELETE /v1/webhooks`
- RFC5988 `Link` header pagination.
- Structured API errors with `trackingId` and `Retry-After` when available.
- Polling-based message receiver for deployments without public HTTP ingress.
- Optional webhook HMAC-SHA1 signature verification behind the `webhooks`
  feature.

Not implemented yet:

- Local file upload multipart helpers.
- Adaptive Card builders beyond raw JSON attachment payloads.
- A native Rust WebSocket/Mercury client. Cisco documents realtime messaging
  listening through the official JavaScript SDK, not as a stable public
  WebSocket protocol. Use REST polling or a JS SDK sidecar until that boundary is
  explicitly supported.

## OAuth Scopes

For a normal generic account that should only access spaces where the account is
a member, start with:

```text
spark:messages_read
spark:messages_write
spark:rooms_read
spark:memberships_read
spark:people_read
spark:kms
```

`spark:kms` is required by Webex for encrypted content such as messages.
`spark:all` is not required for the REST client. Compliance scopes are a
separate organization-wide model and are intentionally not the default.

If your app calls room or membership mutation helpers, add:

```text
spark:rooms_write
spark:memberships_write
```

Webhook management uses the read scope for the resource being monitored. For
example, message webhooks require `spark:messages_read`, membership webhooks
require `spark:memberships_read`, and room webhooks require `spark:rooms_read`.
The default scope set already includes those three read scopes for Messaging
automation.

For Device Grant Flow, the Webex Integration must include Cisco's OAuth helper
service redirect URIs documented by Webex. Device Token polling also requires
the integration client secret because Webex expects HTTP Basic authentication on
that endpoint.

Authorization Code helpers include PKCE support through
`authorization_url_with_pkce` and `exchange_authorization_code_with_pkce`.

## Quick Start

```rust
use webex_headless_messenger::{
    types::{CreateMessage, ListMessages},
    WebexClient,
};

#[tokio::main]
async fn main() -> webex_headless_messenger::Result<()> {
    let client = WebexClient::from_access_token(std::env::var("WEBEX_ACCESS_TOKEN").unwrap())?;

    let room_id = std::env::var("WEBEX_ROOM_ID").unwrap();
    let page = client.list_messages(&ListMessages::room(&room_id)).await?;
    for message in page.items {
        println!("{:?}", message.text);
    }

    client
        .create_message(&CreateMessage::text(room_id, "hello from Rust"))
        .await?;

    Ok(())
}
```

## Device Grant Bootstrap

```rust
use std::time::Duration;

use tokio::time::sleep;
use webex_headless_messenger::{
    DeviceTokenStatus, OAuthClient, OAuthConfig, DEFAULT_MESSAGING_SCOPES,
};

#[tokio::main]
async fn main() -> webex_headless_messenger::Result<()> {
    let config = OAuthConfig::new(std::env::var("WEBEX_CLIENT_ID").unwrap())?
        .with_client_secret(std::env::var("WEBEX_CLIENT_SECRET").unwrap())
        .with_scopes(DEFAULT_MESSAGING_SCOPES.iter().copied());
    let oauth = OAuthClient::new(config);

    let auth = oauth.start_device_authorization().await?;
    println!("Open {} and enter {}", auth.verification_uri, auth.user_code);

    let mut interval = Duration::from_secs(auth.interval.unwrap_or(5));
    loop {
        match oauth.poll_device_token(&auth.device_code).await? {
            DeviceTokenStatus::Authorized(tokens) => {
                println!("{}", serde_json::to_string_pretty(&tokens)?);
                break;
            }
            DeviceTokenStatus::Pending { retry_after } => {
                sleep(retry_after.unwrap_or(Duration::ZERO).max(interval)).await;
            }
            DeviceTokenStatus::SlowDown { retry_after } => {
                interval += Duration::from_secs(5);
                sleep(retry_after.unwrap_or(Duration::ZERO).max(interval)).await;
            }
        }
    }

    Ok(())
}
```

Store the resulting refresh token in your application-owned secret storage. The
crate includes `MemoryTokenStore` for tests and simple processes; production
headless deployments should provide a durable `TokenStore`.

## Polling Without Public Ingress

```rust
use webex_headless_messenger::{MessagePoller, WebexClient};

#[tokio::main]
async fn main() -> webex_headless_messenger::Result<()> {
    let client = WebexClient::from_access_token(std::env::var("WEBEX_ACCESS_TOKEN").unwrap())?;
    let mut events = MessagePoller::new(client, std::env::var("WEBEX_ROOM_ID").unwrap()).spawn();

    while let Some(event) = events.recv().await {
        println!("{:?}", event?);
    }

    Ok(())
}
```

Polling is intentionally conservative. It de-duplicates by message ID in memory
and skips existing messages on the first poll by default.

## Smoke Test

For local validation against a real generic account, create `.env.webex-test`
with:

```text
WEBEX_CLIENT_ID=...
WEBEX_CLIENT_SECRET=...
WEBEX_TEST_ROOM_ID=
WEBEX_TEST_ROOM_LINK=...
WEBEX_TEST_ROOM_TITLE=...
WEBEX_TEST_PERSON_EMAIL=...
```

Then run:

```bash
cargo run --example smoke --all-features
```

The example starts Device Grant Flow, stores the resulting token in
`.codex-tmp/webex-smoke/token.json` with owner-only file permissions on Unix
and disables persistent token caching on non-Unix platforms, resolves the test room from
`WEBEX_TEST_ROOM_ID`, candidates parsed from `WEBEX_TEST_ROOM_LINK`, or a unique
`WEBEX_TEST_ROOM_TITLE`, then performs read/send/reply smoke checks.

See [docs/smoke-testing.md](docs/smoke-testing.md) for Webex Integration setup,
Device Grant authorization steps, expected output, and troubleshooting.

## References

- Webex Messaging REST API: <https://developer.webex.com/docs/api/v1/messages>
- Webex REST API basics, pagination, rate limits, and errors:
  <https://developer.webex.com/docs/rest-api-basics>
- Webex OAuth Integration and Device Grant Flow:
  <https://developer.webex.com/docs/login-with-webex>
- Webex Webhooks:
  <https://developer.webex.com/messaging/docs/api/guides/webhooks>
- Official Webex OpenAPI specs:
  <https://github.com/webex/webex-openapi-specs>
