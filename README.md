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
- Local file upload helper for `multipart/form-data` message creation.
- JavaScript SDK realtime sidecar demo that forwards normalized message events
  to a local Rust loopback receiver.
- `webex-headless` CLI for device authorization, room/message REST calls,
  polling, and a loopback sidecar receiver.
- Optional webhook HMAC-SHA1 signature verification behind the `webhooks`
  feature.

Not implemented yet:

- Adaptive Card builders beyond raw JSON attachment payloads.
- A native Rust WebSocket/Mercury client. Cisco documents realtime messaging
  listening through the official JavaScript SDK, not as a stable public
  WebSocket protocol. Use REST polling or the JS SDK sidecar demo until that
  boundary is explicitly supported.

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

## CLI

The crate also ships a thin `webex-headless` binary for scripts and local
operator workflows. It intentionally mirrors the library API instead of adding a
separate command framework.

Authorize a generic account with Device Grant Flow and store the refreshable
`TokenSet` JSON locally:

```bash
cargo run --bin webex-headless -- \
  --client-id "$WEBEX_CLIENT_ID" \
  --client-secret "$WEBEX_CLIENT_SECRET" \
  auth device --token-file .codex-tmp/webex-token.json
```

Use the token file for REST calls. When `WEBEX_CLIENT_ID` and
`WEBEX_CLIENT_SECRET` are also set, the CLI refreshes expiring access tokens and
updates the file. Token-file persistence is currently Unix-only and writes the
file with owner-only `0600` permissions; on non-Unix platforms, use
`--stdout-token` and store the JSON in platform secret storage.

Long-running services can proactively refresh the same token cache with
`auth refresh`, for example from a systemd timer or cron job:

```bash
cargo run --bin webex-headless -- \
  auth refresh \
  --token-file .codex-tmp/webex-token.json \
  --client-id "$WEBEX_CLIENT_ID" \
  --client-secret "$WEBEX_CLIENT_SECRET"
```

```bash
cargo run --bin webex-headless -- \
  --token-file .codex-tmp/webex-token.json me

cargo run --bin webex-headless -- \
  --token-file .codex-tmp/webex-token.json \
  rooms resolve --link "$WEBEX_TEST_ROOM_LINK"

cargo run --bin webex-headless -- \
  --token-file .codex-tmp/webex-token.json \
  messages send --room-id "$WEBEX_ROOM_ID" --text "hello from webex-headless"

cargo run --bin webex-headless -- \
  --token-file .codex-tmp/webex-token.json \
  messages reply --room-id "$WEBEX_ROOM_ID" --parent-id "$WEBEX_MESSAGE_ID" \
  --markdown "reply from **webex-headless**"
```

One-shot REST commands print pretty JSON. Long-running receivers print compact
JSON Lines so they can be piped into automation:

```bash
cargo run --bin webex-headless -- \
  --token-file .codex-tmp/webex-token.json \
  poll messages --room-id "$WEBEX_ROOM_ID" --interval-seconds 10

cargo run --bin webex-headless -- \
  sidecar receive --token dev-sidecar-token --max-events 1
```

Global auth inputs can also come from `WEBEX_ACCESS_TOKEN`, `WEBEX_TOKEN_FILE`,
`WEBEX_CLIENT_ID`, and `WEBEX_CLIENT_SECRET`. Run
`cargo run --bin webex-headless -- --help` for the command list.

## Local File Upload

```rust
use webex_headless_messenger::{
    types::{CreateMessage, LocalFileAttachment},
    WebexClient,
};

#[tokio::main]
async fn main() -> webex_headless_messenger::Result<()> {
    let client = WebexClient::from_access_token(std::env::var("WEBEX_ACCESS_TOKEN").unwrap())?;

    client
        .create_message_with_file(
            &CreateMessage::text(std::env::var("WEBEX_ROOM_ID").unwrap(), "attached report"),
            &LocalFileAttachment::new("./report.pdf").with_media_type("application/pdf"),
        )
        .await?;

    Ok(())
}
```

Local filesystem uploads use `multipart/form-data` and support one local file per
message. The helper rejects non-regular files, files over 100 MB, CR/LF in file
names, and invalid MIME syntax before sending the request; Webex may still apply
additional server-side validation. On Unix and Windows, local symlinks/reparse
points are opened with no-follow semantics and then rejected. Publicly reachable
file URLs still use the JSON `files` field with `create_message`.

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

## Realtime Sidecar

This crate does not implement Webex Mercury directly. For deployments that need
a realtime listener without public ingress, `examples/sidecar-js/index.mjs` uses
the official Webex JavaScript SDK `messages.listen()` API and forwards normalized
`SidecarEvent` JSON envelopes to the Rust loopback receiver. The sidecar can run
as a long-lived service with token-file reload, bounded forward retries, and a
localhost health endpoint; pair it with REST catch-up polling and message ID
de-duplication for recovery after restarts or network gaps.

Local mock E2E without Webex credentials:

Terminal 1:

```bash
WEBEX_SIDECAR_BIND=127.0.0.1:8787 WEBEX_SIDECAR_MAX_EVENTS=1 \
  WEBEX_SIDECAR_TOKEN=dev-sidecar-token \
  cargo run --example sidecar_receiver --all-features
```

Terminal 2:

```bash
WEBEX_SIDECAR_TARGET_URL=http://127.0.0.1:8787/webex/events \
  WEBEX_SIDECAR_TOKEN=dev-sidecar-token \
  WEBEX_SIDECAR_MOCK_EVENT=1 node examples/sidecar-js/index.mjs
```

See [docs/realtime-sidecar.md](docs/realtime-sidecar.md) for live Webex setup,
required realtime scopes (`spark:all` plus `spark:kms`), forwarding-token
configuration, token refresh/reload, health checks, loopback restrictions, and
security notes. See [deploy/systemd](deploy/systemd) for Linux supervisor
templates.

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
- Webex Browser SDK Messaging Quick Start:
  <https://developer.webex.com/docs/browser-sdk-messaging-tutorial>
- Using Websockets with the Webex JavaScript SDK:
  <https://developer.webex.com/blog/using-websockets-with-the-webex-javascript-sdk>
- Official Webex OpenAPI specs:
  <https://github.com/webex/webex-openapi-specs>
