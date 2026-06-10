# Realtime Sidecar Demo

This crate does not implement Webex Mercury directly. The supported realtime
path is a small JavaScript SDK sidecar that owns the Webex websocket listener
and forwards normalized events to a local Rust receiver over loopback HTTP.

The demo has two pieces:

- `examples/sidecar_receiver.rs`: Rust loopback HTTP receiver that accepts
  `SidecarEvent` JSON envelopes.
- `examples/sidecar-js/index.mjs`: Node.js sidecar that loads the minimal
  WebexCore messages/people/logger plugins, calls the Webex JS SDK
  `messages.listen()` API, and forwards `messages.created` / `messages.deleted`
  events to the Rust receiver.

## Event Envelope

The sidecar posts JSON to `POST /webex/events` by default:

```json
{
  "version": 1,
  "resource": "messages",
  "event": "created",
  "receivedAt": "2026-06-08T15:00:00Z",
  "data": {
    "id": "...",
    "roomId": "...",
    "text": "..."
  }
}
```

The Rust type is `webex_headless_messenger::SidecarEvent`. `data` intentionally
stays raw JSON because the JS SDK can expose fields that are not present in a
standard webhook payload.

## Local E2E Smoke

This path does not require Webex credentials or npm install; it validates the
Rust receiver and the forwarding protocol with a mock event.

Terminal 1:

```bash
WEBEX_SIDECAR_BIND=127.0.0.1:8787 \
WEBEX_SIDECAR_MAX_EVENTS=1 \
WEBEX_SIDECAR_TOKEN=dev-sidecar-token \
cargo run --example sidecar_receiver --all-features
```

Terminal 2:

```bash
WEBEX_SIDECAR_TARGET_URL=http://127.0.0.1:8787/webex/events \
WEBEX_SIDECAR_TOKEN=dev-sidecar-token \
WEBEX_SIDECAR_MOCK_EVENT=1 \
node examples/sidecar-js/index.mjs
```

Expected receiver output includes:

```text
sidecar_event resource=messages event=created payload=...
sidecar_event_accepted_from=127.0.0.1:...
```

## Live Webex Listener

Install the JS sidecar dependency once. The demo intentionally uses the SDK's
minimal messages plugins in Node instead of the default bundle, which also loads
meetings/calling code intended for browser media paths.


```bash
cd examples/sidecar-js
npm install
```

Create one forwarding token and use that exact value in both terminals:

```bash
openssl rand -hex 24
```

Start the Rust receiver in terminal 1:

```bash
WEBEX_SIDECAR_TOKEN=<same-token> \
cargo run --example sidecar_receiver --all-features
```

Start the JS SDK sidecar in terminal 2 with the same forwarding token:

```bash
cd examples/sidecar-js
WEBEX_ACCESS_TOKEN=... \
WEBEX_SIDECAR_TOKEN=<same-token> \
WEBEX_SIDECAR_TARGET_URL=http://127.0.0.1:8787/webex/events \
node index.mjs
```

The token must belong to the generic account or bot identity that should receive
realtime events. For a generic OAuth Integration account, the Webex JS SDK
`messages.listen()` path requires `spark:all` and `spark:kms`; the narrower REST
scopes are not enough for Mercury/WebSocket registration. After changing
Integration permissions, remove the old token cache and re-run Device Grant Flow
with `WEBEX_TEST_SCOPES="spark:all spark:kms"`. For bot tokens, Webex bot
visibility rules apply; the bot may only receive message events it is allowed to
see.

## Security Boundary

- Bind the receiver to loopback, not a public interface.
- Set `WEBEX_SIDECAR_TOKEN` on both processes so local POSTs require a bearer
  token. The receiver refuses to start without it unless
  `WEBEX_SIDECAR_ALLOW_UNAUTHENTICATED=1` is explicitly set for local unsafe
  testing.
- Treat forwarded event bodies as untrusted input. The demo prints them; a real
  automation should validate resource/event and deduplicate by message ID.
- Keep the sidecar and Rust automation under the same supervisor so both stop
  and restart together.

## References

- Webex Browser SDK Messaging Quick Start:
  <https://developer.webex.com/docs/browser-sdk-messaging-tutorial>
- Using Websockets with the Webex JavaScript SDK:
  <https://developer.webex.com/blog/using-websockets-with-the-webex-javascript-sdk>
- Webex JS SDK package:
  <https://www.npmjs.com/package/webex>
