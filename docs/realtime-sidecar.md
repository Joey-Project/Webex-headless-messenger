# Realtime Sidecar

This crate does not implement Webex Mercury directly. The supported realtime
path is a small JavaScript SDK sidecar that owns the Webex websocket listener
and forwards normalized events to a local Rust receiver over loopback HTTP.

The sidecar is suitable for a long-running bot-like service when it is paired
with a supervisor and REST catch-up polling. Realtime events are the low-latency
signal; REST polling remains the recovery path after process restarts, network
breaks, token reload gaps, or receiver failures.

The sidecar pieces are:

- `examples/sidecar-js/index.mjs`: Node.js sidecar that loads the minimal
  WebexCore messages/people/logger plugins, calls the Webex JS SDK
  `messages.listen()` API, and forwards `messages.created` / `messages.deleted`
  events.
- `webex-headless sidecar receive`: loopback HTTP receiver that emits accepted
  `SidecarEvent` envelopes as JSON Lines and exposes a local health endpoint.
- `examples/sidecar_receiver.rs`: smaller receiver example kept for embedding
  and mock protocol tests.

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
WEBEX_SIDECAR_TOKEN=dev-sidecar-token \
cargo run --bin webex-headless -- \
  sidecar receive --bind 127.0.0.1:8787 --max-events 1
```

Terminal 2:

```bash
WEBEX_SIDECAR_TARGET_URL=http://127.0.0.1:8787/webex/events \
WEBEX_SIDECAR_TOKEN=dev-sidecar-token \
WEBEX_SIDECAR_MOCK_EVENT=1 \
node examples/sidecar-js/index.mjs
```

Expected receiver output includes one compact `SidecarEvent` JSON line and a
stderr line like `sidecar_event_accepted_from=127.0.0.1:...`.

## Live Webex Listener

Install the JS sidecar dependency once. The sidecar uses direct WebexCore
messages/people/logger plugin packages instead of the default `webex` bundle,
which also loads meetings/calling code intended for browser media paths. These
are still official SDK packages with their own transitive dependency tree; audit
and pin that tree before treating this as production infrastructure.

```bash
cd examples/sidecar-js
npm install
```

Create one forwarding token and use that exact value in the receiver and JS
sidecar. This token only protects local forwarding; it is not the Webex OAuth
access token.

```bash
openssl rand -hex 24
```

Start the receiver:

```bash
WEBEX_SIDECAR_TOKEN=<same-forwarding-token> \
cargo run --bin webex-headless -- \
  sidecar receive \
  --bind 127.0.0.1:8787 \
  --path /webex/events \
  --health-path /healthz
```

Start the JS SDK sidecar with either a raw access token or the refreshable token
file produced by `webex-headless auth device`:

```bash
cd examples/sidecar-js
WEBEX_TOKEN_FILE=/var/lib/webex-headless/token.json \
WEBEX_SIDECAR_TOKEN=<same-forwarding-token> \
WEBEX_SIDECAR_TARGET_URL=http://127.0.0.1:8787/webex/events \
WEBEX_SIDECAR_HEALTH_BIND=127.0.0.1:8788 \
node index.mjs
```

The token must belong to the generic account or bot identity that should receive
realtime events. For a generic OAuth Integration account, the Webex JS SDK
`messages.listen()` path requires `spark:all` and `spark:kms`; the narrower REST
scopes are not enough for Mercury/WebSocket registration. After changing
Integration permissions, remove the old token cache and re-run Device Grant Flow
with the CLI scope override:

```bash
cargo run --bin webex-headless -- \
  auth device \
  --token-file /var/lib/webex-headless/token.json \
  --scopes "spark:all spark:kms"
```

For bot tokens, Webex bot visibility rules apply; the bot may only receive
message events it is allowed to see.

## Long-Running Service Mode

A service deployment should run three loops under one supervisor or service
manager:

1. Rust bot process: consumes sidecar JSON Lines or receives HTTP events, stores
   processed message IDs, and runs REST catch-up polling for the rooms it cares
   about.
2. JS sidecar process: runs `messages.listen()`, forwards events locally, reloads
   the access token file when it changes, and exits after unrecoverable forward
   failures so the supervisor can restart it.
3. Token refresh process: keeps the shared token file fresh before access-token
   expiry.

Refresh the same token cache proactively with the CLI:

```bash
cargo run --bin webex-headless -- \
  auth refresh \
  --token-file /var/lib/webex-headless/token.json \
  --client-id "$WEBEX_CLIENT_ID" \
  --client-secret "$WEBEX_CLIENT_SECRET"
```

The sidecar reads `WEBEX_TOKEN_FILE` or `WEBEX_ACCESS_TOKEN_FILE`. It accepts the
crate's `TokenSet` JSON (`accessToken`) and raw-token files. When the file token
changes, the sidecar starts a new Webex listener with the new token and then
stops the old listener. A short overlap can produce duplicate events; the bot
must de-duplicate by message ID.

Useful service environment knobs:

```text
WEBEX_TOKEN_FILE=/var/lib/webex-headless/token.json
WEBEX_SIDECAR_TOKEN=<local-forwarding-token>
WEBEX_SIDECAR_TARGET_URL=http://127.0.0.1:8787/webex/events
WEBEX_SIDECAR_TOKEN_RELOAD_INTERVAL_MS=60000
WEBEX_SIDECAR_FORWARD_RETRIES=3
WEBEX_SIDECAR_FORWARD_TIMEOUT_MS=10000
WEBEX_SIDECAR_MAX_IN_FLIGHT=8
WEBEX_SIDECAR_HEALTH_BIND=127.0.0.1:8788
```

Validate sidecar config without loading the Webex SDK. This check also requires `WEBEX_SIDECAR_TOKEN` unless `WEBEX_SIDECAR_ALLOW_UNAUTHENTICATED=1` is explicitly set for local unsafe testing:

```bash
WEBEX_TOKEN_FILE=/var/lib/webex-headless/token.json \
WEBEX_SIDECAR_TOKEN=<local-forwarding-token> \
WEBEX_SIDECAR_VALIDATE_CONFIG=1 \
node examples/sidecar-js/index.mjs
```

Health checks return minimal process state and intentionally omit the token file path and access-token fingerprint:

```bash
curl -fsS http://127.0.0.1:8787/healthz
curl -fsS http://127.0.0.1:8788/readyz
curl -fsS http://127.0.0.1:8788/livez
```

Linux systemd templates for the receiver, JS sidecar, and token refresh timer
are available in [`deploy/systemd`](../deploy/systemd).

## Reliability Model

- Forwarding uses bounded concurrency and retries transient receiver/network
  failures with exponential backoff.
- HTTP 408, HTTP 429, and 5xx responses are retried. Other HTTP 4xx
  responses are treated as configuration or authentication errors and are not
  retried.
- After retries are exhausted, the JS sidecar exits. Let the supervisor restart
  it, and use REST catch-up polling to fill any missed events.
- The sidecar does not provide a durable local queue. Add one only if a concrete
  deployment cannot rely on REST catch-up plus message ID de-duplication.
- The Rust receiver accepts `GET /healthz` without bearer auth by default because
  it binds to loopback unless explicitly overridden.

## Security Boundary

- The receiver bind address, JS target URL, and JS health bind must be loopback
  by default. Set non-loopback override variables only for deployments that add
  their own transport protection and access controls.
- Set `WEBEX_SIDECAR_TOKEN` on both processes so local POSTs require a bearer
  token. The receiver and JS sidecar refuse unauthenticated forwarding unless
  `WEBEX_SIDECAR_ALLOW_UNAUTHENTICATED=1` is explicitly set for local unsafe
  testing.
- Treat forwarded event bodies as untrusted input. A real automation should
  validate `resource`, `event`, room allowlists, and message IDs before acting.
- Store the Webex token file in application-owned secret storage with owner-only
  permissions.
- Keep the sidecar and Rust automation under the same supervisor so both stop
  and restart together.

## References

- Webex Browser SDK Messaging Quick Start:
  <https://developer.webex.com/docs/browser-sdk-messaging-tutorial>
- Using Websockets with the Webex JavaScript SDK:
  <https://developer.webex.com/blog/using-websockets-with-the-webex-javascript-sdk>
- Webex JS SDK package:
  <https://www.npmjs.com/package/webex>
