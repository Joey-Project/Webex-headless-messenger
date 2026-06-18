# Systemd Supervisor Example

This directory contains example system units for running the realtime sidecar
stack as a long-lived Linux service. Treat these files as deployment templates:
review paths, users, restart policy, and hardening settings before installing
them on a host.

The receiver stack manages three pieces:

- `webex-headless-sidecar-receiver.service`: loopback Rust receiver that accepts
  forwarded sidecar events and exposes `/healthz`.
- `webex-headless-sidecar-js.service`: Webex JavaScript SDK listener that
  forwards message events to the receiver and exposes `/readyz` / `/livez`.
- `webex-headless-token-refresh.service` / `.timer`: startup and periodic
  OAuth token refresh.

The bundled receiver writes accepted events to journald as JSON Lines. For a real
automation, this directory also includes an account-bot stack:

- `webex-headless-account-bot.service`: `webex-account-bot`, a Rust
  generic-account process that accepts the same sidecar HTTP events, persists
  processed message IDs, filters self messages, and replies through the REST
  client.
- `webex-headless-account-bot-sidecar-js.service`: the same JS listener wired to
  the account bot.
- `webex-headless-account-bot.target`: a target that starts the account bot, JS
  sidecar, and token refresh timer together.

Use either the receiver stack or the account-bot stack for a given bind port, not
both at once. Keep the same forwarding token, loopback binding, health checks,
and token-refresh timer when swapping in another bot service. OAuth credentials
and the refresh-token cache stay under the token-refresh identity. The
token-refresh service explicitly opts into a group-readable raw access-token
file for the JS sidecar inside a dedicated setgid directory; the receiver runs
under an identity with no OAuth token file access, while the account bot gets
read-only access to the published raw access token so it can reply through REST.

## Assumed Layout

```text
/usr/local/bin/webex-headless
/usr/local/bin/webex-account-bot
/opt/webex-headless-messenger/examples/sidecar-js/index.mjs
/etc/webex-headless/webex-headless.env
/etc/webex-headless/webex-headless-receiver.env
/etc/webex-headless/webex-headless-account-bot.env
/etc/webex-headless/webex-headless-token.env
/etc/webex-headless/webex-client-secret
/var/lib/webex-headless-token/token.json
/var/lib/webex-headless-access/access-token
/var/lib/webex-headless-account-bot/processed-message-ids.txt
```

## Install Binaries

Build and install the Rust binaries:

```bash
cargo build --release --bin webex-headless --bin webex-account-bot
sudo install -m 0755 target/release/webex-headless /usr/local/bin/webex-headless
sudo install -m 0755 target/release/webex-account-bot /usr/local/bin/webex-account-bot
```

Install the JavaScript dependencies in the checkout that the service will use:

```bash
cd /opt/webex-headless-messenger/examples/sidecar-js
npm ci --omit=dev
```

Create locked-down service users and directories:

```bash
sudo groupadd --system webex-headless-sidecar
sudo groupadd --system webex-headless-receiver
sudo groupadd --system webex-headless-account-bot
sudo groupadd --system webex-headless-token
sudo useradd --system --gid webex-headless-sidecar --home /var/lib/webex-headless-access --shell /usr/sbin/nologin webex-headless-sidecar
sudo useradd --system --gid webex-headless-receiver --home /nonexistent --shell /usr/sbin/nologin webex-headless-receiver
sudo useradd --system --gid webex-headless-account-bot --groups webex-headless-sidecar --home /var/lib/webex-headless-account-bot --shell /usr/sbin/nologin webex-headless-account-bot
sudo useradd --system --gid webex-headless-token --home /var/lib/webex-headless-token --shell /usr/sbin/nologin webex-headless-token
sudo install -d -o webex-headless-token -g webex-headless-token -m 0700 /var/lib/webex-headless-token
sudo install -d -o webex-headless-token -g webex-headless-sidecar -m 2750 /var/lib/webex-headless-access
sudo install -d -o webex-headless-account-bot -g webex-headless-account-bot -m 0700 /var/lib/webex-headless-account-bot
sudo install -d -o root -g webex-headless-token -m 0750 /etc/webex-headless
sudo install -o root -g webex-headless-token -m 0640 /dev/null /etc/webex-headless/webex-client-secret
```

Install the env templates and keep them readable only by root. The
`/etc/webex-headless` directory is group-traversable by `webex-headless-token` so
the token-refresh process can open the separate client-secret file, but the env
files remain `root:root 0600`. Put the OAuth client ID and secret file path in
`webex-headless-token.env`, and put the secret value itself in
`/etc/webex-headless/webex-client-secret`. The token-refresh identity can read
that secret file, while the JS sidecar and receiver cannot. Keep the JS
sidecar, receiver, and account-bot env files limited to their runtime settings,
local forwarding token, and published access-token path:

```bash
sudo install -o root -g root -m 0600 \
  deploy/systemd/webex-headless.env.example \
  /etc/webex-headless/webex-headless.env
sudo install -o root -g root -m 0600 \
  deploy/systemd/webex-headless-receiver.env.example \
  /etc/webex-headless/webex-headless-receiver.env
sudo install -o root -g root -m 0600 \
  deploy/systemd/webex-headless-account-bot.env.example \
  /etc/webex-headless/webex-headless-account-bot.env
sudo install -o root -g root -m 0600 \
  deploy/systemd/webex-headless-token.env.example \
  /etc/webex-headless/webex-headless-token.env
sudo editor /etc/webex-headless/webex-headless.env
sudo editor /etc/webex-headless/webex-headless-receiver.env
sudo editor /etc/webex-headless/webex-headless-account-bot.env
sudo editor /etc/webex-headless/webex-headless-token.env
sudo editor /etc/webex-headless/webex-client-secret
```

Install the units:

```bash
sudo install -m 0644 deploy/systemd/webex-headless-sidecar.target /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-sidecar-receiver.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-sidecar-js.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-account-bot.target /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-account-bot.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-account-bot-sidecar-js.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-token-refresh.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-token-refresh.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

Bootstrap the initial token file after setting the Integration client ID, secret
file path, secret file contents, and scopes. Load the protected env file through a
transient systemd service, and pass `--client-secret-file` explicitly so legacy
`WEBEX_CLIENT_SECRET` values left in the env file cannot take precedence. The CLI
reads the client secret from the secret file so the secret value is not placed on
the command line or in the process environment:

```bash
sudo systemd-run --wait --collect --pty \
  --uid=webex-headless-token \
  --gid=webex-headless-token \
  --property=EnvironmentFile=/etc/webex-headless/webex-headless-token.env \
  /usr/local/bin/webex-headless auth device \
    --client-secret-file /etc/webex-headless/webex-client-secret \
    --token-file /var/lib/webex-headless-token/token.json \
    --access-token-file /var/lib/webex-headless-access/access-token \
    --access-token-file-group-readable \
    --scopes 'spark:all spark:kms'
```

Start one stack. The JS sidecar starts after a best-effort token refresh service
run. That refresh keeps the private TokenSet cache current and publishes the raw
access-token file consumed by the JS sidecar and account bot; if refresh fails or
times out, the sidecar uses the access token initially published by Device Grant
or the last token published by a later refresh:

```bash
sudo systemctl enable --now webex-headless-sidecar.target
sudo systemctl enable --now webex-headless-token-refresh.timer
```

Or start the integrated account-bot stack:

```bash
sudo systemctl enable --now webex-headless-account-bot.target
sudo systemctl enable --now webex-headless-token-refresh.timer
```

## Verify

```bash
sudo systemctl status webex-headless-sidecar-receiver.service
sudo systemctl status webex-headless-sidecar-js.service
sudo systemctl status webex-headless-account-bot.service
sudo systemctl status webex-headless-account-bot-sidecar-js.service
sudo systemctl status webex-headless-token-refresh.timer
curl -fsS http://127.0.0.1:8787/healthz
curl -fsS http://127.0.0.1:8788/readyz
curl -fsS http://127.0.0.1:8788/livez
```

Useful logs:

```bash
journalctl -u webex-headless-sidecar-receiver.service -f
journalctl -u webex-headless-sidecar-js.service -f
journalctl -u webex-headless-account-bot.service -f
journalctl -u webex-headless-account-bot-sidecar-js.service -f
journalctl -u webex-headless-token-refresh.service
```

## Operations Notes

- Keep `WEBEX_SIDECAR_TOKEN` identical in the receiver and JS sidecar env files.
  For the account-bot stack, keep it identical in
  `webex-headless-account-bot.env` and `webex-headless.env`.
- Keep `WEBEX_ACCESS_TOKEN_FILE` identical in the JS sidecar, account-bot,
  and token-refresh env files.
  The CLI writes raw access-token files as `0600` by default; this template uses
  `--access-token-file-group-readable` only with the dedicated
  `webex-headless-sidecar` group and setgid access-token directory.
- Keep `WEBEX_REFRESH_TOKEN_FILE` private to the token-refresh env file; it stores
  the full refreshable `TokenSet` cache.
- Keep `WEBEX_CLIENT_SECRET_FILE` private to the token-refresh env file, and keep
  the referenced secret file readable only by root and `webex-headless-token`;
  the parent directory must also be traversable by `webex-headless-token`.
- Keep every bind and target URL on loopback unless another layer provides
  transport security and access control.
- Keep the env files root-only. The JS sidecar and receiver run under separate
  Unix identities from token refresh, and their units make the private token
  paths inaccessible.
- The token refresh timer does not add newly granted scopes to an old token.
  Re-run Device Grant Flow after changing Integration permissions.
- The JS sidecar exits when forwarding retries are exhausted. Systemd restarts
  it; the bot must still use REST catch-up and message ID de-duplication to fill
  restart gaps.
- The account-bot stack is intentionally thin: no rule DSL or handler registry
  is baked into these units. Keep app-specific rule dispatch in the downstream
  bot service until a reusable shape is clear.
- If you replace the receiver or account bot with another service, update the
  target dependencies, the JS service `Requires=` and `After=` lines, and
  `WEBEX_SIDECAR_TARGET_URL` together. Point the bot at the same raw
  `WEBEX_ACCESS_TOKEN_FILE` published by the refresh timer if it needs to reply
  through REST, and keep `WEBEX_SIDECAR_MESSAGE_EVENTS=created` unless the
  replacement bot explicitly handles other message event types.
- The JS startup refresh is best-effort through `Wants=` / `After=` on the
  token-refresh service. `TimeoutStartSec=45s` bounds startup delay when Webex or
  the network hangs. If systemd kills a refresh after Webex has rotated the
  refresh token but before the local cache is saved, re-run Device Grant Flow.
