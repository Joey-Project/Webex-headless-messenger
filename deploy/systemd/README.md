# Systemd Supervisor Example

This directory contains example system units for running the realtime sidecar
stack as a long-lived Linux service. Treat these files as deployment templates:
review paths, users, restart policy, and hardening settings before installing
them on a host.

The example manages three pieces:

- `webex-headless-sidecar-receiver.service`: loopback Rust receiver that accepts
  forwarded sidecar events and exposes `/healthz`.
- `webex-headless-sidecar-js.service`: Webex JavaScript SDK listener that
  forwards message events to the receiver and exposes `/readyz` / `/livez`.
- `webex-headless-token-refresh.service` / `.timer`: startup and periodic
  OAuth token refresh.

The bundled receiver writes accepted events to journald as JSON Lines. For a real
automation, replace that unit with your bot service or make your bot consume the
receiver output. Keep the same forwarding token, loopback binding, health checks,
and token-refresh timer. OAuth credentials and the refresh-token cache stay
under the token-refresh identity. The token-refresh service publishes a separate
raw access-token file for the JS sidecar, and the receiver runs under an identity with no OAuth token
file access.

## Assumed Layout

```text
/usr/local/bin/webex-headless
/opt/webex-headless-messenger/examples/sidecar-js/index.mjs
/etc/webex-headless/webex-headless.env
/etc/webex-headless/webex-headless-receiver.env
/etc/webex-headless/webex-headless-token.env
/etc/webex-headless/webex-client-secret
/var/lib/webex-headless-token/token.json
/var/lib/webex-headless-access/access-token
```

## Install

Build and install the Rust CLI:

```bash
cargo build --release --bin webex-headless
sudo install -m 0755 target/release/webex-headless /usr/local/bin/webex-headless
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
sudo groupadd --system webex-headless-token
sudo useradd --system --gid webex-headless-sidecar --home /var/lib/webex-headless-access --shell /usr/sbin/nologin webex-headless-sidecar
sudo useradd --system --gid webex-headless-receiver --home /nonexistent --shell /usr/sbin/nologin webex-headless-receiver
sudo useradd --system --gid webex-headless-token --home /var/lib/webex-headless-token --shell /usr/sbin/nologin webex-headless-token
sudo install -d -o webex-headless-token -g webex-headless-token -m 0700 /var/lib/webex-headless-token
sudo install -d -o webex-headless-token -g webex-headless-sidecar -m 2750 /var/lib/webex-headless-access
sudo install -d -o root -g root -m 0750 /etc/webex-headless
sudo install -o root -g webex-headless-token -m 0640 /dev/null /etc/webex-headless/webex-client-secret
```

Install the env templates and keep them readable only by root. Put the OAuth
client ID and secret file path in `webex-headless-token.env`, and put the secret
value itself in `/etc/webex-headless/webex-client-secret`. The token-refresh
identity can read that secret file, while the JS sidecar and receiver cannot.
Keep the JS sidecar and receiver env files limited to their runtime settings and
the local forwarding token:

```bash
sudo install -o root -g root -m 0600 \
  deploy/systemd/webex-headless.env.example \
  /etc/webex-headless/webex-headless.env
sudo install -o root -g root -m 0600 \
  deploy/systemd/webex-headless-receiver.env.example \
  /etc/webex-headless/webex-headless-receiver.env
sudo install -o root -g root -m 0600 \
  deploy/systemd/webex-headless-token.env.example \
  /etc/webex-headless/webex-headless-token.env
sudo editor /etc/webex-headless/webex-headless.env
sudo editor /etc/webex-headless/webex-headless-receiver.env
sudo editor /etc/webex-headless/webex-headless-token.env
sudo editor /etc/webex-headless/webex-client-secret
```

Install the units:

```bash
sudo install -m 0644 deploy/systemd/webex-headless-sidecar.target /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-sidecar-receiver.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-sidecar-js.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-token-refresh.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/webex-headless-token-refresh.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

Bootstrap the initial token file after setting the Integration client ID, secret
file path, secret file contents, and scopes. Load the protected env file through a
transient systemd service; the CLI reads the client secret from the secret file so
the secret value is not placed on the command line or in the process environment:

```bash
sudo systemd-run --wait --collect --pty \
  --uid=webex-headless-token \
  --gid=webex-headless-token \
  --property=EnvironmentFile=/etc/webex-headless/webex-headless-token.env \
  /usr/local/bin/webex-headless auth device \
    --token-file /var/lib/webex-headless-token/token.json \
    --access-token-file /var/lib/webex-headless-access/access-token \
    --scopes 'spark:all spark:kms'
```

Start the stack. The JS sidecar starts after a best-effort token refresh service
run. That refresh keeps the private TokenSet cache current and publishes the
raw access-token file consumed by the JS sidecar; if refresh fails or times out,
the sidecar uses the access token initially published by Device Grant or the
last token published by a later refresh:

```bash
sudo systemctl enable --now webex-headless-sidecar.target
sudo systemctl enable --now webex-headless-token-refresh.timer
```

## Verify

```bash
sudo systemctl status webex-headless-sidecar-receiver.service
sudo systemctl status webex-headless-sidecar-js.service
sudo systemctl status webex-headless-token-refresh.timer
curl -fsS http://127.0.0.1:8787/healthz
curl -fsS http://127.0.0.1:8788/readyz
curl -fsS http://127.0.0.1:8788/livez
```

Useful logs:

```bash
journalctl -u webex-headless-sidecar-receiver.service -f
journalctl -u webex-headless-sidecar-js.service -f
journalctl -u webex-headless-token-refresh.service
```

## Operations Notes

- Keep `WEBEX_SIDECAR_TOKEN` identical in the receiver and JS sidecar env files.
- Keep `WEBEX_ACCESS_TOKEN_FILE` identical in the JS sidecar and token-refresh env files.
- Keep `WEBEX_REFRESH_TOKEN_FILE` private to the token-refresh env file; it stores
  the full refreshable `TokenSet` cache.
- Keep `WEBEX_CLIENT_SECRET_FILE` private to the token-refresh env file, and keep
  the referenced secret file readable only by root and `webex-headless-token`.
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
- If you replace the receiver with a bot service, update the target dependencies,
  the JS service `Requires=` and `After=` lines, and
  `WEBEX_SIDECAR_TARGET_URL` together.
- The JS startup refresh is best-effort through `Wants=` / `After=` on the
  token-refresh service. `TimeoutStartSec=45s` bounds startup delay when Webex or
  the network hangs. If systemd kills a refresh after Webex has rotated the
  refresh token but before the local cache is saved, re-run Device Grant Flow.
