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
and token-refresh timer. The receiver uses a separate env file and does not need
OAuth credentials or token-file access.

## Assumed Layout

```text
/usr/local/bin/webex-headless
/opt/webex-headless-messenger/examples/sidecar-js/index.mjs
/etc/webex-headless/webex-headless.env
/etc/webex-headless/webex-headless-receiver.env
/var/lib/webex-headless/token.json
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

Create a locked-down service user and directories:

```bash
sudo useradd --system --home /var/lib/webex-headless --shell /usr/sbin/nologin webex-headless
sudo install -d -o webex-headless -g webex-headless -m 0750 /var/lib/webex-headless
sudo install -d -o root -g webex-headless -m 0750 /etc/webex-headless
```

Install the env templates, edit them, and keep them readable only by root and
the service group. Put OAuth credentials only in `webex-headless.env`; keep the
receiver file limited to receiver settings and the local forwarding token:

```bash
sudo install -o root -g webex-headless -m 0640 \
  deploy/systemd/webex-headless.env.example \
  /etc/webex-headless/webex-headless.env
sudo install -o root -g webex-headless -m 0640 \
  deploy/systemd/webex-headless-receiver.env.example \
  /etc/webex-headless/webex-headless-receiver.env
sudo editor /etc/webex-headless/webex-headless.env
sudo editor /etc/webex-headless/webex-headless-receiver.env
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

Bootstrap the initial token file after setting the Integration credentials and
scopes in `/etc/webex-headless/webex-headless.env`. Load that protected env file
through a transient systemd service so the client secret is not placed on the
command line:

```bash
sudo systemd-run --wait --collect --pty \
  --uid=webex-headless \
  --gid=webex-headless \
  --property=EnvironmentFile=/etc/webex-headless/webex-headless.env \
  /usr/local/bin/webex-headless auth device \
    --token-file /var/lib/webex-headless/token.json \
    --scopes 'spark:all spark:kms'
```

Start the stack. The JS sidecar makes a best-effort startup token refresh before
validating its config and listening, uses the cached token if that refresh fails,
and the timer keeps the same token file fresh afterward:

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
- Keep every bind and target URL on loopback unless another layer provides
  transport security and access control.
- The token refresh timer does not add newly granted scopes to an old token.
  Re-run Device Grant Flow after changing Integration permissions.
- The JS sidecar exits when forwarding retries are exhausted. Systemd restarts
  it; the bot must still use REST catch-up and message ID de-duplication to fill
  restart gaps.
- If you replace the receiver with a bot service, update the target dependencies,
  the JS service `Requires=` line, and `WEBEX_SIDECAR_TARGET_URL`
  together.
- The JS startup refresh is best-effort so a transient Webex or network outage
  does not block startup when the cached token is still usable. The timer keeps
  retrying refresh in the background.
