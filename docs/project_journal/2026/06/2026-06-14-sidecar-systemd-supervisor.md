---
id: 20260614-sidecar-systemd-supervisor
title: Sidecar Systemd Supervisor Templates
status: completed
created: 2026-06-14
updated: 2026-06-14
branch: wip/sidecar-supervisor
pr:
supersedes:
  - 20260612-production-sidecar
superseded_by:
---

# Sidecar Systemd Supervisor Templates

## Summary
- Added Linux systemd deployment templates for the realtime sidecar stack.
- The templates cover the Rust sidecar receiver, JS SDK sidecar, token refresh
  service/timer, shared target, root-only environment file examples,
  split service identities, access-token-only publication, and private refresh-token storage.
- Added deployment docs with install, token bootstrap, start, health check, and
  log inspection commands.
- Token refresh uses a client-secret file path instead of placing the secret
  value in the systemd environment.

## Current State
- The repo now has a concrete Linux supervisor example under `deploy/systemd/`.
- The bundled receiver unit is an operational sample. A production bot can
  replace it while keeping the JS sidecar, token refresh timer, and loopback
  forwarding contract.
- Durable local queue and native Mercury remain deferred; recovery still relies
  on supervisor restart, REST catch-up, and message ID de-duplication.

## Next Steps
- Add platform-specific launchd/container templates only when an actual target
  deployment needs them.
- Add automated verification around unit-file syntax and health endpoint
  behavior if the systemd templates become part of CI.

## Validation
- `cargo fmt --check`
- `git diff --check`
- `cargo test --all-features`
- JS sidecar config validation with `WEBEX_SIDECAR_VALIDATE_CONFIG=1`
- `systemd-analyze verify --root=.codex-tmp/systemd-verify-root` with dummy
  base targets and placeholder executables for the installed paths.
