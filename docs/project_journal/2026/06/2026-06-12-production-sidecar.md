---
id: 20260612-production-sidecar
title: Production Sidecar MVP
status: completed
created: 2026-06-12
updated: 2026-06-12
branch: wip/production-sidecar
pr:
supersedes:
  - 20260609-realtime-sidecar-demo
superseded_by:
---

# Production Sidecar MVP

## Summary
- Added a service-oriented sidecar path for long-running bot-like deployments.
- Added `webex-headless auth refresh` so a supervisor timer can proactively
  refresh the shared OAuth token cache without performing an unrelated REST call.
- Added receiver health checks through `sidecar receive --health-path`, defaulting
  to `GET /healthz` on the same loopback listener.
- Hardened the JS sidecar with token-file loading/reload, bounded forward
  retries, optional localhost health/ready/live endpoints, and a config
  validation mode that does not load the Webex SDK.
- Documented the recommended reliability model: realtime sidecar for low latency,
  REST catch-up polling plus message ID de-duplication for recovery.

## Current State
- The sidecar remains a JS SDK bridge rather than native Rust Mercury.
- The long-running service model is viable for a bot service when run under a
  supervisor with a shared token file and REST catch-up.
- Forwarding is not durable. If the receiver remains unavailable after retries,
  the JS sidecar exits and the supervisor should restart it.

## Next Steps
- Add a durable local queue only for deployments that cannot rely on REST
  catch-up and de-duplication after restart gaps.
- Keep native Rust Mercury/WebSocket support deferred until there is a stable
  public protocol boundary worth binding.

## Validation
- `git diff --check`
- `cargo fmt --check`
- `cargo test --all-features`
- `node --check examples/sidecar-js/index.mjs`
- JS sidecar empty `WEBEX_SIDECAR_MESSAGE_EVENTS` rejection check.
- JS sidecar config validation with a local fake `TokenSet` JSON.
- `npm --prefix examples/sidecar-js run mock` against `webex-headless sidecar receive --max-events 1`.
