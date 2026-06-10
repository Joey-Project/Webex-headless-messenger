---
id: 20260609-realtime-sidecar-demo
title: Realtime Sidecar Demo
status: completed
created: 2026-06-09
updated: 2026-06-10
branch: wip/webex-sidecar-demo
pr: https://github.com/Joey-Project/Webex-headless-messenger/pull/4
supersedes: []
superseded_by:
---

# Realtime Sidecar Demo

## Summary
- Added `SidecarEvent` for normalized realtime event envelopes forwarded by a
  sidecar process.
- Added a Rust loopback HTTP receiver example and a Node.js Webex JavaScript SDK
  sidecar demo that forwards `messages.listen()` events.
- Documented local mock E2E, live Webex listener setup, forwarding-token usage,
  and the security boundary in `docs/realtime-sidecar.md`.
- Hardened receiver error handling so malformed local requests return valid JSON
  and per-connection write failures do not stop the accept loop.
- Enforced loopback defaults for the receiver bind address and JS forwarding
  target, with an explicit non-loopback override for secured deployments.

## Current State
- Realtime sidecar is a demo/bridge, not a native Rust Mercury implementation.
- Local mock E2E can validate the Rust receiver and JS forwarding protocol
  without Webex credentials or npm dependencies.
- Live sidecar operation uses direct Webex JS SDK core/messages plugin packages
  rather than the default `webex` bundle; the dependency tree remains demo-only
  and should be audited before production use.
- Live E2E passed after re-authorizing the generic account with `spark:all` and
  `spark:kms`; U2C postauth catalog returned 200 and the loopback receiver
  accepted two real `messages.created` events from a smoke message and reply.

## Next Steps
- Keep production hardening for the sidecar supervisor/deployment model separate
  from the crate's REST API surface.
- Native Rust Mercury/WebSocket support remains deferred until there is a stable
  public protocol boundary worth binding.

## Evidence
- Webex Browser SDK Messaging Quick Start:
  `https://developer.webex.com/docs/browser-sdk-messaging-tutorial`
- Webex websocket listener blog:
  `https://developer.webex.com/blog/using-websockets-with-the-webex-javascript-sdk`
- Webex JS SDK packages:
  `https://www.npmjs.com/package/@webex/webex-core`,
  `https://www.npmjs.com/package/@webex/plugin-messages`
- Local validation: `cargo test --all-features --all-targets`,
  `cargo clippy --all-features --all-targets -- -D warnings`,
  `cargo doc --no-deps --all-features`, JS `node --check`, mock sidecar E2E,
  aborted-connection receiver regression, fail-closed non-loopback checks,
  receiver parser/handler unit coverage, package-lock dependency check, and live
  sidecar E2E (`live_e2e_result=ok`).
