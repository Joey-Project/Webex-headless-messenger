# Changelog

## v0.1.0 - 2026-06-18

Initial public release of `webex-headless-messenger`.

- Added OAuth Integration helpers for Authorization Code, Device Grant, PKCE,
  refresh-token flows, token providers, and file-backed token cache workflows.
- Added typed async Webex Messaging REST bindings for people, rooms, messages,
  direct messages, memberships, webhooks, pagination, structured API errors, and
  retry metadata.
- Added polling receivers, joined-room discovery, and multi-room REST catch-up
  primitives for generic-account automations without public ingress.
- Added append-only JSONL state storage for processed message IDs, room
  checkpoints, and attempt leases, plus an optional rebuildable SQLite cache.
- Added local file upload support for message attachments.
- Added the `webex-headless` operator CLI for OAuth, room/message REST calls,
  polling, and loopback sidecar receiving.
- Added a JavaScript SDK realtime sidecar demo and the `webex-account-bot`
  integration binary for long-running generic-account bot deployments.
- Added Linux systemd templates for the receiver stack, account-bot stack, and
  shared token refresh timer.
