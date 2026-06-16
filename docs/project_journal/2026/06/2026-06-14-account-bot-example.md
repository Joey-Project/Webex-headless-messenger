---
id: 20260614-account-bot-example
title: Account Bot Example
status: completed
created: 2026-06-14
updated: 2026-06-15
branch: wip/account-bot-demo
pr: https://github.com/Joey-Project/Webex-headless-messenger/pull/8
supersedes:
  - 20260614-sidecar-systemd-supervisor
superseded_by:
---

# Account Bot Example

## Summary
- Added `examples/account_bot.rs`, a concrete generic-account bot example that accepts JS sidecar HTTP events directly.
- The example supports bearer-protected loopback forwarding, health checks, bounded concurrent HTTP handling, mock mode, optional room allowlists, self-message filtering, file-backed attempt/processed-message state, startup state-store verification, and REST replies.
- The REST client uses a reloading access-token provider for `WEBEX_ACCESS_TOKEN_FILE` / `WEBEX_TOKEN_FILE`, so token refresh timer output can be consumed without restarting the bot.
- Retryable Webex read/API errors preserve HTTP status and forward `Retry-After` to the JS sidecar where that is safe; explicit post-`create_message` 401/408/429/5xx API responses remain retryable instead of being marked processed, with upstream 401 token races mapped back to receiver HTTP 503.
- Attempt leases, busy listeners, handler timeouts, retryable Webex API failures, and post-reply state persistence failures now return coherent retryable `Retry-After` responses when the bot owns the HTTP response; max-event admission rejects excess new events after the configured terminal event count, and pre-send aborts defer the attempt lease without marking the message processed. Local post-send `create_message` outcomes that cannot confirm acceptance are recorded as at-most-once `reply_unknown` results to avoid duplicate replies. Duplicate retries against an existing attempt lease return the remaining persisted lease without rewriting state, stale in-memory attempts expire with their lease, and all attempt/state write failures covered by the example mark bot health degraded until volatile processed IDs are durably flushed. Max-event slots are committed only after terminal processed state is durably recorded, so state-persistence retries are not cut off by bounded runs. Max-event admission now rejects only event POSTs while keeping health checks responsive, and JSON token files reject blank `accessToken` values. Replies to messages already in a thread now preserve the original thread parent, unsupported sidecar events do not consume bounded-run message quota, and action JSON logging is bounded best-effort background work so stdout backpressure or broken pipes cannot change the HTTP outcome.
- The JS sidecar now queues forwards when active slots are full, releases slots while sleeping for retry backoff, clamps `Retry-After` delays to Node's safe timer range, and counts both active-slot waiters and retry-delayed payloads against the bounded `WEBEX_SIDECAR_MAX_QUEUED_FORWARDS` outstanding-forward budget.

## Current State
- The account bot is an example-level support layer, not a stabilized framework API.
- It covers the missing receiver/handler slice for a long-running ordinary Webex account bot.
- REST catch-up across all joined spaces and a durable local sidecar queue remain deferred until a concrete deployment needs those guarantees.

## Next Steps
- Promote stable pieces into a library bot runtime only after one real deployment validates the shape.
- Add room discovery plus multi-room REST catch-up if sidecar restart gaps become unacceptable.
- Add a production-specific systemd unit for the bot once the target bot binary/service name is chosen.

## Validation
- `cargo fmt --check`
- `node --check examples/sidecar-js/index.mjs`
- `node --test examples/sidecar-js/index.test.mjs`
- `cargo test --example account_bot --all-features` (60 account bot tests)
- `cargo test --all-features` (includes example tests via explicit Cargo targets)
- `git diff --check`
- Project journal validation passed.
- Mock E2E: `examples/account_bot.rs` in `WEBEX_ACCOUNT_BOT_MOCK=1` mode received a `WEBEX_SIDECAR_MOCK_EVENT=1` forward from `examples/sidecar-js/index.mjs`, returned HTTP 200, emitted `mock_replied`, and persisted `mock-message` in its processed-message state file.
