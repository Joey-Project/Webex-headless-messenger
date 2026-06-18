---
id: 20260618-thin-production-bot-layer
title: Thin Production Bot Layer
status: completed
created: 2026-06-18
updated: 2026-06-18
branch: wip/thin-production-bot-layer
pr: https://github.com/Joey-Project/Webex-headless-messenger/pull/14
supersedes: 20260617-sqlite-state-cache
superseded_by:
---

# Thin Production Bot Layer

## Summary
- Added `webex-account-bot` as a named binary target backed by `examples/account_bot.rs`.
- Added an integrated account-bot systemd stack: bot service, bot-specific JS sidecar unit, target, and production env example.
- Updated deployment docs to install both Rust binaries, create the service identity and state directory, and choose either the receiver stack or account-bot stack.

## Current State
- The account bot remains intentionally thin: sidecar HTTP events, room/self filtering, token-file reload, durable processed-message state, bounded concurrency, and at-most-once reply handling.
- App-specific rule dispatch stays in downstream bot code until a reusable shape proves itself.
- The account-bot stack uses the same token refresh timer and group-readable raw access-token publication model as the JS sidecar.

## Follow-Up Plan
- Build configurable rule dispatch in the downstream bot first, then promote only the reusable parts back into the crate.
- Consider wiring `SqliteStateCache` into long-running bot deployments only if JSONL lookup pressure shows up in real operation.

## Validation
- `cargo fmt --check`
- `git diff --check`
- `node --check examples/sidecar-js/index.mjs`
- `python3 <project-journal-skill>/scripts/project_journal.py validate --repo /home/codex/Joey-Project/Webex-headless-messenger`
- `cargo build --bin webex-account-bot --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test`
- `cargo test --all-features`
- `cargo doc --no-deps --all-features`
- Mock E2E: `webex-account-bot` in `WEBEX_ACCOUNT_BOT_MOCK=1` mode accepted a JS sidecar `WEBEX_SIDECAR_MOCK_EVENT=1` forward on loopback, returned HTTP 200, emitted `mock_replied`, and persisted `mock-message`.
- `systemd-analyze verify --root=.codex-tmp/systemd-verify-root ...` for receiver stack, account-bot stack, and token refresh units.
