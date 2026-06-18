---
id: 20260617-jsonl-state-store
title: JSONL State Store
status: completed
created: 2026-06-17
updated: 2026-06-18
branch: wip/embedded-store
pr: https://github.com/Joey-Project/Webex-headless-messenger/pull/12
supersedes: 20260617-multi-room-catchup
superseded_by:
---

# JSONL State Store

## Summary
- Added `JsonlStateStore` as an append-only JSONL correctness store for long-running generic-account automations.
- Added `StateSnapshot` rebuild support for processed message IDs, active attempt leases, and latest per-room `RoomCheckpoint` values.
- Added simple attempt APIs so handlers can begin processing and receive an `AttemptLease` owner token required to release, defer, or mark the message processed.
- Added torn trailing-record recovery for interrupted appends, plus in-process path locking and reload-before-append behavior to avoid stale store handles claiming the same lease.
- Documented how to pair the store with `MultiRoomMessagePoller` checkpoints and strict recovery behavior.

## Current State
- Processed message IDs are the JSONL source of truth and are intentionally not capped in this first persistence layer.
- Room checkpoints can be loaded from `state.snapshot().room_checkpoints().cloned()` and saved after successful batch handling.
- Attempt leases survive restart until expiry and owner tokens prevent non-owner handles from releasing or shortening another active attempt.
- One JSONL file is intended for one OS-process writer unless the caller adds an external lock or later database-backed layer.

## Follow-Up Plan
- Add an optional SQLite cache/index over the JSONL source of truth if lookup speed becomes a real deployment bottleneck.
- Build the thin production generic-account bot layer around these primitives with configurable rule dispatch and app-specific handler tests.

## Validation
- `cargo fmt --check`
- `git diff --check`
- `cargo clippy --lib --all-features -- -D warnings`
- `cargo test --all-features`
- `cargo doc --no-deps --all-features`
- `project_journal.py validate --repo /home/codex/Joey-Project/Webex-headless-messenger`
