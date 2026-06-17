---
id: 20260617-multi-room-catchup
title: Multi-Room Catch-Up
status: completed
created: 2026-06-17
updated: 2026-06-17
branch: wip/multi-room-catchup
pr:
supersedes: 20260617-room-link-resolver
superseded_by:
---

# Multi-Room Catch-Up

## Summary
- Added joined-room discovery configuration and `discover_joined_rooms` for the authorized generic account.
- Added `MultiRoomMessagePoller` so services can poll every discovered joined room and receive `RoomMessage` values in deterministic chronological order.
- Added `RoomCheckpoint` and `MessagePoller::with_seen_message_ids` so durable state can seed restart catch-up without replaying previously processed messages.

## Current State
- A long-running account bot can combine the JS SDK realtime sidecar for low-latency events with multi-room REST catch-up for restart/offline recovery.
- New rooms without checkpoints establish a first-poll baseline instead of replaying full history by default.
- Persistent correctness state is still owned by the next PR; this PR only adds the library primitives needed by that store.

## Follow-Up Plan
- PR 3: Add an append-only JSONL state log for processed messages, room checkpoints, and attempt leases, with rebuildable in-memory indexes.
- PR 4: Add an optional SQLite cache/index over the JSONL source of truth only if the first deployment needs faster lookups.
- Later bot-specific repo work: business rule matching, concrete handlers, and deployment wiring for the target service.

## Validation
- `cargo fmt --check`
- `cargo test --all-features`
- `cargo clippy --lib --all-features -- -D warnings`
- `cargo doc --no-deps --all-features`
- `git diff --check`
- `project_journal.py validate --repo /home/codex/Joey-Project/Webex-headless-messenger`
