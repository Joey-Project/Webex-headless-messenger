---
id: 20260617-room-link-resolver
title: Room Link Resolver
status: completed
created: 2026-06-17
updated: 2026-06-17
branch: wip/room-link-resolver
pr:
supersedes:
superseded_by:
---

# Room Link Resolver

## Summary
- Added a shared `room_id_candidates_from_link` helper in the library layer.
- The helper preserves existing Webex room/browser link candidates and derives REST room IDs from `webexteams://im?space=<uuid>` app links by encoding the canonical room URI form.
- Updated the CLI `rooms resolve --link` path and the smoke example to use the shared helper instead of maintaining separate parsers.

## Current State
- `.env.webex-test` can use the Webex app `WEBEX_TEST_ROOM_LINK` directly for smoke tests.
- The long-running generic-account bot path has a validated live stack: OAuth token file, JS SDK realtime sidecar, loopback account bot receiver, REST message hydration, and thread replies.
- The remaining production support gap is mostly packaging and recovery behavior, not basic read/reply capability.

## Delivery Plan
- PR 1: Land shared room-link resolution and record the generic-account bot support-layer plan.
- PR 2: Add joined-room discovery plus multi-room REST catch-up primitives so a long-running account bot can recover restart/offline gaps across every joined space.
- PR 3: Add an append-only JSONL state log for correctness state such as processed messages, room checkpoints, and attempt leases, with rebuildable in-memory indexes.
- PR 4: Add an optional SQLite cache/index over the JSONL source of truth if the first deployment needs faster `is_processed` and checkpoint lookups.
- Later bot-specific repo work: business rule matching, concrete handlers, and deployment wiring for the target service.

## Next Steps
- Build PR 2 from latest `master` after PR 1 merges.
- Keep production rule dispatch in the downstream bot unless a reusable runtime primitive becomes clear from the first deployment.
- Decide during PR 3 whether flat-file state remains sufficient for the first deployment or whether JSONL should become the default correctness store.

## Validation
- `cargo fmt`
- `cargo test derives_rest_room_id_from_webexteams_space_uuid --all-features`
- `cargo test parses_room_link_candidates_from_query_and_fragment --all-features`
- `cargo test --bin webex-headless --all-features`
- `cargo test --example smoke --all-features`
- Live Webex resolve: `rooms resolve --link "$WEBEX_TEST_ROOM_LINK"` returned `source=link` and resolved `Webex Headless Messenger Test`.
