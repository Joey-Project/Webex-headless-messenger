---
id: 20260617-sqlite-state-cache
title: SQLite State Cache
status: completed
created: 2026-06-17
updated: 2026-06-17
branch: wip/sqlite-state-cache
pr:
supersedes: 20260617-jsonl-state-store
superseded_by:
---

# SQLite State Cache

## Summary
- Added the optional `sqlite-state-cache` feature with `SqliteStateCache` as a rebuildable SQLite index over the JSONL state source of truth.
- Indexed processed message IDs and room checkpoints for faster lookup without changing JSONL write correctness semantics.
- Added rebuild APIs from `JsonlStateStore`, `StateSnapshot`, or a JSONL path, plus feature-gated tests for index rebuild and stale-index replacement.
- Hardened SQLite cache paths against URI-style opens, symlink targets, unsafe parent directories, unsafe current directories for bare relative filenames, and untrusted Unix file owners.

## Current State
- JSONL remains the correctness source of truth; SQLite is only an acceleration layer and can be rebuilt at any time.
- The cache intentionally does not store attempt owner tokens or active leases. Attempt ownership continues to flow only through `JsonlStateStore` APIs.
- The feature is opt-in so default crate consumers do not pull SQLite or bundled libsqlite.

## Follow-Up Plan
- Build the thin production generic-account bot layer around these primitives with configurable rule dispatch and app-specific handler tests.

## Validation
- `cargo fmt --check`
- `git diff --check`
- `project_journal.py validate --repo /home/codex/Joey-Project/Webex-headless-messenger`
- `cargo clippy --lib --all-features -- -D warnings`
- `cargo test`
- `cargo test --all-features`
- `cargo doc --no-deps --all-features`
