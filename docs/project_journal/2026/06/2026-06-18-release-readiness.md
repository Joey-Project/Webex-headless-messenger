---
id: 20260618-release-readiness
title: Release Readiness
status: completed
created: 2026-06-18
updated: 2026-06-18
branch: wip/release-readiness
pr: https://github.com/Joey-Project/Webex-headless-messenger/pull/15
supersedes: 20260618-thin-production-bot-layer
superseded_by:
---

# Release Readiness

## Summary
- Reset the first public crate release target to `0.1.0`.
- Switched repository licensing to Apache-2.0 and added the standard `LICENSE`
  file.
- Added crates.io metadata for repository, homepage, docs.rs documentation,
  keywords, categories, and README.
- Added release-facing README guidance for installation, production bot
  integration, direct tag publishing, and license.
- Added `CHANGELOG.md` with initial `v0.1.0` release notes.
- Added a `Release` GitHub Actions workflow that can publish from a pushed
  `vX.Y.Z` tag or from manual default-branch dispatch, validates duplicate and
  monotonic versions, runs Rust/JS/systemd gates, dry-runs crates.io publishing,
  waits for `crates-io` environment approval, revalidates the verified source
  commit and live release availability, checks that the crates.io token secret
  is configured before tag creation, creates the tag when needed, publishes or
  skips an already-published crate version, and creates a GitHub Release.

## Current State
- The release flow intentionally skips release candidates for now. A manual
  `Release` workflow dispatch from the repository default branch with tag
  `v0.1.0` is enough to run validation, wait for environment approval, create
  the annotated release tag, and publish.
- GitHub binary artifacts are intentionally deferred; this release publishes the
  library and packaged operator binaries through crates.io.
- Publishing requires a `crates-io` GitHub Environment with required reviewers
  and an environment secret named `CARGO_REGISTRY_TOKEN`; selected deployment
  refs should allow the repository default branch and tag `v*.*.*`.

## Follow-Up Plan
- After this work lands on the repository default branch, run the `Release`
  workflow from that branch with tag `v0.1.0`, approve the `crates-io`
  environment, and let the workflow create the tag and publish.
- If `webex-headless` or `webex-account-bot` becomes a standalone install target,
  add multi-platform GitHub release artifacts similar to the BBDown-rust
  pipeline.

## Validation
- `git diff --check`
- `actionlint .github/workflows/release.yml`
- `bash -n /tmp/webex-release-workflow-scripts/*.sh`
- `shellcheck -s bash /tmp/webex-release-workflow-scripts/*.sh`
- `project_journal.py validate --repo /home/codex/Joey-Project/Webex-headless-messenger`
- `cargo fmt --check`
- `cargo build --all-targets --all-features --locked`
- `cargo clippy --all-targets --all-features --locked -- -D warnings`
- `cargo test --locked`
- `cargo test --all-features --locked`
- `cargo doc --locked --no-deps --all-features`
- `node --check examples/sidecar-js/index.mjs`
- `npm test` in `examples/sidecar-js`
- `systemd-analyze verify --root=.codex-tmp/systemd-verify-root ...`
- `cargo publish --dry-run --locked --allow-dirty`
