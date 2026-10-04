---
id: 20261004-cg001
title: Codex Review Gate V2 Consumer Migration
status: active
created: 2026-10-04
updated: 2026-10-04
branch: codex/install-review-gate-v2-20261004
pr:
supersedes: []
superseded_by:
---

# Codex Review Gate V2 Consumer Migration

## Summary
- Replace the dedicated v1 review-gate producer with the canonical v2 verifier and controller for default-branch pull requests.

## Current State
- The v2 verifier and controller match the canonical consumer templates; the verifier requests `actions: read` and the canonical author-permission setting.
- The controller and `.github/CODEOWNERS` protect the workflow control plane under `@JoeyTeng`; the release workflow remains unchanged.
- Repository ruleset transitions and any `release/*` policy reconciliation remain coordinator-owned and are not part of this consumer installation.

## Next Steps
- Complete the separately owned repository ruleset/status transition and release-branch policy reconciliation after this consumer change lands.

## Evidence
- Canonical consumer installation runbook: `JoeyTeng/codex-review-gate` `docs/install/agent.md`.
