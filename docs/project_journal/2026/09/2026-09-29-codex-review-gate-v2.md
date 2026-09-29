---
id: 20260929-codex-review-gate-v2
title: Codex Review Gate v2 Consumer Migration
status: active
created: 2026-09-29
updated: 2026-09-29
branch: wip/v2-review-gate-consumer
pr:
supersedes: []
superseded_by:
---

# Codex Review Gate v2 Consumer Migration

## Summary

- Replace the v1 consumer workflow with the canonical v2 verifier and controller.
- Protect the workflow control plane through CODEOWNERS approval by @JoeyTeng.

## Current State

- The default branch has the canonical v2 consumer workflows after this change lands.
- The organization-level legacy required check remains active during the v2 canary and ruleset handoff.
- Release branches are outside the temporary Codex required-check scope until v2 release-base support is verified.

## Next Steps

- Exercise a separate v2 canary pull request and verify the exact-head check.
- Activate the v2 required check before retiring the legacy required check.

## Evidence

- Canonical consumer: `JoeyTeng/codex-review-gate-action@v2`.
