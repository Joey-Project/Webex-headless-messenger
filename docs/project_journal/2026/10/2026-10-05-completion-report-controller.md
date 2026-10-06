---
id: 20261005-completion-report-controller
title: Completion Report Controller Update
status: completed
created: 2026-10-05
updated: 2026-10-05
branch: codex/completion-report-v217
pr:
supersedes: []
superseded_by:
---

# Completion Report Controller Update

## Summary

- Align the installed v2 controller with the completion-controller hardening template.

## Current State

- Successful and otherwise non-requestable verifier completions use the metadata-only `report-completion` route; automatic review requests remain limited to a first-attempt verifier failure, one associated pull request, and `CODEX_REVIEW_GATE_AUTO_REQUEST=true`.
- Completion reporting does not request review, rerun or reconcile findings, or replace the verifier's check as gate authority.
- Preserve PR, issue, or dispatch identifiers in the concurrency key when available; otherwise use `workflow_run.id` and then `github.run_id` so unassociated events do not share a missing-number key.
- Accept verifier workflow completions only for the exact workflow path or `.github/workflows/codex-review-gate.yml@refs/pull/<number>/merge`; arbitrary ref suffixes are not sufficient path evidence.
- This controller-only update does not change CODEOWNERS, verifier behavior, runner policy or capacity, concurrency cancellation behavior, workflow permissions, or repository rulesets.
- The separately tracked repository ruleset and release-branch policy work remains outside this controller update.

## Evidence

- Target base: `da1d849d3256bf57c2520ae01676bd5caef835d5`.
- Canonical controller blob: `c6290c800903303151cbfb34ca706463118b0d09`.
