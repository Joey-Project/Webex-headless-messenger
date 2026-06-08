---
id: 20260605-webex-smoke-docs
title: Webex Smoke Testing Docs
status: completed
created: 2026-06-05
updated: 2026-06-05
branch: wip/webex-smoke-docs
pr:
supersedes: []
superseded_by:
---

# Webex Smoke Testing Docs

## Summary
- Added a focused smoke-testing guide for real Webex generic-account validation.
- Documented Webex Integration setup, Device Grant helper redirect URIs, minimal
  Messaging scopes, `.env.webex-test`, room resolution, expected smoke output,
  token cache handling, and troubleshooting.
- Synced the Device Grant redirect URI list with the current Webex Login docs,
  including `oauth-helper-d`.
- Linked the guide from README while keeping README as a concise crate overview.

## Current State
- The first live-account smoke TODO is complete as documentation.
- Active follow-up backlog is now split into simple file upload helper work,
  realtime sidecar evaluation, and deferred Adaptive Card builder work.

## Next Steps
- Implement the local file upload multipart helper as the next simple slice from
  latest `master`.

## Evidence
- Official Webex Login with Webex / Device Grant docs:
  `https://developer.webex.com/docs/login-with-webex`
