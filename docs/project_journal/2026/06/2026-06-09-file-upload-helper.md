---
id: 20260609-file-upload-helper
title: File Upload Helper
status: completed
created: 2026-06-09
updated: 2026-06-09
branch: wip/webex-file-upload
pr:
supersedes: []
superseded_by:
---

# File Upload Helper

## Summary
- Added `LocalFileAttachment` and `WebexClient::create_message_with_file` for one
  local filesystem attachment via `multipart/form-data` on `POST /v1/messages`.
- Kept public file URL support on the existing JSON `CreateMessage.files` path.
- Added local regular-file and 100 MiB size guards before reading attachment
  bytes into the request body.
- Documented the minimal file upload path in README.

## Current State
- File upload helper is implemented with request construction coverage.
- Realtime sidecar work remains a separate follow-up branch and PR.
- Adaptive Cards remain deferred as raw JSON attachment payloads.

## Next Steps
- Split realtime sidecar bridge and live E2E into the next PR after this lands.

## Evidence
- Webex REST API basics local file upload guidance:
  `https://developer.webex.com/docs/rest-api-basics`
