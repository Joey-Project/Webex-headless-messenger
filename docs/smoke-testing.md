# Webex Smoke Testing

This guide describes the manual smoke path for a real Webex generic account.
It is intended for local validation of OAuth Integration setup, Device Grant
authorization, and basic Messaging REST behavior.

Do not commit `.env.webex-test`, access tokens, refresh tokens, client secrets,
or smoke output that contains user or room identifiers. This repository ignores
`.env.*` and `.codex-tmp/` by default.

## Webex Integration

Create a Webex Integration in the Webex Developer Portal. For a headless generic
account smoke test, use these values:

```text
Will this integration use a mobile SDK?
No

App Hub Description
Headless Webex Messaging automation for a dedicated generic account. Reads
messages from spaces where the authorized account is a member and sends or
replies to messages for automation. Uses OAuth Device Grant Flow for local
bootstrap; no public webhook ingress is required.

Redirect URI(s)
https://oauth-helper-a.wbx2.com/helperservice/v1/actions/device/callback
https://oauth-helper-r.wbx2.com/helperservice/v1/actions/device/callback
https://oauth-helper-k.wbx2.com/helperservice/v1/actions/device/callback
https://oauth-helper-d.wbx2.com/helperservice/v1/actions/device/callback
```

The four OAuth helper redirect URIs are required for Webex Device Grant Flow. They
are hosted by Webex and are only used for the OAuth bootstrap.

Start with the Messaging scopes used by `DEFAULT_MESSAGING_SCOPES`:

```text
spark:messages_read
spark:messages_write
spark:rooms_read
spark:memberships_read
spark:people_read
spark:kms
```

Add these only if the application will mutate rooms or memberships:

```text
spark:rooms_write
spark:memberships_write
```

The generic account must already be a member of any space that the smoke test
reads or writes.

For realtime sidecar live testing, the Webex JS SDK `messages.listen()` path
requires broader Messaging SDK scopes. Enable `spark:all` in the Integration
configuration, confirm `spark:kms` is also enabled, remove the old token cache,
and re-authorize with an override:

```bash
WEBEX_TEST_SCOPES="spark:all spark:kms" \
  cargo run --example smoke --all-features
```

Refreshing an existing OAuth token does not add newly enabled scopes; re-run
Device Grant Flow after changing Integration permissions. `WEBEX_TEST_SCOPES` may
be set in the shell environment or in `.env.webex-test`.

## Local Environment

Create `.env.webex-test` in the repository root:

```text
WEBEX_CLIENT_ID=...
WEBEX_CLIENT_SECRET=...
WEBEX_TEST_ROOM_ID=
WEBEX_TEST_ROOM_LINK=...
WEBEX_TEST_ROOM_TITLE=...
WEBEX_TEST_PERSON_EMAIL=...
```

Room resolution uses the first available value:

- `WEBEX_TEST_ROOM_ID`: exact Webex room ID.
- `WEBEX_TEST_ROOM_LINK`: Webex app or browser link. The smoke example extracts
  room-like candidates, derives REST room IDs from `webexteams://im?space=<uuid>`
  links, and verifies them with `GET /v1/rooms/{id}`.
- `WEBEX_TEST_ROOM_TITLE`: unique room title visible to the generic account.

`WEBEX_TEST_PERSON_EMAIL` is optional. When present, the smoke example attempts
to list direct messages for that person. Some Webex orgs or accounts may return
`403`; the example reports that direct-message smoke as skipped.

## Run Device Grant Smoke

Run the example:

```bash
cargo run --example smoke --all-features
```

On the first run, the example prints a verification URL and a user code:

```text
token_cache=miss
verification_uri=https://...
user_code=....
verification_uri_complete=https://...
```

Open `verification_uri_complete` if present. Otherwise open
`verification_uri`, enter `user_code`, sign in as the generic account, and
approve the requested scopes. The CLI polls until Webex returns tokens.

Successful output includes:

```text
token_cache=stored
authorized_as=...
room_id_resolved=true
membership_page_count=...
message_page_count=...
message_created=true
reply_created=true
```

The smoke message and reply are real Webex messages in the selected test room.
Use a dedicated low-noise room for repeated validation.

## Token Cache

On Unix, the example stores the token set at:

```text
.codex-tmp/webex-smoke/token.json
```

The cache file is created with owner-only permissions and is rechecked before
each read. On non-Unix platforms, persistent token caching is disabled by the
example.

Remove the cache when switching integrations, scopes, or generic accounts:

```bash
rm .codex-tmp/webex-smoke/token.json
```

## Troubleshooting

- `invalid_client`: verify `WEBEX_CLIENT_ID`, `WEBEX_CLIENT_SECRET`, and that the
  integration includes the Device Grant redirect URIs.
- `access_denied`: authorize with the generic account and approve all requested
  scopes.
- `authorization_pending`: normal while the CLI waits for browser approval.
- `slow_down`: normal Webex rate feedback; the example increases the polling
  interval.
- `no room matched WEBEX_TEST_ROOM_TITLE`: ensure the generic account is a room
  member or set `WEBEX_TEST_ROOM_ID`.
- `WEBEX_TEST_ROOM_TITLE matched ... rooms`: use `WEBEX_TEST_ROOM_ID` or a more
  unique smoke room title.
- `direct_message_smoke=skipped status=403`: the account or org does not allow
  that direct-message read path; room read/send/reply smoke can still pass.

## References

- Webex Login with Webex and Device Grant Flow:
  <https://developer.webex.com/docs/login-with-webex>
- Webex Messaging REST API:
  <https://developer.webex.com/docs/api/v1/messages>
