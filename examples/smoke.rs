use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(unix)]
use std::{
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
};

use tokio::time::sleep;
use webex_headless_messenger::{
    DeviceTokenStatus, OAuthClient, OAuthConfig, TokenSet, WebexClient,
    room_id_candidates_from_link,
    types::{CreateMessage, ListDirectMessages, ListMemberships, ListMessages, ListRooms, Room},
};

const ENV_FILE: &str = ".env.webex-test";
const TOKEN_FILE: &str = ".codex-tmp/webex-smoke/token.json";

#[tokio::main(flavor = "current_thread")]
async fn main() -> webex_headless_messenger::Result<()> {
    let env = read_env(ENV_FILE)?;
    let client_id = required(&env, "WEBEX_CLIENT_ID")?;
    let client_secret = required(&env, "WEBEX_CLIENT_SECRET")?;
    let room_id = optional(&env, "WEBEX_TEST_ROOM_ID");
    let room_link = optional(&env, "WEBEX_TEST_ROOM_LINK");
    let room_title = optional(&env, "WEBEX_TEST_ROOM_TITLE");
    let person_email = optional(&env, "WEBEX_TEST_PERSON_EMAIL");
    let scope_override = std::env::var("WEBEX_TEST_SCOPES")
        .ok()
        .or_else(|| optional(&env, "WEBEX_TEST_SCOPES").map(ToOwned::to_owned));

    let mut config = OAuthConfig::new(client_id)?.with_client_secret(client_secret.to_owned());
    if let Some(scopes) = scope_override
        .as_deref()
        .map(parse_scopes)
        .filter(|scopes| !scopes.is_empty())
    {
        println!("scope_override_count={}", scopes.len());
        config = config.with_scopes(scopes);
    }
    let requested_scopes = config.scopes.clone();
    let oauth = OAuthClient::new(config);
    let token = load_or_authorize(&oauth, &requested_scopes).await?;
    let client = WebexClient::builder()?
        .token_provider(std::sync::Arc::new(
            webex_headless_messenger::StaticTokenProvider::new(token.access_token),
        ))
        .build()?;

    let me = client.me().await?;
    println!(
        "authorized_as={}",
        me.display_name
            .or_else(|| me.emails.first().cloned())
            .unwrap_or_else(|| "<unknown>".to_owned())
    );

    let room_id = resolve_room_id(&client, room_id, room_link, room_title).await?;
    println!("room_id_resolved=true");

    let memberships = client
        .list_memberships(&ListMemberships {
            room_id: Some(room_id.clone()),
            max: Some(10),
            ..ListMemberships::default()
        })
        .await?;
    println!("membership_page_count={}", memberships.items.len());

    let messages = client
        .list_messages(&ListMessages {
            max: Some(10),
            ..ListMessages::room(&room_id)
        })
        .await?;
    println!("message_page_count={}", messages.items.len());

    let smoke = client
        .create_message(&CreateMessage::text(
            &room_id,
            "webex-headless-messenger smoke test",
        ))
        .await?;
    let smoke_id = smoke.id.clone().ok_or_else(|| {
        webex_headless_messenger::Error::Other("message response had no id".into())
    })?;
    println!("message_created=true");

    client
        .reply_text(&room_id, &smoke_id, "smoke test reply")
        .await?;
    println!("reply_created=true");

    if let Some(email) = person_email {
        match client
            .list_direct_messages(&ListDirectMessages {
                person_email: Some(email.to_owned()),
                ..ListDirectMessages::default()
            })
            .await
        {
            Ok(direct) => println!("direct_message_page_count={}", direct.items.len()),
            Err(webex_headless_messenger::Error::Api(api)) => {
                println!("direct_message_smoke=skipped status={}", api.status);
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

async fn load_or_authorize(
    oauth: &OAuthClient,
    requested_scopes: &[String],
) -> webex_headless_messenger::Result<TokenSet> {
    let token_path = PathBuf::from(TOKEN_FILE);
    if let Some(contents) = read_token_cache(&token_path)? {
        let token = serde_json::from_str::<TokenSet>(&contents)?;
        if !token.is_expiring_within(Duration::from_secs(300)) {
            if token_has_requested_scopes(&token, requested_scopes) {
                println!("token_cache=hit");
                return Ok(token);
            }
            println!("token_cache=scope_miss");
        }
    }

    println!("token_cache=miss");
    let auth = oauth.start_device_authorization().await?;
    println!("verification_uri={}", auth.verification_uri);
    println!("user_code={}", auth.user_code);
    if let Some(complete) = &auth.verification_uri_complete {
        println!("verification_uri_complete={complete}");
    }

    let mut interval = Duration::from_secs(auth.interval.unwrap_or(5));
    loop {
        match oauth.poll_device_token(&auth.device_code).await? {
            DeviceTokenStatus::Authorized(token) => {
                let serialized = serde_json::to_string_pretty(&token)?;
                if write_token_cache(&token_path, &serialized)? {
                    println!("token_cache=stored");
                } else {
                    println!("token_cache=disabled platform=non_unix");
                }
                return Ok(token);
            }
            DeviceTokenStatus::Pending { retry_after } => {
                sleep(retry_after.unwrap_or(Duration::ZERO).max(interval)).await;
            }
            DeviceTokenStatus::SlowDown { retry_after } => {
                interval += Duration::from_secs(5);
                sleep(retry_after.unwrap_or(Duration::ZERO).max(interval)).await;
            }
        }
    }
}

async fn resolve_room_id(
    client: &WebexClient,
    explicit_room_id: Option<&str>,
    room_link: Option<&str>,
    room_title: Option<&str>,
) -> webex_headless_messenger::Result<String> {
    if let Some(room_id) = explicit_room_id.filter(|value| !value.trim().is_empty()) {
        return Ok(room_id.to_owned());
    }

    let candidates = room_link
        .map(room_id_candidates_from_link)
        .unwrap_or_default();
    for candidate in candidates {
        if client.get_room(&candidate).await.is_ok() {
            return Ok(candidate);
        }
    }

    let Some(title) = room_title.filter(|value| !value.trim().is_empty()) else {
        return Err(webex_headless_messenger::Error::Other(
            "set WEBEX_TEST_ROOM_ID or WEBEX_TEST_ROOM_TITLE".to_owned(),
        ));
    };

    let first = client
        .list_rooms(&ListRooms {
            max: Some(100),
            ..ListRooms::default()
        })
        .await?;
    let rooms = client.collect_all::<Room>(first).await?;
    let mut matches = rooms
        .into_iter()
        .filter(|room| room.title.as_deref() == Some(title))
        .filter_map(|room| room.id)
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();

    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(webex_headless_messenger::Error::Other(format!(
            "no room matched WEBEX_TEST_ROOM_TITLE={title:?}"
        ))),
        count => Err(webex_headless_messenger::Error::Other(format!(
            "WEBEX_TEST_ROOM_TITLE={title:?} matched {count} rooms; set WEBEX_TEST_ROOM_ID"
        ))),
    }
}

fn token_has_requested_scopes(token: &TokenSet, requested_scopes: &[String]) -> bool {
    requested_scopes.iter().all(|requested| {
        token
            .scopes
            .iter()
            .any(|actual| scope_matches(actual, requested))
    })
}

fn scope_matches(actual: &str, requested: &str) -> bool {
    actual == requested
        || (actual == "spark:all" && requested.starts_with("spark:") && requested != "spark:kms")
}

fn parse_scopes(value: &str) -> Vec<String> {
    value
        .split(|ch: char| ch.is_ascii_whitespace() || ch == ',')
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(unix)]
fn read_token_cache(path: &Path) -> webex_headless_messenger::Result<Option<String>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    validate_owned_regular_file(path, &metadata)?;
    if let Some(parent) = path.parent() {
        harden_owned_directory(parent)?;
    }

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    validate_owned_regular_file(path, &metadata)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;

    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(Some(contents))
}

#[cfg(not(unix))]
fn read_token_cache(_path: &Path) -> webex_headless_messenger::Result<Option<String>> {
    Ok(None)
}

#[cfg(unix)]
fn write_token_cache(path: &Path, contents: &str) -> webex_headless_messenger::Result<bool> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        harden_owned_directory(parent)?;
    }

    let temp_path = path.with_extension(format!(
        "{}.tmp.{}",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("json"),
        std::process::id()
    ));
    if temp_path.exists() {
        std::fs::remove_file(&temp_path)?;
    }

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temp_path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    std::fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&temp_path, path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(true)
}

#[cfg(unix)]
fn validate_owned_regular_file(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> webex_headless_messenger::Result<()> {
    if !metadata.file_type().is_file() {
        return Err(webex_headless_messenger::Error::Other(format!(
            "refusing to read token cache at {} because it is not a regular file",
            path.display()
        )));
    }
    if metadata.uid() != effective_uid() {
        return Err(webex_headless_messenger::Error::Other(format!(
            "refusing to read token cache at {} because it is not owned by the current user",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn harden_owned_directory(path: &Path) -> webex_headless_messenger::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(webex_headless_messenger::Error::Other(format!(
            "refusing to use token cache directory {} because it is not a directory",
            path.display()
        )));
    }
    if metadata.uid() != effective_uid() {
        return Err(webex_headless_messenger::Error::Other(format!(
            "refusing to use token cache directory {} because it is not owned by the current user",
            path.display()
        )));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(unix)]
fn effective_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and does not dereference pointers.
    unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn write_token_cache(_path: &Path, _contents: &str) -> webex_headless_messenger::Result<bool> {
    Ok(false)
}

fn read_env(path: &str) -> webex_headless_messenger::Result<BTreeMap<String, String>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| webex_headless_messenger::Error::Other(error.to_string()))?;
    let mut values = BTreeMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        values.insert(key.trim().to_owned(), unquote(value.trim()));
    }
    Ok(values)
}

fn required<'a>(
    env: &'a BTreeMap<String, String>,
    key: &str,
) -> webex_headless_messenger::Result<&'a str> {
    optional(env, key).ok_or_else(|| {
        webex_headless_messenger::Error::Other(format!("{key} is required in {ENV_FILE}"))
    })
}

fn optional<'a>(env: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    env.get(key)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn unquote(value: &str) -> String {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(scopes: &[&str]) -> TokenSet {
        TokenSet {
            access_token: "access-token".to_owned(),
            refresh_token: Some("refresh-token".to_owned()),
            token_type: "Bearer".to_owned(),
            scopes: scopes.iter().map(|scope| (*scope).to_owned()).collect(),
            expires_at: None,
            refresh_token_expires_at: None,
        }
    }

    #[test]
    fn spark_all_token_satisfies_default_rest_scopes_when_kms_is_present() {
        let requested = vec![
            "spark:messages_read".to_owned(),
            "spark:messages_write".to_owned(),
            "spark:kms".to_owned(),
        ];

        assert!(token_has_requested_scopes(
            &token(&["spark:all", "spark:kms"]),
            &requested
        ));
    }

    #[test]
    fn cached_token_must_include_all_requested_scopes() {
        let requested = vec!["spark:all".to_owned(), "spark:kms".to_owned()];

        assert!(!token_has_requested_scopes(
            &token(&["spark:messages_read", "spark:kms"]),
            &requested,
        ));
    }
}
