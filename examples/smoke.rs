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
use url::Url;
use webex_headless_messenger::{
    DeviceTokenStatus, OAuthClient, OAuthConfig, TokenSet, WebexClient,
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

    let oauth =
        OAuthClient::new(OAuthConfig::new(client_id)?.with_client_secret(client_secret.to_owned()));
    let token = load_or_authorize(&oauth).await?;
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

async fn load_or_authorize(oauth: &OAuthClient) -> webex_headless_messenger::Result<TokenSet> {
    let token_path = PathBuf::from(TOKEN_FILE);
    if let Some(contents) = read_token_cache(&token_path)? {
        let token = serde_json::from_str::<TokenSet>(&contents)?;
        if !token.is_expiring_within(Duration::from_secs(300)) {
            println!("token_cache=hit");
            return Ok(token);
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
                write_token_cache(&token_path, &serialized)?;
                println!("token_cache=stored");
                return Ok(token);
            }
            DeviceTokenStatus::Pending { retry_after } => {
                sleep(retry_after.unwrap_or(interval)).await;
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

fn room_id_candidates_from_link(link: &str) -> Vec<String> {
    let Ok(url) = Url::parse(link.trim()) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    for segment in url.path_segments().into_iter().flatten() {
        push_candidate(&mut candidates, segment);
    }
    for (key, value) in url.query_pairs() {
        if key.to_ascii_lowercase().contains("room")
            || key.to_ascii_lowercase().contains("space")
            || key.to_ascii_lowercase().contains("conversation")
        {
            push_candidate(&mut candidates, &value);
        }
    }
    if let Some(fragment) = url.fragment() {
        for (key, value) in url::form_urlencoded::parse(fragment.as_bytes()) {
            if key.to_ascii_lowercase().contains("room")
                || key.to_ascii_lowercase().contains("space")
                || key.to_ascii_lowercase().contains("conversation")
            {
                push_candidate(&mut candidates, &value);
            }
        }
    }
    candidates.sort();
    candidates.dedup();
    candidates
}

fn push_candidate(candidates: &mut Vec<String>, value: &str) {
    let value = value.trim();
    if value.len() >= 16
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '/' | '='))
    {
        candidates.push(value.to_owned());
    }
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
fn read_token_cache(path: &Path) -> webex_headless_messenger::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn write_token_cache(path: &Path, contents: &str) -> webex_headless_messenger::Result<()> {
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
    Ok(())
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
fn write_token_cache(path: &Path, contents: &str) -> webex_headless_messenger::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, contents)?;
    Ok(())
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
