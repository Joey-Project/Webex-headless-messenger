use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    error::{ApiError, Error, Result, parse_retry_after},
    types::DEFAULT_SCOPE_STRINGS,
};

/// Recommended scopes for a generic account that reads and writes messages in
/// spaces where that account is a member.
pub const DEFAULT_MESSAGING_SCOPES: &[&str] = DEFAULT_SCOPE_STRINGS;

/// Extra scopes for applications that create/update/delete rooms and memberships.
pub const MANAGEMENT_SCOPES: &[&str] = &["spark:rooms_write", "spark:memberships_write"];

const DEFAULT_BASE_URL: &str = "https://webexapis.com/v1/";
const DEVICE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// OAuth Integration configuration.
#[derive(Clone)]
pub struct OAuthConfig {
    pub client_id: String,
    pub client_secret: Option<String>,
    pub redirect_uri: Option<Url>,
    pub scopes: Vec<String>,
    base_url: Url,
}

impl fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthConfig")
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("redirect_uri", &self.redirect_uri)
            .field("scopes", &self.scopes)
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl OAuthConfig {
    pub fn new(client_id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client_id: client_id.into(),
            client_secret: None,
            redirect_uri: None,
            scopes: DEFAULT_MESSAGING_SCOPES
                .iter()
                .map(|scope| (*scope).to_owned())
                .collect(),
            base_url: Url::parse(DEFAULT_BASE_URL)?,
        })
    }

    pub fn with_client_secret(mut self, client_secret: impl Into<String>) -> Self {
        self.client_secret = Some(client_secret.into());
        self
    }

    pub fn with_redirect_uri(mut self, redirect_uri: Url) -> Self {
        self.redirect_uri = Some(redirect_uri);
        self
    }

    pub fn with_base_url(mut self, base_url: Url) -> Self {
        self.base_url = ensure_directory_url(base_url);
        self
    }

    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    pub fn scope_string(&self) -> String {
        self.scopes.join(" ")
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }
}

/// OAuth access and refresh token set.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenSet {
    pub access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    pub token_type: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token_expires_at: Option<DateTime<Utc>>,
}

impl fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSet")
            .field("access_token", &"<redacted>")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "<redacted>"),
            )
            .field("token_type", &self.token_type)
            .field("scopes", &self.scopes)
            .field("expires_at", &self.expires_at)
            .field("refresh_token_expires_at", &self.refresh_token_expires_at)
            .finish()
    }
}

impl TokenSet {
    pub fn is_expiring_within(&self, skew: Duration) -> bool {
        let Some(expires_at) = self.expires_at else {
            return false;
        };
        let deadline = SystemTime::now() + skew;
        let deadline: DateTime<Utc> = deadline.into();
        expires_at <= deadline
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default = "default_token_type")]
    token_type: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    refresh_token_expires_in: Option<i64>,
}

fn default_token_type() -> String {
    "Bearer".to_owned()
}

impl TokenResponse {
    fn into_token_set(self) -> TokenSet {
        let now = Utc::now();
        TokenSet {
            access_token: self.access_token,
            refresh_token: self.refresh_token,
            token_type: self.token_type,
            scopes: self
                .scope
                .split_whitespace()
                .map(ToOwned::to_owned)
                .collect(),
            expires_at: self
                .expires_in
                .map(|seconds| now + chrono::Duration::seconds(seconds)),
            refresh_token_expires_at: self
                .refresh_token_expires_in
                .map(|seconds| now + chrono::Duration::seconds(seconds)),
        }
    }
}

/// Response from the Webex Device Authorization endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DeviceAuthorization {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: Option<String>,
    pub expires_in: u64,
    pub interval: Option<u64>,
}

/// Result of polling the Device Token endpoint.
#[derive(Debug, Clone)]
pub enum DeviceTokenStatus {
    Pending { retry_after: Option<Duration> },
    SlowDown { retry_after: Option<Duration> },
    Authorized(TokenSet),
}

/// PKCE code challenge method for Authorization Code flow helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkceCodeChallengeMethod {
    Plain,
    S256,
}

impl PkceCodeChallengeMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::S256 => "S256",
        }
    }
}

/// OAuth helper for Webex integrations.
#[derive(Clone)]
pub struct OAuthClient {
    http: reqwest::Client,
    config: OAuthConfig,
}

impl fmt::Debug for OAuthClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl OAuthClient {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    pub fn config(&self) -> &OAuthConfig {
        &self.config
    }

    pub fn authorization_url(&self, state: &str) -> Result<Url> {
        self.build_authorization_url(state, None)
    }

    pub fn authorization_url_with_pkce(
        &self,
        state: &str,
        code_challenge: &str,
        code_challenge_method: PkceCodeChallengeMethod,
    ) -> Result<Url> {
        self.build_authorization_url(state, Some((code_challenge, code_challenge_method)))
    }

    fn build_authorization_url(
        &self,
        state: &str,
        pkce: Option<(&str, PkceCodeChallengeMethod)>,
    ) -> Result<Url> {
        let mut url = self.endpoint("authorize")?;
        let scope = self.config.scope_string();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("response_type", "code");
            query.append_pair("client_id", &self.config.client_id);
            query.append_pair("scope", &scope);
            query.append_pair("state", state);
            if let Some(redirect_uri) = &self.config.redirect_uri {
                query.append_pair("redirect_uri", redirect_uri.as_str());
            }
            if let Some((code_challenge, code_challenge_method)) = pkce {
                query.append_pair("code_challenge", code_challenge);
                query.append_pair("code_challenge_method", code_challenge_method.as_str());
            }
        }
        Ok(url)
    }

    pub async fn exchange_authorization_code(&self, code: &str) -> Result<TokenSet> {
        let form = self.authorization_code_form(code, None);
        self.post_token_form("access_token", &form).await
    }

    pub async fn exchange_authorization_code_with_pkce(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenSet> {
        let form = self.authorization_code_form(code, Some(code_verifier));
        self.post_token_form("access_token", &form).await
    }

    fn authorization_code_form(
        &self,
        code: &str,
        code_verifier: Option<&str>,
    ) -> Vec<(&'static str, String)> {
        let mut form = vec![
            ("grant_type", "authorization_code".to_owned()),
            ("client_id", self.config.client_id.clone()),
            ("code", code.to_owned()),
        ];
        if let Some(client_secret) = &self.config.client_secret {
            form.push(("client_secret", client_secret.clone()));
        }
        if let Some(redirect_uri) = &self.config.redirect_uri {
            form.push(("redirect_uri", redirect_uri.to_string()));
        }
        if let Some(code_verifier) = code_verifier {
            form.push(("code_verifier", code_verifier.to_owned()));
        }
        form
    }

    pub async fn refresh_token(&self, refresh_token: &str) -> Result<TokenSet> {
        let Some(client_secret) = &self.config.client_secret else {
            return Err(Error::Other(
                "client_secret is required for Webex token refresh".to_owned(),
            ));
        };
        let mut form = vec![
            ("grant_type", "refresh_token".to_owned()),
            ("client_id", self.config.client_id.clone()),
            ("refresh_token", refresh_token.to_owned()),
        ];
        form.push(("client_secret", client_secret.clone()));
        self.post_token_form("access_token", &form).await
    }

    pub async fn start_device_authorization(&self) -> Result<DeviceAuthorization> {
        let url = self.endpoint("device/authorize")?;
        let form = [
            ("client_id", self.config.client_id.clone()),
            ("scope", self.config.scope_string()),
        ];
        let response = self.http.post(url).form(&form).send().await?;
        decode_response(response).await
    }

    pub async fn poll_device_token(&self, device_code: &str) -> Result<DeviceTokenStatus> {
        let Some(secret) = &self.config.client_secret else {
            return Err(Error::Other(
                "client_secret is required for Webex Device Token polling".to_owned(),
            ));
        };
        let url = self.endpoint("device/token")?;
        let form = [
            ("grant_type", DEVICE_GRANT_TYPE),
            ("device_code", device_code),
            ("client_id", self.config.client_id.as_str()),
        ];
        let request = self
            .http
            .post(url)
            .basic_auth(&self.config.client_id, Some(secret))
            .form(&form);
        let response = request.send().await?;
        if response.status() == StatusCode::PRECONDITION_REQUIRED {
            return Ok(DeviceTokenStatus::Pending {
                retry_after: parse_retry_after(response.headers()),
            });
        }
        if response.status() == StatusCode::BAD_REQUEST {
            return decode_device_token_bad_request(response).await;
        }
        let token = decode_response::<TokenResponse>(response)
            .await?
            .into_token_set();
        Ok(DeviceTokenStatus::Authorized(token))
    }

    async fn post_token_form(&self, path: &str, form: &[(&str, String)]) -> Result<TokenSet> {
        let response = self
            .http
            .post(self.endpoint(path)?)
            .form(form)
            .send()
            .await?;
        Ok(decode_response::<TokenResponse>(response)
            .await?
            .into_token_set())
    }

    fn endpoint(&self, path: &str) -> Result<Url> {
        Ok(ensure_directory_url(self.config.base_url.clone()).join(path)?)
    }
}

fn ensure_directory_url(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    url
}

async fn decode_device_token_bad_request(response: reqwest::Response) -> Result<DeviceTokenStatus> {
    let status = response.status();
    let headers = response.headers().clone();
    let text = response.text().await.unwrap_or_default();
    let body = serde_json::from_str::<Value>(&text).ok();
    let error_code = body.as_ref().and_then(oauth_error_code);
    match error_code {
        Some("authorization_pending") => Ok(DeviceTokenStatus::Pending {
            retry_after: parse_retry_after(&headers),
        }),
        Some("slow_down") => Ok(DeviceTokenStatus::SlowDown {
            retry_after: parse_retry_after(&headers),
        }),
        _ => Err(ApiError::from_status_body(status, &headers, body).into()),
    }
}

fn oauth_error_code(value: &Value) -> Option<&str> {
    [
        value.get("error").and_then(Value::as_str),
        value.get("errorCode").and_then(Value::as_str),
        value.get("code").and_then(Value::as_str),
        value.get("message").and_then(Value::as_str),
        value.pointer("/errors/0/code").and_then(Value::as_str),
        value.pointer("/errors/0/message").and_then(Value::as_str),
        value
            .pointer("/errors/0/description")
            .and_then(Value::as_str),
    ]
    .into_iter()
    .flatten()
    .find_map(normalize_device_oauth_error_code)
}

fn normalize_device_oauth_error_code(value: &str) -> Option<&'static str> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized == "authorization_pending" || normalized.contains("authorization_pending") {
        Some("authorization_pending")
    } else if normalized == "slow_down" || normalized.contains("slow_down") {
        Some("slow_down")
    } else {
        None
    }
}

async fn decode_response<T>(response: reqwest::Response) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if response.status().is_success() {
        Ok(response.json().await?)
    } else if response.status() == StatusCode::PRECONDITION_REQUIRED {
        Err(ApiError::pending_from_response(&response).into())
    } else {
        Err(ApiError::from_response(response).await.into())
    }
}

/// Supplies access tokens to [`crate::WebexClient`].
#[async_trait]
pub trait AccessTokenProvider: Send + Sync {
    async fn access_token(&self) -> Result<String>;
}

/// Token provider for fixed tokens, useful for tests and developer tokens.
#[derive(Clone)]
pub struct StaticTokenProvider {
    token: String,
}

impl fmt::Debug for StaticTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticTokenProvider")
            .field("token", &"<redacted>")
            .finish()
    }
}

impl StaticTokenProvider {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait]
impl AccessTokenProvider for StaticTokenProvider {
    async fn access_token(&self) -> Result<String> {
        Ok(self.token.clone())
    }
}

/// Storage abstraction for refreshable OAuth tokens.
#[async_trait]
pub trait TokenStore: Send + Sync {
    async fn load(&self) -> Result<Option<TokenSet>>;
    async fn save(&self, token_set: &TokenSet) -> Result<()>;
}

/// In-memory token store. Applications should provide durable storage for
/// long-running headless deployments.
#[derive(Debug, Default)]
pub struct MemoryTokenStore {
    token_set: Mutex<Option<TokenSet>>,
}

impl MemoryTokenStore {
    pub fn new(token_set: Option<TokenSet>) -> Self {
        Self {
            token_set: Mutex::new(token_set),
        }
    }
}

#[async_trait]
impl TokenStore for MemoryTokenStore {
    async fn load(&self) -> Result<Option<TokenSet>> {
        Ok(self.token_set.lock().await.clone())
    }

    async fn save(&self, token_set: &TokenSet) -> Result<()> {
        *self.token_set.lock().await = Some(token_set.clone());
        Ok(())
    }
}

/// Token provider that refreshes an OAuth token set when the access token is
/// close to expiry.
#[derive(Debug)]
pub struct RefreshingTokenProvider<S> {
    oauth: OAuthClient,
    store: Arc<S>,
    refresh_skew: Duration,
    refresh_lock: Mutex<()>,
}

impl<S> RefreshingTokenProvider<S>
where
    S: TokenStore,
{
    pub fn new(oauth: OAuthClient, store: Arc<S>) -> Self {
        Self {
            oauth,
            store,
            refresh_skew: Duration::from_secs(300),
            refresh_lock: Mutex::new(()),
        }
    }

    pub fn with_refresh_skew(mut self, refresh_skew: Duration) -> Self {
        self.refresh_skew = refresh_skew;
        self
    }
}

#[async_trait]
impl<S> AccessTokenProvider for RefreshingTokenProvider<S>
where
    S: TokenStore + 'static,
{
    async fn access_token(&self) -> Result<String> {
        let Some(current) = self.store.load().await? else {
            return Err(Error::MissingToken);
        };
        if !current.is_expiring_within(self.refresh_skew) {
            return Ok(current.access_token);
        }

        let _guard = self.refresh_lock.lock().await;
        let Some(current) = self.store.load().await? else {
            return Err(Error::MissingToken);
        };
        if !current.is_expiring_within(self.refresh_skew) {
            return Ok(current.access_token);
        }
        let refresh_token = current.refresh_token.ok_or(Error::MissingToken)?;
        let mut refreshed = self.oauth.refresh_token(&refresh_token).await?;
        if refreshed.refresh_token.is_none() {
            refreshed.refresh_token = Some(refresh_token);
            refreshed.refresh_token_expires_at = current.refresh_token_expires_at;
        }
        self.store.save(&refreshed).await?;
        Ok(refreshed.access_token)
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::{
        DEFAULT_MESSAGING_SCOPES, MANAGEMENT_SCOPES, OAuthClient, OAuthConfig,
        PkceCodeChallengeMethod, oauth_error_code,
    };

    #[test]
    fn builds_authorization_url() {
        let config = OAuthConfig::new("client-id")
            .unwrap()
            .with_redirect_uri(Url::parse("http://localhost:8080/callback").unwrap());
        let url = OAuthClient::new(config)
            .authorization_url("state-1")
            .unwrap();
        let query = url.query().unwrap();
        assert!(query.contains("response_type=code"));
        assert!(query.contains("client_id=client-id"));
        assert!(query.contains("state=state-1"));
        assert!(query.contains("spark%3Amessages_read"));
        assert!(DEFAULT_MESSAGING_SCOPES.contains(&"spark:messages_write"));
        assert!(DEFAULT_MESSAGING_SCOPES.contains(&"spark:kms"));
    }

    #[test]
    fn builds_authorization_url_with_pkce() {
        let config = OAuthConfig::new("client-id")
            .unwrap()
            .with_redirect_uri(Url::parse("http://localhost:8080/callback").unwrap());
        let url = OAuthClient::new(config)
            .authorization_url_with_pkce("state-1", "challenge-1", PkceCodeChallengeMethod::S256)
            .unwrap();
        let query = url.query().unwrap();
        assert!(query.contains("code_challenge=challenge-1"));
        assert!(query.contains("code_challenge_method=S256"));
    }

    #[test]
    fn normalizes_oauth_base_url_as_directory() {
        let mut config = OAuthConfig::new("client-id")
            .unwrap()
            .with_base_url(Url::parse("https://example.test/v1").unwrap());
        assert_eq!(config.base_url().as_str(), "https://example.test/v1/");

        config.base_url = Url::parse("https://example.test/oauth").unwrap();
        let url = OAuthClient::new(config)
            .authorization_url("state-1")
            .unwrap();
        assert_eq!(url.path(), "/oauth/authorize");
    }

    #[tokio::test]
    async fn refresh_token_requires_client_secret() {
        let oauth = OAuthClient::new(OAuthConfig::new("client-id").unwrap());
        let error = oauth.refresh_token("refresh-token").await.unwrap_err();

        assert!(matches!(error, crate::Error::Other(message) if message.contains("client_secret")));
    }

    #[test]
    fn builds_authorization_code_form_with_pkce() {
        let config = OAuthConfig::new("client-id")
            .unwrap()
            .with_client_secret("client-secret")
            .with_redirect_uri(Url::parse("http://localhost:8080/callback").unwrap());
        let form = OAuthClient::new(config).authorization_code_form("code-1", Some("verifier-1"));

        assert!(
            form.iter()
                .any(|(key, value)| { *key == "code_verifier" && value == "verifier-1" })
        );
        assert!(
            form.iter()
                .any(|(key, value)| { *key == "client_secret" && value == "client-secret" })
        );
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let config = OAuthConfig::new("client-id")
            .unwrap()
            .with_client_secret("client-secret");
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("client-secret"));

        let token_set = super::TokenSet {
            access_token: "access-token".to_owned(),
            refresh_token: Some("refresh-token".to_owned()),
            token_type: "Bearer".to_owned(),
            scopes: Vec::new(),
            expires_at: None,
            refresh_token_expires_at: None,
        };
        let debug = format!("{token_set:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("access-token"));
        assert!(!debug.contains("refresh-token"));
    }

    #[test]
    fn parses_oauth_error_codes() {
        let value = serde_json::json!({ "error": "slow_down" });
        assert_eq!(oauth_error_code(&value), Some("slow_down"));

        let value = serde_json::json!({
            "errors": [{ "description": "authorization_pending" }]
        });
        assert_eq!(oauth_error_code(&value), Some("authorization_pending"));

        let value = serde_json::json!({ "message": "slow_down" });
        assert_eq!(oauth_error_code(&value), Some("slow_down"));
    }

    #[test]
    fn exposes_optional_scope_sets() {
        assert!(MANAGEMENT_SCOPES.contains(&"spark:rooms_write"));
    }
}
