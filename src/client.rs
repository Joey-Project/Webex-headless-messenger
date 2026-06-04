use std::sync::Arc;

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::Method;
use serde::Serialize;
use url::Url;

use crate::{
    auth::{AccessTokenProvider, StaticTokenProvider},
    error::{ApiError, Error, Result},
    pagination::{Collection, Page, next_link},
    types::{
        CreateMembership, CreateMessage, CreateRoom, CreateWebhook, DirectMessage,
        ListDirectMessages, ListMemberships, ListMessage, ListMessages, ListRooms, ListWebhooks,
        Membership, Message, Person, Room, UpdateMembership, UpdateMessage, UpdateRoom,
        UpdateWebhook, Webhook,
    },
};

const DEFAULT_BASE_URL: &str = "https://webexapis.com/v1/";

/// Builder for [`WebexClient`].
#[derive(Clone)]
pub struct ClientBuilder {
    http: reqwest::Client,
    base_url: Url,
    token_provider: Option<Arc<dyn AccessTokenProvider>>,
}

impl ClientBuilder {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::new(),
            base_url: Url::parse(DEFAULT_BASE_URL)?,
            token_provider: None,
        })
    }

    pub fn base_url(mut self, base_url: Url) -> Self {
        self.base_url = ensure_directory_url(base_url);
        self
    }

    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    pub fn access_token(mut self, token: impl Into<String>) -> Self {
        self.token_provider = Some(Arc::new(StaticTokenProvider::new(token)));
        self
    }

    pub fn token_provider(mut self, provider: Arc<dyn AccessTokenProvider>) -> Self {
        self.token_provider = Some(provider);
        self
    }

    pub fn build(self) -> Result<WebexClient> {
        let token_provider = self
            .token_provider
            .ok_or_else(|| crate::Error::Other("token provider is required".to_owned()))?;
        Ok(WebexClient {
            http: self.http,
            base_url: self.base_url,
            token_provider,
        })
    }
}

/// Async Webex Messaging REST client.
#[derive(Clone)]
pub struct WebexClient {
    http: reqwest::Client,
    base_url: Url,
    token_provider: Arc<dyn AccessTokenProvider>,
}

impl WebexClient {
    pub fn builder() -> Result<ClientBuilder> {
        ClientBuilder::new()
    }

    pub fn from_access_token(token: impl Into<String>) -> Result<Self> {
        Self::builder()?.access_token(token).build()
    }

    pub async fn me(&self) -> Result<Person> {
        self.get_json("people/me", &NoQuery {}).await
    }

    pub async fn list_rooms(&self, params: &ListRooms) -> Result<Page<Room>> {
        self.get_page("rooms", params).await
    }

    pub async fn get_room(&self, room_id: &str) -> Result<Room> {
        self.get_json(&format!("rooms/{}", encode_segment(room_id)), &NoQuery {})
            .await
    }

    pub async fn create_room(&self, request: &CreateRoom) -> Result<Room> {
        self.send_json(Method::POST, "rooms", request).await
    }

    pub async fn update_room(&self, room_id: &str, request: &UpdateRoom) -> Result<Room> {
        self.send_json(
            Method::PUT,
            &format!("rooms/{}", encode_segment(room_id)),
            request,
        )
        .await
    }

    pub async fn delete_room(&self, room_id: &str) -> Result<()> {
        self.delete(&format!("rooms/{}", encode_segment(room_id)))
            .await
    }

    pub async fn list_messages(&self, params: &ListMessages) -> Result<Page<ListMessage>> {
        self.get_page("messages", params).await
    }

    pub async fn list_direct_messages(
        &self,
        params: &ListDirectMessages,
    ) -> Result<Page<DirectMessage>> {
        self.get_page("messages/direct", params).await
    }

    pub async fn get_message(&self, message_id: &str) -> Result<Message> {
        self.get_json(
            &format!("messages/{}", encode_segment(message_id)),
            &NoQuery {},
        )
        .await
    }

    pub async fn create_message(&self, request: &CreateMessage) -> Result<Message> {
        self.send_json(Method::POST, "messages", request).await
    }

    pub async fn reply_text(
        &self,
        room_id: impl Into<String>,
        parent_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Message> {
        self.create_message(&CreateMessage::reply_text(room_id, parent_id, text))
            .await
    }

    pub async fn update_message(
        &self,
        message_id: &str,
        request: &UpdateMessage,
    ) -> Result<Message> {
        self.send_json(
            Method::PUT,
            &format!("messages/{}", encode_segment(message_id)),
            request,
        )
        .await
    }

    pub async fn delete_message(&self, message_id: &str) -> Result<()> {
        self.delete(&format!("messages/{}", encode_segment(message_id)))
            .await
    }

    pub async fn list_memberships(&self, params: &ListMemberships) -> Result<Page<Membership>> {
        self.get_page("memberships", params).await
    }

    pub async fn get_membership(&self, membership_id: &str) -> Result<Membership> {
        self.get_json(
            &format!("memberships/{}", encode_segment(membership_id)),
            &NoQuery {},
        )
        .await
    }

    pub async fn create_membership(&self, request: &CreateMembership) -> Result<Membership> {
        self.send_json(Method::POST, "memberships", request).await
    }

    pub async fn update_membership(
        &self,
        membership_id: &str,
        request: &UpdateMembership,
    ) -> Result<Membership> {
        self.send_json(
            Method::PUT,
            &format!("memberships/{}", encode_segment(membership_id)),
            request,
        )
        .await
    }

    pub async fn delete_membership(&self, membership_id: &str) -> Result<()> {
        self.delete(&format!("memberships/{}", encode_segment(membership_id)))
            .await
    }

    pub async fn list_webhooks(&self, params: &ListWebhooks) -> Result<Page<Webhook>> {
        self.get_page("webhooks", params).await
    }

    pub async fn get_webhook(&self, webhook_id: &str) -> Result<Webhook> {
        self.get_json(
            &format!("webhooks/{}", encode_segment(webhook_id)),
            &NoQuery {},
        )
        .await
    }

    pub async fn create_webhook(&self, request: &CreateWebhook) -> Result<Webhook> {
        self.send_json(Method::POST, "webhooks", request).await
    }

    pub async fn update_webhook(
        &self,
        webhook_id: &str,
        request: &UpdateWebhook,
    ) -> Result<Webhook> {
        self.send_json(
            Method::PUT,
            &format!("webhooks/{}", encode_segment(webhook_id)),
            request,
        )
        .await
    }

    pub async fn delete_webhook(&self, webhook_id: &str) -> Result<()> {
        self.delete(&format!("webhooks/{}", encode_segment(webhook_id)))
            .await
    }

    pub async fn next_page<T>(&self, next: Url) -> Result<Page<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.validate_page_url(&next)?;
        let response = self.authenticated(Method::GET, next).await?.send().await?;
        decode_page(response).await
    }

    pub async fn collect_all<T>(&self, first_page: Page<T>) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut items = first_page.items;
        let mut next = first_page.next;
        while let Some(url) = next {
            let page = self.next_page(url).await?;
            items.extend(page.items);
            next = page.next;
        }
        Ok(items)
    }

    async fn get_json<T, Q>(&self, path: &str, query: &Q) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        Q: Serialize + ?Sized,
    {
        let response = self
            .authenticated(Method::GET, self.endpoint(path)?)
            .await?
            .query(query)
            .send()
            .await?;
        decode_json(response).await
    }

    async fn get_page<T, Q>(&self, path: &str, query: &Q) -> Result<Page<T>>
    where
        T: serde::de::DeserializeOwned,
        Q: Serialize + ?Sized,
    {
        let response = self
            .authenticated(Method::GET, self.endpoint(path)?)
            .await?
            .query(query)
            .send()
            .await?;
        decode_page(response).await
    }

    async fn send_json<T, B>(&self, method: Method, path: &str, body: &B) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let response = self
            .authenticated(method, self.endpoint(path)?)
            .await?
            .json(body)
            .send()
            .await?;
        decode_json(response).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let response = self
            .authenticated(Method::DELETE, self.endpoint(path)?)
            .await?
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(ApiError::from_response(response).await.into())
        }
    }

    fn endpoint(&self, path: &str) -> Result<Url> {
        Ok(self.base_url.join(path)?)
    }

    fn validate_page_url(&self, next: &Url) -> Result<()> {
        if next.scheme() == self.base_url.scheme()
            && next.host_str() == self.base_url.host_str()
            && next.port_or_known_default() == self.base_url.port_or_known_default()
            && next.path().starts_with(self.base_url.path())
        {
            Ok(())
        } else {
            Err(Error::Other(format!(
                "refusing to send Webex bearer token to pagination URL outside configured base URL: {next}"
            )))
        }
    }

    async fn authenticated(&self, method: Method, url: Url) -> Result<reqwest::RequestBuilder> {
        let token = self.token_provider.access_token().await?;
        Ok(self.http.request(method, url).bearer_auth(token))
    }
}

async fn decode_json<T>(response: reqwest::Response) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if response.status().is_success() {
        Ok(response.json().await?)
    } else {
        Err(ApiError::from_response(response).await.into())
    }
}

async fn decode_page<T>(response: reqwest::Response) -> Result<Page<T>>
where
    T: serde::de::DeserializeOwned,
{
    if response.status().is_success() {
        let next = next_link(response.headers());
        let collection = response.json::<Collection<T>>().await?;
        Ok(Page {
            items: collection.items,
            next,
        })
    } else {
        Err(ApiError::from_response(response).await.into())
    }
}

fn encode_segment(segment: &str) -> String {
    utf8_percent_encode(segment, NON_ALPHANUMERIC).to_string()
}

fn ensure_directory_url(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    url
}

#[derive(Serialize)]
struct NoQuery {}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::{WebexClient, encode_segment, ensure_directory_url};

    #[test]
    fn encodes_path_segments() {
        assert_eq!(encode_segment("a/b+c"), "a%2Fb%2Bc");
    }

    #[test]
    fn normalizes_base_url_as_directory() {
        let base = ensure_directory_url(Url::parse("https://webexapis.com/v1").unwrap());
        assert_eq!(
            base.join("messages").unwrap().as_str(),
            "https://webexapis.com/v1/messages"
        );
    }

    #[test]
    fn rejects_pagination_urls_outside_base_path() {
        let client = WebexClient {
            http: reqwest::Client::new(),
            base_url: ensure_directory_url(Url::parse("https://gateway.example/webex/v1").unwrap()),
            token_provider: std::sync::Arc::new(crate::auth::StaticTokenProvider::new("token")),
        };

        assert!(
            client
                .validate_page_url(
                    &Url::parse("https://gateway.example/webex/v1/messages").unwrap()
                )
                .is_ok()
        );
        assert!(
            client
                .validate_page_url(
                    &Url::parse("https://gateway.example/other/v1/messages").unwrap()
                )
                .is_err()
        );
    }
}
