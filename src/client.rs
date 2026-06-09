use std::sync::Arc;

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{
    Method,
    multipart::{Form, Part},
};
use serde::Serialize;
use url::Url;

use crate::{
    auth::{AccessTokenProvider, StaticTokenProvider},
    error::{ApiError, Error, Result},
    pagination::{Collection, Page, next_link},
    types::{
        CreateMembership, CreateMessage, CreateRoom, CreateWebhook, DirectMessage,
        ListDirectMessages, ListMemberships, ListMessage, ListMessages, ListRooms, ListWebhooks,
        LocalFileAttachment, Membership, Message, Person, Room, UpdateMembership, UpdateMessage,
        UpdateRoom, UpdateWebhook, Webhook,
    },
};

const DEFAULT_BASE_URL: &str = "https://webexapis.com/v1/";
const MAX_LOCAL_FILE_UPLOAD_BYTES: u64 = 100 * 1024 * 1024;

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

    /// Create a message with one local filesystem attachment.
    pub async fn create_message_with_file(
        &self,
        request: &CreateMessage,
        file: &LocalFileAttachment,
    ) -> Result<Message> {
        let form = message_multipart_form(request, file).await?;
        let response = self
            .authenticated(Method::POST, self.endpoint("messages")?)
            .await?
            .multipart(form)
            .send()
            .await?;
        decode_json(response).await
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
            && is_same_or_child_path(next.path(), self.base_url.path())
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

async fn message_multipart_form(
    request: &CreateMessage,
    file: &LocalFileAttachment,
) -> Result<Form> {
    if !request.files.is_empty() {
        return Err(Error::Other(
            "create_message_with_file accepts one local file attachment; use create_message for public file URLs"
                .to_owned(),
        ));
    }
    if !request.attachments.is_empty() {
        return Err(Error::Other(
            "create_message_with_file does not support Adaptive Card attachments; use raw JSON create_message for card payloads"
                .to_owned(),
        ));
    }

    let mut form = Form::new();
    form = append_optional_text(form, "roomId", &request.room_id);
    form = append_optional_text(form, "parentId", &request.parent_id);
    form = append_optional_text(form, "toPersonId", &request.to_person_id);
    form = append_optional_text(form, "toPersonEmail", &request.to_person_email);
    form = append_optional_text(form, "text", &request.text);
    form = append_optional_text(form, "markdown", &request.markdown);

    let file_name = file
        .file_name()
        .map(ToOwned::to_owned)
        .or_else(|| {
            file.path()
                .file_name()
                .and_then(|name| name.to_str())
                .map(ToOwned::to_owned)
        })
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            Error::Other("local file attachment requires a UTF-8 file name".to_owned())
        })?;

    let metadata = tokio::fs::metadata(file.path()).await?;
    if !metadata.is_file() {
        return Err(Error::Other(
            "local file attachment path must be a regular file".to_owned(),
        ));
    }
    if metadata.len() > MAX_LOCAL_FILE_UPLOAD_BYTES {
        return Err(Error::Other(format!(
            "local file attachment exceeds Webex 100 MiB limit: {} bytes",
            metadata.len()
        )));
    }

    let bytes = tokio::fs::read(file.path()).await?;
    let mut part = Part::bytes(bytes).file_name(file_name);
    part = part.mime_str(file.media_type().unwrap_or("application/octet-stream"))?;
    Ok(form.part("files", part))
}

fn append_optional_text(mut form: Form, name: &'static str, value: &Option<String>) -> Form {
    if let Some(value) = value {
        form = form.text(name, value.clone());
    }
    form
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

fn is_same_or_child_path(path: &str, base_path: &str) -> bool {
    path == base_path || path.starts_with(base_path)
}

#[derive(Serialize)]
struct NoQuery {}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::oneshot,
    };
    use url::Url;

    use crate::types::{CreateMessage, LocalFileAttachment};

    use super::{
        MAX_LOCAL_FILE_UPLOAD_BYTES, WebexClient, encode_segment, ensure_directory_url,
        is_same_or_child_path,
    };

    #[tokio::test]
    async fn create_message_with_file_sends_multipart_form() {
        let file_path = std::env::temp_dir().join(format!(
            "webex-headless-upload-{}-note.txt",
            std::process::id()
        ));
        std::fs::write(&file_path, "file-body").unwrap();

        let (base_url, captured_request) = spawn_capture_server(
            r#"{"id":"message-1","roomId":"room-1","text":"attached","files":["https://example.invalid/file"]}"#,
        )
        .await;
        let client = WebexClient::builder()
            .unwrap()
            .base_url(base_url)
            .access_token("token")
            .build()
            .unwrap();

        let message = client
            .create_message_with_file(
                &CreateMessage::text("room-1", "attached"),
                &LocalFileAttachment::new(&file_path)
                    .with_file_name("note.txt")
                    .with_media_type("text/plain"),
            )
            .await
            .unwrap();

        let request = captured_request.await.unwrap();
        let lower = request.to_ascii_lowercase();
        assert_eq!(message.id.as_deref(), Some("message-1"));
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));
        assert!(lower.contains("authorization: bearer token"));
        assert!(lower.contains("content-type: multipart/form-data; boundary="));
        assert!(request.contains("name=\"roomId\""));
        assert!(request.contains("room-1"));
        assert!(request.contains("name=\"text\""));
        assert!(request.contains("attached"));
        assert!(request.contains("name=\"files\"; filename=\"note.txt\""));
        assert!(lower.contains("content-type: text/plain"));
        assert!(request.contains("file-body"));

        let _ = std::fs::remove_file(file_path);
    }

    #[tokio::test]
    async fn create_message_with_file_rejects_url_files() {
        let client = WebexClient::builder()
            .unwrap()
            .access_token("token")
            .build()
            .unwrap();
        let mut request = CreateMessage::text("room-1", "attached");
        request
            .files
            .push("https://example.invalid/file.png".to_owned());

        let error = client
            .create_message_with_file(
                &request,
                &LocalFileAttachment::new("/tmp/file.png").with_file_name("file.png"),
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("use create_message for public file URLs")
        );
    }

    #[tokio::test]
    async fn create_message_with_file_rejects_oversized_file_before_read() {
        let file_path = std::env::temp_dir().join(format!(
            "webex-headless-upload-{}-oversized.bin",
            std::process::id()
        ));
        let file = std::fs::File::create(&file_path).unwrap();
        file.set_len(MAX_LOCAL_FILE_UPLOAD_BYTES + 1).unwrap();
        drop(file);

        let client = WebexClient::builder()
            .unwrap()
            .access_token("token")
            .build()
            .unwrap();
        let error = client
            .create_message_with_file(
                &CreateMessage::text("room-1", "attached"),
                &LocalFileAttachment::new(&file_path).with_file_name("oversized.bin"),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds Webex 100 MiB limit"));

        let _ = std::fs::remove_file(file_path);
    }

    #[tokio::test]
    async fn create_message_with_file_rejects_non_regular_file() {
        let directory = std::env::temp_dir().join(format!(
            "webex-headless-upload-{}-directory",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();

        let client = WebexClient::builder()
            .unwrap()
            .access_token("token")
            .build()
            .unwrap();
        let error = client
            .create_message_with_file(
                &CreateMessage::text("room-1", "attached"),
                &LocalFileAttachment::new(&directory).with_file_name("directory"),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("path must be a regular file"));

        let _ = std::fs::remove_dir(directory);
    }

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

    #[test]
    fn rejects_sibling_pagination_path_prefixes() {
        assert!(is_same_or_child_path("/webex/v1/messages", "/webex/v1/"));
        assert!(!is_same_or_child_path(
            "/webex/v1evil/messages",
            "/webex/v1/"
        ));
    }

    async fn spawn_capture_server(response_body: &'static str) -> (Url, oneshot::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            let _ = tx.send(String::from_utf8_lossy(&request).into_owned());
        });

        (Url::parse(&format!("http://{address}/v1/")).unwrap(), rx)
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0, "client closed before request completed");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(header_end) = find_bytes(&bytes, b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                if bytes.len() >= header_end + 4 + content_length {
                    return bytes;
                }
            }
        }
    }

    fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }
}
