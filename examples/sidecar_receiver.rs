use std::{collections::BTreeMap, env, net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use webex_headless_messenger::{Error, Result, SidecarEvent};

const DEFAULT_BIND: &str = "127.0.0.1:8787";
const DEFAULT_PATH: &str = "/webex/events";
const MAX_BODY_BYTES: usize = 1024 * 1024;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let bind = env::var("WEBEX_SIDECAR_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_owned());
    let path = env::var("WEBEX_SIDECAR_PATH").unwrap_or_else(|_| DEFAULT_PATH.to_owned());
    let allow_unauthenticated = env::var("WEBEX_SIDECAR_ALLOW_UNAUTHENTICATED")
        .ok()
        .as_deref()
        == Some("1");
    let allow_non_loopback =
        env::var("WEBEX_SIDECAR_ALLOW_NON_LOOPBACK").ok().as_deref() == Some("1");
    validate_loopback_bind(&bind, allow_non_loopback).await?;

    let expected_token = match env::var("WEBEX_SIDECAR_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
    {
        Some(token) => Some(token),
        None if allow_unauthenticated => None,
        None => {
            return Err(Error::Other(
                "WEBEX_SIDECAR_TOKEN is required; set WEBEX_SIDECAR_ALLOW_UNAUTHENTICATED=1 only for local unsafe testing"
                    .to_owned(),
            ));
        }
    };
    let max_events = env::var("WEBEX_SIDECAR_MAX_EVENTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);

    let listener = TcpListener::bind(&bind).await?;
    println!("sidecar_receiver_listening={}", listener.local_addr()?);
    println!("sidecar_receiver_path={path}");
    if expected_token.is_none() {
        println!("sidecar_receiver_unauthenticated=true");
    }
    if allow_non_loopback {
        println!("sidecar_receiver_non_loopback_allowed=true");
    }

    let mut accepted = 0_usize;
    loop {
        let (mut stream, peer) = listener.accept().await?;
        let status = match timeout(Duration::from_secs(10), read_request(&mut stream)).await {
            Ok(Ok(request)) => handle_request(&request, &path, expected_token.as_deref()),
            Ok(Err(error)) => HttpResponse::json_error(400, error.to_string()),
            Err(_) => HttpResponse::json_error(408, "request timeout"),
        };
        let response_status = status.status;
        if let Err(error) = write_response(&mut stream, &status).await {
            eprintln!("sidecar_response_write_failed peer={peer} error={error}");
            continue;
        }
        if response_status == 200 {
            accepted += 1;
            println!("sidecar_event_accepted_from={peer}");
            if max_events > 0 && accepted >= max_events {
                break;
            }
        }
    }

    Ok(())
}

async fn validate_loopback_bind(bind: &str, allow_non_loopback: bool) -> Result<()> {
    let resolved = tokio::net::lookup_host(bind)
        .await?
        .collect::<Vec<SocketAddr>>();
    if resolved.is_empty() {
        return Err(Error::Other(format!(
            "WEBEX_SIDECAR_BIND={bind:?} did not resolve"
        )));
    }
    if !allow_non_loopback && !resolved.iter().all(|addr| addr.ip().is_loopback()) {
        return Err(Error::Other(
            "WEBEX_SIDECAR_BIND must resolve only to loopback addresses; set WEBEX_SIDECAR_ALLOW_NON_LOOPBACK=1 only for an explicitly secured deployment"
                .to_owned(),
        ));
    }
    Ok(())
}

fn handle_request(
    request: &HttpRequest,
    expected_path: &str,
    expected_token: Option<&str>,
) -> HttpResponse {
    if request.method != "POST" {
        return HttpResponse::json_error(405, "method not allowed");
    }
    if request.path != expected_path {
        return HttpResponse::json_error(404, "not found");
    }
    if let Some(token) = expected_token {
        let expected = format!("Bearer {token}");
        if request.headers.get("authorization") != Some(&expected) {
            return HttpResponse::json_error(401, "unauthorized");
        }
    }

    match serde_json::from_slice::<SidecarEvent>(&request.body) {
        Ok(event) => {
            println!(
                "sidecar_event resource={} event={} payload={}",
                event.resource, event.event, event.data
            );
            HttpResponse::json_value(200, serde_json::json!({ "ok": true }))
        }
        Err(error) => HttpResponse::json_error(400, error.to_string()),
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<HttpRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 2048];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(Error::Other(
                "connection closed before request completed".to_owned(),
            ));
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > MAX_BODY_BYTES {
            return Err(Error::Other(
                "request body exceeded maximum size".to_owned(),
            ));
        }
        if let Some(header_end) = find_bytes(&bytes, b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = parse_content_length(&headers)?;
            if content_length > MAX_BODY_BYTES {
                return Err(Error::Other(
                    "request body exceeded maximum size".to_owned(),
                ));
            }
            if bytes.len() >= header_end + 4 + content_length {
                return parse_request(
                    &bytes[..header_end],
                    bytes[header_end + 4..header_end + 4 + content_length].to_vec(),
                );
            }
        }
    }
}

fn parse_request(headers: &[u8], body: Vec<u8>) -> Result<HttpRequest> {
    let text = String::from_utf8_lossy(headers);
    let mut lines = text.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| Error::Other("missing request line".to_owned()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| Error::Other("missing method".to_owned()))?
        .to_owned();
    let path = request_parts
        .next()
        .ok_or_else(|| Error::Other("missing path".to_owned()))?
        .to_owned();
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect::<BTreeMap<_, _>>();

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn parse_content_length(headers: &str) -> Result<usize> {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .ok_or_else(|| Error::Other("missing content-length".to_owned()))
}

async fn write_response(stream: &mut TcpStream, response: &HttpResponse) -> Result<()> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        _ => "Error",
    };
    let raw = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response.status,
        reason,
        response.body.len(),
        response.body
    );
    stream.write_all(raw.as_bytes()).await?;
    Ok(())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct HttpResponse {
    status: u16,
    body: String,
}

impl HttpResponse {
    fn json_value(status: u16, value: serde_json::Value) -> Self {
        Self {
            status,
            body: value.to_string(),
        }
    }

    fn json_error(status: u16, error: impl Into<String>) -> Self {
        Self::json_value(
            status,
            serde_json::json!({
                "ok": false,
                "error": error.into(),
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_error_escapes_error_messages() {
        let response = HttpResponse::json_error(400, "bad \"quoted\" value");
        let parsed: serde_json::Value = serde_json::from_str(&response.body).unwrap();

        assert_eq!(response.status, 400);
        assert_eq!(parsed["ok"], false);
        assert_eq!(parsed["error"], "bad \"quoted\" value");
    }

    fn valid_event_body() -> Vec<u8> {
        serde_json::json!({
            "version": 1,
            "resource": "messages",
            "event": "created",
            "receivedAt": "2026-06-10T00:00:00Z",
            "data": {
                "id": "message-1",
                "roomId": "room-1"
            }
        })
        .to_string()
        .into_bytes()
    }

    fn request(
        method: &str,
        path: &str,
        authorization: Option<&str>,
        body: Vec<u8>,
    ) -> HttpRequest {
        let mut headers = BTreeMap::new();
        if let Some(value) = authorization {
            headers.insert("authorization".to_owned(), value.to_owned());
        }
        HttpRequest {
            method: method.to_owned(),
            path: path.to_owned(),
            headers,
            body,
        }
    }

    async fn read_request_from_bytes(raw: Vec<u8>) -> Result<HttpRequest> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(&raw).await.unwrap();
        });
        let (mut stream, _) = listener.accept().await?;
        let parsed = read_request(&mut stream).await;
        client.await.unwrap();
        parsed
    }

    #[test]
    fn handle_request_accepts_valid_authorized_event() {
        let response = handle_request(
            &request(
                "POST",
                "/webex/events",
                Some("Bearer secret-token"),
                valid_event_body(),
            ),
            "/webex/events",
            Some("secret-token"),
        );

        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response.body).unwrap()["ok"],
            true
        );
    }

    #[test]
    fn handle_request_rejects_unauthorized_wrong_path_and_method() {
        assert_eq!(
            handle_request(
                &request("POST", "/webex/events", None, valid_event_body()),
                "/webex/events",
                Some("secret-token"),
            )
            .status,
            401
        );
        assert_eq!(
            handle_request(
                &request(
                    "POST",
                    "/wrong",
                    Some("Bearer secret-token"),
                    valid_event_body(),
                ),
                "/webex/events",
                Some("secret-token"),
            )
            .status,
            404
        );
        assert_eq!(
            handle_request(
                &request(
                    "GET",
                    "/webex/events",
                    Some("Bearer secret-token"),
                    valid_event_body(),
                ),
                "/webex/events",
                Some("secret-token"),
            )
            .status,
            405
        );
    }

    #[test]
    fn handle_request_rejects_invalid_event_body() {
        let response = handle_request(
            &request(
                "POST",
                "/webex/events",
                Some("Bearer secret-token"),
                br#"{"version":"bad"}"#.to_vec(),
            ),
            "/webex/events",
            Some("secret-token"),
        );

        assert_eq!(response.status, 400);
        serde_json::from_str::<serde_json::Value>(&response.body).unwrap();
    }

    #[tokio::test]
    async fn read_request_parses_valid_request() {
        let body = valid_event_body();
        let raw = format!(
            "POST /webex/events HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret-token\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            String::from_utf8(body).unwrap()
        );
        let request = read_request_from_bytes(raw.into_bytes()).await.unwrap();

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/webex/events");
        assert_eq!(
            request.headers.get("authorization"),
            Some(&"Bearer secret-token".to_owned())
        );
    }

    #[tokio::test]
    async fn read_request_rejects_missing_content_length() {
        let error = read_request_from_bytes(
            b"POST /webex/events HTTP/1.1\r\nHost: localhost\r\n\r\n".to_vec(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("missing content-length"));
    }

    #[tokio::test]
    async fn read_request_rejects_oversized_declared_body() {
        let raw = format!(
            "POST /webex/events HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY_BYTES + 1
        );
        let error = read_request_from_bytes(raw.into_bytes()).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("request body exceeded maximum size")
        );
    }

    #[tokio::test]
    async fn rejects_non_loopback_bind_without_override() {
        let error = validate_loopback_bind("0.0.0.0:8787", false)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("WEBEX_SIDECAR_BIND must resolve only to loopback addresses")
        );
    }

    #[tokio::test]
    async fn accepts_loopback_bind() {
        validate_loopback_bind("127.0.0.1:8787", false)
            .await
            .unwrap();
    }
}
