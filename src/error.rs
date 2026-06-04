use std::time::Duration;

use reqwest::{StatusCode, header::HeaderMap};
use serde_json::Value;
use thiserror::Error as ThisError;

/// Crate-wide result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the Webex client and OAuth helpers.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error("http client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("webex api error: {0}")]
    Api(Box<ApiError>),
    #[error("no usable access token is available")]
    MissingToken,
    #[error("oauth flow is still pending")]
    AuthorizationPending,
    #[error("oauth response did not include an access token")]
    MissingAccessToken,
    #[error("invalid webhook signature")]
    InvalidWebhookSignature,
    #[error("{0}")]
    Other(String),
}

impl From<ApiError> for Error {
    fn from(value: ApiError) -> Self {
        Self::Api(Box::new(value))
    }
}

/// Structured Webex REST error information.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: u16,
    pub reason: String,
    pub message: Option<String>,
    pub tracking_id: Option<String>,
    pub retry_after: Option<Duration>,
    pub details: Vec<ApiErrorDetail>,
    pub body: Option<Value>,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status, self.reason)?;
        if let Some(message) = &self.message {
            write!(f, ": {message}")?;
        }
        if let Some(tracking_id) = &self.tracking_id {
            write!(f, " (trackingId: {tracking_id})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiError {}

/// A single Webex error detail.
#[derive(Debug, Clone)]
pub struct ApiErrorDetail {
    pub description: Option<String>,
    pub code: Option<String>,
    pub reason: Option<String>,
}

impl ApiError {
    pub(crate) fn from_status_body(
        status: StatusCode,
        headers: &HeaderMap,
        body: Option<Value>,
    ) -> Self {
        let message = body.as_ref().and_then(extract_message);
        let details = body.as_ref().map(extract_details).unwrap_or_default();
        let tracking_id = tracking_id_from_headers(headers).or_else(|| {
            body.as_ref()
                .and_then(|value| value.get("trackingId"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        });

        Self {
            status: status.as_u16(),
            reason: status
                .canonical_reason()
                .unwrap_or("Unknown Status")
                .to_owned(),
            message,
            tracking_id,
            retry_after: parse_retry_after(headers),
            details,
            body,
        }
    }

    pub(crate) async fn from_response(response: reqwest::Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        let text = response.text().await.unwrap_or_default();
        let body = serde_json::from_str::<Value>(&text).ok();
        Self::from_status_body(status, &headers, body)
    }

    pub(crate) fn pending_from_response(response: &reqwest::Response) -> Self {
        let status = response.status();
        Self {
            status: status.as_u16(),
            reason: status
                .canonical_reason()
                .unwrap_or("Unknown Status")
                .to_owned(),
            message: Some("authorization is still pending".to_owned()),
            tracking_id: tracking_id_from_headers(response.headers()),
            retry_after: parse_retry_after(response.headers()),
            details: Vec::new(),
            body: None,
        }
    }

    pub fn is_status(&self, status: StatusCode) -> bool {
        self.status == status.as_u16()
    }
}

fn extract_message(value: &Value) -> Option<String> {
    value
        .get("message")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .pointer("/error/message/0/description")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

fn extract_details(value: &Value) -> Vec<ApiErrorDetail> {
    value
        .get("errors")
        .and_then(Value::as_array)
        .map(|errors| {
            errors
                .iter()
                .map(|error| ApiErrorDetail {
                    description: error
                        .get("description")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    code: error
                        .get("code")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    reason: error
                        .get("reason")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn tracking_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("trackingid")
        .or_else(|| headers.get("trackingId"))
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

pub(crate) fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};

    use super::parse_retry_after;

    #[test]
    fn parses_numeric_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("42"));
        assert_eq!(parse_retry_after(&headers).unwrap().as_secs(), 42);
    }
}
