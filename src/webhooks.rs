use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Webex webhook envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookEvent {
    pub id: Option<String>,
    pub name: Option<String>,
    pub target_url: Option<String>,
    pub resource: Option<String>,
    pub event: Option<String>,
    pub filter: Option<String>,
    pub org_id: Option<String>,
    pub created_by: Option<String>,
    pub app_id: Option<String>,
    pub owned_by: Option<String>,
    pub status: Option<String>,
    pub actor_id: Option<String>,
    pub data: Value,
    pub created: Option<DateTime<Utc>>,
}

#[cfg(feature = "webhooks")]
pub fn verify_signature(secret: &str, payload: &[u8], signature_header: &str) -> crate::Result<()> {
    use crate::Error;
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    let trimmed = signature_header.trim();
    let expected = trimmed
        .strip_prefix("sha1=")
        .or_else(|| trimmed.strip_prefix("SHA1="))
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    let mut mac = Hmac::<Sha1>::new_from_slice(secret.as_bytes())
        .map_err(|error| Error::Other(error.to_string()))?;
    mac.update(payload);
    let actual = hex::encode(mac.finalize().into_bytes());
    if constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(Error::InvalidWebhookSignature)
    }
}

#[cfg(feature = "webhooks")]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

#[cfg(all(test, feature = "webhooks"))]
mod tests {
    use super::verify_signature;

    #[test]
    fn verifies_hmac_sha1_signature() {
        verify_signature(
            "secret",
            b"payload",
            "sha1=f75efc0f29bf50c23f99b30b86f7c78fdaf5f11d",
        )
        .unwrap();
    }
}
