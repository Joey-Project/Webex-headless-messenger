use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Event envelope forwarded by the JavaScript SDK realtime sidecar.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SidecarEvent {
    pub version: u16,
    pub resource: String,
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_at: Option<DateTime<Utc>>,
    pub data: Value,
}

impl SidecarEvent {
    pub fn new(resource: impl Into<String>, event: impl Into<String>, data: Value) -> Self {
        Self {
            version: 1,
            resource: resource.into(),
            event: event.into(),
            received_at: Some(Utc::now()),
            data,
        }
    }

    pub fn message_created(data: Value) -> Self {
        Self::new("messages", "created", data)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::SidecarEvent;

    #[test]
    fn deserializes_sidecar_event_envelope() {
        let event = serde_json::from_value::<SidecarEvent>(json!({
            "version": 1,
            "resource": "messages",
            "event": "created",
            "receivedAt": "2026-06-08T15:00:00Z",
            "data": {
                "id": "message-1",
                "roomId": "room-1",
                "text": "hello"
            }
        }))
        .unwrap();

        assert_eq!(event.version, 1);
        assert_eq!(event.resource, "messages");
        assert_eq!(event.event, "created");
        assert_eq!(event.data["roomId"], "room-1");
        assert!(event.received_at.is_some());
    }

    #[test]
    fn creates_message_event_envelope() {
        let event = SidecarEvent::message_created(json!({"id": "message-1"}));
        assert_eq!(event.version, 1);
        assert_eq!(event.resource, "messages");
        assert_eq!(event.event, "created");
        assert_eq!(event.data["id"], "message-1");
    }
}
