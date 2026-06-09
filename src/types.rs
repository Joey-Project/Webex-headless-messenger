use std::{
    fmt,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Recommended OAuth scopes for headless messaging automation as a normal user.
pub const DEFAULT_SCOPE_STRINGS: &[&str] = &[
    "spark:messages_read",
    "spark:messages_write",
    "spark:rooms_read",
    "spark:memberships_read",
    "spark:people_read",
    "spark:kms",
];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Person {
    pub id: Option<String>,
    #[serde(default)]
    pub emails: Vec<String>,
    pub display_name: Option<String>,
    pub nick_name: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar: Option<String>,
    pub org_id: Option<String>,
    pub department: Option<String>,
    pub title: Option<String>,
    pub timezone: Option<String>,
    pub status: Option<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub licenses: Vec<String>,
    pub created: Option<DateTime<Utc>>,
    pub last_modified: Option<DateTime<Utc>>,
    pub last_activity: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Room {
    pub id: Option<String>,
    pub title: Option<String>,
    #[serde(rename = "type")]
    pub room_type: Option<String>,
    pub is_locked: Option<bool>,
    pub team_id: Option<String>,
    pub last_activity: Option<DateTime<Utc>>,
    pub creator_id: Option<String>,
    pub created: Option<DateTime<Utc>>,
    pub owner_id: Option<String>,
    pub classification_id: Option<String>,
    pub is_announcement_only: Option<bool>,
    pub is_read_only: Option<bool>,
    pub is_public: Option<bool>,
    pub made_public: Option<DateTime<Utc>>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Membership {
    pub id: Option<String>,
    pub room_id: Option<String>,
    pub person_id: Option<String>,
    pub person_email: Option<String>,
    pub person_display_name: Option<String>,
    pub person_org_id: Option<String>,
    pub is_moderator: Option<bool>,
    pub is_room_hidden: Option<bool>,
    pub room_type: Option<String>,
    pub is_monitor: Option<bool>,
    pub created: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: Option<String>,
    pub parent_id: Option<String>,
    pub room_id: Option<String>,
    pub room_type: Option<String>,
    pub to_person_id: Option<String>,
    pub to_person_email: Option<String>,
    pub text: Option<String>,
    pub markdown: Option<String>,
    pub html: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub attachments: Vec<Value>,
    pub person_id: Option<String>,
    pub person_email: Option<String>,
    #[serde(default)]
    pub mentioned_people: Vec<String>,
    #[serde(default)]
    pub mentioned_groups: Vec<String>,
    pub created: Option<DateTime<Utc>>,
    pub updated: Option<DateTime<Utc>>,
    pub is_voice_clip: Option<bool>,
}

pub type ListMessage = Message;
pub type DirectMessage = Message;

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Webhook {
    pub id: Option<String>,
    pub name: Option<String>,
    pub target_url: Option<String>,
    pub resource: Option<String>,
    pub event: Option<String>,
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    pub status: Option<String>,
    pub created: Option<DateTime<Utc>>,
    pub owned_by: Option<String>,
}

impl fmt::Debug for Webhook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Webhook")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("target_url", &self.target_url)
            .field("resource", &self.resource)
            .field("event", &self.event)
            .field("filter", &self.filter)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field("status", &self.status)
            .field("created", &self.created)
            .field("owned_by", &self.owned_by)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListMessages {
    pub room_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mentioned_people: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<u16>,
}

impl ListMessages {
    pub fn room(room_id: impl Into<String>) -> Self {
        Self {
            room_id: room_id.into(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListDirectMessages {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub person_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub person_email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_person_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_person_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub markdown: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Value>,
}

impl CreateMessage {
    pub fn text(room_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            room_id: Some(room_id.into()),
            text: Some(text.into()),
            ..Self::default()
        }
    }

    pub fn markdown(room_id: impl Into<String>, markdown: impl Into<String>) -> Self {
        Self {
            room_id: Some(room_id.into()),
            markdown: Some(markdown.into()),
            ..Self::default()
        }
    }

    pub fn reply_text(
        room_id: impl Into<String>,
        parent_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            room_id: Some(room_id.into()),
            parent_id: Some(parent_id.into()),
            text: Some(text.into()),
            ..Self::default()
        }
    }

    pub fn direct_text_to_email(email: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            to_person_email: Some(email.into()),
            text: Some(text.into()),
            ..Self::default()
        }
    }
}

/// Local filesystem attachment uploaded with a multipart message request.
///
/// The message helper performs local preflight validation before sending the
/// request: it rejects non-regular files, files over the Webex 100 MB limit,
/// CR/LF in multipart file names, and invalid MIME syntax. Webex may still
/// apply additional server-side validation.
#[derive(Clone)]
pub struct LocalFileAttachment {
    path: PathBuf,
    file_name: Option<String>,
    media_type: Option<String>,
}

impl LocalFileAttachment {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            file_name: None,
            media_type: None,
        }
    }

    pub fn with_file_name(mut self, file_name: impl Into<String>) -> Self {
        self.file_name = Some(file_name.into());
        self
    }

    pub fn with_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.media_type = Some(media_type.into());
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_name(&self) -> Option<&str> {
        self.file_name.as_deref()
    }

    pub fn media_type(&self) -> Option<&str> {
        self.media_type.as_deref()
    }
}

impl fmt::Debug for LocalFileAttachment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFileAttachment")
            .field("path", &self.path)
            .field("file_name", &self.file_name)
            .field("media_type", &self.media_type)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMessage {
    pub room_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub markdown: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListRooms {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub room_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_public_spaces: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRoom {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classification_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_locked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_public: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_announcement_only: Option<bool>,
}

impl CreateRoom {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            team_id: None,
            classification_id: None,
            is_locked: None,
            is_public: None,
            description: None,
            is_announcement_only: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateRoom {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classification_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_locked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_public: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_announcement_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_read_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListMemberships {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub person_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub person_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<u16>,
}

impl ListMemberships {
    pub fn room(room_id: impl Into<String>) -> Self {
        Self {
            room_id: Some(room_id.into()),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMembership {
    pub room_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub person_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub person_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_moderator: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMembership {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_moderator: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_room_hidden: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListWebhooks {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owned_by: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateWebhook {
    pub name: String,
    pub target_url: String,
    pub resource: String,
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owned_by: Option<String>,
}

impl fmt::Debug for CreateWebhook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateWebhook")
            .field("name", &self.name)
            .field("target_url", &self.target_url)
            .field("resource", &self.resource)
            .field("event", &self.event)
            .field("filter", &self.filter)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field("owned_by", &self.owned_by)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateWebhook {
    pub name: String,
    pub target_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owned_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

impl fmt::Debug for UpdateWebhook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpdateWebhook")
            .field("name", &self.name)
            .field("target_url", &self.target_url)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field("owned_by", &self.owned_by)
            .field("status", &self.status)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{CreateMessage, CreateWebhook, LocalFileAttachment, UpdateMembership};

    #[test]
    fn serializes_reply_message_body() {
        let body =
            serde_json::to_value(CreateMessage::reply_text("room-1", "parent-1", "ok")).unwrap();
        assert_eq!(
            body,
            json!({
                "roomId": "room-1",
                "parentId": "parent-1",
                "text": "ok"
            })
        );
    }

    #[test]
    fn serializes_direct_message_body_without_room() {
        let body =
            serde_json::to_value(CreateMessage::direct_text_to_email("bot@example.com", "hi"))
                .unwrap();
        assert_eq!(
            body,
            json!({
                "toPersonEmail": "bot@example.com",
                "text": "hi"
            })
        );
    }

    #[test]
    fn local_file_attachment_keeps_path_and_metadata() {
        let upload = LocalFileAttachment::new("/tmp/report.txt")
            .with_file_name("report.txt")
            .with_media_type("text/plain");
        assert_eq!(upload.path(), std::path::Path::new("/tmp/report.txt"));
        assert_eq!(upload.file_name(), Some("report.txt"));
        assert_eq!(upload.media_type(), Some("text/plain"));
    }

    #[test]
    fn serializes_partial_membership_update() {
        let body = serde_json::to_value(UpdateMembership {
            is_moderator: Some(true),
            is_room_hidden: None,
        })
        .unwrap();
        assert_eq!(
            body,
            json!({
                "isModerator": true
            })
        );
    }

    #[test]
    fn debug_output_redacts_webhook_secret() {
        let webhook = CreateWebhook {
            name: "name".to_owned(),
            target_url: "https://example.com/webhook".to_owned(),
            resource: "messages".to_owned(),
            event: "created".to_owned(),
            filter: None,
            secret: Some("secret-value".to_owned()),
            owned_by: None,
        };
        let debug = format!("{webhook:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-value"));
    }
}
