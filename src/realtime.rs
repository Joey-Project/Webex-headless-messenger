use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use tokio::{sync::mpsc, time};
use url::Url;

use crate::{
    client::WebexClient,
    error::Result,
    types::{ListMessage, ListMessages},
};

/// Polling configuration for machines that cannot expose a public webhook URL.
#[derive(Debug, Clone)]
pub struct PollingConfig {
    pub interval: Duration,
    pub page_size: u16,
    /// Maximum pages to fetch during one poll tick. If this limit is reached,
    /// the poller stores the next page URL and resumes it on the next tick.
    pub max_pages_per_poll: usize,
    pub emit_existing_on_first_poll: bool,
    /// Maximum message IDs retained for in-memory de-duplication. Values below
    /// 1 are treated as 1, trading duplicate suppression for bounded memory.
    pub max_seen_ids: usize,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            page_size: 50,
            max_pages_per_poll: 5,
            emit_existing_on_first_poll: false,
            max_seen_ids: 10_000,
        }
    }
}

#[derive(Clone, Default)]
struct SeenMessageIds {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl SeenMessageIds {
    fn insert(&mut self, id: String, max_ids: usize) -> bool {
        if self.ids.contains(&id) {
            return false;
        }

        self.ids.insert(id.clone());
        self.order.push_back(id);

        let max_ids = max_ids.max(1);
        while self.ids.len() > max_ids {
            if let Some(stale) = self.order.pop_front() {
                self.ids.remove(&stale);
            } else {
                break;
            }
        }

        true
    }
}

/// Simple room message poller with in-memory de-duplication.
#[derive(Clone)]
pub struct MessagePoller {
    client: WebexClient,
    room_id: String,
    config: PollingConfig,
    seen: SeenMessageIds,
    backlog_next: Option<Url>,
    initialized: bool,
}

impl MessagePoller {
    pub fn new(client: WebexClient, room_id: impl Into<String>) -> Self {
        Self {
            client,
            room_id: room_id.into(),
            config: PollingConfig::default(),
            seen: SeenMessageIds::default(),
            backlog_next: None,
            initialized: false,
        }
    }

    pub fn with_config(mut self, config: PollingConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn poll_once(&mut self) -> Result<Vec<ListMessage>> {
        let mut page = if let Some(next) = self.backlog_next.take() {
            self.client.next_page(next).await?
        } else {
            let mut params = ListMessages::room(self.room_id.clone());
            params.max = Some(self.config.page_size);
            self.client.list_messages(&params).await?
        };
        let max_pages = self.config.max_pages_per_poll.max(1);

        let mut fresh = Vec::new();
        for page_index in 0..max_pages {
            let mut saw_known_message = false;
            for message in page.items {
                let Some(id) = message.id.clone() else {
                    continue;
                };
                let is_new = self.seen.insert(id, self.config.max_seen_ids);
                saw_known_message |= !is_new;
                if is_new && (self.initialized || self.config.emit_existing_on_first_poll) {
                    fresh.push(message);
                }
            }

            if saw_known_message && self.initialized {
                self.backlog_next = None;
                break;
            }
            let Some(next) = page.next.take() else {
                self.backlog_next = None;
                break;
            };
            if page_index + 1 >= max_pages {
                if self.initialized || self.config.emit_existing_on_first_poll {
                    self.backlog_next = Some(next);
                }
                break;
            }
            page = self.client.next_page(next).await?;
        }

        self.initialized = true;
        fresh.reverse();
        Ok(fresh)
    }

    pub fn spawn(mut self) -> mpsc::Receiver<Result<ListMessage>> {
        let (sender, receiver) = mpsc::channel(256);
        tokio::spawn(async move {
            let mut interval = time::interval(self.config.interval);
            loop {
                interval.tick().await;
                match self.poll_once().await {
                    Ok(messages) => {
                        for message in messages {
                            if sender.send(Ok(message)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(error) => {
                        if sender.send(Err(error)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::SeenMessageIds;

    #[test]
    fn seen_message_ids_are_capacity_bound() {
        let mut seen = SeenMessageIds::default();

        assert!(seen.insert("a".to_owned(), 2));
        assert!(seen.insert("b".to_owned(), 2));
        assert!(seen.insert("c".to_owned(), 2));

        assert_eq!(seen.ids.len(), 2);
        assert!(!seen.ids.contains("a"));
        assert!(seen.ids.contains("b"));
        assert!(seen.ids.contains("c"));
        assert!(seen.insert("a".to_owned(), 2));
    }
}
