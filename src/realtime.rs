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
    /// the poller stores the next page URL and buffers newer messages until it
    /// can emit the full catch-up batch in chronological order.
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
    fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }

    fn remember_newest_first<I>(&mut self, ids: I, max_ids: usize)
    where
        I: IntoIterator<Item = String>,
        I::IntoIter: DoubleEndedIterator,
    {
        let max_ids = max_ids.max(1);

        for id in ids.into_iter().rev() {
            if self.ids.insert(id.clone()) {
                self.order.push_front(id);
            }

            while self.ids.len() > max_ids {
                if let Some(stale) = self.order.pop_back() {
                    self.ids.remove(&stale);
                } else {
                    break;
                }
            }
        }
    }
}

fn collect_page_messages(
    seen: &SeenMessageIds,
    items: Vec<ListMessage>,
    initialized: bool,
    emit_existing_on_first_poll: bool,
    local_seen_ids: &mut HashSet<String>,
) -> (Vec<ListMessage>, Vec<String>, bool) {
    let mut fresh = Vec::new();
    let mut new_ids = Vec::new();
    let mut saw_known_message = false;

    for message in items {
        let Some(id) = message.id.clone() else {
            continue;
        };
        if seen.contains(&id) {
            saw_known_message = true;
            if initialized {
                break;
            }
            continue;
        }
        if !local_seen_ids.insert(id.clone()) {
            continue;
        }

        new_ids.push(id);
        if initialized || emit_existing_on_first_poll {
            fresh.push(message);
        }
    }

    (fresh, new_ids, saw_known_message)
}

fn preserve_pending_on_page_error(initialized: bool, emit_existing_on_first_poll: bool) -> bool {
    initialized || emit_existing_on_first_poll
}

/// Simple room message poller with in-memory de-duplication.
#[derive(Clone)]
pub struct MessagePoller {
    client: WebexClient,
    room_id: String,
    config: PollingConfig,
    seen: SeenMessageIds,
    backlog_next: Option<Url>,
    pending_fresh: Vec<ListMessage>,
    pending_seen_ids: Vec<String>,
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
            pending_fresh: Vec::new(),
            pending_seen_ids: Vec::new(),
            initialized: false,
        }
    }

    pub fn with_config(mut self, config: PollingConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn poll_once(&mut self) -> Result<Vec<ListMessage>> {
        let mut page = if let Some(next) = self.backlog_next.clone() {
            match self.client.next_page(next.clone()).await {
                Ok(page) => {
                    self.backlog_next = None;
                    page
                }
                Err(error) => {
                    self.backlog_next = Some(next);
                    return Err(error);
                }
            }
        } else {
            let mut params = ListMessages::room(self.room_id.clone());
            params.max = Some(self.config.page_size);
            self.client.list_messages(&params).await?
        };
        let max_pages = self.config.max_pages_per_poll.max(1);

        let mut fresh = self.pending_fresh.clone();
        let mut new_ids = self.pending_seen_ids.clone();
        let mut local_seen_ids = new_ids.iter().cloned().collect::<HashSet<_>>();
        for page_index in 0..max_pages {
            let (mut page_fresh, mut page_ids, saw_known_message) = collect_page_messages(
                &self.seen,
                page.items,
                self.initialized,
                self.config.emit_existing_on_first_poll,
                &mut local_seen_ids,
            );
            fresh.append(&mut page_fresh);
            new_ids.append(&mut page_ids);

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
                    self.pending_fresh = fresh;
                    self.pending_seen_ids = new_ids;
                    self.backlog_next = Some(next);
                    self.initialized = true;
                    return Ok(Vec::new());
                }
                break;
            }
            page = match self.client.next_page(next.clone()).await {
                Ok(page) => page,
                Err(error) => {
                    if preserve_pending_on_page_error(
                        self.initialized,
                        self.config.emit_existing_on_first_poll,
                    ) {
                        self.pending_fresh = fresh;
                        self.pending_seen_ids = new_ids;
                        self.backlog_next = Some(next);
                        self.initialized = true;
                    }
                    return Err(error);
                }
            };
        }

        self.seen
            .remember_newest_first(new_ids, self.config.max_seen_ids);
        self.pending_fresh.clear();
        self.pending_seen_ids.clear();
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
    use std::collections::HashSet;

    use crate::types::Message;

    use super::{SeenMessageIds, collect_page_messages, preserve_pending_on_page_error};

    #[test]
    fn seen_message_ids_keep_newest_ids_from_api_order() {
        let mut seen = SeenMessageIds::default();

        seen.remember_newest_first(
            vec![
                "newest".to_owned(),
                "middle".to_owned(),
                "oldest".to_owned(),
            ],
            2,
        );

        assert_eq!(seen.ids.len(), 2);
        assert!(seen.ids.contains("newest"));
        assert!(seen.ids.contains("middle"));
        assert!(!seen.ids.contains("oldest"));
    }

    #[test]
    fn page_scan_stops_at_known_boundary_before_retaining_new_ids() {
        let mut seen = SeenMessageIds::default();
        seen.remember_newest_first(vec!["known-newest".to_owned()], 1);

        let mut local_seen_ids = HashSet::new();
        let (fresh, new_ids, saw_known) = collect_page_messages(
            &seen,
            vec![
                message("new-2"),
                message("new-1"),
                message("known-newest"),
                message("old-duplicate"),
            ],
            true,
            false,
            &mut local_seen_ids,
        );

        assert!(saw_known);
        assert_eq!(new_ids, ["new-2", "new-1"]);
        assert_eq!(
            fresh
                .iter()
                .map(|message| message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["new-2", "new-1"]
        );
        assert!(!seen.ids.contains("new-2"));
        seen.remember_newest_first(new_ids, 1);
        assert!(seen.ids.contains("new-2"));
        assert!(!seen.ids.contains("old-duplicate"));
    }

    #[test]
    fn multi_page_commit_keeps_first_page_newest_ids() {
        let mut seen = SeenMessageIds::default();
        let mut local_seen_ids = HashSet::new();
        let mut ids = Vec::new();

        let (_, mut page_ids, _) = collect_page_messages(
            &seen,
            vec![message("page-1-newest"), message("page-1-older")],
            true,
            false,
            &mut local_seen_ids,
        );
        ids.append(&mut page_ids);

        let (_, mut page_ids, _) = collect_page_messages(
            &seen,
            vec![message("page-2-older")],
            true,
            false,
            &mut local_seen_ids,
        );
        ids.append(&mut page_ids);

        seen.remember_newest_first(ids, 2);

        assert!(seen.ids.contains("page-1-newest"));
        assert!(seen.ids.contains("page-1-older"));
        assert!(!seen.ids.contains("page-2-older"));
    }

    #[test]
    fn initial_poll_error_does_not_commit_seen_boundary() {
        let seen = SeenMessageIds::default();
        let mut local_seen_ids = HashSet::new();
        let (_, new_ids, _) = collect_page_messages(
            &seen,
            vec![message("existing-newest"), message("existing-older")],
            false,
            false,
            &mut local_seen_ids,
        );

        assert_eq!(new_ids, ["existing-newest", "existing-older"]);
        assert!(!seen.ids.contains("existing-newest"));
    }

    #[test]
    fn initial_default_page_error_retries_baseline() {
        assert!(!preserve_pending_on_page_error(false, false));
        assert!(preserve_pending_on_page_error(true, false));
        assert!(preserve_pending_on_page_error(false, true));
    }

    fn message(id: &str) -> Message {
        Message {
            id: Some(id.to_owned()),
            ..Message::default()
        }
    }
}
