use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use tokio::{sync::mpsc, time};
use url::Url;

use crate::{
    client::WebexClient,
    error::{Error, Result},
    types::{ListMessage, ListMessages, ListRooms, Room},
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
    /// Maximum messages or pending message IDs buffered while preserving
    /// chronological catch-up order across multiple poll ticks.
    pub max_pending_messages: usize,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            page_size: 50,
            max_pages_per_poll: 5,
            emit_existing_on_first_poll: false,
            max_seen_ids: 10_000,
            max_pending_messages: 10_000,
        }
    }
}

/// Room discovery configuration for generic-account catch-up across joined spaces.
#[derive(Debug, Clone)]
pub struct RoomDiscoveryConfig {
    /// Maximum rooms to request per Webex page.
    pub page_size: u16,
    /// Maximum joined rooms to keep in one poller. Values below 1 are treated as 1.
    pub max_rooms: usize,
    /// Optional Webex room type filter, such as `group` or `direct`.
    pub room_type: Option<String>,
    /// Optional Webex team filter.
    pub team_id: Option<String>,
    /// Optional Webex room sort key.
    pub sort_by: Option<String>,
}

impl Default for RoomDiscoveryConfig {
    fn default() -> Self {
        Self {
            page_size: 100,
            max_rooms: 1_000,
            room_type: None,
            team_id: None,
            sort_by: None,
        }
    }
}

/// Polling configuration for all joined-room catch-up.
#[derive(Debug, Clone)]
pub struct MultiRoomPollingConfig {
    pub discovery: RoomDiscoveryConfig,
    pub room_polling: PollingConfig,
    pub room_refresh_interval: Duration,
}

impl Default for MultiRoomPollingConfig {
    fn default() -> Self {
        Self {
            discovery: RoomDiscoveryConfig::default(),
            room_polling: PollingConfig::default(),
            room_refresh_interval: Duration::from_secs(300),
        }
    }
}

/// A durable checkpoint seed for one room.
///
/// `seen_message_ids` should be ordered newest-first when the list is bounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomCheckpoint {
    pub room_id: String,
    pub seen_message_ids: Vec<String>,
}

impl RoomCheckpoint {
    pub fn new<I, S>(room_id: impl Into<String>, seen_message_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            room_id: room_id.into(),
            seen_message_ids: seen_message_ids.into_iter().map(Into::into).collect(),
        }
    }
}

/// A message emitted from a joined-room catch-up pass.
#[derive(Debug, Clone)]
pub struct RoomMessage {
    pub room_id: String,
    pub room: Room,
    pub message: ListMessage,
}

/// Discover rooms currently visible to the authorized account.
pub async fn discover_joined_rooms(
    client: &WebexClient,
    config: &RoomDiscoveryConfig,
) -> Result<Vec<Room>> {
    let first = client
        .list_rooms(&ListRooms {
            team_id: config.team_id.clone(),
            room_type: config.room_type.clone(),
            sort_by: config.sort_by.clone(),
            max: Some(config.page_size),
            ..ListRooms::default()
        })
        .await?;
    let limit = config.max_rooms.max(1);
    let mut page = first;
    let mut rooms = Vec::new();
    loop {
        rooms.extend(page.items);
        if rooms.len() > limit {
            return Err(Error::Other(format!(
                "joined-room discovery exceeded max_rooms={limit}; narrow discovery or raise the limit"
            )));
        }
        let Some(next) = page.next.take() else {
            break;
        };
        page = client.next_page::<Room>(next).await?;
    }
    Ok(rooms)
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

fn effective_poll_interval(interval: Duration) -> Duration {
    interval.max(Duration::from_millis(1))
}

fn ensure_pending_within_limit(
    pending_messages: usize,
    pending_ids: usize,
    max_pending_messages: usize,
) -> Result<()> {
    let limit = max_pending_messages.max(1);
    if pending_messages > limit || pending_ids > limit {
        Err(Error::Other(format!(
            "message poller backlog exceeded max_pending_messages={limit}; increase PollingConfig::max_pending_messages or poll more frequently"
        )))
    } else {
        Ok(())
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

    /// Seed the in-memory de-duplication boundary from durable state.
    ///
    /// Seed IDs should be ordered newest-first when the list is bounded.
    /// Seeded pollers are treated as initialized, so the next poll emits
    /// messages newer than the supplied IDs instead of only establishing a
    /// baseline.
    pub fn with_seen_message_ids<I, S>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.seen.remember_newest_first(
            ids.into_iter().map(Into::into).collect::<Vec<_>>(),
            self.config.max_seen_ids,
        );
        self.initialized = true;
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
            ensure_pending_within_limit(
                fresh.len(),
                new_ids.len(),
                self.config.max_pending_messages,
            )?;

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
            let mut interval = time::interval(effective_poll_interval(self.config.interval));
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

#[derive(Clone)]
struct RoomPollerEntry {
    room: Room,
    poller: MessagePoller,
}

/// Multi-room message poller for generic accounts joined to many spaces.
pub struct MultiRoomMessagePoller {
    client: WebexClient,
    config: MultiRoomPollingConfig,
    rooms: BTreeMap<String, RoomPollerEntry>,
    checkpoints: BTreeMap<String, Vec<String>>,
    rooms_initialized: bool,
    last_room_discovery: Option<Instant>,
}

impl MultiRoomMessagePoller {
    pub fn new(client: WebexClient) -> Self {
        Self {
            client,
            config: MultiRoomPollingConfig::default(),
            rooms: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            rooms_initialized: false,
            last_room_discovery: None,
        }
    }

    pub fn with_config(mut self, config: MultiRoomPollingConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_room_checkpoints<I>(mut self, checkpoints: I) -> Self
    where
        I: IntoIterator<Item = RoomCheckpoint>,
    {
        self.checkpoints = checkpoints
            .into_iter()
            .map(|checkpoint| (checkpoint.room_id, checkpoint.seen_message_ids))
            .collect();
        self
    }

    pub fn room_ids(&self) -> Vec<String> {
        self.rooms.keys().cloned().collect()
    }

    pub async fn refresh_rooms(&mut self) -> Result<Vec<Room>> {
        let discovered = discover_joined_rooms(&self.client, &self.config.discovery).await?;
        let mut next_rooms = BTreeMap::new();
        for room in discovered.iter().cloned() {
            let Some(room_id) = room.id.clone() else {
                continue;
            };
            if let Some(mut existing) = self.rooms.remove(&room_id) {
                existing.room = room;
                next_rooms.insert(room_id, existing);
            } else {
                let mut poller = MessagePoller::new(self.client.clone(), room_id.clone())
                    .with_config(self.config.room_polling.clone());
                if let Some(seen_ids) = self.checkpoints.remove(&room_id) {
                    poller = poller.with_seen_message_ids(seen_ids);
                }
                next_rooms.insert(room_id, RoomPollerEntry { room, poller });
            }
        }
        self.rooms = next_rooms;
        self.rooms_initialized = true;
        self.last_room_discovery = Some(Instant::now());
        Ok(discovered)
    }

    pub async fn poll_once(&mut self) -> Result<Vec<RoomMessage>> {
        if !self.rooms_initialized {
            self.refresh_rooms().await?;
        }

        let mut messages = Vec::new();
        for (room_id, entry) in self.rooms.iter_mut() {
            let room_messages = entry.poller.poll_once().await?;
            for message in room_messages {
                messages.push(RoomMessage {
                    room_id: room_id.clone(),
                    room: entry.room.clone(),
                    message,
                });
            }
        }
        messages.sort_by(compare_room_messages);
        Ok(messages)
    }

    pub fn spawn(mut self) -> mpsc::Receiver<Result<RoomMessage>> {
        let (sender, receiver) = mpsc::channel(256);
        tokio::spawn(async move {
            let mut interval =
                time::interval(effective_poll_interval(self.config.room_polling.interval));
            loop {
                interval.tick().await;
                if self
                    .last_room_discovery
                    .map(|last| last.elapsed() >= self.config.room_refresh_interval)
                    .unwrap_or(false)
                {
                    if let Err(error) = self.refresh_rooms().await {
                        if sender.send(Err(error)).await.is_err() {
                            return;
                        }
                        continue;
                    }
                }

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

fn compare_room_messages(left: &RoomMessage, right: &RoomMessage) -> Ordering {
    left.message
        .created
        .cmp(&right.message.created)
        .then_with(|| left.room_id.cmp(&right.room_id))
        .then_with(|| left.message.id.cmp(&right.message.id))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashSet, VecDeque},
        time::Duration,
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };
    use url::Url;

    use crate::{WebexClient, types::Message};

    use super::{
        MessagePoller, MultiRoomMessagePoller, MultiRoomPollingConfig, PollingConfig,
        RoomCheckpoint, RoomDiscoveryConfig, SeenMessageIds, collect_page_messages,
        discover_joined_rooms, effective_poll_interval, ensure_pending_within_limit,
        preserve_pending_on_page_error,
    };

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

    #[test]
    fn zero_poll_interval_is_clamped() {
        assert_eq!(
            effective_poll_interval(Duration::ZERO),
            Duration::from_millis(1)
        );
        assert_eq!(
            effective_poll_interval(Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn pending_backlog_limit_is_enforced() {
        assert!(ensure_pending_within_limit(2, 2, 2).is_ok());
        assert!(ensure_pending_within_limit(3, 2, 2).is_err());
        assert!(ensure_pending_within_limit(2, 3, 2).is_err());
    }

    #[test]
    fn seeded_message_poller_is_initialized() {
        let poller = MessagePoller::new(dummy_client(), "room-1")
            .with_config(PollingConfig {
                max_seen_ids: 1,
                ..PollingConfig::default()
            })
            .with_seen_message_ids(["newer", "older"]);

        assert!(poller.initialized);
        assert!(poller.seen.contains("newer"));
        assert!(!poller.seen.contains("older"));
    }

    #[tokio::test]
    async fn discovers_joined_rooms_with_query_config() {
        let (base_url, requests) = spawn_sequence_server(vec![MockResponse::json(
            r#"{"items":[{"id":"room-1","title":"Room 1"}]}"#,
        )])
        .await;
        let client = client_for(base_url);

        let rooms = discover_joined_rooms(
            &client,
            &RoomDiscoveryConfig {
                page_size: 25,
                max_rooms: 10,
                room_type: Some("group".to_owned()),
                team_id: Some("team-1".to_owned()),
                sort_by: Some("lastactivity".to_owned()),
            },
        )
        .await
        .unwrap();

        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].id.as_deref(), Some("room-1"));
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("GET /v1/rooms?"));
        assert!(requests[0].contains("max=25"));
        assert!(requests[0].contains("type=group"));
        assert!(requests[0].contains("teamId=team-1"));
        assert!(requests[0].contains("sortBy=lastactivity"));
    }

    #[tokio::test]
    async fn discover_joined_rooms_rejects_room_count_over_limit() {
        let (base_url, requests) = spawn_sequence_server(vec![MockResponse::json(
            r#"{"items":[{"id":"room-1"},{"id":"room-2"}]}"#,
        )])
        .await;
        let client = client_for(base_url);

        let error = discover_joined_rooms(
            &client,
            &RoomDiscoveryConfig {
                max_rooms: 1,
                ..RoomDiscoveryConfig::default()
            },
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("joined-room discovery exceeded max_rooms=1")
        );
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn multi_room_poller_discovers_rooms_and_emits_checkpointed_catchup() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(
                r#"{"items":[{"id":"room-b","title":"Room B"},{"id":"room-a","title":"Room A"}]}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:02Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:01Z"}]}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"b-new","roomId":"room-b","text":"B new","created":"2026-06-17T00:00:01Z"},{"id":"b-seen","roomId":"room-b","text":"B seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_polling: PollingConfig {
                    page_size: 10,
                    ..PollingConfig::default()
                },
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([
                RoomCheckpoint::new("room-a", ["a-seen"]),
                RoomCheckpoint::new("room-b", ["b-seen"]),
            ]);

        let messages = poller.poll_once().await.unwrap();

        assert_eq!(poller.room_ids(), ["room-a", "room-b"]);
        assert_eq!(
            messages
                .iter()
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["b-new", "a-new"]
        );
        assert_eq!(
            messages
                .iter()
                .map(|message| message.room.title.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["Room B", "Room A"]
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[1].starts_with("GET /v1/messages?"));
        assert!(requests[1].contains("roomId=room-a"));
        assert!(requests[2].starts_with("GET /v1/messages?"));
        assert!(requests[2].contains("roomId=room-b"));
    }

    fn message(id: &str) -> Message {
        Message {
            id: Some(id.to_owned()),
            ..Message::default()
        }
    }

    fn dummy_client() -> WebexClient {
        WebexClient::from_access_token("token").unwrap()
    }

    fn client_for(base_url: Url) -> WebexClient {
        WebexClient::builder()
            .unwrap()
            .base_url(base_url)
            .access_token("token")
            .build()
            .unwrap()
    }

    struct MockResponse {
        body: String,
    }

    impl MockResponse {
        fn json(body: impl Into<String>) -> Self {
            Self { body: body.into() }
        }
    }

    async fn spawn_sequence_server(responses: Vec<MockResponse>) -> (Url, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            let mut responses = VecDeque::from(responses);
            while let Some(response) = responses.pop_front() {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                requests.push(String::from_utf8_lossy(&request).into_owned());
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.body.len(),
                    response.body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });

        (
            Url::parse(&format!("http://{address}/v1/")).unwrap(),
            server,
        )
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0, "client closed before request completed");
            bytes.extend_from_slice(&buffer[..read]);
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                return bytes;
            }
        }
    }
}
