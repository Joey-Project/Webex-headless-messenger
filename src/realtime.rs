use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use futures_util::{StreamExt, stream::FuturesUnordered};
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
    /// Optional timeout for joined-room discovery and refresh. Values below 1 ms are clamped.
    pub room_discovery_timeout: Option<Duration>,
    /// Optional timeout for each room's polling pass. Values below 1 ms are clamped.
    pub room_poll_timeout: Option<Duration>,
    /// Maximum rooms polled concurrently. Values below 1 are treated as 1.
    pub max_concurrent_room_polls: usize,
    /// Maximum inactive room poller states retained for rooms that disappear from discovery.
    /// Values below 1 are treated as 1.
    pub max_inactive_rooms: usize,
}

impl Default for MultiRoomPollingConfig {
    fn default() -> Self {
        Self {
            discovery: RoomDiscoveryConfig::default(),
            room_polling: PollingConfig::default(),
            room_refresh_interval: Duration::from_secs(300),
            room_discovery_timeout: Some(Duration::from_secs(60)),
            room_poll_timeout: Some(Duration::from_secs(60)),
            max_concurrent_room_polls: 16,
            max_inactive_rooms: 1_000,
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
    /// Initialize the room even when the seed list is empty.
    pub initialize_empty: bool,
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
            initialize_empty: false,
        }
    }

    /// Create an explicit empty-room checkpoint for rooms known to have no prior messages.
    pub fn known_empty(room_id: impl Into<String>) -> Self {
        Self {
            room_id: room_id.into(),
            seen_message_ids: Vec::new(),
            initialize_empty: true,
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
    let mut room_indexes = BTreeMap::new();
    loop {
        for room in page.items {
            if let Some(room_id) = room.id.clone() {
                if let Some(index) = room_indexes.get(&room_id).copied() {
                    rooms[index] = room;
                } else {
                    room_indexes.insert(room_id, rooms.len());
                    rooms.push(room);
                }
            } else {
                rooms.push(room);
            }
        }
        let next = page.next.take();
        if rooms.len() > limit {
            return Err(Error::Other(format!(
                "joined-room discovery exceeded max_rooms={limit}; narrow discovery or raise the limit"
            )));
        }
        let Some(next) = next else {
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

async fn poll_tick_or_receiver_open<T>(
    sender: &mpsc::Sender<T>,
    interval: &mut time::Interval,
) -> bool {
    if sender.is_closed() {
        return false;
    }
    tokio::select! {
        _ = sender.closed() => false,
        _ = interval.tick() => true,
    }
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

struct MessagePollUpdate {
    messages: Vec<ListMessage>,
    seen_ids: Vec<String>,
    pending_fresh: Vec<ListMessage>,
    pending_seen_ids: Vec<String>,
    backlog_next: Option<Url>,
    initialized: bool,
    commit_seen: bool,
}

struct MessagePollError {
    error: Error,
    update: Option<MessagePollUpdate>,
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
        let ids = ids.into_iter().map(Into::into).collect::<Vec<_>>();
        if ids.is_empty() {
            return self;
        }
        self.seen
            .remember_newest_first(ids, self.config.max_seen_ids);
        self.initialized = true;
        self
    }

    pub fn with_empty_seen_boundary(mut self) -> Self {
        self.initialized = true;
        self
    }

    fn has_pending_backlog(&self) -> bool {
        self.backlog_next.is_some()
            || !self.pending_fresh.is_empty()
            || !self.pending_seen_ids.is_empty()
    }

    pub async fn poll_once(&mut self) -> Result<Vec<ListMessage>> {
        match self.poll_once_transaction().await {
            Ok(mut update) => {
                let messages = std::mem::take(&mut update.messages);
                self.apply_update(update);
                Ok(messages)
            }
            Err(error) => {
                if let Some(update) = error.update {
                    self.apply_update(update);
                }
                Err(error.error)
            }
        }
    }

    async fn poll_once_transaction(
        &self,
    ) -> std::result::Result<MessagePollUpdate, MessagePollError> {
        if self.backlog_next.is_none() && !self.pending_fresh.is_empty() {
            let mut fresh = self.pending_fresh.clone();
            fresh.reverse();
            return Ok(MessagePollUpdate {
                messages: fresh,
                seen_ids: self.pending_seen_ids.clone(),
                pending_fresh: Vec::new(),
                pending_seen_ids: Vec::new(),
                backlog_next: None,
                initialized: true,
                commit_seen: true,
            });
        }

        let mut page = if let Some(next) = self.backlog_next.clone() {
            self.client
                .next_page(next.clone())
                .await
                .map_err(|error| MessagePollError {
                    error,
                    update: None,
                })?
        } else {
            let mut params = ListMessages::room(self.room_id.clone());
            params.max = Some(self.config.page_size);
            self.client
                .list_messages(&params)
                .await
                .map_err(|error| MessagePollError {
                    error,
                    update: None,
                })?
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
            )
            .map_err(|error| MessagePollError {
                error,
                update: None,
            })?;

            if saw_known_message && self.initialized {
                break;
            }
            let Some(next) = page.next.take() else {
                break;
            };
            if page_index + 1 >= max_pages {
                if self.initialized || self.config.emit_existing_on_first_poll {
                    return Ok(MessagePollUpdate {
                        messages: Vec::new(),
                        seen_ids: Vec::new(),
                        pending_fresh: fresh,
                        pending_seen_ids: new_ids,
                        backlog_next: Some(next),
                        initialized: true,
                        commit_seen: false,
                    });
                }
                break;
            }
            page = match self.client.next_page(next.clone()).await {
                Ok(page) => page,
                Err(error) => {
                    let update = preserve_pending_on_page_error(
                        self.initialized,
                        self.config.emit_existing_on_first_poll,
                    )
                    .then_some(MessagePollUpdate {
                        messages: Vec::new(),
                        seen_ids: Vec::new(),
                        pending_fresh: fresh,
                        pending_seen_ids: new_ids,
                        backlog_next: Some(next),
                        initialized: true,
                        commit_seen: false,
                    });
                    return Err(MessagePollError { error, update });
                }
            };
        }

        fresh.reverse();
        Ok(MessagePollUpdate {
            messages: fresh,
            seen_ids: new_ids,
            pending_fresh: Vec::new(),
            pending_seen_ids: Vec::new(),
            backlog_next: None,
            initialized: true,
            commit_seen: true,
        })
    }

    fn apply_update(&mut self, update: MessagePollUpdate) {
        if update.commit_seen {
            self.seen
                .remember_newest_first(update.seen_ids, self.config.max_seen_ids);
        }
        self.pending_fresh = update.pending_fresh;
        self.pending_seen_ids = update.pending_seen_ids;
        self.backlog_next = update.backlog_next;
        self.initialized = update.initialized;
    }

    pub fn spawn(mut self) -> mpsc::Receiver<Result<ListMessage>> {
        let (sender, receiver) = mpsc::channel(256);
        tokio::spawn(async move {
            let mut interval = time::interval(effective_poll_interval(self.config.interval));
            loop {
                if !poll_tick_or_receiver_open(&sender, &mut interval).await {
                    return;
                }
                if sender.is_closed() {
                    return;
                }
                let result = tokio::select! {
                    _ = sender.closed() => return,
                    result = self.poll_once() => result,
                };
                match result {
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

struct RoomPollerEntry {
    room: Room,
    poller: MessagePoller,
}

/// Multi-room message poller for generic accounts joined to many spaces.
pub struct MultiRoomMessagePoller {
    client: WebexClient,
    config: MultiRoomPollingConfig,
    rooms: BTreeMap<String, RoomPollerEntry>,
    inactive_rooms: BTreeMap<String, RoomPollerEntry>,
    inactive_room_order: VecDeque<String>,
    checkpoints: BTreeMap<String, (Vec<String>, bool)>,
    rooms_initialized: bool,
    last_room_discovery: Option<Instant>,
}

impl MultiRoomMessagePoller {
    pub fn new(client: WebexClient) -> Self {
        Self {
            client,
            config: MultiRoomPollingConfig::default(),
            rooms: BTreeMap::new(),
            inactive_rooms: BTreeMap::new(),
            inactive_room_order: VecDeque::new(),
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
            .map(|checkpoint| {
                (
                    checkpoint.room_id,
                    (checkpoint.seen_message_ids, checkpoint.initialize_empty),
                )
            })
            .collect();
        self
    }

    pub fn room_ids(&self) -> Vec<String> {
        self.rooms.keys().cloned().collect()
    }

    pub async fn refresh_rooms(&mut self) -> Result<Vec<Room>> {
        self.refresh_rooms_with_discovery_timeout(self.config.room_discovery_timeout)
            .await
    }

    async fn refresh_rooms_with_discovery_timeout(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Vec<Room>> {
        let discovered =
            discover_joined_rooms_with_timeout(&self.client, &self.config.discovery, timeout).await;
        self.last_room_discovery = Some(Instant::now());
        let discovered = discovered?;
        let mut next_rooms: BTreeMap<String, RoomPollerEntry> = BTreeMap::new();
        let mut next_inactive_rooms = std::mem::take(&mut self.inactive_rooms);
        let mut next_inactive_room_order = std::mem::take(&mut self.inactive_room_order);
        for room in discovered.iter().cloned() {
            let Some(room_id) = room.id.clone() else {
                continue;
            };
            if let Some(existing) = next_rooms.get_mut(&room_id) {
                existing.room = room;
                continue;
            }
            if let Some(mut existing) = self.rooms.remove(&room_id) {
                existing.room = room;
                next_rooms.insert(room_id, existing);
            } else if let Some(mut existing) = next_inactive_rooms.remove(&room_id) {
                next_inactive_room_order.retain(|inactive_room_id| inactive_room_id != &room_id);
                existing.room = room;
                next_rooms.insert(room_id, existing);
            } else {
                let mut poller = MessagePoller::new(self.client.clone(), room_id.clone())
                    .with_config(self.config.room_polling.clone());
                if let Some((seen_ids, initialize_empty)) = self.checkpoints.remove(&room_id) {
                    poller = if seen_ids.is_empty() && initialize_empty {
                        poller.with_empty_seen_boundary()
                    } else {
                        poller.with_seen_message_ids(seen_ids)
                    };
                }
                next_rooms.insert(room_id, RoomPollerEntry { room, poller });
            }
        }
        for (room_id, entry) in std::mem::take(&mut self.rooms) {
            remember_inactive_room(
                &mut next_inactive_rooms,
                &mut next_inactive_room_order,
                room_id,
                entry,
            );
        }
        prune_inactive_rooms(
            &mut next_inactive_rooms,
            &mut next_inactive_room_order,
            self.config.max_inactive_rooms,
        );
        self.rooms = next_rooms;
        self.inactive_rooms = next_inactive_rooms;
        self.inactive_room_order = next_inactive_room_order;
        self.rooms_initialized = true;
        Ok(discovered)
    }

    /// Poll discovered rooms once.
    ///
    /// The outer error is reserved for initial room discovery failures. After
    /// rooms are initialized, refresh and per-room polling failures are returned
    /// as individual error events so successful rooms can still make progress.
    pub async fn poll_once(&mut self) -> Result<Vec<Result<RoomMessage>>> {
        let refreshed_this_poll = if !self.rooms_initialized {
            self.refresh_rooms_with_discovery_timeout(self.config.room_discovery_timeout)
                .await?;
            true
        } else {
            false
        };

        let mut errors = Vec::new();
        if !refreshed_this_poll && self.room_refresh_due() {
            if let Err(error) = self
                .refresh_rooms_with_discovery_timeout(self.config.room_discovery_timeout)
                .await
            {
                errors.push(Err(error));
            }
        }

        let room_batch = self.poll_rooms_concurrently().await;
        let mut messages = Vec::new();
        for event in room_batch.events {
            match event {
                Ok(message) => messages.push(message),
                Err(error) => errors.push(Err(error)),
            }
        }
        let mut events = if room_batch.has_pending_backlog {
            Vec::new()
        } else {
            messages.sort_by(compare_room_messages);
            messages.into_iter().map(Ok).collect::<Vec<_>>()
        };
        events.extend(errors);
        for (room_id, mut update) in room_batch.completed_updates {
            if room_batch.has_pending_backlog && update.commit_seen && !update.messages.is_empty() {
                let mut pending_fresh = std::mem::take(&mut update.messages);
                pending_fresh.reverse();
                update.pending_fresh = pending_fresh;
                update.pending_seen_ids = std::mem::take(&mut update.seen_ids);
                update.commit_seen = false;
            }
            if let Some(current) = self.rooms.get_mut(&room_id) {
                current.poller.apply_update(update);
            }
        }
        Ok(events)
    }

    async fn poll_rooms_concurrently(&self) -> RoomPollBatch {
        let timeout = self.config.room_poll_timeout;
        let max_concurrent = self.config.max_concurrent_room_polls.max(1);
        let mut remaining_rooms = self.rooms.iter();
        let mut active_polls = FuturesUnordered::new();
        let mut events = Vec::new();
        let mut completed_updates = Vec::new();
        let mut has_pending_backlog = false;

        for _ in 0..max_concurrent {
            let Some((room_id, entry)) = remaining_rooms.next() else {
                break;
            };
            active_polls.push(poll_room_entry(room_id, entry, timeout));
        }

        while let Some((room_id, room, had_pending_backlog, poll_result)) =
            active_polls.next().await
        {
            match poll_result {
                RoomPollResult::Completed(result) => match *result {
                    Ok(update) => {
                        if !update.commit_seen {
                            has_pending_backlog = true;
                        }
                        for message in update.messages.iter().cloned() {
                            events.push(Ok(RoomMessage {
                                room_id: room_id.clone(),
                                room: room.clone(),
                                message,
                            }));
                        }
                        completed_updates.push((room_id.clone(), update));
                    }
                    Err(error) => {
                        let mut preserved_update = false;
                        if let Some(update) = error.update {
                            preserved_update = true;
                            if !update.commit_seen {
                                has_pending_backlog = true;
                            }
                            completed_updates.push((room_id.clone(), update));
                        }
                        if had_pending_backlog && !preserved_update {
                            has_pending_backlog = true;
                        }
                        events.push(Err(Error::RoomPoll {
                            room_id,
                            source: Box::new(error.error),
                        }));
                    }
                },
                RoomPollResult::TimedOut(timeout) => {
                    if had_pending_backlog {
                        has_pending_backlog = true;
                    }
                    events.push(Err(Error::RoomPoll {
                        room_id,
                        source: Box::new(Error::Other(format!(
                            "room poll timed out after {:?}",
                            effective_poll_interval(timeout)
                        ))),
                    }));
                }
            }

            if let Some((room_id, entry)) = remaining_rooms.next() {
                active_polls.push(poll_room_entry(room_id, entry, timeout));
            }
        }

        RoomPollBatch {
            events,
            completed_updates,
            has_pending_backlog,
        }
    }

    fn room_refresh_due(&self) -> bool {
        self.last_room_discovery
            .map(|last| last.elapsed() >= self.config.room_refresh_interval)
            .unwrap_or(false)
    }

    /// Spawn a background multi-room poll loop.
    ///
    /// The receiver yields both room messages and recoverable refresh/per-room
    /// errors. A failed room does not stop polling other rooms.
    pub fn spawn(mut self) -> mpsc::Receiver<Result<RoomMessage>> {
        let (sender, receiver) = mpsc::channel(256);
        tokio::spawn(async move {
            let mut interval =
                time::interval(effective_poll_interval(self.config.room_polling.interval));
            loop {
                if !poll_tick_or_receiver_open(&sender, &mut interval).await {
                    return;
                }
                if sender.is_closed() {
                    return;
                }
                let result = tokio::select! {
                    _ = sender.closed() => return,
                    result = self.poll_once() => result,
                };
                match result {
                    Ok(events) => {
                        for event in events {
                            if sender.send(event).await.is_err() {
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

async fn discover_joined_rooms_with_timeout(
    client: &WebexClient,
    config: &RoomDiscoveryConfig,
    timeout: Option<Duration>,
) -> Result<Vec<Room>> {
    let Some(timeout) = timeout else {
        return discover_joined_rooms(client, config).await;
    };
    match time::timeout(
        effective_poll_interval(timeout),
        discover_joined_rooms(client, config),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(Error::Other(format!(
            "joined-room discovery timed out after {:?}",
            effective_poll_interval(timeout)
        ))),
    }
}

struct RoomPollBatch {
    events: Vec<Result<RoomMessage>>,
    completed_updates: Vec<(String, MessagePollUpdate)>,
    has_pending_backlog: bool,
}

enum RoomPollResult {
    Completed(Box<std::result::Result<MessagePollUpdate, MessagePollError>>),
    TimedOut(Duration),
}

fn remember_inactive_room(
    inactive_rooms: &mut BTreeMap<String, RoomPollerEntry>,
    inactive_room_order: &mut VecDeque<String>,
    room_id: String,
    entry: RoomPollerEntry,
) {
    inactive_rooms.insert(room_id.clone(), entry);
    inactive_room_order.retain(|inactive_room_id| inactive_room_id != &room_id);
    inactive_room_order.push_back(room_id);
}

fn prune_inactive_rooms(
    inactive_rooms: &mut BTreeMap<String, RoomPollerEntry>,
    inactive_room_order: &mut VecDeque<String>,
    max_inactive_rooms: usize,
) {
    let limit = max_inactive_rooms.max(1);
    inactive_room_order.retain(|room_id| inactive_rooms.contains_key(room_id));
    while inactive_rooms.len() > limit {
        let Some(room_id) = inactive_room_order.pop_front() else {
            let Some(room_id) = inactive_rooms.keys().next().cloned() else {
                break;
            };
            inactive_rooms.remove(&room_id);
            continue;
        };
        inactive_rooms.remove(&room_id);
    }
}

async fn poll_room_entry(
    room_id: &str,
    entry: &RoomPollerEntry,
    timeout: Option<Duration>,
) -> (String, Room, bool, RoomPollResult) {
    let had_pending_backlog = entry.poller.has_pending_backlog();
    let result = poll_room_once(&entry.poller, timeout).await;
    (
        room_id.to_owned(),
        entry.room.clone(),
        had_pending_backlog,
        result,
    )
}

async fn poll_room_once(poller: &MessagePoller, timeout: Option<Duration>) -> RoomPollResult {
    let Some(timeout) = timeout else {
        return RoomPollResult::Completed(Box::new(poller.poll_once_transaction().await));
    };
    match time::timeout(
        effective_poll_interval(timeout),
        poller.poll_once_transaction(),
    )
    .await
    {
        Ok(result) => RoomPollResult::Completed(Box::new(result)),
        Err(_) => RoomPollResult::TimedOut(timeout),
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
        time::{Duration, Instant},
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{mpsc, oneshot},
        task::JoinHandle,
        time,
    };
    use url::Url;

    use crate::{Error, WebexClient, types::Message};

    use super::{
        MessagePoller, MultiRoomMessagePoller, MultiRoomPollingConfig, PollingConfig,
        RoomCheckpoint, RoomDiscoveryConfig, SeenMessageIds, collect_page_messages,
        discover_joined_rooms, effective_poll_interval, ensure_pending_within_limit,
        poll_tick_or_receiver_open, preserve_pending_on_page_error,
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

    #[tokio::test]
    async fn poll_tick_stops_when_receiver_is_closed() {
        let (sender, receiver) = mpsc::channel::<()>(1);
        let mut interval = time::interval(Duration::from_secs(60));
        drop(receiver);

        assert!(!poll_tick_or_receiver_open(&sender, &mut interval).await);
    }

    #[tokio::test]
    async fn message_poller_spawn_cancels_pending_poll_when_receiver_closes() {
        let (base_url, requests, request_started, client_closed) =
            spawn_pending_request_server(Vec::new()).await;
        let receiver = MessagePoller::new(client_for(base_url), "room-a")
            .with_config(PollingConfig {
                interval: Duration::from_millis(1),
                ..PollingConfig::default()
            })
            .spawn();

        time::timeout(Duration::from_secs(1), request_started)
            .await
            .expect("poll request did not start")
            .unwrap();
        drop(receiver);
        time::timeout(Duration::from_secs(1), client_closed)
            .await
            .expect("pending poll did not cancel after receiver close")
            .unwrap();

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("GET /v1/messages?"));
        assert!(requests[0].contains("roomId=room-a"));
    }

    #[tokio::test]
    async fn multi_room_poller_spawn_cancels_pending_poll_when_receiver_closes() {
        let (base_url, requests, request_started, client_closed) =
            spawn_pending_request_server(vec![MockResponse::json(
                r#"{"items":[{"id":"room-a","title":"Room A"}]}"#,
            )])
            .await;
        let receiver = MultiRoomMessagePoller::new(client_for(base_url))
            .with_config(MultiRoomPollingConfig {
                room_poll_timeout: None,
                room_polling: PollingConfig {
                    interval: Duration::from_millis(1),
                    ..PollingConfig::default()
                },
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])])
            .spawn();

        time::timeout(Duration::from_secs(1), request_started)
            .await
            .expect("room poll request did not start")
            .unwrap();
        drop(receiver);
        time::timeout(Duration::from_secs(1), client_closed)
            .await
            .expect("pending room poll did not cancel after receiver close")
            .unwrap();

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET /v1/rooms?"));
        assert!(requests[1].starts_with("GET /v1/messages?"));
        assert!(requests[1].contains("roomId=room-a"));
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

    #[test]
    fn empty_seeded_message_poller_is_not_initialized() {
        let poller = MessagePoller::new(dummy_client(), "room-1")
            .with_seen_message_ids(std::iter::empty::<&str>());

        assert!(!poller.initialized);
        assert!(poller.seen.ids.is_empty());
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
    async fn discover_joined_rooms_rejects_exact_limit_with_next_page() {
        let (base_url, requests) = spawn_two_page_room_server(
            r#"{"items":[{"id":"room-1"}]}"#,
            r#"{"items":[{"id":"room-2"}]}"#,
        )
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
        assert_eq!(requests.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn discover_joined_rooms_deduplicates_before_max_room_limit() {
        let (base_url, requests) = spawn_sequence_server(vec![MockResponse::json(
            r#"{"items":[{"id":"room-1","title":"Room 1"},{"id":"room-1","title":"Room 1 Updated"}]}"#,
        )])
        .await;
        let client = client_for(base_url);

        let rooms = discover_joined_rooms(
            &client,
            &RoomDiscoveryConfig {
                max_rooms: 1,
                ..RoomDiscoveryConfig::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].id.as_deref(), Some("room-1"));
        assert_eq!(rooms[0].title.as_deref(), Some("Room 1 Updated"));
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn discover_joined_rooms_deduplicates_across_pages_before_max_room_limit() {
        let (base_url, requests) = spawn_two_page_room_server(
            r#"{"items":[{"id":"room-1","title":"Room 1"}]}"#,
            r#"{"items":[{"id":"room-1","title":"Room 1 Updated"}]}"#,
        )
        .await;
        let client = client_for(base_url);

        let rooms = discover_joined_rooms(
            &client,
            &RoomDiscoveryConfig {
                max_rooms: 1,
                ..RoomDiscoveryConfig::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].id.as_deref(), Some("room-1"));
        assert_eq!(rooms[0].title.as_deref(), Some("Room 1 Updated"));
        assert_eq!(requests.await.unwrap().len(), 2);
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
    async fn multi_room_poller_deduplicates_discovery_without_losing_checkpoint() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(
                r#"{"items":[{"id":"room-a","title":"Room A"},{"id":"room-a","title":"Room A Updated"}]}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        let events = poller.poll_once().await.unwrap();
        let messages = events
            .into_iter()
            .map(|event| event.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(poller.room_ids(), ["room-a"]);
        assert_eq!(
            messages
                .iter()
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new"]
        );
        assert_eq!(messages[0].room.title.as_deref(), Some("Room A Updated"));

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].contains("roomId=room-a"));
    }

    #[tokio::test]
    async fn multi_room_poller_restores_state_when_room_reappears() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
            MockResponse::json(r#"{"items":[]}"#),
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A Restored"}]}"#),
            MockResponse::json(
                r#"{"items":[{"id":"a-next","roomId":"room-a","text":"A next","created":"2026-06-17T00:00:02Z"},{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_refresh_interval: Duration::ZERO,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        let first_events = poller.poll_once().await.unwrap();
        assert_eq!(
            first_events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new"]
        );

        let missing_events = poller.poll_once().await.unwrap();
        assert!(missing_events.is_empty());
        assert!(poller.room_ids().is_empty());

        let restored_events = poller.poll_once().await.unwrap();
        let restored_messages = restored_events
            .into_iter()
            .map(|event| event.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            restored_messages
                .iter()
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-next"]
        );
        assert_eq!(
            restored_messages[0].room.title.as_deref(),
            Some("Room A Restored")
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 5);
        assert!(requests[1].contains("roomId=room-a"));
        assert!(requests[4].contains("roomId=room-a"));
    }

    #[tokio::test]
    async fn multi_room_poller_prunes_inactive_room_state() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::json(r#"{"items":[{"id":"room-b","title":"Room B"}]}"#),
            MockResponse::json(r#"{"items":[{"id":"room-c","title":"Room C"}]}"#),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client).with_config(MultiRoomPollingConfig {
            max_inactive_rooms: 1,
            ..MultiRoomPollingConfig::default()
        });

        poller.refresh_rooms().await.unwrap();
        poller.refresh_rooms().await.unwrap();
        poller.refresh_rooms().await.unwrap();

        assert_eq!(poller.room_ids(), ["room-c"]);
        assert_eq!(
            poller.inactive_rooms.keys().cloned().collect::<Vec<_>>(),
            ["room-b"]
        );
        assert_eq!(
            poller.inactive_room_order,
            VecDeque::from(["room-b".to_owned()])
        );
        assert_eq!(requests.await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn multi_room_poller_discovers_rooms_and_emits_checkpointed_catchup() {
        let (base_url, requests) = spawn_route_server(
            vec![
                (
                    "/v1/rooms?",
                    MockResponse::json(
                        r#"{"items":[{"id":"room-b","title":"Room B"},{"id":"room-a","title":"Room A"}]}"#,
                    ),
                ),
                (
                    "roomId=room-a",
                    MockResponse::json(
                        r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:02Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:01Z"}]}"#,
                    ),
                ),
                (
                    "roomId=room-b",
                    MockResponse::json(
                        r#"{"items":[{"id":"b-new","roomId":"room-b","text":"B new","created":"2026-06-17T00:00:01Z"},{"id":"b-seen","roomId":"room-b","text":"B seen","created":"2026-06-17T00:00:00Z"}]}"#,
                    ),
                ),
            ],
            3,
        )
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_polling: PollingConfig {
                    page_size: 10,
                    ..PollingConfig::default()
                },
                max_concurrent_room_polls: 1,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([
                RoomCheckpoint::new("room-a", ["a-seen"]),
                RoomCheckpoint::new("room-b", ["b-seen"]),
            ]);

        let events = poller.poll_once().await.unwrap();
        let messages = events
            .into_iter()
            .map(|event| event.unwrap())
            .collect::<Vec<_>>();

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

    #[tokio::test]
    async fn multi_room_poller_holds_messages_until_room_backlogs_drain() {
        let (base_url, requests) = spawn_sequence_server_with_address(|address| {
            let next = format!(r#"<http://{address}/v1/messages?page=2>; rel="next""#);
            vec![
                MockResponse::json(
                    r#"{"items":[{"id":"room-a","title":"Room A"},{"id":"room-b","title":"Room B"}]}"#,
                ),
                MockResponse::json(
                    r#"{"items":[{"id":"a-newer","roomId":"room-a","text":"A newer","created":"2026-06-17T00:00:03Z"}]}"#,
                )
                .with_header("Link", next),
                MockResponse::json(
                    r#"{"items":[{"id":"b-later","roomId":"room-b","text":"B later","created":"2026-06-17T00:00:04Z"},{"id":"b-seen","roomId":"room-b","text":"B seen","created":"2026-06-17T00:00:00Z"}]}"#,
                ),
                MockResponse::json(
                    r#"{"items":[{"id":"a-older","roomId":"room-a","text":"A older","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
                ),
            ]
        })
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_polling: PollingConfig {
                    page_size: 10,
                    max_pages_per_poll: 1,
                    ..PollingConfig::default()
                },
                max_concurrent_room_polls: 1,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([
                RoomCheckpoint::new("room-a", ["a-seen"]),
                RoomCheckpoint::new("room-b", ["b-seen"]),
            ]);

        let first_events = poller.poll_once().await.unwrap();
        let second_events = poller.poll_once().await.unwrap();
        let messages = second_events
            .into_iter()
            .map(|event| event.unwrap())
            .collect::<Vec<_>>();

        assert!(first_events.is_empty());
        assert_eq!(
            messages
                .iter()
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-older", "a-newer", "b-later"]
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[1].contains("roomId=room-a"));
        assert!(requests[2].contains("roomId=room-b"));
        assert!(requests[3].starts_with("GET /v1/messages?page=2"));
    }

    #[tokio::test]
    async fn multi_room_poller_holds_messages_when_pending_backlog_room_fails() {
        let (base_url, requests) = spawn_sequence_server_with_address(|address| {
            let next = format!(r#"<http://{address}/v1/messages?page=2>; rel="next""#);
            vec![
                MockResponse::json(
                    r#"{"items":[{"id":"room-a","title":"Room A"},{"id":"room-b","title":"Room B"}]}"#,
                ),
                MockResponse::json(
                    r#"{"items":[{"id":"a-newer","roomId":"room-a","text":"A newer","created":"2026-06-17T00:00:03Z"}]}"#,
                )
                .with_header("Link", next),
                MockResponse::json(
                    r#"{"items":[{"id":"b-later","roomId":"room-b","text":"B later","created":"2026-06-17T00:00:04Z"},{"id":"b-seen","roomId":"room-b","text":"B seen","created":"2026-06-17T00:00:00Z"}]}"#,
                ),
                MockResponse::status_json(
                    "503 Service Unavailable",
                    r#"{"message":"room a unavailable"}"#,
                ),
                MockResponse::json(
                    r#"{"items":[{"id":"a-older","roomId":"room-a","text":"A older","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
                ),
            ]
        })
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_polling: PollingConfig {
                    page_size: 10,
                    max_pages_per_poll: 1,
                    ..PollingConfig::default()
                },
                max_concurrent_room_polls: 1,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([
                RoomCheckpoint::new("room-a", ["a-seen"]),
                RoomCheckpoint::new("room-b", ["b-seen"]),
            ]);

        let first_events = poller.poll_once().await.unwrap();
        let second_events = poller.poll_once().await.unwrap();
        let third_events = poller.poll_once().await.unwrap();
        let second_errors = second_events
            .iter()
            .filter_map(|event| event.as_ref().err())
            .collect::<Vec<_>>();
        let third_messages = third_events
            .into_iter()
            .map(|event| event.unwrap())
            .collect::<Vec<_>>();

        assert!(first_events.is_empty());
        assert_eq!(second_errors.len(), 1);
        assert!(second_events.iter().all(Result::is_err));
        assert_eq!(
            third_messages
                .iter()
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-older", "a-newer", "b-later"]
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 5);
        assert!(requests[1].contains("roomId=room-a"));
        assert!(requests[2].contains("roomId=room-b"));
        assert!(requests[3].starts_with("GET /v1/messages?page=2"));
        assert!(requests[4].starts_with("GET /v1/messages?page=2"));
    }

    #[tokio::test]
    async fn multi_room_poller_known_empty_checkpoint_emits_catchup() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_room_checkpoints([RoomCheckpoint::known_empty("room-a")]);

        let events = poller.poll_once().await.unwrap();
        let messages = events
            .into_iter()
            .map(|event| event.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            messages
                .iter()
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new"]
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].contains("roomId=room-a"));
    }

    #[tokio::test]
    async fn multi_room_poller_commits_empty_baseline_while_other_room_has_backlog() {
        let (base_url, requests) = spawn_sequence_server_with_address(|address| {
            let next = format!(r#"<http://{address}/v1/messages?page=2>; rel="next""#);
            vec![
                MockResponse::json(
                    r#"{"items":[{"id":"room-a","title":"Room A"},{"id":"room-b","title":"Room B"}]}"#,
                ),
                MockResponse::json(
                    r#"{"items":[{"id":"a-newer","roomId":"room-a","text":"A newer","created":"2026-06-17T00:00:03Z"}]}"#,
                )
                .with_header("Link", next),
                MockResponse::json(r#"{"items":[]}"#),
                MockResponse::json(
                    r#"{"items":[{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
                ),
                MockResponse::json(
                    r#"{"items":[{"id":"b-after-baseline","roomId":"room-b","text":"B after","created":"2026-06-17T00:00:04Z"}]}"#,
                ),
            ]
        })
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_polling: PollingConfig {
                    page_size: 10,
                    max_pages_per_poll: 1,
                    ..PollingConfig::default()
                },
                max_concurrent_room_polls: 1,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        let first_events = poller.poll_once().await.unwrap();
        let second_events = poller.poll_once().await.unwrap();
        let messages = second_events
            .into_iter()
            .map(|event| event.unwrap())
            .collect::<Vec<_>>();

        assert!(first_events.is_empty());
        assert_eq!(
            messages
                .iter()
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-newer", "b-after-baseline"]
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 5);
        assert!(requests[1].contains("roomId=room-a"));
        assert!(requests[2].contains("roomId=room-b"));
        assert!(requests[3].starts_with("GET /v1/messages?page=2"));
        assert!(requests[4].contains("roomId=room-b"));
    }

    fn message(id: &str) -> Message {
        Message {
            id: Some(id.to_owned()),
            ..Message::default()
        }
    }

    #[tokio::test]
    async fn multi_room_poller_times_out_hung_room_poll() {
        let (base_url, _requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::delay_json(
                Duration::from_secs(60),
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_poll_timeout: Some(Duration::from_millis(1)),
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        let events = poller.poll_once().await.unwrap();

        assert_eq!(events.len(), 1);
        let error = events.into_iter().next().unwrap().unwrap_err();
        match error {
            Error::RoomPoll { room_id, source } => {
                assert_eq!(room_id, "room-a");
                assert!(source.to_string().contains("room poll timed out"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn multi_room_poller_cancellation_keeps_room_state() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::delay_json(
                Duration::from_millis(50),
                r#"{"items":[{"id":"stale","roomId":"room-a","text":"stale","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_poll_timeout: None,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        poller.refresh_rooms().await.unwrap();
        tokio::time::timeout(Duration::from_millis(1), poller.poll_once())
            .await
            .unwrap_err();

        assert_eq!(poller.room_ids(), ["room-a"]);
        let events = poller.poll_once().await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new"]
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[1].contains("roomId=room-a"));
        assert!(requests[2].contains("roomId=room-a"));
    }

    #[tokio::test]
    async fn multi_room_poller_cancellation_after_fast_room_keeps_seen_state_uncommitted() {
        let (base_url, _requests) = spawn_route_server(
            vec![
                (
                    "/v1/rooms?",
                    MockResponse::json(
                        r#"{"items":[{"id":"room-a","title":"Room A"},{"id":"room-b","title":"Room B"}]}"#,
                    ),
                ),
                (
                    "roomId=room-a",
                    MockResponse::json(
                        r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
                    ),
                ),
                (
                    "roomId=room-b",
                    MockResponse::delay_json(
                        Duration::from_millis(50),
                        r#"{"items":[{"id":"b-new","roomId":"room-b","text":"B new","created":"2026-06-17T00:00:02Z"},{"id":"b-seen","roomId":"room-b","text":"B seen","created":"2026-06-17T00:00:00Z"}]}"#,
                    ),
                ),
            ],
            5,
        )
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                max_concurrent_room_polls: 2,
                room_poll_timeout: None,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([
                RoomCheckpoint::new("room-a", ["a-seen"]),
                RoomCheckpoint::new("room-b", ["b-seen"]),
            ]);

        poller.refresh_rooms().await.unwrap();
        tokio::time::timeout(Duration::from_millis(10), poller.poll_once())
            .await
            .unwrap_err();

        assert_eq!(poller.room_ids(), ["room-a", "room-b"]);
        let events = poller.poll_once().await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new", "b-new"]
        );
    }

    #[tokio::test]
    async fn multi_room_poller_polls_later_room_while_earlier_rooms_timeout() {
        let (base_url, _requests) = spawn_route_server(
            vec![
                (
                    "/v1/rooms?",
                    MockResponse::json(
                        r#"{"items":[{"id":"room-a","title":"Room A"},{"id":"room-b","title":"Room B"},{"id":"room-c","title":"Room C"}]}"#,
                    ),
                ),
                (
                    "roomId=room-a",
                    MockResponse::delay_json(Duration::from_secs(60), r#"{"items":[]}"#),
                ),
                (
                    "roomId=room-b",
                    MockResponse::delay_json(Duration::from_secs(60), r#"{"items":[]}"#),
                ),
                (
                    "roomId=room-c",
                    MockResponse::json(
                        r#"{"items":[{"id":"c-new","roomId":"room-c","text":"C new","created":"2026-06-17T00:00:02Z"},{"id":"c-seen","roomId":"room-c","text":"C seen","created":"2026-06-17T00:00:00Z"}]}"#,
                    ),
                ),
            ],
            4,
        )
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_poll_timeout: Some(Duration::from_millis(10)),
                max_concurrent_room_polls: 3,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([
                RoomCheckpoint::new("room-a", ["a-seen"]),
                RoomCheckpoint::new("room-b", ["b-seen"]),
                RoomCheckpoint::new("room-c", ["c-seen"]),
            ]);

        let events = poller.poll_once().await.unwrap();

        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["c-new"]
        );
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().err())
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn multi_room_poller_reports_room_error_without_dropping_other_rooms() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(
                r#"{"items":[{"id":"room-a","title":"Room A"},{"id":"room-b","title":"Room B"},{"id":"room-c","title":"Room C"}]}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
            MockResponse::status_json(
                "500 Internal Server Error",
                r#"{"message":"room b unavailable"}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"c-new","roomId":"room-c","text":"C new","created":"2026-06-17T00:00:02Z"},{"id":"c-seen","roomId":"room-c","text":"C seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client).with_room_checkpoints([
            RoomCheckpoint::new("room-a", ["a-seen"]),
            RoomCheckpoint::new("room-b", ["b-seen"]),
            RoomCheckpoint::new("room-c", ["c-seen"]),
        ]);

        let events = poller.poll_once().await.unwrap();

        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new", "c-new"]
        );
        let errors = events
            .iter()
            .filter_map(|event| event.as_ref().err())
            .collect::<Vec<_>>();
        assert_eq!(errors.len(), 1);
        match errors[0] {
            Error::RoomPoll { room_id, source } => {
                assert_eq!(room_id, "room-b");
                assert!(source.to_string().contains("500 Internal Server Error"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[1].contains("roomId=room-a"));
        assert!(requests[2].contains("roomId=room-b"));
        assert!(requests[3].contains("roomId=room-c"));
    }

    #[tokio::test]
    async fn multi_room_poller_times_out_hung_refresh_and_polls_existing_rooms() {
        let (base_url, _requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::delay_json(
                Duration::from_secs(60),
                r#"{"items":[{"id":"room-a","title":"Room A"}]}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_discovery_timeout: Some(Duration::from_millis(1)),
                room_refresh_interval: Duration::ZERO,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        poller.config.room_discovery_timeout = None;
        poller.refresh_rooms().await.unwrap();
        poller.config.room_discovery_timeout = Some(Duration::from_millis(1));
        let events = poller.poll_once().await.unwrap();

        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new"]
        );
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().err())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn multi_room_poller_cancelled_refresh_does_not_advance_retry_interval() {
        let (base_url, _requests) = spawn_tolerant_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::delay_json(
                Duration::from_millis(50),
                r#"{"items":[{"id":"room-a","title":"Room A"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_discovery_timeout: None,
                room_refresh_interval: Duration::from_secs(60),
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        poller.refresh_rooms().await.unwrap();
        let previous_discovery = Instant::now() - Duration::from_secs(120);
        poller.last_room_discovery = Some(previous_discovery);
        tokio::time::timeout(Duration::from_millis(1), poller.poll_once())
            .await
            .unwrap_err();

        assert_eq!(poller.last_room_discovery, Some(previous_discovery));
        assert!(poller.room_refresh_due());
    }

    #[tokio::test]
    async fn multi_room_poller_slow_refresh_timeout_starts_retry_interval_after_failure() {
        let (base_url, _requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::delay_json(Duration::from_millis(50), r#"{"items":[]}"#),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-next","roomId":"room-a","text":"A next","created":"2026-06-17T00:00:02Z"},{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_discovery_timeout: Some(Duration::from_millis(1)),
                room_refresh_interval: Duration::from_secs(60),
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        poller.config.room_discovery_timeout = None;
        poller.refresh_rooms().await.unwrap();
        poller.config.room_discovery_timeout = Some(Duration::from_millis(1));
        poller.last_room_discovery = Some(Instant::now() - Duration::from_secs(120));

        let first_events = poller.poll_once().await.unwrap();
        let second_events = poller.poll_once().await.unwrap();

        assert_eq!(
            first_events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new"]
        );
        assert_eq!(
            first_events
                .iter()
                .filter_map(|event| event.as_ref().err())
                .count(),
            1
        );
        assert_eq!(
            second_events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-next"]
        );
        assert!(second_events.iter().all(Result::is_ok));
    }

    #[tokio::test]
    async fn multi_room_poller_refresh_failure_is_throttled_by_refresh_interval() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::status_json(
                "503 Service Unavailable",
                r#"{"message":"room discovery unavailable"}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-next","roomId":"room-a","text":"A next","created":"2026-06-17T00:00:02Z"},{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_refresh_interval: Duration::from_secs(60),
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        poller.refresh_rooms().await.unwrap();
        poller.last_room_discovery = Some(Instant::now() - Duration::from_secs(120));

        let first_events = poller.poll_once().await.unwrap();
        let second_events = poller.poll_once().await.unwrap();

        assert_eq!(
            first_events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new"]
        );
        assert_eq!(
            first_events
                .iter()
                .filter_map(|event| event.as_ref().err())
                .count(),
            1
        );
        assert_eq!(
            second_events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-next"]
        );
        assert!(second_events.iter().all(Result::is_ok));

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[0].starts_with("GET /v1/rooms?"));
        assert!(requests[1].starts_with("GET /v1/rooms?"));
        assert!(requests[2].contains("roomId=room-a"));
        assert!(requests[3].contains("roomId=room-a"));
    }

    #[tokio::test]
    async fn multi_room_poller_refresh_failure_does_not_stop_existing_room_polling() {
        let (base_url, requests) = spawn_sequence_server(vec![
            MockResponse::json(r#"{"items":[{"id":"room-a","title":"Room A"}]}"#),
            MockResponse::status_json(
                "503 Service Unavailable",
                r#"{"message":"room discovery unavailable"}"#,
            ),
            MockResponse::json(
                r#"{"items":[{"id":"a-new","roomId":"room-a","text":"A new","created":"2026-06-17T00:00:01Z"},{"id":"a-seen","roomId":"room-a","text":"A seen","created":"2026-06-17T00:00:00Z"}]}"#,
            ),
        ])
        .await;
        let client = client_for(base_url);
        let mut poller = MultiRoomMessagePoller::new(client)
            .with_config(MultiRoomPollingConfig {
                room_refresh_interval: Duration::ZERO,
                ..MultiRoomPollingConfig::default()
            })
            .with_room_checkpoints([RoomCheckpoint::new("room-a", ["a-seen"])]);

        poller.refresh_rooms().await.unwrap();
        let events = poller.poll_once().await.unwrap();

        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .map(|message| message.message.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["a-new"]
        );
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.as_ref().err())
                .count(),
            1
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("GET /v1/rooms?"));
        assert!(requests[1].starts_with("GET /v1/rooms?"));
        assert!(requests[2].contains("roomId=room-a"));
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

    #[derive(Clone)]
    struct MockResponse {
        status: &'static str,
        headers: Vec<(&'static str, String)>,
        delay: Option<Duration>,
        body: String,
    }

    impl MockResponse {
        fn json(body: impl Into<String>) -> Self {
            Self::status_json("200 OK", body)
        }

        fn status_json(status: &'static str, body: impl Into<String>) -> Self {
            Self {
                status,
                headers: Vec::new(),
                delay: None,
                body: body.into(),
            }
        }

        fn delay_json(delay: Duration, body: impl Into<String>) -> Self {
            Self {
                delay: Some(delay),
                ..Self::json(body)
            }
        }

        fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
            self.headers.push((name, value.into()));
            self
        }
    }

    async fn spawn_two_page_room_server(
        first_body: &str,
        second_body: &str,
    ) -> (Url, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let link = format!(r#"<http://{address}/v1/rooms?page=2>; rel="next""#);
        let responses = VecDeque::from(vec![
            MockResponse::json(first_body).with_header("Link", link),
            MockResponse::json(second_body),
        ]);
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            let mut responses = responses;
            while let Some(response) = responses.pop_front() {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                requests.push(String::from_utf8_lossy(&request).into_owned());
                write_mock_response(&mut stream, response).await;
            }
            requests
        });

        (
            Url::parse(&format!("http://{address}/v1/")).unwrap(),
            server,
        )
    }

    async fn spawn_pending_request_server(
        responses_before_pending: Vec<MockResponse>,
    ) -> (
        Url,
        JoinHandle<Vec<String>>,
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_started_tx, request_started_rx) = oneshot::channel();
        let (client_closed_tx, client_closed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            let mut responses = VecDeque::from(responses_before_pending);
            while let Some(response) = responses.pop_front() {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                requests.push(String::from_utf8_lossy(&request).into_owned());
                write_mock_response(&mut stream, response).await;
            }

            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            requests.push(String::from_utf8_lossy(&request).into_owned());
            let _ = request_started_tx.send(());
            let mut buffer = [0_u8; 1];
            let _ = stream.read(&mut buffer).await;
            let _ = client_closed_tx.send(());
            requests
        });

        (
            Url::parse(&format!("http://{address}/v1/")).unwrap(),
            server,
            request_started_rx,
            client_closed_rx,
        )
    }

    async fn spawn_sequence_server_with_address<F>(
        make_responses: F,
    ) -> (Url, JoinHandle<Vec<String>>)
    where
        F: FnOnce(std::net::SocketAddr) -> Vec<MockResponse>,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let responses = make_responses(address);
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            let mut responses = VecDeque::from(responses);
            while let Some(response) = responses.pop_front() {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                requests.push(String::from_utf8_lossy(&request).into_owned());
                write_mock_response(&mut stream, response).await;
            }
            requests
        });

        (
            Url::parse(&format!("http://{address}/v1/")).unwrap(),
            server,
        )
    }

    async fn spawn_sequence_server(responses: Vec<MockResponse>) -> (Url, JoinHandle<Vec<String>>) {
        spawn_sequence_server_with_options(responses, false).await
    }

    async fn spawn_tolerant_sequence_server(
        responses: Vec<MockResponse>,
    ) -> (Url, JoinHandle<Vec<String>>) {
        spawn_sequence_server_with_options(responses, true).await
    }

    async fn spawn_sequence_server_with_options(
        responses: Vec<MockResponse>,
        allow_abandoned_connections: bool,
    ) -> (Url, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            let mut responses = VecDeque::from(responses);
            let mut response_tasks = Vec::new();
            while !responses.is_empty() {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = if allow_abandoned_connections {
                    let Some(request) = read_http_request_opt(&mut stream, false).await else {
                        continue;
                    };
                    request
                } else {
                    read_http_request(&mut stream).await
                };
                let response = responses.pop_front().unwrap();
                requests.push(String::from_utf8_lossy(&request).into_owned());
                response_tasks.push(tokio::spawn(async move {
                    write_mock_response(&mut stream, response).await;
                }));
            }
            for task in response_tasks {
                task.await.unwrap();
            }
            requests
        });

        (
            Url::parse(&format!("http://{address}/v1/")).unwrap(),
            server,
        )
    }

    async fn spawn_route_server(
        routes: Vec<(&'static str, MockResponse)>,
        expected_requests: usize,
    ) -> (Url, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            let mut response_tasks = Vec::new();
            while requests.len() < expected_requests {
                let (mut stream, _) = listener.accept().await.unwrap();
                let Some(request) = read_http_request_opt(&mut stream, false).await else {
                    continue;
                };
                let request = String::from_utf8_lossy(&request).into_owned();
                let response = routes
                    .iter()
                    .find(|(needle, _)| request.contains(needle))
                    .map(|(_, response)| response.clone())
                    .unwrap_or_else(|| {
                        MockResponse::status_json(
                            "404 Not Found",
                            format!(r#"{{"message":"no route for {request:?}"}}"#),
                        )
                    });
                requests.push(request);
                response_tasks.push(tokio::spawn(async move {
                    write_mock_response(&mut stream, response).await;
                }));
            }
            for task in response_tasks {
                task.await.unwrap();
            }
            requests
        });

        (
            Url::parse(&format!("http://{address}/v1/")).unwrap(),
            server,
        )
    }

    async fn write_mock_response(stream: &mut tokio::net::TcpStream, response: MockResponse) {
        if let Some(delay) = response.delay {
            tokio::time::sleep(delay).await;
        }
        let mut headers = String::new();
        for (name, value) in response.headers {
            headers.push_str(&format!("{name}: {value}\r\n"));
        }
        let response = format!(
            "HTTP/1.1 {}\r\nContent-Type: application/json\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.status,
            headers,
            response.body.len(),
            response.body
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        read_http_request_opt(stream, true)
            .await
            .expect("client closed before request completed")
    }

    async fn read_http_request_opt(
        stream: &mut tokio::net::TcpStream,
        assert_complete: bool,
    ) -> Option<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            if read == 0 {
                if assert_complete {
                    panic!("client closed before request completed");
                }
                return None;
            }
            bytes.extend_from_slice(&buffer[..read]);
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                return Some(bytes);
            }
        }
    }
}
