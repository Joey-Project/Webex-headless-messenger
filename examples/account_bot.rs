use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    env,
    fs::{self, OpenOptions},
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, TryLockError,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot},
    task::JoinSet,
    time::{Instant, sleep, timeout},
};
use webex_headless_messenger::{
    AccessTokenProvider, Error, Result, SidecarEvent, TokenSet, WebexClient,
    types::{CreateMessage, Message},
};

const DEFAULT_BIND: &str = "127.0.0.1:8787";
const DEFAULT_EVENT_PATH: &str = "/webex/events";
const DEFAULT_HEALTH_PATH: &str = "/healthz";
const DEFAULT_STATE_FILE: &str = ".codex-tmp/account-bot/processed-message-ids.txt";
const DEFAULT_REPLY_PREFIX: &str = "ack";
const DEFAULT_MAX_PROCESSED_IDS: usize = 10_000;
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 16;
const DEFAULT_MAX_CONCURRENT_CONNECTIONS: usize = 64;
const DEFAULT_REQUEST_READ_TIMEOUT_SECS: u64 = 10;
const DEFAULT_HANDLER_TIMEOUT_SECS: u64 = 8;
const DEFAULT_WEBEX_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_ATTEMPT_LEASE_SECS: u64 = 30;
const DEFAULT_IN_FLIGHT_WAIT_SECS: u64 = 8;
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_HEADER_BYTES: usize = 16 * 1024;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    validate_loopback_bind(&config.bind, config.allow_non_loopback).await?;

    let listener = TcpListener::bind(&config.bind).await?;
    eprintln!("account_bot_listening={}", listener.local_addr()?);
    eprintln!("account_bot_event_path={}", config.event_path);
    eprintln!("account_bot_health_path={}", config.health_path);
    if config.mock {
        eprintln!("account_bot_mock=true");
    }
    if config.expected_forward_token.is_none() {
        eprintln!("account_bot_forward_unauthenticated=true");
    }
    if config.allow_non_loopback {
        eprintln!("account_bot_non_loopback_allowed=true");
    }

    let bot = Arc::new(AccountBot::new(config).await?);
    run_http_loop(listener, bot).await
}

async fn run_http_loop(listener: TcpListener, bot: Arc<AccountBot>) -> Result<()> {
    let max_events = bot.config.max_events;
    let event_semaphore = Arc::new(Semaphore::new(bot.config.max_concurrent_requests));
    let connection_semaphore = Arc::new(Semaphore::new(bot.config.max_concurrent_connections));
    let accepted_events = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(Notify::new());
    let mut tasks = JoinSet::new();

    loop {
        if max_events_reached(
            max_events,
            bot.event_slots.as_ref(),
            bot.completed_event_slots.as_ref(),
            accepted_events.as_ref(),
        ) {
            break;
        }

        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                match connection_semaphore.clone().try_acquire_owned() {
                    Ok(connection_permit) => {
                        tasks.spawn(handle_connection(
                            stream,
                            peer,
                            bot.clone(),
                            accepted_events.clone(),
                            shutdown.clone(),
                            event_semaphore.clone(),
                            connection_permit,
                        ));
                    }
                    Err(_) => {
                        if timeout(
                            Duration::from_secs(1),
                            reject_busy_connection(
                                stream,
                                peer,
                                retry_after_for_lease(bot.config.in_flight_wait),
                            ),
                        )
                        .await
                        .is_err()
                        {
                            eprintln!("account_bot_busy_response_timeout peer={peer}");
                        }
                    }
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = joined {
                    eprintln!("account_bot_connection_task_failed error={error}");
                }
            }
            _ = bot.max_events_notify.notified(), if max_events > 0 => {
                if max_events_reached(
                    max_events,
                    bot.event_slots.as_ref(),
                    bot.completed_event_slots.as_ref(),
                    accepted_events.as_ref(),
                ) {
                    break;
                }
            }
            _ = shutdown.notified(), if max_events > 0 => {
                if max_events_reached(
                    max_events,
                    bot.event_slots.as_ref(),
                    bot.completed_event_slots.as_ref(),
                    accepted_events.as_ref(),
                ) {
                    break;
                }
            }
        }
    }

    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            eprintln!("account_bot_connection_task_failed error={error}");
        }
    }
    Ok(())
}

fn max_event_quota_reached(
    max_events: usize,
    completed_event_slots: &AtomicUsize,
    accepted_events: &AtomicUsize,
) -> bool {
    max_events > 0
        && (completed_event_slots.load(Ordering::Relaxed) >= max_events
            || accepted_events.load(Ordering::Relaxed) >= max_events)
}

fn max_event_admission_closed(
    max_events: usize,
    event_slots: &AtomicUsize,
    completed_event_slots: &AtomicUsize,
    accepted_events: &AtomicUsize,
) -> bool {
    max_events > 0
        && (event_slots.load(Ordering::Relaxed) >= max_events
            || max_event_quota_reached(max_events, completed_event_slots, accepted_events))
}

fn max_events_reached(
    max_events: usize,
    event_slots: &AtomicUsize,
    completed_event_slots: &AtomicUsize,
    accepted_events: &AtomicUsize,
) -> bool {
    max_event_quota_reached(max_events, completed_event_slots, accepted_events)
        && event_slots.load(Ordering::Relaxed) == completed_event_slots.load(Ordering::Relaxed)
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    bot: Arc<AccountBot>,
    accepted_events: Arc<AtomicUsize>,
    shutdown: Arc<Notify>,
    semaphore: Arc<Semaphore>,
    _connection_permit: OwnedSemaphorePermit,
) {
    let response = match timeout(bot.config.request_read_timeout, read_request(&mut stream)).await {
        Ok(Ok(request)) => {
            let is_event_post = request.path == bot.config.event_path && request.method == "POST";
            let event_permit = if is_event_post {
                match semaphore.try_acquire_owned() {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        return write_final_response(
                            &mut stream,
                            peer,
                            HandleResponse {
                                response: busy_response(retry_after_for_lease(
                                    bot.config.in_flight_wait,
                                )),
                                event_counted: false,
                            },
                            &bot,
                            &accepted_events,
                            &shutdown,
                        )
                        .await;
                    }
                }
            } else {
                None
            };
            handle_parsed_request(&bot, &request, event_permit, &accepted_events).await
        }
        Ok(Err(error)) => HandleResponse {
            response: HttpResponse::json_error(400, error.to_string()),
            event_counted: false,
        },
        Err(_) => HandleResponse {
            response: HttpResponse::json_error(408, "request timeout"),
            event_counted: false,
        },
    };

    write_final_response(
        &mut stream,
        peer,
        response,
        &bot,
        &accepted_events,
        &shutdown,
    )
    .await;
}

async fn handle_parsed_request(
    bot: &AccountBot,
    request: &HttpRequest,
    event_permit: Option<OwnedSemaphorePermit>,
    accepted_events: &AtomicUsize,
) -> HandleResponse {
    match timeout(
        bot.config.handler_timeout,
        bot.handle_request(request, event_permit, accepted_events),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => HandleResponse {
            response: error_response(&BotError::from(error)),
            event_counted: false,
        },
        Err(_) => HandleResponse {
            response: HttpResponse::json_error(503, "handler timeout").with_header(
                "Retry-After",
                retry_after_secs(retry_after_for_lease(bot.config.attempt_lease)),
            ),
            event_counted: false,
        },
    }
}

async fn write_final_response<W>(
    stream: &mut W,
    peer: SocketAddr,
    response: HandleResponse,
    bot: &AccountBot,
    accepted_events: &AtomicUsize,
    shutdown: &Notify,
) where
    W: AsyncWrite + Unpin,
{
    let response_status = response.response.status;
    let accepted = if response.event_counted {
        let accepted = accepted_events.fetch_add(1, Ordering::Relaxed) + 1;
        if bot.config.max_events > 0 && accepted >= bot.config.max_events {
            shutdown.notify_one();
        }
        Some(accepted)
    } else {
        None
    };

    if let Err(error) = write_response(stream, &response.response).await {
        eprintln!("account_bot_response_write_failed peer={peer} error={error}");
        if let Some(accepted) = accepted {
            eprintln!(
                "account_bot_event_handled_from={peer} status={response_status} accepted={accepted} response_write_failed=true"
            );
        }
        return;
    }
    if let Some(accepted) = accepted {
        eprintln!(
            "account_bot_event_handled_from={peer} status={response_status} accepted={accepted}"
        );
    }
}

async fn reject_busy_connection(mut stream: TcpStream, peer: SocketAddr, retry_after: Duration) {
    let response = busy_response(retry_after);
    if let Err(error) = write_response(&mut stream, &response).await {
        eprintln!("account_bot_busy_response_write_failed peer={peer} error={error}");
    }
}

fn busy_response(retry_after: Duration) -> HttpResponse {
    HttpResponse::json_error(503, "server busy")
        .with_header("Retry-After", retry_after_secs(retry_after))
}

#[derive(Debug, Clone)]
struct Config {
    bind: String,
    event_path: String,
    health_path: String,
    expected_forward_token: Option<String>,
    allow_non_loopback: bool,
    mock: bool,
    max_events: usize,
    max_concurrent_requests: usize,
    max_concurrent_connections: usize,
    request_read_timeout: Duration,
    handler_timeout: Duration,
    webex_request_timeout: Duration,
    attempt_lease: Duration,
    in_flight_wait: Duration,
    state_file: PathBuf,
    max_processed_ids: usize,
    allowed_room_ids: BTreeSet<String>,
    reply_prefix: String,
    self_person_id: Option<String>,
}

impl Config {
    fn from_env() -> Result<Self> {
        let expected_forward_token = env::var("WEBEX_SIDECAR_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty());
        let allow_unauthenticated = env_bool("WEBEX_ACCOUNT_BOT_ALLOW_UNAUTHENTICATED")
            || env_bool("WEBEX_SIDECAR_ALLOW_UNAUTHENTICATED");
        if expected_forward_token.is_none() && !allow_unauthenticated {
            return Err(Error::Other(
                "WEBEX_SIDECAR_TOKEN is required; set WEBEX_ACCOUNT_BOT_ALLOW_UNAUTHENTICATED=1 only for local unsafe testing"
                    .to_owned(),
            ));
        }

        let max_concurrent_requests = env_usize(
            "WEBEX_ACCOUNT_BOT_MAX_CONCURRENT_REQUESTS",
            DEFAULT_MAX_CONCURRENT_REQUESTS,
        )
        .max(1);
        let max_concurrent_connections = env_usize(
            "WEBEX_ACCOUNT_BOT_MAX_CONCURRENT_CONNECTIONS",
            DEFAULT_MAX_CONCURRENT_CONNECTIONS,
        )
        .max(max_concurrent_requests)
        .max(1);

        let config = Self {
            bind: env_or("WEBEX_ACCOUNT_BOT_BIND", DEFAULT_BIND),
            event_path: env_or("WEBEX_ACCOUNT_BOT_PATH", DEFAULT_EVENT_PATH),
            health_path: env_or("WEBEX_ACCOUNT_BOT_HEALTH_PATH", DEFAULT_HEALTH_PATH),
            expected_forward_token,
            allow_non_loopback: env_bool("WEBEX_ACCOUNT_BOT_ALLOW_NON_LOOPBACK"),
            mock: env_bool("WEBEX_ACCOUNT_BOT_MOCK"),
            max_events: env_usize("WEBEX_ACCOUNT_BOT_MAX_EVENTS", 0),
            max_concurrent_requests,
            max_concurrent_connections,
            request_read_timeout: Duration::from_secs(
                env_u64(
                    "WEBEX_ACCOUNT_BOT_REQUEST_READ_TIMEOUT_SECS",
                    DEFAULT_REQUEST_READ_TIMEOUT_SECS,
                )
                .max(1),
            ),
            handler_timeout: Duration::from_secs(
                env_u64(
                    "WEBEX_ACCOUNT_BOT_HANDLER_TIMEOUT_SECS",
                    DEFAULT_HANDLER_TIMEOUT_SECS,
                )
                .max(1),
            ),
            webex_request_timeout: Duration::from_secs(
                env_u64(
                    "WEBEX_ACCOUNT_BOT_WEBEX_REQUEST_TIMEOUT_SECS",
                    DEFAULT_WEBEX_REQUEST_TIMEOUT_SECS,
                )
                .max(1),
            ),
            attempt_lease: Duration::from_secs(
                env_u64(
                    "WEBEX_ACCOUNT_BOT_ATTEMPT_LEASE_SECS",
                    DEFAULT_ATTEMPT_LEASE_SECS,
                )
                .max(1),
            ),
            in_flight_wait: Duration::from_secs(env_u64(
                "WEBEX_ACCOUNT_BOT_IN_FLIGHT_WAIT_SECS",
                DEFAULT_IN_FLIGHT_WAIT_SECS,
            )),
            state_file: PathBuf::from(env_or("WEBEX_ACCOUNT_BOT_STATE_FILE", DEFAULT_STATE_FILE)),
            max_processed_ids: env_usize(
                "WEBEX_ACCOUNT_BOT_MAX_PROCESSED_IDS",
                DEFAULT_MAX_PROCESSED_IDS,
            )
            .max(1),
            allowed_room_ids: env_set("WEBEX_ACCOUNT_BOT_ROOM_IDS"),
            reply_prefix: env_or("WEBEX_ACCOUNT_BOT_REPLY_PREFIX", DEFAULT_REPLY_PREFIX),
            self_person_id: env_optional("WEBEX_ACCOUNT_BOT_SELF_PERSON_ID"),
        };
        validate_account_bot_paths(&config.event_path, &config.health_path)?;
        Ok(config)
    }
}

struct AccountBot {
    config: Config,
    client: Option<WebexClient>,
    self_person_id: Option<String>,
    processed: Arc<Mutex<ProcessedMessageStore>>,
    state_persist_failed: Arc<AtomicBool>,
    event_slots: Arc<AtomicUsize>,
    completed_event_slots: Arc<AtomicUsize>,
    max_events_notify: Arc<Notify>,
}

struct EventSlot {
    slots: Option<Arc<AtomicUsize>>,
    completed: Option<Arc<AtomicUsize>>,
    notify: Option<Arc<Notify>>,
    committed: bool,
}

impl EventSlot {
    fn unlimited() -> Self {
        Self {
            slots: None,
            completed: None,
            notify: None,
            committed: true,
        }
    }

    fn counted(slots: Arc<AtomicUsize>, completed: Arc<AtomicUsize>, notify: Arc<Notify>) -> Self {
        Self {
            slots: Some(slots),
            completed: Some(completed),
            notify: Some(notify),
            committed: false,
        }
    }

    fn commit(mut self) {
        if !self.committed {
            if let Some(completed) = &self.completed {
                completed.fetch_add(1, Ordering::AcqRel);
                if let Some(notify) = &self.notify {
                    notify.notify_one();
                }
            }
            self.committed = true;
        }
    }
}

impl Drop for EventSlot {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(slots) = &self.slots {
                slots.fetch_sub(1, Ordering::AcqRel);
            }
            if let Some(notify) = &self.notify {
                notify.notify_one();
            }
        }
    }
}

impl AccountBot {
    async fn new(config: Config) -> Result<Self> {
        let client = if config.mock {
            None
        } else {
            Some(build_webex_client_from_env(config.webex_request_timeout).await?)
        };
        let self_person_id = if let Some(person_id) = config.self_person_id.clone() {
            Some(person_id)
        } else if let Some(client) = &client {
            client.me().await?.id
        } else {
            None
        };
        let processed =
            ProcessedMessageStore::load(config.state_file.clone(), config.max_processed_ids)?;
        processed.verify_persistable()?;
        if let Some(person_id) = &self_person_id {
            eprintln!("account_bot_self_person_id={person_id}");
        } else {
            eprintln!("account_bot_self_person_id=unknown");
        }

        Ok(Self {
            config,
            client,
            self_person_id,
            processed: Arc::new(Mutex::new(processed)),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        })
    }

    async fn handle_request(
        &self,
        request: &HttpRequest,
        event_permit: Option<OwnedSemaphorePermit>,
        accepted_events: &AtomicUsize,
    ) -> Result<HandleResponse> {
        if request.path == self.config.health_path {
            return Ok(HandleResponse {
                response: if request.method == "GET" {
                    self.health_response()?
                } else {
                    HttpResponse::json_error(405, "method not allowed")
                },
                event_counted: false,
            });
        }

        if request.method != "POST" {
            return Ok(HandleResponse {
                response: HttpResponse::json_error(405, "method not allowed"),
                event_counted: false,
            });
        }
        if request.path != self.config.event_path {
            return Ok(HandleResponse {
                response: HttpResponse::json_error(404, "not found"),
                event_counted: false,
            });
        }
        if let Some(token) = &self.config.expected_forward_token {
            let expected = format!("Bearer {token}");
            if request.headers.get("authorization") != Some(&expected) {
                return Ok(HandleResponse {
                    response: HttpResponse::json_error(401, "unauthorized"),
                    event_counted: false,
                });
            }
        }
        if self.event_admission_closed(accepted_events) {
            return Ok(HandleResponse {
                response: error_response(&BotError::retry_after(
                    "max events reached".to_owned(),
                    retry_after_for_lease(self.config.in_flight_wait),
                )),
                event_counted: false,
            });
        }

        let event = match serde_json::from_slice::<SidecarEvent>(&request.body) {
            Ok(event) => event,
            Err(error) => {
                return Ok(HandleResponse {
                    response: HttpResponse::json_error(400, error.to_string()),
                    event_counted: false,
                });
            }
        };
        match self.handle_event_with_permit(event, event_permit).await {
            Ok(action) => {
                if let Err(error) = print_json_line(&action) {
                    let _ = writeln!(
                        io::stderr().lock(),
                        "account_bot_action_log_failed error={error}"
                    );
                }
                Ok(HandleResponse {
                    response: HttpResponse::json_value(
                        200,
                        json!({ "ok": true, "action": action }),
                    ),
                    event_counted: true,
                })
            }
            Err(error) => Ok(HandleResponse {
                response: error_response(&error),
                event_counted: false,
            }),
        }
    }

    fn health_response(&self) -> Result<HttpResponse> {
        let (processed_message_ids, volatile_message_ids, busy) = match self.processed.try_lock() {
            Ok(processed) => (Some(processed.len()), Some(processed.volatile_len()), false),
            Err(TryLockError::WouldBlock) => (None, None, true),
            Err(TryLockError::Poisoned(_)) => {
                return Err(Error::Other(
                    "processed message store lock poisoned".to_owned(),
                ));
            }
        };

        let state_persistence_healthy = !self.state_persist_failed.load(Ordering::Relaxed)
            && volatile_message_ids.unwrap_or_default() == 0;
        Ok(HttpResponse::json_value(
            if state_persistence_healthy { 200 } else { 503 },
            json!({
                "ok": state_persistence_healthy,
                "busy": busy,
                "statePersistenceHealthy": state_persistence_healthy,
                "processedMessageIds": processed_message_ids,
                "volatileProcessedIds": volatile_message_ids,
                "selfPersonIdKnown": self.self_person_id.is_some(),
                "mock": self.config.mock,
            }),
        ))
    }

    #[cfg(test)]
    async fn handle_event(&self, event: SidecarEvent) -> BotResult<BotAction> {
        self.handle_event_with_permit(event, None).await
    }

    async fn handle_event_with_permit(
        &self,
        event: SidecarEvent,
        event_permit: Option<OwnedSemaphorePermit>,
    ) -> BotResult<BotAction> {
        if event.version != 1 {
            return Ok(BotAction::ignored("unsupported_event_version", None, None));
        }
        if event.resource != "messages" || event.event != "created" {
            return Ok(BotAction::ignored(
                "unsupported_event",
                Some(event.resource),
                None,
            ));
        }

        let mut message = match BotMessage::from_value(event.data) {
            Ok(message) => message,
            Err(_) => return Ok(BotAction::ignored("invalid_message_payload", None, None)),
        };
        let Some(message_id) = message.id.clone() else {
            return Ok(BotAction::ignored("missing_message_id", None, None));
        };
        let (inflight, event_slot) = loop {
            match self.begin_in_flight_attempt(message_id.clone())? {
                BeginInFlight::Started(inflight) => {
                    if let Some(event_slot) = self.try_acquire_event_slot() {
                        break (inflight, event_slot);
                    }
                    self.release_or_retry_after(inflight, &message_id, "max events reached")?;
                    return Err(Error::Other("max events reached".to_owned()).into());
                }
                BeginInFlight::Processed => {
                    return Ok(BotAction::ignored(
                        "duplicate_message",
                        Some(message_id),
                        message.room_id.clone(),
                    ));
                }
                BeginInFlight::InProgress => {
                    match self.wait_for_in_flight_attempt(&message_id).await? {
                        BeginInFlight::Started(inflight) => {
                            if let Some(event_slot) = self.try_acquire_event_slot() {
                                break (inflight, event_slot);
                            }
                            self.release_or_retry_after(
                                inflight,
                                &message_id,
                                "max events reached",
                            )?;
                            return Err(Error::Other("max events reached".to_owned()).into());
                        }
                        BeginInFlight::Processed => {
                            return Ok(BotAction::ignored(
                                "duplicate_message",
                                Some(message_id),
                                message.room_id.clone(),
                            ));
                        }
                        BeginInFlight::InProgress => {
                            return Err(BotError::retry_after(
                                format!(
                                    "message {message_id} reply attempt is already in progress"
                                ),
                                self.config.in_flight_wait,
                            ));
                        }
                        BeginInFlight::AttemptLeased(retry_after) => {
                            return Err(BotError::retry_after(
                                format!(
                                    "message {message_id} reply attempt is pending retry lease"
                                ),
                                retry_after,
                            ));
                        }
                    }
                }
                BeginInFlight::AttemptLeased(retry_after) => {
                    return Err(BotError::retry_after(
                        format!("message {message_id} reply attempt is pending retry lease"),
                        retry_after,
                    ));
                }
            }
        };
        if let Some(room_id) = message.room_id.clone() {
            if !self.config.allowed_room_ids.is_empty()
                && !self.config.allowed_room_ids.contains(&room_id)
            {
                self.release_or_retry_after(inflight, &message_id, "room not allowed")?;
                event_slot.commit();
                return Ok(BotAction::ignored(
                    "room_not_allowed",
                    Some(message_id),
                    Some(room_id),
                ));
            }
        }

        if !self.config.mock && message.needs_hydration() {
            if let Some(client) = &self.client {
                match client.get_message(&message_id).await {
                    Ok(hydrated) => message.merge(hydrated),
                    Err(error) if is_webex_api_status(&error, 404) => {
                        self.remember_or_retry_after(
                            inflight,
                            &message_id,
                            "missing Webex message",
                        )?;
                        event_slot.commit();
                        return Ok(BotAction::ignored(
                            "message_not_found",
                            Some(message_id),
                            message.room_id.clone(),
                        ));
                    }
                    Err(error @ Error::Api(_)) => {
                        Self::finish_after_known_api_failure(
                            &self.state_persist_failed,
                            inflight,
                            &message_id,
                            &error,
                            self.config.attempt_lease,
                        );
                        return Err(BotError::retryable_api(error, self.config.attempt_lease));
                    }
                    Err(error) => {
                        self.release_or_retry_after(inflight, &message_id, "hydration failure")?;
                        return Err(error.into());
                    }
                }
            }
        }
        let Some(room_id) = message.room_id.clone() else {
            self.release_or_retry_after(inflight, &message_id, "missing room id")?;
            event_slot.commit();
            return Ok(BotAction::ignored(
                "missing_room_id",
                Some(message_id),
                None,
            ));
        };

        if !self.config.allowed_room_ids.is_empty()
            && !self.config.allowed_room_ids.contains(&room_id)
        {
            self.release_or_retry_after(inflight, &message_id, "room not allowed")?;
            event_slot.commit();
            return Ok(BotAction::ignored(
                "room_not_allowed",
                Some(message_id),
                Some(room_id),
            ));
        }
        if self
            .self_person_id
            .as_deref()
            .is_some_and(|self_id| message.person_id.as_deref() == Some(self_id))
        {
            self.release_or_retry_after(inflight, &message_id, "self message")?;
            event_slot.commit();
            return Ok(BotAction::ignored(
                "self_message",
                Some(message_id),
                Some(room_id),
            ));
        }
        let reply_text = reply_text(&self.config.reply_prefix, &message);
        if self.config.mock {
            self.remember_or_retry_after(inflight, &message_id, "mock reply")?;
            event_slot.commit();
            return Ok(BotAction::mock_replied(message_id, room_id, reply_text));
        }

        let reply_parent_id = message
            .parent_id
            .clone()
            .unwrap_or_else(|| message_id.clone());
        match self
            .send_reply(
                inflight,
                message_id.clone(),
                room_id.clone(),
                reply_parent_id,
                reply_text.clone(),
                event_permit,
                event_slot,
            )
            .await?
        {
            ReplyOutcome::Replied(reply_id) => Ok(BotAction::replied(
                message_id, room_id, reply_text, reply_id,
            )),
            ReplyOutcome::Ignored(reason) => {
                Ok(BotAction::ignored(reason, Some(message_id), Some(room_id)))
            }
        }
    }

    fn try_acquire_event_slot(&self) -> Option<EventSlot> {
        if self.config.max_events == 0 {
            return Some(EventSlot::unlimited());
        }
        let mut current = self.event_slots.load(Ordering::Relaxed);
        loop {
            if current >= self.config.max_events {
                return None;
            }
            match self.event_slots.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(EventSlot::counted(
                        self.event_slots.clone(),
                        self.completed_event_slots.clone(),
                        self.max_events_notify.clone(),
                    ));
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn event_admission_closed(&self, accepted_events: &AtomicUsize) -> bool {
        max_event_admission_closed(
            self.config.max_events,
            self.event_slots.as_ref(),
            self.completed_event_slots.as_ref(),
            accepted_events,
        )
    }

    async fn send_reply(
        &self,
        inflight: InFlightMessage,
        message_id: String,
        room_id: String,
        reply_parent_id: String,
        reply_text: String,
        event_permit: Option<OwnedSemaphorePermit>,
        event_slot: EventSlot,
    ) -> BotResult<ReplyOutcome> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Other("Webex client is not configured".to_owned()))?
            .clone();
        let state_persist_failed = self.state_persist_failed.clone();
        let attempt_lease = self.config.attempt_lease;
        let (result_sender, result_receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = send_reply_worker(
                client,
                state_persist_failed,
                attempt_lease,
                inflight,
                message_id,
                room_id,
                reply_parent_id,
                reply_text,
                event_permit,
                event_slot,
            )
            .await;
            let _ = result_sender.send(result);
        });
        let result = result_receiver.await.map_err(|error| {
            self.state_persist_failed.store(true, Ordering::Relaxed);
            BotError::retry_after(
                format!("message reply worker failed before completion: {error}"),
                retry_after_for_lease(self.config.attempt_lease),
            )
        })?;
        result
    }

    fn finish_after_known_api_failure(
        state_persist_failed: &Arc<AtomicBool>,
        mut inflight: InFlightMessage,
        message_id: &str,
        error: &Error,
        attempt_lease: Duration,
    ) {
        inflight.mark_reply_not_accepted();
        let result = if let Some(retry_after) = retryable_api_failure_lease(error, attempt_lease) {
            inflight.defer(retry_after)
        } else {
            inflight.release()
        };
        if let Err(persist_error) = result {
            state_persist_failed.store(true, Ordering::Relaxed);
            eprintln!(
                "account_bot_attempt_finish_failed message_id={message_id} error={persist_error}"
            );
        }
    }

    fn release_or_retry_after(
        &self,
        inflight: InFlightMessage,
        message_id: &str,
        context: &str,
    ) -> BotResult<()> {
        Self::release_or_retry_after_with_lease(
            &self.state_persist_failed,
            inflight,
            message_id,
            context,
            self.config.attempt_lease,
        )
    }

    fn release_or_retry_after_with_lease(
        state_persist_failed: &Arc<AtomicBool>,
        mut inflight: InFlightMessage,
        message_id: &str,
        context: &str,
        attempt_lease: Duration,
    ) -> BotResult<()> {
        inflight.mark_reply_not_accepted();
        inflight.release().map_err(|error| {
            state_persist_failed.store(true, Ordering::Relaxed);
            BotError::retry_after(
                format!(
                    "processed message state persist failed while releasing {context} attempt for message {message_id}: {error}"
                ),
                retry_after_for_lease(attempt_lease),
            )
        })
    }

    fn remember_or_retry_after(
        &self,
        inflight: InFlightMessage,
        message_id: &str,
        context: &str,
    ) -> BotResult<()> {
        Self::remember_or_retry_after_with_lease(
            &self.state_persist_failed,
            inflight,
            message_id,
            context,
            self.config.attempt_lease,
        )
    }

    fn remember_or_retry_after_with_lease(
        state_persist_failed: &Arc<AtomicBool>,
        inflight: InFlightMessage,
        message_id: &str,
        context: &str,
        attempt_lease: Duration,
    ) -> BotResult<()> {
        inflight.remember().map_err(|error| {
            state_persist_failed.store(true, Ordering::Relaxed);
            BotError::retry_after(
                format!(
                    "processed message state persist failed while recording {context} for message {message_id}: {error}"
                ),
                retry_after_for_lease(attempt_lease),
            )
        })
    }

    fn remember_after_reply_or_retry_after(
        state_persist_failed: &Arc<AtomicBool>,
        inflight: InFlightMessage,
        message_id: &str,
        attempt_lease: Duration,
    ) -> BotResult<()> {
        if let Err(error) = inflight.remember_after_reply() {
            state_persist_failed.store(true, Ordering::Relaxed);
            eprintln!(
                "account_bot_state_persist_failed_after_reply message_id={message_id} error={error}"
            );
            return Err(BotError::retry_after(
                format!(
                    "processed message state persist failed after reply for message {message_id}: {error}"
                ),
                retry_after_for_lease(attempt_lease),
            ));
        }
        Ok(())
    }

    fn begin_in_flight_attempt(&self, message_id: String) -> BotResult<BeginInFlight> {
        InFlightMessage::begin(
            &self.processed,
            &self.state_persist_failed,
            message_id.clone(),
            self.config.attempt_lease,
        )
        .map_err(|error| {
            self.state_persist_failed.store(true, Ordering::Relaxed);
            BotError::retry_after(
                format!(
                    "processed message state persist failed while leasing message {message_id}: {error}"
                ),
                retry_after_for_lease(self.config.attempt_lease),
            )
        })
    }

    async fn wait_for_in_flight_attempt(&self, message_id: &str) -> BotResult<BeginInFlight> {
        let deadline = Instant::now() + self.config.in_flight_wait;
        loop {
            if Instant::now() >= deadline {
                return Ok(BeginInFlight::InProgress);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            sleep(remaining.min(Duration::from_millis(100))).await;
            match self.begin_in_flight_attempt(message_id.to_owned())? {
                BeginInFlight::InProgress => continue,
                other => return Ok(other),
            }
        }
    }
}

async fn send_reply_worker(
    client: WebexClient,
    state_persist_failed: Arc<AtomicBool>,
    attempt_lease: Duration,
    mut inflight: InFlightMessage,
    message_id: String,
    room_id: String,
    reply_parent_id: String,
    reply_text: String,
    _event_permit: Option<OwnedSemaphorePermit>,
    event_slot: EventSlot,
) -> BotResult<ReplyOutcome> {
    let reply_request = CreateMessage::reply_text(&room_id, &reply_parent_id, reply_text);
    let reply_result = client.create_message(&reply_request).await;

    match reply_result {
        Ok(reply) => {
            AccountBot::remember_after_reply_or_retry_after(
                &state_persist_failed,
                inflight,
                &message_id,
                attempt_lease,
            )?;
            event_slot.commit();
            Ok(ReplyOutcome::Replied(reply.id))
        }
        Err(error) if is_webex_api_status(&error, 404) => {
            inflight.mark_reply_not_accepted();
            AccountBot::remember_or_retry_after_with_lease(
                &state_persist_failed,
                inflight,
                &message_id,
                "missing Webex message",
                attempt_lease,
            )?;
            event_slot.commit();
            Ok(ReplyOutcome::Ignored("message_not_found"))
        }
        Err(error @ Error::Api(_))
            if retryable_api_failure_lease(&error, attempt_lease).is_some() =>
        {
            AccountBot::finish_after_known_api_failure(
                &state_persist_failed,
                inflight,
                &message_id,
                &error,
                attempt_lease,
            );
            Err(BotError::retryable_api(error, attempt_lease))
        }
        Err(error @ Error::Api(_)) => {
            AccountBot::finish_after_known_api_failure(
                &state_persist_failed,
                inflight,
                &message_id,
                &error,
                attempt_lease,
            );
            Err(error.into())
        }
        Err(error) if create_message_error_could_have_been_accepted(&error) => {
            inflight.mark_reply_started();
            AccountBot::remember_after_reply_or_retry_after(
                &state_persist_failed,
                inflight,
                &message_id,
                attempt_lease,
            )?;
            event_slot.commit();
            eprintln!(
                "account_bot_reply_unknown_after_create_message_error message_id={message_id} error={error}"
            );
            Ok(ReplyOutcome::Ignored("reply_unknown"))
        }
        Err(error) => {
            AccountBot::release_or_retry_after_with_lease(
                &state_persist_failed,
                inflight,
                &message_id,
                "create_message failure",
                attempt_lease,
            )?;
            Err(error.into())
        }
    }
}

type BotResult<T> = std::result::Result<T, BotError>;

#[derive(Debug)]
enum ReplyOutcome {
    Replied(Option<String>),
    Ignored(&'static str),
}

#[derive(Debug)]
enum BotError {
    Webex(Error),
    RetryableApi {
        error: Error,
        retry_after: Duration,
    },
    RetryAfter {
        message: String,
        retry_after: Duration,
    },
}

impl BotError {
    fn retry_after(message: String, retry_after: Duration) -> Self {
        Self::RetryAfter {
            message,
            retry_after,
        }
    }

    fn retryable_api(error: Error, fallback: Duration) -> Self {
        if let Some(retry_after) = retryable_api_failure_lease(&error, fallback) {
            Self::RetryableApi { error, retry_after }
        } else {
            Self::Webex(error)
        }
    }
}

impl From<Error> for BotError {
    fn from(error: Error) -> Self {
        Self::Webex(error)
    }
}

impl std::fmt::Display for BotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Webex(error) | Self::RetryableApi { error, .. } => error.fmt(formatter),
            Self::RetryAfter { message, .. } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for BotError {}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct BotMessage {
    id: Option<String>,
    room_id: Option<String>,
    parent_id: Option<String>,
    person_id: Option<String>,
    text: Option<String>,
    markdown: Option<String>,
}

impl BotMessage {
    fn from_value(value: Value) -> Result<Self> {
        Ok(serde_json::from_value(value)?)
    }

    fn needs_hydration(&self) -> bool {
        self.room_id.is_none() || self.person_id.is_none()
    }

    fn merge(&mut self, message: Message) {
        if self.id.is_none() {
            self.id = message.id;
        }
        if self.room_id.is_none() {
            self.room_id = message.room_id;
        }
        if self.parent_id.is_none() {
            self.parent_id = message.parent_id;
        }
        if self.person_id.is_none() {
            self.person_id = message.person_id;
        }
        if self.text.is_none() {
            self.text = message.text;
        }
        if self.markdown.is_none() {
            self.markdown = message.markdown;
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BotAction {
    action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    room_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_text: Option<String>,
}

impl BotAction {
    fn ignored(reason: &'static str, message_id: Option<String>, room_id: Option<String>) -> Self {
        Self {
            action: "ignored",
            reason: Some(reason),
            message_id,
            room_id,
            reply_id: None,
            reply_text: None,
        }
    }

    fn mock_replied(message_id: String, room_id: String, reply_text: String) -> Self {
        Self {
            action: "mock_replied",
            reason: None,
            message_id: Some(message_id),
            room_id: Some(room_id),
            reply_id: None,
            reply_text: Some(reply_text),
        }
    }

    fn replied(
        message_id: String,
        room_id: String,
        reply_text: String,
        reply_id: Option<String>,
    ) -> Self {
        Self {
            action: "replied",
            reason: None,
            message_id: Some(message_id),
            room_id: Some(room_id),
            reply_id,
            reply_text: Some(reply_text),
        }
    }
}

#[derive(Debug)]
struct ProcessedMessageStore {
    path: PathBuf,
    ids: BTreeSet<String>,
    attempts: BTreeMap<String, u64>,
    inflight: BTreeSet<String>,
    order: VecDeque<String>,
    max_ids: usize,
    volatile_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BeginAttempt {
    Started,
    Processed,
    ProcessedAfterPersist,
    InProgress,
    AttemptLeased(Duration),
}

impl ProcessedMessageStore {
    fn load(path: PathBuf, max_ids: usize) -> Result<Self> {
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };
        let mut order = VecDeque::new();
        let mut ids = BTreeSet::new();
        let mut attempts = BTreeMap::new();
        let now = unix_timestamp_secs();
        for line in contents
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            if let Some(id) = line.strip_prefix("processed_v2\t") {
                if let Some(id) = decode_state_id(id) {
                    add_processed_id(&mut ids, &mut order, max_ids, &id);
                }
                continue;
            }
            if let Some(rest) = line.strip_prefix("attempt_v2\t") {
                if let Some((expires_at, id)) = rest.split_once('\t') {
                    if let (Ok(expires_at), Some(id)) =
                        (expires_at.parse::<u64>(), decode_state_id(id))
                    {
                        if expires_at > now && !id.trim().is_empty() {
                            attempts.insert(id, expires_at);
                        }
                    }
                }
                continue;
            }
            if let Some(id) = line.strip_prefix("processed\t") {
                add_processed_id(&mut ids, &mut order, max_ids, id);
                continue;
            }
            if let Some(rest) = line.strip_prefix("attempt\t") {
                if let Some((expires_at, id)) = rest.split_once('\t') {
                    if let Ok(expires_at) = expires_at.parse::<u64>() {
                        if expires_at > now && !id.trim().is_empty() {
                            attempts.insert(id.to_owned(), expires_at);
                        }
                    }
                }
                continue;
            }
            add_processed_id(&mut ids, &mut order, max_ids, line);
        }
        Ok(Self {
            path,
            ids,
            attempts,
            inflight: BTreeSet::new(),
            order,
            max_ids,
            volatile_ids: BTreeSet::new(),
        })
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn volatile_len(&self) -> usize {
        self.volatile_ids.len()
    }

    fn verify_persistable(&self) -> Result<()> {
        self.persist_state()
    }

    #[cfg(test)]
    fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }

    fn begin_attempt(&mut self, id: &str, lease: Duration) -> Result<BeginAttempt> {
        self.begin_attempt_at(id, lease, unix_timestamp_secs())
    }

    fn begin_attempt_at(&mut self, id: &str, lease: Duration, now: u64) -> Result<BeginAttempt> {
        self.prune_expired_attempts(now);
        if self.ids.contains(id) {
            return Ok(BeginAttempt::Processed);
        }
        if self.volatile_ids.contains(id) {
            self.complete_at(id, now)?;
            return Ok(BeginAttempt::ProcessedAfterPersist);
        }
        if self.inflight.contains(id) {
            return Ok(BeginAttempt::InProgress);
        }
        if let Some(expires_at) = self.attempts.get(id) {
            return Ok(BeginAttempt::AttemptLeased(Duration::from_secs(
                expires_at.saturating_sub(now).max(1),
            )));
        }

        self.inflight.insert(id.to_owned());
        self.attempts
            .insert(id.to_owned(), now.saturating_add(lease.as_secs().max(1)));
        if let Err(error) = self.persist() {
            self.inflight.remove(id);
            self.attempts.remove(id);
            return Err(error);
        }
        Ok(BeginAttempt::Started)
    }

    fn release(&mut self, id: &str) -> Result<()> {
        let old_attempts = self.attempts.clone();
        let old_inflight = self.inflight.clone();
        self.inflight.remove(id);
        self.attempts.remove(id);
        if let Err(error) = self.persist() {
            self.attempts = old_attempts;
            self.inflight = old_inflight;
            return Err(error);
        }
        Ok(())
    }

    fn defer_attempt(&mut self, id: &str, lease: Duration) -> Result<()> {
        self.defer_attempt_at(id, lease, unix_timestamp_secs())
    }

    fn defer_attempt_at(&mut self, id: &str, lease: Duration, now: u64) -> Result<()> {
        self.prune_expired_attempts(now);
        self.inflight.remove(id);
        self.attempts
            .insert(id.to_owned(), now.saturating_add(lease.as_secs().max(1)));
        self.persist()
    }

    fn remember(&mut self, id: &str) -> Result<()> {
        self.complete_at(id, unix_timestamp_secs())
    }

    fn complete_at(&mut self, id: &str, now: u64) -> Result<()> {
        let old_ids = self.ids.clone();
        let old_attempts = self.attempts.clone();
        let old_inflight = self.inflight.clone();
        let old_order = self.order.clone();
        let old_volatile_ids = self.volatile_ids.clone();

        self.prune_expired_attempts(now);
        self.inflight.remove(id);
        let already_processed = self.ids.contains(id);
        self.attempts.remove(id);
        self.volatile_ids.remove(id);
        if !already_processed {
            self.ids.insert(id.to_owned());
            self.order.push_back(id.to_owned());
            while self.order.len() > self.max_ids {
                if let Some(removed) = self.order.pop_front() {
                    self.ids.remove(&removed);
                }
            }
        }
        if let Err(error) = self.persist() {
            self.ids = old_ids;
            self.attempts = old_attempts;
            self.inflight = old_inflight;
            self.order = old_order;
            self.volatile_ids = old_volatile_ids;
            return Err(error);
        }
        Ok(())
    }

    fn prune_expired_attempts(&mut self, now: u64) {
        let mut expired = Vec::new();
        self.attempts.retain(|id, expires_at| {
            let keep = *expires_at > now;
            if !keep {
                expired.push(id.clone());
            }
            keep
        });
        for id in expired {
            self.inflight.remove(&id);
        }
    }

    fn persist(&mut self) -> Result<()> {
        let old_ids = self.ids.clone();
        let old_attempts = self.attempts.clone();
        let old_inflight = self.inflight.clone();
        let old_order = self.order.clone();
        let old_volatile_ids = self.volatile_ids.clone();

        self.absorb_volatile_ids();
        if let Err(error) = self.persist_state() {
            self.ids = old_ids;
            self.attempts = old_attempts;
            self.inflight = old_inflight;
            self.order = old_order;
            self.volatile_ids = old_volatile_ids;
            return Err(error);
        }
        Ok(())
    }

    fn absorb_volatile_ids(&mut self) {
        let volatile_ids = std::mem::take(&mut self.volatile_ids);
        for id in volatile_ids {
            self.inflight.remove(&id);
            self.attempts.remove(&id);
            add_processed_id(&mut self.ids, &mut self.order, self.max_ids, &id);
        }
    }

    fn persist_state(&self) -> Result<()> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let tmp = temp_path_for(&self.path);
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&tmp)?;
        for id in &self.order {
            writeln!(file, "processed_v2\t{}", encode_state_id(id))?;
        }
        for (id, expires_at) in &self.attempts {
            writeln!(file, "attempt_v2\t{expires_at}\t{}", encode_state_id(id))?;
        }
        file.flush()?;
        file.sync_all()?;
        drop(file);
        replace_state_file(&tmp, &self.path)?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    fn mark_volatile_processed(&mut self, id: &str, now: u64) {
        self.prune_expired_attempts(now);
        self.inflight.remove(id);
        self.attempts.remove(id);
        self.volatile_ids.insert(id.to_owned());
    }

    #[cfg(test)]
    fn contains_volatile(&self, id: &str) -> bool {
        self.volatile_ids.contains(id)
    }
}

fn replace_state_file(tmp: &Path, path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        if !path.exists() {
            fs::rename(tmp, path)?;
            return Ok(());
        }

        let backup = temp_path_for(path);
        match fs::rename(path, &backup) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::rename(tmp, path)?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
        if let Err(error) = fs::rename(tmp, path) {
            let _ = fs::rename(&backup, path);
            return Err(error.into());
        }
        let _ = fs::remove_file(backup);
        Ok(())
    }

    #[cfg(not(windows))]
    {
        fs::rename(tmp, path)?;
        Ok(())
    }
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn encode_state_id(id: &str) -> String {
    let mut encoded = String::with_capacity(id.len());
    for byte in id.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => {
                use std::fmt::Write as _;
                let _ = write!(&mut encoded, "%{byte:02X}");
            }
        }
    }
    encoded
}

fn decode_state_id(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = bytes.get(index + 1).copied().and_then(hex_value)?;
            let low = bytes.get(index + 2).copied().and_then(hex_value)?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn add_processed_id(
    ids: &mut BTreeSet<String>,
    order: &mut VecDeque<String>,
    max_ids: usize,
    id: &str,
) {
    if ids.insert(id.to_owned()) {
        order.push_back(id.to_owned());
        while order.len() > max_ids {
            if let Some(removed) = order.pop_front() {
                ids.remove(&removed);
            }
        }
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

enum BeginInFlight {
    Started(InFlightMessage),
    Processed,
    InProgress,
    AttemptLeased(Duration),
}

struct InFlightMessage {
    store: Arc<Mutex<ProcessedMessageStore>>,
    state_persist_failed: Arc<AtomicBool>,
    id: String,
    lease: Duration,
    active: bool,
    reply_started: bool,
}

impl InFlightMessage {
    fn begin(
        store: &Arc<Mutex<ProcessedMessageStore>>,
        state_persist_failed: &Arc<AtomicBool>,
        id: String,
        lease: Duration,
    ) -> Result<BeginInFlight> {
        let mut processed = store
            .lock()
            .map_err(|_| Error::Other("processed message store lock poisoned".to_owned()))?;
        match processed.begin_attempt(&id, lease)? {
            BeginAttempt::Started => {
                state_persist_failed.store(false, Ordering::Relaxed);
                Ok(BeginInFlight::Started(Self {
                    store: store.clone(),
                    state_persist_failed: state_persist_failed.clone(),
                    id,
                    lease,
                    active: true,
                    reply_started: false,
                }))
            }
            BeginAttempt::Processed => Ok(BeginInFlight::Processed),
            BeginAttempt::ProcessedAfterPersist => {
                state_persist_failed.store(false, Ordering::Relaxed);
                Ok(BeginInFlight::Processed)
            }
            BeginAttempt::InProgress => Ok(BeginInFlight::InProgress),
            BeginAttempt::AttemptLeased(retry_after) => Ok(BeginInFlight::AttemptLeased(
                retry_after_for_lease(retry_after),
            )),
        }
    }

    fn mark_reply_started(&mut self) {
        self.reply_started = true;
    }

    fn mark_reply_not_accepted(&mut self) {
        self.reply_started = false;
    }

    fn release(mut self) -> Result<()> {
        let mut processed = self
            .store
            .lock()
            .map_err(|_| Error::Other("processed message store lock poisoned".to_owned()))?;
        let result = processed.release(&self.id);
        if result.is_ok() {
            self.state_persist_failed.store(false, Ordering::Relaxed);
            self.active = false;
        }
        result
    }

    fn remember(mut self) -> Result<()> {
        let mut processed = self
            .store
            .lock()
            .map_err(|_| Error::Other("processed message store lock poisoned".to_owned()))?;
        let result = processed.remember(&self.id);
        if result.is_ok() {
            self.state_persist_failed.store(false, Ordering::Relaxed);
            self.active = false;
        }
        result
    }

    fn defer(mut self, lease: Duration) -> Result<()> {
        self.lease = lease;
        let mut processed = self
            .store
            .lock()
            .map_err(|_| Error::Other("processed message store lock poisoned".to_owned()))?;
        let result = processed.defer_attempt(&self.id, lease);
        if result.is_ok() {
            self.state_persist_failed.store(false, Ordering::Relaxed);
            self.active = false;
        }
        result
    }

    fn remember_after_reply(mut self) -> Result<()> {
        let mut processed = self
            .store
            .lock()
            .map_err(|_| Error::Other("processed message store lock poisoned".to_owned()))?;
        let result = processed.remember(&self.id);
        if let Err(error) = result {
            processed.mark_volatile_processed(&self.id, unix_timestamp_secs());
            self.active = false;
            return Err(error);
        }
        self.state_persist_failed.store(false, Ordering::Relaxed);
        self.active = false;
        Ok(())
    }
}

impl Drop for InFlightMessage {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        match self.store.lock() {
            Ok(mut processed) if self.reply_started => {
                if let Err(error) = processed.remember(&self.id) {
                    processed.mark_volatile_processed(&self.id, unix_timestamp_secs());
                    self.state_persist_failed.store(true, Ordering::Relaxed);
                    eprintln!(
                        "account_bot_state_persist_failed_after_unknown_reply message_id={} error={error}",
                        self.id
                    );
                } else {
                    self.state_persist_failed.store(false, Ordering::Relaxed);
                }
            }
            Ok(mut processed) => {
                if let Err(error) = processed.defer_attempt(&self.id, self.lease) {
                    self.state_persist_failed.store(true, Ordering::Relaxed);
                    eprintln!(
                        "account_bot_attempt_defer_failed message_id={} error={error}",
                        self.id
                    );
                } else {
                    self.state_persist_failed.store(false, Ordering::Relaxed);
                }
            }
            Err(_) => {
                self.state_persist_failed.store(true, Ordering::Relaxed);
                eprintln!(
                    "account_bot_attempt_defer_failed message_id={} error=processed message store lock poisoned",
                    self.id
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
enum TokenSource {
    Static(String),
    File(PathBuf),
}

#[derive(Debug, Clone)]
struct ReloadingAccessTokenProvider {
    source: TokenSource,
}

impl ReloadingAccessTokenProvider {
    fn from_env() -> Result<Self> {
        if let Some(path) = env_optional("WEBEX_ACCESS_TOKEN_FILE")
            .or_else(|| env_optional("WEBEX_TOKEN_FILE"))
            .map(PathBuf::from)
        {
            return Ok(Self {
                source: TokenSource::File(path),
            });
        }
        if let Some(token) = env_optional("WEBEX_ACCESS_TOKEN") {
            return Ok(Self {
                source: TokenSource::Static(token),
            });
        }
        Err(Error::MissingToken)
    }
}

#[async_trait]
impl AccessTokenProvider for ReloadingAccessTokenProvider {
    async fn access_token(&self) -> Result<String> {
        match &self.source {
            TokenSource::Static(token) => Ok(token.clone()),
            TokenSource::File(path) => load_access_token_file(path),
        }
    }
}

async fn build_webex_client_from_env(request_timeout: Duration) -> Result<WebexClient> {
    let provider = ReloadingAccessTokenProvider::from_env()?;
    let http = reqwest::Client::builder()
        .timeout(request_timeout)
        .build()?;
    WebexClient::builder()?
        .http_client(http)
        .token_provider(Arc::new(provider))
        .build()
}

fn load_access_token_file(path: &Path) -> Result<String> {
    let raw = fs::read_to_string(path)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::MissingToken);
    }
    if !trimmed.starts_with('{') {
        return Ok(trimmed.to_owned());
    }

    let token_set = serde_json::from_str::<TokenSet>(trimmed)?;
    let access_token = token_set.access_token.trim();
    if access_token.is_empty() {
        return Err(Error::MissingToken);
    }
    Ok(access_token.to_owned())
}

async fn validate_loopback_bind(bind: &str, allow_non_loopback: bool) -> Result<()> {
    let resolved = tokio::net::lookup_host(bind)
        .await?
        .collect::<Vec<SocketAddr>>();
    if resolved.is_empty() {
        return Err(Error::Other(format!(
            "bind address {bind:?} did not resolve"
        )));
    }
    if !allow_non_loopback && !resolved.iter().all(|addr| addr.ip().is_loopback()) {
        return Err(Error::Other(
            "WEBEX_ACCOUNT_BOT_BIND must resolve only to loopback addresses; set WEBEX_ACCOUNT_BOT_ALLOW_NON_LOOPBACK=1 only for explicitly secured deployments"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_account_bot_paths(event_path: &str, health_path: &str) -> Result<()> {
    validate_http_path("WEBEX_ACCOUNT_BOT_PATH", event_path)?;
    validate_http_path("WEBEX_ACCOUNT_BOT_HEALTH_PATH", health_path)?;
    if event_path == health_path {
        return Err(Error::Other(
            "WEBEX_ACCOUNT_BOT_PATH must differ from WEBEX_ACCOUNT_BOT_HEALTH_PATH".to_owned(),
        ));
    }
    Ok(())
}

fn validate_http_path(name: &str, path: &str) -> Result<()> {
    if path.starts_with('/') {
        Ok(())
    } else {
        Err(Error::Other(format!("{name} must start with '/'")))
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
        if let Some(header_end) = find_bytes(&bytes, b"\r\n\r\n") {
            if header_end > MAX_HEADER_BYTES {
                return Err(Error::Other(
                    "request headers exceeded maximum size".to_owned(),
                ));
            }
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = parse_content_length(&headers)?.unwrap_or(0);
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
        } else if bytes.len() > MAX_HEADER_BYTES {
            return Err(Error::Other(
                "request headers exceeded maximum size".to_owned(),
            ));
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

fn parse_content_length(headers: &str) -> Result<Option<usize>> {
    Ok(headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| {
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| Error::Other("invalid content-length".to_owned()))
            })
        })
        .transpose()?)
}

async fn write_response(
    stream: &mut (impl AsyncWrite + Unpin),
    response: &HttpResponse,
) -> Result<()> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let mut raw = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        reason,
        response.body.len(),
    );
    for (name, value) in &response.headers {
        raw.push_str(name);
        raw.push_str(": ");
        raw.push_str(value);
        raw.push_str("\r\n");
    }
    raw.push_str("\r\n");
    raw.push_str(&response.body);
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

#[derive(Debug)]
struct HandleResponse {
    response: HttpResponse,
    event_counted: bool,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: BTreeMap<&'static str, String>,
    body: String,
}

impl HttpResponse {
    fn json_value(status: u16, value: serde_json::Value) -> Self {
        Self {
            status,
            headers: BTreeMap::new(),
            body: serde_json::to_string(&value).expect("JSON value serialization cannot fail"),
        }
    }

    fn json_error(status: u16, error: impl AsRef<str>) -> Self {
        Self::json_value(status, json!({ "ok": false, "error": error.as_ref() }))
    }

    fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.insert(name, value.into());
        self
    }
}

fn error_response(error: &BotError) -> HttpResponse {
    match error {
        BotError::RetryableApi {
            error: Error::Api(api_error),
            retry_after,
        } if (400..=599).contains(&api_error.status) => {
            let receiver_status = if api_error.status == 401 {
                503
            } else {
                api_error.status
            };
            HttpResponse::json_error(receiver_status, error.to_string())
                .with_header("Retry-After", retry_after_secs(*retry_after))
        }
        BotError::Webex(Error::Api(api_error)) if (400..=599).contains(&api_error.status) => {
            let mut response = HttpResponse::json_error(api_error.status, error.to_string());
            if let Some(retry_after) = api_error.retry_after {
                response = response.with_header("Retry-After", retry_after_secs(retry_after));
            }
            response
        }
        BotError::RetryAfter { retry_after, .. } => {
            HttpResponse::json_error(503, error.to_string())
                .with_header("Retry-After", retry_after_secs(*retry_after))
        }
        _ => HttpResponse::json_error(503, error.to_string()),
    }
}

fn is_webex_api_status(error: &Error, status: u16) -> bool {
    matches!(error, Error::Api(api_error) if api_error.status == status)
}

fn retryable_api_failure_lease(error: &Error, fallback: Duration) -> Option<Duration> {
    match error {
        Error::Api(api_error)
            if api_error.status == 401
                || api_error.status == 408
                || api_error.status == 429
                || api_error.status >= 500 =>
        {
            Some(retry_after_for_lease(
                api_error.retry_after.unwrap_or(fallback),
            ))
        }
        _ => None,
    }
}

fn create_message_error_could_have_been_accepted(error: &Error) -> bool {
    match error {
        Error::Http(error) => !error.is_builder() && !error.is_connect(),
        Error::Json(_) => true,
        _ => false,
    }
}

fn retry_after_secs(duration: Duration) -> String {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0))
        .max(1)
        .to_string()
}

fn retry_after_for_lease(lease: Duration) -> Duration {
    let lease_secs = lease
        .as_secs()
        .saturating_add(u64::from(lease.subsec_nanos() > 0))
        .max(1);
    Duration::from_secs(lease_secs)
}

fn reply_text(prefix: &str, message: &BotMessage) -> String {
    let body = message
        .markdown
        .as_deref()
        .or(message.text.as_deref())
        .map(collapse_whitespace)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "message".to_owned());
    let preview = truncate_chars(&body, 240);
    if prefix.trim().is_empty() {
        preview
    } else {
        format!("{}: {preview}", prefix.trim())
    }
}

fn collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let mut truncated = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        truncated.push_str("...");
    }
    truncated
}

fn temp_path_for(path: &Path) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    path.with_extension(format!("tmp.{}.{}", std::process::id(), suffix))
}

fn env_or(name: &str, fallback: &str) -> String {
    env_optional(name).unwrap_or_else(|| fallback.to_owned())
}

fn env_optional(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> bool {
    env::var(name).ok().as_deref() == Some("1")
}

fn env_usize(name: &str, fallback: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(fallback)
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(fallback)
}

fn env_set(name: &str) -> BTreeSet<String> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .split(|ch: char| ch.is_ascii_whitespace() || ch == ',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn print_json_line<T>(value: &T) -> Result<()>
where
    T: Serialize,
{
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir().join(format!(
            "webex-headless-account-bot-{name}-{}-{suffix}",
            std::process::id()
        ))
    }

    fn mock_config(state_file: PathBuf) -> Config {
        Config {
            bind: DEFAULT_BIND.to_owned(),
            event_path: DEFAULT_EVENT_PATH.to_owned(),
            health_path: DEFAULT_HEALTH_PATH.to_owned(),
            expected_forward_token: None,
            allow_non_loopback: false,
            mock: true,
            max_events: 0,
            max_concurrent_requests: 1,
            max_concurrent_connections: 4,
            request_read_timeout: Duration::from_secs(1),
            handler_timeout: Duration::from_secs(1),
            webex_request_timeout: Duration::from_secs(30),
            attempt_lease: Duration::from_secs(60),
            in_flight_wait: Duration::from_secs(1),
            state_file,
            max_processed_ids: 10,
            allowed_room_ids: BTreeSet::new(),
            reply_prefix: DEFAULT_REPLY_PREFIX.to_owned(),
            self_person_id: Some("bot-person".to_owned()),
        }
    }

    #[test]
    fn max_events_reached_counts_accepted_events_without_active_slots() {
        let event_slots = AtomicUsize::new(0);
        let completed_event_slots = AtomicUsize::new(0);
        let accepted_events = AtomicUsize::new(0);
        assert!(!max_events_reached(
            1,
            &event_slots,
            &completed_event_slots,
            &accepted_events
        ));

        accepted_events.store(1, Ordering::Relaxed);
        assert!(max_event_quota_reached(
            1,
            &completed_event_slots,
            &accepted_events
        ));
        assert!(max_events_reached(
            1,
            &event_slots,
            &completed_event_slots,
            &accepted_events
        ));

        event_slots.store(1, Ordering::Relaxed);
        assert!(max_event_quota_reached(
            1,
            &completed_event_slots,
            &accepted_events
        ));
        assert!(max_event_admission_closed(
            1,
            &event_slots,
            &completed_event_slots,
            &accepted_events
        ));
        assert!(!max_events_reached(
            1,
            &event_slots,
            &completed_event_slots,
            &accepted_events
        ));

        accepted_events.store(0, Ordering::Relaxed);
        assert!(max_event_admission_closed(
            1,
            &event_slots,
            &completed_event_slots,
            &accepted_events
        ));

        completed_event_slots.store(1, Ordering::Relaxed);
        assert!(max_events_reached(
            1,
            &event_slots,
            &completed_event_slots,
            &accepted_events
        ));

        assert!(!max_events_reached(
            0,
            &event_slots,
            &completed_event_slots,
            &accepted_events
        ));
    }

    struct MissingTokenProvider;

    #[async_trait::async_trait]
    impl AccessTokenProvider for MissingTokenProvider {
        async fn access_token(&self) -> Result<String> {
            Err(Error::MissingToken)
        }
    }

    struct SlowTokenProvider {
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl AccessTokenProvider for SlowTokenProvider {
        async fn access_token(&self) -> Result<String> {
            sleep(self.delay).await;
            Ok("token".to_owned())
        }
    }

    async fn account_bot_with_webex_response(
        state_file: PathBuf,
        status: u16,
        body: &'static str,
    ) -> (AccountBot, tokio::task::JoinHandle<String>) {
        let (base_url, captured_request) = spawn_webex_response_server(status, body).await;
        let client = WebexClient::builder()
            .unwrap()
            .base_url(base_url)
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            ..mock_config(state_file.clone())
        };
        let self_person_id = config.self_person_id.clone();
        let processed = ProcessedMessageStore::load(state_file, config.max_processed_ids).unwrap();
        processed.verify_persistable().unwrap();
        let bot = AccountBot {
            config,
            client: Some(client),
            self_person_id,
            processed: Arc::new(Mutex::new(processed)),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        };

        (bot, captured_request)
    }

    async fn spawn_webex_response_server(
        status: u16,
        body: &'static str,
    ) -> (url::Url, tokio::task::JoinHandle<String>) {
        spawn_webex_response_server_with_retry_after(status, body, None).await
    }

    async fn spawn_webex_response_server_with_retry_after(
        status: u16,
        body: &'static str,
        retry_after: Option<&'static str>,
    ) -> (url::Url, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let captured_request = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_raw_http_request(&mut stream).await;
            let reason = match status {
                200 => "OK",
                500 => "Internal Server Error",
                _ => "Error",
            };
            let retry_after = retry_after
                .map(|value| format!("Retry-After: {value}\r\n"))
                .unwrap_or_default();
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n{retry_after}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });

        (
            url::Url::parse(&format!("http://{address}/v1/")).unwrap(),
            captured_request,
        )
    }

    async fn read_raw_http_request(stream: &mut TcpStream) -> Vec<u8> {
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

    async fn captured_request(request: tokio::task::JoinHandle<String>) -> String {
        tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .unwrap()
            .unwrap()
    }

    async fn send_event_request(address: SocketAddr, message_id: &str) -> String {
        let body = serde_json::to_string(&SidecarEvent::message_created(json!({
            "id": message_id,
            "roomId": "room-1",
            "personId": "person-1",
            "text": "hello"
        })))
        .unwrap();
        let request = format!(
            "POST /webex/events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        response
    }

    async fn send_health_request(address: SocketAddr) -> String {
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        response
    }

    async fn post_event_to_bot(bot: Arc<AccountBot>, event: SidecarEvent) -> (String, usize) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted_events = Arc::new(AtomicUsize::new(0));
        let accepted_events_for_assert = accepted_events.clone();
        let shutdown = Arc::new(Notify::new());
        let semaphore = Arc::new(Semaphore::new(1));
        let connection_permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(
                stream,
                peer,
                bot,
                accepted_events,
                shutdown,
                semaphore,
                connection_permit,
            )
            .await;
        });

        let body = serde_json::to_string(&event).unwrap();
        let request = format!(
            "POST /webex/events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        server.await.unwrap();

        (response, accepted_events_for_assert.load(Ordering::Relaxed))
    }

    fn assert_attempt_can_restart(bot: &AccountBot, message_id: &str) {
        let mut processed = bot.processed.lock().unwrap();
        assert_eq!(
            processed
                .begin_attempt_at(message_id, Duration::from_secs(60), unix_timestamp_secs())
                .unwrap(),
            BeginAttempt::Started
        );
    }

    fn assert_attempt_leased(result: BeginAttempt, min_secs: u64, max_secs: u64) {
        match result {
            BeginAttempt::AttemptLeased(retry_after) => {
                assert!(
                    (min_secs..=max_secs).contains(&retry_after.as_secs()),
                    "unexpected retry_after: {retry_after:?}"
                );
            }
            other => panic!("expected attempt lease, got {other:?}"),
        }
    }

    fn assert_bot_attempt_leased(bot: &AccountBot, message_id: &str, min_secs: u64, max_secs: u64) {
        let mut processed = bot.processed.lock().unwrap();
        assert_attempt_leased(
            processed
                .begin_attempt_at(message_id, Duration::from_secs(60), unix_timestamp_secs())
                .unwrap(),
            min_secs,
            max_secs,
        );
    }

    #[test]
    fn processed_store_persists_and_rejects_duplicates() {
        let path = temp_file("store");
        let mut store = ProcessedMessageStore::load(path.clone(), 2).unwrap();

        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1000)
                .unwrap(),
            BeginAttempt::Started
        );
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1001)
                .unwrap(),
            BeginAttempt::InProgress
        );
        store.release("message-1").unwrap();
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1002)
                .unwrap(),
            BeginAttempt::Started
        );
        store.remember("message-1").unwrap();
        store.remember("message-2").unwrap();
        store.remember("message-3").unwrap();
        store.remember("message-3").unwrap();

        let mut reloaded = ProcessedMessageStore::load(path.clone(), 2).unwrap();
        assert!(!reloaded.contains("message-1"));
        assert!(reloaded.contains("message-2"));
        assert!(reloaded.contains("message-3"));
        assert_eq!(reloaded.len(), 2);
        assert_eq!(
            reloaded
                .begin_attempt_at("message-3", Duration::from_secs(60), 1003)
                .unwrap(),
            BeginAttempt::Processed
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn processed_store_escapes_delimited_message_ids() {
        let path = temp_file("escaped-message-ids");
        let mut store = ProcessedMessageStore::load(path.clone(), 10).unwrap();
        let message_id = "message-1\nprocessed\tinjected\tattempt\t9999999999\tpoison";
        let attempt_id = "message-2\twith-tab";
        let now = unix_timestamp_secs();

        store.remember(message_id).unwrap();
        assert_eq!(
            store
                .begin_attempt_at(attempt_id, Duration::from_secs(60), now)
                .unwrap(),
            BeginAttempt::Started
        );

        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("processed_v2\t"));
        assert!(contents.contains(&format!("attempt_v2\t{}\tmessage-2%09with-tab", now + 60)));
        assert!(!contents.contains("processed\tinjected"));
        assert!(!contents.contains("\\nattempt\\t9999999999\\tpoison"));

        let mut reloaded = ProcessedMessageStore::load(path.clone(), 10).unwrap();
        assert!(reloaded.contains(message_id));
        assert_attempt_leased(
            reloaded
                .begin_attempt_at(attempt_id, Duration::from_secs(60), now + 1)
                .unwrap(),
            59,
            59,
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn processed_store_attempt_lease_survives_restart_and_expires() {
        let path = temp_file("attempt");
        let mut store = ProcessedMessageStore::load(path.clone(), 10).unwrap();

        let now = unix_timestamp_secs();
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), now)
                .unwrap(),
            BeginAttempt::Started
        );
        let mut reloaded = ProcessedMessageStore::load(path.clone(), 10).unwrap();
        assert_attempt_leased(
            reloaded
                .begin_attempt_at("message-1", Duration::from_secs(60), now + 1)
                .unwrap(),
            59,
            59,
        );

        fs::write(&path, "attempt\t1\tmessage-1\nlegacy-message\n").unwrap();
        let mut reloaded = ProcessedMessageStore::load(path.clone(), 10).unwrap();
        assert!(reloaded.contains("legacy-message"));
        assert_eq!(
            reloaded
                .begin_attempt_at("message-1", Duration::from_secs(60), 1002)
                .unwrap(),
            BeginAttempt::Started
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn processed_store_deferred_retry_keeps_attempt_until_lease_expires() {
        let path = temp_file("deferred-retry");
        let mut store = ProcessedMessageStore::load(path.clone(), 10).unwrap();

        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1000)
                .unwrap(),
            BeginAttempt::Started
        );
        store
            .defer_attempt_at("message-1", Duration::from_secs(60), 1000)
            .unwrap();
        assert_attempt_leased(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1001)
                .unwrap(),
            59,
            59,
        );
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1061)
                .unwrap(),
            BeginAttempt::Started
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn processed_store_expired_attempt_clears_stale_inflight_marker() {
        let path = temp_file("expired-inflight");
        let mut store = ProcessedMessageStore::load(path.clone(), 10).unwrap();

        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(1), 1000)
                .unwrap(),
            BeginAttempt::Started
        );
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1000)
                .unwrap(),
            BeginAttempt::InProgress
        );
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1001)
                .unwrap(),
            BeginAttempt::Started
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn processed_store_release_clears_attempt_for_immediate_retry() {
        let path = temp_file("release-retry");
        let mut store = ProcessedMessageStore::load(path.clone(), 10).unwrap();

        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1000)
                .unwrap(),
            BeginAttempt::Started
        );
        store.release("message-1").unwrap();
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1001)
                .unwrap(),
            BeginAttempt::Started
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn processed_store_complete_rollback_restores_evicted_ids_on_persist_failure() {
        let path = temp_file("rollback-store");
        let bad_parent = temp_file("rollback-parent");
        let mut store = ProcessedMessageStore::load(path.clone(), 2).unwrap();

        store.complete_at("message-1", 1000).unwrap();
        store.complete_at("message-2", 1001).unwrap();
        fs::write(&bad_parent, "not a directory").unwrap();
        store.path = bad_parent.join("state");

        assert!(store.complete_at("message-3", 1002).is_err());
        assert!(store.contains("message-1"));
        assert!(store.contains("message-2"));
        assert!(!store.contains("message-3"));
        assert_eq!(store.len(), 2);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[test]
    fn processed_store_volatile_processed_retries_persist_before_ack() {
        let path = temp_file("volatile-persist-retry-store");
        let bad_parent = temp_file("volatile-persist-retry-parent");
        let mut store = ProcessedMessageStore::load(path.clone(), 2).unwrap();

        store.complete_at("old-message-1", 1000).unwrap();
        store.complete_at("old-message-2", 1001).unwrap();
        store.mark_volatile_processed("message-1", 1002);
        fs::write(&bad_parent, "not a directory").unwrap();
        store.path = bad_parent.join("state");

        assert!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1003)
                .is_err()
        );
        assert!(store.contains_volatile("message-1"));
        assert!(store.contains("old-message-1"));
        assert!(store.contains("old-message-2"));
        assert!(!store.contains("message-1"));

        store.path = path.clone();
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), 1004)
                .unwrap(),
            BeginAttempt::ProcessedAfterPersist
        );
        assert!(!store.contains_volatile("message-1"));
        assert!(!store.contains("old-message-1"));
        assert!(store.contains("old-message-2"));
        assert!(store.contains("message-1"));
        let reloaded = ProcessedMessageStore::load(path.clone(), 2).unwrap();
        assert!(reloaded.contains("message-1"));

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[test]
    fn in_flight_drop_refreshes_attempt_lease_for_timeout_retry() {
        let path = temp_file("drop-refresh-lease");
        let store = Arc::new(Mutex::new(
            ProcessedMessageStore::load(path.clone(), 10).unwrap(),
        ));
        {
            let mut processed = store.lock().unwrap();
            assert_eq!(
                processed
                    .begin_attempt_at("message-1", Duration::from_secs(1), 1)
                    .unwrap(),
                BeginAttempt::Started
            );
        }
        let state_persist_failed = Arc::new(AtomicBool::new(false));
        drop(InFlightMessage {
            store: store.clone(),
            state_persist_failed: state_persist_failed.clone(),
            id: "message-1".to_owned(),
            lease: Duration::from_secs(60),
            active: true,
            reply_started: false,
        });
        assert!(!state_persist_failed.load(Ordering::Relaxed));

        let mut reloaded = ProcessedMessageStore::load(path.clone(), 10).unwrap();
        assert_attempt_leased(
            reloaded
                .begin_attempt_at("message-1", Duration::from_secs(60), unix_timestamp_secs())
                .unwrap(),
            1,
            60,
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn in_flight_drop_keeps_refreshed_lease_in_memory_when_persist_fails() {
        let path = temp_file("drop-refresh-persist-fail-store");
        let bad_parent = temp_file("drop-refresh-persist-fail-parent");
        let store = Arc::new(Mutex::new(
            ProcessedMessageStore::load(path.clone(), 10).unwrap(),
        ));
        {
            let mut processed = store.lock().unwrap();
            assert_eq!(
                processed
                    .begin_attempt_at("message-1", Duration::from_secs(1), 1)
                    .unwrap(),
                BeginAttempt::Started
            );
        }
        fs::write(&bad_parent, "not a directory").unwrap();
        store.lock().unwrap().path = bad_parent.join("state");
        let state_persist_failed = Arc::new(AtomicBool::new(false));
        drop(InFlightMessage {
            store: store.clone(),
            state_persist_failed: state_persist_failed.clone(),
            id: "message-1".to_owned(),
            lease: Duration::from_secs(60),
            active: true,
            reply_started: false,
        });

        assert!(state_persist_failed.load(Ordering::Relaxed));
        assert_attempt_leased(
            store
                .lock()
                .unwrap()
                .begin_attempt_at("message-1", Duration::from_secs(60), unix_timestamp_secs())
                .unwrap(),
            1,
            60,
        );

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[test]
    fn in_flight_release_failure_still_drops_active_inflight_marker() {
        let path = temp_file("release-drop-store");
        let bad_parent = temp_file("release-drop-parent");
        let store = Arc::new(Mutex::new(
            ProcessedMessageStore::load(path.clone(), 10).unwrap(),
        ));
        let state_persist_failed = Arc::new(AtomicBool::new(false));
        let inflight = match InFlightMessage::begin(
            &store,
            &state_persist_failed,
            "message-1".to_owned(),
            Duration::from_secs(60),
        )
        .unwrap()
        {
            BeginInFlight::Started(inflight) => inflight,
            _ => panic!("message-1 should start an in-flight attempt"),
        };
        fs::write(&bad_parent, "not a directory").unwrap();
        store.lock().unwrap().path = bad_parent.join("state");

        assert!(inflight.release().is_err());
        assert_attempt_leased(
            store
                .lock()
                .unwrap()
                .begin_attempt_at("message-1", Duration::from_secs(60), unix_timestamp_secs())
                .unwrap(),
            1,
            60,
        );

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[tokio::test]
    async fn account_bot_health_check_bypasses_max_events_admission() {
        let path = temp_file("health-bypass-max-events-admission");
        let config = Config {
            max_events: 1,
            max_concurrent_connections: 2,
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot::new(config).await.unwrap());
        bot.event_slots.store(1, Ordering::Relaxed);
        let accepted_events = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let loop_bot = bot.clone();
        let loop_accepted_events = accepted_events.clone();
        let shutdown = Arc::new(Notify::new());
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            let connection_permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
            handle_connection(
                stream,
                peer,
                loop_bot,
                loop_accepted_events,
                shutdown,
                Arc::new(Semaphore::new(1)),
                connection_permit,
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        server.await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response:?}");
        assert_eq!(accepted_events.load(Ordering::Relaxed), 0);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_health_check_bypasses_event_semaphore() {
        let path = temp_file("health-bypass-semaphore");
        let bot = Arc::new(AccountBot::new(mock_config(path.clone())).await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted_events = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(Notify::new());
        let semaphore = Arc::new(Semaphore::new(0));
        let connection_permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(
                stream,
                peer,
                bot,
                accepted_events,
                shutdown,
                semaphore,
                connection_permit,
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        server.await.unwrap();

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "unexpected health response: {response:?}"
        );
        assert!(
            response.contains("\"statePersistenceHealthy\":true"),
            "unexpected health response body: {response:?}"
        );

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_max_events_reservation_rejects_concurrent_extra_event() {
        let path = temp_file("max-events-reservation");
        let config = Config {
            max_events: 1,
            max_concurrent_requests: 2,
            max_concurrent_connections: 2,
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot::new(config).await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted_events = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(Notify::new());
        let semaphore = Arc::new(Semaphore::new(2));
        let server_bot = bot.clone();
        let server_accepted_events = accepted_events.clone();
        let server = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            for _ in 0..2 {
                let (stream, peer) = listener.accept().await.unwrap();
                let connection_permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
                tasks.spawn(handle_connection(
                    stream,
                    peer,
                    server_bot.clone(),
                    server_accepted_events.clone(),
                    shutdown.clone(),
                    semaphore.clone(),
                    connection_permit,
                ));
            }
            while let Some(joined) = tasks.join_next().await {
                joined.unwrap();
            }
        });

        let first = tokio::spawn(send_event_request(address, "message-1"));
        let second = tokio::spawn(send_event_request(address, "message-2"));
        let mut responses = vec![first.await.unwrap(), second.await.unwrap()];
        responses.sort();
        server.await.unwrap();

        assert_eq!(accepted_events.load(Ordering::Relaxed), 1);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 1);
        assert!(
            responses
                .iter()
                .any(|response| response.starts_with("HTTP/1.1 200 OK"))
        );
        assert!(responses.iter().any(|response| {
            response.starts_with("HTTP/1.1 503 Service Unavailable")
                && response.contains("max events reached")
        }));
        assert_eq!(bot.processed.lock().unwrap().len(), 1);

        let _ = fs::remove_file(path);
    }

    struct FailingWriter;

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn account_bot_max_events_counts_processed_event_when_response_write_fails() {
        let path = temp_file("max-events-response-write-failure");
        let config = Config {
            max_events: 1,
            ..mock_config(path.clone())
        };
        let bot = AccountBot::new(config).await.unwrap();
        let accepted_events = AtomicUsize::new(0);
        let shutdown = Notify::new();
        let mut writer = FailingWriter;

        write_final_response(
            &mut writer,
            "127.0.0.1:1".parse().unwrap(),
            HandleResponse {
                response: HttpResponse::json_value(200, json!({ "ok": true })),
                event_counted: true,
            },
            &bot,
            &accepted_events,
            &shutdown,
        )
        .await;

        assert_eq!(accepted_events.load(Ordering::Relaxed), 1);
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_max_events_retryable_busy_failure_allows_next_event() {
        let path = temp_file("max-events-busy-then-success");
        let config = Config {
            max_events: 1,
            max_concurrent_requests: 1,
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot::new(config).await.unwrap());
        let accepted_events = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(Notify::new());

        let busy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let busy_address = busy_listener.local_addr().unwrap();
        let busy_bot = bot.clone();
        let busy_accepted_events = accepted_events.clone();
        let busy_shutdown = shutdown.clone();
        let busy_server = tokio::spawn(async move {
            let (stream, peer) = busy_listener.accept().await.unwrap();
            let connection_permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
            handle_connection(
                stream,
                peer,
                busy_bot,
                busy_accepted_events,
                busy_shutdown,
                Arc::new(Semaphore::new(0)),
                connection_permit,
            )
            .await;
        });
        let busy_response = send_event_request(busy_address, "message-1").await;
        busy_server.await.unwrap();
        assert!(busy_response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(busy_response.contains("server busy"));
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 0);

        let success_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let success_address = success_listener.local_addr().unwrap();
        let success_bot = bot.clone();
        let success_accepted_events = accepted_events.clone();
        let success_server = tokio::spawn(async move {
            let (stream, peer) = success_listener.accept().await.unwrap();
            let connection_permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
            handle_connection(
                stream,
                peer,
                success_bot,
                success_accepted_events,
                shutdown,
                Arc::new(Semaphore::new(1)),
                connection_permit,
            )
            .await;
        });
        let success_response = send_event_request(success_address, "message-1").await;
        success_server.await.unwrap();
        assert!(success_response.starts_with("HTTP/1.1 200 OK"));
        assert_eq!(accepted_events.load(Ordering::Relaxed), 1);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 1);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_event_busy_response_includes_retry_after() {
        let path = temp_file("event-busy-retry-after");
        let bot = Arc::new(AccountBot::new(mock_config(path.clone())).await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted_events = Arc::new(AtomicUsize::new(0));
        let accepted_events_for_assert = accepted_events.clone();
        let shutdown = Arc::new(Notify::new());
        let semaphore = Arc::new(Semaphore::new(0));
        let connection_permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(
                stream,
                peer,
                bot,
                accepted_events,
                shutdown,
                semaphore,
                connection_permit,
            )
            .await;
        });

        let body = serde_json::to_string(&SidecarEvent::message_created(json!({
            "id": "message-1",
            "roomId": "room-1",
            "personId": "person-1",
            "text": "hello"
        })))
        .unwrap();
        let request = format!(
            "POST /webex/events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        server.await.unwrap();

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("Retry-After: 1\r\n"));
        assert!(response.contains("server busy"));
        assert_eq!(accepted_events_for_assert.load(Ordering::Relaxed), 0);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_connection_busy_response_includes_retry_after() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            reject_busy_connection(stream, peer, Duration::from_secs(3)).await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        server.await.unwrap();

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("Retry-After: 3\r\n"));
        assert!(response.contains("server busy"));
    }

    #[tokio::test]
    async fn read_request_rejects_headers_that_exceed_cap_before_terminator() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream).await.unwrap_err().to_string()
        });

        let mut request = Vec::new();
        request.extend_from_slice(b"GET /healthz HTTP/1.1\r\nX-Fill: ");
        request.extend(std::iter::repeat(b'a').take(MAX_HEADER_BYTES));
        request.extend_from_slice(b"\r\n\r\n");
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&request).await.unwrap();
        drop(client);

        let error = server.await.unwrap();
        assert!(error.contains("request headers exceeded maximum size"));
    }

    #[tokio::test]
    async fn account_bot_waits_for_duplicate_active_attempt_to_finish() {
        let path = temp_file("in-progress-event");
        let bot = AccountBot::new(mock_config(path.clone())).await.unwrap();
        {
            let mut processed = bot.processed.lock().unwrap();
            assert_eq!(
                processed
                    .begin_attempt_at("message-1", Duration::from_secs(60), unix_timestamp_secs())
                    .unwrap(),
                BeginAttempt::Started
            );
        }
        let processed = bot.processed.clone();
        let finish_attempt = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            processed.lock().unwrap().remember("message-1").unwrap();
        });

        let action = bot
            .handle_event(SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })))
            .await
            .unwrap();

        finish_attempt.await.unwrap();
        assert_eq!(action.action, "ignored");
        assert_eq!(action.reason, Some("duplicate_message"));
        assert_eq!(action.message_id.as_deref(), Some("message-1"));
        assert_eq!(action.room_id.as_deref(), Some("room-1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_duplicate_takes_over_after_active_attempt_releases() {
        let path = temp_file("in-progress-release-event");
        let bot = AccountBot::new(mock_config(path.clone())).await.unwrap();
        {
            let mut processed = bot.processed.lock().unwrap();
            assert_eq!(
                processed
                    .begin_attempt_at("message-1", Duration::from_secs(60), unix_timestamp_secs())
                    .unwrap(),
                BeginAttempt::Started
            );
        }
        let processed = bot.processed.clone();
        let release_attempt = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            processed.lock().unwrap().release("message-1").unwrap();
        });

        let action = bot
            .handle_event(SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })))
            .await
            .unwrap();

        release_attempt.await.unwrap();
        assert_eq!(action.action, "mock_replied");
        assert_eq!(action.message_id.as_deref(), Some("message-1"));
        assert_eq!(action.room_id.as_deref(), Some("room-1"));
        assert!(bot.processed.lock().unwrap().contains("message-1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_still_active_duplicate_returns_retryable_http_error() {
        let path = temp_file("still-active-duplicate");
        let mut config = mock_config(path.clone());
        config.in_flight_wait = Duration::from_millis(10);
        let bot = Arc::new(AccountBot::new(config).await.unwrap());
        {
            let mut processed = bot.processed.lock().unwrap();
            assert_eq!(
                processed
                    .begin_attempt_at("message-1", Duration::from_secs(60), unix_timestamp_secs())
                    .unwrap(),
                BeginAttempt::Started
            );
        }
        let (response, accepted_events) = post_event_to_bot(
            bot,
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("Retry-After: 1\r\n"));
        assert!(response.contains("reply attempt is already in progress"));
        assert_eq!(accepted_events, 0);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_attempt_lease_duplicate_returns_retry_after() {
        let path = temp_file("attempt-lease-duplicate-retry-after");
        let mut config = mock_config(path.clone());
        config.attempt_lease = Duration::from_secs(3);
        let bot = Arc::new(AccountBot::new(config).await.unwrap());
        {
            let mut processed = bot.processed.lock().unwrap();
            assert_eq!(
                processed
                    .begin_attempt_at("message-1", Duration::from_secs(3), unix_timestamp_secs())
                    .unwrap(),
                BeginAttempt::Started
            );
            processed
                .defer_attempt("message-1", Duration::from_secs(3))
                .unwrap();
        }
        let (response, accepted_events) = post_event_to_bot(
            bot,
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("Retry-After: "));
        assert!(response.contains("reply attempt is pending retry lease"));
        assert_eq!(accepted_events, 0);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_wait_for_active_attempt_returns_in_progress() {
        let path = temp_file("wait-for-active-attempt");
        let mut config = mock_config(path.clone());
        config.in_flight_wait = Duration::from_millis(50);
        let bot = AccountBot::new(config).await.unwrap();
        let inflight = match bot.begin_in_flight_attempt("message-1".to_owned()).unwrap() {
            BeginInFlight::Started(inflight) => inflight,
            _ => panic!("unexpected begin result"),
        };

        let result = bot.wait_for_in_flight_attempt("message-1").await.unwrap();
        assert!(matches!(result, BeginInFlight::InProgress));

        drop(inflight);
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_max_events_slot_survives_handler_timeout_after_post() {
        let path = temp_file("max-events-timeout-after-post");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let webex_request_count = Arc::new(AtomicUsize::new(0));
        let first_webex_request_seen = Arc::new(Notify::new());
        let request_count = webex_request_count.clone();
        let request_seen = first_webex_request_seen.clone();
        let webex_server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            request_count.fetch_add(1, Ordering::Relaxed);
            request_seen.notify_waiters();
            let _ = read_raw_http_request(&mut stream).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse(&format!("http://{address}/v1/")).unwrap())
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            max_events: 1,
            max_concurrent_requests: 2,
            handler_timeout: Duration::from_millis(100),
            attempt_lease: Duration::from_secs(6),
            in_flight_wait: Duration::from_millis(20),
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });

        let first_attempt = tokio::spawn(post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        ));
        tokio::time::timeout(Duration::from_secs(2), first_webex_request_seen.notified())
            .await
            .unwrap();
        let (first_response, first_accepted_events) = first_attempt.await.unwrap();
        assert!(first_response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(first_response.contains("handler timeout"));
        assert_eq!(first_accepted_events, 0);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 1);

        let (second_response, second_accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-2",
                "roomId": "room-1",
                "personId": "person-2",
                "text": "hello"
            })),
        )
        .await;
        assert!(second_response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(second_response.contains("max events reached"));
        assert_eq!(second_accepted_events, 0);
        assert_eq!(webex_request_count.load(Ordering::Relaxed), 1);

        webex_server.abort();
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_max_events_loop_exits_after_post_timeout_completion() {
        let path = temp_file("max-events-loop-exits-after-timeout-completion");
        let webex_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let webex_address = webex_listener.local_addr().unwrap();
        let request_seen = Arc::new(Notify::new());
        let request_seen_for_server = request_seen.clone();
        let webex_server = tokio::spawn(async move {
            let (mut stream, _) = webex_listener.accept().await.unwrap();
            let _ = read_raw_http_request(&mut stream).await;
            request_seen_for_server.notify_waiters();
            tokio::time::sleep(Duration::from_millis(800)).await;
            let body = r#"{"id":"reply-1","roomId":"room-1","parentId":"message-1","text":"ack"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse(&format!("http://{webex_address}/v1/")).unwrap())
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            max_events: 1,
            max_concurrent_connections: 1,
            handler_timeout: Duration::from_millis(500),
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let loop_bot = bot.clone();
        let loop_task = tokio::spawn(async move { run_http_loop(listener, loop_bot).await });

        let response_task = tokio::spawn(send_event_request(address, "message-1"));
        tokio::time::timeout(Duration::from_secs(2), request_seen.notified())
            .await
            .unwrap();
        let response = response_task.await.unwrap();
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("handler timeout"));

        tokio::time::timeout(Duration::from_secs(2), loop_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        webex_server.await.unwrap();
        assert_eq!(bot.completed_event_slots.load(Ordering::Relaxed), 1);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 1);
        assert!(bot.processed.lock().unwrap().contains("message-1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_max_events_waits_for_active_slot_after_duplicate_messages() {
        let path = temp_file("max-events-waits-active-slot-after-duplicates");
        let webex_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let webex_address = webex_listener.local_addr().unwrap();
        let request_seen = Arc::new(Notify::new());
        let request_seen_for_server = request_seen.clone();
        let (release_reply, wait_for_release) = oneshot::channel::<()>();
        let webex_server = tokio::spawn(async move {
            let (mut stream, _) = webex_listener.accept().await.unwrap();
            let _ = read_raw_http_request(&mut stream).await;
            request_seen_for_server.notify_waiters();
            let _ = wait_for_release.await;
            let body = r#"{"id":"reply-1","roomId":"room-1","parentId":"message-1","text":"ack"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse(&format!("http://{webex_address}/v1/")).unwrap())
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            max_events: 2,
            max_concurrent_requests: 2,
            max_concurrent_connections: 2,
            handler_timeout: Duration::from_millis(100),
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });
        {
            let mut processed = bot.processed.lock().unwrap();
            processed.remember("message-2").unwrap();
            processed.remember("message-3").unwrap();
            processed.remember("message-4").unwrap();
            assert!(processed.contains("message-2"));
            assert!(processed.contains("message-3"));
            assert!(processed.contains("message-4"));
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let loop_bot = bot.clone();
        let loop_task = tokio::spawn(async move { run_http_loop(listener, loop_bot).await });

        let first_response = tokio::spawn(send_event_request(address, "message-1"));
        tokio::time::timeout(Duration::from_secs(2), request_seen.notified())
            .await
            .unwrap();
        let first_response = first_response.await.unwrap();
        assert!(first_response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(first_response.contains("handler timeout"));
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 1);
        assert_eq!(bot.completed_event_slots.load(Ordering::Relaxed), 0);

        let duplicate_response = send_event_request(address, "message-2").await;
        assert!(
            duplicate_response.starts_with("HTTP/1.1 200 OK"),
            "{duplicate_response}"
        );
        assert!(duplicate_response.contains("duplicate_message"));
        let second_duplicate_response = send_event_request(address, "message-3").await;
        assert!(
            second_duplicate_response.starts_with("HTTP/1.1 200 OK"),
            "{second_duplicate_response}"
        );
        assert!(second_duplicate_response.contains("duplicate_message"));
        let extra_response = send_event_request(address, "message-4").await;
        assert!(
            extra_response.starts_with("HTTP/1.1 503 Service Unavailable"),
            "{extra_response}"
        );
        assert!(extra_response.contains("max events reached"));
        assert!(!loop_task.is_finished());

        let health_response = send_health_request(address).await;
        assert!(
            health_response.starts_with("HTTP/1.1 200 OK"),
            "{health_response}"
        );
        assert!(health_response.contains("\"statePersistenceHealthy\":true"));
        assert!(!loop_task.is_finished());

        release_reply.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), loop_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        webex_server.await.unwrap();
        assert_eq!(bot.completed_event_slots.load(Ordering::Relaxed), 1);
        assert!(bot.processed.lock().unwrap().contains("message-1"));
        assert!(bot.processed.lock().unwrap().contains("message-2"));
        assert!(bot.processed.lock().unwrap().contains("message-3"));
        assert!(bot.processed.lock().unwrap().contains("message-4"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_max_events_loop_exits_after_duplicate_message() {
        let path = temp_file("max-events-loop-exits-after-duplicate");
        let config = Config {
            max_events: 1,
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot::new(config).await.unwrap());
        bot.processed.lock().unwrap().remember("message-1").unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let loop_bot = bot.clone();
        let loop_task = tokio::spawn(async move { run_http_loop(listener, loop_bot).await });

        let response = send_event_request(address, "message-1").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("duplicate_message"));

        tokio::time::timeout(Duration::from_secs(2), loop_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(bot.completed_event_slots.load(Ordering::Relaxed), 0);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 0);
        assert!(bot.processed.lock().unwrap().contains("message-1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_new_attempt_persist_failure_returns_retry_after_and_degrades_health() {
        let path = temp_file("new-attempt-persist-failure");
        let bad_parent = temp_file("new-attempt-bad-parent");
        let mut config = mock_config(path.clone());
        config.attempt_lease = Duration::from_secs(6);
        let bot = Arc::new(AccountBot::new(config).await.unwrap());
        fs::write(&bad_parent, "not a directory").unwrap();
        bot.processed.lock().unwrap().path = bad_parent.join("state");

        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("Retry-After: 6\r\n"));
        assert!(response.contains("processed message state persist failed while leasing message"));
        assert_eq!(accepted_events, 0);
        let health = bot.health_response().unwrap();
        assert_eq!(health.status, 503);
        assert!(health.body.contains("\"statePersistenceHealthy\":false"));

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[tokio::test]
    async fn account_bot_health_degrades_while_volatile_ids_remain() {
        let path = temp_file("volatile-health");
        let bot = AccountBot::new(mock_config(path.clone())).await.unwrap();
        bot.state_persist_failed.store(false, Ordering::Relaxed);
        bot.processed
            .lock()
            .unwrap()
            .mark_volatile_processed("message-1", unix_timestamp_secs());

        let health = bot.health_response().unwrap();
        assert_eq!(health.status, 503);
        assert!(health.body.contains("\"statePersistenceHealthy\":false"));
        assert!(health.body.contains("\"volatileProcessedIds\":1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_attempt_lease_duplicate_does_not_rewrite_state() {
        let path = temp_file("attempt-refresh-persist-failure");
        let bad_parent = temp_file("attempt-refresh-bad-parent");
        let mut config = mock_config(path.clone());
        config.attempt_lease = Duration::from_secs(6);
        let bot = Arc::new(AccountBot::new(config).await.unwrap());
        {
            let mut processed = bot.processed.lock().unwrap();
            assert_eq!(
                processed
                    .begin_attempt_at("message-1", Duration::from_secs(6), unix_timestamp_secs())
                    .unwrap(),
                BeginAttempt::Started
            );
            processed
                .defer_attempt("message-1", Duration::from_secs(6))
                .unwrap();
        }
        fs::write(&bad_parent, "not a directory").unwrap();
        bot.processed.lock().unwrap().path = bad_parent.join("state");

        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("Retry-After: "));
        assert!(response.contains("reply attempt is pending retry lease"));
        assert_eq!(accepted_events, 0);
        let health = bot.health_response().unwrap();
        assert_eq!(health.status, 200);
        assert!(health.body.contains("\"statePersistenceHealthy\":true"));

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[tokio::test]
    async fn account_bot_hydration_api_4xx_preserves_http_status() {
        let path = temp_file("hydration-api-4xx");
        let (bot, request) =
            account_bot_with_webex_response(path.clone(), 403, r#"{"message":"forbidden"}"#).await;
        let (response, accepted_events) = post_event_to_bot(
            Arc::new(bot),
            SidecarEvent::message_created(json!({
                "id": "message-1"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(response.contains("403 Forbidden"));
        assert_eq!(accepted_events, 0);
        let request = captured_request(request).await;
        assert!(request.starts_with("GET /v1/messages/message%2D1 HTTP/1.1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_create_message_api_4xx_preserves_http_status() {
        let path = temp_file("create-message-api-4xx");
        let (bot, request) =
            account_bot_with_webex_response(path.clone(), 403, r#"{"message":"forbidden"}"#).await;
        let (response, accepted_events) = post_event_to_bot(
            Arc::new(bot),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(response.contains("403 Forbidden"));
        assert_eq!(accepted_events, 0);
        let request = captured_request(request).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_create_message_api_401_is_receiver_retryable() {
        let path = temp_file("create-message-api-401-retryable");
        let mut config = mock_config(path.clone());
        config.attempt_lease = Duration::from_secs(6);
        let (base_url, request) =
            spawn_webex_response_server(401, r#"{"message":"unauthorized"}"#).await;
        let client = WebexClient::builder()
            .unwrap()
            .base_url(base_url)
            .access_token("token")
            .build()
            .unwrap();
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config: Config {
                mock: false,
                ..config
            },
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });

        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(
            response.contains("Retry-After: 6\r\n"),
            "unexpected response: {response:?}"
        );
        assert!(
            response.contains("401 Unauthorized"),
            "unexpected response: {response:?}"
        );
        assert_eq!(accepted_events, 0);
        assert_bot_attempt_leased(&bot, "message-1", 1, 6);
        let request = captured_request(request).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_create_message_api_429_forwards_retry_after() {
        let path = temp_file("create-message-api-429-retry-after");
        let (base_url, request) = spawn_webex_response_server_with_retry_after(
            429,
            r#"{"message":"rate limited"}"#,
            Some("30"),
        )
        .await;
        let client = WebexClient::builder()
            .unwrap()
            .base_url(base_url)
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });
        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 429 Too Many Requests"));
        assert!(response.contains("Retry-After: 30\r\n"));
        assert!(response.contains("429 Too Many Requests"));
        assert_eq!(accepted_events, 0);
        let request = captured_request(request).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));
        assert_bot_attempt_leased(&bot, "message-1", 1, 30);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_max_events_retryable_api_failure_releases_slot() {
        let path = temp_file("max-events-api-failure-releases-slot");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_for_server = request_count.clone();
        let webex_server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                request_count_for_server.fetch_add(1, Ordering::Relaxed);
                let _ = read_raw_http_request(&mut stream).await;
                let (status, reason, retry_after, body) = if attempt == 0 {
                    (
                        429,
                        "Too Many Requests",
                        "Retry-After: 2\r\n",
                        r#"{"message":"rate limited"}"#,
                    )
                } else {
                    (
                        200,
                        "OK",
                        "",
                        r#"{"id":"reply-2","roomId":"room-1","parentId":"message-2","text":"ack"}"#,
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n{retry_after}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse(&format!("http://{address}/v1/")).unwrap())
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            max_events: 1,
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });

        let (first_response, first_accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;
        assert!(first_response.starts_with("HTTP/1.1 429 Too Many Requests"));
        assert!(first_response.contains("Retry-After: 2\r\n"));
        assert_eq!(first_accepted_events, 0);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 0);
        assert_bot_attempt_leased(&bot, "message-1", 1, 2);

        let (second_response, second_accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-2",
                "roomId": "room-1",
                "personId": "person-2",
                "text": "hello"
            })),
        )
        .await;
        assert!(second_response.starts_with("HTTP/1.1 200 OK"));
        assert!(second_response.contains("replied"));
        assert_eq!(second_accepted_events, 1);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 1);
        assert!(bot.processed.lock().unwrap().contains("message-2"));
        assert_eq!(request_count.load(Ordering::Relaxed), 2);

        webex_server.await.unwrap();
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_create_message_5xx_without_retry_after_uses_attempt_lease() {
        let path = temp_file("create-message-api-500-fallback-retry-after");
        let (base_url, request) = spawn_webex_response_server(500, r#"{"message":"boom"}"#).await;
        let client = WebexClient::builder()
            .unwrap()
            .base_url(base_url)
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            attempt_lease: Duration::from_secs(7),
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });

        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 500 Internal Server Error"));
        assert!(response.contains("Retry-After: 7\r\n"));
        assert_eq!(accepted_events, 0);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 0);
        assert_bot_attempt_leased(&bot, "message-1", 1, 7);
        let request = captured_request(request).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_room_allowlist_skips_hydration_when_room_is_known() {
        let path = temp_file("allowlist-before-hydration");
        let (base_url, request) =
            spawn_webex_response_server(500, r#"{"message":"should not hydrate"}"#).await;
        let client = WebexClient::builder()
            .unwrap()
            .base_url(base_url)
            .access_token("token")
            .build()
            .unwrap();
        let mut allowed_room_ids = BTreeSet::new();
        allowed_room_ids.insert("allowed-room".to_owned());
        let config = Config {
            mock: false,
            allowed_room_ids,
            ..mock_config(path.clone())
        };
        let bot = AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        };

        let action = bot
            .handle_event(SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "denied-room"
            })))
            .await
            .unwrap();

        assert_eq!(action.action, "ignored");
        assert_eq!(action.reason, Some("room_not_allowed"));
        assert_eq!(action.room_id.as_deref(), Some("denied-room"));
        assert_attempt_can_restart(&bot, "message-1");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), request)
                .await
                .is_err()
        );

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_hydration_404_is_accepted_as_stale_message() {
        let path = temp_file("hydration-404-stale");
        let (bot, request) =
            account_bot_with_webex_response(path.clone(), 404, r#"{"message":"not found"}"#).await;
        let bot = Arc::new(bot);
        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("message_not_found"));
        assert_eq!(accepted_events, 1);
        assert!(bot.processed.lock().unwrap().contains("message-1"));
        let request = captured_request(request).await;
        assert!(request.starts_with("GET /v1/messages/message%2D1 HTTP/1.1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_reply_404_is_accepted_as_stale_message() {
        let path = temp_file("reply-404-stale");
        let (bot, request) =
            account_bot_with_webex_response(path.clone(), 404, r#"{"message":"not found"}"#).await;
        let bot = Arc::new(bot);
        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("message_not_found"));
        assert_eq!(accepted_events, 1);
        assert!(bot.processed.lock().unwrap().contains("message-1"));
        let request = captured_request(request).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_hydration_retryable_api_failure_defers_attempt() {
        let path = temp_file("hydration-failure");
        let (bot, request) =
            account_bot_with_webex_response(path.clone(), 500, r#"{"message":"boom"}"#).await;

        let error = bot
            .handle_event(SidecarEvent::message_created(json!({
                "id": "message-1"
            })))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("500 Internal Server Error"));
        let request = captured_request(request).await;
        assert!(request.starts_with("GET /v1/messages/message%2D1 HTTP/1.1"));
        assert_bot_attempt_leased(&bot, "message-1", 1, 60);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_reply_success_state_persist_failure_returns_retry_after_and_degrades_health()
     {
        let path = temp_file("reply-state-persist-failure");
        let bad_parent = temp_file("reply-state-bad-parent");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse(&format!("http://{address}/v1/")).unwrap())
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            max_events: 1,
            max_processed_ids: 2,
            attempt_lease: Duration::from_secs(2),
            ..mock_config(path.clone())
        };
        let self_person_id = config.self_person_id.clone();
        let mut processed =
            ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap();
        processed.complete_at("old-message-1", 1000).unwrap();
        processed.complete_at("old-message-2", 1001).unwrap();
        processed.verify_persistable().unwrap();
        let bot = Arc::new(AccountBot {
            config,
            client: Some(client),
            self_person_id,
            processed: Arc::new(Mutex::new(processed)),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });
        fs::write(&bad_parent, "not a directory").unwrap();
        let bad_state_path = bad_parent.join("state");
        let processed = bot.processed.clone();
        let captured = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_raw_http_request(&mut stream).await;
            processed.lock().unwrap().path = bad_state_path;
            let body = r#"{"id":"reply-1","roomId":"room-1","parentId":"message-1","text":"ack"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });

        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("Retry-After: 2\r\n"));
        assert!(response.contains("processed message state persist failed after reply"));
        assert_eq!(accepted_events, 0);
        assert_eq!(bot.event_slots.load(Ordering::Relaxed), 0);
        assert_eq!(bot.completed_event_slots.load(Ordering::Relaxed), 0);
        {
            let processed = bot.processed.lock().unwrap();
            assert!(processed.contains("old-message-1"));
            assert!(processed.contains("old-message-2"));
            assert!(!processed.contains("message-1"));
            assert!(processed.contains_volatile("message-1"));
        }
        let health = bot.health_response().unwrap();
        assert_eq!(health.status, 503);
        assert!(health.body.contains("\"statePersistenceHealthy\":false"));
        let request = captured_request(captured).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));

        let (retry_response, retry_accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;
        assert!(retry_response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(
            retry_response.contains("Retry-After: 2\r\n"),
            "unexpected retry response: {retry_response:?}"
        );
        assert!(
            retry_response.contains("processed message state persist failed while leasing message")
        );
        assert_eq!(retry_accepted_events, 0);

        bot.processed.lock().unwrap().path = path.clone();
        let (recovered_response, recovered_accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;
        assert!(recovered_response.starts_with("HTTP/1.1 200 OK"));
        assert!(recovered_response.contains("duplicate_message"));
        assert_eq!(recovered_accepted_events, 1);
        let recovered_health = bot.health_response().unwrap();
        assert_eq!(recovered_health.status, 200);
        assert!(
            recovered_health
                .body
                .contains("\"statePersistenceHealthy\":true")
        );
        {
            let processed = bot.processed.lock().unwrap();
            assert!(!processed.contains_volatile("message-1"));
            assert!(!processed.contains("old-message-1"));
            assert!(processed.contains("old-message-2"));
            assert!(processed.contains("message-1"));
        }

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[test]
    fn volatile_processed_ids_flush_on_next_successful_persist() {
        let path = temp_file("volatile-flush");
        let bad_parent = temp_file("volatile-flush-bad-parent");
        let store = Arc::new(Mutex::new(
            ProcessedMessageStore::load(path.clone(), 10).unwrap(),
        ));
        let state_persist_failed = Arc::new(AtomicBool::new(false));
        let inflight = match InFlightMessage::begin(
            &store,
            &state_persist_failed,
            "message-1".to_owned(),
            Duration::from_secs(60),
        )
        .unwrap()
        {
            BeginInFlight::Started(inflight) => inflight,
            _ => panic!("message-1 should start an in-flight attempt"),
        };
        fs::write(&bad_parent, "not a directory").unwrap();
        let good_path = path.clone();
        store.lock().unwrap().path = bad_parent.join("state");
        assert!(inflight.remember_after_reply().is_err());
        state_persist_failed.store(true, Ordering::Relaxed);
        assert!(store.lock().unwrap().contains_volatile("message-1"));

        store.lock().unwrap().path = good_path.clone();
        let second = match InFlightMessage::begin(
            &store,
            &state_persist_failed,
            "message-2".to_owned(),
            Duration::from_secs(60),
        )
        .unwrap()
        {
            BeginInFlight::Started(inflight) => inflight,
            _ => panic!("message-2 should start an in-flight attempt"),
        };
        assert!(!state_persist_failed.load(Ordering::Relaxed));
        second.remember().unwrap();

        let reloaded = ProcessedMessageStore::load(path.clone(), 10).unwrap();
        assert!(reloaded.contains("message-1"));
        assert!(reloaded.contains("message-2"));
        assert!(!store.lock().unwrap().contains_volatile("message-1"));

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[tokio::test]
    async fn account_bot_create_message_decode_failure_marks_processed() {
        let path = temp_file("create-message-decode-failure-lease");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let webex_request_count = Arc::new(AtomicUsize::new(0));
        let request_count = webex_request_count.clone();
        let webex_server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                request_count.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let _ = read_raw_http_request(&mut stream).await;
                    let body =
                        r#"{"id":"reply-1","roomId":"room-1","parentId":"message-1","text":"ack""#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                });
            }
        });
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse(&format!("http://{address}/v1/")).unwrap())
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            attempt_lease: Duration::from_secs(6),
            in_flight_wait: Duration::from_millis(20),
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });

        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("reply_unknown"));
        assert_eq!(accepted_events, 1);
        assert!(bot.processed.lock().unwrap().contains("message-1"));

        let (retry_response, retry_accepted_events) = post_event_to_bot(
            bot,
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(retry_response.starts_with("HTTP/1.1 200 OK"));
        assert!(retry_response.contains("duplicate_message"));
        assert_eq!(retry_accepted_events, 1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(webex_request_count.load(Ordering::Relaxed), 1);

        webex_server.abort();
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_connection_close_after_post_marks_processed() {
        let path = temp_file("connection-close-after-post");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let webex_request_count = Arc::new(AtomicUsize::new(0));
        let request_count = webex_request_count.clone();
        let webex_server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                request_count.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let _ = read_raw_http_request(&mut stream).await;
                });
            }
        });
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse(&format!("http://{address}/v1/")).unwrap())
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            attempt_lease: Duration::from_secs(6),
            in_flight_wait: Duration::from_millis(20),
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });

        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("reply_unknown"));
        assert_eq!(accepted_events, 1);
        assert!(bot.processed.lock().unwrap().contains("message-1"));

        let (retry_response, retry_accepted_events) = post_event_to_bot(
            bot,
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(retry_response.starts_with("HTTP/1.1 200 OK"));
        assert!(retry_response.contains("duplicate_message"));
        assert_eq!(retry_accepted_events, 1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(webex_request_count.load(Ordering::Relaxed), 1);

        webex_server.abort();
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_abort_before_post_defers_attempt() {
        let path = temp_file("abort-before-post");
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse("http://127.0.0.1:9/v1/").unwrap())
            .token_provider(Arc::new(SlowTokenProvider {
                delay: Duration::from_secs(60),
            }))
            .build()
            .unwrap();
        let store = Arc::new(Mutex::new(
            ProcessedMessageStore::load(path.clone(), 10).unwrap(),
        ));
        let state_persist_failed = Arc::new(AtomicBool::new(false));
        let inflight = match InFlightMessage::begin(
            &store,
            &state_persist_failed,
            "message-1".to_owned(),
            Duration::from_secs(6),
        )
        .unwrap()
        {
            BeginInFlight::Started(inflight) => inflight,
            _ => panic!("message-1 should start an in-flight attempt"),
        };
        let worker = tokio::spawn(send_reply_worker(
            client,
            state_persist_failed.clone(),
            Duration::from_secs(6),
            inflight,
            "message-1".to_owned(),
            "room-1".to_owned(),
            "message-1".to_owned(),
            "ack".to_owned(),
            None,
            EventSlot::unlimited(),
        ));

        tokio::time::sleep(Duration::from_millis(20)).await;
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());

        assert!(!store.lock().unwrap().contains("message-1"));
        assert!(!state_persist_failed.load(Ordering::Relaxed));
        assert_attempt_leased(
            store
                .lock()
                .unwrap()
                .begin_attempt_at("message-1", Duration::from_secs(6), unix_timestamp_secs())
                .unwrap(),
            1,
            6,
        );

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_api_error_preserves_retry_after_when_defer_persist_fails() {
        let path = temp_file("api-error-defer-persist-fails");
        let bad_parent = temp_file("api-error-defer-bad-parent");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        fs::write(&bad_parent, "not a directory").unwrap();
        let bad_state_path = bad_parent.join("state");
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse(&format!("http://{address}/v1/")).unwrap())
            .access_token("token")
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            attempt_lease: Duration::from_secs(6),
            ..mock_config(path.clone())
        };
        let processed = Arc::new(Mutex::new(
            ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
        ));
        let processed_for_server = processed.clone();
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed,
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });
        let captured = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_raw_http_request(&mut stream).await;
            processed_for_server.lock().unwrap().path = bad_state_path;
            let body = r#"{"message":"rate limited"}"#;
            let response = format!(
                "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nRetry-After: 120\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });

        let (response, accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 429 Too Many Requests"));
        assert!(response.contains("Retry-After: 120\r\n"));
        assert!(response.contains("429 Too Many Requests"));
        assert_eq!(accepted_events, 0);
        let health = bot.health_response().unwrap();
        assert_eq!(health.status, 503);
        assert!(health.body.contains("\"statePersistenceHealthy\":false"));
        assert!(!bot.processed.lock().unwrap().contains("message-1"));
        assert_bot_attempt_leased(&bot, "message-1", 100, 120);
        let request = captured_request(captured).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bad_parent);
    }

    #[tokio::test]
    async fn account_bot_timeout_before_send_keeps_in_progress() {
        let path = temp_file("timeout-before-send-keeps-lease");
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse("http://127.0.0.1:9/v1/").unwrap())
            .token_provider(Arc::new(SlowTokenProvider {
                delay: Duration::from_secs(5),
            }))
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            handler_timeout: Duration::from_millis(100),
            attempt_lease: Duration::from_secs(3),
            in_flight_wait: Duration::from_millis(20),
            ..mock_config(path.clone())
        };
        let bot = Arc::new(AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        });

        let (first_response, first_accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(first_response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(first_response.contains("Retry-After: 3\r\n"));
        assert!(first_response.contains("handler timeout"));
        assert_eq!(first_accepted_events, 0);
        assert!(!bot.processed.lock().unwrap().contains("message-1"));
        tokio::time::sleep(Duration::from_millis(1200)).await;

        let (retry_response, retry_accepted_events) = post_event_to_bot(
            bot.clone(),
            SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })),
        )
        .await;

        assert!(retry_response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(retry_response.contains("Retry-After: 1\r\n"));
        assert!(retry_response.contains("reply attempt is already in progress"));
        assert_eq!(retry_accepted_events, 0);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_token_error_releases_attempt_for_immediate_retry() {
        let path = temp_file("token-error-releases-attempt");
        let client = WebexClient::builder()
            .unwrap()
            .base_url(url::Url::parse("http://127.0.0.1:9/v1/").unwrap())
            .token_provider(Arc::new(MissingTokenProvider))
            .build()
            .unwrap();
        let config = Config {
            mock: false,
            ..mock_config(path.clone())
        };
        let bot = AccountBot {
            self_person_id: config.self_person_id.clone(),
            processed: Arc::new(Mutex::new(
                ProcessedMessageStore::load(path.clone(), config.max_processed_ids).unwrap(),
            )),
            config,
            client: Some(client),
            state_persist_failed: Arc::new(AtomicBool::new(false)),
            event_slots: Arc::new(AtomicUsize::new(0)),
            completed_event_slots: Arc::new(AtomicUsize::new(0)),
            max_events_notify: Arc::new(Notify::new()),
        };

        let error = bot
            .handle_event(SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("no usable access token"));
        assert!(!bot.processed.lock().unwrap().contains("message-1"));
        assert_attempt_can_restart(&bot, "message-1");

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_state_write_failures_return_retry_after_and_degrade_health() {
        let release_path = temp_file("release-state-write-failure");
        let release_bad_parent = temp_file("release-state-write-bad-parent");
        let mut release_config = mock_config(release_path.clone());
        release_config.attempt_lease = Duration::from_secs(6);
        let release_bot = AccountBot::new(release_config).await.unwrap();
        let release_inflight = match release_bot
            .begin_in_flight_attempt("message-release".to_owned())
            .unwrap()
        {
            BeginInFlight::Started(inflight) => inflight,
            _ => panic!("expected new in-flight attempt"),
        };
        fs::write(&release_bad_parent, "not a directory").unwrap();
        release_bot.processed.lock().unwrap().path = release_bad_parent.join("state");

        let release_error = release_bot
            .release_or_retry_after(release_inflight, "message-release", "room not allowed")
            .unwrap_err();

        match release_error {
            BotError::RetryAfter {
                message,
                retry_after,
            } => {
                assert_eq!(retry_after, Duration::from_secs(6));
                assert!(message.contains("state persist failed while releasing room not allowed"));
            }
            other => panic!("unexpected error: {other}"),
        }
        let release_health = release_bot.health_response().unwrap();
        assert_eq!(release_health.status, 503);
        assert!(
            release_health
                .body
                .contains("\"statePersistenceHealthy\":false")
        );

        let remember_path = temp_file("remember-state-write-failure");
        let remember_bad_parent = temp_file("remember-state-write-bad-parent");
        let mut remember_config = mock_config(remember_path.clone());
        remember_config.attempt_lease = Duration::from_secs(6);
        let remember_bot = AccountBot::new(remember_config).await.unwrap();
        let remember_inflight = match remember_bot
            .begin_in_flight_attempt("message-remember".to_owned())
            .unwrap()
        {
            BeginInFlight::Started(inflight) => inflight,
            _ => panic!("expected new in-flight attempt"),
        };
        fs::write(&remember_bad_parent, "not a directory").unwrap();
        remember_bot.processed.lock().unwrap().path = remember_bad_parent.join("state");

        let remember_error = remember_bot
            .remember_or_retry_after(remember_inflight, "message-remember", "mock reply")
            .unwrap_err();

        match remember_error {
            BotError::RetryAfter {
                message,
                retry_after,
            } => {
                assert_eq!(retry_after, Duration::from_secs(6));
                assert!(message.contains("state persist failed while recording mock reply"));
            }
            other => panic!("unexpected error: {other}"),
        }
        let remember_health = remember_bot.health_response().unwrap();
        assert_eq!(remember_health.status, 503);
        assert!(
            remember_health
                .body
                .contains("\"statePersistenceHealthy\":false")
        );

        let _ = fs::remove_file(release_path);
        let _ = fs::remove_file(release_bad_parent);
        let _ = fs::remove_file(remember_path);
        let _ = fs::remove_file(remember_bad_parent);
    }

    #[tokio::test]
    async fn account_bot_reply_preserves_existing_thread_parent() {
        let path = temp_file("reply-existing-thread-parent");
        let (bot, request) =
            account_bot_with_webex_response(path.clone(), 500, r#"{"message":"boom"}"#).await;

        let error = bot
            .handle_event(SidecarEvent::message_created(json!({
                "id": "reply-message-1",
                "parentId": "root-message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("500 Internal Server Error"));
        let request = captured_request(request).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));
        assert!(request.contains(r#""parentId":"root-message-1""#));
        assert!(!request.contains(r#""parentId":"reply-message-1""#));

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_bot_create_message_5xx_defers_attempt_for_retry() {
        let path = temp_file("create-message-failure");
        let (bot, request) =
            account_bot_with_webex_response(path.clone(), 500, r#"{"message":"boom"}"#).await;

        let error = bot
            .handle_event(SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("500 Internal Server Error"));
        let request = captured_request(request).await;
        assert!(request.starts_with("POST /v1/messages HTTP/1.1"));
        assert!(request.contains(r#""parentId":"message-1""#));
        assert!(!bot.processed.lock().unwrap().contains("message-1"));
        assert_bot_attempt_leased(&bot, "message-1", 1, 60);

        let retry_error = bot
            .handle_event(SidecarEvent::message_created(json!({
                "id": "message-1",
                "roomId": "room-1",
                "personId": "person-1",
                "text": "hello"
            })))
            .await
            .unwrap_err();
        assert!(
            retry_error
                .to_string()
                .contains("reply attempt is pending retry lease")
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn sync_parent_dir_accepts_bare_state_file_names() {
        sync_parent_dir(Path::new("processed-message-ids.txt")).unwrap();
    }

    #[test]
    fn processed_store_verify_persistable_creates_state_file() {
        let path = temp_file("persistable");
        let store = ProcessedMessageStore::load(path.clone(), 2).unwrap();

        store.verify_persistable().unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reply_text_collapses_and_truncates_message_body() {
        let message = BotMessage {
            text: Some("hello\nfrom\twebex".to_owned()),
            ..BotMessage::default()
        };

        assert_eq!(reply_text("ack", &message), "ack: hello from webex");

        let message = BotMessage {
            text: Some("a".repeat(260)),
            ..BotMessage::default()
        };
        assert!(reply_text("", &message).ends_with("..."));
    }

    #[test]
    fn token_loader_accepts_raw_and_token_set_files() {
        let raw_path = temp_file("raw-token");
        fs::write(&raw_path, "raw-token\n").unwrap();
        assert_eq!(load_access_token_file(&raw_path).unwrap(), "raw-token");
        let _ = fs::remove_file(raw_path);

        let json_path = temp_file("json-token");
        fs::write(
            &json_path,
            serde_json::to_string(&TokenSet {
                access_token: " json-token\n".to_owned(),
                refresh_token: None,
                token_type: "Bearer".to_owned(),
                scopes: Vec::new(),
                expires_at: None,
                refresh_token_expires_at: None,
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(load_access_token_file(&json_path).unwrap(), "json-token");
        let _ = fs::remove_file(json_path);

        let empty_json_path = temp_file("json-empty-token");
        fs::write(
            &empty_json_path,
            serde_json::to_string(&TokenSet {
                access_token: "  ".to_owned(),
                refresh_token: None,
                token_type: "Bearer".to_owned(),
                scopes: Vec::new(),
                expires_at: None,
                refresh_token_expires_at: None,
            })
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            load_access_token_file(&empty_json_path),
            Err(Error::MissingToken)
        ));
        let _ = fs::remove_file(empty_json_path);
    }

    #[test]
    fn message_event_data_deserializes_camel_case_fields() {
        let message = BotMessage::from_value(json!({
            "id": "message-1",
            "roomId": "room-1",
            "personId": "person-1",
            "text": "hello"
        }))
        .unwrap();

        assert_eq!(message.id.as_deref(), Some("message-1"));
        assert_eq!(message.room_id.as_deref(), Some("room-1"));
        assert_eq!(message.person_id.as_deref(), Some("person-1"));
    }

    #[test]
    fn account_bot_paths_must_be_distinct_absolute_paths() {
        validate_account_bot_paths("/webex/events", "/healthz").unwrap();

        assert!(validate_account_bot_paths("/webex/events", "/webex/events").is_err());
        assert!(validate_account_bot_paths("webex/events", "/healthz").is_err());
        assert!(validate_account_bot_paths("/webex/events", "healthz").is_err());
    }
}
