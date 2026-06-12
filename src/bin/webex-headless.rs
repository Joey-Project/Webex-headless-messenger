use std::{
    collections::BTreeMap,
    env,
    error::Error as StdError,
    fmt,
    io::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::{sleep, timeout},
};
use url::Url;
use webex_headless_messenger::{
    DEFAULT_MESSAGING_SCOPES, DeviceTokenStatus, MessagePoller, OAuthClient, OAuthConfig,
    PollingConfig, RefreshingTokenProvider, SidecarEvent, TokenSet, TokenStore, WebexClient,
    error::{Error as WebexError, Result as WebexResult},
    types::{
        CreateMessage, ListDirectMessages, ListMessages, ListRooms, LocalFileAttachment, Room,
    },
};

const DEFAULT_SIDECAR_BIND: &str = "127.0.0.1:8787";
const DEFAULT_SIDECAR_PATH: &str = "/webex/events";
const DEFAULT_SIDECAR_HEALTH_PATH: &str = "/healthz";
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_SIDECAR_CONNECTIONS: usize = 32;

type CliResult<T> = Result<T, Box<dyn StdError + Send + Sync>>;

fn main() -> CliResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async_main())
}

async fn async_main() -> CliResult<()> {
    let cli = Cli::parse(env::args().skip(1))?;
    if matches!(cli.command, Command::Help) {
        print_usage();
        return Ok(());
    }
    execute(cli).await
}

async fn execute(cli: Cli) -> CliResult<()> {
    match cli.command {
        Command::Help => unreachable!("help is handled before command execution"),
        Command::AuthDevice(args) => auth_device(&cli.global, args).await,
        Command::AuthRefresh(args) => auth_refresh(&cli.global, args).await,
        Command::Me => {
            let client = build_client(&cli.global).await?;
            print_json(&client.me().await?)
        }
        Command::RoomsList(args) => {
            let client = build_client(&cli.global).await?;
            let first = client
                .list_rooms(&ListRooms {
                    max: args.max,
                    room_type: args.room_type,
                    team_id: args.team_id,
                    ..ListRooms::default()
                })
                .await?;
            if args.all {
                print_json(&client.collect_all::<Room>(first).await?)
            } else {
                print_json(&first.items)
            }
        }
        Command::RoomsGet { room_id } => {
            let client = build_client(&cli.global).await?;
            print_json(&client.get_room(&room_id).await?)
        }
        Command::RoomsResolve(args) => {
            let client = build_client(&cli.global).await?;
            print_json(&resolve_room(&client, args).await?)
        }
        Command::MessagesList(args) => {
            let client = build_client(&cli.global).await?;
            let first = client
                .list_messages(&ListMessages {
                    parent_id: args.parent_id,
                    max: args.max,
                    ..ListMessages::room(args.room_id)
                })
                .await?;
            if args.all {
                print_json(&client.collect_all(first).await?)
            } else {
                print_json(&first.items)
            }
        }
        Command::MessagesDirect(args) => {
            let client = build_client(&cli.global).await?;
            let first = client
                .list_direct_messages(&ListDirectMessages {
                    parent_id: args.parent_id,
                    person_id: args.person_id,
                    person_email: args.person_email,
                })
                .await?;
            if args.all {
                print_json(&client.collect_all(first).await?)
            } else {
                print_json(&first.items)
            }
        }
        Command::MessagesGet { message_id } => {
            let client = build_client(&cli.global).await?;
            print_json(&client.get_message(&message_id).await?)
        }
        Command::MessagesSend(args) => {
            let client = build_client(&cli.global).await?;
            let request = create_message_from_send(args.target, args.body);
            let message = if let Some(file) = args.file {
                client
                    .create_message_with_file(&request, &file.into_attachment())
                    .await?
            } else {
                client.create_message(&request).await?
            };
            print_json(&message)
        }
        Command::MessagesReply(args) => {
            let client = build_client(&cli.global).await?;
            let mut request = create_message_from_body(args.body);
            request.room_id = Some(args.room_id);
            request.parent_id = Some(args.parent_id);
            print_json(&client.create_message(&request).await?)
        }
        Command::MessagesDelete { message_id } => {
            let client = build_client(&cli.global).await?;
            client.delete_message(&message_id).await?;
            print_json(&json!({ "deleted": true, "messageId": message_id }))
        }
        Command::PollMessages(args) => {
            let client = build_client(&cli.global).await?;
            poll_messages(client, args).await
        }
        Command::SidecarReceive(args) => sidecar_receive(args).await,
    }
}

#[derive(Debug)]
struct Cli {
    global: GlobalOptions,
    command: Command,
}

#[derive(Debug, Default)]
struct GlobalOptions {
    access_token: Option<String>,
    token_file: Option<PathBuf>,
    client_id: Option<String>,
    client_secret: Option<String>,
}

#[derive(Debug)]
enum Command {
    Help,
    AuthDevice(AuthDeviceArgs),
    AuthRefresh(AuthRefreshArgs),
    Me,
    RoomsList(RoomsListArgs),
    RoomsGet { room_id: String },
    RoomsResolve(RoomsResolveArgs),
    MessagesList(MessagesListArgs),
    MessagesDirect(MessagesDirectArgs),
    MessagesGet { message_id: String },
    MessagesSend(MessagesSendArgs),
    MessagesReply(MessagesReplyArgs),
    MessagesDelete { message_id: String },
    PollMessages(PollMessagesArgs),
    SidecarReceive(SidecarReceiveArgs),
}

#[derive(Debug, Default)]
struct AuthDeviceArgs {
    client_id: Option<String>,
    client_secret: Option<String>,
    token_file: Option<PathBuf>,
    scopes: Vec<String>,
    stdout_token: bool,
}

#[derive(Debug, Default)]
struct AuthRefreshArgs {
    client_id: Option<String>,
    client_secret: Option<String>,
    token_file: Option<PathBuf>,
}

#[derive(Debug, Default)]
struct RoomsListArgs {
    max: Option<u16>,
    room_type: Option<String>,
    team_id: Option<String>,
    all: bool,
}

#[derive(Debug, Default)]
struct RoomsResolveArgs {
    room_id: Option<String>,
    link: Option<String>,
    title: Option<String>,
}

#[derive(Debug)]
struct MessagesListArgs {
    room_id: String,
    parent_id: Option<String>,
    max: Option<u16>,
    all: bool,
}

#[derive(Debug, Default)]
struct MessagesDirectArgs {
    person_id: Option<String>,
    person_email: Option<String>,
    parent_id: Option<String>,
    all: bool,
}

#[derive(Debug)]
struct MessagesSendArgs {
    target: MessageTarget,
    body: MessageBody,
    file: Option<FileArgs>,
}

#[derive(Debug)]
struct MessagesReplyArgs {
    room_id: String,
    parent_id: String,
    body: MessageBody,
}

#[derive(Debug)]
struct PollMessagesArgs {
    room_id: String,
    interval_seconds: u64,
    page_size: u16,
    emit_existing: bool,
}

#[derive(Debug)]
struct SidecarReceiveArgs {
    bind: String,
    path: String,
    health_path: String,
    token: Option<String>,
    max_events: usize,
    allow_unauthenticated: bool,
    allow_non_loopback: bool,
}

#[derive(Debug)]
enum MessageTarget {
    Room(String),
    PersonEmail(String),
}

#[derive(Debug)]
enum MessageBody {
    Text(String),
    Markdown(String),
}

#[derive(Debug)]
struct FileArgs {
    path: PathBuf,
    media_type: Option<String>,
    file_name: Option<String>,
}

impl FileArgs {
    fn into_attachment(self) -> LocalFileAttachment {
        let mut attachment = LocalFileAttachment::new(self.path);
        if let Some(media_type) = self.media_type {
            attachment = attachment.with_media_type(media_type);
        }
        if let Some(file_name) = self.file_name {
            attachment = attachment.with_file_name(file_name);
        }
        attachment
    }
}

impl Cli {
    fn parse<I>(args: I) -> CliResult<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut cursor = ArgCursor::new(args);
        let mut global = GlobalOptions::default();

        while let Some(arg) = cursor.peek().map(str::to_owned) {
            if !arg.starts_with('-') {
                break;
            }
            if cursor.take_flag("--help") || cursor.take_flag("-h") {
                return Ok(Self {
                    global,
                    command: Command::Help,
                });
            }
            if let Some(value) = cursor.take_option("--access-token")? {
                global.access_token = Some(value);
            } else if let Some(value) = cursor.take_option("--token-file")? {
                global.token_file = Some(PathBuf::from(value));
            } else if let Some(value) = cursor.take_option("--client-id")? {
                global.client_id = Some(value);
            } else if let Some(value) = cursor.take_option("--client-secret")? {
                global.client_secret = Some(value);
            } else {
                return Err(CliError(format!("unknown global option {arg:?}")).into());
            }
        }

        let command = match cursor.next() {
            None => Command::Help,
            Some(command) if command == "auth" => parse_auth(&mut cursor)?,
            Some(command) if command == "me" => {
                cursor.finish()?;
                Command::Me
            }
            Some(command) if command == "rooms" => parse_rooms(&mut cursor)?,
            Some(command) if command == "messages" => parse_messages(&mut cursor)?,
            Some(command) if command == "poll" => parse_poll(&mut cursor)?,
            Some(command) if command == "sidecar" => parse_sidecar(&mut cursor)?,
            Some(command) if command == "--help" || command == "-h" => Command::Help,
            Some(command) => {
                return Err(CliError(format!("unknown command {command:?}")).into());
            }
        };
        Ok(Self { global, command })
    }
}

fn parse_auth(cursor: &mut ArgCursor) -> CliResult<Command> {
    let subcommand = cursor.required("auth subcommand")?;
    match subcommand.as_str() {
        "device" => parse_auth_device(cursor),
        "refresh" => parse_auth_refresh(cursor),
        _ => Err(CliError(format!("unknown auth subcommand {subcommand:?}")).into()),
    }
}

fn parse_auth_device(cursor: &mut ArgCursor) -> CliResult<Command> {
    let mut args = AuthDeviceArgs::default();
    while let Some(arg) = cursor.peek().map(str::to_owned) {
        if cursor.take_flag("--stdout-token") {
            args.stdout_token = true;
        } else if let Some(value) = cursor.take_option("--client-id")? {
            args.client_id = Some(value);
        } else if let Some(value) = cursor.take_option("--client-secret")? {
            args.client_secret = Some(value);
        } else if let Some(value) = cursor.take_option("--token-file")? {
            args.token_file = Some(PathBuf::from(value));
        } else if let Some(value) = cursor.take_option("--scopes")? {
            args.scopes.extend(parse_scopes(&value));
        } else if let Some(value) = cursor.take_option("--scope")? {
            args.scopes.extend(parse_scopes(&value));
        } else {
            return Err(CliError(format!("unknown auth device option {arg:?}")).into());
        }
    }
    Ok(Command::AuthDevice(args))
}

fn parse_auth_refresh(cursor: &mut ArgCursor) -> CliResult<Command> {
    let mut args = AuthRefreshArgs::default();
    while let Some(arg) = cursor.peek().map(str::to_owned) {
        if let Some(value) = cursor.take_option("--client-id")? {
            args.client_id = Some(value);
        } else if let Some(value) = cursor.take_option("--client-secret")? {
            args.client_secret = Some(value);
        } else if let Some(value) = cursor.take_option("--token-file")? {
            args.token_file = Some(PathBuf::from(value));
        } else {
            return Err(CliError(format!("unknown auth refresh option {arg:?}")).into());
        }
    }
    Ok(Command::AuthRefresh(args))
}

fn parse_rooms(cursor: &mut ArgCursor) -> CliResult<Command> {
    let subcommand = cursor.required("rooms subcommand")?;
    match subcommand.as_str() {
        "list" => {
            let mut args = RoomsListArgs::default();
            while let Some(arg) = cursor.peek().map(str::to_owned) {
                if cursor.take_flag("--all") {
                    args.all = true;
                } else if let Some(value) = cursor.take_option("--max")? {
                    args.max = Some(parse_u16("--max", &value)?);
                } else if let Some(value) = cursor.take_option("--type")? {
                    args.room_type = Some(value);
                } else if let Some(value) = cursor.take_option("--team-id")? {
                    args.team_id = Some(value);
                } else {
                    return Err(CliError(format!("unknown rooms list option {arg:?}")).into());
                }
            }
            Ok(Command::RoomsList(args))
        }
        "get" => {
            let mut room_id = None;
            while let Some(arg) = cursor.peek().map(str::to_owned) {
                if let Some(value) = cursor.take_option("--room-id")? {
                    room_id = Some(value);
                } else {
                    return Err(CliError(format!("unknown rooms get option {arg:?}")).into());
                }
            }
            Ok(Command::RoomsGet {
                room_id: required_value(room_id, "--room-id")?,
            })
        }
        "resolve" => {
            let mut args = RoomsResolveArgs::default();
            while let Some(arg) = cursor.peek().map(str::to_owned) {
                if let Some(value) = cursor.take_option("--room-id")? {
                    args.room_id = Some(value);
                } else if let Some(value) = cursor.take_option("--link")? {
                    args.link = Some(value);
                } else if let Some(value) = cursor.take_option("--title")? {
                    args.title = Some(value);
                } else {
                    return Err(CliError(format!("unknown rooms resolve option {arg:?}")).into());
                }
            }
            Ok(Command::RoomsResolve(args))
        }
        _ => Err(CliError(format!("unknown rooms subcommand {subcommand:?}")).into()),
    }
}

fn parse_messages(cursor: &mut ArgCursor) -> CliResult<Command> {
    let subcommand = cursor.required("messages subcommand")?;
    match subcommand.as_str() {
        "list" => {
            let mut room_id = None;
            let mut parent_id = None;
            let mut max = None;
            let mut all = false;
            while let Some(arg) = cursor.peek().map(str::to_owned) {
                if cursor.take_flag("--all") {
                    all = true;
                } else if let Some(value) = cursor.take_option("--room-id")? {
                    room_id = Some(value);
                } else if let Some(value) = cursor.take_option("--parent-id")? {
                    parent_id = Some(value);
                } else if let Some(value) = cursor.take_option("--max")? {
                    max = Some(parse_u16("--max", &value)?);
                } else {
                    return Err(CliError(format!("unknown messages list option {arg:?}")).into());
                }
            }
            Ok(Command::MessagesList(MessagesListArgs {
                room_id: required_value(room_id, "--room-id")?,
                parent_id,
                max,
                all,
            }))
        }
        "direct" => {
            let mut args = MessagesDirectArgs::default();
            while let Some(arg) = cursor.peek().map(str::to_owned) {
                if cursor.take_flag("--all") {
                    args.all = true;
                } else if let Some(value) = cursor.take_option("--person-id")? {
                    args.person_id = Some(value);
                } else if let Some(value) = cursor.take_option("--person-email")? {
                    args.person_email = Some(value);
                } else if let Some(value) = cursor.take_option("--parent-id")? {
                    args.parent_id = Some(value);
                } else {
                    return Err(CliError(format!("unknown messages direct option {arg:?}")).into());
                }
            }
            match (args.person_id.is_some(), args.person_email.is_some()) {
                (false, false) => {
                    return Err(CliError(
                        "messages direct requires --person-id or --person-email".into(),
                    )
                    .into());
                }
                (true, true) => {
                    return Err(CliError("messages direct accepts only one target".into()).into());
                }
                _ => {}
            }
            Ok(Command::MessagesDirect(args))
        }
        "get" => {
            let mut message_id = None;
            while let Some(arg) = cursor.peek().map(str::to_owned) {
                if let Some(value) = cursor.take_option("--message-id")? {
                    message_id = Some(value);
                } else {
                    return Err(CliError(format!("unknown messages get option {arg:?}")).into());
                }
            }
            Ok(Command::MessagesGet {
                message_id: required_value(message_id, "--message-id")?,
            })
        }
        "send" => parse_messages_send(cursor),
        "reply" => parse_messages_reply(cursor),
        "delete" => {
            let mut message_id = None;
            while let Some(arg) = cursor.peek().map(str::to_owned) {
                if let Some(value) = cursor.take_option("--message-id")? {
                    message_id = Some(value);
                } else {
                    return Err(CliError(format!("unknown messages delete option {arg:?}")).into());
                }
            }
            Ok(Command::MessagesDelete {
                message_id: required_value(message_id, "--message-id")?,
            })
        }
        _ => Err(CliError(format!("unknown messages subcommand {subcommand:?}")).into()),
    }
}

fn parse_messages_send(cursor: &mut ArgCursor) -> CliResult<Command> {
    let mut room_id = None;
    let mut to_person_email = None;
    let mut text = None;
    let mut markdown = None;
    let mut file_path = None;
    let mut media_type = None;
    let mut file_name = None;
    while let Some(arg) = cursor.peek().map(str::to_owned) {
        if let Some(value) = cursor.take_option("--room-id")? {
            room_id = Some(value);
        } else if let Some(value) = cursor.take_option("--to-person-email")? {
            to_person_email = Some(value);
        } else if let Some(value) = cursor.take_option("--text")? {
            text = Some(value);
        } else if let Some(value) = cursor.take_option("--markdown")? {
            markdown = Some(value);
        } else if let Some(value) = cursor.take_option("--file")? {
            file_path = Some(PathBuf::from(value));
        } else if let Some(value) = cursor.take_option("--media-type")? {
            media_type = Some(value);
        } else if let Some(value) = cursor.take_option("--file-name")? {
            file_name = Some(value);
        } else {
            return Err(CliError(format!("unknown messages send option {arg:?}")).into());
        }
    }

    let target = match (room_id, to_person_email) {
        (Some(room_id), None) => MessageTarget::Room(room_id),
        (None, Some(email)) => MessageTarget::PersonEmail(email),
        (Some(_), Some(_)) => {
            return Err(CliError("messages send accepts only one target".into()).into());
        }
        (None, None) => {
            return Err(
                CliError("messages send requires --room-id or --to-person-email".into()).into(),
            );
        }
    };
    let body = message_body(text, markdown)?;
    let file = file_path.map(|path| FileArgs {
        path,
        media_type,
        file_name,
    });
    Ok(Command::MessagesSend(MessagesSendArgs {
        target,
        body,
        file,
    }))
}

fn parse_messages_reply(cursor: &mut ArgCursor) -> CliResult<Command> {
    let mut room_id = None;
    let mut parent_id = None;
    let mut text = None;
    let mut markdown = None;
    while let Some(arg) = cursor.peek().map(str::to_owned) {
        if let Some(value) = cursor.take_option("--room-id")? {
            room_id = Some(value);
        } else if let Some(value) = cursor.take_option("--parent-id")? {
            parent_id = Some(value);
        } else if let Some(value) = cursor.take_option("--text")? {
            text = Some(value);
        } else if let Some(value) = cursor.take_option("--markdown")? {
            markdown = Some(value);
        } else {
            return Err(CliError(format!("unknown messages reply option {arg:?}")).into());
        }
    }
    Ok(Command::MessagesReply(MessagesReplyArgs {
        room_id: required_value(room_id, "--room-id")?,
        parent_id: required_value(parent_id, "--parent-id")?,
        body: message_body(text, markdown)?,
    }))
}

fn parse_poll(cursor: &mut ArgCursor) -> CliResult<Command> {
    let subcommand = cursor.required("poll subcommand")?;
    if subcommand != "messages" {
        return Err(CliError(format!("unknown poll subcommand {subcommand:?}")).into());
    }
    let mut room_id = None;
    let mut interval_seconds = 15;
    let mut page_size = 50;
    let mut emit_existing = false;
    while let Some(arg) = cursor.peek().map(str::to_owned) {
        if cursor.take_flag("--emit-existing") {
            emit_existing = true;
        } else if let Some(value) = cursor.take_option("--room-id")? {
            room_id = Some(value);
        } else if let Some(value) = cursor.take_option("--interval-seconds")? {
            interval_seconds = parse_u64("--interval-seconds", &value)?;
        } else if let Some(value) = cursor.take_option("--max")? {
            page_size = parse_u16("--max", &value)?;
        } else {
            return Err(CliError(format!("unknown poll messages option {arg:?}")).into());
        }
    }
    Ok(Command::PollMessages(PollMessagesArgs {
        room_id: required_value(room_id, "--room-id")?,
        interval_seconds,
        page_size,
        emit_existing,
    }))
}

fn parse_sidecar(cursor: &mut ArgCursor) -> CliResult<Command> {
    let subcommand = cursor.required("sidecar subcommand")?;
    if subcommand != "receive" {
        return Err(CliError(format!("unknown sidecar subcommand {subcommand:?}")).into());
    }

    let mut args = SidecarReceiveArgs {
        bind: DEFAULT_SIDECAR_BIND.to_owned(),
        path: DEFAULT_SIDECAR_PATH.to_owned(),
        health_path: DEFAULT_SIDECAR_HEALTH_PATH.to_owned(),
        token: None,
        max_events: 0,
        allow_unauthenticated: false,
        allow_non_loopback: false,
    };
    while let Some(arg) = cursor.peek().map(str::to_owned) {
        if cursor.take_flag("--allow-unauthenticated") {
            args.allow_unauthenticated = true;
        } else if cursor.take_flag("--allow-non-loopback") {
            args.allow_non_loopback = true;
        } else if let Some(value) = cursor.take_option("--bind")? {
            args.bind = value;
        } else if let Some(value) = cursor.take_option("--path")? {
            args.path = value;
        } else if let Some(value) = cursor.take_option("--health-path")? {
            args.health_path = value;
        } else if let Some(value) = cursor.take_option("--token")? {
            args.token = Some(value);
        } else if let Some(value) = cursor.take_option("--max-events")? {
            args.max_events = parse_usize("--max-events", &value)?;
        } else {
            return Err(CliError(format!("unknown sidecar receive option {arg:?}")).into());
        }
    }
    if args.path == args.health_path {
        return Err(
            CliError("sidecar receive requires --path and --health-path to differ".into()).into(),
        );
    }
    Ok(Command::SidecarReceive(args))
}

#[derive(Debug)]
struct ArgCursor {
    args: Vec<String>,
    index: usize,
}

impl ArgCursor {
    fn new<I>(args: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        Self {
            args: args.into_iter().collect(),
            index: 0,
        }
    }

    fn peek(&self) -> Option<&str> {
        self.args.get(self.index).map(String::as_str)
    }

    fn next(&mut self) -> Option<String> {
        let value = self.args.get(self.index).cloned()?;
        self.index += 1;
        Some(value)
    }

    fn required(&mut self, name: &str) -> CliResult<String> {
        self.next()
            .ok_or_else(|| CliError(format!("missing {name}")).into())
    }

    fn take_flag(&mut self, name: &str) -> bool {
        if self.peek() == Some(name) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn take_option(&mut self, name: &str) -> CliResult<Option<String>> {
        let Some(arg) = self.peek() else {
            return Ok(None);
        };
        if arg == name {
            self.index += 1;
            return Ok(Some(self.required(name)?));
        }
        if let Some(value) = arg.strip_prefix(&format!("{name}=")) {
            let value = value.to_owned();
            self.index += 1;
            return Ok(Some(value));
        }
        Ok(None)
    }

    fn finish(&self) -> CliResult<()> {
        if let Some(arg) = self.peek() {
            Err(CliError(format!("unexpected argument {arg:?}")).into())
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct CliError(String);

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for CliError {}

fn required_value(value: Option<String>, name: &str) -> CliResult<String> {
    value.ok_or_else(|| CliError(format!("{name} is required")).into())
}

fn parse_u16(name: &str, value: &str) -> CliResult<u16> {
    value
        .parse()
        .map_err(|error| CliError(format!("{name} must be a u16: {error}")).into())
}

fn parse_u64(name: &str, value: &str) -> CliResult<u64> {
    value
        .parse()
        .map_err(|error| CliError(format!("{name} must be a u64: {error}")).into())
}

fn parse_usize(name: &str, value: &str) -> CliResult<usize> {
    value
        .parse()
        .map_err(|error| CliError(format!("{name} must be a usize: {error}")).into())
}

fn message_body(text: Option<String>, markdown: Option<String>) -> CliResult<MessageBody> {
    match (text, markdown) {
        (Some(text), None) => Ok(MessageBody::Text(text)),
        (None, Some(markdown)) => Ok(MessageBody::Markdown(markdown)),
        (Some(_), Some(_)) => Err(CliError("use --text or --markdown, not both".into()).into()),
        (None, None) => Err(CliError("--text or --markdown is required".into()).into()),
    }
}

fn create_message_from_send(target: MessageTarget, body: MessageBody) -> CreateMessage {
    let mut request = create_message_from_body(body);
    match target {
        MessageTarget::Room(room_id) => request.room_id = Some(room_id),
        MessageTarget::PersonEmail(email) => request.to_person_email = Some(email),
    }
    request
}

fn create_message_from_body(body: MessageBody) -> CreateMessage {
    match body {
        MessageBody::Text(text) => CreateMessage {
            text: Some(text),
            ..CreateMessage::default()
        },
        MessageBody::Markdown(markdown) => CreateMessage {
            markdown: Some(markdown),
            ..CreateMessage::default()
        },
    }
}

fn parse_scopes(value: &str) -> Vec<String> {
    value
        .split(|ch: char| ch.is_ascii_whitespace() || ch == ',')
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn env_or_value(value: Option<String>, name: &str) -> Option<String> {
    value
        .or_else(|| env::var(name).ok())
        .filter(|value| !value.trim().is_empty())
}

#[derive(Debug, PartialEq, Eq)]
enum AuthSource {
    AccessToken(String),
    TokenFile(PathBuf),
}

fn resolve_auth_source(global: &GlobalOptions) -> CliResult<AuthSource> {
    auth_source_from_parts(
        global.access_token.clone(),
        global.token_file.clone(),
        env::var("WEBEX_TOKEN_FILE").ok().map(PathBuf::from),
        env::var("WEBEX_ACCESS_TOKEN").ok(),
    )
}

fn auth_source_from_parts(
    explicit_access_token: Option<String>,
    explicit_token_file: Option<PathBuf>,
    env_token_file: Option<PathBuf>,
    env_access_token: Option<String>,
) -> CliResult<AuthSource> {
    let explicit_access_token = explicit_access_token.filter(|value| !value.trim().is_empty());
    let explicit_token_file = explicit_token_file.filter(|path| !path.as_os_str().is_empty());
    if explicit_access_token.is_some() && explicit_token_file.is_some() {
        return Err(CliError("use --access-token or --token-file, not both".into()).into());
    }
    if let Some(token) = explicit_access_token {
        return Ok(AuthSource::AccessToken(token));
    }
    if let Some(path) = explicit_token_file {
        return Ok(AuthSource::TokenFile(path));
    }
    if let Some(path) = env_token_file.filter(|path| !path.as_os_str().is_empty()) {
        return Ok(AuthSource::TokenFile(path));
    }
    if let Some(token) = env_access_token.filter(|value| !value.trim().is_empty()) {
        return Ok(AuthSource::AccessToken(token));
    }
    Err(CliError(
        "set --access-token, --token-file, WEBEX_TOKEN_FILE, or WEBEX_ACCESS_TOKEN".into(),
    )
    .into())
}

async fn build_client(global: &GlobalOptions) -> CliResult<WebexClient> {
    match resolve_auth_source(global)? {
        AuthSource::AccessToken(token) => Ok(WebexClient::from_access_token(token)?),
        AuthSource::TokenFile(token_file) => build_token_file_client(global, token_file).await,
    }
}

async fn build_token_file_client(
    global: &GlobalOptions,
    token_file: PathBuf,
) -> CliResult<WebexClient> {
    let token_set = read_token_file(&token_file).await?;
    let client_id = env_or_value(global.client_id.clone(), "WEBEX_CLIENT_ID");
    let client_secret = env_or_value(global.client_secret.clone(), "WEBEX_CLIENT_SECRET");

    if let (Some(client_id), Some(client_secret), Some(_)) =
        (client_id, client_secret, token_set.refresh_token.as_ref())
    {
        let config = OAuthConfig::new(client_id)?.with_client_secret(client_secret);
        let oauth = OAuthClient::new(config);
        let store = Arc::new(FileTokenStore::new(token_file));
        let provider = RefreshingTokenProvider::new(oauth, store);
        return Ok(WebexClient::builder()?
            .token_provider(Arc::new(provider))
            .build()?);
    }

    Ok(WebexClient::from_access_token(token_set.access_token)?)
}

async fn auth_refresh(global: &GlobalOptions, args: AuthRefreshArgs) -> CliResult<()> {
    let token_file = args
        .token_file
        .or_else(|| global.token_file.clone())
        .or_else(|| env::var("WEBEX_TOKEN_FILE").ok().map(PathBuf::from))
        .ok_or_else(|| CliError("auth refresh requires --token-file or WEBEX_TOKEN_FILE".into()))?;
    let current = read_token_file(&token_file).await?;
    let refresh_token = current
        .refresh_token
        .clone()
        .ok_or_else(|| CliError("token file does not contain a refresh token".into()))?;
    let client_id = env_or_value(
        args.client_id.or_else(|| global.client_id.clone()),
        "WEBEX_CLIENT_ID",
    )
    .ok_or_else(|| CliError("--client-id or WEBEX_CLIENT_ID is required".into()))?;
    let client_secret = env_or_value(
        args.client_secret.or_else(|| global.client_secret.clone()),
        "WEBEX_CLIENT_SECRET",
    )
    .ok_or_else(|| CliError("--client-secret or WEBEX_CLIENT_SECRET is required".into()))?;

    let oauth = OAuthClient::new(OAuthConfig::new(client_id)?.with_client_secret(client_secret));
    let mut refreshed = oauth.refresh_token(&refresh_token).await?;
    if refreshed.refresh_token.is_none() {
        refreshed.refresh_token = Some(refresh_token);
        refreshed.refresh_token_expires_at = current.refresh_token_expires_at;
    }
    save_token_file(&token_file, &refreshed).await?;

    print_json(&json!({
        "refreshed": true,
        "tokenFile": token_file.display().to_string(),
        "expiresAt": refreshed.expires_at,
        "refreshTokenExpiresAt": refreshed.refresh_token_expires_at,
        "scopes": refreshed.scopes,
    }))
}

async fn auth_device(global: &GlobalOptions, args: AuthDeviceArgs) -> CliResult<()> {
    let client_id = env_or_value(
        args.client_id.or_else(|| global.client_id.clone()),
        "WEBEX_CLIENT_ID",
    )
    .ok_or_else(|| CliError("--client-id or WEBEX_CLIENT_ID is required".into()))?;
    let client_secret = env_or_value(
        args.client_secret.or_else(|| global.client_secret.clone()),
        "WEBEX_CLIENT_SECRET",
    )
    .ok_or_else(|| CliError("--client-secret or WEBEX_CLIENT_SECRET is required".into()))?;
    let token_file = args
        .token_file
        .or_else(|| global.token_file.clone())
        .or_else(|| env::var("WEBEX_TOKEN_FILE").ok().map(PathBuf::from));
    if token_file.is_none() && !args.stdout_token {
        return Err(CliError(
            "auth device requires --token-file/WEBEX_TOKEN_FILE or --stdout-token".into(),
        )
        .into());
    }

    let scopes = if args.scopes.is_empty() {
        DEFAULT_MESSAGING_SCOPES
            .iter()
            .map(|scope| (*scope).to_owned())
            .collect::<Vec<_>>()
    } else {
        args.scopes
    };
    let oauth = OAuthClient::new(
        OAuthConfig::new(client_id)?
            .with_client_secret(client_secret)
            .with_scopes(scopes.clone()),
    );

    let auth = oauth.start_device_authorization().await?;
    eprintln!("verification_uri={}", auth.verification_uri);
    eprintln!("user_code={}", auth.user_code);
    if let Some(complete) = &auth.verification_uri_complete {
        eprintln!("verification_uri_complete={complete}");
    }

    let deadline = Instant::now() + Duration::from_secs(auth.expires_in);
    let mut interval = Duration::from_secs(auth.interval.unwrap_or(5));
    let token = loop {
        if Instant::now() >= deadline {
            return Err(CliError("device authorization expired before approval".into()).into());
        }
        match oauth.poll_device_token(&auth.device_code).await? {
            DeviceTokenStatus::Authorized(token) => break token,
            DeviceTokenStatus::Pending { retry_after } => {
                sleep(retry_after.unwrap_or(Duration::ZERO).max(interval)).await;
            }
            DeviceTokenStatus::SlowDown { retry_after } => {
                interval += Duration::from_secs(5);
                sleep(retry_after.unwrap_or(Duration::ZERO).max(interval)).await;
            }
        }
    };

    if let Some(path) = token_file.as_deref() {
        save_token_file(path, &token).await?;
    }

    if args.stdout_token {
        print_json(&token)
    } else {
        print_json(&json!({
            "tokenFile": token_file.map(|path| path.display().to_string()),
            "scopes": token.scopes,
            "expiresAt": token.expires_at,
            "refreshTokenExpiresAt": token.refresh_token_expires_at,
        }))
    }
}

#[derive(Debug, Clone)]
struct FileTokenStore {
    path: PathBuf,
}

impl FileTokenStore {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl TokenStore for FileTokenStore {
    async fn load(&self) -> WebexResult<Option<TokenSet>> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn save(&self, token_set: &TokenSet) -> WebexResult<()> {
        save_token_file(&self.path, token_set).await
    }
}

async fn read_token_file(path: &Path) -> CliResult<TokenSet> {
    let bytes = tokio::fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(unix)]
async fn save_token_file(path: &Path, token_set: &TokenSet) -> WebexResult<()> {
    let bytes = serde_json::to_vec_pretty(token_set)?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent).await?;
    }

    let path = path.to_owned();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("token.json");
        let tmp_path = path.with_file_name(format!(".{file_name}.tmp.{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp_path);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, &path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        Ok(())
    })
    .await
    .map_err(|error| WebexError::Other(format!("token file write task failed: {error}")))??;

    Ok(())
}

#[cfg(not(unix))]
async fn save_token_file(path: &Path, _token_set: &TokenSet) -> WebexResult<()> {
    Err(WebexError::Other(format!(
        "persistent token files are only supported on Unix by this CLI because refresh tokens need owner-only file permissions; use --stdout-token and store tokens in platform secret storage instead: {}",
        path.display()
    )))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RoomResolution {
    room_id: String,
    source: String,
    room: Room,
}

async fn resolve_room(client: &WebexClient, args: RoomsResolveArgs) -> CliResult<RoomResolution> {
    if let Some(room_id) = args.room_id.filter(|value| !value.trim().is_empty()) {
        let room = client.get_room(&room_id).await?;
        return Ok(RoomResolution {
            room_id,
            source: "explicit".to_owned(),
            room,
        });
    }

    let link_supplied = args
        .link
        .as_deref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let link_candidates = args
        .link
        .as_deref()
        .map(room_id_candidates_from_link)
        .unwrap_or_default();
    let mut last_link_error = None;
    for candidate in link_candidates.iter().cloned() {
        match client.get_room(&candidate).await {
            Ok(room) => {
                return Ok(RoomResolution {
                    room_id: candidate,
                    source: "link".to_owned(),
                    room,
                });
            }
            Err(error) => last_link_error = Some(error.to_string()),
        }
    }

    let Some(title) = args.title.filter(|value| !value.trim().is_empty()) else {
        if link_supplied {
            if link_candidates.is_empty() {
                return Err(CliError("no room id candidates found in --link".into()).into());
            }
            let detail = last_link_error
                .map(|error| format!("; last error: {error}"))
                .unwrap_or_default();
            return Err(CliError(format!(
                "no room id candidates from --link resolved to an accessible room{detail}"
            ))
            .into());
        }
        return Err(CliError("rooms resolve requires --room-id, --link, or --title".into()).into());
    };
    let first = client
        .list_rooms(&ListRooms {
            max: Some(100),
            ..ListRooms::default()
        })
        .await?;
    let rooms = client.collect_all::<Room>(first).await?;
    let mut matches = rooms
        .into_iter()
        .filter(|room| room.title.as_deref() == Some(title.as_str()))
        .collect::<Vec<_>>();
    matches.sort_by(|a, b| a.id.cmp(&b.id));
    matches.dedup_by(|a, b| a.id == b.id);

    match matches.len() {
        1 => {
            let room = matches.remove(0);
            let room_id = room.id.clone().ok_or_else(|| {
                CliError(format!("room title {title:?} matched a room with no id"))
            })?;
            Ok(RoomResolution {
                room_id,
                source: "title".to_owned(),
                room,
            })
        }
        0 => Err(CliError(format!("no room matched title {title:?}")).into()),
        count => Err(CliError(format!(
            "title {title:?} matched {count} rooms; rerun with --room-id"
        ))
        .into()),
    }
}

fn room_id_candidates_from_link(link: &str) -> Vec<String> {
    let Ok(url) = Url::parse(link.trim()) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    for segment in url.path_segments().into_iter().flatten() {
        push_room_candidate(&mut candidates, segment);
    }
    for (key, value) in url.query_pairs() {
        if key_contains_room_hint(&key) {
            push_room_candidate(&mut candidates, &value);
        }
    }
    if let Some(fragment) = url.fragment() {
        for (key, value) in url::form_urlencoded::parse(fragment.as_bytes()) {
            if key_contains_room_hint(&key) {
                push_room_candidate(&mut candidates, &value);
            }
        }
    }
    candidates.sort();
    candidates.dedup();
    candidates
}

fn key_contains_room_hint(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key == "id" || key.contains("room") || key.contains("space") || key.contains("conversation")
}

fn push_room_candidate(candidates: &mut Vec<String>, value: &str) {
    let value = value.trim();
    if value.len() >= 16
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '/' | '='))
    {
        candidates.push(value.to_owned());
    }
}

async fn poll_messages(client: WebexClient, args: PollMessagesArgs) -> CliResult<()> {
    let config = PollingConfig {
        interval: Duration::from_secs(args.interval_seconds),
        page_size: args.page_size,
        emit_existing_on_first_poll: args.emit_existing,
        ..PollingConfig::default()
    };
    let mut receiver = MessagePoller::new(client, args.room_id)
        .with_config(config)
        .spawn();
    while let Some(message) = receiver.recv().await {
        print_json_line(&message?)?;
    }
    Ok(())
}

async fn sidecar_receive(args: SidecarReceiveArgs) -> CliResult<()> {
    validate_loopback_bind(&args.bind, args.allow_non_loopback).await?;
    let expected_token = args
        .token
        .or_else(|| env::var("WEBEX_SIDECAR_TOKEN").ok())
        .filter(|token| !token.is_empty());
    if expected_token.is_none() && !args.allow_unauthenticated {
        return Err(CliError(
            "sidecar receive requires --token/WEBEX_SIDECAR_TOKEN or --allow-unauthenticated"
                .into(),
        )
        .into());
    }

    let listener = TcpListener::bind(&args.bind).await?;
    eprintln!("sidecar_receiver_listening={}", listener.local_addr()?);
    eprintln!("sidecar_receiver_path={}", args.path);
    eprintln!("sidecar_receiver_health_path={}", args.health_path);
    if args.allow_non_loopback {
        eprintln!("sidecar_receiver_non_loopback_allowed=true");
    }
    if expected_token.is_none() {
        eprintln!("sidecar_receiver_unauthenticated=true");
    }

    let semaphore = Arc::new(Semaphore::new(MAX_SIDECAR_CONNECTIONS));
    let (accepted_sender, mut accepted_receiver) = mpsc::channel::<SidecarAccepted>(256);
    let accept_task = tokio::spawn(accept_sidecar_connections(
        listener,
        args.path,
        args.health_path,
        expected_token,
        semaphore,
        accepted_sender,
    ));

    let mut accepted = 0_usize;
    while let Some(accepted_event) = accepted_receiver.recv().await {
        print_json_line(&accepted_event.event)?;
        accepted += 1;
        eprintln!(
            "sidecar_event_accepted_from={} status={} accepted={accepted}",
            accepted_event.peer, accepted_event.response_status
        );
        if args.max_events > 0 && accepted >= args.max_events {
            accept_task.abort();
            break;
        }
    }
    Ok(())
}

async fn accept_sidecar_connections(
    listener: TcpListener,
    path: String,
    health_path: String,
    expected_token: Option<String>,
    semaphore: Arc<Semaphore>,
    accepted_sender: mpsc::Sender<SidecarAccepted>,
) {
    loop {
        let Ok((mut stream, peer)) = listener.accept().await else {
            return;
        };
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            let response = HttpResponse::json_error(503, "too many sidecar connections");
            if let Err(error) = write_response(&mut stream, &response).await {
                eprintln!("sidecar_response_write_failed peer={peer} error={error}");
            }
            continue;
        };
        tokio::spawn(handle_sidecar_connection(
            stream,
            peer,
            path.clone(),
            health_path.clone(),
            expected_token.clone(),
            accepted_sender.clone(),
            permit,
        ));
    }
}

async fn handle_sidecar_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    path: String,
    health_path: String,
    expected_token: Option<String>,
    accepted_sender: mpsc::Sender<SidecarAccepted>,
    _permit: OwnedSemaphorePermit,
) {
    let result = match timeout(Duration::from_secs(10), read_request(&mut stream)).await {
        Ok(Ok(request)) => {
            handle_sidecar_request(&request, &path, &health_path, expected_token.as_deref())
        }
        Ok(Err(error)) => SidecarHandleResult {
            response: HttpResponse::json_error(400, error.to_string()),
            event: None,
        },
        Err(_) => SidecarHandleResult {
            response: HttpResponse::json_error(408, "request timeout"),
            event: None,
        },
    };
    let response_status = result.response.status;
    if let Err(error) = write_response(&mut stream, &result.response).await {
        eprintln!("sidecar_response_write_failed peer={peer} error={error}");
        return;
    }
    if let Some(event) = result.event {
        let _ = accepted_sender
            .send(SidecarAccepted {
                peer,
                response_status,
                event,
            })
            .await;
    }
}

async fn validate_loopback_bind(bind: &str, allow_non_loopback: bool) -> CliResult<()> {
    let resolved = tokio::net::lookup_host(bind)
        .await?
        .collect::<Vec<SocketAddr>>();
    if resolved.is_empty() {
        return Err(CliError(format!("bind address {bind:?} did not resolve")).into());
    }
    if !allow_non_loopback && !resolved.iter().all(|addr| addr.ip().is_loopback()) {
        return Err(CliError(
            "sidecar bind must resolve only to loopback addresses; use --allow-non-loopback only for explicitly secured deployments"
                .into(),
        )
        .into());
    }
    Ok(())
}

#[derive(Debug)]
struct SidecarAccepted {
    peer: SocketAddr,
    response_status: u16,
    event: SidecarEvent,
}

#[derive(Debug)]
struct SidecarHandleResult {
    response: HttpResponse,
    event: Option<SidecarEvent>,
}

fn handle_sidecar_request(
    request: &HttpRequest,
    expected_path: &str,
    health_path: &str,
    expected_token: Option<&str>,
) -> SidecarHandleResult {
    if request.path == health_path {
        return if request.method == "GET" {
            SidecarHandleResult {
                response: HttpResponse::json_value(200, json!({ "ok": true })),
                event: None,
            }
        } else {
            SidecarHandleResult {
                response: HttpResponse::json_error(405, "method not allowed"),
                event: None,
            }
        };
    }
    if request.method != "POST" {
        return SidecarHandleResult {
            response: HttpResponse::json_error(405, "method not allowed"),
            event: None,
        };
    }
    if request.path != expected_path {
        return SidecarHandleResult {
            response: HttpResponse::json_error(404, "not found"),
            event: None,
        };
    }
    if let Some(token) = expected_token {
        let expected = format!("Bearer {token}");
        if request.headers.get("authorization") != Some(&expected) {
            return SidecarHandleResult {
                response: HttpResponse::json_error(401, "unauthorized"),
                event: None,
            };
        }
    }

    match serde_json::from_slice::<SidecarEvent>(&request.body) {
        Ok(event) => SidecarHandleResult {
            response: HttpResponse::json_value(200, json!({ "ok": true })),
            event: Some(event),
        },
        Err(error) => SidecarHandleResult {
            response: HttpResponse::json_error(400, error.to_string()),
            event: None,
        },
    }
}

async fn read_request(stream: &mut TcpStream) -> CliResult<HttpRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 2048];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(CliError("connection closed before request completed".into()).into());
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(header_end) = find_bytes(&bytes, b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = parse_content_length(&headers)?.unwrap_or(0);
            if content_length > MAX_BODY_BYTES {
                return Err(CliError("request body exceeded maximum size".into()).into());
            }
            if bytes.len() >= header_end + 4 + content_length {
                return parse_request(
                    &bytes[..header_end],
                    bytes[header_end + 4..header_end + 4 + content_length].to_vec(),
                );
            }
        } else if bytes.len() > MAX_HEADER_BYTES {
            return Err(CliError("request headers exceeded maximum size".into()).into());
        }
    }
}

fn parse_request(headers: &[u8], body: Vec<u8>) -> CliResult<HttpRequest> {
    let text = String::from_utf8_lossy(headers);
    let mut lines = text.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| CliError("missing request line".into()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| CliError("missing method".into()))?
        .to_owned();
    let path = request_parts
        .next()
        .ok_or_else(|| CliError("missing path".into()))?
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

fn parse_content_length(headers: &str) -> CliResult<Option<usize>> {
    Ok(headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| {
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| CliError("invalid content-length".into()))
            })
        })
        .transpose()?)
}

async fn write_response(stream: &mut TcpStream, response: &HttpResponse) -> CliResult<()> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        503 => "Service Unavailable",
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

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

impl HttpResponse {
    fn json_value(status: u16, value: serde_json::Value) -> Self {
        Self {
            status,
            body: serde_json::to_string(&value).expect("JSON value serialization cannot fail"),
        }
    }

    fn json_error(status: u16, error: impl AsRef<str>) -> Self {
        Self::json_value(status, json!({ "ok": false, "error": error.as_ref() }))
    }
}

fn print_json<T>(value: &T) -> CliResult<()>
where
    T: Serialize,
{
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn print_json_line<T>(value: &T) -> CliResult<()>
where
    T: Serialize,
{
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

fn print_usage() {
    println!(
        "\
webex-headless [global-options] <command> [options]

Global options:
  --access-token TOKEN       Webex bearer token. Env: WEBEX_ACCESS_TOKEN
  --token-file PATH          TokenSet JSON file. Env: WEBEX_TOKEN_FILE
  --client-id ID             Integration client ID. Env: WEBEX_CLIENT_ID
  --client-secret SECRET     Integration client secret. Env: WEBEX_CLIENT_SECRET

Commands:
  auth device [--token-file PATH] [--scopes LIST] [--scope S] [--stdout-token]
  auth refresh [--token-file PATH] [--client-id ID] [--client-secret SECRET]
  me
  rooms list [--max N] [--type group|direct] [--team-id ID] [--all]
  rooms get --room-id ID
  rooms resolve [--room-id ID] [--link URL] [--title TITLE]
  messages list --room-id ID [--parent-id ID] [--max N] [--all]
  messages direct (--person-id ID | --person-email EMAIL) [--parent-id ID] [--all]
  messages get --message-id ID
  messages send (--room-id ID | --to-person-email EMAIL) (--text TEXT | --markdown MARKDOWN) [--file PATH] [--media-type TYPE] [--file-name NAME]
  messages reply --room-id ID --parent-id ID (--text TEXT | --markdown MARKDOWN)
  messages delete --message-id ID
  poll messages --room-id ID [--interval-seconds N] [--max N] [--emit-existing]
  sidecar receive [--bind ADDR] [--path PATH] [--health-path PATH] [--token TOKEN] [--max-events N] [--allow-unauthenticated] [--allow-non-loopback]
"
    );
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parse_scopes_accepts_commas_and_whitespace() {
        assert_eq!(
            parse_scopes("spark:messages_read, spark:messages_write\nspark:kms"),
            [
                "spark:messages_read".to_owned(),
                "spark:messages_write".to_owned(),
                "spark:kms".to_owned()
            ]
        );
    }

    #[test]
    fn parses_room_link_candidates_from_query_and_fragment() {
        let candidates = room_id_candidates_from_link(
            "https://web.webex.com/rooms?id=Y2lzY29zcGFyazovL3VzL1JPT00vabc#spaceId=room-1234567890123456",
        );

        assert!(candidates.contains(&"Y2lzY29zcGFyazovL3VzL1JPT00vabc".to_owned()));
        assert!(candidates.contains(&"room-1234567890123456".to_owned()));
    }

    #[test]
    fn parses_global_and_command_options() {
        let cli = Cli::parse([
            "--token-file".to_owned(),
            "token.json".to_owned(),
            "messages".to_owned(),
            "send".to_owned(),
            "--room-id".to_owned(),
            "room-1".to_owned(),
            "--text".to_owned(),
            "hello".to_owned(),
        ])
        .unwrap();

        assert_eq!(cli.global.token_file, Some(PathBuf::from("token.json")));
        assert!(matches!(
            cli.command,
            Command::MessagesSend(MessagesSendArgs {
                target: MessageTarget::Room(_),
                body: MessageBody::Text(_),
                file: None,
            })
        ));
    }

    #[test]
    fn parses_auth_refresh_token_file() {
        let cli = Cli::parse([
            "auth".to_owned(),
            "refresh".to_owned(),
            "--token-file".to_owned(),
            "token.json".to_owned(),
        ])
        .unwrap();

        assert!(matches!(
            cli.command,
            Command::AuthRefresh(AuthRefreshArgs { token_file: Some(path), .. })
                if path == Path::new("token.json")
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn token_file_store_round_trips_tokens() {
        use std::os::unix::fs::PermissionsExt as _;

        let path = std::env::temp_dir().join(format!(
            "webex-headless-token-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let token = TokenSet {
            access_token: "access-token".to_owned(),
            refresh_token: Some("refresh-token".to_owned()),
            token_type: "Bearer".to_owned(),
            scopes: vec!["spark:messages_read".to_owned()],
            expires_at: None,
            refresh_token_expires_at: None,
        };

        save_token_file(&path, &token).await.unwrap();
        let loaded = read_token_file(&path).await.unwrap();

        assert_eq!(loaded.access_token, "access-token");
        assert_eq!(loaded.refresh_token.as_deref(), Some("refresh-token"));
        #[cfg(unix)]
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_file(&path).unwrap();
    }

    #[cfg(not(unix))]
    #[tokio::test]
    async fn token_file_store_rejects_non_unix_persistence() {
        let token = TokenSet {
            access_token: "access-token".to_owned(),
            refresh_token: Some("refresh-token".to_owned()),
            token_type: "Bearer".to_owned(),
            scopes: vec!["spark:messages_read".to_owned()],
            expires_at: None,
            refresh_token_expires_at: None,
        };
        let error = save_token_file(Path::new("token.json"), &token)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("only supported on Unix"));
    }

    #[test]
    fn explicit_token_file_wins_over_env_access_token() {
        assert_eq!(
            auth_source_from_parts(
                None,
                Some(PathBuf::from("token.json")),
                None,
                Some("env-access-token".to_owned())
            )
            .unwrap(),
            AuthSource::TokenFile(PathBuf::from("token.json"))
        );
    }

    #[test]
    fn env_token_file_wins_over_env_access_token() {
        assert_eq!(
            auth_source_from_parts(
                None,
                None,
                Some(PathBuf::from("token.json")),
                Some("env-access-token".to_owned())
            )
            .unwrap(),
            AuthSource::TokenFile(PathBuf::from("token.json"))
        );
    }

    #[test]
    fn rejects_explicit_auth_source_conflict() {
        let error = auth_source_from_parts(
            Some("access-token".to_owned()),
            Some(PathBuf::from("token.json")),
            None,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("not both"));
    }

    #[test]
    fn rejects_direct_message_double_target() {
        let error = Cli::parse([
            "messages".to_owned(),
            "direct".to_owned(),
            "--person-id".to_owned(),
            "person-1".to_owned(),
            "--person-email".to_owned(),
            "person@example.com".to_owned(),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("only one target"));
    }

    #[test]
    fn rejects_sidecar_event_and_health_path_conflict() {
        let error = Cli::parse([
            "sidecar".to_owned(),
            "receive".to_owned(),
            "--path".to_owned(),
            "/same".to_owned(),
            "--health-path".to_owned(),
            "/same".to_owned(),
            "--token".to_owned(),
            "token-1".to_owned(),
        ])
        .unwrap_err();

        assert!(error.to_string().contains("--path and --health-path"));
    }

    #[test]
    fn sidecar_rejects_missing_bearer_token() {
        let request = HttpRequest {
            method: "POST".to_owned(),
            path: DEFAULT_SIDECAR_PATH.to_owned(),
            headers: BTreeMap::new(),
            body: serde_json::to_vec(&SidecarEvent::message_created(json!({
                "id": "message-1"
            })))
            .unwrap(),
        };

        let result = handle_sidecar_request(
            &request,
            DEFAULT_SIDECAR_PATH,
            DEFAULT_SIDECAR_HEALTH_PATH,
            Some("token-1"),
        );
        assert_eq!(result.response.status, 401);
        assert!(result.event.is_none());
    }

    #[test]
    fn sidecar_health_check_does_not_require_bearer_token() {
        let request = HttpRequest {
            method: "GET".to_owned(),
            path: DEFAULT_SIDECAR_HEALTH_PATH.to_owned(),
            headers: BTreeMap::new(),
            body: Vec::new(),
        };

        let result = handle_sidecar_request(
            &request,
            DEFAULT_SIDECAR_PATH,
            DEFAULT_SIDECAR_HEALTH_PATH,
            Some("token-1"),
        );
        assert_eq!(result.response.status, 200);
        assert!(result.event.is_none());
    }

    #[test]
    fn sidecar_accepts_valid_event() {
        let mut headers = BTreeMap::new();
        headers.insert("authorization".to_owned(), "Bearer token-1".to_owned());
        let request = HttpRequest {
            method: "POST".to_owned(),
            path: DEFAULT_SIDECAR_PATH.to_owned(),
            headers,
            body: serde_json::to_vec(&SidecarEvent::message_created(json!({
                "id": "message-1"
            })))
            .unwrap(),
        };

        let result = handle_sidecar_request(
            &request,
            DEFAULT_SIDECAR_PATH,
            DEFAULT_SIDECAR_HEALTH_PATH,
            Some("token-1"),
        );
        assert_eq!(result.response.status, 200);
        assert_eq!(result.event.unwrap().resource, "messages");
    }
}
