use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

#[cfg(all(unix, feature = "sqlite-state-cache"))]
use std::os::unix::fs::DirBuilderExt as _;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};

use chrono::{DateTime, Utc};
#[cfg(feature = "sqlite-state-cache")]
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    realtime::RoomCheckpoint,
};

const STATE_RECORD_VERSION: u8 = 1;
static STATE_PATH_LOCKS: OnceLock<Mutex<BTreeSet<StateLockKey>>> = OnceLock::new();
static NEXT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum StateLockKey {
    Process,
    Path(PathBuf),
    #[cfg(unix)]
    FileId {
        dev: u64,
        ino: u64,
    },
}

struct StatePathLock {
    keys: Vec<StateLockKey>,
}

impl StatePathLock {
    fn acquire(path: &Path) -> Result<Self> {
        let locks = STATE_PATH_LOCKS.get_or_init(|| Mutex::new(BTreeSet::new()));
        loop {
            let keys = state_lock_keys(path)?;
            {
                let mut locked = locks
                    .lock()
                    .map_err(|_| Error::Other("state path lock is poisoned".to_owned()))?;
                if keys.iter().all(|key| !locked.contains(key)) {
                    for key in &keys {
                        locked.insert(key.clone());
                    }
                    return Ok(Self { keys });
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for StatePathLock {
    fn drop(&mut self) {
        if let Some(locks) = STATE_PATH_LOCKS.get() {
            if let Ok(mut locked) = locks.lock() {
                for key in &self.keys {
                    locked.remove(key);
                }
            }
        }
    }
}

/// Append-only JSONL state for long-running generic-account automations.
///
/// The JSONL file is the correctness source of truth. The in-memory snapshot is
/// rebuilt by replaying records from the beginning of the file.
#[derive(Debug)]
pub struct JsonlStateStore {
    path: PathBuf,
    snapshot: StateSnapshot,
}

/// Rebuildable SQLite index over the JSONL state source of truth.
///
/// This cache is optional acceleration only. Rebuild it from a [`StateSnapshot`],
/// [`JsonlStateStore`], or JSONL file whenever the caller needs fresh indexed
/// lookups. The JSONL state remains the correctness source of truth.
#[cfg(feature = "sqlite-state-cache")]
#[derive(Debug)]
pub struct SqliteStateCache {
    conn: Connection,
}

/// Rebuilt view of the append-only state log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateSnapshot {
    processed_message_ids: BTreeSet<String>,
    attempt_leases: BTreeMap<String, AttemptLease>,
    room_checkpoints: BTreeMap<String, RoomCheckpoint>,
}

/// Result of trying to begin processing a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptStart {
    /// The caller owns the attempt and should eventually mark, release, or defer it.
    Started(AttemptLease),
    /// The message was already durably processed.
    Processed,
    /// A prior attempt lease is still active.
    Leased(Duration),
}

/// Ownership token for a started processing attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptLease {
    message_id: String,
    attempt_id: String,
    expires_at: DateTime<Utc>,
}

impl AttemptLease {
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

/// Public view of an active attempt lease.
///
/// This is not an ownership token and cannot be used to release, defer, or
/// mark a message processed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptLeaseStatus {
    message_id: String,
    expires_at: DateTime<Utc>,
}

impl AttemptLeaseStatus {
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

impl From<&AttemptLease> for AttemptLeaseStatus {
    fn from(attempt: &AttemptLease) -> Self {
        Self {
            message_id: attempt.message_id.clone(),
            expires_at: attempt.expires_at,
        }
    }
}

/// One JSONL state record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateRecord {
    pub version: u8,
    #[serde(flatten)]
    pub event: StateEvent,
}

/// Event payload stored in a [`StateRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum StateEvent {
    ProcessedMessage {
        message_id: String,
        processed_at: DateTime<Utc>,
    },
    AttemptLeased {
        message_id: String,
        attempt_id: String,
        expires_at: DateTime<Utc>,
    },
    AttemptReleased {
        message_id: String,
        attempt_id: String,
        released_at: DateTime<Utc>,
    },
    RoomCheckpoint {
        checkpoint: RoomCheckpoint,
        updated_at: DateTime<Utc>,
    },
}

impl JsonlStateStore {
    /// Load or create an empty state store view at `path`.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        Self::load_at(path, Utc::now())
    }

    /// Load a state store view using `now` for attempt lease expiry.
    pub fn load_at(path: impl Into<PathBuf>, now: DateTime<Utc>) -> Result<Self> {
        let path = stable_state_path(&path.into())?;
        let _guard = StatePathLock::acquire(&path)?;
        repair_incomplete_tail(&path, now)?;
        let snapshot = StateSnapshot::load_at(&path, now)?;
        Ok(Self { path, snapshot })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn snapshot(&self) -> &StateSnapshot {
        &self.snapshot
    }

    pub fn into_snapshot(self) -> StateSnapshot {
        self.snapshot
    }

    pub fn contains_processed_message(&self, message_id: &str) -> bool {
        self.snapshot.contains_processed_message(message_id)
    }

    pub fn begin_attempt(&mut self, message_id: &str, lease: Duration) -> Result<AttemptStart> {
        self.begin_attempt_at(message_id, lease, Utc::now())
    }

    pub fn begin_attempt_at(
        &mut self,
        message_id: &str,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<AttemptStart> {
        ensure_message_id(message_id)?;
        let _guard = StatePathLock::acquire(&self.path)?;
        self.refresh_at_unlocked(now)?;
        if self.snapshot.contains_processed_message(message_id) {
            return Ok(AttemptStart::Processed);
        }
        if let Some(active) = self.snapshot.active_attempt_lease(message_id) {
            if let Some(remaining) = remaining_lease(active.expires_at, now) {
                return Ok(AttemptStart::Leased(remaining));
            }
        }

        let attempt = new_attempt_lease(message_id, now, lease)?;
        let record = StateRecord::new(StateEvent::AttemptLeased {
            message_id: attempt.message_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            expires_at: attempt.expires_at,
        });
        self.append_after_current_snapshot_unlocked(record, now)?;
        Ok(AttemptStart::Started(attempt))
    }

    pub fn release_attempt(&mut self, attempt: &AttemptLease) -> Result<()> {
        self.release_attempt_at(attempt, Utc::now())
    }

    pub fn release_attempt_at(&mut self, attempt: &AttemptLease, now: DateTime<Utc>) -> Result<()> {
        let _guard = StatePathLock::acquire(&self.path)?;
        self.refresh_at_unlocked(now)?;
        self.ensure_attempt_owner(attempt, now)?;
        let record = StateRecord::new(StateEvent::AttemptReleased {
            message_id: attempt.message_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            released_at: now,
        });
        self.append_after_current_snapshot_unlocked(record, now)
    }

    pub fn defer_attempt(&mut self, attempt: &AttemptLease, lease: Duration) -> Result<()> {
        self.defer_attempt_at(attempt, lease, Utc::now())
    }

    pub fn defer_attempt_at(
        &mut self,
        attempt: &AttemptLease,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let _guard = StatePathLock::acquire(&self.path)?;
        self.refresh_at_unlocked(now)?;
        self.ensure_attempt_owner(attempt, now)?;
        let expires_at = lease_expires_at(now, lease)?;
        let record = StateRecord::new(StateEvent::AttemptLeased {
            message_id: attempt.message_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            expires_at,
        });
        self.append_after_current_snapshot_unlocked(record, now)
    }

    pub fn mark_processed(&mut self, attempt: &AttemptLease) -> Result<()> {
        self.mark_processed_at(attempt, Utc::now())
    }

    pub fn mark_processed_at(&mut self, attempt: &AttemptLease, now: DateTime<Utc>) -> Result<()> {
        let _guard = StatePathLock::acquire(&self.path)?;
        self.refresh_at_unlocked(now)?;
        if self
            .snapshot
            .contains_processed_message(attempt.message_id())
        {
            return Ok(());
        }
        self.ensure_attempt_owner(attempt, now)?;
        let record = StateRecord::new(StateEvent::ProcessedMessage {
            message_id: attempt.message_id.clone(),
            processed_at: now,
        });
        self.append_after_current_snapshot_unlocked(record, now)
    }

    pub fn save_room_checkpoint(&mut self, checkpoint: RoomCheckpoint) -> Result<()> {
        self.save_room_checkpoint_at(checkpoint, Utc::now())
    }

    pub fn save_room_checkpoint_at(
        &mut self,
        checkpoint: RoomCheckpoint,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let _guard = StatePathLock::acquire(&self.path)?;
        self.refresh_at_unlocked(now)?;
        let record = StateRecord::new(StateEvent::RoomCheckpoint {
            checkpoint,
            updated_at: now,
        });
        self.append_after_current_snapshot_unlocked(record, now)
    }

    pub fn save_room_checkpoints<I>(&mut self, checkpoints: I) -> Result<()>
    where
        I: IntoIterator<Item = RoomCheckpoint>,
    {
        let now = Utc::now();
        let _guard = StatePathLock::acquire(&self.path)?;
        self.refresh_at_unlocked(now)?;
        for checkpoint in checkpoints {
            let record = StateRecord::new(StateEvent::RoomCheckpoint {
                checkpoint,
                updated_at: now,
            });
            self.append_after_current_snapshot_unlocked(record, now)?;
        }
        Ok(())
    }

    fn ensure_attempt_owner(&self, attempt: &AttemptLease, now: DateTime<Utc>) -> Result<()> {
        ensure_message_id(attempt.message_id())?;
        match self.snapshot.active_attempt_lease(attempt.message_id()) {
            Some(active)
                if active.attempt_id == attempt.attempt_id
                    && remaining_lease(active.expires_at, now).is_some() =>
            {
                Ok(())
            }
            _ => Err(Error::Other(format!(
                "attempt lease {} is not active for message {}",
                attempt.attempt_id, attempt.message_id
            ))),
        }
    }

    fn refresh_at_unlocked(&mut self, now: DateTime<Utc>) -> Result<()> {
        repair_incomplete_tail(&self.path, now)?;
        self.snapshot = StateSnapshot::load_at(&self.path, now)?;
        Ok(())
    }

    fn append_after_current_snapshot_unlocked(
        &mut self,
        record: StateRecord,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let mut next_snapshot = self.snapshot.clone();
        next_snapshot.apply_record(record.clone(), now)?;
        append_state_record(&self.path, &record)?;
        self.snapshot = next_snapshot;
        Ok(())
    }
}

#[cfg(feature = "sqlite-state-cache")]
impl SqliteStateCache {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        prepare_sqlite_cache_file(path)?;
        let conn = open_sqlite_cache_connection(path)?;
        let cache = Self { conn };
        cache.initialize()?;
        Ok(cache)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(sqlite_error)?;
        let cache = Self { conn };
        cache.initialize()?;
        Ok(cache)
    }

    pub fn rebuild_from_jsonl(&mut self, path: impl AsRef<Path>) -> Result<()> {
        self.rebuild_from_locked_jsonl(path.as_ref(), Utc::now())
    }

    pub fn rebuild_from_store(&mut self, store: &JsonlStateStore) -> Result<()> {
        self.rebuild_from_locked_jsonl(store.path(), Utc::now())
    }

    pub fn rebuild_from_snapshot(&mut self, snapshot: &StateSnapshot) -> Result<()> {
        let tx = self.conn.transaction().map_err(sqlite_error)?;
        tx.execute_batch(
            "DELETE FROM processed_messages;\nDELETE FROM room_checkpoints;\nDELETE FROM metadata;",
        )
        .map_err(sqlite_error)?;
        tx.execute(
            "INSERT INTO metadata(key, value) VALUES (?1, ?2)",
            params!["schema_version", "1"],
        )
        .map_err(sqlite_error)?;
        {
            let mut statement = tx
                .prepare("INSERT INTO processed_messages(message_id) VALUES (?1)")
                .map_err(sqlite_error)?;
            for message_id in snapshot.processed_message_ids() {
                statement
                    .execute(params![message_id])
                    .map_err(sqlite_error)?;
            }
        }
        {
            let mut statement = tx
                .prepare("INSERT INTO room_checkpoints(room_id, checkpoint_json) VALUES (?1, ?2)")
                .map_err(sqlite_error)?;
            for checkpoint in snapshot.room_checkpoints() {
                let checkpoint_json = serde_json::to_string(checkpoint)?;
                statement
                    .execute(params![checkpoint.room_id.as_str(), checkpoint_json])
                    .map_err(sqlite_error)?;
            }
        }
        tx.commit().map_err(sqlite_error)
    }

    pub fn contains_processed_message(&self, message_id: &str) -> Result<bool> {
        ensure_message_id(message_id)?;
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(1) FROM processed_messages WHERE message_id = ?1",
                params![message_id],
                |row| row.get(0),
            )
            .map_err(sqlite_error)?;
        Ok(count > 0)
    }

    pub fn processed_message_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(1) FROM processed_messages", [], |row| {
                row.get(0)
            })
            .map_err(sqlite_error)?;
        u64::try_from(count)
            .map_err(|_| Error::Other("processed message count is negative".to_owned()))
    }

    pub fn room_checkpoint(&self, room_id: &str) -> Result<Option<RoomCheckpoint>> {
        if room_id.trim().is_empty() {
            return Err(Error::Other("room checkpoint id is empty".to_owned()));
        }
        let checkpoint_json = self
            .conn
            .query_row(
                "SELECT checkpoint_json FROM room_checkpoints WHERE room_id = ?1",
                params![room_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sqlite_error)?;
        checkpoint_json
            .map(|checkpoint_json| serde_json::from_str(&checkpoint_json).map_err(Into::into))
            .transpose()
    }

    pub fn room_checkpoints(&self) -> Result<Vec<RoomCheckpoint>> {
        let mut statement = self
            .conn
            .prepare("SELECT checkpoint_json FROM room_checkpoints ORDER BY room_id")
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sqlite_error)?;
        let mut checkpoints = Vec::new();
        for row in rows {
            let checkpoint_json = row.map_err(sqlite_error)?;
            checkpoints.push(serde_json::from_str(&checkpoint_json)?);
        }
        Ok(checkpoints)
    }

    fn initialize(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS metadata (\n    key TEXT PRIMARY KEY,\n    value TEXT NOT NULL\n);\nCREATE TABLE IF NOT EXISTS processed_messages (\n    message_id TEXT PRIMARY KEY\n);\nCREATE TABLE IF NOT EXISTS room_checkpoints (\n    room_id TEXT PRIMARY KEY,\n    checkpoint_json TEXT NOT NULL\n);",
            )
            .map_err(sqlite_error)
    }

    fn rebuild_from_locked_jsonl(&mut self, path: &Path, now: DateTime<Utc>) -> Result<()> {
        let path = stable_state_path(path)?;
        let _guard = StatePathLock::acquire(&path)?;
        repair_incomplete_tail(&path, now)?;
        let snapshot = StateSnapshot::load_at(&path, now)?;
        self.rebuild_from_snapshot(&snapshot)
    }
}

impl StateSnapshot {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        Self::load_at(path, Utc::now())
    }

    pub fn load_at(path: impl AsRef<Path>, now: DateTime<Utc>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error.into()),
        };
        let (snapshot, _) = parse_snapshot_bytes(path, &bytes, now, true)?;
        Ok(snapshot)
    }

    pub fn processed_message_count(&self) -> usize {
        self.processed_message_ids.len()
    }

    pub fn contains_processed_message(&self, message_id: &str) -> bool {
        self.processed_message_ids.contains(message_id)
    }

    pub fn processed_message_ids(&self) -> impl Iterator<Item = &str> {
        self.processed_message_ids.iter().map(String::as_str)
    }

    /// Return the public status for an active attempt lease.
    ///
    /// The returned status deliberately omits the owner token. Only the
    /// [`AttemptLease`] returned by [`JsonlStateStore::begin_attempt`] can be
    /// used to release, defer, or mark the attempt processed.
    ///
    /// ```compile_fail
    /// # use webex_headless_messenger::{JsonlStateStore, StateSnapshot};
    /// # fn cannot_use_snapshot_status_as_owner_token(
    /// #     mut store: JsonlStateStore,
    /// #     snapshot: StateSnapshot,
    /// # ) -> webex_headless_messenger::Result<()> {
    /// let status = snapshot.attempt_lease("message-1").unwrap();
    /// store.release_attempt(&status)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn attempt_lease(&self, message_id: &str) -> Option<AttemptLeaseStatus> {
        self.attempt_leases.get(message_id).map(Into::into)
    }

    pub fn attempt_leases(&self) -> impl Iterator<Item = AttemptLeaseStatus> + '_ {
        self.attempt_leases.values().map(Into::into)
    }

    pub fn room_checkpoint(&self, room_id: &str) -> Option<&RoomCheckpoint> {
        self.room_checkpoints.get(room_id)
    }

    pub fn room_checkpoints(&self) -> impl Iterator<Item = &RoomCheckpoint> {
        self.room_checkpoints.values()
    }

    fn apply_record(&mut self, record: StateRecord, now: DateTime<Utc>) -> Result<()> {
        if record.version != STATE_RECORD_VERSION {
            return Err(Error::Other(format!(
                "unsupported state record version {}; expected {STATE_RECORD_VERSION}",
                record.version
            )));
        }
        match record.event {
            StateEvent::ProcessedMessage { message_id, .. } => {
                ensure_message_id(&message_id)?;
                self.processed_message_ids.insert(message_id.clone());
                self.attempt_leases.remove(&message_id);
            }
            StateEvent::AttemptLeased {
                message_id,
                attempt_id,
                expires_at,
            } => {
                ensure_message_id(&message_id)?;
                ensure_attempt_id(&attempt_id)?;
                if self.processed_message_ids.contains(&message_id) {
                    self.attempt_leases.remove(&message_id);
                } else if expires_at > now {
                    self.attempt_leases.insert(
                        message_id.clone(),
                        AttemptLease {
                            message_id,
                            attempt_id,
                            expires_at,
                        },
                    );
                } else {
                    self.attempt_leases.remove(&message_id);
                }
            }
            StateEvent::AttemptReleased {
                message_id,
                attempt_id,
                ..
            } => {
                ensure_message_id(&message_id)?;
                ensure_attempt_id(&attempt_id)?;
                if self
                    .attempt_leases
                    .get(&message_id)
                    .is_some_and(|active| active.attempt_id == attempt_id)
                {
                    self.attempt_leases.remove(&message_id);
                }
            }
            StateEvent::RoomCheckpoint { checkpoint, .. } => {
                if checkpoint.room_id.trim().is_empty() {
                    return Err(Error::Other("room checkpoint id is empty".to_owned()));
                }
                self.room_checkpoints
                    .insert(checkpoint.room_id.clone(), checkpoint);
            }
        }
        self.prune_expired_attempts(now);
        Ok(())
    }

    fn prune_expired_attempts(&mut self, now: DateTime<Utc>) {
        self.attempt_leases
            .retain(|_, attempt| remaining_lease(attempt.expires_at, now).is_some());
    }

    fn active_attempt_lease(&self, message_id: &str) -> Option<&AttemptLease> {
        self.attempt_leases.get(message_id)
    }
}

impl StateRecord {
    pub fn new(event: StateEvent) -> Self {
        Self {
            version: STATE_RECORD_VERSION,
            event,
        }
    }
}

fn parse_snapshot_bytes(
    path: &Path,
    bytes: &[u8],
    now: DateTime<Utc>,
    allow_torn_tail: bool,
) -> Result<(StateSnapshot, u64)> {
    let mut snapshot = StateSnapshot::default();
    let mut offset = 0usize;
    let mut line_number = 1usize;

    while offset < bytes.len() {
        let relative_newline = bytes[offset..].iter().position(|byte| *byte == b'\n');
        let (line_bytes, next_offset, has_newline) = match relative_newline {
            Some(index) => (&bytes[offset..offset + index], offset + index + 1, true),
            None => (&bytes[offset..], bytes.len(), false),
        };
        let line = match std::str::from_utf8(line_bytes) {
            Ok(line) => line.trim(),
            Err(error) => {
                if allow_torn_tail && !has_newline {
                    return Ok((snapshot, offset as u64));
                }
                return Err(Error::Other(format!(
                    "invalid state JSONL record at {}:{line_number}: {error}",
                    path.display()
                )));
            }
        };

        if !line.is_empty() {
            let record = match serde_json::from_str::<StateRecord>(line) {
                Ok(record) => record,
                Err(error) => {
                    if allow_torn_tail && !has_newline {
                        return Ok((snapshot, offset as u64));
                    }
                    return Err(Error::Other(format!(
                        "invalid state JSONL record at {}:{line_number}: {error}",
                        path.display()
                    )));
                }
            };
            snapshot.apply_record(record, now).map_err(|error| {
                Error::Other(format!(
                    "invalid state JSONL record at {}:{line_number}: {error}",
                    path.display()
                ))
            })?;
        }

        offset = next_offset;
        line_number += 1;
    }

    snapshot.prune_expired_attempts(now);
    Ok((snapshot, bytes.len() as u64))
}

fn repair_incomplete_tail(path: &Path, now: DateTime<Utc>) -> Result<()> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let (_, valid_len) = parse_snapshot_bytes(path, &bytes, now, true)?;
    if valid_len < bytes.len() as u64 {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(valid_len)?;
        file.sync_all()?;
    }
    Ok(())
}

fn state_lock_keys(path: &Path) -> Result<Vec<StateLockKey>> {
    let mut keys = vec![
        StateLockKey::Process,
        StateLockKey::Path(stable_state_path(path)?),
    ];
    #[cfg(unix)]
    {
        match fs::metadata(path) {
            Ok(metadata) => keys.push(StateLockKey::FileId {
                dev: metadata.dev(),
                ino: metadata.ino(),
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn stable_state_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    stable_state_path_inner(absolute, 0)
}

fn stable_state_path_inner(path: PathBuf, symlink_depth: usize) -> Result<PathBuf> {
    if symlink_depth > 40 {
        return Err(Error::Other(
            "state path has too many symbolic links".to_owned(),
        ));
    }
    if path.exists() {
        return Ok(path.canonicalize()?);
    }
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if metadata.file_type().is_symlink() {
            let target = fs::read_link(&path)?;
            let resolved = if target.is_absolute() {
                target
            } else {
                path.parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."))
                    .join(target)
            };
            return stable_state_path_inner(resolved, symlink_depth + 1);
        }
    }
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
        if parent.exists() {
            return Ok(parent.canonicalize()?.join(file_name));
        }
    }
    Ok(normalize_lexical_path(path))
}

fn normalize_lexical_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn append_state_record(path: &Path, record: &StateRecord) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let existed = path.exists();
    let mut options = OpenOptions::new();
    options.create(true).append(true).read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    ensure_trailing_newline(&mut file)?;
    serde_json::to_writer(&mut file, record)?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    if !existed {
        sync_parent_dir(path)?;
    }
    Ok(())
}

fn ensure_trailing_newline(file: &mut File) -> Result<()> {
    if file.metadata()?.len() == 0 {
        return Ok(());
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last = [0u8; 1];
    file.read_exact(&mut last)?;
    if last[0] != b'\n' {
        file.seek(SeekFrom::End(0))?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn new_attempt_lease(
    message_id: &str,
    now: DateTime<Utc>,
    lease: Duration,
) -> Result<AttemptLease> {
    ensure_message_id(message_id)?;
    let sequence = NEXT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed);
    Ok(AttemptLease {
        message_id: message_id.to_owned(),
        attempt_id: format!(
            "{}-{}-{sequence}",
            now.timestamp_micros(),
            std::process::id()
        ),
        expires_at: lease_expires_at(now, lease)?,
    })
}

fn lease_expires_at(now: DateTime<Utc>, lease: Duration) -> Result<DateTime<Utc>> {
    let lease = if lease.is_zero() {
        Duration::from_secs(1)
    } else {
        lease
    };
    let lease = chrono::Duration::from_std(lease)
        .map_err(|_| Error::Other("attempt lease duration is too large".to_owned()))?;
    now.checked_add_signed(lease)
        .ok_or_else(|| Error::Other("attempt lease expiry overflowed".to_owned()))
}

fn remaining_lease(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> Option<Duration> {
    let remaining = expires_at.signed_duration_since(now).to_std().ok()?;
    (!remaining.is_zero()).then_some(remaining)
}

#[cfg(feature = "sqlite-state-cache")]
fn open_sqlite_cache_connection(path: &Path) -> Result<Connection> {
    reject_sqlite_uri_path(path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let conn = Connection::open_with_flags(path, flags).map_err(sqlite_error)?;
    #[cfg(unix)]
    set_existing_sqlite_cache_file_private(path)?;
    Ok(conn)
}

#[cfg(feature = "sqlite-state-cache")]
fn reject_sqlite_uri_path(path: &Path) -> Result<()> {
    let path_text = path.as_os_str().to_string_lossy();
    if path_text
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("file:"))
    {
        return Err(Error::Other(format!(
            "sqlite state cache path {} must not use a file: URI",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(feature = "sqlite-state-cache")]
fn prepare_sqlite_cache_parent(path: &Path) -> Result<()> {
    if let Some(parent) = sqlite_cache_parent_path(path) {
        validate_existing_sqlite_cache_parent_dirs(parent)?;
        create_sqlite_cache_parent_dir(parent)?;
        validate_sqlite_cache_parent_dirs(parent)?;
    }
    Ok(())
}

#[cfg(feature = "sqlite-state-cache")]
fn sqlite_cache_parent_path(path: &Path) -> Option<&Path> {
    match path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        Some(parent) => Some(parent),
        None if path.is_relative() => Some(Path::new(".")),
        None => None,
    }
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn create_sqlite_cache_parent_dir(parent: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700).create(parent)?;
    Ok(())
}

#[cfg(all(not(unix), feature = "sqlite-state-cache"))]
fn create_sqlite_cache_parent_dir(parent: &Path) -> Result<()> {
    fs::create_dir_all(parent)?;
    Ok(())
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn validate_existing_sqlite_cache_parent_dirs(parent: &Path) -> Result<()> {
    for dir in sqlite_cache_parent_chain(parent)? {
        match fs::symlink_metadata(&dir) {
            Ok(metadata) => validate_sqlite_cache_parent_dir(&dir, &metadata)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(all(not(unix), feature = "sqlite-state-cache"))]
fn validate_existing_sqlite_cache_parent_dirs(_parent: &Path) -> Result<()> {
    Ok(())
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn validate_sqlite_cache_parent_dirs(parent: &Path) -> Result<()> {
    for dir in sqlite_cache_parent_chain(parent)? {
        let metadata = fs::symlink_metadata(&dir)?;
        validate_sqlite_cache_parent_dir(&dir, &metadata)?;
    }
    Ok(())
}

#[cfg(all(not(unix), feature = "sqlite-state-cache"))]
fn validate_sqlite_cache_parent_dirs(_parent: &Path) -> Result<()> {
    Ok(())
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn sqlite_cache_parent_chain(parent: &Path) -> Result<Vec<PathBuf>> {
    let absolute = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        std::env::current_dir()?.join(parent)
    };
    let mut chain = Vec::new();
    let mut current = PathBuf::new();
    for component in absolute.components() {
        current.push(component.as_os_str());
        chain.push(current.clone());
    }
    Ok(chain)
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn validate_sqlite_cache_parent_dir(parent: &Path, metadata: &fs::Metadata) -> Result<()> {
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(Error::Other(format!(
            "sqlite state cache parent {} is a symbolic link",
            parent.display()
        )));
    }
    if !file_type.is_dir() {
        return Err(Error::Other(format!(
            "sqlite state cache parent {} is not a directory",
            parent.display()
        )));
    }
    ensure_sqlite_cache_trusted_owner(parent, metadata)?;
    let mode = metadata.mode();
    let group_or_world_writable = mode & 0o022 != 0;
    let sticky = mode & 0o1000 != 0;
    if group_or_world_writable && !sticky {
        return Err(Error::Other(format!(
            "sqlite state cache parent {} is group/world-writable without sticky bit",
            parent.display()
        )));
    }
    Ok(())
}

#[cfg(feature = "sqlite-state-cache")]
fn prepare_sqlite_cache_file(path: &Path) -> Result<()> {
    reject_sqlite_uri_path(path)?;
    prepare_sqlite_cache_parent(path)?;

    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure_sqlite_cache_regular_file(path, &metadata)?;
            #[cfg(unix)]
            set_existing_sqlite_cache_file_private(path)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            create_sqlite_cache_file(path)?;
        }
        Err(error) => return Err(error.into()),
    }

    Ok(())
}

#[cfg(feature = "sqlite-state-cache")]
fn create_sqlite_cache_file(path: &Path) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    match options.open(path) {
        Ok(file) => {
            #[cfg(unix)]
            set_sqlite_cache_file_handle_private(&file)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)?;
            ensure_sqlite_cache_regular_file(path, &metadata)?;
            #[cfg(unix)]
            set_existing_sqlite_cache_file_private(path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(feature = "sqlite-state-cache")]
fn ensure_sqlite_cache_regular_file(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(Error::Other(format!(
            "sqlite state cache path {} is a symbolic link",
            path.display()
        )));
    }
    if !file_type.is_file() {
        return Err(Error::Other(format!(
            "sqlite state cache path {} is not a regular file",
            path.display()
        )));
    }
    ensure_sqlite_cache_trusted_owner(path, metadata)?;
    Ok(())
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn ensure_sqlite_cache_trusted_owner(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    let owner = metadata.uid();
    let current = current_effective_uid()?;
    if owner != 0 && owner != current {
        return Err(Error::Other(format!(
            "sqlite state cache path {} is not owned by root or the current user",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(all(not(unix), feature = "sqlite-state-cache"))]
fn ensure_sqlite_cache_trusted_owner(_path: &Path, _metadata: &fs::Metadata) -> Result<()> {
    Ok(())
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn current_effective_uid() -> Result<u32> {
    Ok(rustix::process::geteuid().as_raw())
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn set_existing_sqlite_cache_file_private(path: &Path) -> Result<()> {
    let file = File::open(path).or_else(|read_error| {
        OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|_| read_error)
    })?;
    let path_metadata = fs::symlink_metadata(path)?;
    ensure_sqlite_cache_regular_file(path, &path_metadata)?;
    let file_metadata = file.metadata()?;
    if file_metadata.dev() != path_metadata.dev() || file_metadata.ino() != path_metadata.ino() {
        return Err(Error::Other(format!(
            "sqlite state cache path {} changed while opening",
            path.display()
        )));
    }
    set_sqlite_cache_file_handle_private(&file)
}

#[cfg(all(unix, feature = "sqlite-state-cache"))]
fn set_sqlite_cache_file_handle_private(file: &File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = file.metadata()?.permissions();
    if permissions.mode() & 0o777 != 0o600 {
        permissions.set_mode(0o600);
        file.set_permissions(permissions)?;
    }
    Ok(())
}

#[cfg(feature = "sqlite-state-cache")]
fn sqlite_error(error: rusqlite::Error) -> Error {
    Error::Other(format!("sqlite state cache error: {error}"))
}

fn ensure_attempt_id(attempt_id: &str) -> Result<()> {
    if attempt_id.trim().is_empty() {
        return Err(Error::Other("attempt id is empty".to_owned()));
    }
    Ok(())
}

fn ensure_message_id(message_id: &str) -> Result<()> {
    if message_id.trim().is_empty() {
        return Err(Error::Other("message id is empty".to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use super::*;

    static NEXT_TEMP_FILE: AtomicUsize = AtomicUsize::new(0);
    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    static CWD_TEST_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

    fn ts(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().unwrap()
    }

    fn temp_file(name: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "webex-headless-state-{}-{}-{name}.jsonl",
            std::process::id(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn started_attempt(result: AttemptStart) -> AttemptLease {
        match result {
            AttemptStart::Started(attempt) => attempt,
            other => panic!("expected started attempt, got {other:?}"),
        }
    }

    #[test]
    fn jsonl_state_store_rebuilds_processed_messages_and_checkpoints() {
        let path = temp_file("processed-and-checkpoints");
        let mut store = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();

        let attempt = started_attempt(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        store.mark_processed_at(&attempt, ts(1001)).unwrap();
        store
            .save_room_checkpoint_at(RoomCheckpoint::new("room-a", ["message-1"]), ts(1002))
            .unwrap();

        let reloaded = JsonlStateStore::load_at(path.clone(), ts(1003)).unwrap();
        assert!(reloaded.contains_processed_message("message-1"));
        assert!(reloaded.snapshot().attempt_lease("message-1").is_none());
        assert_eq!(
            reloaded.snapshot().room_checkpoint("room-a"),
            Some(&RoomCheckpoint::new("room-a", ["message-1"]))
        );

        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("\"type\":\"attempt_leased\""));
        assert!(contents.contains("\"type\":\"processed_message\""));
        assert!(contents.contains("\"type\":\"room_checkpoint\""));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn attempt_lease_survives_restart_and_expires() {
        let path = temp_file("attempt-lease");
        let mut store = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();

        let _attempt = started_attempt(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        assert_eq!(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1001))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(59))
        );

        let mut reloaded = JsonlStateStore::load_at(path.clone(), ts(1001)).unwrap();
        assert_eq!(
            reloaded
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1001))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(59))
        );

        let mut after_expiry = JsonlStateStore::load_at(path.clone(), ts(1061)).unwrap();
        let _attempt = started_attempt(
            after_expiry
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1061))
                .unwrap(),
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn release_attempt_clears_lease_for_restart() {
        let path = temp_file("release-attempt");
        let mut store = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();

        let attempt = started_attempt(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        store.release_attempt_at(&attempt, ts(1001)).unwrap();

        let mut reloaded = JsonlStateStore::load_at(path.clone(), ts(1002)).unwrap();
        let _attempt = started_attempt(
            reloaded
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1002))
                .unwrap(),
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn processed_message_clears_existing_lease() {
        let path = temp_file("processed-clears-lease");
        let mut store = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();

        let attempt = started_attempt(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        store.mark_processed_at(&attempt, ts(1001)).unwrap();

        let mut reloaded = JsonlStateStore::load_at(path.clone(), ts(1002)).unwrap();
        assert_eq!(
            reloaded
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1002))
                .unwrap(),
            AttemptStart::Processed
        );
        assert!(reloaded.snapshot().attempt_lease("message-1").is_none());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn room_checkpoint_latest_record_wins() {
        let path = temp_file("checkpoint-latest-wins");
        let mut store = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();

        store
            .save_room_checkpoint_at(RoomCheckpoint::new("room-a", ["old"]), ts(1000))
            .unwrap();
        store
            .save_room_checkpoint_at(RoomCheckpoint::known_empty("room-b"), ts(1001))
            .unwrap();
        store
            .save_room_checkpoint_at(RoomCheckpoint::new("room-a", ["new"]), ts(1002))
            .unwrap();

        let reloaded = JsonlStateStore::load_at(path.clone(), ts(1003)).unwrap();
        assert_eq!(
            reloaded.snapshot().room_checkpoint("room-a"),
            Some(&RoomCheckpoint::new("room-a", ["new"]))
        );
        assert_eq!(
            reloaded.snapshot().room_checkpoint("room-b"),
            Some(&RoomCheckpoint::known_empty("room-b"))
        );
        assert_eq!(reloaded.snapshot().room_checkpoints().count(), 2);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_jsonl_record_reports_line_number() {
        let path = temp_file("invalid-jsonl");
        fs::write(
            &path,
            "{\"version\":1,\"type\":\"processed_message\"}\nnot json\n",
        )
        .unwrap();

        let error = StateSnapshot::load_at(&path, ts(1000)).unwrap_err();
        assert!(
            error.to_string().contains(":1:"),
            "unexpected error: {error}"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn empty_message_id_is_rejected_before_append() {
        let path = temp_file("empty-message-id");
        let mut store = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();

        let error = store
            .begin_attempt_at(" ", Duration::from_secs(60), ts(1000))
            .unwrap_err();
        assert!(error.to_string().contains("message id is empty"));
        assert!(!path.exists());
    }

    #[test]
    fn empty_room_checkpoint_id_is_rejected_before_append() {
        let path = temp_file("empty-room-id");
        let mut store = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();

        let error = store
            .save_room_checkpoint_at(RoomCheckpoint::new(" ", ["message-1"]), ts(1000))
            .unwrap_err();
        assert!(error.to_string().contains("room checkpoint id is empty"));
        assert!(!path.exists());
    }

    #[test]
    fn invalid_jsonl_syntax_reports_line_number() {
        let path = temp_file("invalid-jsonl-syntax");
        let first = serde_json::to_string(&StateRecord::new(StateEvent::ProcessedMessage {
            message_id: "message-1".to_owned(),
            processed_at: ts(1000),
        }))
        .unwrap();
        fs::write(&path, format!("{first}\nnot json\n")).unwrap();

        let error = StateSnapshot::load_at(&path, ts(1000)).unwrap_err();
        assert!(
            error.to_string().contains(":2:"),
            "unexpected error: {error}"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn snapshot_load_ignores_torn_trailing_record() {
        let path = temp_file("snapshot-torn-tail");
        let first = serde_json::to_string(&StateRecord::new(StateEvent::ProcessedMessage {
            message_id: "message-1".to_owned(),
            processed_at: ts(1000),
        }))
        .unwrap();
        fs::write(
            &path,
            format!(
                "{first}\n{{\"version\":1,\"type\":\"attempt_leased\",\"messageId\":\"message-2\""
            ),
        )
        .unwrap();

        let snapshot = StateSnapshot::load_at(&path, ts(1001)).unwrap();
        assert!(snapshot.contains_processed_message("message-1"));
        assert!(snapshot.attempt_lease("message-2").is_none());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn store_load_truncates_torn_tail_before_next_append() {
        let path = temp_file("store-truncates-torn-tail");
        let first = serde_json::to_string(&StateRecord::new(StateEvent::ProcessedMessage {
            message_id: "message-1".to_owned(),
            processed_at: ts(1000),
        }))
        .unwrap();
        fs::write(&path, format!("{first}\n{{\"version\":1,\"type\":")).unwrap();
        let len_before = fs::metadata(&path).unwrap().len();

        let mut store = JsonlStateStore::load_at(path.clone(), ts(1001)).unwrap();
        assert!(fs::metadata(&path).unwrap().len() < len_before);
        let attempt = started_attempt(
            store
                .begin_attempt_at("message-2", Duration::from_secs(60), ts(1002))
                .unwrap(),
        );
        store.mark_processed_at(&attempt, ts(1002)).unwrap();

        let reloaded = JsonlStateStore::load_at(path.clone(), ts(1003)).unwrap();
        assert!(reloaded.contains_processed_message("message-1"));
        assert!(reloaded.contains_processed_message("message-2"));
        let contents = fs::read_to_string(&path).unwrap();
        for line in contents.lines() {
            serde_json::from_str::<StateRecord>(line).unwrap();
        }

        let _ = fs::remove_file(path);
    }

    fn state_lock_keys_overlap(left: &Path, right: &Path) -> bool {
        let left = state_lock_keys(left).unwrap();
        let right = state_lock_keys(right).unwrap();
        left.iter().any(|key| right.contains(key))
    }

    #[test]
    fn state_lock_keys_overlap_for_distinct_paths() {
        let first = temp_file("lock-key-distinct-first");
        let second = temp_file("lock-key-distinct-second");

        assert!(state_lock_keys_overlap(&first, &second));
    }

    #[test]
    fn state_lock_keys_overlap_before_and_after_file_creation() {
        let path = temp_file("lock-key-create-transition");
        let before = state_lock_keys(&path).unwrap();
        File::create(&path).unwrap();
        let after = state_lock_keys(&path).unwrap();

        assert!(
            before.iter().any(|key| after.contains(key)),
            "lock key sets should overlap across first file creation: before={before:?}, after={after:?}"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn begin_attempt_reloads_before_claiming_stale_snapshot() {
        let path = temp_file("stale-begin-attempt");
        let mut first = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();
        let mut second = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();

        let _attempt = started_attempt(
            first
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        assert_eq!(
            second
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(60))
        );

        let _ = fs::remove_file(path);
    }

    #[cfg(unix)]
    #[test]
    fn begin_attempt_uses_same_lock_for_hard_link_aliases() {
        let path = temp_file("hard-link-alias");
        File::create(&path).unwrap();
        let alias = path.with_extension("hardlink.jsonl");
        fs::hard_link(&path, &alias).unwrap();
        let mut first = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();
        let mut second = JsonlStateStore::load_at(alias.clone(), ts(1000)).unwrap();

        assert!(state_lock_keys_overlap(&path, &alias));
        let _attempt = started_attempt(
            first
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        assert_eq!(
            second
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(60))
        );

        let _ = fs::remove_file(alias);
        let _ = fs::remove_file(path);
    }

    #[cfg(unix)]
    #[test]
    fn begin_attempt_resolves_dangling_symlink_before_target_creation() {
        let path = temp_file("dangling-symlink-target");
        let alias = path.with_extension("symlink.jsonl");
        std::os::unix::fs::symlink(path.file_name().unwrap(), &alias).unwrap();
        let mut first = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();
        let mut second = JsonlStateStore::load_at(alias.clone(), ts(1000)).unwrap();

        assert_eq!(first.path(), path.as_path());
        assert_eq!(second.path(), path.as_path());
        assert!(state_lock_keys_overlap(&path, &alias));
        let _attempt = started_attempt(
            first
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        assert_eq!(
            second
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(60))
        );

        let _ = fs::remove_file(alias);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn begin_attempt_uses_same_lock_for_lexical_path_aliases() {
        let path = temp_file("alias-begin-attempt");
        let alias = path
            .parent()
            .unwrap()
            .join(".")
            .join(path.file_name().unwrap());
        let mut first = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();
        let mut second = JsonlStateStore::load_at(alias, ts(1000)).unwrap();

        assert_eq!(first.path(), path.as_path());
        assert_eq!(second.path(), path.as_path());
        let _attempt = started_attempt(
            first
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        assert_eq!(
            second
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(60))
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn snapshot_attempt_status_does_not_grant_attempt_ownership() {
        let path = temp_file("snapshot-status-no-token");
        let mut owner = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();
        let attempt = started_attempt(
            owner
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        let mut observer = JsonlStateStore::load_at(path.clone(), ts(1001)).unwrap();

        let status = observer.snapshot().attempt_lease("message-1").unwrap();
        assert_eq!(status.message_id(), "message-1");
        assert_eq!(status.expires_at(), attempt.expires_at());
        assert_eq!(observer.snapshot().attempt_leases().count(), 1);
        assert_eq!(
            observer
                .begin_attempt_at(status.message_id(), Duration::from_secs(60), ts(1002))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(58))
        );

        owner.mark_processed_at(&attempt, ts(1003)).unwrap();
        assert!(
            JsonlStateStore::load_at(path.clone(), ts(1004))
                .unwrap()
                .contains_processed_message("message-1")
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn non_owner_attempt_token_cannot_release_or_defer_active_lease() {
        let path = temp_file("non-owner-token");
        let mut owner = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();
        let attempt = started_attempt(
            owner
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        let mut forged = attempt.clone();
        forged.attempt_id.push_str("-forged");
        let mut other = JsonlStateStore::load_at(path.clone(), ts(1001)).unwrap();

        assert!(other.release_attempt_at(&forged, ts(1001)).is_err());
        assert!(
            other
                .defer_attempt_at(&forged, Duration::from_secs(1), ts(1001))
                .is_err()
        );
        assert_eq!(
            other
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1002))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(58))
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn expired_attempt_token_cannot_release_newer_owner_lease() {
        let path = temp_file("expired-owner-token");
        let mut first = JsonlStateStore::load_at(path.clone(), ts(1000)).unwrap();
        let old_attempt = started_attempt(
            first
                .begin_attempt_at("message-1", Duration::from_secs(1), ts(1000))
                .unwrap(),
        );
        let mut second = JsonlStateStore::load_at(path.clone(), ts(1001)).unwrap();
        let _new_attempt = started_attempt(
            second
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1001))
                .unwrap(),
        );

        assert!(first.release_attempt_at(&old_attempt, ts(1002)).is_err());
        assert_eq!(
            first
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1002))
                .unwrap(),
            AttemptStart::Leased(Duration::from_secs(59))
        );

        let _ = fs::remove_file(path);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_repairs_existing_permissive_file() {
        use std::os::unix::fs::PermissionsExt as _;

        let db_path = temp_file("sqlite-cache-permissive-mode");
        File::create(&db_path).unwrap();
        fs::set_permissions(&db_path, fs::Permissions::from_mode(0o644)).unwrap();

        let _cache = SqliteStateCache::open(&db_path).unwrap();

        assert_eq!(
            fs::metadata(&db_path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let _ = fs::remove_file(db_path);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_repairs_existing_read_only_permissive_file() {
        use std::os::unix::fs::PermissionsExt as _;

        let db_path = temp_file("sqlite-cache-readonly-mode");
        File::create(&db_path).unwrap();
        fs::set_permissions(&db_path, fs::Permissions::from_mode(0o444)).unwrap();

        let _cache = SqliteStateCache::open(&db_path).unwrap();

        assert_eq!(
            fs::metadata(&db_path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let _ = fs::remove_file(db_path);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_creates_private_parent_directory() {
        use std::os::unix::fs::PermissionsExt as _;

        let parent = temp_file("sqlite-cache-private-parent");
        let db_path = parent.join("cache.db");

        let _cache = SqliteStateCache::open(&db_path).unwrap();

        assert_eq!(
            fs::metadata(&parent).unwrap().permissions().mode() & 0o777,
            0o700
        );

        let _ = fs::remove_file(db_path);
        let _ = fs::remove_dir(parent);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_rejects_unsafe_parent_directory() {
        use std::os::unix::fs::PermissionsExt as _;

        let parent = temp_file("sqlite-cache-unsafe-parent");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o777)).unwrap();
        let db_path = parent.join("cache.db");

        assert!(SqliteStateCache::open(&db_path).is_err());
        assert!(!db_path.exists());
        assert_eq!(
            fs::symlink_metadata(&parent).unwrap().permissions().mode() & 0o777,
            0o777
        );

        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let _ = fs::remove_dir(parent);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_rejects_unsafe_ancestor_directory() {
        use std::os::unix::fs::PermissionsExt as _;

        let ancestor = temp_file("sqlite-cache-unsafe-ancestor");
        fs::create_dir(&ancestor).unwrap();
        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o777)).unwrap();
        let parent = ancestor.join("safe-parent");
        let db_path = parent.join("cache.db");

        assert!(SqliteStateCache::open(&db_path).is_err());
        assert!(!parent.exists());
        assert!(!db_path.exists());
        assert_eq!(
            fs::symlink_metadata(&ancestor)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o777
        );

        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o700)).unwrap();
        let _ = fs::remove_dir(ancestor);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_rejects_bare_relative_path_in_unsafe_cwd() {
        use std::os::unix::fs::PermissionsExt as _;

        let _guard = CWD_TEST_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap();
        let old_cwd = env::current_dir().unwrap();
        let cwd = temp_file("sqlite-cache-unsafe-cwd");
        fs::create_dir(&cwd).unwrap();
        fs::set_permissions(&cwd, fs::Permissions::from_mode(0o777)).unwrap();
        env::set_current_dir(&cwd).unwrap();

        let result = SqliteStateCache::open(Path::new("cache.db"));
        let cache_exists = Path::new("cache.db").exists();

        env::set_current_dir(&old_cwd).unwrap();
        assert!(result.is_err());
        assert!(!cache_exists);

        fs::set_permissions(&cwd, fs::Permissions::from_mode(0o700)).unwrap();
        let _ = fs::remove_dir(cwd);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_rejects_existing_directory_without_chmod() {
        use std::os::unix::fs::PermissionsExt as _;

        let db_path = temp_file("sqlite-cache-directory");
        fs::create_dir(&db_path).unwrap();
        fs::set_permissions(&db_path, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(SqliteStateCache::open(&db_path).is_err());
        assert_eq!(
            fs::symlink_metadata(&db_path).unwrap().permissions().mode() & 0o777,
            0o755
        );

        fs::set_permissions(&db_path, fs::Permissions::from_mode(0o700)).unwrap();
        let _ = fs::remove_dir(db_path);
    }

    #[cfg(feature = "sqlite-state-cache")]
    #[test]
    fn sqlite_state_cache_rejects_uri_style_path() {
        let target_path = temp_file("sqlite-cache-uri-target");
        let uri_path = PathBuf::from(format!("file:{}?mode=rwc", target_path.display()));

        assert!(SqliteStateCache::open(&uri_path).is_err());
        assert!(!target_path.exists());
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_sqlite_open_rejects_symlink() {
        use std::os::unix::fs::PermissionsExt as _;

        let target_path = temp_file("sqlite-cache-open-symlink-target");
        let link_path = temp_file("sqlite-cache-open-symlink-link");
        File::create(&target_path).unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o644)).unwrap();
        std::os::unix::fs::symlink(&target_path, &link_path).unwrap();

        assert!(open_sqlite_cache_connection(&link_path).is_err());
        assert_eq!(
            fs::metadata(&target_path).unwrap().permissions().mode() & 0o777,
            0o644
        );

        let _ = fs::remove_file(link_path);
        let _ = fs::remove_file(target_path);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_rejects_symlink_without_chmod_target() {
        use std::os::unix::fs::PermissionsExt as _;

        let target_path = temp_file("sqlite-cache-symlink-target");
        let link_path = temp_file("sqlite-cache-symlink-link");
        File::create(&target_path).unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o644)).unwrap();
        std::os::unix::fs::symlink(&target_path, &link_path).unwrap();

        assert!(SqliteStateCache::open(&link_path).is_err());
        assert_eq!(
            fs::metadata(&target_path).unwrap().permissions().mode() & 0o777,
            0o644
        );

        let _ = fs::remove_file(link_path);
        let _ = fs::remove_file(target_path);
    }

    #[cfg(all(unix, feature = "sqlite-state-cache"))]
    #[test]
    fn sqlite_state_cache_created_private_by_default() {
        use std::os::unix::fs::PermissionsExt as _;

        let db_path = temp_file("sqlite-cache-mode");
        let _cache = SqliteStateCache::open(&db_path).unwrap();

        assert_eq!(
            fs::metadata(&db_path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let _ = fs::remove_file(db_path);
    }

    #[cfg(feature = "sqlite-state-cache")]
    #[test]
    fn sqlite_state_cache_rebuilds_from_store_snapshot() {
        let state_path = temp_file("sqlite-cache-state");
        let db_path = temp_file("sqlite-cache-db");
        let mut store = JsonlStateStore::load_at(state_path.clone(), ts(1000)).unwrap();
        let attempt = started_attempt(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        store.mark_processed_at(&attempt, ts(1001)).unwrap();
        store
            .save_room_checkpoint_at(RoomCheckpoint::new("room-a", ["message-1"]), ts(1002))
            .unwrap();
        store
            .save_room_checkpoint_at(RoomCheckpoint::known_empty("room-b"), ts(1003))
            .unwrap();

        let mut cache = SqliteStateCache::open(&db_path).unwrap();
        cache.rebuild_from_store(&store).unwrap();

        assert!(cache.contains_processed_message("message-1").unwrap());
        assert!(!cache.contains_processed_message("message-2").unwrap());
        assert_eq!(cache.processed_message_count().unwrap(), 1);
        assert_eq!(
            cache.room_checkpoint("room-a").unwrap(),
            Some(RoomCheckpoint::new("room-a", ["message-1"]))
        );
        assert_eq!(
            cache.room_checkpoint("room-b").unwrap(),
            Some(RoomCheckpoint::known_empty("room-b"))
        );
        assert_eq!(cache.room_checkpoints().unwrap().len(), 2);

        let _ = fs::remove_file(state_path);
        let _ = fs::remove_file(db_path);
    }

    #[cfg(feature = "sqlite-state-cache")]
    #[test]
    fn sqlite_state_cache_rebuild_replaces_stale_index() {
        let mut cache = SqliteStateCache::open_in_memory().unwrap();
        let state_path = temp_file("sqlite-cache-rebuild-jsonl");
        let mut store = JsonlStateStore::load_at(state_path.clone(), ts(1000)).unwrap();
        let attempt = started_attempt(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        store.mark_processed_at(&attempt, ts(1001)).unwrap();
        store
            .save_room_checkpoint_at(RoomCheckpoint::new("room-a", ["message-1"]), ts(1002))
            .unwrap();

        cache.rebuild_from_jsonl(&state_path).unwrap();
        assert!(cache.contains_processed_message("message-1").unwrap());
        assert!(cache.room_checkpoint("room-a").unwrap().is_some());

        cache
            .rebuild_from_snapshot(&StateSnapshot::default())
            .unwrap();
        assert!(!cache.contains_processed_message("message-1").unwrap());
        assert_eq!(cache.processed_message_count().unwrap(), 0);
        assert!(cache.room_checkpoint("room-a").unwrap().is_none());
        assert!(cache.room_checkpoints().unwrap().is_empty());

        let _ = fs::remove_file(state_path);
    }

    #[cfg(feature = "sqlite-state-cache")]
    #[test]
    fn sqlite_state_cache_rebuild_from_store_reloads_latest_jsonl() {
        let state_path = temp_file("sqlite-cache-stale-store");
        let db_path = temp_file("sqlite-cache-stale-store-db");
        let stale_store = JsonlStateStore::load_at(state_path.clone(), ts(1000)).unwrap();
        let mut writer = JsonlStateStore::load_at(state_path.clone(), ts(1000)).unwrap();
        let attempt = started_attempt(
            writer
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        writer.mark_processed_at(&attempt, ts(1001)).unwrap();

        let mut cache = SqliteStateCache::open(&db_path).unwrap();
        cache.rebuild_from_store(&stale_store).unwrap();

        assert!(cache.contains_processed_message("message-1").unwrap());
        assert_eq!(cache.processed_message_count().unwrap(), 1);

        let _ = fs::remove_file(state_path);
        let _ = fs::remove_file(db_path);
    }

    #[cfg(feature = "sqlite-state-cache")]
    #[test]
    fn sqlite_state_cache_rebuild_from_jsonl_waits_for_state_path_lock() {
        let state_path = temp_file("sqlite-cache-locked-state");
        let mut store = JsonlStateStore::load_at(state_path.clone(), ts(1000)).unwrap();
        let attempt = started_attempt(
            store
                .begin_attempt_at("message-1", Duration::from_secs(60), ts(1000))
                .unwrap(),
        );
        store.mark_processed_at(&attempt, ts(1001)).unwrap();

        let stable_path = stable_state_path(&state_path).unwrap();
        let guard = StatePathLock::acquire(&stable_path).unwrap();
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (done_sender, done_receiver) = std::sync::mpsc::channel();
        let thread_state_path = state_path.clone();
        let handle = std::thread::spawn(move || {
            let mut cache = SqliteStateCache::open_in_memory().unwrap();
            started_sender.send(()).unwrap();
            cache.rebuild_from_jsonl(&thread_state_path).unwrap();
            done_sender
                .send(cache.contains_processed_message("message-1").unwrap())
                .unwrap();
        });

        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(
            done_receiver
                .recv_timeout(Duration::from_millis(200))
                .is_err()
        );

        drop(guard);

        assert!(done_receiver.recv_timeout(Duration::from_secs(2)).unwrap());
        handle.join().unwrap();

        let _ = fs::remove_file(state_path);
    }

    #[test]
    fn state_record_uses_versioned_camel_case_json_shape() {
        let record = StateRecord::new(StateEvent::ProcessedMessage {
            message_id: "message-1".to_owned(),
            processed_at: ts(1000),
        });

        let value = serde_json::to_value(record).unwrap();
        assert_eq!(
            value,
            json!({
                "version": 1,
                "type": "processed_message",
                "messageId": "message-1",
                "processedAt": "1970-01-01T00:16:40Z"
            })
        );
    }
}
