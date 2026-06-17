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

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};

use chrono::{DateTime, Utc};
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
