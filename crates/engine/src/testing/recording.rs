//! A [`Backend`] that forwards to a [`MockBackend`] and records what it
//! applied.
//!
//! It can freeze forever after a chosen operation, or the delete of a chosen
//! path, was applied, so that dropping the runtime kills the daemon right
//! after it, pause after it until released, or fail the delete of a chosen
//! path.

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::time::Instant;

use super::mock_backend::{Fault, MockBackend, MockOp};
use crate::backend::{Backend, TransientError};
use crate::types::{Change, Cursor, FileEntry, FileId, Quota, Revision};

/// A temporary upload the backend applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    pub path: PathBuf,
    pub data: Vec<u8>,
    pub file_id: String,
}

/// A rename-over the backend applied, with the content it placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub source: PathBuf,
    pub target: PathBuf,
    pub data: Vec<u8>,
}

#[derive(Debug, Default)]
struct Entries {
    uploads: Vec<Upload>,
    placed: Vec<Placed>,
    calls: HashMap<MockOp, usize>,
    replace_attempts: Vec<Instant>,
    fetched: Vec<Range<u64>>,
    deleted: Vec<String>,
    uploading: usize,
    max_uploading: usize,
}

/// Effects on the backend, shared by every daemon run of one test.
#[derive(Debug, Clone, Default)]
pub struct BackendLog(Arc<Mutex<Entries>>);

impl BackendLog {
    fn entries(&self) -> MutexGuard<'_, Entries> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Temporary uploads the backend applied.
    #[must_use]
    pub fn uploads(&self) -> Vec<Upload> {
        self.entries().uploads.clone()
    }

    /// Rename-overs the backend applied.
    #[must_use]
    pub fn placed(&self) -> Vec<Placed> {
        self.entries().placed.clone()
    }

    /// Calls of `op`, including failed ones.
    #[must_use]
    pub fn calls(&self, op: MockOp) -> usize {
        self.entries().calls.get(&op).copied().unwrap_or(0)
    }

    /// Start of every rename-over attempt.
    #[must_use]
    pub fn replace_attempts(&self) -> Vec<Instant> {
        self.entries().replace_attempts.clone()
    }

    /// Byte ranges fetched; a whole download is `0..u64::MAX`.
    #[must_use]
    pub fn fetched(&self) -> Vec<Range<u64>> {
        self.entries().fetched.clone()
    }

    /// Paths of the deletes the backend applied, in order.
    #[must_use]
    pub fn deleted(&self) -> Vec<String> {
        self.entries().deleted.clone()
    }

    /// The most uploads seen in flight at once.
    #[must_use]
    pub fn max_uploading(&self) -> usize {
        self.entries().max_uploading
    }
}

/// Holds a call after it was applied, until released.
#[derive(Debug, Default)]
pub struct Hold {
    pub reached: Notify,
    pub released: Notify,
}

/// Forwards to a [`MockBackend`] and records into a [`BackendLog`].
#[derive(Debug)]
pub struct RecordingBackend {
    inner: Arc<MockBackend>,
    id: String,
    log: BackendLog,
    freeze_after: Option<MockOp>,
    freeze_after_delete_of: Mutex<Option<String>>,
    frozen: Notify,
    pause_after: Mutex<Option<(MockOp, Arc<Hold>)>>,
    fail_reply_after: Mutex<Option<MockOp>>,
    fail_delete_of: Mutex<Vec<(String, Fault)>>,
}

impl RecordingBackend {
    fn with(
        inner: &Arc<MockBackend>,
        log: &BackendLog,
        id: &str,
        freeze: Option<MockOp>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::clone(inner),
            id: id.to_owned(),
            log: log.clone(),
            freeze_after: freeze,
            freeze_after_delete_of: Mutex::new(None),
            frozen: Notify::new(),
            pause_after: Mutex::new(None),
            fail_reply_after: Mutex::new(None),
            fail_delete_of: Mutex::new(Vec::new()),
        })
    }

    /// Forwards to `inner` under its id.
    #[must_use]
    pub fn new(inner: &Arc<MockBackend>, log: &BackendLog) -> Arc<Self> {
        Self::with(inner, log, inner.id(), None)
    }

    /// Forwards to `inner` under the backend id `id`.
    #[must_use]
    pub fn named(inner: &Arc<MockBackend>, log: &BackendLog, id: &str) -> Arc<Self> {
        Self::with(inner, log, id, None)
    }

    /// Freezes forever once a call of `op` was applied.
    #[must_use]
    pub fn freezing_after(inner: &Arc<MockBackend>, log: &BackendLog, op: MockOp) -> Arc<Self> {
        Self::with(inner, log, inner.id(), Some(op))
    }

    /// Freezes forever once the next delete of the root-relative `path` was
    /// applied.
    pub fn freeze_after_delete_of(&self, path: &str) {
        *self
            .freeze_after_delete_of
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(path.to_owned());
    }

    /// The log this backend records into.
    #[must_use]
    pub const fn log(&self) -> &BackendLog {
        &self.log
    }

    /// Waits until the backend froze.
    pub async fn frozen(&self) {
        self.frozen.notified().await;
    }

    /// Pauses the next applied call of `op` until the returned hold is
    /// released.
    #[must_use]
    pub fn pause_after(&self, op: MockOp) -> Arc<Hold> {
        let hold = Arc::new(Hold::default());
        *self
            .pause_after
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((op, Arc::clone(&hold)));
        hold
    }

    /// Reports the next applied call of `op` as unreachable, as a reply lost
    /// after the backend applied it.
    pub fn fail_reply_after(&self, op: MockOp) {
        *self
            .fail_reply_after
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(op);
    }

    /// Makes the next delete of the root-relative `path` take `fault`
    /// before it reaches the backend.
    pub fn fail_delete_of(&self, path: &str, fault: Fault) {
        self.fail_delete_of
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((path.to_owned(), fault));
    }

    async fn delete_fault(&self, path: &str) -> anyhow::Result<()> {
        let fault = {
            let mut faults = self
                .fail_delete_of
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            faults
                .iter()
                .position(|(failing, _)| failing == path)
                .map(|at| faults.remove(at).1)
        };
        match fault {
            Some(Fault::Unreachable) => {
                Err(TransientError::Unreachable(format!("delete of {path}")).into())
            }
            Some(Fault::Busy) => Err(TransientError::Busy(format!("delete of {path}")).into()),
            Some(Fault::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn note(&self, op: MockOp) {
        *self.log.entries().calls.entry(op).or_default() += 1;
    }

    async fn applied(&self, op: MockOp) -> anyhow::Result<()> {
        if self.freeze_after == Some(op) {
            self.frozen.notify_one();
            std::future::pending::<()>().await;
        }
        let hold = {
            let mut pause = self
                .pause_after
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if pause.as_ref().is_some_and(|(paused, _)| *paused == op) {
                pause.take().map(|(_, hold)| hold)
            } else {
                None
            }
        };
        if let Some(hold) = hold {
            hold.reached.notify_one();
            hold.released.notified().await;
        }
        let reply_lost = self
            .fail_reply_after
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_if(|failing| *failing == op)
            .is_some();
        if reply_lost {
            return Err(TransientError::Unreachable(format!("reply of {op:?}")).into());
        }
        Ok(())
    }
}

#[async_trait]
impl Backend for RecordingBackend {
    fn id(&self) -> &str {
        &self.id
    }

    fn display_name(&self) -> &str {
        self.inner.display_name()
    }

    async fn quota(&self) -> anyhow::Result<Option<Quota>> {
        self.inner.quota().await
    }

    async fn changes(&self, cursor: Option<&Cursor>) -> anyhow::Result<(Vec<Change>, Cursor)> {
        self.note(MockOp::Changes);
        self.inner.changes(cursor).await
    }

    async fn metadata(&self, path: &Path) -> anyhow::Result<FileEntry> {
        self.note(MockOp::Metadata);
        self.inner.metadata(path).await
    }

    async fn download(&self, file: &FileEntry) -> anyhow::Result<Vec<u8>> {
        self.note(MockOp::Download);
        self.log.entries().fetched.push(0..u64::MAX);
        self.inner.download(file).await
    }

    async fn read_range(
        &self,
        file: &FileEntry,
        offset: u64,
        length: u32,
    ) -> anyhow::Result<Vec<u8>> {
        self.note(MockOp::Download);
        let bytes = self.inner.read_range(file, offset, length).await?;
        let len = u64::try_from(bytes.len())?;
        self.log
            .entries()
            .fetched
            .push(offset..offset.saturating_add(len));
        Ok(bytes)
    }

    async fn upload(
        &self,
        path: &Path,
        data: &[u8],
        parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        self.note(MockOp::Upload);
        {
            let mut entries = self.log.entries();
            entries.uploading += 1;
            entries.max_uploading = entries.max_uploading.max(entries.uploading);
        }
        let entry = self.inner.upload(path, data, parent_id).await;
        self.log.entries().uploading -= 1;
        let entry = entry?;
        self.log.entries().uploads.push(Upload {
            path: path.to_path_buf(),
            data: data.to_vec(),
            file_id: entry.id.native_id().to_owned(),
        });
        self.applied(MockOp::Upload).await?;
        Ok(entry)
    }

    async fn update(&self, file_id: &FileId, data: &[u8]) -> anyhow::Result<FileEntry> {
        self.note(MockOp::Update);
        self.inner.update(file_id, data).await
    }

    async fn create_dir(&self, path: &Path) -> anyhow::Result<FileEntry> {
        self.note(MockOp::CreateDir);
        self.inner.create_dir(path).await
    }

    async fn create_dir_with_parent(
        &self,
        name: &str,
        parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        self.note(MockOp::CreateDir);
        self.inner.create_dir_with_parent(name, parent_id).await
    }

    async fn delete(&self, file: &FileEntry) -> anyhow::Result<()> {
        self.note(MockOp::Delete);
        self.delete_fault(&file.path).await?;
        self.inner.delete(file).await?;
        self.log.entries().deleted.push(file.path.clone());
        let freezes = self
            .freeze_after_delete_of
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_if(|freezing| *freezing == file.path)
            .is_some();
        if freezes {
            self.frozen.notify_one();
            std::future::pending::<()>().await;
        }
        self.applied(MockOp::Delete).await?;
        Ok(())
    }

    async fn move_entry(&self, src: &Path, dst: &Path) -> anyhow::Result<FileEntry> {
        self.note(MockOp::Move);
        self.inner.move_entry(src, dst).await
    }

    async fn move_by_id(
        &self,
        src_id: &FileId,
        dst_parent_id: &FileId,
        new_name: &str,
    ) -> anyhow::Result<FileEntry> {
        self.note(MockOp::Move);
        self.inner.move_by_id(src_id, dst_parent_id, new_name).await
    }

    async fn list_children(&self, parent_native_id: &str) -> anyhow::Result<Vec<FileEntry>> {
        self.note(MockOp::ListChildren);
        let children = self.inner.list_children(parent_native_id).await?;
        self.applied(MockOp::ListChildren).await?;
        Ok(children)
    }

    async fn revision(&self, entry: &FileEntry) -> anyhow::Result<Revision> {
        self.note(MockOp::Revision);
        self.inner.revision(entry).await
    }

    async fn replace(&self, source: &Path, target: &Path) -> anyhow::Result<(FileEntry, Revision)> {
        self.note(MockOp::Replace);
        self.log.entries().replace_attempts.push(Instant::now());
        let replaced = self.inner.replace(source, target).await?;
        let data = self.inner.content(target).unwrap_or_default();
        self.log.entries().placed.push(Placed {
            source: source.to_path_buf(),
            target: target.to_path_buf(),
            data,
        });
        self.applied(MockOp::Replace).await?;
        Ok(replaced)
    }

    async fn poll_interval(&self) -> Option<Duration> {
        self.inner.poll_interval().await
    }
}
