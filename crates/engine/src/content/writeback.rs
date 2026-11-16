//! Write-back of saved edits, after rclone's `vfs/vfscache/item.go` and
//! `vfs/vfscache/writeback/writeback.go`.
//!
//! A save is owned by the daemon before its reply: the content moves into a
//! dirty object and the item takes a new content version. State lives on the
//! object's `dirty` flag; an in-memory timer list, rebuilt from the flags at
//! start, uploads each dirty object once its delay expires. A save during an
//! upload restarts it with the new content; a failed upload is retried with a
//! doubling delay capped at five minutes. The local edit wins: the upload
//! replaces whatever the backend holds and recreates a deleted file.
//!
//! Uploads go to `.cascade-tmp-<uid>` beside the target and are renamed over
//! it. The object records the temporary name and file id before the rename,
//! so start-up recovery recognises a finished rename by file id and re-links
//! the uid without a second upload.
//!
//! The record, the rename-over and the bookkeeping after it run under the
//! item's operation lock, which saves, renames and deletes of the item also
//! take; a record therefore always describes the object's current content.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use anyhow::{Result, anyhow};
use chrono::Utc;
use tokio::sync::{Notify, OwnedMutexGuard, Semaphore, watch};
use tokio::time::Instant;

use super::cache::ContentCache;
use super::is_not_found;
use super::names::check_name;
use super::replica::{ItemKind, Replica, ReplicaItem, TEMP_UPLOAD_PREFIX, is_temp_upload};
use crate::backend::BackendError;
use crate::db::content::{ContentObject, NewContentObject, UploadRecord};
use crate::types::{FileId, ItemId};

/// Delay between a save and its upload, as rclone's `--vfs-write-back`.
pub const WRITE_BACK_DELAY: Duration = Duration::from_secs(5);

/// Upper bound of the doubling retry delay.
pub const MAX_UPLOAD_DELAY: Duration = Duration::from_mins(5);

/// Write-back state of an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteBackState {
    /// The backend holds the presented content.
    Clean,
    /// A dirty object waits for its timer.
    Dirty,
    /// NFS writes edit an object in place; the edit is not yet a save.
    Editing,
    /// The dirty object is being uploaded to its temporary name.
    Uploading,
    /// The temporary name and file id are recorded; the rename-over is due.
    Recorded,
    /// The rename-over finished; the object is not yet marked clean.
    Renamed,
    /// The item is marked as being deleted; its backend delete is due or in
    /// flight and its objects are kept.
    Deleting,
    /// The item and its objects are gone.
    Deleted,
}

/// Event that moves an item between write-back states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteBackEvent {
    /// A save is accepted into a new dirty object.
    Save,
    /// An NFS write or size change goes into the item's edited object.
    Write,
    /// The item's timer expires and an upload starts.
    TimerDue,
    /// The upload to the temporary name fails.
    UploadFailed,
    /// The upload to the temporary name finishes and is recorded.
    TempUploaded,
    /// The rename-over succeeds.
    RenameOver,
    /// The rename-over fails, for instance busy or unreachable.
    RenameFailed,
    /// The object is marked clean and the uid re-linked.
    Bookkeeping,
    /// A direct rename of the item within its folder.
    Rename,
    /// A direct move of the item into another folder.
    Reparent,
    /// A direct delete of the item or of a folder above it starts.
    Delete,
    /// The backend delete returns and the local state is removed.
    RemoteDeleted,
    /// The backend delete fails; the mark is cleared.
    DeleteFailed,
    /// A scan lists the item's folder.
    Scan,
    /// The daemon is killed and restarts with recovery.
    Restart,
}

/// One row of [`TRANSITIONS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Transition {
    pub from: WriteBackState,
    pub event: WriteBackEvent,
    pub to: WriteBackState,
}

const fn row(from: WriteBackState, event: WriteBackEvent, to: WriteBackState) -> Transition {
    Transition { from, event, to }
}

/// Every write-back transition. A kill at a transition is a
/// [`WriteBackEvent::Restart`] from its source or target state.
///
/// - A save always lands in a new dirty object; an upload that finds its
///   object superseded removes its temporary file.
/// - A write into an item without an edited object starts one from a clone
///   of its current object, which supersedes that object; further writes go
///   into the same object. The timer of an edited object fetches its missing
///   ranges and accepts it as a save before the upload; a fetch that fails
///   leaves the edit in place for the next timer.
/// - A save after a failed rename-over leaves the record with the superseded
///   object; the next upload removes the stale temporary file.
/// - Rename and scan follow name and parent by stable id and mint no content
///   version; the upload resolves its target under the operation lock.
/// - A reparent removes the temporary file of a dirty object from the folder
///   the item leaves and clears its record; an upload that finds its item
///   moved removes its temporary file and starts again.
/// - A delete keeps the objects until the backend delete returned; write-back
///   skips a marked item and recovery finishes a marked delete.
/// - Recovery finishes a rename it finds under the target name by file id,
///   resumes a recorded temporary file that still exists and removes every
///   other temporary file in the scan set and the folders of dirty objects.
/// - A timer after a failed rename-over settles the record the same way: a
///   rename that applied but whose reply failed is found under the target by
///   file id, and a temporary file that still exists is renamed over again.
pub const TRANSITIONS: &[Transition] = {
    use WriteBackEvent as E;
    use WriteBackState as S;
    &[
        row(S::Clean, E::Save, S::Dirty),
        row(S::Clean, E::Write, S::Editing),
        row(S::Clean, E::Rename, S::Clean),
        row(S::Clean, E::Reparent, S::Clean),
        row(S::Clean, E::Delete, S::Deleting),
        row(S::Clean, E::Scan, S::Clean),
        row(S::Clean, E::Restart, S::Clean),
        row(S::Dirty, E::Save, S::Dirty),
        row(S::Dirty, E::Write, S::Editing),
        row(S::Dirty, E::TimerDue, S::Uploading),
        row(S::Dirty, E::Rename, S::Dirty),
        row(S::Dirty, E::Reparent, S::Dirty),
        row(S::Dirty, E::Delete, S::Deleting),
        row(S::Dirty, E::Scan, S::Dirty),
        row(S::Dirty, E::Restart, S::Dirty),
        row(S::Editing, E::Write, S::Editing),
        row(S::Editing, E::Save, S::Dirty),
        row(S::Editing, E::TimerDue, S::Uploading),
        row(S::Editing, E::TimerDue, S::Editing),
        row(S::Editing, E::Rename, S::Editing),
        row(S::Editing, E::Reparent, S::Editing),
        row(S::Editing, E::Delete, S::Deleting),
        row(S::Editing, E::Scan, S::Editing),
        row(S::Editing, E::Restart, S::Editing),
        row(S::Uploading, E::Save, S::Dirty),
        row(S::Uploading, E::Write, S::Editing),
        row(S::Uploading, E::UploadFailed, S::Dirty),
        row(S::Uploading, E::TempUploaded, S::Recorded),
        row(S::Uploading, E::Rename, S::Uploading),
        row(S::Uploading, E::Reparent, S::Uploading),
        row(S::Uploading, E::Delete, S::Deleting),
        row(S::Uploading, E::Scan, S::Uploading),
        row(S::Uploading, E::Restart, S::Dirty),
        row(S::Recorded, E::Save, S::Dirty),
        row(S::Recorded, E::Write, S::Editing),
        row(S::Recorded, E::RenameOver, S::Renamed),
        row(S::Recorded, E::RenameFailed, S::Recorded),
        row(S::Recorded, E::Rename, S::Recorded),
        row(S::Recorded, E::Reparent, S::Dirty),
        row(S::Recorded, E::Delete, S::Deleting),
        row(S::Recorded, E::Scan, S::Recorded),
        row(S::Recorded, E::Restart, S::Recorded),
        row(S::Recorded, E::TimerDue, S::Renamed),
        row(S::Recorded, E::TimerDue, S::Recorded),
        row(S::Renamed, E::Bookkeeping, S::Clean),
        row(S::Renamed, E::Scan, S::Renamed),
        row(S::Renamed, E::Restart, S::Clean),
        row(S::Deleting, E::RemoteDeleted, S::Deleted),
        row(S::Deleting, E::DeleteFailed, S::Clean),
        row(S::Deleting, E::DeleteFailed, S::Dirty),
        row(S::Deleting, E::DeleteFailed, S::Editing),
        row(S::Deleting, E::Restart, S::Deleted),
        row(S::Deleted, E::Restart, S::Deleted),
    ]
};

/// Temporary upload name of an item.
fn temp_name(uid: u64) -> String {
    format!("{TEMP_UPLOAD_PREFIX}{uid}")
}

fn sibling(target: &Path, name: &str) -> PathBuf {
    target
        .parent()
        .map_or_else(|| PathBuf::from(name), |parent| parent.join(name))
}

pub(super) fn file_id(backend_id: &str, native_id: &str) -> FileId {
    FileId(ItemId::new(backend_id, native_id).0)
}

#[derive(Debug)]
struct Timer {
    due: Instant,
    delay: Duration,
    uploading: bool,
    resaved: bool,
}

/// Outcome of one upload attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Uploaded {
    /// The object is clean.
    Clean,
    /// A save replaced the object; its content is uploaded next.
    Superseded,
    /// The item has no dirty object.
    Gone,
}

/// What recovery found for a recorded upload.
enum Settled {
    /// The rename had happened; the object is clean.
    Clean,
    /// The temporary file is still there; the rename-over is due.
    Resume {
        object_id: super::ObjectId,
        temp: PathBuf,
    },
    /// Nothing recorded; the object needs a fresh upload.
    Pending,
}

type Locks = std::sync::Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>;

/// In-memory timer list of dirty items, their operation locks, the upload
/// permits and the latch recovery opens.
#[derive(Debug)]
pub(crate) struct WriteBack {
    timers: std::sync::Mutex<HashMap<u64, Timer>>,
    wake: Notify,
    locks: Locks,
    uploads: Semaphore,
    started: AtomicBool,
    recovered: watch::Sender<bool>,
}

/// An item's operation lock; its map entry is dropped on release when no
/// one else holds or waits for it.
pub(crate) struct Operation<'a> {
    locks: &'a Locks,
    uid: u64,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for Operation<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut locks = self.locks.lock().unwrap_or_else(PoisonError::into_inner);
        if locks
            .get(&self.uid)
            .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            locks.remove(&self.uid);
        }
    }
}

impl WriteBack {
    /// A timer list running at most `transfers` uploads at once.
    pub(crate) fn new(transfers: usize) -> Self {
        Self {
            timers: std::sync::Mutex::default(),
            wake: Notify::new(),
            locks: std::sync::Mutex::default(),
            uploads: Semaphore::new(transfers.max(1)),
            started: AtomicBool::new(false),
            recovered: watch::Sender::new(false),
        }
    }

    fn timers(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Timer>> {
        self.timers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits until start-up recovery finished.
    pub(crate) async fn recovered(&self) -> Result<()> {
        self.recovered.subscribe().wait_for(|done| *done).await?;
        Ok(())
    }

    /// Arms the item's timer, restarting it and resetting its delay. An
    /// upload in progress is superseded.
    pub(crate) fn add(&self, uid: u64) {
        let due = Instant::now() + WRITE_BACK_DELAY;
        self.timers()
            .entry(uid)
            .and_modify(|timer| {
                timer.due = due;
                timer.delay = WRITE_BACK_DELAY;
                timer.resaved = timer.uploading;
            })
            .or_insert(Timer {
                due,
                delay: WRITE_BACK_DELAY,
                uploading: false,
                resaved: false,
            });
        self.wake.notify_one();
    }

    /// Cancels the item's timer.
    pub(crate) fn remove(&self, uid: u64) {
        self.timers().remove(&uid);
    }

    /// The operation lock of an item.
    pub(crate) async fn lock(&self, uid: u64) -> Operation<'_> {
        let lock = Arc::clone(
            self.locks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(uid)
                .or_default(),
        );
        let mut operation = Operation {
            locks: &self.locks,
            uid,
            guard: None,
        };
        operation.guard = Some(lock.lock_owned().await);
        operation
    }

    /// Items whose timer expired, now marked uploading, and the next expiry.
    fn take_due(&self, now: Instant) -> (Vec<u64>, Option<Instant>) {
        let mut timers = self.timers();
        let mut due = Vec::new();
        let mut next = None::<Instant>;
        for (&uid, timer) in timers.iter_mut().filter(|(_, timer)| !timer.uploading) {
            if timer.due <= now {
                timer.uploading = true;
                due.push(uid);
            } else {
                next = Some(next.map_or(timer.due, |next| next.min(timer.due)));
            }
        }
        due.sort_unstable();
        (due, next)
    }

    /// Settles the timer after an upload attempt.
    fn finish(&self, uid: u64, outcome: &Result<Uploaded>) {
        let mut timers = self.timers();
        let Some(timer) = timers.get_mut(&uid) else {
            return;
        };
        timer.uploading = false;
        if std::mem::take(&mut timer.resaved) {
            drop(timers);
            self.wake.notify_one();
            return;
        }
        match outcome {
            Ok(Uploaded::Clean | Uploaded::Gone) => {
                timers.remove(&uid);
            }
            Ok(Uploaded::Superseded) => {
                timer.delay = WRITE_BACK_DELAY;
                timer.due = Instant::now() + WRITE_BACK_DELAY;
            }
            Err(_) => {
                timer.delay = timer.delay.saturating_mul(2).min(MAX_UPLOAD_DELAY);
                timer.due = Instant::now() + timer.delay;
            }
        }
        drop(timers);
        self.wake.notify_one();
    }
}

impl ContentCache {
    /// Takes ownership of the saved file at `contents` into a dirty object
    /// and commits a new content version before returning; the caller may
    /// delete the file once this returns. The item is uploaded later.
    ///
    /// # Errors
    ///
    /// Fails when the item is unknown or no file, its name is rejected by
    /// TR-10, or the file cannot be copied into the cache.
    pub async fn save(&self, uid: u64, contents: &Path) -> Result<ReplicaItem> {
        let operation = self.shared().writeback.lock(uid).await;
        self.save_locked(uid, contents, &operation).await
    }

    /// [`save`](Self::save) under the item's operation lock, held by the
    /// caller as `_operation`.
    pub(super) async fn save_locked(
        &self,
        uid: u64,
        contents: &Path,
        _operation: &Operation<'_>,
    ) -> Result<ReplicaItem> {
        let item = self
            .item(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        if item.kind != ItemKind::File {
            return Err(anyhow!("item {uid} is not a file"));
        }
        check_name(&item.name)?;
        let writeback = &self.shared().writeback;
        let slot = self.slot(uid);
        let mut current = slot.lock().await;
        if let Some(transfer) = current.take() {
            self.discard(&transfer, false).await?;
        }
        self.take_carried(uid);
        let object_id = self.db().insert_content_object(&NewContentObject {
            uid,
            content_version: "",
            source_revision: &item.remote_revision,
            size: 0,
        })?;
        let store = &self.shared().store;
        let placed = store.delete(object_id).and_then(|()| {
            reflink_copy::reflink_or_copy(contents, store.path(object_id))?;
            Ok(std::fs::metadata(store.path(object_id))?.len())
        });
        let size = match placed {
            Ok(size) => size,
            Err(error) => {
                store.remove(self.db(), object_id)?;
                return Err(error.into());
            }
        };
        let (saved, superseded) =
            self.db()
                .accept_content_save(uid, object_id, size, Utc::now())?;
        self.shared().eviction.touch(object_id);
        drop(current);
        for object in superseded {
            store.delete(object)?;
        }
        writeback.add(uid);
        Ok(saved)
    }

    /// Start-up step after [`register`](Self::register): finishes deletes
    /// marked before a kill, finishes uploads whose rename already happened,
    /// removes orphan temporary files from the scan set of every registered
    /// backend and the folders of dirty objects,
    /// lets [`scan`](Self::scan) proceed, then starts the write-back worker
    /// on the current runtime with a timer for every dirty object.
    ///
    /// A backend that cannot be reached leaves its objects to the worker.
    ///
    /// # Errors
    ///
    /// Fails when the state database fails.
    pub async fn recover(&self) -> Result<()> {
        let writeback = &self.shared().writeback;
        for uid in self.db().replica_deletes()? {
            if let Err(error) = self.resume_delete(uid).await {
                tracing::warn!(uid, error = %format!("{error:#}"), "delete deferred");
            }
        }
        let mut sweeps: BTreeMap<u64, HashSet<String>> = BTreeMap::new();
        for replica in self.replicas() {
            for folder in self.db().replica_scan_folders(replica.backend_id())? {
                sweeps.entry(folder).or_default();
            }
        }
        for object in self.db().dirty_content_objects()? {
            let uid = object.uid;
            if self.db().replica_item_deleting(uid)? {
                continue;
            }
            let Some(folder) = self.item(uid)?.and_then(|item| item.parent_uid) else {
                continue;
            };
            let kept = sweeps.entry(folder).or_default();
            match self.replica(uid) {
                Ok(replica) => {
                    let _operation = writeback.lock(uid).await;
                    match self.settle(&replica, uid).await {
                        Ok(Settled::Resume { temp, .. }) => {
                            kept.extend(temp.file_name().map(|n| n.to_string_lossy().into_owned()));
                        }
                        Ok(Settled::Clean | Settled::Pending) => {}
                        Err(error) => {
                            tracing::warn!(uid, error = %format!("{error:#}"), "recovery deferred");
                            kept.extend(object.upload.map(|record| record.temp_name));
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(uid, error = %format!("{error:#}"), "recovery skipped");
                    kept.extend(object.upload.map(|record| record.temp_name));
                }
            }
            if self.dirty_object(uid)?.is_some() {
                writeback.add(uid);
            }
        }
        for (folder, kept) in sweeps {
            if let Err(error) = self.sweep_temps(folder, &kept).await {
                tracing::warn!(folder, error = %format!("{error:#}"), "temporary files not removed");
            }
        }
        writeback.recovered.send_replace(true);
        if !writeback.started.swap(true, Ordering::AcqRel) {
            tokio::spawn(self.clone().run_write_back());
        }
        Ok(())
    }

    /// Finishes a delete whose mark survived a restart: the backend delete
    /// when the item is still there, then its local state.
    async fn resume_delete(&self, uid: u64) -> Result<()> {
        let _operation = self.shared().writeback.lock(uid).await;
        let Some(path) = self.item_path(uid)? else {
            return self.db().clear_replica_delete(uid);
        };
        let replica = self.replica(uid)?;
        self.remove_remote(&replica, &path).await?;
        self.finish_delete(&replica, uid, &path).await
    }

    /// The item's object awaiting upload: dirty or edited.
    fn dirty_object(&self, uid: u64) -> Result<Option<ContentObject>> {
        Ok(self
            .db()
            .content_objects_of(uid)?
            .into_iter()
            .find(|object| object.dirty || object.edited))
    }

    async fn run_write_back(self) {
        let writeback = &self.shared().writeback;
        loop {
            let (due, next) = writeback.take_due(Instant::now());
            for uid in due {
                tokio::spawn(self.clone().write_back(uid));
            }
            let woken = writeback.wake.notified();
            match next {
                Some(at) => {
                    let _ = tokio::time::timeout_at(at, woken).await;
                }
                None => woken.await,
            }
        }
    }

    async fn write_back(self, uid: u64) {
        let permit = self.shared().writeback.uploads.acquire().await;
        let outcome = self.upload(uid).await;
        drop(permit);
        if let Err(error) = &outcome {
            tracing::warn!(uid, error = %format!("{error:#}"), "upload failed; retrying later");
        }
        if matches!(outcome, Ok(Uploaded::Clean)) {
            self.shared().working_set.raise();
        }
        self.shared().writeback.finish(uid, &outcome);
    }

    /// One upload attempt of the item's dirty object.
    async fn upload(&self, uid: u64) -> Result<Uploaded> {
        let writeback = &self.shared().writeback;
        let Some(object) = self.dirty_object(uid)? else {
            return Ok(Uploaded::Gone);
        };
        if self.db().replica_item_deleting(uid)? {
            return Ok(Uploaded::Gone);
        }
        let replica = self.replica(uid)?;
        let operation = if object.edited || object.upload.is_some() {
            Some(writeback.lock(uid).await)
        } else {
            None
        };
        if object.edited {
            self.seal_edit(uid).await?;
        }
        if object.upload.is_some() {
            match self.settle(&replica, uid).await? {
                Settled::Clean => return Ok(Uploaded::Clean),
                Settled::Resume { object_id, temp } => {
                    return self.rename_over(&replica, uid, object_id, &temp).await;
                }
                Settled::Pending => {}
            }
        }
        let Some(object) = self.dirty_object(uid)? else {
            return Ok(Uploaded::Gone);
        };
        if object.edited {
            return Ok(Uploaded::Superseded);
        }
        drop(operation);
        let target = self.upload_target(&replica, uid).await?;
        let folder = self.parent_uid(uid)?;
        let temp = sibling(&target, &temp_name(uid));
        self.remove_remote(&replica, &temp).await?;
        let parent_id = self.parent_file_id(&replica, uid)?;
        let shared = self.shared();
        let remote = &shared.remote;
        let backend = replica.backend();
        let local = shared.store.path(object.object_id);
        let pieces = object.size.div_ceil(shared.config.request_size.max(1));
        let entry = remote
            .call_for_pieces(pieces, backend.upload_from_path(&temp, &local, &parent_id))
            .await?;
        let revision = remote.call(backend.revision(&entry)).await?;

        let _operation = writeback.lock(uid).await;
        let current = self.dirty_object(uid)?.map(|current| current.object_id);
        let gone = current.is_none() || self.db().replica_item_deleting(uid)?;
        if gone || current != Some(object.object_id) || self.parent_uid(uid)? != folder {
            if let Err(error) = self.remove_remote(&replica, &temp).await {
                tracing::warn!(uid, error = %format!("{error:#}"), "superseded upload not removed");
            }
            return Ok(if gone {
                Uploaded::Gone
            } else {
                Uploaded::Superseded
            });
        }
        self.db().record_content_upload(
            object.object_id,
            &UploadRecord {
                temp_name: temp_name(uid),
                file_id: revision.stable_id,
            },
        )?;
        self.rename_over(&replica, uid, object.object_id, &temp)
            .await
    }

    /// Renames the recorded temporary file over the item's target and marks
    /// the object clean. Runs under the item's operation lock.
    async fn rename_over(
        &self,
        replica: &Replica,
        uid: u64,
        object_id: super::ObjectId,
        temp: &Path,
    ) -> Result<Uploaded> {
        let target = self
            .item_path(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        let (entry, revision) = self
            .shared()
            .remote
            .call(replica.backend().replace(temp, &target))
            .await?;
        let clean = self
            .db()
            .finish_content_upload(uid, object_id, &entry, &revision)?;
        Ok(if clean {
            Uploaded::Clean
        } else {
            Uploaded::Superseded
        })
    }

    /// Resolves a recorded upload of the item's dirty object: a target that
    /// carries the recorded file id finished its rename; a temporary file
    /// still present is resumed; otherwise the record is cleared. Runs under
    /// the item's operation lock.
    async fn settle(&self, replica: &Replica, uid: u64) -> Result<Settled> {
        let Some(object) = self.dirty_object(uid)? else {
            return Ok(Settled::Pending);
        };
        let Some(record) = object.upload else {
            return Ok(Settled::Pending);
        };
        let Some(target) = self.item_path(uid)? else {
            return Ok(Settled::Pending);
        };
        if let Some((entry, revision)) = self.lookup(replica, uid).await?
            && revision.stable_id == record.file_id
        {
            let clean =
                self.db()
                    .finish_content_upload(uid, object.object_id, &entry, &revision)?;
            return Ok(if clean {
                Settled::Clean
            } else {
                Settled::Pending
            });
        }
        let temp = sibling(&target, &record.temp_name);
        match self
            .shared()
            .remote
            .call(replica.backend().metadata(&temp))
            .await
        {
            Ok(_) => Ok(Settled::Resume {
                object_id: object.object_id,
                temp,
            }),
            Err(error) if is_not_found(&error) => {
                self.db().clear_content_upload(object.object_id)?;
                Ok(Settled::Pending)
            }
            Err(error) => Err(error),
        }
    }

    /// The path the item's upload goes to: its last known parent and name,
    /// after following a rename in its folder by stable id and recreating
    /// missing folders.
    async fn upload_target(&self, replica: &Replica, uid: u64) -> Result<PathBuf> {
        let item = self
            .item(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        let same_file = self
            .lookup(replica, uid)
            .await?
            .is_some_and(|(_, revision)| revision.stable_id == item.stable_id);
        if !same_file {
            self.refresh_parent(replica, uid).await?;
        }
        self.ensure_folders(replica, uid).await?;
        self.item_path(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")).into())
    }

    /// Recreates the folders above the item that are gone on the backend and
    /// re-links their items.
    async fn ensure_folders(&self, replica: &Replica, uid: u64) -> Result<()> {
        let remote = &self.shared().remote;
        let backend = replica.backend();
        let mut folders = Vec::new();
        let mut next = self.item(uid)?.and_then(|item| item.parent_uid);
        while let Some(folder_uid) = next {
            let Some(folder) = self.item(folder_uid)? else {
                break;
            };
            next = folder.parent_uid;
            if folder.parent_uid.is_some() {
                folders.push(folder_uid);
            }
        }
        let Some(&parent) = folders.first() else {
            return Ok(());
        };
        if self.remote_exists(replica, parent).await? {
            return Ok(());
        }
        for folder_uid in folders.into_iter().rev() {
            if self.remote_exists(replica, folder_uid).await? {
                continue;
            }
            let path = self
                .item_path(folder_uid)?
                .ok_or_else(|| BackendError::NotFound(format!("item {folder_uid}")))?;
            let entry = remote.call(backend.create_dir(&path)).await?;
            let revision = remote.call(backend.revision(&entry)).await?;
            self.db()
                .relink_replica_item(folder_uid, &entry, &revision)?;
        }
        Ok(())
    }

    async fn remote_exists(&self, replica: &Replica, uid: u64) -> Result<bool> {
        let path = self
            .item_path(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        match self
            .shared()
            .remote
            .call(replica.backend().metadata(&path))
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if is_not_found(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn parent_uid(&self, uid: u64) -> Result<Option<u64>> {
        Ok(self.item(uid)?.and_then(|item| item.parent_uid))
    }

    fn parent_file_id(&self, replica: &Replica, uid: u64) -> Result<FileId> {
        let parent = self
            .item(uid)?
            .and_then(|item| item.parent_uid)
            .map(|parent_uid| self.item(parent_uid))
            .transpose()?
            .flatten()
            .ok_or_else(|| anyhow!("item {uid} has no parent folder"))?;
        Ok(file_id(replica.backend_id(), &parent.native_id))
    }

    /// Removes the temporary file of the item's dirty object beside `path`,
    /// recorded or not, and clears its record, before the item leaves the
    /// folder that holds it. Runs under the item's operation lock.
    pub(super) async fn remove_temp_upload(
        &self,
        replica: &Replica,
        uid: u64,
        path: &Path,
    ) -> Result<()> {
        let Some(object) = self.dirty_object(uid)? else {
            return Ok(());
        };
        let name = object
            .upload
            .as_ref()
            .map_or_else(|| temp_name(uid), |record| record.temp_name.clone());
        self.remove_remote(replica, &sibling(path, &name)).await?;
        if object.upload.is_some() {
            self.db().clear_content_upload(object.object_id)?;
        }
        Ok(())
    }

    /// Deletes the backend file at `path`; a missing file is not an error.
    pub(super) async fn remove_remote(&self, replica: &Replica, path: &Path) -> Result<()> {
        let remote = &self.shared().remote;
        let backend = replica.backend();
        match remote.call(backend.metadata(path)).await {
            Ok(entry) => match remote.call(backend.delete(&entry)).await {
                Err(error) if !is_not_found(&error) => Err(error),
                _ => Ok(()),
            },
            Err(error) if is_not_found(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Removes the temporary files of a folder other than `kept`.
    async fn sweep_temps(&self, folder_uid: u64, kept: &HashSet<String>) -> Result<()> {
        let Some(folder) = self.item(folder_uid)? else {
            return Ok(());
        };
        let replica = self.replica(folder_uid)?;
        let remote = &self.shared().remote;
        let backend = replica.backend();
        let children = remote
            .call(backend.list_children(&folder.native_id))
            .await?;
        for entry in children
            .iter()
            .filter(|entry| is_temp_upload(&entry.name) && !kept.contains(&entry.name))
        {
            match remote.call(backend.delete(entry)).await {
                Err(error) if !is_not_found(&error) => return Err(error),
                _ => tracing::info!(name = %entry.name, "orphan temporary upload removed"),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "writeback_tests.rs"]
mod tests;
