//! Writes at an offset and size changes, as an NFS client issues them, after
//! rclone's `vfs/vfscache/item.go`.
//!
//! An edit goes into the item's object in place: the written range becomes
//! present and the object dirty and edited before the reply, without a
//! backend call. Further writes go into the same object, and the write-back
//! timer restarts with each. When the timer expires, the missing ranges are
//! fetched and the edit is accepted as one save with one content version.
//!
//! The ranges of an edited object that were not written come from its source
//! revision. When the backend holds another revision, those ranges are
//! dropped and fetched from the new one, so an edited object never mixes two
//! remote versions. Reads fetch the missing ranges they cover the same way.

use std::fs::OpenOptions;
use std::ops::Range;
use std::os::unix::fs::FileExt;

use anyhow::{Result, anyhow};
use chrono::Utc;

use super::cache::{ContentCache, MAX_RESTARTS};
use super::names::check_name;
use super::range_map::RangeMap;
use super::replica::{ItemKind, ReplicaItem};
use super::transfer::TransferError;
use super::writeback::Operation;
use crate::backend::BackendError;
use crate::db::content::{ContentObject, ContentState, NewContentObject};

impl ContentCache {
    /// Writes `data` at `offset` of the item's content, extending it with
    /// zeros past its end. The edit is owned by the cache before this
    /// returns and is uploaded once writes pause.
    ///
    /// # Errors
    ///
    /// Fails when the item is unknown or no file, its name is rejected by
    /// TR-10, or the object cannot be written.
    pub async fn write_at(&self, uid: u64, offset: u64, data: &[u8]) -> Result<ReplicaItem> {
        if data.is_empty() {
            return self
                .item(uid)?
                .ok_or_else(|| BackendError::NotFound(format!("item {uid}")).into());
        }
        let operation = self.shared().writeback.lock(uid).await;
        let object = self.edited_object(uid, &operation).await?;
        let end = offset.saturating_add(u64::try_from(data.len())?);
        OpenOptions::new()
            .write(true)
            .open(self.shared().store.path(object.object_id))?
            .write_all_at(data, offset)?;
        let written = offset.min(object.size)..end;
        let item = self
            .db()
            .record_content_write(uid, object.object_id, written, Utc::now())?;
        self.shared().writeback.add(uid);
        Ok(item)
    }

    /// Truncates or zero-extends the item's content to `size` bytes. The
    /// edit is owned by the cache before this returns and is uploaded once
    /// writes pause.
    ///
    /// # Errors
    ///
    /// Fails when the item is unknown or no file, its name is rejected by
    /// TR-10, or the object cannot be resized.
    pub async fn set_size(&self, uid: u64, size: u64) -> Result<ReplicaItem> {
        let operation = self.shared().writeback.lock(uid).await;
        let object = self.edited_object(uid, &operation).await?;
        OpenOptions::new()
            .write(true)
            .open(self.shared().store.path(object.object_id))?
            .set_len(size)?;
        let item = self
            .db()
            .truncate_content_edit(uid, object.object_id, size, Utc::now())?;
        self.shared().writeback.add(uid);
        Ok(item)
    }

    /// Syncs the data of the item's current object to disk: its object
    /// awaiting upload, or else the object of its content version. An item
    /// without an object has nothing to sync. Records nothing.
    ///
    /// # Errors
    ///
    /// Fails when the item is unknown or the object cannot be synced.
    pub async fn sync(&self, uid: u64) -> Result<()> {
        let _operation = self.shared().writeback.lock(uid).await;
        let item = self
            .item(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        let current = match self.dirty_object(uid)? {
            Some(object) => Some(object),
            None => self
                .db()
                .content_objects_of(uid)?
                .into_iter()
                .find(|object| object.content_version == item.content_version),
        };
        if let Some(object) = current {
            self.shared().store.sync(object.object_id)?;
        }
        Ok(())
    }

    /// The item's edited object, if any.
    pub(super) fn edit_of(&self, uid: u64) -> Result<Option<ContentObject>> {
        Ok(self
            .db()
            .content_objects_of(uid)?
            .into_iter()
            .find(|object| object.edited))
    }

    /// The item's edited object, started from a clone of the object holding
    /// its current version, or from an empty object of its size, when the
    /// item has none. Runs under the item's operation lock, held by the
    /// caller as `_operation`.
    async fn edited_object(&self, uid: u64, _operation: &Operation<'_>) -> Result<ContentObject> {
        if let Some(object) = self.edit_of(uid)? {
            return Ok(object);
        }
        let item = self
            .item(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        if item.kind != ItemKind::File {
            return Err(anyhow!("item {uid} is not a file"));
        }
        check_name(&item.name)?;
        let slot = self.slot(uid);
        let mut current = slot.lock().await;
        let base = self
            .db()
            .content_objects_of(uid)?
            .into_iter()
            .find(|object| object.content_version == item.content_version);
        let (source_revision, size, present) = match &base {
            Some(object) if object.state == ContentState::Complete => (
                &object.source_revision,
                object.size,
                std::iter::once(0..object.size).collect(),
            ),
            Some(object) => (
                &object.source_revision,
                object.size,
                self.db().content_ranges(object.object_id)?,
            ),
            None => (&item.remote_revision, item.size.unwrap_or(0), Vec::new()),
        };
        let object_id = self.db().insert_content_object(&NewContentObject {
            uid,
            content_version: &item.content_version,
            source_revision,
            size,
        })?;
        let store = &self.shared().store;
        let placed = base.as_ref().map_or_else(
            || store.create(object_id, size).map(drop),
            |base| {
                store
                    .delete(object_id)
                    .and_then(|()| store.clone_to(base.object_id, &store.path(object_id)))
                    .map(drop)
            },
        );
        if let Err(error) = placed {
            store.remove(self.db(), object_id)?;
            return Err(error.into());
        }
        if let Some(transfer) = current.take() {
            self.discard(&transfer, false).await?;
        }
        self.take_carried(uid);
        let superseded = self.db().begin_content_edit(uid, object_id, &present)?;
        self.shared().eviction.touch(object_id);
        drop(current);
        for object in superseded {
            store.delete(object)?;
        }
        self.edit_of(uid)?
            .ok_or_else(|| anyhow!("edit of item {uid} not recorded"))
    }

    /// Fetches the ranges of `wanted` the edited object `object` lacks from
    /// one consistent remote revision. A file gone from the backend leaves
    /// zeros. Runs under the item's operation lock.
    pub(super) async fn fill_edit(&self, object: &ContentObject, wanted: Range<u64>) -> Result<()> {
        let uid = object.uid;
        let wanted = wanted.start.min(object.size)..wanted.end.min(object.size);
        let replica = self.replica(uid)?;
        let mut source = object.source_revision.clone();
        for _ in 0..MAX_RESTARTS {
            let missing = self.missing(object, &wanted)?;
            if missing.is_empty() {
                return Ok(());
            }
            let chunk = self.shared().config.request_size.max(1);
            let Some((entry, revision)) = self.lookup(&replica, uid).await? else {
                for piece in pieces(&missing, chunk) {
                    self.place(object, piece, &[])?;
                }
                return Ok(());
            };
            if revision != source {
                tracing::info!(
                    uid,
                    "remote changed under an edit; refetching its other bytes"
                );
                self.db().rebase_content_edit(object.object_id, &revision)?;
                source = revision;
                continue;
            }
            let available = revision.size.unwrap_or(u64::MAX);
            for piece in pieces(&missing, chunk) {
                let fetched = piece.start..piece.end.min(available).max(piece.start);
                let bytes = if fetched.is_empty() {
                    Vec::new()
                } else {
                    let length = u32::try_from(fetched.end - fetched.start)?;
                    self.shared()
                        .remote
                        .call(replica.backend().read_range(&entry, fetched.start, length))
                        .await?
                };
                self.place(object, piece, &bytes)?;
            }
            if self
                .lookup(&replica, uid)
                .await?
                .is_some_and(|(_, revision)| revision == source)
            {
                return Ok(());
            }
        }
        Err(TransferError::VersionNoLongerAvailable.into())
    }

    fn missing(&self, object: &ContentObject, wanted: &Range<u64>) -> Result<Vec<Range<u64>>> {
        let mut present = RangeMap::default();
        for range in self.db().content_ranges(object.object_id)? {
            present.insert(range);
        }
        Ok(present.missing(wanted.clone()))
    }

    /// Writes `bytes` at the start of `range` of the object and zeros for
    /// the rest of it, and records the range as present.
    fn place(&self, object: &ContentObject, range: Range<u64>, bytes: &[u8]) -> Result<()> {
        let mut buf = vec![0; usize::try_from(range.end - range.start)?];
        let copied = bytes.len().min(buf.len());
        if let (Some(target), Some(source)) = (buf.get_mut(..copied), bytes.get(..copied)) {
            target.copy_from_slice(source);
        }
        self.shared()
            .store
            .write_at(object.object_id, range.start, &buf)?;
        self.db().add_content_range(object.object_id, range)
    }

    /// Reads up to `len` bytes at `offset` of the item's edited object,
    /// fetching the ranges it lacks. Returns `None` when the item has no
    /// edited object any more.
    pub(super) async fn read_edit(
        &self,
        uid: u64,
        offset: u64,
        len: u32,
    ) -> Result<Option<Vec<u8>>> {
        let _operation = self.shared().writeback.lock(uid).await;
        let Some(object) = self.edit_of(uid)? else {
            return Ok(None);
        };
        let end = offset.saturating_add(u64::from(len)).min(object.size);
        if offset >= end {
            return Ok(Some(Vec::new()));
        }
        self.fill_edit(&object, offset..end).await?;
        let mut buf = vec![0; usize::try_from(end - offset)?];
        let read = self
            .shared()
            .store
            .read_at(object.object_id, offset, &mut buf)?;
        buf.truncate(read);
        self.shared().eviction.touch(object.object_id);
        Ok(Some(buf))
    }

    /// Completes the item's edited object and accepts it as a save, minting
    /// its content version. Runs under the item's operation lock.
    pub(super) async fn seal_edit(&self, uid: u64) -> Result<()> {
        let Some(object) = self.edit_of(uid)? else {
            return Ok(());
        };
        self.fill_edit(&object, 0..object.size).await?;
        let mtime = self
            .item(uid)?
            .and_then(|item| item.mtime)
            .unwrap_or_else(Utc::now);
        self.db()
            .accept_content_save(uid, object.object_id, object.size, mtime)?;
        Ok(())
    }
}

/// `ranges` split into pieces of at most `chunk` bytes.
fn pieces(ranges: &[Range<u64>], chunk: u64) -> impl Iterator<Item = Range<u64>> + '_ {
    ranges.iter().flat_map(move |range| {
        (range.start..range.end)
            .step_by(usize::try_from(chunk).unwrap_or(usize::MAX))
            .map(move |start| start..range.end.min(start.saturating_add(chunk)))
    })
}
