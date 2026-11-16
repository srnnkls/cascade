//! What a File Provider domain reads and changes through the cache.
//!
//! Mounts, items, listings, sync anchors, changes, the working set and its
//! signal, pins that ask the system to download, file creates and progress.

use std::path::Path;

use anyhow::Result;
use tokio::sync::watch;

use super::anchor::SyncAnchor;
use super::cache::ContentCache;
use super::names::check_name;
use super::replica::{Changes, ItemKind, ReplicaItem};
use super::transfer::CacheSignal;
use super::writeback::file_id;
use crate::backend::BackendError;
use crate::db::content::ContentState;

/// Failure of a domain request a caller matches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    /// A folder delete without `recursive` found children.
    #[error("the folder is not empty")]
    DirectoryNotEmpty,
}

/// Generation counter of the working set, raised after a scan advanced the
/// replica and after an upload finished.
#[derive(Debug)]
pub(crate) struct WorkingSetSignal {
    generation: watch::Sender<u64>,
}

impl Default for WorkingSetSignal {
    fn default() -> Self {
        Self {
            generation: watch::Sender::new(0),
        }
    }
}

impl WorkingSetSignal {
    pub(crate) fn raise(&self) {
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

/// A registered backend, mounted under the neutral root by its id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    pub backend_id: String,
    pub root_uid: u64,
}

/// Bytes of an item's current content version held in the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachedBytes {
    pub done: u64,
    pub total: u64,
}

impl ContentCache {
    /// The registered backends, ordered by backend id.
    #[must_use]
    pub fn mounts(&self) -> Vec<Mount> {
        self.replicas()
            .iter()
            .map(|replica| Mount {
                backend_id: replica.backend_id().to_owned(),
                root_uid: replica.root_uid(),
            })
            .collect()
    }

    /// The children of a folder as the replica holds them, without a
    /// listing.
    ///
    /// # Errors
    ///
    /// Fails when the state database fails.
    pub fn children(&self, folder_uid: u64) -> Result<Vec<ReplicaItem>> {
        self.db().replica_children(folder_uid)
    }

    /// The child named `name` of a folder as the replica holds it.
    ///
    /// # Errors
    ///
    /// Fails when the state database fails.
    pub fn child(&self, folder_uid: u64, name: &str) -> Result<Option<ReplicaItem>> {
        self.db().replica_child(folder_uid, name)
    }

    /// The anchor of the latest change in the replica store.
    ///
    /// # Errors
    ///
    /// Fails when the state database fails.
    pub fn current_anchor(&self) -> Result<SyncAnchor> {
        self.shared().replicas.current_anchor()
    }

    /// The changes of every backend since `anchor`, or [`Changes::Expired`].
    ///
    /// # Errors
    ///
    /// Fails when the state database fails.
    pub fn changes(&self, anchor: &SyncAnchor) -> Result<Changes> {
        self.shared().replicas.changes(anchor)
    }

    /// The materialised and pinned items, in uid order.
    ///
    /// # Errors
    ///
    /// Fails when the state database fails.
    pub fn working_set(&self) -> Result<Vec<ReplicaItem>> {
        self.db().replica_working_set()
    }

    /// A receiver that changes after a scan advanced the replica and after an
    /// upload finished.
    #[must_use]
    pub fn working_set_changes(&self) -> watch::Receiver<u64> {
        self.shared().working_set.generation.subscribe()
    }

    /// Pins or unpins the item as Keep Downloaded and Remove Download ask;
    /// a pinned file raises [`CacheSignal::RequestDownload`] so the system
    /// materialises it.
    ///
    /// # Errors
    ///
    /// Fails as [`set_pinned`](Self::set_pinned) does.
    pub async fn keep_downloaded(&self, uid: u64, pinned: bool) -> Result<()> {
        self.set_pinned(uid, pinned).await?;
        if pinned
            && self
                .item(uid)?
                .is_some_and(|item| item.kind == ItemKind::File)
        {
            self.shared()
                .remote
                .signal(CacheSignal::RequestDownload(uid));
        }
        Ok(())
    }

    /// Deletes the item; a folder with children is refused with
    /// [`DomainError::DirectoryNotEmpty`] unless `recursive`. An unknown item
    /// is already deleted.
    ///
    /// # Errors
    ///
    /// Fails when the listing, the backend or the state database fails.
    pub async fn delete_item(&self, uid: u64, recursive: bool) -> Result<()> {
        let Some(item) = self.item(uid)? else {
            return Ok(());
        };
        if item.kind == ItemKind::Folder && !recursive {
            let replica = self.replica(uid)?;
            self.shared().remote.call(replica.refresh(uid)).await?;
            if !self.db().replica_children(uid)?.is_empty() {
                return Err(DomainError::DirectoryNotEmpty.into());
            }
        }
        self.delete(uid).await
    }

    /// Creates an empty file named `name` in `parent_uid` on the backend and,
    /// with `contents`, saves them into it for write-back.
    ///
    /// # Errors
    ///
    /// Fails with [`NameError`](super::names::NameError) for a name rejected
    /// by TR-10 before anything is applied, with [`BackendError::Conflict`]
    /// when the parent holds the name, when the parent is unknown, and when
    /// the backend or the save fails.
    pub async fn create_file(
        &self,
        parent_uid: u64,
        name: &str,
        contents: Option<&Path>,
    ) -> Result<ReplicaItem> {
        check_name(name)?;
        let parent = self.folder(parent_uid)?;
        let replica = self.replica(parent_uid)?;
        self.ensure_absent(&parent, name).await?;
        let path = self
            .item_path(parent.uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {parent_uid}")))?
            .join(name);
        let folder_id = file_id(replica.backend_id(), &parent.native_id);
        let entry = self
            .shared()
            .remote
            .call(replica.backend().upload(&path, &[], &folder_id))
            .await?;
        let item = self.record_created(&replica, parent.uid, entry).await?;
        match contents {
            Some(contents) => self.save(item.uid, contents).await,
            None => Ok(item),
        }
    }

    /// Bytes of the item's current content version held in the cache.
    ///
    /// # Errors
    ///
    /// Fails when the item is unknown or the state database fails.
    pub fn cached_bytes(&self, uid: u64) -> Result<CachedBytes> {
        let item = self
            .item(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        let object = self
            .db()
            .content_objects_of(uid)?
            .into_iter()
            .find(|object| object.content_version == item.content_version);
        let Some(object) = object else {
            return Ok(CachedBytes {
                done: 0,
                total: item.size.unwrap_or_default(),
            });
        };
        let done = if object.state == ContentState::Complete || object.dirty {
            object.size
        } else {
            self.db()
                .content_ranges(object.object_id)?
                .iter()
                .map(|range| range.end - range.start)
                .sum()
        };
        Ok(CachedBytes {
            done,
            total: object.size,
        })
    }
}
