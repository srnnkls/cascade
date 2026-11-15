//! Revisioned replica store: daemon-assigned uids mapped to backend stable
//! ids, with tombstones and sync anchors for change enumeration.
//!
//! The store is the unit of identity and change tracking across every
//! backend; a [`Replica`] is the unit of listing and scanning for one.

use std::path::{Component, Path};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use chrono::Utc;

use super::anchor::SyncAnchor;
use crate::backend::Backend;
use crate::db::StateDb;
pub use crate::db::replica::{ItemKind, ReplicaItem};
use crate::types::{FileEntry, Revision};

/// Name prefix of temporary upload files, hidden from the replica.
pub const TEMP_UPLOAD_PREFIX: &str = ".cascade-tmp-";

/// Whether `name` is a temporary upload name.
#[must_use]
pub fn is_temp_upload(name: &str) -> bool {
    name.starts_with(TEMP_UPLOAD_PREFIX)
}

/// Changes since an anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeSet {
    /// Items created or changed since the anchor.
    pub updated: Vec<ReplicaItem>,
    /// Uids deleted since the anchor.
    pub deleted: Vec<u64>,
    /// Anchor to resume from.
    pub anchor: SyncAnchor,
}

/// Reply to a change request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Changes {
    /// The changes since the anchor.
    Since(ChangeSet),
    /// The anchor belongs to another store or lies below the tombstone floor.
    Expired,
}

/// Replica store kept in the state database: item identity, revision,
/// tombstones and change enumeration across all backends.
#[derive(Debug, Clone)]
pub struct ReplicaStore {
    db: Arc<StateDb>,
}

impl ReplicaStore {
    /// Open the replica store of `db`.
    ///
    /// # Errors
    ///
    /// Fails when the state database cannot be read.
    pub fn open(db: Arc<StateDb>) -> Result<Self> {
        db.replica_meta()?;
        Ok(Self { db })
    }

    /// Return the per-backend handle of `backend`, registering its root
    /// folder.
    ///
    /// # Errors
    ///
    /// Fails when the state database cannot be read or written.
    pub fn replica(&self, backend: Arc<dyn Backend>) -> Result<Replica> {
        let root_uid = self
            .db
            .ensure_replica_root(backend.id(), backend.root_native_id())?;
        Ok(Replica {
            db: Arc::clone(&self.db),
            backend,
            root_uid,
        })
    }

    /// Return the item with the given uid.
    ///
    /// # Errors
    ///
    /// Fails when the state database cannot be read.
    pub fn item(&self, uid: u64) -> Result<Option<ReplicaItem>> {
        self.db.replica_item(uid)
    }

    /// Return the anchor of the latest change in the store.
    ///
    /// # Errors
    ///
    /// Fails when the state database cannot be read.
    pub fn current_anchor(&self) -> Result<SyncAnchor> {
        let meta = self.db.replica_meta()?;
        Ok(SyncAnchor::new(meta.store_id, meta.revision))
    }

    /// Return the changes of every backend since `anchor`, or
    /// [`Changes::Expired`] when the anchor was minted by another store, lies
    /// below the tombstone floor or ahead of the store.
    ///
    /// # Errors
    ///
    /// Fails when the state database cannot be read.
    pub fn changes(&self, anchor: &SyncAnchor) -> Result<Changes> {
        let changes = self.db.replica_changes_since(anchor.revision())?;
        let meta = changes.meta;
        if anchor.store_id() != meta.store_id
            || anchor.revision() < meta.floor
            || anchor.revision() > meta.revision
        {
            return Ok(Changes::Expired);
        }
        Ok(Changes::Since(ChangeSet {
            updated: changes.updated,
            deleted: changes.deleted,
            anchor: SyncAnchor::new(meta.store_id, meta.revision),
        }))
    }

    /// Drop tombstones up to and including `revision`; anchors below it
    /// expire.
    ///
    /// # Errors
    ///
    /// Fails when the state database cannot be written.
    pub fn prune_tombstones_through(&self, revision: u64) -> Result<()> {
        self.db.prune_replica_tombstones(revision)
    }
}

/// Handle of one backend in the replica store: resolves paths, lists folders
/// and is the unit [`scan`](super::scan::scan) rescans.
pub struct Replica {
    db: Arc<StateDb>,
    backend: Arc<dyn Backend>,
    root_uid: u64,
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("backend", &self.backend.id())
            .field("root_uid", &self.root_uid)
            .finish_non_exhaustive()
    }
}

impl Replica {
    /// Uid of the backend's root folder.
    #[must_use]
    pub const fn root_uid(&self) -> u64 {
        self.root_uid
    }

    pub(crate) fn db(&self) -> &StateDb {
        &self.db
    }

    pub(crate) fn backend_id(&self) -> &str {
        self.backend.id()
    }

    /// Return the uid of the item at `path`, relative to the backend root,
    /// listing and registering folders along the way whose child is missing.
    /// Returns `None` when no such item exists.
    ///
    /// # Errors
    ///
    /// Fails when a listing or the state database fails.
    pub async fn resolve(&self, path: &Path) -> Result<Option<u64>> {
        let mut current = self.root_uid;
        for component in path.components() {
            let name = match component {
                Component::Normal(name) => match name.to_str() {
                    Some(name) if !is_temp_upload(name) => name,
                    _ => return Ok(None),
                },
                Component::RootDir | Component::CurDir => continue,
                Component::ParentDir | Component::Prefix(_) => return Ok(None),
            };
            if self.db.replica_child(current, name)?.is_none() {
                self.refresh(current).await?;
            }
            let Some(child) = self.db.replica_child(current, name)? else {
                return Ok(None);
            };
            current = child.uid;
        }
        Ok(Some(current))
    }

    /// List a folder from the backend, record the result and add the folder
    /// to the scan set. Temporary upload names are left out.
    ///
    /// # Errors
    ///
    /// Fails when `folder_uid` is not a known folder or the listing fails.
    pub async fn list(&self, folder_uid: u64) -> Result<Vec<ReplicaItem>> {
        self.refresh(folder_uid).await?;
        self.db.mark_replica_listed(folder_uid, Utc::now())?;
        self.db.replica_children(folder_uid)
    }

    /// Fetch the children of a folder with their revisions, leaving out
    /// temporary upload names.
    pub(crate) async fn fetch_children(
        &self,
        folder: &ReplicaItem,
    ) -> Result<Vec<(FileEntry, Revision)>> {
        if folder.kind != ItemKind::Folder {
            return Err(anyhow!("replica item {} is not a folder", folder.uid));
        }
        let mut children = self
            .backend
            .list_children_with_revisions(&folder.native_id)
            .await?;
        children.retain(|(entry, _)| !is_temp_upload(&entry.name));
        Ok(children)
    }

    async fn refresh(&self, folder_uid: u64) -> Result<()> {
        let folder = self
            .db
            .replica_item(folder_uid)?
            .ok_or_else(|| anyhow!("no replica item {folder_uid}"))?;
        let children = self.fetch_children(&folder).await?;
        let seen = self
            .db
            .upsert_replica_children(self.backend_id(), folder_uid, &children)?;
        self.db.remove_replica_children_except(folder_uid, &seen)?;
        Ok(())
    }
}
