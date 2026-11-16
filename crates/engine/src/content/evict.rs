//! Eviction and pins, after rclone's `vfs/vfscache/cache.go` purge and dios
//! `pool/epoch.rs` (`FrameGuard`) and `pool/clock.rs`.
//!
//! Every reader of an object holds a [`ReadGuard`]; an object is evictable
//! when it is clean, unpinned and without a live guard. Recency is kept in
//! memory and written to `last_access` only by the sweep. A sweep removes
//! whole objects in recency order until the populated bytes fall under the
//! cache maximum. The object of a materialised item is not deleted by the
//! sweep: the host app is signalled with [`CacheSignal::Evict`], and the
//! object goes once the item leaves the materialised set.
//!
//! The guard count and the eviction mark of an object are the lock-level
//! piece the loom model checks; they take their lock from
//! [`crate::loom_sync`].

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::PoisonError;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::time::Instant;

use super::ObjectId;
use super::cache::{ContentCache, Slot, Source};
use super::replica::{ItemKind, ReplicaItem, item_path};
use super::transfer::{CacheSignal, Failure};
use crate::backend::BackendError;
use crate::db::content::ContentObject;
pub use crate::db::replica::PinReport;
use crate::loom_sync::{Arc, Mutex, MutexGuard};

/// Failure of a pin or an evict request a caller matches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EvictError {
    /// Pinned bytes would exceed the cache maximum.
    #[error("pinned bytes would exceed the cache maximum")]
    OutOfSpace,
    /// The object holds an edit not yet uploaded or has a live reader.
    #[error("the cached object is dirty or open")]
    Busy,
}

#[derive(Debug, Default)]
struct Resident {
    readers: usize,
    marked: bool,
}

/// Live reader guards and eviction marks per object. A mark is granted only
/// to an object without a live guard, and no guard is granted to a marked
/// object.
#[derive(Debug, Default)]
pub(crate) struct Residency {
    objects: Mutex<HashMap<ObjectId, Resident>>,
}

impl Residency {
    fn lock(&self) -> MutexGuard<'_, HashMap<ObjectId, Resident>> {
        self.objects.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A reader guard of the object, or `None` while it is marked.
    pub(crate) fn guard(this: &Arc<Self>, id: ObjectId) -> Option<ReadGuard> {
        let mut objects = this.lock();
        let resident = objects.entry(id).or_default();
        if resident.marked {
            return None;
        }
        resident.readers = resident.readers.saturating_add(1);
        drop(objects);
        Some(ReadGuard {
            residency: Arc::clone(this),
            id,
        })
    }

    /// The eviction mark of the object, or `None` while it has a live guard
    /// or another mark.
    pub(crate) fn mark(this: &Arc<Self>, id: ObjectId) -> Option<Marked> {
        let mut objects = this.lock();
        let resident = objects.entry(id).or_default();
        if resident.marked || resident.readers > 0 {
            return None;
        }
        resident.marked = true;
        drop(objects);
        Some(Marked {
            residency: Arc::clone(this),
            id,
        })
    }

    /// Live reader guards of the object.
    #[cfg(all(test, cascade_loom))]
    pub(crate) fn readers(&self, id: ObjectId) -> usize {
        self.lock().get(&id).map_or(0, |resident| resident.readers)
    }

    /// Whether the object is marked for eviction.
    #[cfg(all(test, cascade_loom))]
    pub(crate) fn is_marked(&self, id: ObjectId) -> bool {
        self.lock().get(&id).is_some_and(|resident| resident.marked)
    }

    fn settle(&self, id: ObjectId, change: impl FnOnce(&mut Resident)) {
        let mut objects = self.lock();
        if let Some(resident) = objects.get_mut(&id) {
            change(resident);
            if resident.readers == 0 && !resident.marked {
                objects.remove(&id);
            }
        }
    }
}

/// A live reader of an object; the object is not evicted while it is held.
#[derive(Debug)]
pub(crate) struct ReadGuard {
    residency: Arc<Residency>,
    id: ObjectId,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.residency.settle(self.id, |resident| {
            resident.readers = resident.readers.saturating_sub(1);
        });
    }
}

/// The eviction mark of an object; no reader guard is granted while it is
/// held.
#[derive(Debug)]
pub(crate) struct Marked {
    residency: Arc<Residency>,
    id: ObjectId,
}

impl Drop for Marked {
    fn drop(&mut self) {
        self.residency
            .settle(self.id, |resident| resident.marked = false);
    }
}

/// Eviction state of the cache: guards and marks, the in-memory recency,
/// the cache maximum and the materialised items the host app was asked to
/// evict.
#[derive(Debug)]
pub(crate) struct Eviction {
    residency: Arc<Residency>,
    recency: std::sync::Mutex<HashMap<ObjectId, Instant>>,
    max_size: AtomicU64,
    requested: std::sync::Mutex<HashSet<u64>>,
    /// Held by a sweep and by the deletions of [`ContentCache::set_materialized`],
    /// so each one starts from the populated bytes the previous one left.
    removing: tokio::sync::Mutex<()>,
}

impl Default for Eviction {
    fn default() -> Self {
        Self {
            residency: Arc::default(),
            recency: std::sync::Mutex::default(),
            max_size: AtomicU64::new(u64::MAX),
            requested: std::sync::Mutex::default(),
            removing: tokio::sync::Mutex::default(),
        }
    }
}

impl Eviction {
    pub(crate) fn guard(&self, id: ObjectId) -> Option<ReadGuard> {
        Residency::guard(&self.residency, id)
    }

    fn mark(&self, id: ObjectId) -> Option<Marked> {
        Residency::mark(&self.residency, id)
    }

    /// Records a read or handoff of the object.
    pub(crate) fn touch(&self, id: ObjectId) {
        self.recency_map().insert(id, Instant::now());
    }

    fn recency_map(&self) -> std::sync::MutexGuard<'_, HashMap<ObjectId, Instant>> {
        self.recency.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn requested(&self) -> std::sync::MutexGuard<'_, HashSet<u64>> {
        self.requested
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The recency recorded since the last sweep as wall-clock times.
    fn take_recency(&self) -> Vec<(ObjectId, DateTime<Utc>)> {
        let now = Instant::now();
        let wall = Utc::now();
        self.recency_map()
            .drain()
            .map(|(id, at)| {
                let age = chrono::Duration::from_std(now.saturating_duration_since(at))
                    .unwrap_or_default();
                (id, wall - age)
            })
            .collect()
    }
}

/// Outcome of removing one cached object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Removal {
    Removed,
    Gone,
    Busy,
}

/// The files of `items` with their paths relative to the backend root, in
/// path order.
fn file_paths(items: Vec<ReplicaItem>) -> Result<Vec<(PathBuf, ReplicaItem)>> {
    let parents: HashMap<u64, (Option<u64>, String)> = items
        .iter()
        .map(|item| (item.uid, (item.parent_uid, item.name.clone())))
        .collect();
    let mut files = Vec::new();
    for item in items {
        if item.kind != ItemKind::File {
            continue;
        }
        if let Some(path) = item_path(item.uid, |uid| Ok(parents.get(&uid).cloned()))? {
            files.push((path, item));
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(files)
}

impl ContentCache {
    /// Sets `cache.max_size`, the bound of the sweep and of pinned bytes.
    pub fn set_max_size(&self, bytes: u64) {
        self.shared()
            .eviction
            .max_size
            .store(bytes, Ordering::Relaxed);
    }

    /// The cache maximum in bytes.
    #[must_use]
    pub fn max_size(&self) -> u64 {
        self.shared().eviction.max_size.load(Ordering::Relaxed)
    }

    /// Persists the recency of reads, then removes clean, unpinned objects
    /// without a live reader in least-recently-used order until the
    /// populated bytes are at most the cache maximum. Materialised items are
    /// signalled with [`CacheSignal::Evict`] and skipped. Returns the
    /// populated bytes freed.
    ///
    /// # Errors
    ///
    /// Fails when the state database or an object file fails.
    pub async fn sweep(&self) -> Result<u64> {
        let _removing = self.shared().eviction.removing.lock().await;
        self.db()
            .persist_content_recency(&self.shared().eviction.take_recency())?;
        let max = self.max_size();
        let mut populated = self.db().content_populated_bytes()?;
        if populated <= max {
            return Ok(0);
        }
        let materialized: HashSet<u64> = self.db().materialized_items()?.into_iter().collect();
        let mut freed = 0_u64;
        for candidate in self.db().content_eviction_candidates()? {
            if populated <= max {
                break;
            }
            let object = &candidate.object;
            if materialized.contains(&object.uid) {
                if !self.db().replica_item_deleting(object.uid)? {
                    self.shared().eviction.requested().insert(object.uid);
                    self.shared().remote.signal(CacheSignal::Evict(object.uid));
                }
                continue;
            }
            let slot = self.slot(object.uid);
            if self.remove_object(&slot, object, true).await? == Removal::Removed {
                populated = populated.saturating_sub(candidate.populated_bytes);
                freed = freed.saturating_add(candidate.populated_bytes);
            }
        }
        tracing::debug!(freed, populated, max, "eviction sweep finished");
        Ok(freed)
    }

    /// Pins or unpins the item. A pinned file is downloaded in the
    /// background without being read and never evicted.
    ///
    /// # Errors
    ///
    /// Fails with [`EvictError::OutOfSpace`], leaving the item unpinned,
    /// when pinned bytes would exceed the cache maximum, and when the item
    /// is unknown or the state database fails.
    pub async fn set_pinned(&self, uid: u64, pinned: bool) -> Result<()> {
        let _operation = self.shared().writeback.lock(uid).await;
        let item = self
            .item(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        if !pinned {
            return self.db().unpin_replica_item(uid);
        }
        if !self.db().pin_replica_item(uid, self.max_size())? {
            return Err(EvictError::OutOfSpace.into());
        }
        self.shared().eviction.requested().remove(&uid);
        if item.kind == ItemKind::File {
            self.spawn_prefetch(uid);
        }
        Ok(())
    }

    /// Makes the files `matches` accepts, given a backend id and a path
    /// relative to its root, the items pinned by rules: rule pins of other
    /// items are cleared, and new matches are pinned in path order until
    /// pinned bytes reach the cache maximum and downloaded in the
    /// background; the remaining matches are logged and refused. Explicit
    /// pins are kept.
    ///
    /// # Errors
    ///
    /// Fails when the state database fails.
    pub fn reconcile_rule_pins(&self, matches: impl Fn(&str, &Path) -> bool) -> Result<PinReport> {
        let mut wanted = Vec::new();
        for replica in self.replicas() {
            let backend_id = replica.backend_id();
            for (path, item) in file_paths(self.db().replica_items_of(backend_id)?)? {
                if matches(backend_id, &path) {
                    wanted.push(item.uid);
                }
            }
        }
        let report = self.db().reconcile_rule_pins(&wanted, self.max_size())?;
        for &uid in &report.pinned {
            self.shared().eviction.requested().remove(&uid);
            self.spawn_prefetch(uid);
        }
        for uid in &report.refused {
            tracing::warn!(
                uid,
                "pin rule match left unpinned: pinned bytes would exceed the cache maximum"
            );
        }
        Ok(report)
    }

    /// Replaces the materialised set. The objects of items the host app was
    /// asked to evict and that left the set are deleted.
    ///
    /// # Errors
    ///
    /// Fails when the state database or an object file fails.
    pub async fn set_materialized(&self, uids: &[u64]) -> Result<()> {
        self.db().replace_materialized_items(uids)?;
        let current: HashSet<u64> = uids.iter().copied().collect();
        let left: Vec<u64> = {
            let mut requested = self.shared().eviction.requested();
            let left = requested
                .iter()
                .filter(|uid| !current.contains(uid))
                .copied()
                .collect();
            requested.retain(|uid| current.contains(uid));
            left
        };
        let _removing = self.shared().eviction.removing.lock().await;
        for uid in left {
            let slot = self.slot(uid);
            for object in self.db().content_objects_of(uid)? {
                let removal = self.remove_object(&slot, &object, true).await?;
                tracing::debug!(uid, object = %object.object_id, ?removal, "evicted item left the materialised set");
            }
        }
        Ok(())
    }

    /// Deletes the cached objects of the item, as Remove Download does
    /// after the system evicted it.
    ///
    /// # Errors
    ///
    /// Fails with [`EvictError::Busy`] for a dirty object or one with a live
    /// reader, and when the state database or an object file fails.
    pub async fn evict(&self, uid: u64) -> Result<()> {
        self.shared().eviction.requested().remove(&uid);
        let objects = self.db().content_objects_of(uid)?;
        if objects.iter().any(|object| object.dirty) {
            return Err(EvictError::Busy.into());
        }
        let slot = self.slot(uid);
        for object in &objects {
            if self.remove_object(&slot, object, false).await? == Removal::Busy {
                return Err(EvictError::Busy.into());
            }
        }
        Ok(())
    }

    /// Removes `object` under its item's slot lock unless it is dirty,
    /// being deleted, pinned with `keep_pinned`, or guarded by a reader.
    async fn remove_object(
        &self,
        slot: &Slot,
        object: &ContentObject,
        keep_pinned: bool,
    ) -> Result<Removal> {
        let mut current = slot.lock().await;
        let id = object.object_id;
        let Some(row) = self
            .db()
            .content_object(id)?
            .filter(|row| row.uid == object.uid)
        else {
            return Ok(Removal::Gone);
        };
        if row.dirty || self.db().replica_item_deleting(row.uid)? {
            return Ok(Removal::Busy);
        }
        if keep_pinned && self.item(row.uid)?.is_some_and(|item| item.pinned) {
            return Ok(Removal::Busy);
        }
        let eviction = &self.shared().eviction;
        let Some(marked) = eviction.mark(id) else {
            return Ok(Removal::Busy);
        };
        let store = &self.shared().store;
        match current.take() {
            Some(transfer) if transfer.object_id == id => {
                transfer.discard(store, self.db())?;
            }
            other => {
                *current = other;
                store.remove(self.db(), id)?;
            }
        }
        eviction.recency_map().remove(&id);
        drop(marked);
        drop(current);
        tracing::debug!(uid = row.uid, object = %id, "cached object evicted");
        Ok(Removal::Removed)
    }

    fn spawn_prefetch(&self, uid: u64) {
        let cache = self.clone();
        tokio::spawn(async move {
            if let Err(error) = cache.prefetch(uid).await {
                tracing::warn!(uid, error = %format!("{error:#}"), "pinned item not downloaded");
            }
        });
    }

    /// Downloads every byte of a pinned item without handing it out.
    async fn prefetch(&self, uid: u64) -> Result<()> {
        let admit = |item: Option<ReplicaItem>| {
            Ok(item
                .filter(|item| item.pinned)
                .map_or(ControlFlow::Break(()), ControlFlow::Continue))
        };
        self.restarting(uid, admit, |source| async move {
            match source {
                Source::Complete(..) => Ok(Some(())),
                Source::Transfer(transfer, _guard) => {
                    let wait = transfer.waiters.wait(transfer.whole());
                    self.ensure_download(&transfer);
                    match wait.await {
                        Ok(()) => Ok(Some(())),
                        Err(Failure::Changed) => Ok(None),
                        Err(failure) => Err(failure.into_error()),
                    }
                }
                Source::Changed => Ok(None),
            }
        })
        .await
    }
}
