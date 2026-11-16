//! `ContentCache`: the facade presenters fetch and read content through.
//!
//! A fetch for File Provider starts one download of the whole object and is
//! answered with a clone once its range is present; a ranged read for NFS
//! fetches the missing part plus a read-ahead window without a full
//! download. While an object is partial, each handoff and each fetched read
//! is checked against the backend revision recorded when its download
//! started; a complete object is handed out without contacting the backend.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::ErrorKind;
use std::ops::{ControlFlow, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};

use anyhow::{Result, anyhow};
use tokio::sync::broadcast;

use super::evict::{Eviction, ReadGuard};
use super::names::check_name;
use super::range_map::{RangeMap, align};
use super::replica::{ItemKind, Replica, ReplicaItem, ReplicaStore};
use super::store::ObjectStore;
use super::transfer::{
    CacheSignal, Failure, Remote, Transfer, TransferConfig, TransferError, TransferSource,
    is_unreachable,
};
use super::writeback::WriteBack;
use super::{ObjectId, is_not_found};
use crate::backend::{Backend, BackendError};
use crate::db::StateDb;
use crate::db::content::{ContentObject, ContentState, NewContentObject};
use crate::types::{FileEntry, Revision};

/// Attempts of one request across remote changes before it gives up.
const MAX_RESTARTS: usize = 4;

/// A partial fetch as File Provider issues it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRequest {
    pub uid: u64,
    pub requested_version: Option<String>,
    pub strict: bool,
    pub offset: u64,
    pub length: u64,
    pub alignment: u64,
}

/// A clone of a cached object in the staging directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handoff {
    /// Path of the clone.
    pub path: PathBuf,
    /// Range of the clone that holds the requested bytes.
    pub range: Range<u64>,
    /// Content version the clone holds.
    pub content_version: String,
}

/// An object that holds every byte of the item's current version.
#[derive(Debug, Clone)]
pub(super) struct Complete {
    uid: u64,
    object_id: ObjectId,
    size: u64,
    content_version: String,
}

impl From<&ContentObject> for Complete {
    fn from(object: &ContentObject) -> Self {
        Self {
            uid: object.uid,
            object_id: object.object_id,
            size: object.size,
            content_version: object.content_version.clone(),
        }
    }
}

/// Where a request is served from, with the reader guard that keeps the
/// object from eviction while it is served.
pub(super) enum Source {
    Complete(Complete, ReadGuard),
    Transfer(Arc<Transfer>, ReadGuard),
    /// The remote changed; the request starts over.
    Changed,
}

pub(super) type Slot = Arc<tokio::sync::Mutex<Option<Arc<Transfer>>>>;

pub(super) struct Shared {
    replicas: ReplicaStore,
    backends: std::sync::RwLock<HashMap<String, Arc<Replica>>>,
    pub(super) store: ObjectStore,
    staging_dir: PathBuf,
    pub(super) config: TransferConfig,
    pub(super) remote: Remote,
    slots: std::sync::Mutex<HashMap<u64, Slot>>,
    carried: std::sync::Mutex<HashMap<u64, RangeMap>>,
    handoffs: AtomicU64,
    pub(super) writeback: WriteBack,
    pub(super) eviction: Eviction,
}

/// Cache of the replica store's content: one sparse object per cached item
/// and content version, filled by ranged reads and whole-object downloads
/// from the registered backend that holds the item.
#[derive(Clone)]
pub struct ContentCache {
    shared: Arc<Shared>,
}

impl fmt::Debug for ContentCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContentCache")
            .field("replicas", &self.shared.replicas)
            .field("staging_dir", &self.shared.staging_dir)
            .field("config", &self.shared.config)
            .finish_non_exhaustive()
    }
}

impl ContentCache {
    /// Opens the cache of `replicas` with object files under `cache_dir` and
    /// handoff clones under `staging_dir`. Items are served once the backend
    /// holding them is [registered](Self::register).
    ///
    /// # Errors
    ///
    /// Fails when either directory cannot be created.
    pub fn open(
        replicas: ReplicaStore,
        cache_dir: PathBuf,
        staging_dir: PathBuf,
        config: TransferConfig,
    ) -> Result<Self> {
        let store = ObjectStore::open(cache_dir)?;
        fs::create_dir_all(&staging_dir)?;
        let remote = Remote::new(config.call_timeout);
        Ok(Self {
            shared: Arc::new(Shared {
                replicas,
                backends: std::sync::RwLock::default(),
                store,
                staging_dir,
                config,
                remote,
                slots: std::sync::Mutex::default(),
                carried: std::sync::Mutex::default(),
                handoffs: AtomicU64::new(0),
                writeback: WriteBack::new(config.upload_transfers),
                eviction: Eviction::default(),
            }),
        })
    }

    /// Serves the items of `backend` through it, replacing a backend
    /// registered under the same id.
    ///
    /// # Errors
    ///
    /// Fails when the backend's root cannot be recorded in the replica store.
    pub fn register(&self, backend: Arc<dyn Backend>) -> Result<()> {
        let registered = self
            .shared
            .backends
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(backend.id())
            .is_some_and(|replica| Arc::ptr_eq(replica.backend(), &backend));
        if registered {
            return Ok(());
        }
        let id = backend.id().to_owned();
        let replica = Arc::new(self.shared.replicas.replica(backend)?);
        self.shared
            .backends
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, replica);
        Ok(())
    }

    /// Signals for the host app, such as [`CacheSignal::ErrorResolved`].
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<CacheSignal> {
        self.shared.remote.subscribe()
    }

    /// Rescans the scan set of every registered backend, once start-up
    /// [recovery](Self::recover) finished. Returns whether a replica's
    /// revision advanced.
    ///
    /// # Errors
    ///
    /// Fails when a listing or the state database fails.
    pub async fn scan(&self) -> Result<bool> {
        self.shared.writeback.recovered().await?;
        let mut advanced = false;
        for replica in self.replicas() {
            advanced |= super::scan::scan(&replica).await?;
        }
        Ok(advanced)
    }

    /// Reads up to `len` bytes at `offset` of the item's current version,
    /// fetching what is missing plus a read-ahead window.
    ///
    /// # Errors
    ///
    /// Fails when the item is unknown, the backend fails or the item keeps
    /// changing while it is read.
    pub async fn read(&self, uid: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        let admit = |item: Option<ReplicaItem>| {
            item.map(ControlFlow::Continue)
                .ok_or_else(|| BackendError::NotFound(format!("item {uid}")).into())
        };
        self.restarting(uid, admit, |source| async move {
            match source {
                Source::Complete(object, _guard) => {
                    let bytes = self.read_complete(&object, offset, len)?;
                    self.shared.eviction.touch(object.object_id);
                    Ok(Some(bytes))
                }
                Source::Transfer(transfer, _guard) => {
                    let bytes = self.read_through(&transfer, offset, len).await?;
                    if bytes.is_some() {
                        self.shared.eviction.touch(transfer.object_id);
                    }
                    Ok(bytes)
                }
                Source::Changed => Ok(None),
            }
        })
        .await
    }

    /// Hands out a clone of the whole item once every byte is present.
    ///
    /// # Errors
    ///
    /// Fails with [`TransferError::VersionNoLongerAvailable`] when the item
    /// is gone, and otherwise when the backend fails.
    pub async fn fetch(&self, uid: u64, requested_version: Option<&str>) -> Result<Handoff> {
        self.serve(uid, requested_version, false, |size| 0..size)
            .await
    }

    /// Hands out a clone of the item once the aligned requested range is
    /// present, starting a download of the whole item.
    ///
    /// # Errors
    ///
    /// Fails with [`TransferError::VersionNoLongerAvailable`] when a strict
    /// request names a version that is gone, and otherwise when the backend
    /// fails.
    pub async fn fetch_range(&self, request: &RangeRequest) -> Result<Handoff> {
        self.serve(
            request.uid,
            request.requested_version.as_deref(),
            request.strict,
            |size| align(request.offset, request.length, request.alignment, size),
        )
        .await
    }

    async fn serve(
        &self,
        uid: u64,
        requested_version: Option<&str>,
        strict: bool,
        range_of: impl Fn(u64) -> Range<u64> + Send + Sync,
    ) -> Result<Handoff> {
        let admit = |item: Option<ReplicaItem>| match item {
            Some(item)
                if !strict
                    || requested_version.is_none_or(|version| version == item.content_version) =>
            {
                Ok(ControlFlow::Continue(item))
            }
            _ => Err(TransferError::VersionNoLongerAvailable.into()),
        };
        let range_of = &range_of;
        self.restarting(uid, admit, |source| async move {
            match source {
                Source::Complete(object, _guard) => {
                    let handoff = self.hand_off_complete(&object, range_of(object.size))?;
                    self.shared.eviction.touch(object.object_id);
                    Ok(Some(handoff))
                }
                Source::Transfer(transfer, _guard) => {
                    let range = range_of(transfer.size);
                    let handoff = self.hand_off(&transfer, range).await?;
                    if handoff.is_some() {
                        self.shared.eviction.touch(transfer.object_id);
                    }
                    Ok(handoff)
                }
                Source::Changed => Ok(None),
            }
        })
        .await
    }

    /// Serves a request from the source of the item's current version,
    /// starting over while the remote changes it. Before each attempt
    /// `admit` takes the item as the replica holds it and continues with it
    /// or ends the request; `attempt` answers `None` when the remote
    /// changed.
    pub(super) async fn restarting<T, F>(
        &self,
        uid: u64,
        admit: impl Fn(Option<ReplicaItem>) -> Result<ControlFlow<T, ReplicaItem>>,
        attempt: impl Fn(Source) -> F,
    ) -> Result<T>
    where
        F: Future<Output = Result<Option<T>>>,
    {
        for _ in 0..MAX_RESTARTS {
            let item = match admit(self.item(uid)?)? {
                ControlFlow::Break(done) => return Ok(done),
                ControlFlow::Continue(item) => item,
            };
            let replica = self.replica(uid)?;
            let source = self.source(&replica, &item).await?;
            if let Some(done) = attempt(source).await? {
                return Ok(done);
            }
        }
        Err(TransferError::VersionNoLongerAvailable.into())
    }

    pub(super) fn shared(&self) -> &Shared {
        &self.shared
    }

    pub(super) fn db(&self) -> &StateDb {
        self.shared.replicas.db()
    }

    /// The replicas of every registered backend, ordered by backend id.
    pub(super) fn replicas(&self) -> Vec<Arc<Replica>> {
        let mut replicas: Vec<Arc<Replica>> = self
            .shared
            .backends
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        replicas.sort_by(|a, b| a.backend_id().cmp(b.backend_id()));
        replicas
    }

    /// The replica of the registered backend holding the item.
    pub(super) fn replica(&self, uid: u64) -> Result<Arc<Replica>> {
        let backend_id = self
            .db()
            .replica_item_backend(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        self.shared
            .backends
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&backend_id)
            .cloned()
            .ok_or_else(|| anyhow!("backend {backend_id} of item {uid} is not registered"))
    }

    pub(super) fn item(&self, uid: u64) -> Result<Option<ReplicaItem>> {
        self.db().replica_item(uid)
    }

    pub(super) fn slot(&self, uid: u64) -> Slot {
        let mut slots = self
            .shared
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Arc::clone(slots.entry(uid).or_default())
    }

    pub(super) fn take_carried(&self, uid: u64) -> RangeMap {
        self.shared
            .carried
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&uid)
            .unwrap_or_default()
    }

    /// The object serving the item's current version: a complete object, the
    /// transfer in progress, or a new or resumed transfer. Its reader guard is
    /// taken under the item's slot lock, which eviction also holds; an object
    /// being evicted answers [`Source::Changed`].
    pub(super) async fn source(
        &self,
        replica: &Arc<Replica>,
        item: &ReplicaItem,
    ) -> Result<Source> {
        let slot = self.slot(item.uid);
        let mut current = slot.lock().await;
        let eviction = &self.shared.eviction;
        if let Some(transfer) = current.take() {
            if !transfer.is_discarded() && transfer.content_version == item.content_version {
                let Some(guard) = eviction.guard(transfer.object_id) else {
                    *current = Some(transfer);
                    return Ok(Source::Changed);
                };
                if transfer.is_complete() {
                    let object = Complete {
                        uid: transfer.uid,
                        object_id: transfer.object_id,
                        size: transfer.size,
                        content_version: transfer.content_version.clone(),
                    };
                    return Ok(Source::Complete(object, guard));
                }
                *current = Some(Arc::clone(&transfer));
                return Ok(Source::Transfer(transfer, guard));
            }
            self.discard(&transfer, false).await?;
        }
        let existing = self
            .db()
            .content_objects_of(item.uid)?
            .into_iter()
            .find(|object| object.content_version == item.content_version);
        if let Some(object) = &existing
            && (object.state == ContentState::Complete || object.dirty)
        {
            return Ok(eviction
                .guard(object.object_id)
                .map_or(Source::Changed, |guard| {
                    Source::Complete(Complete::from(object), guard)
                }));
        }
        let Some(transfer) = self.start(replica, item, existing).await? else {
            return Ok(Source::Changed);
        };
        let Some(guard) = eviction.guard(transfer.object_id) else {
            return Ok(Source::Changed);
        };
        let transfer = Arc::new(transfer);
        *current = Some(Arc::clone(&transfer));
        Ok(Source::Transfer(transfer, guard))
    }

    /// Starts a download of the item, resuming `existing` when the backend
    /// still holds the revision it was started on. Returns `None` when the
    /// remote changed.
    async fn start(
        &self,
        replica: &Arc<Replica>,
        item: &ReplicaItem,
        existing: Option<ContentObject>,
    ) -> Result<Option<Transfer>> {
        let current = self.lookup(replica, item.uid).await?;
        let carried = self.take_carried(item.uid);
        let source = |object_id, entry, source_revision, size| TransferSource {
            replica: Arc::clone(replica),
            uid: item.uid,
            object_id,
            content_version: item.content_version.clone(),
            source_revision,
            entry,
            size,
        };
        match (existing, current) {
            (Some(object), Some((entry, revision))) if revision == object.source_revision => {
                let mut present = RangeMap::default();
                for range in self.db().content_ranges(object.object_id)? {
                    present.insert(range);
                }
                let source = source(object.object_id, entry, revision, object.size);
                Ok(Some(Transfer::new(source, present, &carried)))
            }
            (Some(object), _) => {
                self.shared.store.remove(self.db(), object.object_id)?;
                self.refresh_parent(replica, item.uid).await?;
                Ok(None)
            }
            (None, None) => {
                self.refresh_parent(replica, item.uid).await?;
                Ok(None)
            }
            (None, Some((entry, revision))) => {
                let size = revision.size.or(item.size).unwrap_or_default();
                let object_id = self.db().insert_content_object(&NewContentObject {
                    uid: item.uid,
                    content_version: &item.content_version,
                    source_revision: &revision,
                    size,
                })?;
                if let Err(error) = self.shared.store.create(object_id, size) {
                    self.shared.store.remove(self.db(), object_id)?;
                    return Err(error.into());
                }
                self.shared.eviction.touch(object_id);
                let source = source(object_id, entry, revision, size);
                Ok(Some(Transfer::new(source, RangeMap::default(), &carried)))
            }
        }
    }

    /// Path of the item relative to the backend root.
    pub(super) fn item_path(&self, uid: u64) -> Result<Option<PathBuf>> {
        super::replica::item_path(uid, |uid| {
            Ok(self.item(uid)?.map(|item| (item.parent_uid, item.name)))
        })
    }

    /// The entry and revision at the item's current path, or `None` when the
    /// path no longer resolves.
    pub(super) async fn lookup(
        &self,
        replica: &Replica,
        uid: u64,
    ) -> Result<Option<(FileEntry, Revision)>> {
        let Some(path) = self.item_path(uid)? else {
            return Ok(None);
        };
        let remote = &self.shared.remote;
        let backend = replica.backend();
        match remote.call(backend.metadata(&path)).await {
            Ok(entry) => {
                let revision = remote.call(backend.revision(&entry)).await?;
                Ok(Some((entry, revision)))
            }
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Whether the backend no longer holds the revision `transfer` started on.
    async fn changed(&self, transfer: &Transfer) -> Result<bool> {
        Ok(self
            .lookup(&transfer.replica, transfer.uid)
            .await?
            .is_none_or(|(_, revision)| revision != transfer.source_revision))
    }

    /// Discards the object of `transfer` and keeps the ranges handed out from
    /// it for its successor. With `refresh`, the item's folder is relisted
    /// first, so a waiter woken by the discard restarts on the new version.
    pub(super) async fn discard(&self, transfer: &Transfer, refresh: bool) -> Result<()> {
        if transfer.is_discarded() {
            return Ok(());
        }
        let refreshed = if refresh {
            self.refresh_parent(&transfer.replica, transfer.uid).await
        } else {
            Ok(())
        };
        let Some(handed_out) = transfer.discard(&self.shared.store, self.db())? else {
            return refreshed;
        };
        tracing::info!(
            uid = transfer.uid,
            object = %transfer.object_id,
            "remote changed; cached object discarded"
        );
        self.shared
            .carried
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(transfer.uid, handed_out);
        refreshed
    }

    /// Relists the folder of the item so the replica records its new version
    /// or its removal.
    pub(super) async fn refresh_parent(&self, replica: &Replica, uid: u64) -> Result<()> {
        let Some(parent_uid) = self.item(uid)?.and_then(|item| item.parent_uid) else {
            return Ok(());
        };
        if self.item(parent_uid)?.is_none() {
            return Ok(());
        }
        self.shared.remote.call(replica.refresh(parent_uid)).await
    }

    fn staging_target(&self, uid: u64, object_id: ObjectId) -> Result<PathBuf> {
        loop {
            let n = self.shared.handoffs.fetch_add(1, Ordering::Relaxed);
            let target = self
                .shared
                .staging_dir
                .join(format!("{uid}-{object_id}-{n}"));
            if !target.try_exists()? {
                return Ok(target);
            }
        }
    }

    fn hand_off_complete(&self, object: &Complete, range: Range<u64>) -> Result<Handoff> {
        let target = self.staging_target(object.uid, object.object_id)?;
        self.shared.store.clone_to(object.object_id, &target)?;
        Ok(Handoff {
            path: target,
            range,
            content_version: object.content_version.clone(),
        })
    }

    /// Waits for `range` and the carried ranges, clones the object and checks
    /// the revision. Returns `None` when the remote changed.
    async fn hand_off(
        &self,
        transfer: &Arc<Transfer>,
        range: Range<u64>,
    ) -> Result<Option<Handoff>> {
        let waits: Vec<_> = std::iter::once(range.clone())
            .chain(transfer.carried.iter().cloned())
            .map(|wanted| transfer.waiters.wait(wanted))
            .collect();
        self.ensure_download(transfer);
        for wait in waits {
            match wait.await {
                Ok(()) => {}
                Err(Failure::Changed) => return Ok(None),
                Err(failure) => return Err(failure.into_error()),
            }
        }
        let target = self.staging_target(transfer.uid, transfer.object_id)?;
        if transfer.clone_to(&self.shared.store, &target)?.is_none() {
            return Ok(None);
        }
        if !transfer.is_complete() {
            match self.changed(transfer).await {
                Ok(false) => transfer.finish(self.db())?,
                Ok(true) => {
                    remove_clone(&target);
                    self.discard(transfer, true).await?;
                    return Ok(None);
                }
                Err(error) => {
                    remove_clone(&target);
                    return Err(error);
                }
            }
        }
        transfer.mark_handed_out(range.clone());
        Ok(Some(Handoff {
            path: target,
            range,
            content_version: transfer.content_version.clone(),
        }))
    }

    fn read_complete(&self, object: &Complete, offset: u64, len: u32) -> Result<Vec<u8>> {
        let len = usize::try_from(u64::from(len).min(object.size.saturating_sub(offset)))?;
        let mut buf = vec![0; len];
        let read = self
            .shared
            .store
            .read_at(object.object_id, offset, &mut buf)?;
        buf.truncate(read);
        Ok(buf)
    }

    /// Reads through a partial object. Returns `None` when the remote
    /// changed.
    async fn read_through(
        &self,
        transfer: &Arc<Transfer>,
        offset: u64,
        len: u32,
    ) -> Result<Option<Vec<u8>>> {
        if offset >= transfer.size || len == 0 {
            return Ok(Some(Vec::new()));
        }
        let wanted = offset..offset.saturating_add(u64::from(len)).min(transfer.size);
        let wait = transfer.waiters.wait(wanted.clone());
        let config = &self.shared.config;
        let window = align(
            offset,
            u64::from(len).saturating_add(config.read_ahead),
            config.request_size,
            transfer.size,
        );
        let claimed = transfer.waiters.claim(window, config.request_size);
        if !claimed.is_empty() {
            tokio::spawn(self.clone().fetch_pieces(Arc::clone(transfer), claimed));
        }
        match wait.await {
            Ok(()) => {}
            Err(Failure::Changed) => return Ok(None),
            Err(failure) => return Err(failure.into_error()),
        }
        let mut buf = vec![0; usize::try_from(wanted.end - wanted.start)?];
        let Some(read) = transfer.read_at(&self.shared.store, wanted.start, &mut buf)? else {
            return Ok(None);
        };
        buf.truncate(read);
        if let Some(written) = transfer.check_due() {
            if self.changed(transfer).await? {
                self.discard(transfer, true).await?;
                return Ok(None);
            }
            transfer.mark_verified(written);
        }
        Ok(Some(buf))
    }

    pub(super) fn ensure_download(&self, transfer: &Arc<Transfer>) {
        if transfer.want() {
            tokio::spawn(self.clone().download(Arc::clone(transfer)));
        }
    }

    /// Reads one piece from the backend into the object. A failure other
    /// than an unreachable backend is checked against the revision, and the
    /// object is discarded when the remote changed.
    async fn fetch_piece(&self, transfer: &Transfer, piece: Range<u64>) -> Result<()> {
        let result = self.read_piece(transfer, piece).await;
        if let Err(error) = &result
            && !is_unreachable(error)
            && self.changed(transfer).await.unwrap_or(false)
            && let Err(error) = self.discard(transfer, true).await
        {
            tracing::warn!(uid = transfer.uid, error = %format!("{error:#}"), "discard failed");
        }
        result
    }

    async fn read_piece(&self, transfer: &Transfer, piece: Range<u64>) -> Result<()> {
        let expected = piece.end - piece.start;
        let length = u32::try_from(expected)?;
        let bytes = self
            .shared
            .remote
            .call(
                transfer
                    .replica
                    .backend()
                    .read_range(&transfer.entry, piece.start, length),
            )
            .await?;
        if u64::try_from(bytes.len())? != expected {
            return Err(anyhow!(
                "short read of {} bytes at {} of item {}",
                bytes.len(),
                piece.start,
                transfer.uid
            ));
        }
        transfer.write(&self.shared.store, self.db(), piece, &bytes)?;
        Ok(())
    }

    /// Fetches the pieces a ranged read claimed, in order.
    async fn fetch_pieces(self, transfer: Arc<Transfer>, pieces: Vec<Range<u64>>) {
        let mut pieces = pieces.into_iter();
        while let Some(piece) = pieces.next() {
            if transfer.is_discarded() {
                return;
            }
            if let Err(error) = self.fetch_piece(&transfer, piece.clone()).await {
                tracing::warn!(uid = transfer.uid, error = %format!("{error:#}"), "ranged read failed");
                transfer
                    .waiters
                    .fail_overlapping(&piece, &Failure::of(&error));
                for rest in pieces {
                    transfer.waiters.release(&rest);
                }
                return;
            }
        }
    }

    /// Downloads the whole object: ranges with waiters first, in arrival
    /// order, then sequentially from the lowest missing offset. Stops when
    /// idle, when the object is complete or discarded, and fails its waiters
    /// after an unreachable backend or too many consecutive errors.
    async fn download(self, transfer: Arc<Transfer>) {
        let config = self.shared.config;
        let mut errors = 0_u32;
        loop {
            if transfer.is_discarded() {
                transfer.try_stop(None);
                return;
            }
            if !transfer.waiters.has_waiters()
                && transfer.idle_for() >= config.idle_stop
                && transfer.try_stop(Some(config.idle_stop))
            {
                tracing::debug!(uid = transfer.uid, "download stopped while idle");
                return;
            }
            let Some(piece) = transfer
                .waiters
                .claim_next(transfer.whole(), config.request_size)
            else {
                let missing = transfer.waiters.missing(transfer.whole());
                let Some(gap) = missing.into_iter().next() else {
                    self.finish_download(&transfer).await;
                    transfer.try_stop(None);
                    return;
                };
                if transfer.waiters.wait(gap).await.is_err() {
                    tokio::time::sleep(config.retry_delay).await;
                }
                continue;
            };
            let Err(error) = self.fetch_piece(&transfer, piece.clone()).await else {
                errors = 0;
                continue;
            };
            transfer.waiters.release(&piece);
            if transfer.is_discarded() {
                continue;
            }
            errors = errors.saturating_add(1);
            tracing::warn!(
                uid = transfer.uid,
                errors,
                error = %format!("{error:#}"),
                "download read failed"
            );
            if is_unreachable(&error) || errors > config.max_errors {
                transfer.waiters.fail_all(&Failure::of(&error));
                errors = 0;
                if transfer.try_stop(None) {
                    return;
                }
            }
            tokio::time::sleep(config.retry_delay).await;
        }
    }

    /// Checks the revision after the last read and marks the object complete,
    /// or discards it when the remote changed.
    async fn finish_download(&self, transfer: &Transfer) {
        let outcome = match self.changed(transfer).await {
            Ok(false) => transfer.finish(self.db()),
            Ok(true) => self.discard(transfer, true).await,
            Err(error) => Err(error),
        };
        if let Err(error) = outcome {
            tracing::warn!(uid = transfer.uid, error = %format!("{error:#}"), "download not finished");
        }
    }
}

/// Renames, reparents, deletes and folder creates, applied directly against
/// the backend. An unreachable backend fails them with
/// [`TransientError::Unreachable`](crate::backend::TransientError), which the
/// caller maps to `serverUnreachable`.
impl ContentCache {
    /// Moves the item into `parent_uid` under `name`.
    ///
    /// # Errors
    ///
    /// Fails with [`NameError`](super::names::NameError) for a name rejected by TR-10 before anything
    /// is applied, when either item is unknown, and when the backend fails.
    pub async fn rename(&self, uid: u64, parent_uid: u64, name: &str) -> Result<ReplicaItem> {
        check_name(name)?;
        let parent = self.folder(parent_uid)?;
        let replica = self.replica(uid)?;
        let _operation = self.shared.writeback.lock(uid).await;
        let item_path = self
            .item_path(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        if self.item(uid)?.and_then(|item| item.parent_uid) != Some(parent.uid) {
            self.remove_temp_upload(&replica, uid, &item_path).await?;
        }
        let target = self
            .item_path(parent.uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {parent_uid}")))?
            .join(name);
        let remote = &self.shared.remote;
        let backend = replica.backend();
        let entry = remote.call(backend.move_entry(&item_path, &target)).await?;
        let revision = remote.call(backend.revision(&entry)).await?;
        self.db().upsert_replica_children(
            replica.backend_id(),
            parent.uid,
            &[(entry, revision)],
        )?;
        self.item(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")).into())
    }

    /// Deletes the item with its descendants on the backend and in the
    /// replica. The item is marked as being deleted before the backend
    /// delete, so write-back skips it and a restart finishes the delete;
    /// its objects stay until the backend delete returned. A failed backend
    /// delete clears the mark and leaves the item as it was. An unknown item
    /// is already deleted.
    ///
    /// # Errors
    ///
    /// Fails when the backend or the state database fails.
    pub async fn delete(&self, uid: u64) -> Result<()> {
        if self.item(uid)?.is_none() {
            return Ok(());
        }
        let replica = self.replica(uid)?;
        let _operation = self.shared.writeback.lock(uid).await;
        let Some(path) = self.item_path(uid)? else {
            return Ok(());
        };
        self.db().mark_replica_delete(uid)?;
        if let Err(error) = self.remove_remote(&replica, &path).await {
            self.db().clear_replica_delete(uid)?;
            for object in self.db().subtree_content_objects(uid)? {
                if object.dirty {
                    self.shared.writeback.add(object.uid);
                }
            }
            return Err(error);
        }
        self.finish_delete(&replica, uid, &path).await
    }

    /// Removes what is left of the item at `path` after its backend delete
    /// returned: the temporary file of its dirty object, the write-back
    /// timers, objects and replica items of its subtree, and the mark. Runs
    /// under the item's operation lock.
    pub(super) async fn finish_delete(
        &self,
        replica: &Replica,
        uid: u64,
        path: &Path,
    ) -> Result<()> {
        self.remove_temp_upload(replica, uid, path).await?;
        let writeback = &self.shared.writeback;
        for object in self.db().subtree_content_objects(uid)? {
            writeback.remove(object.uid);
        }
        let slot = self.slot(uid);
        let mut current = slot.lock().await;
        if let Some(transfer) = current.take() {
            self.discard(&transfer, false).await?;
        }
        self.take_carried(uid);
        for object in self.db().remove_replica_item(uid)? {
            self.shared.store.delete(object)?;
        }
        drop(current);
        Ok(())
    }

    /// Creates a folder named `name` in `parent_uid`.
    ///
    /// # Errors
    ///
    /// Fails with [`NameError`](super::names::NameError) for a name rejected by TR-10 before anything
    /// is applied, when the parent is unknown, and when the backend fails.
    pub async fn create_folder(&self, parent_uid: u64, name: &str) -> Result<ReplicaItem> {
        check_name(name)?;
        let parent = self.folder(parent_uid)?;
        let replica = self.replica(parent_uid)?;
        let path = self
            .item_path(parent.uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {parent_uid}")))?
            .join(name);
        let remote = &self.shared.remote;
        let backend = replica.backend();
        let entry = remote.call(backend.create_dir(&path)).await?;
        let revision = remote.call(backend.revision(&entry)).await?;
        let uids = self.db().upsert_replica_children(
            replica.backend_id(),
            parent.uid,
            &[(entry, revision)],
        )?;
        uids.first()
            .map(|&uid| self.item(uid))
            .transpose()?
            .flatten()
            .ok_or_else(|| anyhow!("folder {name:?} not recorded"))
    }

    fn folder(&self, uid: u64) -> Result<ReplicaItem> {
        let folder = self
            .item(uid)?
            .ok_or_else(|| BackendError::NotFound(format!("item {uid}")))?;
        if folder.kind != ItemKind::Folder {
            return Err(anyhow!("item {uid} is not a folder"));
        }
        Ok(folder)
    }
}

fn remove_clone(path: &Path) {
    if let Err(error) = fs::remove_file(path)
        && error.kind() != ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), %error, "stale clone not removed");
    }
}

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
