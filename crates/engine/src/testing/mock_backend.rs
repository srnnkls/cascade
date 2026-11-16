//! A seeded in-memory [`Backend`].
//!
//! From its seed it orders the replies of concurrent calls; a test injects
//! one-shot faults into chosen operations and holds an operation at a stop
//! point before it changes the tree. Replies are reproducible per seed on a
//! single-threaded runtime.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Notify;

use crate::backend::{Backend, BackendError, TransientError};
use crate::types::{Change, Cursor, FileEntry, FileId, ItemId, Quota, Revision};

const BACKEND_ID: &str = "mock";
const ROOT_ID: &str = "root";
const MAX_REPLY_YIELDS: u64 = 8;
const CLOCK_EPOCH_SECS: i64 = 1_700_000_000;

/// A backend operation a fault or a stop point targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MockOp {
    Changes,
    Metadata,
    Download,
    Upload,
    Update,
    CreateDir,
    Delete,
    Move,
    ListChildren,
    Revision,
    Replace,
}

/// A fault the next call of an operation takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Fails the call with [`TransientError::Unreachable`].
    Unreachable,
    /// Fails the call with [`TransientError::Busy`].
    Busy,
    /// Delays the call, then lets it proceed.
    Delay(Duration),
}

/// A call held before it changes the tree, until [`StopPoint::release`].
#[derive(Debug, Clone)]
pub struct StopPoint {
    signals: Arc<StopSignals>,
}

#[derive(Debug, Default)]
struct StopSignals {
    reached: Notify,
    released: Notify,
}

impl StopPoint {
    /// Waits until a call of the operation arrives at the stop point.
    pub async fn reached(&self) {
        self.signals.reached.notified().await;
    }

    /// Lets the held call proceed.
    pub fn release(&self) {
        self.signals.released.notify_one();
    }
}

/// A seeded in-memory backend with fault injection and stop points.
#[derive(Debug)]
pub struct MockBackend {
    seed: u64,
    calls: AtomicU64,
    tree: Mutex<Tree>,
    faults: Mutex<HashMap<MockOp, VecDeque<Fault>>>,
    stops: Mutex<HashMap<MockOp, StopPoint>>,
    upload_replaces: AtomicBool,
}

impl MockBackend {
    /// An empty tree whose reply order derives from `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            calls: AtomicU64::new(0),
            tree: Mutex::new(Tree::new()),
            faults: Mutex::new(HashMap::new()),
            stops: Mutex::new(HashMap::new()),
            upload_replaces: AtomicBool::new(false),
        }
    }

    /// Makes an upload to the path of an existing file replace its content,
    /// as SMB opens with `OverwriteIf`, instead of failing with a conflict.
    pub fn replace_existing_on_upload(&self) {
        self.upload_replaces.store(true, Ordering::Relaxed);
    }

    /// Writes a file at the root-relative `path`, creating missing parent
    /// folders, without faults, reordering or stop points.
    pub fn add_file(&self, path: &str, data: &[u8]) -> FileEntry {
        let mut tree = lock(&self.tree);
        let key = path_key(Path::new(path));
        let id = tree.seed_file(&key, data);
        tree.entry_by_id(&id)
            .unwrap_or_else(|| FileEntry::file(item_id(&id), item_id(ROOT_ID), key))
    }

    /// Creates a folder at the root-relative `path`, creating missing parent
    /// folders, without faults, reordering or stop points.
    ///
    /// # Errors
    ///
    /// Fails when `path` exists.
    pub fn add_folder(&self, path: &str) -> anyhow::Result<FileEntry> {
        let mut tree = lock(&self.tree);
        let key = path_key(Path::new(path));
        tree.seed_ancestors(&key);
        let id = tree.insert(&key, true, &[])?;
        tree.entry(&id)
    }

    /// Rewrites a file in place, keeping its size and last-write time and
    /// moving its change time, without faults, reordering or stop points.
    ///
    /// # Errors
    ///
    /// Fails when `path` is not a file or `data` differs in length.
    pub fn rewrite_keeping_size_and_mtime(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        let mut tree = lock(&self.tree);
        let key = path_key(Path::new(path));
        let id = tree
            .paths
            .get(&key)
            .cloned()
            .ok_or_else(|| not_found(&key))?;
        let now = tree.tick();
        let node = tree.node_mut(&id)?;
        if node.is_dir || node.data.len() != data.len() {
            return Err(BackendError::Conflict(format!(
                "{key} is not a file of {} bytes",
                data.len()
            ))
            .into());
        }
        node.data = data.to_vec();
        node.changed = now;
        Ok(())
    }

    /// Moves the item at `from` to `to`, without faults, reordering or stop
    /// points.
    ///
    /// # Errors
    ///
    /// Fails when `from` is missing, `to` exists or its parent is no folder.
    pub fn rename(&self, from: &str, to: &str) -> anyhow::Result<FileEntry> {
        let mut tree = lock(&self.tree);
        let id = tree.relocate(&path_key(Path::new(from)), &path_key(Path::new(to)))?;
        tree.entry(&id)
    }

    /// Deletes the item at `path` with its descendants, without faults,
    /// reordering or stop points.
    ///
    /// # Errors
    ///
    /// Fails when `path` is the root.
    pub fn remove(&self, path: &str) -> anyhow::Result<()> {
        lock(&self.tree).remove(&path_key(Path::new(path)))
    }

    /// Content of the file at the root-relative `path`, without faults,
    /// reordering or stop points; `None` when no file is there.
    #[must_use]
    pub fn content(&self, path: &Path) -> Option<Vec<u8>> {
        let tree = lock(&self.tree);
        let node = tree.nodes.get(tree.paths.get(&path_key(path))?)?;
        (!node.is_dir).then(|| node.data.clone())
    }

    /// Makes the next call of `op` take `fault`. Faults for one operation
    /// are taken in the order they were injected.
    pub fn inject(&self, op: MockOp, fault: Fault) {
        lock(&self.faults).entry(op).or_default().push_back(fault);
    }

    /// Holds the next call of `op` before it changes the tree.
    #[must_use]
    pub fn stop_at(&self, op: MockOp) -> StopPoint {
        let stop = StopPoint {
            signals: Arc::default(),
        };
        lock(&self.stops).insert(op, stop.clone());
        stop
    }

    async fn gate(&self, op: MockOp) -> anyhow::Result<()> {
        let fault = lock(&self.faults)
            .get_mut(&op)
            .and_then(VecDeque::pop_front);
        match fault {
            Some(Fault::Unreachable) => {
                return Err(TransientError::Unreachable(format!("{op:?}")).into());
            }
            Some(Fault::Busy) => return Err(TransientError::Busy(format!("{op:?}")).into()),
            Some(Fault::Delay(delay)) => tokio::time::sleep(delay).await,
            None => {}
        }
        self.reorder().await;
        let stop = lock(&self.stops).remove(&op);
        if let Some(stop) = stop {
            stop.signals.reached.notify_one();
            stop.signals.released.notified().await;
        }
        Ok(())
    }

    async fn reorder(&self) {
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        let yields = splitmix64(self.seed ^ splitmix64(call)) % MAX_REPLY_YIELDS;
        for _ in 0..yields {
            tokio::task::yield_now().await;
        }
    }
}

#[async_trait]
impl Backend for MockBackend {
    fn id(&self) -> &'static str {
        BACKEND_ID
    }

    fn display_name(&self) -> &'static str {
        "Mock"
    }

    async fn quota(&self) -> anyhow::Result<Option<Quota>> {
        Ok(None)
    }

    async fn changes(&self, _cursor: Option<&Cursor>) -> anyhow::Result<(Vec<Change>, Cursor)> {
        self.gate(MockOp::Changes).await?;
        let tree = lock(&self.tree);
        let changes = tree
            .paths
            .values()
            .filter(|id| id.as_str() != ROOT_ID)
            .filter_map(|id| tree.entry_by_id(id))
            .map(Change::Created)
            .collect();
        Ok((changes, Cursor(tree.clock.to_string())))
    }

    async fn metadata(&self, path: &Path) -> anyhow::Result<FileEntry> {
        self.gate(MockOp::Metadata).await?;
        let key = path_key(path);
        let tree = lock(&self.tree);
        tree.paths
            .get(&key)
            .and_then(|id| tree.entry_by_id(id))
            .ok_or_else(|| not_found(&key))
    }

    async fn download(&self, file: &FileEntry) -> anyhow::Result<Vec<u8>> {
        self.gate(MockOp::Download).await?;
        let tree = lock(&self.tree);
        let node = tree.node(file.id.native_id())?;
        if node.is_dir {
            return Err(BackendError::Conflict(format!("{} is a folder", node.path)).into());
        }
        Ok(node.data.clone())
    }

    async fn upload(
        &self,
        path: &Path,
        data: &[u8],
        _parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        self.gate(MockOp::Upload).await?;
        let mut tree = lock(&self.tree);
        let key = path_key(path);
        let existing = tree.paths.get(&key).cloned();
        let id = match existing {
            Some(id) if self.upload_replaces.load(Ordering::Relaxed) => {
                let now = tree.tick();
                let node = tree.node_mut(&id)?;
                if node.is_dir {
                    return Err(BackendError::Conflict(format!("{key} is a folder")).into());
                }
                node.data = data.to_vec();
                node.modified = now;
                node.changed = now;
                id
            }
            _ => tree.insert(&key, false, data)?,
        };
        tree.entry(&id)
    }

    async fn update(&self, file_id: &FileId, data: &[u8]) -> anyhow::Result<FileEntry> {
        self.gate(MockOp::Update).await?;
        let mut tree = lock(&self.tree);
        let now = tree.tick();
        let node = tree.node_mut(file_id.native_id())?;
        if node.is_dir {
            return Err(BackendError::Conflict(format!("{} is a folder", node.path)).into());
        }
        node.data = data.to_vec();
        node.modified = now;
        node.changed = now;
        tree.entry(file_id.native_id())
    }

    async fn create_dir(&self, path: &Path) -> anyhow::Result<FileEntry> {
        self.gate(MockOp::CreateDir).await?;
        let mut tree = lock(&self.tree);
        let id = tree.insert(&path_key(path), true, &[])?;
        tree.entry(&id)
    }

    async fn create_dir_with_parent(
        &self,
        name: &str,
        parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        self.gate(MockOp::CreateDir).await?;
        let mut tree = lock(&self.tree);
        let key = join(&tree.node(parent_id.native_id())?.path, name);
        let id = tree.insert(&key, true, &[])?;
        tree.entry(&id)
    }

    async fn delete(&self, file: &FileEntry) -> anyhow::Result<()> {
        self.gate(MockOp::Delete).await?;
        let mut tree = lock(&self.tree);
        let key = tree.node(file.id.native_id())?.path.clone();
        tree.remove(&key)
    }

    async fn move_entry(&self, src: &Path, dst: &Path) -> anyhow::Result<FileEntry> {
        self.gate(MockOp::Move).await?;
        let mut tree = lock(&self.tree);
        let id = tree.relocate(&path_key(src), &path_key(dst))?;
        tree.entry(&id)
    }

    async fn move_by_id(
        &self,
        src_id: &FileId,
        dst_parent_id: &FileId,
        new_name: &str,
    ) -> anyhow::Result<FileEntry> {
        self.gate(MockOp::Move).await?;
        let mut tree = lock(&self.tree);
        let src = tree.node(src_id.native_id())?.path.clone();
        let dst = join(&tree.node(dst_parent_id.native_id())?.path, new_name);
        let id = tree.relocate(&src, &dst)?;
        tree.entry(&id)
    }

    async fn list_children(&self, parent_native_id: &str) -> anyhow::Result<Vec<FileEntry>> {
        self.gate(MockOp::ListChildren).await?;
        let tree = lock(&self.tree);
        let parent = tree.node(parent_native_id)?.path.clone();
        Ok(tree
            .paths
            .iter()
            .filter(|(key, id)| id.as_str() != ROOT_ID && parent_key(key) == parent)
            .filter_map(|(_, id)| tree.entry_by_id(id))
            .collect())
    }

    async fn revision(&self, entry: &FileEntry) -> anyhow::Result<Revision> {
        self.gate(MockOp::Revision).await?;
        let tree = lock(&self.tree);
        let id = entry.id.native_id();
        Ok(tree.node(id)?.revision(id))
    }

    async fn replace(&self, source: &Path, target: &Path) -> anyhow::Result<(FileEntry, Revision)> {
        self.gate(MockOp::Replace).await?;
        let mut tree = lock(&self.tree);
        let source = path_key(source);
        let target = path_key(target);
        let source_id = tree
            .paths
            .get(&source)
            .cloned()
            .ok_or_else(|| not_found(&source))?;
        if tree.node(&source_id)?.is_dir {
            return Err(BackendError::Conflict(format!("{source} is a folder")).into());
        }
        if let Some(target_id) = tree.paths.get(&target).cloned() {
            if tree.node(&target_id)?.is_dir {
                return Err(BackendError::Conflict(format!("{target} is a folder")).into());
            }
            tree.remove(&target)?;
        }
        let id = tree.relocate(&source, &target)?;
        let revision = tree.node(&id)?.revision(&id);
        Ok((tree.entry(&id)?, revision))
    }

    async fn poll_interval(&self) -> Option<Duration> {
        None
    }
}

#[derive(Debug)]
struct Node {
    path: String,
    is_dir: bool,
    data: Vec<u8>,
    modified: DateTime<Utc>,
    changed: DateTime<Utc>,
}

impl Node {
    fn size(&self) -> Option<u64> {
        (!self.is_dir).then(|| u64::try_from(self.data.len()).unwrap_or(u64::MAX))
    }

    fn revision(&self, id: &str) -> Revision {
        Revision {
            stable_id: id.to_owned(),
            size: self.size(),
            last_write_time: Some(self.modified),
            change_time: Some(self.changed),
        }
    }
}

/// Nodes keyed by their stable native id, and the id at each root-relative path.
#[derive(Debug)]
struct Tree {
    nodes: HashMap<String, Node>,
    paths: BTreeMap<String, String>,
    next_id: u64,
    clock: i64,
}

impl Tree {
    fn new() -> Self {
        let mut tree = Self {
            nodes: HashMap::new(),
            paths: BTreeMap::new(),
            next_id: 0,
            clock: 0,
        };
        let now = tree.tick();
        tree.nodes.insert(
            ROOT_ID.to_owned(),
            Node {
                path: String::new(),
                is_dir: true,
                data: Vec::new(),
                modified: now,
                changed: now,
            },
        );
        tree.paths.insert(String::new(), ROOT_ID.to_owned());
        tree
    }

    fn tick(&mut self) -> DateTime<Utc> {
        self.clock = self.clock.saturating_add(1);
        DateTime::from_timestamp(CLOCK_EPOCH_SECS.saturating_add(self.clock), 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
    }

    fn node(&self, id: &str) -> anyhow::Result<&Node> {
        self.nodes.get(id).ok_or_else(|| not_found(id))
    }

    fn node_mut(&mut self, id: &str) -> anyhow::Result<&mut Node> {
        self.nodes.get_mut(id).ok_or_else(|| not_found(id))
    }

    fn entry(&self, id: &str) -> anyhow::Result<FileEntry> {
        self.entry_by_id(id).ok_or_else(|| not_found(id))
    }

    fn entry_by_id(&self, id: &str) -> Option<FileEntry> {
        let node = self.nodes.get(id)?;
        let parent = self
            .paths
            .get(&parent_key(&node.path))
            .map_or(ROOT_ID, String::as_str);
        let name = node.path.rsplit('/').next().unwrap_or_default().to_owned();
        let entry = if node.is_dir {
            FileEntry::dir(item_id(id), item_id(parent), name)
        } else {
            FileEntry::file(item_id(id), item_id(parent), name)
        };
        let mut entry = entry.with_path(node.path.clone()).with_size(node.size());
        entry.mod_time = Some(node.modified);
        Some(entry)
    }

    fn seed_ancestors(&mut self, key: &str) {
        let mut ancestor = String::new();
        for segment in parent_key(key).split('/').filter(|s| !s.is_empty()) {
            ancestor = join(&ancestor, segment);
            if !self.paths.contains_key(&ancestor) {
                self.add_node(&ancestor, true, &[]);
            }
        }
    }

    fn seed_file(&mut self, key: &str, data: &[u8]) -> String {
        self.seed_ancestors(key);
        match self.paths.get(key).cloned() {
            Some(id) => {
                let now = self.tick();
                if let Some(node) = self.nodes.get_mut(&id) {
                    node.data = data.to_vec();
                    node.modified = now;
                    node.changed = now;
                }
                id
            }
            None => self.add_node(key, false, data),
        }
    }

    fn insert(&mut self, key: &str, is_dir: bool, data: &[u8]) -> anyhow::Result<String> {
        if self.paths.contains_key(key) {
            return Err(BackendError::Conflict(format!("{key} exists")).into());
        }
        self.require_folder(&parent_key(key))?;
        Ok(self.add_node(key, is_dir, data))
    }

    fn add_node(&mut self, key: &str, is_dir: bool, data: &[u8]) -> String {
        self.next_id = self.next_id.saturating_add(1);
        let id = format!("n{}", self.next_id);
        let now = self.tick();
        self.nodes.insert(
            id.clone(),
            Node {
                path: key.to_owned(),
                is_dir,
                data: data.to_vec(),
                modified: now,
                changed: now,
            },
        );
        self.paths.insert(key.to_owned(), id.clone());
        self.touch_parent(key, now);
        id
    }

    /// A create, delete or rename moves the parent folder's times.
    fn touch_parent(&mut self, key: &str, now: DateTime<Utc>) {
        let Some(parent_id) = self.paths.get(&parent_key(key)) else {
            return;
        };
        if let Some(parent) = self.nodes.get_mut(parent_id) {
            parent.modified = now;
            parent.changed = now;
        }
    }

    fn require_folder(&self, key: &str) -> anyhow::Result<()> {
        let id = self.paths.get(key).ok_or_else(|| not_found(key))?;
        if self.node(id)?.is_dir {
            Ok(())
        } else {
            Err(BackendError::Conflict(format!("{key} is not a folder")).into())
        }
    }

    fn subtree(&self, key: &str) -> Vec<String> {
        let prefix = format!("{key}/");
        self.paths
            .keys()
            .filter(|path| path.as_str() == key || path.starts_with(&prefix))
            .cloned()
            .collect()
    }

    fn remove(&mut self, key: &str) -> anyhow::Result<()> {
        if key.is_empty() {
            return Err(BackendError::Forbidden("the root cannot be deleted".to_owned()).into());
        }
        let subtree = self.subtree(key);
        if subtree.is_empty() {
            return Err(not_found(key));
        }
        for path in subtree {
            if let Some(id) = self.paths.remove(&path) {
                self.nodes.remove(&id);
            }
        }
        let now = self.tick();
        self.touch_parent(key, now);
        Ok(())
    }

    fn relocate(&mut self, src: &str, dst: &str) -> anyhow::Result<String> {
        if src.is_empty() {
            return Err(BackendError::Forbidden("the root cannot be moved".to_owned()).into());
        }
        let id = self.paths.get(src).cloned().ok_or_else(|| not_found(src))?;
        if self.paths.contains_key(dst) {
            return Err(BackendError::Conflict(format!("{dst} exists")).into());
        }
        if dst.starts_with(&format!("{src}/")) {
            return Err(BackendError::Conflict(format!("{dst} is inside {src}")).into());
        }
        self.require_folder(&parent_key(dst))?;
        for path in self.subtree(src) {
            let Some(suffix) = path.strip_prefix(src) else {
                continue;
            };
            let moved = format!("{dst}{suffix}");
            if let Some(node_id) = self.paths.remove(&path) {
                if let Some(node) = self.nodes.get_mut(&node_id) {
                    node.path.clone_from(&moved);
                }
                self.paths.insert(moved, node_id);
            }
        }
        let now = self.tick();
        self.node_mut(&id)?.changed = now;
        self.touch_parent(src, now);
        self.touch_parent(dst, now);
        Ok(id)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn item_id(native_id: &str) -> ItemId {
    ItemId::new(BACKEND_ID, native_id)
}

fn not_found(subject: &str) -> anyhow::Error {
    BackendError::NotFound(subject.to_owned()).into()
}

fn path_key(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(segment) => Some(segment.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn parent_key(key: &str) -> String {
    key.rsplit_once('/')
        .map_or_else(String::new, |(parent, _)| parent.to_owned())
}

fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

const fn splitmix64(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
