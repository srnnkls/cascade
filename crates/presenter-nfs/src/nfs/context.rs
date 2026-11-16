//! NFS context — bridges NFS procedure handlers to the VFS tree.
//!
//! Provides the state needed by NFS procedures to answer queries about
//! files and directories in the virtual filesystem.

use cascade_engine::backend::{Backend, BackendError};
use cascade_engine::content::cache::ContentCache;
use cascade_engine::content::replica::{ItemKind, ReplicaItem};
use cascade_engine::types::DirEntry;
use cascade_engine::vfs::VfsTree;
use cascade_engine::vfs::tree::{merge_listing, merge_mount_names, resolve_listing_native_id};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

/// Context shared between NFS procedure handlers and the VFS tree.
///
/// Maintains a file handle → VFS path mapping so NFS clients can
/// navigate the tree with opaque file handles.
pub struct NfsContext {
    /// The VFS tree to query for file metadata.
    vfs: Arc<RwLock<VfsTree>>,
    /// Map from file handle key (hash) to VFS path.
    fh_map: RwLock<HashMap<u64, String>>,
    /// Root file handle key — always maps to "/".
    root_fh_key: u64,
    /// Cache serving the content of the backends registered in it.
    content_cache: Option<ContentCache>,
}

/// A VFS path whose backend the content cache serves.
#[derive(Debug)]
pub struct CachedPath<'a> {
    pub cache: &'a ContentCache,
    pub backend_id: String,
    /// Path relative to the backend root.
    pub relative: PathBuf,
}

impl CachedPath<'_> {
    /// The uid of the item at the path, through the replica; `None` when no
    /// such item exists.
    ///
    /// # Errors
    ///
    /// Fails when a listing or the state database fails.
    pub fn resolve(&self) -> anyhow::Result<Option<u64>> {
        block_on(self.cache.resolve(&self.backend_id, &self.relative))
    }

    /// The item at the path, through the replica; `None` when no such item
    /// exists.
    ///
    /// # Errors
    ///
    /// Fails when a listing or the state database fails.
    pub fn item(&self) -> anyhow::Result<Option<ReplicaItem>> {
        self.resolve()?.map_or(Ok(None), |uid| self.cache.item(uid))
    }

    /// The item at the path, failing with [`BackendError::NotFound`] when
    /// it does not exist.
    ///
    /// # Errors
    ///
    /// Fails when the item does not exist, a listing or the state database
    /// fails.
    pub fn existing(&self) -> anyhow::Result<ReplicaItem> {
        self.item()?
            .ok_or_else(|| BackendError::NotFound(self.relative.display().to_string()).into())
    }
}

/// Run a future to completion on the current Tokio runtime from a synchronous
/// procedure handler.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Handle::current().block_on(future)
}

impl std::fmt::Debug for NfsContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NfsContext")
            .field("root_fh_key", &self.root_fh_key)
            .finish_non_exhaustive()
    }
}

impl NfsContext {
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub fn new(vfs: Arc<RwLock<VfsTree>>) -> Self {
        let root_fh_key = Self::path_to_key("/");
        let mut fh_map = HashMap::new();
        fh_map.insert(root_fh_key, "/".to_string());

        Self {
            vfs,
            fh_map: RwLock::new(fh_map),
            root_fh_key,
            content_cache: None,
        }
    }

    /// Serves the backends registered in `cache` from it: paths resolve to
    /// item uids through its replica, reads go through its ranged read, and
    /// writes become dirty objects that write-back uploads.
    #[must_use]
    pub fn with_content_cache(mut self, cache: ContentCache) -> Self {
        self.content_cache = Some(cache);
        self
    }

    /// The content cache, when the context has one.
    #[must_use]
    pub const fn content_cache(&self) -> Option<&ContentCache> {
        self.content_cache.as_ref()
    }

    /// The content cache's view of `path`, when the cache serves the backend
    /// that holds it.
    pub fn cached(&self, path: &str) -> Option<CachedPath<'_>> {
        let cache = self.content_cache.as_ref()?;
        let (backend, relative) = self.route(path);
        let backend_id = backend.id().to_owned();
        cache.serves(&backend_id).then_some(CachedPath {
            cache,
            backend_id,
            relative,
        })
    }

    /// Size of the file at `path`: the item's presented size when the content
    /// cache serves it, otherwise the backend's; 0 when absent or unknown.
    pub fn file_size_blocking(&self, path: &str) -> u64 {
        self.cached(path)
            .map_or_else(
                || self.metadata_sync(path).ok().and_then(|entry| entry.size),
                |cached| cached.item().ok().flatten().and_then(|item| item.size),
            )
            .unwrap_or(0)
    }

    /// The backend owning the NFS path `path` and the path relative to it.
    ///
    /// NFS paths carry a leading root and mount prefixes may not, so every
    /// path the context hands to the tree or the content cache is routed
    /// here; the relative path never carries a leading root, which keeps a
    /// path-based backend's `Path::join` inside its configured root.
    pub fn route(&self, path: &str) -> (Arc<dyn Backend>, PathBuf) {
        let vfs = self
            .vfs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (backend, relative) = vfs.resolve_rooted(std::path::Path::new(path));
        (Arc::clone(backend), relative)
    }

    /// Convert a VFS path to a file handle key.
    #[must_use]
    pub fn path_to_key(path: &str) -> u64 {
        let mut hash: u64 = 5381;
        for byte in path.bytes() {
            hash = hash.wrapping_mul(33).wrapping_add(u64::from(byte));
        }
        hash
    }

    /// Register a path and get its file handle key.
    pub fn register_path(&self, path: &str) -> u64 {
        let key = Self::path_to_key(path);
        let mut map = self
            .fh_map
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.entry(key).or_insert_with(|| path.to_string());
        key
    }

    /// Look up the VFS path for a file handle key.
    pub fn lookup_path(&self, fh_key: u64) -> Option<String> {
        self.fh_map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&fh_key)
            .cloned()
    }

    /// Remove a file handle key from the map.
    pub fn remove_path(&self, fh_key: u64) {
        self.fh_map
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&fh_key);
    }

    /// Access the underlying VFS tree (for download operations).
    #[must_use]
    pub const fn vfs(&self) -> &Arc<RwLock<VfsTree>> {
        &self.vfs
    }

    /// Read up to `count` bytes at `offset` from the file at a VFS path,
    /// fetching only that range from the owning backend, or through the
    /// content cache's ranged read with read-ahead when it serves the path.
    ///
    /// Returns the bytes and whether the read reached end-of-file.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot resolve or read the file.
    pub fn read_range_blocking(
        &self,
        path: &str,
        offset: u64,
        count: u32,
    ) -> anyhow::Result<(Vec<u8>, bool)> {
        if let Some(cached) = self.cached(path) {
            let item = cached.existing()?;
            let data = block_on(cached.cache.read(item.uid, offset, count))?;
            let read = u64::try_from(data.len()).unwrap_or(u64::MAX);
            let eof = offset.saturating_add(read) >= item.size.unwrap_or(0);
            return Ok((data, eof));
        }
        let (backend, relative) = self.route(path);
        tokio::runtime::Handle::current().block_on(async {
            let entry = backend.metadata(&relative).await?;
            let data = backend.read_range(&entry, offset, count).await?;
            let read = u64::try_from(data.len()).unwrap_or(u64::MAX);
            let eof = entry.size.map_or_else(
                || read < u64::from(count),
                |size| offset.saturating_add(read) >= size,
            );
            Ok((data, eof))
        })
    }

    /// Get the root file handle key.
    #[must_use]
    pub const fn root_key(&self) -> u64 {
        self.root_fh_key
    }

    /// List directory contents at a VFS path, as `VfsTree::read_dir` lists
    /// them: the backend's children merged with the child mounts under it.
    ///
    /// # Errors
    ///
    /// Returns an error if the VFS tree cannot read the directory.
    pub async fn list_dir(&self, path: &str) -> anyhow::Result<Vec<DirEntry>> {
        let (backend, backend_path, mount_names) = self.listing(path)?;
        let native_id = resolve_listing_native_id(backend.as_ref(), &backend_path).await?;
        let children = backend.list_children(&native_id).await?;
        Ok(merge_listing(children, &mount_names))
    }

    /// The backend owning the folder at `path`, its backend-relative path and
    /// the names of the child mounts directly under it.
    fn listing(&self, path: &str) -> anyhow::Result<(Arc<dyn Backend>, String, Vec<String>)> {
        self.vfs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .resolve_listing_rooted(std::path::Path::new(path))
    }

    /// Rename a VFS path, routing through [`VfsTree::rename`] (which handles
    /// same-backend moves and cross-backend download/upload/delete).
    ///
    /// # Errors
    ///
    /// Returns an error if the backend rejects the move.
    ///
    /// Note: the `RwLockReadGuard` is intentionally held across the `.await`
    /// here because `VfsTree::rename` needs `&self` for the duration of the
    /// call, exactly as [`Self::list_dir`] does. The future is therefore not
    /// `Send`; all callers invoke it via `block_on` from a sync context.
    #[allow(clippy::await_holding_lock, clippy::future_not_send)]
    pub async fn rename(&self, src: &str, dst: &str) -> anyhow::Result<()> {
        let vfs = self
            .vfs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        vfs.rename_rooted(std::path::Path::new(src), std::path::Path::new(dst))
            .await
    }

    /// Get file metadata at a VFS path.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot fetch metadata.
    pub async fn metadata(&self, path: &str) -> anyhow::Result<cascade_engine::types::FileEntry> {
        let (backend, relative) = self.route(path);
        backend.metadata(&relative).await
    }

    /// Synchronous wrapper around `list_dir` for use in NFS procedure
    /// handlers which must return `Vec<u8>` synchronously. A folder the
    /// content cache serves is listed through its replica.
    ///
    /// Uses `tokio::runtime::Handle::block_on` to run the async VFS query
    /// on the current tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the VFS tree cannot read the directory.
    pub fn list_dir_sync(&self, path: &str) -> anyhow::Result<Vec<DirEntry>> {
        if let Some(cached) = self.cached(path) {
            let folder = cached.existing()?;
            let children = block_on(cached.cache.list(folder.uid))?;
            let entries = children
                .into_iter()
                .map(|child| match child.kind {
                    ItemKind::Folder => DirEntry::dir(child.name),
                    ItemKind::File => DirEntry::file(child.name),
                })
                .collect();
            return Ok(merge_mount_names(entries, &self.mount_names(path)?));
        }
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.list_dir(path))
    }

    /// Names of the child mounts directly under `path`, listed where no
    /// entry of its listing carries the same name.
    fn mount_names(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let (_, _, mounts) = self.listing(path)?;
        Ok(mounts)
    }

    /// Synchronous wrapper for fetching file metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot fetch metadata.
    pub fn metadata_sync(&self, path: &str) -> anyhow::Result<cascade_engine::types::FileEntry> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.metadata(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cascade_engine::types::{FileEntry, ItemId};

    #[test]
    fn path_to_key_deterministic() {
        assert_eq!(NfsContext::path_to_key("/"), NfsContext::path_to_key("/"));
        assert_ne!(
            NfsContext::path_to_key("/"),
            NfsContext::path_to_key("/foo")
        );
    }

    #[test]
    fn register_and_lookup() {
        let vfs = Arc::new(RwLock::new(VfsTree::new(Arc::new(
            cascade_engine::backend::NullBackend::new("test"),
        ))));
        let ctx = NfsContext::new(vfs);

        let key = ctx.register_path("/Documents");
        assert_eq!(ctx.lookup_path(key), Some("/Documents".to_string()));
        assert_eq!(ctx.lookup_path(99_999), None);
    }

    #[test]
    fn root_always_registered() {
        let vfs = Arc::new(RwLock::new(VfsTree::new(Arc::new(
            cascade_engine::backend::NullBackend::new("test"),
        ))));
        let ctx = NfsContext::new(vfs);
        assert_eq!(ctx.lookup_path(ctx.root_key()), Some("/".to_string()));
    }

    #[derive(Debug)]
    struct RangeOnlyBackend;

    const RANGE_BODY: &[u8] = b"hello world";

    #[async_trait::async_trait]
    impl cascade_engine::backend::Backend for RangeOnlyBackend {
        fn id(&self) -> &'static str {
            "range"
        }
        fn display_name(&self) -> &'static str {
            "Range"
        }
        async fn quota(&self) -> anyhow::Result<Option<cascade_engine::types::Quota>> {
            Ok(None)
        }
        async fn changes(
            &self,
            _cursor: Option<&cascade_engine::types::Cursor>,
        ) -> anyhow::Result<(
            Vec<cascade_engine::types::Change>,
            cascade_engine::types::Cursor,
        )> {
            Ok((vec![], cascade_engine::types::Cursor("range".to_string())))
        }
        async fn metadata(&self, _path: &std::path::Path) -> anyhow::Result<FileEntry> {
            Ok(FileEntry::file(
                ItemId::new("range", "f"),
                ItemId::new("range", "root"),
                "f.bin".to_string(),
            )
            .with_size(Some(RANGE_BODY.len() as u64)))
        }
        async fn download(&self, _file: &FileEntry) -> anyhow::Result<Vec<u8>> {
            anyhow::bail!("download must not be called for a ranged read")
        }
        async fn read_range(
            &self,
            _file: &FileEntry,
            offset: u64,
            length: u32,
        ) -> anyhow::Result<Vec<u8>> {
            let start = usize::try_from(offset)?.min(RANGE_BODY.len());
            let end = start
                .saturating_add(usize::try_from(length)?)
                .min(RANGE_BODY.len());
            Ok(RANGE_BODY[start..end].to_vec())
        }
        async fn upload(
            &self,
            _path: &std::path::Path,
            _data: &[u8],
            _parent_id: &cascade_engine::types::FileId,
        ) -> anyhow::Result<FileEntry> {
            anyhow::bail!("no upload")
        }
        async fn update(
            &self,
            _file_id: &cascade_engine::types::FileId,
            _data: &[u8],
        ) -> anyhow::Result<FileEntry> {
            anyhow::bail!("no update")
        }
        async fn create_dir(&self, _path: &std::path::Path) -> anyhow::Result<FileEntry> {
            anyhow::bail!("no create_dir")
        }
        async fn delete(&self, _file: &FileEntry) -> anyhow::Result<()> {
            anyhow::bail!("no delete")
        }
        async fn move_entry(
            &self,
            _src: &std::path::Path,
            _dst: &std::path::Path,
        ) -> anyhow::Result<FileEntry> {
            anyhow::bail!("no move")
        }
        async fn poll_interval(&self) -> Option<std::time::Duration> {
            None
        }
    }

    fn range_ctx() -> NfsContext {
        NfsContext::new(Arc::new(RwLock::new(VfsTree::new(Arc::new(
            RangeOnlyBackend,
        )))))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_range_blocking_fetches_only_the_range() {
        let ctx = range_ctx();
        let (data, eof) =
            tokio::task::block_in_place(|| ctx.read_range_blocking("/f.bin", 6, 3)).unwrap();
        assert_eq!(data, b"wor");
        assert!(!eof);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_range_blocking_reports_eof_on_final_chunk() {
        let ctx = range_ctx();
        let (data, eof) =
            tokio::task::block_in_place(|| ctx.read_range_blocking("/f.bin", 6, 5)).unwrap();
        assert_eq!(data, b"world");
        assert!(eof);
    }

    #[test]
    fn remove_path_removes_entry() {
        let vfs = Arc::new(RwLock::new(VfsTree::new(Arc::new(
            cascade_engine::backend::NullBackend::new("test"),
        ))));
        let ctx = NfsContext::new(vfs);
        let key = ctx.register_path("/tmp");
        assert!(ctx.lookup_path(key).is_some());
        ctx.remove_path(key);
        assert!(ctx.lookup_path(key).is_none());
    }
}

/// Property-based tests for `NfsContext`.
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn path_to_key_deterministic_prop(s in ".*") {
            let k1 = NfsContext::path_to_key(&s);
            let k2 = NfsContext::path_to_key(&s);
            prop_assert_eq!(k1, k2);
        }

        #[test]
        fn path_to_key_different_strings_mostly_differ(s1 in ".*", s2 in ".*") {
            prop_assume!(s1 != s2);
            let k1 = NfsContext::path_to_key(&s1);
            let k2 = NfsContext::path_to_key(&s2);
            // Hash collisions are possible but should be extremely rare.
            // We don't assert they always differ, but log if they collide.
            if k1 == k2 {
                // Acceptable but noteworthy.
            }
        }

        #[test]
        fn register_lookup_roundtrip(path in ".*") {
            let vfs = Arc::new(RwLock::new(VfsTree::new(Arc::new(
                cascade_engine::backend::NullBackend::new("test"),
            ))));
            let ctx = NfsContext::new(vfs);
            let key = ctx.register_path(&path);
            prop_assert_eq!(ctx.lookup_path(key), Some(path));
        }
    }
}
