//! Shared write-path helpers for the NFS presenters (v3 and v4).
//!
//! These funnel every NFS write into the exact engine operations the other
//! presenters use — `Backend::upload`, `Backend::update`, `Backend::create_dir`,
//! `Backend::delete`, and `VfsTree::rename` — resolved through `VfsTree::resolve`
//! just as the `WebDAV` presenter does. There is no parallel write path in the
//! engine; the NFS layer only translates RFC 1813 / RFC 5661 procedure arguments
//! into those calls and maps the resulting errors back to NFS status codes.
//!
//! When the context has a content cache serving the path's backend, paths
//! resolve to item uids through its replica instead: WRITE and SETATTR size
//! become dirty objects that write-back uploads, CREATE, REMOVE, RENAME and
//! MKDIR apply directly through the cache, and names the backend cannot store are refused
//! before anything is applied.
//!
//! Writes are unauthenticated at the NFS layer by protocol — the trust model is
//! a localhost mount. A write must nonetheless not escape the export root, so
//! every path is validated by [`validate_vfs_path`] before it reaches a backend.

use std::path::Path;
#[cfg(test)]
use std::sync::Arc;

use cascade_engine::backend::{Backend, BackendError};
use cascade_engine::content::domain::DomainError;
use cascade_engine::content::names::{NameError, check_name};
use cascade_engine::content::replica::ItemKind;
use cascade_engine::types::FileId;

use super::context::{CachedPath, NfsContext};

/// A write rejected before it reached a backend, or a categorised backend error.
///
/// Presenters translate each variant into the protocol-appropriate status code
/// (`NFS3ERR_*` / `NFS4ERR_*`).
#[derive(Debug, Clone, Copy)]
pub enum WriteError {
    /// The path attempted to escape the export root (`..`, absolute escape).
    Traversal,
    /// The target resource does not exist.
    NotFound,
    /// The operation was refused for permission reasons.
    Forbidden,
    /// The operation conflicts with current state (name taken, dir not empty).
    Conflict,
    /// The argument was malformed (empty name, bad path component).
    Invalid,
    /// The backend ran out of space.
    NoSpace,
    /// The underlying backend reported an I/O failure.
    Io,
}

impl WriteError {
    /// Categorise an `anyhow::Error` from a backend call, downcasting to
    /// [`BackendError`] where the backend tagged the failure, otherwise
    /// inspecting the error chain for a disk-full `io::Error`, and finally
    /// falling back to a generic I/O failure.
    fn from_backend(err: &anyhow::Error) -> Self {
        if err.downcast_ref::<NameError>().is_some() {
            return Self::Invalid;
        }
        if matches!(
            err.downcast_ref::<DomainError>(),
            Some(DomainError::DirectoryNotEmpty)
        ) {
            return Self::Conflict;
        }
        if let Some(backend_err) = err.downcast_ref::<BackendError>() {
            return match backend_err {
                BackendError::NotFound(_) => Self::NotFound,
                BackendError::Forbidden(_) | BackendError::ReadOnly(_) => Self::Forbidden,
                BackendError::Conflict(_) => Self::Conflict,
            };
        }
        for cause in err.chain() {
            if let Some(io_err) = cause.downcast_ref::<std::io::Error>()
                && io_err.kind() == std::io::ErrorKind::StorageFull
            {
                return Self::NoSpace;
            }
        }
        Self::Io
    }
}

/// Reject any VFS path that could escape the export root.
///
/// VFS paths are server-absolute (`/a/b`). A path is rejected if it is empty,
/// not rooted at `/`, or contains a `.` or `..` component. Plain `/` is the
/// export root and is valid.
///
/// # Errors
///
/// Returns [`WriteError::Traversal`] for any path that is not safely contained
/// within the export.
pub fn validate_vfs_path(path: &str) -> Result<(), WriteError> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(WriteError::Traversal);
    }
    for component in path.split('/') {
        if component.is_empty() {
            // Leading slash and any doubled slash produce empty components;
            // a trailing slash likewise. These are benign for a rooted path.
            continue;
        }
        if component == "." || component == ".." {
            return Err(WriteError::Traversal);
        }
    }
    Ok(())
}

/// Reject a single path component (a file or directory name) that is empty or
/// would alter the path's depth.
///
/// # Errors
///
/// Returns [`WriteError::Invalid`] for an empty name and [`WriteError::Traversal`]
/// for `.`, `..`, or a name containing a path separator.
pub fn validate_name(name: &str) -> Result<(), WriteError> {
    if name.is_empty() {
        return Err(WriteError::Invalid);
    }
    if name == "." || name == ".." {
        return Err(WriteError::Traversal);
    }
    if name.contains('/') {
        return Err(WriteError::Traversal);
    }
    Ok(())
}

/// Refuse a name the content cache's backends cannot store (TR-10), before
/// anything is applied.
///
/// # Errors
///
/// Returns [`WriteError::Invalid`] for a rejected name when the context has a
/// content cache.
pub fn check_cached_name(ctx: &NfsContext, name: &str) -> Result<(), WriteError> {
    if ctx.content_cache().is_some() {
        check_name(name).map_err(|_| WriteError::Invalid)?;
    }
    Ok(())
}

/// The uid of the item at `cached`, failing with [`WriteError::NotFound`]
/// when it does not exist.
fn existing_uid(cached: &CachedPath<'_>) -> Result<u64, WriteError> {
    cached
        .resolve()
        .map_err(|e| WriteError::from_backend(&e))?
        .ok_or(WriteError::NotFound)
}

/// Create an empty file at `cached` through the content cache and return its
/// uid.
fn create_cached(cached: &CachedPath<'_>) -> Result<u64, WriteError> {
    let name = cached
        .relative
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(WriteError::Invalid)?;
    let parent = CachedPath {
        cache: cached.cache,
        backend_id: cached.backend_id.clone(),
        relative: cached
            .relative
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default(),
    };
    let parent_uid = existing_uid(&parent)?;
    block_on(cached.cache.create_file(parent_uid, name, None))
        .map(|item| item.uid)
        .map_err(|e| WriteError::from_backend(&e))
}

/// Delete the item at `cached` through the content cache. A kind other than
/// `expect_dir` is [`WriteError::Invalid`] and a folder with children
/// [`WriteError::Conflict`].
fn remove_cached(cached: &CachedPath<'_>, expect_dir: Option<bool>) -> Result<(), WriteError> {
    let item = cached
        .existing()
        .map_err(|e| WriteError::from_backend(&e))?;
    let is_dir = item.kind == ItemKind::Folder;
    if expect_dir.is_some_and(|expect_dir| expect_dir != is_dir) {
        return Err(WriteError::Invalid);
    }
    let cache = cached.cache;
    let deleted = if is_dir {
        block_on(cache.delete_item(item.uid, false))
    } else {
        block_on(cache.delete(item.uid))
    };
    deleted.map_err(|e| WriteError::from_backend(&e))
}

/// Join a validated parent path and child name into a server-absolute VFS path.
fn join_child(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

/// Run an async backend operation to completion on the current Tokio runtime.
///
/// All NFS procedure handlers are synchronous (they return `Vec<u8>`), so —
/// exactly as the read path does — the write path bridges into async via
/// `block_on` on the ambient runtime handle.
fn block_on<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::runtime::Handle::current().block_on(future)
}

/// Write `data` at `offset` into the file at `path`, returning the file's new
/// total size.
///
/// Mirrors a POSIX `pwrite`: the existing content is fetched, `data` is spliced
/// in at `offset` (extending with zero bytes if `offset` is past the current
/// end), and the whole content is written back through `Backend::update` when
/// the file already exists or `Backend::upload` when it does not. The download
/// is required because the engine's write contract replaces whole-file content;
/// `Minimal` cache mode keeps nothing on local disk beyond this in-flight
/// buffer.
///
/// # Errors
///
/// Returns a [`WriteError`] if the path escapes the export, the parent cannot be
/// resolved, or the backend rejects the write.
pub fn write_file(
    ctx: &NfsContext,
    path: &str,
    offset: u64,
    data: &[u8],
) -> Result<u64, WriteError> {
    validate_vfs_path(path)?;
    if let Some(cached) = ctx.cached(path) {
        let resolved = cached.resolve().map_err(|e| WriteError::from_backend(&e))?;
        let uid = if let Some(uid) = resolved {
            uid
        } else {
            let name = path.rsplit('/').next().unwrap_or_default();
            check_cached_name(ctx, name)?;
            create_cached(&cached)?
        };
        let item = block_on(cached.cache.write_at(uid, offset, data))
            .map_err(|e| WriteError::from_backend(&e))?;
        return Ok(item.size.unwrap_or(0));
    }
    let (backend, relative) = ctx.route(path);
    let off = usize::try_from(offset).map_err(|_| WriteError::Invalid)?;

    block_on(async move {
        let existing = lookup_existing(backend.as_ref(), &relative).await?;

        let mut buf = if let Some(entry) = &existing {
            backend
                .download(entry)
                .await
                .map_err(|e| WriteError::from_backend(&e))?
        } else {
            Vec::new()
        };

        if buf.len() < off {
            buf.resize(off, 0);
        }
        let end = off.saturating_add(data.len());
        if buf.len() < end {
            buf.resize(end, 0);
        }
        if let Some(slot) = buf.get_mut(off..end) {
            slot.copy_from_slice(data);
        } else {
            return Err(WriteError::Io);
        }

        let new_size = u64::try_from(buf.len()).map_err(|_| WriteError::Io)?;

        if let Some(entry) = existing {
            let file_id = FileId(entry.id.0.clone());
            backend
                .update(&file_id, &buf)
                .await
                .map_err(|e| WriteError::from_backend(&e))?;
        } else {
            let parent_id = parent_file_id(backend.as_ref(), &relative).await;
            backend
                .upload(&relative, &buf, &parent_id)
                .await
                .map_err(|e| WriteError::from_backend(&e))?;
        }

        Ok(new_size)
    })
}

/// The RFC 1813 §3.3.8 `createmode3` discriminant governing how `CREATE`
/// treats a name that already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateMode {
    /// `UNCHECKED` — create if absent, otherwise leave the existing file's
    /// content untouched (a no-op create). Never truncates.
    Unchecked,
    /// `GUARDED` — fail with [`WriteError::Conflict`] if the name already
    /// exists.
    Guarded,
    /// `EXCLUSIVE` — create only if absent, keyed by a client verifier. Cascade
    /// has no per-file verifier store, so it applies the same existence guard as
    /// `GUARDED`: an existing name is refused.
    Exclusive,
}

/// Create a regular file named `name` in the directory at `parent_path`,
/// honouring the `createmode3` semantics, and return the new file's
/// server-absolute VFS path.
///
/// `GUARDED`/`EXCLUSIVE` refuse an existing name with [`WriteError::Conflict`]
/// (which the v3 handler maps to `NFS3ERR_EXIST`). `UNCHECKED` on an existing
/// name is a no-op that preserves the existing content — it must never call
/// `Backend::upload`, which writes a zero-length buffer and truncates. A fresh
/// create uploads empty content.
///
/// # Errors
///
/// Returns a [`WriteError`] if the parent path escapes the export, the name is
/// invalid, the mode forbids an existing target, or the backend rejects the
/// upload.
pub fn create_file(
    ctx: &NfsContext,
    parent_path: &str,
    name: &str,
    mode: CreateMode,
) -> Result<String, WriteError> {
    validate_vfs_path(parent_path)?;
    validate_name(name)?;
    check_cached_name(ctx, name)?;
    let child_path = join_child(parent_path, name);
    if let Some(cached) = ctx.cached(&child_path) {
        let existing = cached.resolve().map_err(|e| WriteError::from_backend(&e))?;
        match (mode, existing) {
            (CreateMode::Guarded | CreateMode::Exclusive, Some(_)) => {
                return Err(WriteError::Conflict);
            }
            (CreateMode::Unchecked, Some(_)) => {}
            (_, None) => {
                create_cached(&cached)?;
            }
        }
        return Ok(child_path);
    }
    let (backend, relative) = ctx.route(&child_path);

    block_on(async move {
        let existing = lookup_existing(backend.as_ref(), &relative).await?;
        match (mode, existing) {
            // GUARDED/EXCLUSIVE over an existing name: refuse, do not touch it.
            (CreateMode::Guarded | CreateMode::Exclusive, Some(_)) => {
                return Err(WriteError::Conflict);
            }
            // UNCHECKED over an existing name: no-op, preserve content.
            (CreateMode::Unchecked, Some(_)) => return Ok(child_path),
            // Absent for any mode: create empty.
            (_, None) => {}
        }
        let parent_id = parent_file_id(backend.as_ref(), &relative).await;
        backend
            .upload(&relative, &[], &parent_id)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;
        Ok(child_path)
    })
}

/// Truncate or extend the file at `path` to exactly `size` bytes.
///
/// # Errors
///
/// Returns a [`WriteError`] if the path escapes the export, the file does not
/// exist, or the backend rejects the write.
pub fn truncate_file(ctx: &NfsContext, path: &str, size: u64) -> Result<(), WriteError> {
    validate_vfs_path(path)?;
    if let Some(cached) = ctx.cached(path) {
        let uid = existing_uid(&cached)?;
        block_on(cached.cache.set_size(uid, size)).map_err(|e| WriteError::from_backend(&e))?;
        return Ok(());
    }
    let (backend, relative) = ctx.route(path);
    let target = usize::try_from(size).map_err(|_| WriteError::Invalid)?;

    block_on(async move {
        let entry = backend
            .metadata(&relative)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;

        let mut buf = backend
            .download(&entry)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;
        buf.resize(target, 0);

        let file_id = FileId(entry.id.0.clone());
        backend
            .update(&file_id, &buf)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;
        Ok(())
    })
}

/// Create a directory named `name` under `parent_path`, returning the new
/// directory's server-absolute VFS path.
///
/// # Errors
///
/// Returns a [`WriteError`] if the parent path escapes the export, the name is
/// invalid, or the backend rejects the operation.
pub fn make_dir(ctx: &NfsContext, parent_path: &str, name: &str) -> Result<String, WriteError> {
    validate_vfs_path(parent_path)?;
    validate_name(name)?;
    check_cached_name(ctx, name)?;
    let child_path = join_child(parent_path, name);
    if let Some(parent) = ctx.cached(parent_path)
        && ctx
            .cached(&child_path)
            .is_some_and(|child| child.backend_id == parent.backend_id)
    {
        let parent_uid = existing_uid(&parent)?;
        block_on(parent.cache.create_folder(parent_uid, name))
            .map_err(|e| WriteError::from_backend(&e))?;
        return Ok(child_path);
    }
    let (backend, relative) = ctx.route(&child_path);

    block_on(async move {
        backend
            .create_dir(&relative)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;
        Ok(child_path)
    })
}

/// Remove the file or directory named `name` under `parent_path`.
///
/// `expect_dir` selects the kind being removed (`REMOVE` vs `RMDIR`): a
/// mismatch between the request and the on-disk kind is reported as
/// [`WriteError::Invalid`] so the caller can map it to the correct status.
///
/// # Errors
///
/// Returns a [`WriteError`] if the path escapes the export, the entry does not
/// exist, the kind does not match, or the backend rejects the delete.
pub fn remove_entry(
    ctx: &NfsContext,
    parent_path: &str,
    name: &str,
    expect_dir: bool,
) -> Result<String, WriteError> {
    validate_vfs_path(parent_path)?;
    validate_name(name)?;
    let child_path = join_child(parent_path, name);
    if let Some(cached) = ctx.cached(&child_path) {
        remove_cached(&cached, Some(expect_dir))?;
        return Ok(child_path);
    }
    let (backend, relative) = ctx.route(&child_path);

    block_on(async move {
        let entry = backend
            .metadata(&relative)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;
        if entry.is_dir != expect_dir {
            return Err(WriteError::Invalid);
        }
        backend
            .delete(&entry)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;
        Ok(child_path)
    })
}

/// Remove the file or directory named `name` under `parent_path` regardless of
/// kind. Used by `NFSv4` REMOVE, which does not distinguish files from
/// directories at the protocol level.
///
/// # Errors
///
/// Returns a [`WriteError`] if the path escapes the export, the entry does not
/// exist, or the backend rejects the delete.
pub fn remove_any(ctx: &NfsContext, parent_path: &str, name: &str) -> Result<String, WriteError> {
    validate_vfs_path(parent_path)?;
    validate_name(name)?;
    let child_path = join_child(parent_path, name);
    if let Some(cached) = ctx.cached(&child_path) {
        remove_cached(&cached, None)?;
        return Ok(child_path);
    }
    let (backend, relative) = ctx.route(&child_path);

    block_on(async move {
        let entry = backend
            .metadata(&relative)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;
        backend
            .delete(&entry)
            .await
            .map_err(|e| WriteError::from_backend(&e))?;
        Ok(child_path)
    })
}

/// Rename `from_name` under `from_parent` to `to_name` under `to_parent`.
///
/// Routes through [`cascade_engine::vfs::VfsTree::rename`], which handles both
/// same-backend moves and cross-backend download/upload/delete.
///
/// # Errors
///
/// Returns a [`WriteError`] if either path escapes the export, a name is
/// invalid, or the backend rejects the move.
pub fn rename_entry(
    ctx: &NfsContext,
    from_parent: &str,
    from_name: &str,
    to_parent: &str,
    to_name: &str,
) -> Result<(String, String), WriteError> {
    validate_vfs_path(from_parent)?;
    validate_vfs_path(to_parent)?;
    validate_name(from_name)?;
    validate_name(to_name)?;
    check_cached_name(ctx, to_name)?;
    let src_path = join_child(from_parent, from_name);
    let dst_path = join_child(to_parent, to_name);
    if let (Some(src), Some(parent)) = (ctx.cached(&src_path), ctx.cached(to_parent))
        && src.backend_id == parent.backend_id
        && ctx
            .cached(&dst_path)
            .is_some_and(|dst| dst.backend_id == src.backend_id)
    {
        let uid = existing_uid(&src)?;
        let parent_uid = existing_uid(&parent)?;
        block_on(src.cache.rename(uid, parent_uid, to_name))
            .map_err(|e| WriteError::from_backend(&e))?;
        return Ok((src_path, dst_path));
    }

    block_on(async {
        ctx.rename(&src_path, &dst_path)
            .await
            .map_err(|e| WriteError::from_backend(&e))
    })?;
    Ok((src_path, dst_path))
}

/// Fetch the metadata of an existing target, distinguishing genuine absence
/// from a transient or permission failure.
///
/// A `WRITE` is a partial pwrite at an offset: the existing content must be
/// fetched and the new fragment spliced in, then the whole object written back.
/// If `metadata` fails for any reason other than genuine absence (a timeout, a
/// 5xx, a 403), treating it as "absent" would skip the download and overwrite
/// the entire object with just the new fragment — silent truncation. So only
/// [`BackendError::NotFound`] yields `Ok(None)` (the create case); every other
/// error aborts the write loudly.
///
/// # Errors
///
/// Returns a categorised [`WriteError`] for any non-`NotFound` backend failure.
async fn lookup_existing(
    backend: &dyn Backend,
    relative: &Path,
) -> Result<Option<cascade_engine::types::FileEntry>, WriteError> {
    match backend.metadata(relative).await {
        Ok(entry) => Ok(Some(entry)),
        Err(err) => match WriteError::from_backend(&err) {
            WriteError::NotFound => Ok(None),
            other => Err(other),
        },
    }
}

/// Resolve the `FileId` of the parent directory of a backend-relative path.
///
/// Mirrors the `WebDAV` presenter: the parent is resolved by `Backend::metadata`
/// when present, otherwise the backend's conventional root id (`{backend}:root`)
/// is used so a top-level create still lands in the right place.
async fn parent_file_id(backend: &dyn Backend, relative: &Path) -> FileId {
    let root_id = FileId(format!("{}:root", backend.id()));
    let Some(parent) = relative.parent() else {
        return root_id;
    };
    if parent.as_os_str().is_empty() {
        return root_id;
    }
    match backend.metadata(parent).await {
        Ok(entry) => FileId(entry.id.0),
        Err(_) => root_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_vfs_path_accepts_root_and_nested() {
        assert!(validate_vfs_path("/").is_ok());
        assert!(validate_vfs_path("/a").is_ok());
        assert!(validate_vfs_path("/a/b/c").is_ok());
    }

    #[test]
    fn validate_vfs_path_rejects_traversal() {
        assert!(matches!(
            validate_vfs_path("/a/../b"),
            Err(WriteError::Traversal)
        ));
        assert!(matches!(
            validate_vfs_path("/.."),
            Err(WriteError::Traversal)
        ));
        assert!(matches!(
            validate_vfs_path("a/b"),
            Err(WriteError::Traversal)
        ));
        assert!(matches!(validate_vfs_path(""), Err(WriteError::Traversal)));
    }

    #[test]
    fn validate_name_rejects_separators_and_dots() {
        assert!(validate_name("file.txt").is_ok());
        assert!(matches!(validate_name(""), Err(WriteError::Invalid)));
        assert!(matches!(validate_name(".."), Err(WriteError::Traversal)));
        assert!(matches!(validate_name("."), Err(WriteError::Traversal)));
        assert!(matches!(validate_name("a/b"), Err(WriteError::Traversal)));
    }

    #[test]
    fn join_child_handles_root_and_nested() {
        assert_eq!(join_child("/", "a"), "/a");
        assert_eq!(join_child("/a", "b"), "/a/b");
    }
}

/// Tests for the metadata-error discrimination in [`write_file`], using a stub
/// backend whose `metadata` fails with a category other than `NotFound`.
#[cfg(test)]
mod discrimination_tests {
    use super::*;
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicBool, Ordering};

    use cascade_engine::backend::{Backend, BackendError};
    use cascade_engine::types::{Change, Cursor, FileEntry, FileId, Quota};
    use cascade_engine::vfs::VfsTree;

    /// Backend whose `metadata` returns the configured non-`NotFound` error.
    /// Records whether any content-writing call (`upload`/`update`) was made so
    /// a test can assert that a transient metadata failure never reaches the
    /// truncating write.
    #[derive(Debug)]
    struct FlakyBackend {
        wrote: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Backend for FlakyBackend {
        fn id(&self) -> &'static str {
            "flaky"
        }
        fn display_name(&self) -> &'static str {
            "Flaky"
        }
        async fn quota(&self) -> anyhow::Result<Option<Quota>> {
            Ok(None)
        }
        async fn changes(&self, _cursor: Option<&Cursor>) -> anyhow::Result<(Vec<Change>, Cursor)> {
            Ok((vec![], Cursor("flaky".to_string())))
        }
        async fn metadata(&self, _path: &Path) -> anyhow::Result<FileEntry> {
            // A transient/permission failure on an existing file — NOT absence.
            Err(BackendError::Forbidden("transient HEAD failure".to_string()).into())
        }
        async fn download(&self, _file: &FileEntry) -> anyhow::Result<Vec<u8>> {
            anyhow::bail!("unused")
        }
        async fn upload(
            &self,
            _path: &Path,
            _data: &[u8],
            _parent_id: &FileId,
        ) -> anyhow::Result<FileEntry> {
            self.wrote.store(true, Ordering::SeqCst);
            anyhow::bail!("upload must not be reached on a transient metadata error")
        }
        async fn update(&self, _file_id: &FileId, _data: &[u8]) -> anyhow::Result<FileEntry> {
            self.wrote.store(true, Ordering::SeqCst);
            anyhow::bail!("update must not be reached on a transient metadata error")
        }
        async fn create_dir(&self, _path: &Path) -> anyhow::Result<FileEntry> {
            anyhow::bail!("unused")
        }
        async fn delete(&self, _file: &FileEntry) -> anyhow::Result<()> {
            anyhow::bail!("unused")
        }
        async fn move_entry(&self, _src: &Path, _dst: &Path) -> anyhow::Result<FileEntry> {
            anyhow::bail!("unused")
        }
        async fn poll_interval(&self) -> Option<std::time::Duration> {
            None
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_aborts_on_non_notfound_metadata_error() {
        let wrote = Arc::new(AtomicBool::new(false));
        let backend: Arc<dyn Backend> = Arc::new(FlakyBackend {
            wrote: Arc::clone(&wrote),
        });
        let vfs = Arc::new(RwLock::new(VfsTree::new(backend)));
        let ctx = NfsContext::new(vfs);

        let result = tokio::task::block_in_place(|| write_file(&ctx, "/data.bin", 0, b"fragment"));

        // The transient error must surface (here as Forbidden), and crucially the
        // truncating upload/update must never have been attempted.
        assert!(matches!(result, Err(WriteError::Forbidden)));
        assert!(
            !wrote.load(Ordering::SeqCst),
            "write_file truncated through a non-NotFound metadata error"
        );
    }
}
