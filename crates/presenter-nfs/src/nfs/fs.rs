//! The file system `nfs3_server` serves: items of the content cache named by
//! their uid, other paths of the VFS tree by the key the context registers.

use std::sync::Arc;

use cascade_engine::content::replica::{ItemKind, ReplicaItem};
use chrono::{DateTime, Utc};
use nfs3_server::vfs::{
    DirEntryPlus, FileHandle, NextResult, NfsFileSystem, NfsReadFileSystem, ReadDirPlusIterator,
    VFSCapabilities,
};
use nfs3_types::nfs3::{
    FSF3_CANSETTIME, FSF3_HOMOGENEOUS, FSINFO3resok, Nfs3Option, createverf3, fattr3, filename3,
    ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3, stable_how,
};

use super::context::NfsContext;
use super::server::NfsCacheMode;
use super::sidecar::{Sidecars, is_appledouble};
use super::write::{self, CreateMode, WriteError, join_child};

/// First byte of the handle of a path the context registered.
const PATH_TAG: u8 = b'p';
/// First bytes of the handle of an `AppleDouble` companion.
const SIDECAR_TAG: [u8; 2] = *b"ad";
/// Bit set in the file id of an `AppleDouble` companion, above every uid.
const SIDECAR_FILEID: u64 = 1 << 63;
/// Largest READ and WRITE, and the preferred READDIR size.
pub(super) const TRANSFER_SIZE: u32 = 1024 * 1024;
/// Status of a folder named, or a rename across, an `AppleDouble` name.
const APPLEDOUBLE_REFUSAL: nfsstat3 = nfsstat3::NFS3ERR_INVAL;

/// Identity of a file or folder in a file handle, after the generation the
/// server prefixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handle {
    /// An item the content cache serves: its uid, big-endian.
    Item([u8; 8]),
    /// A path the context registered: a tag byte and its key, big-endian.
    Path([u8; 9]),
    /// An `AppleDouble` companion: two tag bytes and its id, big-endian.
    Sidecar([u8; 10]),
}

impl Handle {
    #[must_use]
    pub const fn item(uid: u64) -> Self {
        Self::Item(uid.to_be_bytes())
    }

    #[must_use]
    pub const fn path(key: u64) -> Self {
        let [k0, k1, k2, k3, k4, k5, k6, k7] = key.to_be_bytes();
        Self::Path([PATH_TAG, k0, k1, k2, k3, k4, k5, k6, k7])
    }

    #[must_use]
    pub const fn sidecar(id: u64) -> Self {
        let [i0, i1, i2, i3, i4, i5, i6, i7] = id.to_be_bytes();
        let [t0, t1] = SIDECAR_TAG;
        Self::Sidecar([t0, t1, i0, i1, i2, i3, i4, i5, i6, i7])
    }

    /// The uid, path key or companion id the handle carries.
    #[must_use]
    pub const fn id(&self) -> u64 {
        match self {
            Self::Item(uid) => u64::from_be_bytes(*uid),
            Self::Path([_, key @ ..]) => u64::from_be_bytes(*key),
            Self::Sidecar([_, _, id @ ..]) => u64::from_be_bytes(*id),
        }
    }

    /// The file id of what the handle names.
    #[must_use]
    pub const fn fileid(&self) -> u64 {
        match self {
            Self::Item(_) | Self::Path(_) => self.id(),
            Self::Sidecar(_) => SIDECAR_FILEID | self.id(),
        }
    }
}

impl FileHandle for Handle {
    fn len(&self) -> usize {
        self.as_bytes().len()
    }

    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Item(bytes) => bytes,
            Self::Path(bytes) => bytes,
            Self::Sidecar(bytes) => bytes,
        }
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            [_, _, _, _, _, _, _, _] => <[u8; 8]>::try_from(bytes).ok().map(Self::Item),
            [_, _, _, _, _, _, _, _, _] => <[u8; 9]>::try_from(bytes).ok().map(Self::Path),
            [_, _, _, _, _, _, _, _, _, _] => <[u8; 10]>::try_from(bytes).ok().map(Self::Sidecar),
            _ => None,
        }
    }
}

/// The uid an item handle carries; a uid the state database cannot hold
/// names no item.
fn stored_uid(handle: &Handle) -> Result<u64, nfsstat3> {
    let uid = handle.id();
    i64::try_from(uid)
        .map(|_| uid)
        .map_err(|_| nfsstat3::NFS3ERR_STALE)
}

const fn status(error: WriteError) -> nfsstat3 {
    match error {
        WriteError::Traversal | WriteError::Forbidden => nfsstat3::NFS3ERR_ACCES,
        WriteError::NotFound => nfsstat3::NFS3ERR_NOENT,
        WriteError::Conflict => nfsstat3::NFS3ERR_EXIST,
        WriteError::Invalid => nfsstat3::NFS3ERR_INVAL,
        WriteError::NoSpace => nfsstat3::NFS3ERR_NOSPC,
        WriteError::Io => nfsstat3::NFS3ERR_IO,
    }
}

/// The status of a failed write to an item a handle named: an item no
/// longer there is stale.
const fn stale_if_missing(error: WriteError) -> nfsstat3 {
    match error {
        WriteError::NotFound => nfsstat3::NFS3ERR_STALE,
        other => status(other),
    }
}

/// The status of a failed operation on an item a handle named: an item no
/// longer there is stale.
fn item_status(error: &anyhow::Error) -> nfsstat3 {
    stale_if_missing(WriteError::from_backend(error))
}

fn name_of<'n>(name: &'n filename3<'_>) -> Result<&'n str, nfsstat3> {
    std::str::from_utf8(name.as_ref()).map_err(|_| nfsstat3::NFS3ERR_INVAL)
}

/// The folder holding `path`; the root for the root.
fn parent_of(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some(("", _)) | None => "/",
        Some((parent, _)) => parent,
    }
}

fn nfs_time(time: Option<DateTime<Utc>>) -> nfstime3 {
    time.map_or_else(nfstime3::default, |time| nfstime3 {
        seconds: u32::try_from(time.timestamp()).unwrap_or_default(),
        nseconds: time.timestamp_subsec_nanos(),
    })
}

fn attributes(fileid: u64, is_dir: bool, size: u64, mtime: Option<DateTime<Utc>>) -> fattr3 {
    let time = nfs_time(mtime);
    fattr3 {
        type_: if is_dir {
            ftype3::NF3DIR
        } else {
            ftype3::NF3REG
        },
        mode: if is_dir { 0o755 } else { 0o644 },
        nlink: if is_dir { 2 } else { 1 },
        uid: 501,
        gid: 20,
        size,
        used: size,
        rdev: specdata3::default(),
        fsid: 0,
        fileid,
        atime: time,
        mtime: time,
        ctime: time,
    }
}

fn item_attributes(item: &ReplicaItem) -> fattr3 {
    attributes(
        item.uid,
        item.kind == ItemKind::Folder,
        item.size.unwrap_or(0),
        item.mtime,
    )
}

/// One page of a folder listing, in file id order.
#[derive(Debug)]
pub struct Listing(std::vec::IntoIter<DirEntryPlus<Handle>>);

impl ReadDirPlusIterator<Handle> for Listing {
    async fn next(&mut self) -> NextResult<DirEntryPlus<Handle>> {
        self.0.next().map_or(NextResult::Eof, NextResult::Ok)
    }
}

/// The VFS tree and its content cache as an `NFSv3` file system.
#[derive(Debug)]
pub struct CascadeFs {
    ctx: Arc<NfsContext>,
    cache_mode: NfsCacheMode,
    root: Handle,
    sidecars: Sidecars,
}

impl CascadeFs {
    /// The file system over `ctx`, its root resolved.
    ///
    /// # Errors
    ///
    /// Fails when the content cache cannot resolve the root.
    pub async fn new(ctx: Arc<NfsContext>, cache_mode: NfsCacheMode) -> anyhow::Result<Self> {
        let mut fs = Self {
            ctx,
            cache_mode,
            root: Handle::path(NfsContext::path_to_key("/")),
            sidecars: Sidecars::default(),
        };
        fs.root = fs
            .handle_for("/")
            .await
            .map_err(|code| anyhow::anyhow!("resolve the export root: {code:?}"))?;
        Ok(fs)
    }

    /// The handle generation of the content cache's replica store, the same
    /// for every server on one state database; `None` without a content
    /// cache.
    ///
    /// # Errors
    ///
    /// Fails when the state database fails.
    pub fn generation(&self) -> anyhow::Result<Option<u64>> {
        self.ctx
            .content_cache()
            .map(|cache| {
                cache
                    .current_anchor()
                    .map(|anchor| NfsContext::path_to_key(anchor.store_id()))
            })
            .transpose()
    }

    /// The handle of the existing item or path at `path`.
    async fn handle_for(&self, path: &str) -> Result<Handle, nfsstat3> {
        let Some(cached) = self.ctx.cached(path) else {
            return Ok(Handle::path(self.ctx.register_path(path)));
        };
        match cached.resolve().await {
            Ok(Some(uid)) => Ok(Handle::item(uid)),
            Ok(None) => Err(nfsstat3::NFS3ERR_NOENT),
            Err(error) => Err(status(WriteError::from_backend(&error))),
        }
    }

    /// The NFS path of the item or path `handle` names.
    fn path_of(&self, handle: &Handle) -> Result<String, nfsstat3> {
        let path = match handle {
            Handle::Item(_) => self
                .ctx
                .item_path(stored_uid(handle)?)
                .map_err(|_| nfsstat3::NFS3ERR_IO)?,
            Handle::Path(_) => self.ctx.lookup_path(handle.id()),
            Handle::Sidecar(_) => return Err(nfsstat3::NFS3ERR_NOTDIR),
        };
        path.ok_or(nfsstat3::NFS3ERR_STALE)
    }

    /// The file id of the existing item or path at `path`, without
    /// registering it; `None` when it cannot be resolved.
    async fn fileid_of(&self, path: &str) -> Option<u64> {
        match self.ctx.cached(path) {
            Some(cached) => cached.resolve().await.ok().flatten(),
            None => Some(NfsContext::path_to_key(path)),
        }
    }

    /// Whether `path` is the root or the mount point of a backend.
    fn is_mount(&self, path: &str) -> bool {
        let Some((_, name)) = path.rsplit_once('/') else {
            return false;
        };
        name.is_empty()
            || self
                .ctx
                .mount_names(parent_of(path))
                .is_ok_and(|mounts| mounts.iter().any(|mount| mount == name))
    }

    /// The handle of `name` in the folder `dirid` names, at `dir`.
    async fn child(&self, dirid: &Handle, dir: &str, name: &str) -> Result<Handle, nfsstat3> {
        if is_appledouble(name) {
            return self
                .sidecars
                .lookup(dirid.fileid(), name)
                .map(Handle::sidecar)
                .ok_or(nfsstat3::NFS3ERR_NOENT);
        }
        if write::validate_name(name).is_err() {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }
        let path = join_child(dir, name);
        if self.ctx.cached(&path).is_some() || self.is_mount(&path) {
            return self.handle_for(&path).await;
        }
        match self.ctx.metadata(&path).await {
            Ok(_) => self.handle_for(&path).await,
            Err(error) => Err(status(WriteError::from_backend(&error))),
        }
    }

    /// The entries of the folder at `path` after `cookie`, a file id, in
    /// file id order; the first page lists the folder again.
    async fn entries(
        &self,
        path: &str,
        cookie: u64,
    ) -> Result<Vec<DirEntryPlus<Handle>>, nfsstat3> {
        let mut entries = Vec::new();
        let mut names = Vec::new();
        if let Some(cached) = self.ctx.cached(path) {
            let folder = cached.existing().await.map_err(|e| item_status(&e))?;
            if folder.kind != ItemKind::Folder {
                return Err(nfsstat3::NFS3ERR_NOTDIR);
            }
            let items = if cookie == 0 {
                cached.cache.list(folder.uid).await
            } else {
                cached.cache.children(folder.uid)
            }
            .map_err(|e| item_status(&e))?;
            for item in items {
                names.push(item.name.clone());
                entries.push(DirEntryPlus {
                    fileid: item.uid,
                    name: item.name.clone().into_bytes().into(),
                    cookie: item.uid,
                    name_attributes: Some(item_attributes(&item)),
                    name_handle: Some(Handle::item(item.uid)),
                });
            }
            let mounts = self
                .ctx
                .mount_names(path)
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            for mount in mounts.into_iter().filter(|mount| !names.contains(mount)) {
                entries.push(self.entry(path, mount).await?);
            }
        } else {
            let listing = self.ctx.list_dir(path).await.map_err(|e| item_status(&e))?;
            for entry in listing {
                entries.push(self.entry(path, entry.name).await?);
            }
        }
        entries.retain(|entry| {
            entry.fileid > cookie
                && std::str::from_utf8(entry.name.as_ref()).is_ok_and(|name| !is_appledouble(name))
        });
        entries.sort_by_key(|entry| entry.fileid);
        Ok(entries)
    }

    /// The listing entry of `name` in the folder at `dir`.
    async fn entry(&self, dir: &str, name: String) -> Result<DirEntryPlus<Handle>, nfsstat3> {
        let handle = self.handle_for(&join_child(dir, &name)).await?;
        let name_attributes = match handle {
            Handle::Item(_) => self.getattr(&handle).await.ok(),
            Handle::Path(_) | Handle::Sidecar(_) => None,
        };
        Ok(DirEntryPlus {
            fileid: handle.fileid(),
            name: name.into_bytes().into(),
            cookie: handle.fileid(),
            name_attributes,
            name_handle: Some(handle),
        })
    }

    /// The handle and attributes of the folder a create made at `path`.
    async fn created(&self, path: &str) -> Result<(Handle, fattr3), nfsstat3> {
        let handle = self.handle_for(path).await?;
        Ok((handle, self.getattr(&handle).await?))
    }
}

impl NfsReadFileSystem for CascadeFs {
    type Handle = Handle;

    fn root_dir(&self) -> Handle {
        self.root
    }

    async fn lookup(&self, dirid: &Handle, filename: &filename3<'_>) -> Result<Handle, nfsstat3> {
        let dir = self.path_of(dirid)?;
        match name_of(filename).map_err(|_| nfsstat3::NFS3ERR_NOENT)? {
            "." => Ok(*dirid),
            ".." => self.handle_for(parent_of(&dir)).await,
            name => self.child(dirid, &dir, name).await,
        }
    }

    async fn getattr(&self, id: &Handle) -> Result<fattr3, nfsstat3> {
        if let Handle::Sidecar(_) = id {
            let (size, mtime) = self
                .sidecars
                .attributes(id.id())
                .ok_or(nfsstat3::NFS3ERR_STALE)?;
            return Ok(attributes(id.fileid(), false, size, Some(mtime)));
        }
        if let Handle::Item(_) = id {
            let cache = self.ctx.content_cache().ok_or(nfsstat3::NFS3ERR_STALE)?;
            return match cache.item(stored_uid(id)?) {
                Ok(Some(item)) => Ok(item_attributes(&item)),
                Ok(None) => Err(nfsstat3::NFS3ERR_STALE),
                Err(_) => Err(nfsstat3::NFS3ERR_IO),
            };
        }
        let path = self.path_of(id)?;
        if self.is_mount(&path) {
            return Ok(attributes(id.id(), true, 0, None));
        }
        let entry = self
            .ctx
            .metadata(&path)
            .await
            .map_err(|e| item_status(&e))?;
        Ok(attributes(
            id.id(),
            entry.is_dir,
            entry.size.unwrap_or(0),
            entry.mod_time,
        ))
    }

    async fn read(
        &self,
        id: &Handle,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        if let Handle::Sidecar(_) = id {
            return self
                .sidecars
                .read(id.id(), offset, count)
                .ok_or(nfsstat3::NFS3ERR_STALE);
        }
        let path = self.path_of(id)?;
        self.ctx
            .read_range(&path, offset, count)
            .await
            .map_err(|e| item_status(&e))
    }

    async fn readdirplus(
        &self,
        dirid: &Handle,
        cookie: u64,
    ) -> Result<impl ReadDirPlusIterator<Handle>, nfsstat3> {
        let path = self.path_of(dirid)?;
        Ok(Listing(self.entries(&path, cookie).await?.into_iter()))
    }

    async fn readlink(&self, id: &Handle) -> Result<nfspath3<'_>, nfsstat3> {
        self.path_of(id)?;
        Err(nfsstat3::NFS3ERR_INVAL)
    }

    async fn fsinfo(&self, root_fileid: &Handle) -> Result<FSINFO3resok, nfsstat3> {
        let attributes = self.getattr(root_fileid).await?;
        Ok(FSINFO3resok {
            obj_attributes: nfs3_types::nfs3::post_op_attr::Some(attributes),
            rtmax: TRANSFER_SIZE,
            rtpref: TRANSFER_SIZE,
            rtmult: TRANSFER_SIZE,
            wtmax: TRANSFER_SIZE,
            wtpref: TRANSFER_SIZE,
            wtmult: TRANSFER_SIZE,
            dtpref: TRANSFER_SIZE,
            maxfilesize: u64::MAX,
            time_delta: nfstime3 {
                seconds: 0,
                nseconds: 1_000_000,
            },
            properties: FSF3_HOMOGENEOUS | FSF3_CANSETTIME,
        })
    }
}

impl NfsFileSystem for CascadeFs {
    fn capabilities(&self) -> VFSCapabilities {
        if self.cache_mode.writes_permitted() {
            VFSCapabilities::ReadWrite
        } else {
            VFSCapabilities::ReadOnly
        }
    }

    async fn setattr(&self, id: &Handle, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        if let (Handle::Sidecar(_), Nfs3Option::Some(size)) = (id, setattr.size) {
            self.sidecars
                .set_size(id.id(), size)
                .ok_or(nfsstat3::NFS3ERR_STALE)?;
        }
        if let Handle::Sidecar(_) = id {
            return self.getattr(id).await;
        }
        let path = self.path_of(id)?;
        if let Nfs3Option::Some(size) = setattr.size {
            write::truncate_file(&self.ctx, &path, size)
                .await
                .map_err(stale_if_missing)?;
        }
        self.getattr(id).await
    }

    async fn write(
        &self,
        id: &Handle,
        offset: u64,
        data: &[u8],
        stable: stable_how,
    ) -> Result<(fattr3, stable_how), nfsstat3> {
        if let Handle::Sidecar(_) = id {
            self.sidecars
                .write(id.id(), offset, data)
                .ok_or(nfsstat3::NFS3ERR_STALE)?;
            return Ok((self.getattr(id).await?, stable));
        }
        let path = self.path_of(id)?;
        write::write_at(&self.ctx, &path, offset, data)
            .await
            .map_err(stale_if_missing)?;
        if stable != stable_how::UNSTABLE {
            write::sync_file(&self.ctx, &path)
                .await
                .map_err(stale_if_missing)?;
        }
        Ok((self.getattr(id).await?, stable))
    }

    async fn create(
        &self,
        dirid: &Handle,
        filename: &filename3<'_>,
        attr: sattr3,
    ) -> Result<(Handle, fattr3), nfsstat3> {
        let handle = self
            .create_file(dirid, filename, CreateMode::Unchecked)
            .await?;
        if let Nfs3Option::Some(size) = attr.size {
            self.setattr(
                &handle,
                sattr3 {
                    size: Nfs3Option::Some(size),
                    ..sattr3::default()
                },
            )
            .await?;
        }
        Ok((handle, self.getattr(&handle).await?))
    }

    async fn create_exclusive(
        &self,
        dirid: &Handle,
        filename: &filename3<'_>,
        _createverf: createverf3,
    ) -> Result<Handle, nfsstat3> {
        self.create_file(dirid, filename, CreateMode::Exclusive)
            .await
    }

    async fn mkdir(
        &self,
        dirid: &Handle,
        dirname: &filename3<'_>,
    ) -> Result<(Handle, fattr3), nfsstat3> {
        let name = name_of(dirname)?;
        if is_appledouble(name) {
            return Err(APPLEDOUBLE_REFUSAL);
        }
        let parent = self.path_of(dirid)?;
        let path = write::make_dir(&self.ctx, &parent, name)
            .await
            .map_err(status)?;
        self.created(&path).await
    }

    async fn remove(&self, dirid: &Handle, filename: &filename3<'_>) -> Result<(), nfsstat3> {
        let name = name_of(filename)?;
        let parent = self.path_of(dirid)?;
        if is_appledouble(name) {
            return if self.sidecars.remove(dirid.fileid(), name) {
                Ok(())
            } else {
                Err(nfsstat3::NFS3ERR_NOENT)
            };
        }
        let removed = self.fileid_of(&join_child(&parent, name)).await;
        let path = write::remove_entry(&self.ctx, &parent, name)
            .await
            .map_err(|e| match e {
                WriteError::Conflict => nfsstat3::NFS3ERR_NOTEMPTY,
                other => status(other),
            })?;
        self.ctx.remove_path(NfsContext::path_to_key(&path));
        self.sidecars.remove(dirid.fileid(), &format!("._{name}"));
        if let Some(folder) = removed {
            self.sidecars.remove_folder(folder);
        }
        Ok(())
    }

    async fn rename<'a>(
        &self,
        from_dirid: &Handle,
        from_filename: &filename3<'a>,
        to_dirid: &Handle,
        to_filename: &filename3<'a>,
    ) -> Result<(), nfsstat3> {
        let from_name = name_of(from_filename)?;
        let to_name = name_of(to_filename)?;
        let from = self.path_of(from_dirid)?;
        let to = self.path_of(to_dirid)?;
        match (is_appledouble(from_name), is_appledouble(to_name)) {
            (true, true) => {
                let (from, to) = (from_dirid.fileid(), to_dirid.fileid());
                return if self.sidecars.rename(from, from_name, to, to_name) {
                    Ok(())
                } else {
                    Err(nfsstat3::NFS3ERR_NOENT)
                };
            }
            (false, false) => {}
            _ => return Err(APPLEDOUBLE_REFUSAL),
        }
        let (source, _) = write::rename_entry(&self.ctx, &from, from_name, &to, to_name)
            .await
            .map_err(status)?;
        self.ctx.remove_path(NfsContext::path_to_key(&source));
        Ok(())
    }

    async fn symlink<'a>(
        &self,
        dirid: &Handle,
        _linkname: &filename3<'a>,
        _symlink: &nfspath3<'a>,
        _attr: &sattr3,
    ) -> Result<(Handle, fattr3), nfsstat3> {
        self.path_of(dirid)?;
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn commit(&self, id: &Handle, _offset: u64, _count: u32) -> Result<(), nfsstat3> {
        self.getattr(id).await?;
        if let Handle::Sidecar(_) = id {
            return Ok(());
        }
        let path = self.path_of(id)?;
        write::sync_file(&self.ctx, &path)
            .await
            .map_err(stale_if_missing)
    }
}

impl CascadeFs {
    /// Creates the file `filename` in the folder `dirid` names as `mode`
    /// asks and returns its handle; an `AppleDouble` name is created among
    /// the companions.
    async fn create_file(
        &self,
        dirid: &Handle,
        filename: &filename3<'_>,
        mode: CreateMode,
    ) -> Result<Handle, nfsstat3> {
        let name = name_of(filename)?;
        let parent = self.path_of(dirid)?;
        if is_appledouble(name) {
            let exclusive = mode != CreateMode::Unchecked;
            return self
                .sidecars
                .create(dirid.fileid(), name, exclusive)
                .map(Handle::sidecar);
        }
        let path = write::create_file(&self.ctx, &parent, name, mode)
            .await
            .map_err(status)?;
        self.handle_for(&path).await
    }
}
