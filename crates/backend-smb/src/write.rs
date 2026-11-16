//! Writes: content in requests of at most [`MAX_REQUEST`], renames, folder creates and deletes.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use cascade_engine::backend::{BackendError, TransientError};
use cascade_engine::types::{FileEntry, Revision};
use smb::binrw_util::file_time::FileTime;
use smb::{
    CreateDisposition, CreateOptions, DirAccessMask, FileAccessMask, FileAttributes,
    FileBasicInformation, FileCreateArgs, FileDispositionInformation, FileNetworkOpenInformation,
    FileRenameInformation, Resource, ResourceHandle, Status, WriteAt,
};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::client::MAX_REQUEST;
use crate::listing::{self, file_time_to_utc};
use crate::{SmbBackend, close_open_file, status_of};

/// 100-nanosecond intervals from 1601-01-01 to 1970-01-01.
const UNIX_EPOCH_FILE_TIME: u64 = 116_444_736_000_000_000;

/// Which file a content write opens.
#[derive(Clone, Copy)]
pub enum Target {
    /// Create the file, truncating one that exists.
    CreateOrReplace,
    /// Truncate an existing file; fail when it is absent.
    Existing,
}

/// The local file at `local`, opened for reading, and its last-write time.
pub async fn local_source(local: &Path) -> anyhow::Result<(tokio::fs::File, Option<SystemTime>)> {
    let file = tokio::fs::File::open(local).await?;
    let modified = file.metadata().await?.modified().ok();
    Ok((file, modified))
}

fn file_time(time: SystemTime) -> Option<FileTime> {
    let since_unix = time.duration_since(UNIX_EPOCH).ok()?;
    let intervals = u64::try_from(since_unix.as_nanos() / 100).ok()?;
    intervals
        .checked_add(UNIX_EPOCH_FILE_TIME)
        .map(FileTime::from)
}

fn conflict_on_collision(err: anyhow::Error, native_id: &str) -> anyhow::Error {
    if status_of(&err) == Some(Status::U32_OBJECT_NAME_COLLISION) {
        BackendError::Conflict(native_id.to_string()).into()
    } else {
        err
    }
}

/// Fill `buf` from `source`; fewer bytes than its length means the end was reached.
async fn read_chunk<R: AsyncRead + Unpin>(
    source: &mut R,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    let mut filled = 0;
    while let Some(rest) = buf.get_mut(filled..).filter(|rest| !rest.is_empty()) {
        let n = source.read(rest).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

async fn write_all_at(file: &smb::File, chunk: &[u8], offset: u64) -> anyhow::Result<()> {
    let mut written = 0;
    while let Some(rest) = chunk.get(written..).filter(|rest| !rest.is_empty()) {
        let at = offset.saturating_add(u64::try_from(written)?);
        let n = file.write_at(rest, at).await?;
        if n == 0 {
            anyhow::bail!("SMB server accepted no bytes at offset {at}");
        }
        written += n;
    }
    Ok(())
}

async fn close_handle(handle: &ResourceHandle, result: anyhow::Result<()>) -> anyhow::Result<()> {
    let closed = handle.close().await;
    result?;
    Ok(closed?)
}

impl SmbBackend {
    /// Write `source` as the content of `native_id` and set its last-write time
    /// to `last_write` when given.
    pub(super) async fn write_content<R>(
        &self,
        native_id: &str,
        target: Target,
        mut source: R,
        last_write: Option<SystemTime>,
    ) -> anyhow::Result<FileEntry>
    where
        R: AsyncRead + Unpin + Send,
    {
        let disposition = match target {
            Target::CreateOrReplace => CreateDisposition::OverwriteIf,
            Target::Existing => CreateDisposition::Overwrite,
        };
        let args = |disposition| FileCreateArgs {
            disposition,
            attributes: FileAttributes::new(),
            options: CreateOptions::new(),
            desired_access: FileAccessMask::new()
                .with_generic_read(true)
                .with_generic_write(true),
        };
        let first = || args(disposition);
        let reopen = || args(CreateDisposition::Open);
        let (first, reopen) = (&first, &reopen);
        let last_write = last_write.and_then(file_time);
        let handle = std::sync::Mutex::new(None);
        let handle_ref = &handle;
        let result = async {
            let mut buf = vec![0u8; MAX_REQUEST];
            let mut offset = 0u64;
            loop {
                let n = read_chunk(&mut source, &mut buf).await?;
                let chunk = buf.get(..n).unwrap_or_default();
                self.client
                    .run(|client| async move {
                        let file = self
                            .file_on(handle_ref, &client, native_id, first, reopen)
                            .await?;
                        write_all_at(&file, chunk, offset).await
                    })
                    .await?;
                offset = offset.saturating_add(u64::try_from(n)?);
                if n < MAX_REQUEST {
                    break;
                }
            }
            self.client
                .run(|client| async move {
                    let file = self
                        .file_on(handle_ref, &client, native_id, first, reopen)
                        .await?;
                    if let Some(last_write_time) = last_write {
                        file.set_info(FileBasicInformation {
                            creation_time: FileTime::ZERO,
                            last_access_time: FileTime::ZERO,
                            last_write_time,
                            change_time: FileTime::ZERO,
                            file_attributes: FileAttributes::new(),
                        })
                        .await?;
                    }
                    Ok(file.query_info::<FileNetworkOpenInformation>().await?)
                })
                .await
        }
        .await;
        close_open_file(handle, native_id).await;
        let info = result?;
        Ok(self.entry_for(
            native_id,
            false,
            info.end_of_file,
            file_time_to_utc(info.last_write_time),
        ))
    }

    /// Rename `source` to `destination`, over an existing `destination` when
    /// `replace` is set. A replace refused because the target is held open
    /// elsewhere is [`TransientError::Busy`].
    pub(super) async fn rename(
        &self,
        source: &str,
        destination: &str,
        replace: bool,
    ) -> anyhow::Result<(FileEntry, Revision)> {
        let access = FileAccessMask::new()
            .with_delete(true)
            .with_file_read_attributes(true)
            .with_synchronize(true);
        let new_name = &self.share_relative(destination);
        let (revision, is_dir) = self
            .client
            .run(|client| async move {
                let resource = self.open(&client, source, access).await?;
                let handle: &ResourceHandle = match &resource {
                    Resource::File(file) => file,
                    Resource::Directory(dir) => dir,
                    Resource::Pipe(_) => anyhow::bail!("SMB path {source} is a pipe"),
                };
                let renamed = async {
                    handle
                        .set_info(FileRenameInformation {
                            replace_if_exists: replace.into(),
                            root_directory: 0,
                            file_name: new_name.as_str().into(),
                        })
                        .await?;
                    listing::revision_of(handle).await
                }
                .await;
                let closed = handle.close().await;
                let renamed = renamed?;
                closed?;
                Ok(renamed)
            })
            .await
            .map_err(|err| match status_of(&err) {
                Some(Status::U32_ACCESS_DENIED | Status::U32_SHARING_VIOLATION) if replace => {
                    TransientError::Busy(format!("{destination}: {err:#}")).into()
                }
                _ => conflict_on_collision(err, destination),
            })?;
        let entry = self.entry_for(
            destination,
            is_dir,
            revision.size.unwrap_or(0),
            revision.last_write_time,
        );
        Ok((entry, revision))
    }

    /// Create the directory `native_id`.
    pub(super) async fn create_directory(&self, native_id: &str) -> anyhow::Result<FileEntry> {
        let info = self
            .client
            .run(|client| async move {
                let args = FileCreateArgs {
                    disposition: CreateDisposition::Create,
                    attributes: FileAttributes::new().with_directory(true),
                    options: CreateOptions::new().with_directory_file(true),
                    desired_access: DirAccessMask::new()
                        .with_read_attributes(true)
                        .with_synchronize(true)
                        .into(),
                };
                let Resource::Directory(dir) = self.open_with(&client, native_id, &args).await?
                else {
                    anyhow::bail!("SMB path {native_id} is not a directory");
                };
                let info = dir.query_info::<FileNetworkOpenInformation>().await;
                dir.close().await?;
                Ok(info?)
            })
            .await
            .map_err(|err| conflict_on_collision(err, native_id))?;
        Ok(self.entry_for(native_id, true, 0, file_time_to_utc(info.last_write_time)))
    }

    /// Delete `native_id`, with everything beneath it when it is a directory.
    pub(super) async fn delete_tree(&self, native_id: &str, is_dir: bool) -> anyhow::Result<()> {
        if is_dir {
            for entry in self.list_tree(native_id).await?.iter().rev() {
                self.delete_one(entry.id.native_id()).await?;
            }
        }
        self.delete_one(native_id).await
    }

    async fn delete_one(&self, native_id: &str) -> anyhow::Result<()> {
        let access = FileAccessMask::new()
            .with_delete(true)
            .with_synchronize(true);
        self.client
            .run(|client| async move {
                let resource = self.open(&client, native_id, access).await?;
                let handle: &ResourceHandle = match &resource {
                    Resource::File(file) => file,
                    Resource::Directory(dir) => dir,
                    Resource::Pipe(_) => anyhow::bail!("SMB path {native_id} is a pipe"),
                };
                let marked = handle
                    .set_info(FileDispositionInformation::default())
                    .await
                    .map_err(anyhow::Error::from);
                close_handle(handle, marked).await
            })
            .await
    }
}
