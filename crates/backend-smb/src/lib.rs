#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::string_slice
    )
)]
//! SMB2/3 backend for Cascade.
//!
//! Serves a share on an SMB server (Samba, Windows, a NAS) through the
//! pure-Rust `smb` client. Native ids are share-relative paths with `/`
//! separators; the share root is `/`.

use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use cascade_engine::backend::{Backend, BackendError};
use cascade_engine::types::{Change, Cursor, FileEntry, FileId, ItemId, Quota, Revision};
use chrono::{DateTime, Utc};
use smb::{
    Client, DirAccessMask, File, FileAccessMask, FileCreateArgs, FileNetworkOpenInformation,
    ReadAt, Resource, Status, UncPath,
};

mod client;
mod config;
pub mod credentials;
mod listing;
mod names;
mod write;

use client::{MAX_REQUEST, REQUEST_DEADLINE, SmbClient};
use listing::file_time_to_utc;

/// A file handle and the client it was opened on.
type OpenFile = (Arc<Client>, Arc<File>);
pub use config::{SmbConfig, parse_smb_config};

/// Native id of the share root.
const ROOT_ID: &str = "/";

// ── Backend struct ───────────────────────────────────────────────────────────

/// SMB backend.
pub struct SmbBackend {
    config: SmbConfig,
    backend_id: String,
    display_name: String,
    share_path: UncPath,
    client: SmbClient,
}

impl std::fmt::Debug for SmbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmbBackend")
            .field("backend_id", &self.backend_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

// ── Factory ──────────────────────────────────────────────────────────────────

/// Create an SMB backend from a TOML config value.
///
/// Required keys: `host`, `share`, `username`.
///
/// Optional keys:
/// - `password` — takes precedence over the keychain; without it the password
///   is the keychain's SMB internet password for `host` and `username` on
///   macOS (see [`credentials`]) and the key is required elsewhere
/// - `id` — unique backend identifier, supplied by the caller; the Cascade
///   CLI sets it to the name the backend is configured under. The fallback
///   `"smb"` applies only when the crate is used without the CLI.
/// - `port` — TCP port (default: 445)
/// - `prefix` — sub-directory of the share to serve
///
/// The connection is established on first use.
///
/// # Errors
///
/// Returns an error if a required key is missing, the password is not found,
/// or the share path is invalid.
pub fn create_backend(config: &toml::Value) -> anyhow::Result<Box<dyn Backend>> {
    let smb_config = parse_smb_config(config, &credentials::system_store())?;
    let backend_id = config
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("smb")
        .to_string();
    Ok(Box::new(SmbBackend::new(smb_config, backend_id)?))
}

// ── Path mapping ─────────────────────────────────────────────────────────────

/// Normalise a backend-relative path to a native id (`a/b`, or `/` for the root).
fn native_id_for_path(path: &Path) -> String {
    let joined = path
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    if joined.is_empty() {
        ROOT_ID.to_string()
    } else {
        joined
    }
}

/// Native id of the parent of `native_id`.
fn parent_native_id(native_id: &str) -> &str {
    native_id
        .rsplit_once('/')
        .map_or(ROOT_ID, |(parent, _)| parent)
}

/// Native id of `name` inside the directory `parent`.
fn child_native_id(parent: &str, name: &str) -> String {
    if parent == ROOT_ID {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

/// The SMB status a server replied with, if `err` carries one.
fn status_of(err: &anyhow::Error) -> Option<u32> {
    err.chain()
        .find_map(|e| e.downcast_ref::<smb::Error>())
        .and_then(|e| match e {
            smb::Error::ReceivedErrorMessage(status, _)
            | smb::Error::UnexpectedMessageStatus(status) => Some(*status),
            _ => None,
        })
}

impl SmbBackend {
    /// Build a backend without connecting.
    ///
    /// # Errors
    ///
    /// Returns an error if `host` and `share` do not form a valid UNC path.
    pub fn new(config: SmbConfig, backend_id: String) -> anyhow::Result<Self> {
        let unc = format!(r"\\{}\{}", config.host, config.share);
        let share_path = UncPath::from_str(&unc)
            .map_err(|e| anyhow::anyhow!("invalid SMB share path {unc}: {e}"))?;
        let display_name = format!("{}/{}", config.host, config.share);
        Ok(Self {
            client: SmbClient::new(config.clone(), share_path.clone()),
            config,
            backend_id,
            display_name,
            share_path,
        })
    }

    /// Share-relative path of the item with the given native id, backslash-separated.
    fn share_relative(&self, native_id: &str) -> String {
        let relative = if native_id == ROOT_ID { "" } else { native_id };
        let full = match (&self.config.prefix, relative) {
            (Some(prefix), "") => prefix.clone(),
            (Some(prefix), rel) => format!("{prefix}/{rel}"),
            (None, rel) => rel.to_string(),
        };
        full.replace('/', "\\")
    }

    /// UNC path of the item with the given native id.
    fn unc_for(&self, native_id: &str) -> UncPath {
        self.share_path
            .clone()
            .with_path(&self.share_relative(native_id))
    }

    async fn open(
        &self,
        client: &Client,
        native_id: &str,
        access: FileAccessMask,
    ) -> anyhow::Result<Resource> {
        self.open_with(
            client,
            native_id,
            &FileCreateArgs::make_open_existing(access),
        )
        .await
    }

    async fn open_with(
        &self,
        client: &Client,
        native_id: &str,
        args: &FileCreateArgs,
    ) -> anyhow::Result<Resource> {
        client
            .create_file(&self.unc_for(native_id), args)
            .await
            .map_err(|err| {
                let err = anyhow::Error::from(err);
                match status_of(&err) {
                    Some(Status::U32_OBJECT_NAME_NOT_FOUND | Status::U32_OBJECT_PATH_NOT_FOUND) => {
                        BackendError::NotFound(native_id.to_string()).into()
                    }
                    _ => err,
                }
            })
    }

    fn entry_for(
        &self,
        native_id: &str,
        is_dir: bool,
        size: u64,
        mod_time: Option<DateTime<Utc>>,
    ) -> FileEntry {
        let id = ItemId::new(&self.backend_id, native_id);
        let parent_id = ItemId::new(&self.backend_id, parent_native_id(native_id));
        let name = native_id
            .rsplit_once('/')
            .map_or(native_id, |(_, name)| name)
            .to_string();
        let mut entry = if is_dir {
            FileEntry::dir(id, parent_id, name)
        } else {
            FileEntry::file(id, parent_id, name).with_size(Some(size))
        };
        entry.mod_time = mod_time;
        entry
    }

    async fn list_dir(&self, native_id: &str) -> anyhow::Result<Vec<(FileEntry, Revision)>> {
        self.client
            .run(|client| async move { self.list_dir_on(&client, native_id).await })
            .await
    }

    async fn list_dir_on(
        &self,
        client: &Client,
        native_id: &str,
    ) -> anyhow::Result<Vec<(FileEntry, Revision)>> {
        let access = DirAccessMask::new()
            .with_list_directory(true)
            .with_read_attributes(true)
            .with_synchronize(true)
            .into();
        let Resource::Directory(dir) = self.open(client, native_id, access).await? else {
            anyhow::bail!("SMB path {native_id} is not a directory");
        };
        let dir = Arc::new(dir);
        let listed = listing::list(&dir).await;
        dir.close().await?;
        Ok(listed?
            .into_iter()
            .map(|listed| {
                let entry = self.entry_for(
                    &child_native_id(native_id, &listed.name),
                    listed.is_dir,
                    listed.revision.size.unwrap_or(0),
                    listed.revision.last_write_time,
                );
                (entry, listed.revision)
            })
            .collect())
    }

    /// Read `length` bytes at `offset`, or to the end of the file when `None`,
    /// in requests of at most [`MAX_REQUEST`] on one handle reopened after a reconnect.
    async fn read(
        &self,
        native_id: &str,
        offset: u64,
        length: Option<usize>,
    ) -> anyhow::Result<Vec<u8>> {
        let handle = std::sync::Mutex::new(None);
        let read_args =
            || FileCreateArgs::make_open_existing(FileAccessMask::new().with_generic_read(true));
        let read_args = &read_args;
        let mut data = Vec::new();
        let result = loop {
            let want = length.map_or(MAX_REQUEST, |l| {
                l.saturating_sub(data.len()).min(MAX_REQUEST)
            });
            if want == 0 {
                break Ok(());
            }
            let at = offset.saturating_add(u64::try_from(data.len()).unwrap_or(u64::MAX));
            let handle = &handle;
            let read = self
                .client
                .run(move |client| async move {
                    let file = self
                        .file_on(handle, &client, native_id, read_args, read_args)
                        .await?;
                    let mut chunk = vec![0u8; want];
                    let n = file.read_at(&mut chunk, at).await?;
                    chunk.truncate(n);
                    Ok(chunk)
                })
                .await;
            match read {
                Ok(chunk) if chunk.is_empty() => break Ok(()),
                Ok(chunk) => data.extend_from_slice(&chunk),
                Err(e) => break Err(e),
            }
        };
        close_open_file(handle, native_id).await;
        result.map(|()| data)
    }

    /// The open file in `handle` when it belongs to `client`, otherwise one
    /// opened with `first`, or with `reopen` when `handle` held a file of an
    /// earlier client.
    async fn file_on(
        &self,
        handle: &std::sync::Mutex<Option<OpenFile>>,
        client: &Arc<Client>,
        native_id: &str,
        first: &(dyn Fn() -> FileCreateArgs + Sync),
        reopen: &(dyn Fn() -> FileCreateArgs + Sync),
    ) -> anyhow::Result<Arc<File>> {
        let (reusable, reopened) = {
            let held = handle.lock().unwrap_or_else(PoisonError::into_inner);
            let reusable = held
                .as_ref()
                .filter(|(owner, _)| Arc::ptr_eq(owner, client))
                .map(|(_, file)| Arc::clone(file));
            (reusable, held.is_some())
        };
        if let Some(file) = reusable {
            return Ok(file);
        }
        let args = if reopened { reopen() } else { first() };
        let Resource::File(file) = self.open_with(client, native_id, &args).await? else {
            anyhow::bail!("SMB path {native_id} is not a file");
        };
        let file = Arc::new(file);
        *handle.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((Arc::clone(client), Arc::clone(&file)));
        Ok(file)
    }

    /// Every entry under the directory `root`, parents before children.
    async fn list_tree(&self, root: &str) -> anyhow::Result<Vec<FileEntry>> {
        let mut all = Vec::new();
        let mut pending = vec![root.to_string()];
        while let Some(dir) = pending.pop() {
            for (entry, _) in self.list_dir(&dir).await? {
                if entry.is_dir {
                    pending.push(entry.id.native_id().to_string());
                }
                all.push(entry);
            }
        }
        Ok(all)
    }
}

/// Close the file left in `handle`, logging a failure.
async fn close_open_file(handle: std::sync::Mutex<Option<OpenFile>>, native_id: &str) {
    let open = handle.into_inner().unwrap_or_else(PoisonError::into_inner);
    if let Some((_, file)) = open {
        match tokio::time::timeout(REQUEST_DEADLINE, file.close()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::debug!(native_id, error = %e, "closing SMB file failed"),
            Err(_) => tracing::debug!(native_id, "closing SMB file timed out"),
        }
    }
}

// ── Backend impl ─────────────────────────────────────────────────────────────

#[async_trait]
impl Backend for SmbBackend {
    fn id(&self) -> &str {
        &self.backend_id
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn root_native_id(&self) -> &'static str {
        ROOT_ID
    }

    async fn quota(&self) -> anyhow::Result<Option<Quota>> {
        Ok(None)
    }

    async fn changes(&self, cursor: Option<&Cursor>) -> anyhow::Result<(Vec<Change>, Cursor)> {
        let since: Option<DateTime<Utc>> = cursor
            .and_then(|c| DateTime::parse_from_rfc3339(&c.0).ok())
            .map(|dt| dt.with_timezone(&Utc));
        let new_cursor = Cursor(Utc::now().to_rfc3339());

        let changes = self
            .list_tree(ROOT_ID)
            .await?
            .into_iter()
            .filter(|e| since.is_none_or(|since_dt| e.mod_time.is_none_or(|mt| mt > since_dt)))
            .map(Change::Created)
            .collect();
        Ok((changes, new_cursor))
    }

    async fn metadata(&self, path: &Path) -> anyhow::Result<FileEntry> {
        let native_id = &native_id_for_path(path);
        let access = FileAccessMask::new()
            .with_file_read_attributes(true)
            .with_synchronize(true);
        let (info, is_dir) = self
            .client
            .run(|client| async move {
                match self.open(&client, native_id, access).await? {
                    Resource::File(file) => {
                        let info = file.query_info::<FileNetworkOpenInformation>().await;
                        file.close().await?;
                        Ok((info?, false))
                    }
                    Resource::Directory(dir) => {
                        let info = dir.query_info::<FileNetworkOpenInformation>().await;
                        dir.close().await?;
                        Ok((info?, true))
                    }
                    Resource::Pipe(_) => anyhow::bail!("SMB path {native_id} is a pipe"),
                }
            })
            .await?;
        Ok(self.entry_for(
            native_id,
            is_dir,
            info.end_of_file,
            file_time_to_utc(info.last_write_time),
        ))
    }

    async fn download(&self, file: &FileEntry) -> anyhow::Result<Vec<u8>> {
        self.read(file.id.native_id(), 0, None).await
    }

    async fn read_range(
        &self,
        file: &FileEntry,
        offset: u64,
        length: u32,
    ) -> anyhow::Result<Vec<u8>> {
        let length = usize::try_from(length).unwrap_or(usize::MAX);
        self.read(file.id.native_id(), offset, Some(length)).await
    }

    async fn upload(
        &self,
        path: &Path,
        data: &[u8],
        _parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        let native_id = native_id_for_path(path);
        names::check(&native_id)?;
        self.write_content(&native_id, write::Target::CreateOrReplace, data, None)
            .await
    }

    async fn update(&self, file_id: &FileId, data: &[u8]) -> anyhow::Result<FileEntry> {
        self.write_content(file_id.native_id(), write::Target::Existing, data, None)
            .await
    }

    async fn upload_from_path(
        &self,
        path: &Path,
        local: &Path,
        _parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        let native_id = native_id_for_path(path);
        names::check(&native_id)?;
        let (source, last_write) = write::local_source(local).await?;
        self.write_content(
            &native_id,
            write::Target::CreateOrReplace,
            source,
            last_write,
        )
        .await
    }

    async fn update_from_path(&self, file_id: &FileId, local: &Path) -> anyhow::Result<FileEntry> {
        let (source, last_write) = write::local_source(local).await?;
        self.write_content(
            file_id.native_id(),
            write::Target::Existing,
            source,
            last_write,
        )
        .await
    }

    async fn replace(&self, source: &Path, target: &Path) -> anyhow::Result<(FileEntry, Revision)> {
        self.rename(
            &native_id_for_path(source),
            &native_id_for_path(target),
            true,
        )
        .await
    }

    async fn create_dir(&self, path: &Path) -> anyhow::Result<FileEntry> {
        let native_id = native_id_for_path(path);
        names::check(&native_id)?;
        self.create_directory(&native_id).await
    }

    async fn delete(&self, file: &FileEntry) -> anyhow::Result<()> {
        self.delete_tree(file.id.native_id(), file.is_dir).await
    }

    async fn move_entry(&self, src: &Path, dst: &Path) -> anyhow::Result<FileEntry> {
        let destination = native_id_for_path(dst);
        names::check(&destination)?;
        let (entry, _) = self
            .rename(&native_id_for_path(src), &destination, false)
            .await?;
        Ok(entry)
    }

    async fn list_children(&self, parent_native_id: &str) -> anyhow::Result<Vec<FileEntry>> {
        Ok(self
            .list_dir(parent_native_id)
            .await?
            .into_iter()
            .map(|(entry, _)| entry)
            .collect())
    }

    async fn list_children_with_revisions(
        &self,
        parent_native_id: &str,
    ) -> anyhow::Result<Vec<(FileEntry, Revision)>> {
        self.list_dir(parent_native_id).await
    }

    async fn revision(&self, entry: &FileEntry) -> anyhow::Result<Revision> {
        let native_id = entry.id.native_id();
        let access = FileAccessMask::new()
            .with_file_read_attributes(true)
            .with_synchronize(true);
        self.client
            .run(|client| async move {
                let (revision, close) = match self.open(&client, native_id, access).await? {
                    Resource::File(file) => (listing::revision_of(&file).await, file.close().await),
                    Resource::Directory(dir) => {
                        (listing::revision_of(&dir).await, dir.close().await)
                    }
                    Resource::Pipe(_) => anyhow::bail!("SMB path {native_id} is a pipe"),
                };
                close?;
                Ok(revision?.0)
            })
            .await
    }

    async fn poll_interval(&self) -> Option<Duration> {
        #[allow(unknown_lints, clippy::duration_suboptimal_units)]
        Some(Duration::from_secs(60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{CredentialStore, PasswordLocation};

    /// Fails every call: a TOML holding `password` needs no lookup.
    struct UnusedStore;

    impl CredentialStore for UnusedStore {
        fn find(&self, _: &str, _: &str, _: Option<u16>) -> anyhow::Result<Option<String>> {
            anyhow::bail!("credential store consulted")
        }

        fn store(
            &self,
            _: &str,
            _: &str,
            _: Option<u16>,
            _: &str,
        ) -> anyhow::Result<PasswordLocation> {
            anyhow::bail!("credential store consulted")
        }
    }

    fn make_config(extra: &[(&str, toml::Value)]) -> toml::Value {
        let mut table = toml::map::Map::new();
        for (key, value) in [
            ("host", "nas.local"),
            ("share", "data"),
            ("username", "cascade"),
            ("password", "secret"),
        ] {
            table.insert(key.to_string(), toml::Value::String(value.to_string()));
        }
        for (key, value) in extra {
            table.insert((*key).to_string(), value.clone());
        }
        toml::Value::Table(table)
    }

    #[test]
    fn create_backend_defaults_id_and_display_name() {
        let backend = create_backend(&make_config(&[])).unwrap();
        assert_eq!(backend.id(), "smb");
        assert_eq!(backend.display_name(), "nas.local/data");
        assert_eq!(backend.root_native_id(), "/");
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn create_backend_missing_password_errors() {
        let mut config = make_config(&[]);
        config.as_table_mut().unwrap().remove("password");
        let Err(err) = create_backend(&config) else {
            panic!("missing password must be rejected");
        };
        assert!(err.to_string().contains("'password'"));
    }

    #[test]
    fn parse_port_and_prefix() {
        let config = parse_smb_config(
            &make_config(&[
                ("port", toml::Value::Integer(1445)),
                ("prefix", toml::Value::String("/media/photos/".to_string())),
            ]),
            &UnusedStore,
        )
        .unwrap();
        assert_eq!(config.port, Some(1445));
        assert_eq!(config.prefix.as_deref(), Some("media/photos"));
    }

    #[test]
    fn parse_rejects_out_of_range_port() {
        let err = parse_smb_config(
            &make_config(&[("port", toml::Value::Integer(70_000))]),
            &UnusedStore,
        )
        .unwrap_err();
        assert!(err.to_string().contains("'port'"));
    }

    #[test]
    fn debug_output_omits_password() {
        let config = parse_smb_config(&make_config(&[]), &UnusedStore).unwrap();
        assert!(!format!("{config:?}").contains("secret"));
    }

    #[test]
    fn native_ids_map_paths() {
        assert_eq!(native_id_for_path(Path::new("")), "/");
        assert_eq!(native_id_for_path(Path::new("/")), "/");
        assert_eq!(native_id_for_path(Path::new("/a/b.txt")), "a/b.txt");
        assert_eq!(parent_native_id("a/b.txt"), "a");
        assert_eq!(parent_native_id("a"), "/");
        assert_eq!(child_native_id("/", "a"), "a");
        assert_eq!(child_native_id("a", "b"), "a/b");
    }
}
