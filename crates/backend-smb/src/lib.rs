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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cascade_engine::backend::Backend;
use cascade_engine::types::{Change, Cursor, FileEntry, FileId, ItemId, Quota};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use smb::binrw_util::file_time::FileTime;
use smb::{
    Client, ClientConfig, ConnectionConfig, DirAccessMask, Directory, FileAccessMask,
    FileCreateArgs, FileFullDirectoryInformation, FileNetworkOpenInformation, ReadAt, Resource,
    UncPath,
};
use tokio::sync::OnceCell;

/// Native id of the share root.
const ROOT_ID: &str = "/";

/// Upper bound on a single `read_at` request when draining a whole file.
const DOWNLOAD_CHUNK: usize = 1024 * 1024;

// ── Config ───────────────────────────────────────────────────────────────────

/// Configuration for an SMB backend.
#[derive(Clone)]
pub struct SmbConfig {
    /// Server host name or address.
    pub host: String,
    /// Share name on the server.
    pub share: String,
    /// TCP port; `None` uses the SMB default (445).
    pub port: Option<u16>,
    /// User name for NTLM authentication.
    pub username: String,
    /// Password for NTLM authentication.
    pub password: String,
    /// Optional sub-directory of the share to serve (no leading or trailing slash).
    pub prefix: Option<String>,
}

impl std::fmt::Debug for SmbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmbConfig")
            .field("host", &self.host)
            .field("share", &self.share)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

// ── Backend struct ───────────────────────────────────────────────────────────

/// SMB backend.
pub struct SmbBackend {
    config: SmbConfig,
    backend_id: String,
    display_name: String,
    share_path: UncPath,
    client: OnceCell<Client>,
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
/// Required keys: `host`, `share`, `username`, `password`.
///
/// Optional keys:
/// - `id` — unique backend identifier (default: `"smb"`)
/// - `port` — TCP port (default: 445)
/// - `prefix` — sub-directory of the share to serve
///
/// The connection is established on first use.
///
/// # Errors
///
/// Returns an error if a required key is missing or the share path is invalid.
pub fn create_backend(config: &toml::Value) -> anyhow::Result<Box<dyn Backend>> {
    let smb_config = parse_smb_config(config)?;
    let backend_id = config
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("smb")
        .to_string();
    Ok(Box::new(SmbBackend::new(smb_config, backend_id)?))
}

fn parse_smb_config(config: &toml::Value) -> anyhow::Result<SmbConfig> {
    let get_str = |key: &str| -> anyhow::Result<String> {
        config
            .get(key)
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| anyhow::anyhow!("SMB backend requires '{key}' config"))
    };

    let port = config
        .get("port")
        .and_then(toml::Value::as_integer)
        .map(u16::try_from)
        .transpose()
        .map_err(|_| anyhow::anyhow!("SMB backend 'port' must be between 0 and 65535"))?;

    Ok(SmbConfig {
        host: get_str("host")?,
        share: get_str("share")?,
        port,
        username: get_str("username")?,
        password: get_str("password")?,
        prefix: config
            .get("prefix")
            .and_then(|v| v.as_str())
            .map(|s| s.trim_matches('/'))
            .filter(|s| !s.is_empty())
            .map(String::from),
    })
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

fn file_time_to_utc(time: FileTime) -> Option<DateTime<Utc>> {
    (!time.is_zero()).then(|| DateTime::<Utc>::from(std::time::SystemTime::from(time)))
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
            config,
            backend_id,
            display_name,
            share_path,
            client: OnceCell::new(),
        })
    }

    async fn client(&self) -> anyhow::Result<&Client> {
        self.client
            .get_or_try_init(|| async {
                let client = Client::new(ClientConfig {
                    dfs: false,
                    connection: ConnectionConfig {
                        port: self.config.port,
                        ..Default::default()
                    },
                    ..Default::default()
                });
                client
                    .share_connect(
                        &self.share_path,
                        &self.config.username,
                        self.config.password.clone(),
                    )
                    .await?;
                tracing::debug!(share = %self.display_name, "connected to SMB share");
                anyhow::Ok(client)
            })
            .await
    }

    /// UNC path of the item with the given native id.
    fn unc_for(&self, native_id: &str) -> UncPath {
        let relative = if native_id == ROOT_ID { "" } else { native_id };
        let full = match (&self.config.prefix, relative) {
            (Some(prefix), "") => prefix.clone(),
            (Some(prefix), rel) => format!("{prefix}/{rel}"),
            (None, rel) => rel.to_string(),
        };
        self.share_path.clone().with_path(&full.replace('/', "\\"))
    }

    async fn open(&self, native_id: &str, access: FileAccessMask) -> anyhow::Result<Resource> {
        let client = self.client().await?;
        let resource = client
            .create_file(
                &self.unc_for(native_id),
                &FileCreateArgs::make_open_existing(access),
            )
            .await?;
        Ok(resource)
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

    async fn list_dir(&self, native_id: &str) -> anyhow::Result<Vec<FileEntry>> {
        let access = DirAccessMask::new()
            .with_list_directory(true)
            .with_synchronize(true)
            .into();
        let Resource::Directory(dir) = self.open(native_id, access).await? else {
            anyhow::bail!("SMB path {native_id} is not a directory");
        };
        let dir = Arc::new(dir);
        let mut entries = Vec::new();
        {
            let mut stream = Directory::query::<FileFullDirectoryInformation>(&dir, "*").await?;
            while let Some(info) = stream.next().await {
                let info = info?;
                let name = info.file_name.to_string();
                if name == "." || name == ".." {
                    continue;
                }
                entries.push(self.entry_for(
                    &child_native_id(native_id, &name),
                    info.file_attributes.directory(),
                    info.end_of_file,
                    file_time_to_utc(info.last_write_time),
                ));
            }
        }
        dir.close().await?;
        Ok(entries)
    }

    /// Every entry under the share, parents before children.
    async fn list_tree(&self) -> anyhow::Result<Vec<FileEntry>> {
        let mut all = Vec::new();
        let mut pending = vec![ROOT_ID.to_string()];
        while let Some(dir) = pending.pop() {
            for entry in self.list_dir(&dir).await? {
                if entry.is_dir {
                    pending.push(entry.id.native_id().to_string());
                }
                all.push(entry);
            }
        }
        Ok(all)
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
            .list_tree()
            .await?
            .into_iter()
            .filter(|e| since.is_none_or(|since_dt| e.mod_time.is_none_or(|mt| mt > since_dt)))
            .map(Change::Created)
            .collect();
        Ok((changes, new_cursor))
    }

    async fn metadata(&self, path: &Path) -> anyhow::Result<FileEntry> {
        let native_id = native_id_for_path(path);
        let access = FileAccessMask::new()
            .with_file_read_attributes(true)
            .with_synchronize(true);
        let (info, is_dir) = match self.open(&native_id, access).await? {
            Resource::File(file) => {
                let info = file.query_info::<FileNetworkOpenInformation>().await;
                file.close().await?;
                (info?, false)
            }
            Resource::Directory(dir) => {
                let info = dir.query_info::<FileNetworkOpenInformation>().await;
                dir.close().await?;
                (info?, true)
            }
            Resource::Pipe(_) => anyhow::bail!("SMB path {native_id} is a pipe"),
        };
        Ok(self.entry_for(
            &native_id,
            is_dir,
            info.end_of_file,
            file_time_to_utc(info.last_write_time),
        ))
    }

    async fn download(&self, file: &FileEntry) -> anyhow::Result<Vec<u8>> {
        let native_id = file.id.native_id();
        let Resource::File(handle) = self
            .open(native_id, FileAccessMask::new().with_generic_read(true))
            .await?
        else {
            anyhow::bail!("SMB path {native_id} is not a file");
        };
        let mut data = Vec::new();
        let mut chunk = vec![0u8; DOWNLOAD_CHUNK];
        let result = loop {
            let offset = u64::try_from(data.len()).unwrap_or(u64::MAX);
            match handle.read_at(&mut chunk, offset).await {
                Ok(0) => break Ok(()),
                Ok(n) => data.extend_from_slice(chunk.get(..n).unwrap_or_default()),
                Err(e) => break Err(e),
            }
        };
        handle.close().await?;
        result?;
        Ok(data)
    }

    async fn read_range(
        &self,
        file: &FileEntry,
        offset: u64,
        length: u32,
    ) -> anyhow::Result<Vec<u8>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let native_id = file.id.native_id();
        let Resource::File(handle) = self
            .open(native_id, FileAccessMask::new().with_generic_read(true))
            .await?
        else {
            anyhow::bail!("SMB path {native_id} is not a file");
        };
        let mut buf = vec![0u8; usize::try_from(length).unwrap_or(usize::MAX)];
        let mut filled = 0;
        let result = loop {
            let Some(rest) = buf.get_mut(filled..) else {
                break Ok(());
            };
            if rest.is_empty() {
                break Ok(());
            }
            let at = offset.saturating_add(u64::try_from(filled).unwrap_or(u64::MAX));
            match handle.read_at(rest, at).await {
                Ok(0) => break Ok(()),
                Ok(n) => filled += n,
                Err(e) => break Err(e),
            }
        };
        handle.close().await?;
        result?;
        buf.truncate(filled);
        Ok(buf)
    }

    async fn upload(
        &self,
        _path: &Path,
        _data: &[u8],
        _parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        anyhow::bail!("SMB backend does not support writes yet")
    }

    async fn update(&self, _file_id: &FileId, _data: &[u8]) -> anyhow::Result<FileEntry> {
        anyhow::bail!("SMB backend does not support writes yet")
    }

    async fn create_dir(&self, _path: &Path) -> anyhow::Result<FileEntry> {
        anyhow::bail!("SMB backend does not support writes yet")
    }

    async fn delete(&self, _file: &FileEntry) -> anyhow::Result<()> {
        anyhow::bail!("SMB backend does not support writes yet")
    }

    async fn move_entry(&self, _src: &Path, _dst: &Path) -> anyhow::Result<FileEntry> {
        anyhow::bail!("SMB backend does not support writes yet")
    }

    async fn list_children(&self, parent_native_id: &str) -> anyhow::Result<Vec<FileEntry>> {
        self.list_dir(parent_native_id).await
    }

    async fn poll_interval(&self) -> Option<Duration> {
        #[allow(unknown_lints, clippy::duration_suboptimal_units)]
        Some(Duration::from_secs(60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let config = parse_smb_config(&make_config(&[
            ("port", toml::Value::Integer(1445)),
            ("prefix", toml::Value::String("/media/photos/".to_string())),
        ]))
        .unwrap();
        assert_eq!(config.port, Some(1445));
        assert_eq!(config.prefix.as_deref(), Some("media/photos"));
    }

    #[test]
    fn parse_rejects_out_of_range_port() {
        let err =
            parse_smb_config(&make_config(&[("port", toml::Value::Integer(70_000))])).unwrap_err();
        assert!(err.to_string().contains("'port'"));
    }

    #[test]
    fn debug_output_omits_password() {
        let config = parse_smb_config(&make_config(&[])).unwrap();
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
