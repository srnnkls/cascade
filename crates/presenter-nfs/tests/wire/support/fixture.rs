//! The presenter started as `cascade start --app-group` starts it: an
//! `NfsPresenter` over the engine's VFS tree with the content cache attached,
//! served by `NfsServer` on a loopback port, over the seeded mock backend.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use cascade_engine::backend::{Backend, MountedBackend, NullBackend};
use cascade_engine::content::store::ObjectStore;
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::content::writeback::WRITE_BACK_DELAY;
use cascade_engine::testing::{BackendLog, Daemon, Disk, MockBackend, MockOp, RecordingBackend};
use cascade_engine::vfs::{NEUTRAL_ROOT_ID, VfsTree};
use cascade_presenter_nfs::NfsPresenter;
use cascade_presenter_nfs::nfs::context::NfsContext;
use cascade_presenter_nfs::nfs::server::{NfsCacheMode, NfsServer, NfsServerConfig};

use super::client::Client;

/// Longer than the write-back delay with room for a slow runner.
pub const SETTLE: Duration = Duration::from_secs(30);
/// Longer than an upload retried once after a failure, with room for a slow
/// runner.
pub const SETTLE_AFTER_RETRY: Duration = Duration::from_mins(1);
/// Long enough for an upload that should not happen to have happened.
pub const QUIET: Duration = WRITE_BACK_DELAY.saturating_add(Duration::from_secs(3));
/// Bound on a listener of a stopped server releasing its port.
const REBIND: Duration = Duration::from_secs(5);
/// Id of a backend the daemon mounts at its default prefix.
pub const NAS: &str = "nas";

/// A second backend in the VFS tree, beside the root backend.
#[derive(Debug, Clone, Copy)]
pub struct Mount {
    pub prefix: &'static str,
    pub id: &'static str,
    pub registered_in_cache: bool,
}

/// How the backends are arranged in the VFS tree.
#[derive(Debug, Clone, Copy)]
pub enum Layout {
    /// The mock backend at the root of the tree.
    Root,
    /// The mock backend at the root and a second backend at a prefix.
    Mounting(Mount),
    /// The mock backend named [`NAS`] under the neutral root at the prefix
    /// the engine derives for it, the content cache attached when `cached`.
    Named { cached: bool },
}

/// Starts `NfsServer` for `ctx` on `127.0.0.1:port` as the daemon does,
/// retrying while a stopped server still holds the port.
pub async fn serve(ctx: Arc<NfsContext>, mode: NfsCacheMode, port: u16) -> Result<NfsServer> {
    let deadline = tokio::time::Instant::now() + REBIND;
    loop {
        let config = NfsServerConfig {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            export_path: "/".to_owned(),
            cache_mode: mode,
        };
        match NfsServer::start(config, Arc::clone(&ctx)).await {
            Ok(server) => return Ok(server),
            Err(_) if port != 0 && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error).context("start the NFS server"),
        }
    }
}

/// Stops `server` through the presenter's stop path.
pub fn stop(server: NfsServer) -> Result<()> {
    server.stop()
}

/// Polls `done` until it holds, failing with `what` after `limit`.
pub async fn eventually_within(
    limit: Duration,
    what: &str,
    mut done: impl FnMut() -> bool,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + limit;
    while !done() {
        if tokio::time::Instant::now() >= deadline {
            bail!("{what}: not within {limit:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

pub async fn eventually(what: &str, done: impl FnMut() -> bool) -> Result<()> {
    eventually_within(SETTLE, what, done).await
}

/// The content-cache daemon over the mock backend with the NFS server on top.
pub struct Fixture {
    root: tempfile::TempDir,
    disk: Disk,
    layout: Layout,
    mode: NfsCacheMode,
    backend: Arc<dyn Backend>,
    mounted: Option<Arc<dyn Backend>>,
    pub mock: Arc<MockBackend>,
    pub other: Arc<MockBackend>,
    pub log: BackendLog,
    pub other_log: BackendLog,
    pub daemon: Daemon,
    server: Option<NfsServer>,
}

impl std::fmt::Debug for Fixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fixture")
            .field("root", &self.root.path())
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

impl Fixture {
    pub async fn new(config: TransferConfig) -> Result<Self> {
        Self::build(config, Layout::Root, NfsCacheMode::default()).await
    }

    pub async fn build(config: TransferConfig, layout: Layout, mode: NfsCacheMode) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let mock = Arc::new(MockBackend::new(11));
        let other = Arc::new(MockBackend::new(13));
        let log = BackendLog::default();
        let other_log = BackendLog::default();
        let backend: Arc<dyn Backend> = match layout {
            Layout::Named { .. } => RecordingBackend::named(&mock, &log, NAS),
            Layout::Root | Layout::Mounting(_) => RecordingBackend::new(&mock, &log),
        };
        let mounted: Option<Arc<dyn Backend>> = match layout {
            Layout::Mounting(mount) => Some(RecordingBackend::named(&other, &other_log, mount.id)),
            Layout::Root | Layout::Named { .. } => None,
        };
        let disk = Disk::at(root.path(), config)?;
        let daemon = disk.start(Arc::clone(&backend)).await?;
        let mut fixture = Self {
            root,
            disk,
            layout,
            mode,
            backend,
            mounted,
            mock,
            other,
            log,
            other_log,
            daemon,
            server: None,
        };
        fixture.register_mounted()?;
        fixture.server = Some(serve(fixture.context(), mode, 0).await?);
        Ok(fixture)
    }

    fn register_mounted(&self) -> Result<()> {
        if let (Layout::Mounting(mount), Some(mounted)) = (self.layout, &self.mounted)
            && mount.registered_in_cache
        {
            self.daemon.cache.register(Arc::clone(mounted))?;
        }
        Ok(())
    }

    fn tree(&self) -> VfsTree {
        match self.layout {
            Layout::Root | Layout::Mounting(_) => {
                let mut tree = VfsTree::new(Arc::clone(&self.backend));
                if let (Layout::Mounting(mount), Some(mounted)) = (self.layout, &self.mounted) {
                    tree.mount(PathBuf::from(mount.prefix), Arc::clone(mounted));
                }
                tree
            }
            Layout::Named { .. } => {
                let mut tree = VfsTree::new(Arc::new(NullBackend::new(NEUTRAL_ROOT_ID)));
                let prefix = MountedBackend::at_default(Arc::clone(&self.backend)).resolve_prefix();
                tree.mount(prefix, Arc::clone(&self.backend));
                tree
            }
        }
    }

    /// The NFS context the presenter builds, as `AppGroupDaemon::start` does.
    fn context(&self) -> Arc<NfsContext> {
        let cached = !matches!(self.layout, Layout::Named { cached: false });
        let presenter = NfsPresenter::with_vfs(
            self.root.path().join("mnt"),
            Arc::new(RwLock::new(self.tree())),
        )
        .with_db(Arc::clone(&self.daemon.db));
        let presenter = if cached {
            presenter.with_content_cache(self.daemon.cache.clone())
        } else {
            presenter
        };
        Arc::clone(presenter.context())
    }

    pub fn addr(&self) -> Result<SocketAddr> {
        Ok(self
            .server
            .as_ref()
            .context("the server is stopped")?
            .local_addr)
    }

    /// A client that mounted the export.
    pub async fn client(&self) -> Result<Client> {
        Client::mount(self.addr()?).await
    }

    /// Stops the NFS server.
    pub fn stop(&mut self) -> Result<()> {
        stop(
            self.server
                .take()
                .context("the server is already stopped")?,
        )
    }

    /// Stops the server, reopens the daemon on the same state database and
    /// starts the server again on the same port, as a daemon restart does.
    pub async fn restart(&mut self) -> Result<()> {
        let port = self.addr()?.port();
        self.stop()?;
        self.daemon = self.disk.start(Arc::clone(&self.backend)).await?;
        self.register_mounted()?;
        self.server = Some(serve(self.context(), self.mode, port).await?);
        Ok(())
    }

    /// The uid of the item at the backend-relative `path`.
    pub async fn uid(&self, path: &str) -> Result<u64> {
        self.daemon
            .resolve(path)
            .await?
            .with_context(|| format!("{path} is not in the replica"))
    }

    /// The object store of the daemon's cache directory.
    pub fn objects(&self) -> Result<ObjectStore> {
        Ok(ObjectStore::open(self.root.path().join("cache"))?)
    }

    /// Content at `path` on the backend, read past any injected fault.
    pub fn on_backend(&self, path: &str) -> Option<Vec<u8>> {
        self.mock.content(Path::new(path))
    }

    /// Names in a folder of the backend, temporary uploads included.
    pub async fn names_on_backend(&self, folder: &str) -> Result<Vec<String>> {
        names_in(&self.mock, folder).await
    }

    pub fn placed(&self) -> Vec<Vec<u8>> {
        self.log
            .placed()
            .into_iter()
            .map(|placed| placed.data)
            .collect()
    }

    pub fn uploaded(&self) -> usize {
        [MockOp::Upload, MockOp::Update, MockOp::Replace]
            .into_iter()
            .map(|op| self.log.calls(op))
            .sum()
    }

    /// Saves `data` over the item at `path` as File Provider's `modify` does.
    pub async fn save(&self, path: &str, data: &[u8]) -> Result<()> {
        let uid = self.uid(path).await?;
        let incoming = self.daemon.incoming_file(data)?;
        self.daemon.cache.save(uid, &incoming).await?;
        std::fs::remove_file(&incoming)?;
        Ok(())
    }

    /// Asserts the root-level `report.txt` keeps its content and its item.
    pub async fn assert_root_report_untouched(&self) -> Result<()> {
        anyhow::ensure!(
            self.on_backend("report.txt").as_deref() == Some(b"at the root".as_slice()),
            "report.txt at the root changed on the backend"
        );
        anyhow::ensure!(
            self.daemon.resolve("report.txt").await?.is_some(),
            "report.txt at the root left the replica"
        );
        Ok(())
    }

    /// Asserts nothing named `name` exists at the export root, on the backend
    /// or in the replica.
    pub async fn assert_absent_at_root(&self, name: &str) -> Result<()> {
        anyhow::ensure!(self.on_backend(name).is_none(), "{name} on the backend");
        anyhow::ensure!(
            self.daemon.resolve(name).await?.is_none(),
            "{name} in the replica"
        );
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            let _ = server.stop();
        }
    }
}

/// Names in a folder of `mock`, temporary uploads included, sorted.
pub async fn names_in(mock: &MockBackend, folder: &str) -> Result<Vec<String>> {
    let folder = mock.metadata(Path::new(folder)).await?;
    let mut names: Vec<String> = mock
        .list_children(folder.id.native_id())
        .await?
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    names.sort();
    Ok(names)
}

/// Every path on `mock` below `folder`, folders included.
pub async fn tree_of(mock: &MockBackend, folder: &str) -> Result<Vec<String>> {
    let mut found = Vec::new();
    let mut pending = vec![folder.to_owned()];
    while let Some(dir) = pending.pop() {
        let entry = mock.metadata(Path::new(&dir)).await?;
        for child in mock.list_children(entry.id.native_id()).await? {
            let path = if dir.is_empty() {
                child.name.clone()
            } else {
                format!("{dir}/{}", child.name)
            };
            if child.is_dir {
                pending.push(path.clone());
            }
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}
