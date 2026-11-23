use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use cascade_engine::backend::MountedBackend;
use cascade_engine::content::cache::ContentCache;
use cascade_engine::engine::{EngineHandle, NativeEngine};
use cascade_engine::presenter::VfsPresenter;
use cascade_presenter_fileprovider::server::ContentServer;
use cascade_presenter_nfs::NfsPresenter;
use cascade_presenter_nfs::nfs::server::NfsServer;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::helpers::{
    apply_root_pins, engine_config, ensure_directory, root_config, write_pid_file,
};
use super::nfs::start_nfs_volume;
use super::{
    CliContext, PresenterResources, ResolvedP2pConfig, WebOptions, finish_shutdown, mark_web_ready,
    start_web, stop_web, wait_for_shutdown_signal,
};

/// Interval of the change scan of listed and materialised folders.
const APP_GROUP_SCAN_INTERVAL: Duration = Duration::from_mins(1);

/// A presenter that consumes nothing.
///
/// The File Provider model is *pull-based*: Finder, via the
/// `NSFileProviderReplicatedExtension`, drives the daemon by calling
/// `enumerateItems` / `enumerateChanges` / `fetchContents` over the RPC
/// socket and tracking the sync anchor. Nothing in the extension listens
/// for engine-initiated push notifications, so the sync runner — which
/// exists to poll backends, apply changes to the state DB, and feed the
/// change index — has no presenter to push to. This no-op satisfies the
/// `VfsPresenter` contract while discarding the push direction the File
/// Provider path does not use.
#[derive(Debug)]
pub(super) struct NoopPresenter;

#[async_trait::async_trait]
impl VfsPresenter for NoopPresenter {
    async fn upsert_item(&self, _item: cascade_engine::types::VfsItem) -> Result<()> {
        Ok(())
    }

    async fn delete_item(&self, _id: &cascade_engine::types::ItemId) -> Result<()> {
        Ok(())
    }

    async fn update_state(
        &self,
        _id: &cascade_engine::types::ItemId,
        _state: cascade_engine::types::CacheState,
    ) -> Result<()> {
        Ok(())
    }

    async fn fetch_contents(&self, _id: &cascade_engine::types::ItemId) -> Result<PathBuf> {
        // Content is fetched on demand through the File Provider RPC
        // server's `fetchContents` handler, never through the sync
        // runner's presenter. Reaching here means the runner tried to
        // pull content itself, which it does not do for File Provider.
        anyhow::bail!("no-op presenter does not serve content")
    }

    async fn evict_item(&self, _id: &cascade_engine::types::ItemId) -> Result<()> {
        Ok(())
    }

    async fn start(&self, _mount_point: &Path) -> Result<()> {
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

/// Try to serve the macOS File Provider RPC bridge.
///
/// Unlike `FSKit`, the daemon does not mount anything here. It stands up the
/// Unix-socket server (`fp.sock` in the App Group container or config directory) that the
/// Swift `CascadeFileProvider` extension connects to per request, and runs
/// the sync runner so the state database and the engine's change feed stay
/// populated. The extension surfaces the tree in Finder once its File
/// Provider domain is registered — a step performed by the Swift host app
/// (`NSFileProviderManager`), not by this daemon. See
/// `docs/fileprovider-smoke-test.md` for the end-to-end bring-up.
///
/// With `app_group` the [`AppGroupDaemon`] serves protocol v2 and the
/// NFS volume instead, and `no_mount` skips the NFS mount. Without it,
/// `no_mount` has no effect: the extension owns the mount.
pub(super) async fn try_fileprovider(
    ctx: &CliContext,
    mount_path: &Path,
    backends: Vec<cascade_engine::backend::MountedBackend>,
    no_mount: bool,
    app_group: bool,
    p2p: &ResolvedP2pConfig,
    web: &WebOptions,
) -> Result<()> {
    use cascade_presenter_fileprovider::engine_handlers::EngineHandlers;
    use cascade_presenter_fileprovider::server::FileProviderServer;

    if app_group {
        return try_app_group(ctx, mount_path, backends, no_mount, p2p, web).await;
    }

    let paths = ctx.app_paths().context("resolve File Provider paths")?;
    let root = root_config(mount_path)?;

    let engine = Arc::new(NativeEngine::new(engine_config(
        ctx, mount_path, backends, &paths, &root, p2p,
    ))?);
    // Wire the management-plane dispatcher into every backend that serves
    // remote management (the P2P backend). Done before the presenter starts so
    // an inbound ManageRequest is authorised and executed rather than refused.
    engine.wire_manage_dispatch().await;
    apply_root_pins(&engine, &root).await?;

    // Clone the engine handle for the HTTP API before the engine is moved into
    // the presenter resources; the API server is started only once the winning
    // presenter is confirmed up, so a failed attempt never binds the port.
    let engine_for_web = engine.clone();

    // The File Provider RPC server answers inbound queries from the Swift
    // extension against the engine's live VFS, state DB, cache directory,
    // and change feed.
    let socket_path = paths.bindable_socket_path()?.to_path_buf();
    ensure_directory(&paths.cache_dir, "File Provider cache directory")?;
    ensure_directory(&paths.staging_dir, "File Provider staging directory")?;
    let handlers = Arc::new(EngineHandlers::new(
        engine.vfs().clone(),
        engine.db().clone(),
        paths.cache_dir,
        engine.change_feed(),
    ));

    let server = Arc::new(FileProviderServer::new(socket_path.clone(), handlers));

    // Cancellation channel for the server task; signalled on Ctrl+C below.
    let (server_cancel_tx, server_cancel_rx) = tokio::sync::watch::channel(false);
    let server_for_task = server.clone();
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server_for_task.serve(server_cancel_rx).await {
            tracing::error!(error = %e, "File Provider server exited with error");
        }
    });

    // The sync runner drives backend polling: it populates the state DB
    // and the change feed (attached by `create_sync_runner`). Its push
    // direction has no consumer in the pull-based File Provider model, so
    // it pushes to a no-op presenter.
    let sync_runner = engine.create_sync_runner(Arc::new(NoopPresenter));
    let cancel = engine.cancel_flag();
    let sync_handle = tokio::spawn(async move {
        if let Err(e) = sync_runner.run(cancel).await {
            tracing::error!(error = %e, "sync runner exited with error");
        }
    });

    let engine_handle = engine.start()?;

    let resources = PresenterResources {
        engine,
        engine_handle,
        sync_handle,
    };

    if let Err(e) = write_pid_file(&ctx.pid_path) {
        let _ = server_cancel_tx.send(true);
        server_handle.abort();
        resources.shutdown();
        return Err(e);
    }

    println!("Cascade started (File Provider bridge).");
    println!("  RPC socket: {}", socket_path.display());
    println!("  PID: {}", std::process::id());
    println!();
    println!("The Swift File Provider extension surfaces the tree in Finder once its");
    println!(
        "domain is registered via the Cascade host app — see docs/fileprovider-smoke-test.md."
    );
    println!();
    println!("Press Ctrl+C to stop.");

    // The presenter is up: spawn the HTTP API (feature-gated, when enabled) and
    // flip the F3 data-plane readiness bit so its data routes begin serving.
    let web_runtime = start_web(&engine_for_web, web).await?;
    mark_web_ready(&web_runtime);

    wait_for_shutdown_signal().await?;

    // Stop the HTTP API before tearing the rest down.
    stop_web(web_runtime).await;

    tracing::info!("Shutting down...");

    let _ = server_cancel_tx.send(true);
    // Give the server task a moment to unbind the socket cleanly, then
    // abort if it has not returned.
    if tokio::time::timeout(std::time::Duration::from_secs(2), server_handle)
        .await
        .is_err()
    {
        tracing::warn!("File Provider server did not stop within 2s");
    }
    let _ = std::fs::remove_file(&socket_path);
    resources.shutdown();

    let _ = std::fs::remove_file(&ctx.pid_path);

    tracing::info!("Cascade stopped.");
    Ok(())
}

/// How long [`AppGroupDaemon::shutdown`] waits for the socket server to stop.
const SERVER_STOP_DEADLINE: Duration = Duration::from_secs(2);

/// The `--app-group` daemon: one engine and one content cache behind the
/// File Provider protocol v2 socket and the NFS volume, with the change
/// scanner, the write-back worker and the eviction sweep running beside them.
pub(super) struct AppGroupDaemon {
    socket_path: PathBuf,
    engine: Arc<NativeEngine>,
    engine_handle: EngineHandle,
    server_cancel: watch::Sender<bool>,
    server_handle: JoinHandle<()>,
    scanner_handle: JoinHandle<()>,
    #[cfg(test)]
    nfs: Arc<cascade_presenter_nfs::nfs::context::NfsContext>,
    nfs_server: NfsServer,
    mount: Option<PathBuf>,
}

impl AppGroupDaemon {
    /// Starts every component and returns once the socket accepts
    /// connections and the NFS server listens; the volume is mounted at
    /// `mount_path` unless `no_mount` is set.
    ///
    /// # Errors
    ///
    /// Fails when the engine, its content cache, recovery, the socket, the
    /// NFS server or the mount cannot be brought up; components already
    /// started are stopped first.
    pub(super) async fn start(
        ctx: &CliContext,
        mount_path: &Path,
        backends: Vec<MountedBackend>,
        no_mount: bool,
        p2p: &ResolvedP2pConfig,
        scan_interval: Duration,
    ) -> Result<Self> {
        let paths = ctx.app_paths().context("resolve App Group paths")?;
        let socket_path = paths.bindable_socket_path()?.to_path_buf();
        let root = root_config(mount_path)?;
        let engine = Arc::new(NativeEngine::new(engine_config(
            ctx, mount_path, backends, &paths, &root, p2p,
        ))?);
        engine.wire_manage_dispatch().await;
        let cache = engine.content_cache().context("open the content cache")?;
        engine
            .apply_ignore_rules(&root)
            .await
            .context("apply the ignore rules of the root .cascade config")?;
        apply_root_pins(&engine, &root).await?;
        cache.recover().await.context("recover the content cache")?;

        let server = ContentServer::new(socket_path.clone(), cache.clone());
        let listener = match server.listen().await {
            Ok(listener) => listener,
            Err(error) => {
                engine.shutdown();
                return Err(error).context("start the File Provider server");
            }
        };
        let (server_cancel, cancel_rx) = watch::channel(false);
        let server_handle = tokio::spawn(async move {
            if let Err(error) = server.serve_on(listener, cancel_rx).await {
                tracing::error!(error = %format!("{error:#}"), "File Provider server exited with error");
            }
        });

        let presenter = NfsPresenter::with_vfs(mount_path, engine.vfs().clone())
            .with_db(engine.db().clone())
            .with_content_cache(cache.clone());
        let nfs = Arc::clone(presenter.context());
        let nfs_server =
            match start_nfs_volume(mount_path, no_mount, Arc::clone(&nfs), &root.nfs).await {
                Ok(server) => server,
                Err(error) => {
                    let _ = server_cancel.send(true);
                    server_handle.abort();
                    engine.shutdown();
                    return Err(error);
                }
            };

        let scanner_handle = tokio::spawn(run_scanner(cache, scan_interval));
        let engine_handle = engine.start()?;

        Ok(Self {
            socket_path,
            engine,
            engine_handle,
            server_cancel,
            server_handle,
            scanner_handle,
            #[cfg(test)]
            nfs,
            nfs_server,
            mount: (!no_mount).then(|| mount_path.to_path_buf()),
        })
    }

    /// The File Provider socket, `fp.sock` in the App Group container.
    pub(super) fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// The NFS handlers serving the volume.
    #[cfg(test)]
    pub(super) const fn nfs(&self) -> &Arc<cascade_presenter_nfs::nfs::context::NfsContext> {
        &self.nfs
    }

    /// The mount point of the NFS volume, unless it was started unmounted.
    pub(super) fn mount(&self) -> Option<&Path> {
        self.mount.as_deref()
    }

    /// Stops the scanner, the socket server, the NFS server and the
    /// engine's background tasks, in that order; the caller unmounts
    /// [`Self::mount`] first.
    pub(super) async fn shutdown(self) {
        self.scanner_handle.abort();
        let _ = self.server_cancel.send(true);
        if tokio::time::timeout(SERVER_STOP_DEADLINE, self.server_handle)
            .await
            .is_err()
        {
            tracing::warn!(deadline = ?SERVER_STOP_DEADLINE, "File Provider server did not stop in time");
        }
        let _ = std::fs::remove_file(&self.socket_path);
        if let Err(error) = self.nfs_server.stop() {
            tracing::warn!(error = %error, "NFS server stop returned an error");
        }
        self.engine.shutdown();
        self.engine_handle.abort();
    }
}

/// Rescans the listed and materialised folders every `interval`.
async fn run_scanner(cache: ContentCache, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(error) = cache.scan().await {
            tracing::warn!(error = %format!("{error:#}"), "change scan failed");
        }
    }
}

/// Serve `cascade start --app-group`: start the [`AppGroupDaemon`], write
/// the PID file, wait for the shutdown signal and stop in order.
async fn try_app_group(
    ctx: &CliContext,
    mount_path: &Path,
    backends: Vec<MountedBackend>,
    no_mount: bool,
    p2p: &ResolvedP2pConfig,
    web: &WebOptions,
) -> Result<()> {
    let daemon = AppGroupDaemon::start(
        ctx,
        mount_path,
        backends,
        no_mount,
        p2p,
        APP_GROUP_SCAN_INTERVAL,
    )
    .await?;

    if let Err(error) = write_pid_file(&ctx.pid_path) {
        let mount = daemon.mount().map(Path::to_path_buf);
        finish_shutdown(&ctx.pid_path, mount, daemon.shutdown()).await?;
        return Err(error);
    }

    println!("Cascade started (File Provider and NFS).");
    println!("  RPC socket: {}", daemon.socket_path().display());
    if no_mount {
        println!("  NFS port: {}", daemon.nfs_server.local_addr.port());
    } else {
        println!("  Mount point: {}", mount_path.display());
    }
    println!("  PID: {}", std::process::id());
    println!();
    println!("Press Ctrl+C to stop.");

    let web_runtime = start_web(&daemon.engine, web).await?;
    mark_web_ready(&web_runtime);

    wait_for_shutdown_signal().await?;

    stop_web(web_runtime).await;
    tracing::info!("Shutting down...");
    let mount = daemon.mount().map(Path::to_path_buf);
    finish_shutdown(&ctx.pid_path, mount, daemon.shutdown()).await
}

#[cfg(test)]
#[path = "fileprovider_tests.rs"]
mod tests;
