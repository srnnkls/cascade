use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use cascade_engine::engine::{EngineConfig, NativeEngine};
use cascade_engine::presenter::VfsPresenter;

use super::backends::cli_backend_factory;
use super::helpers::{ensure_directory, write_pid_file};
use super::{
    CliContext, PresenterResources, ResolvedP2pConfig, WebOptions, mark_web_ready, start_web,
    stop_web, wait_for_shutdown_signal,
};

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
/// Unix-socket server (`~/.config/cascade/fileprovider.sock`) that the
/// Swift `CascadeFileProvider` extension connects to per request, and runs
/// the sync runner so the state database and the engine's change feed stay
/// populated. The extension surfaces the tree in Finder once its File
/// Provider domain is registered — a step performed by the Swift host app
/// (`NSFileProviderManager`), not by this daemon. See
/// `docs/fileprovider-smoke-test.md` for the end-to-end bring-up.
///
/// `no_mount` is accepted for signature parity with the other strategies
/// but has no effect: there is no OS-level mount command to skip, because
/// the extension owns the mount.
pub(super) async fn try_fileprovider(
    ctx: &CliContext,
    mount_path: &Path,
    backends: Vec<cascade_engine::backend::MountedBackend>,
    _no_mount: bool,
    p2p: &ResolvedP2pConfig,
    web: &WebOptions,
) -> Result<()> {
    use cascade_presenter_fileprovider::engine_handlers::EngineHandlers;
    use cascade_presenter_fileprovider::server::FileProviderServer;

    let engine_config = EngineConfig {
        db_path: ctx.db_path.clone(),
        mount_point: mount_path.to_path_buf(),
        backends,
        cache_dir: None,
        enable_p2p: p2p.enable_p2p,
        p2p_data_dir: None,
        p2p_posture: p2p.posture,
        p2p_relay_endpoints: p2p.relay_endpoints.clone(),
        p2p_relay_shared_secret: p2p.relay_shared_secret,
        backend_factory: Some(cli_backend_factory()),
    };
    let engine = Arc::new(NativeEngine::new(engine_config)?);
    // Wire the management-plane dispatcher into every backend that serves
    // remote management (the P2P backend). Done before the presenter starts so
    // an inbound ManageRequest is authorised and executed rather than refused.
    engine.wire_manage_dispatch().await;

    // Clone the engine handle for the HTTP API before the engine is moved into
    // the presenter resources; the API server is started only once the winning
    // presenter is confirmed up, so a failed attempt never binds the port.
    let engine_for_web = engine.clone();

    // The File Provider RPC server answers inbound queries from the Swift
    // extension against the engine's live VFS, state DB, cache directory,
    // and change feed. The cache directory is a sibling of the state DB
    // under the config root.
    let cache_dir = ctx.config_dir.join("cache");
    ensure_directory(&cache_dir, "File Provider cache directory")?;
    let handlers = Arc::new(EngineHandlers::new(
        engine.vfs().clone(),
        engine.db().clone(),
        cache_dir,
        engine.change_feed(),
    ));

    let socket_path = cascade_presenter_fileprovider::bridge::default_socket_path()
        .context("resolve File Provider socket path")?;
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
