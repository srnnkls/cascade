use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use cascade_config::NfsConfig;
use cascade_engine::engine::NativeEngine;
use cascade_presenter_nfs::nfs::context::NfsContext;
#[cfg(target_os = "macos")]
use cascade_presenter_nfs::nfs::server::macos_mount_options;
use cascade_presenter_nfs::nfs::server::{NfsCacheMode, NfsServer, NfsServerConfig};

use super::helpers::{
    apply_root_pins, engine_config, ensure_directory, root_config, write_pid_file,
};
use super::{
    CliContext, PresenterResources, ResolvedP2pConfig, WebOptions, finish_shutdown, mark_web_ready,
    start_web, stop_web, wait_for_shutdown_signal,
};

/// Starts an NFS server for `nfs` on a loopback port and, unless
/// `no_mount`, mounts it at `mount_path` with the root `[nfs]` section
/// `options`; a failed mount stops the server.
pub(super) async fn start_nfs_volume(
    mount_path: &Path,
    no_mount: bool,
    nfs: Arc<NfsContext>,
    options: &NfsConfig,
) -> Result<NfsServer> {
    let server_config = NfsServerConfig {
        bind_addr: "127.0.0.1:0".parse()?,
        export_path: "/".to_owned(),
        cache_mode: NfsCacheMode::default(),
    };
    let server = NfsServer::start(server_config, nfs)
        .await
        .context("failed to start NFS server")?;
    let nfs_port = server.local_addr.port();
    tracing::info!(port = nfs_port, "NFS server started");
    if !no_mount && let Err(error) = mount_nfs(mount_path, nfs_port, options) {
        tracing::error!(error = %format!("{error:#}"), "NFS mount failed");
        if let Err(stop_error) = server.stop() {
            tracing::warn!(error = %stop_error, "NFS server stop returned an error");
        }
        return Err(error);
    }
    Ok(server)
}

/// Try to mount via the NFS presenter.
///
/// Works on all platforms (Linux/macOS).  Same shutdown guarantees as
/// the macOS presenter functions. On Windows the chain doesn't fall
/// back to NFS (no built-in NFS client we can talk to from cascade),
/// so this function would be unused there — silence the lint.
#[cfg_attr(target_os = "windows", allow(dead_code))]
pub(super) async fn try_nfs(
    ctx: &CliContext,
    mount_path: &Path,
    backends: Vec<cascade_engine::backend::MountedBackend>,
    no_mount: bool,
    p2p: &ResolvedP2pConfig,
    web: &WebOptions,
) -> Result<()> {
    let paths = ctx.app_paths()?;
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

    let presenter = Arc::new(
        cascade_presenter_nfs::NfsPresenter::with_vfs(mount_path, engine.vfs().clone())
            .with_db(engine.db().clone()),
    );
    let nfs_ctx = presenter.context().clone();

    let sync_runner = engine.create_sync_runner(presenter);
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

    let server = match start_nfs_volume(mount_path, no_mount, nfs_ctx, &root.nfs).await {
        Ok(server) => server,
        Err(e) => {
            resources.shutdown();
            return Err(e);
        }
    };
    let nfs_port = server.local_addr.port();

    if let Err(e) = write_pid_file(&ctx.pid_path) {
        resources.shutdown();
        return Err(e);
    }

    if no_mount {
        println!("Cascade started (NFS, no-mount).");
    } else {
        println!("Cascade started (NFS).");
        println!("  Mount point: {}", mount_path.display());
    }
    println!("  NFS port: {nfs_port}");
    println!("  PID: {}", std::process::id());

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

    let mount = (!no_mount).then(|| mount_path.to_path_buf());
    finish_shutdown(&ctx.pid_path, mount, async move {
        if let Err(e) = server.stop() {
            tracing::warn!(error = %e, "NFS server stop returned an error");
        }
        resources.shutdown();
    })
    .await
}

/// Mount the cascade NFS server at `mount_point` as the current user with
/// the root `[nfs]` section `nfs` (macOS).
///
/// # Errors
///
/// Fails when the mount point cannot be created or `mount_nfs` fails.
#[cfg(target_os = "macos")]
pub(super) fn mount_nfs(mount_point: &Path, port: u16, nfs: &NfsConfig) -> Result<()> {
    ensure_directory(mount_point, "NFS mount point")?;
    let mut cmd = macos_nfs_mount_command(mount_point, port, nfs);
    tracing::info!(mount = %mount_point.display(), port, "mounting NFS");
    let output = cmd.output().context("failed to invoke /sbin/mount_nfs")?;
    if !output.status.success() {
        anyhow::bail!(
            "mount_nfs -o {} failed: {}",
            macos_nfs_mount_options(port, nfs),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    tracing::info!(path = %mount_point.display(), "NFS mounted");
    Ok(())
}

/// The `mount_nfs` command for the cascade server on `port` with the root
/// `[nfs]` section `nfs`.
#[cfg(target_os = "macos")]
pub(super) fn macos_nfs_mount_command(
    mount_point: &Path,
    port: u16,
    nfs: &NfsConfig,
) -> std::process::Command {
    let mut cmd = std::process::Command::new("/sbin/mount_nfs");
    cmd.arg("-o")
        .arg(macos_nfs_mount_options(port, nfs))
        .arg("127.0.0.1:/")
        .arg(mount_point);
    cmd
}

/// The `-o` options: the fixed options the server depends on, then the
/// configured ones.
#[cfg(target_os = "macos")]
fn macos_nfs_mount_options(port: u16, nfs: &NfsConfig) -> String {
    let mut options = vec![
        macos_mount_options(port),
        (if nfs.interruptible { "intr" } else { "nointr" }).to_owned(),
    ];
    if let Some(timeout) = nfs.dead_timeout {
        options.push(format!("deadtimeout={}", timeout.as_secs()));
    }
    if let Some(cache) = nfs.attribute_cache {
        let (min, max) = (cache.min().as_secs(), cache.max().as_secs());
        options.push(format!(
            "acregmin={min},acregmax={max},acdirmin={min},acdirmax={max},\
             acrootdirmin={min},acrootdirmax={max}"
        ));
    }
    options.join(",")
}

/// Mount the cascade NFS server on Linux using `mount -t nfs`.
///
/// The cascade NFS server speaks `NFSv3`, so we always request v3 and
/// connect over TCP. `mount(8)` on Linux requires root for NFS mounts;
/// when we hit `EACCES` or a "must be root" / "permission denied" stderr
/// we surface a clear hint to retry with `sudo` rather than letting the
/// raw `mount` error reach the user.
#[cfg(target_os = "linux")]
pub(super) fn mount_nfs(mount_point: &Path, port: u16, _nfs: &NfsConfig) -> Result<()> {
    ensure_directory(mount_point, "NFS mount point")?;

    let mut cmd = linux_nfs_mount_command(mount_point, port);
    tracing::info!(
        mount = %mount_point.display(),
        port,
        "mounting NFS (Linux, v3 over TCP)"
    );

    let output = cmd
        .output()
        .context("failed to invoke /bin/mount (is it installed?)")?;

    if output.status.success() {
        tracing::info!(path = %mount_point.display(), "NFS mounted");
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if is_linux_permission_error(&stderr, output.status.code()) {
        anyhow::bail!(
            "mount: NFS mounts on Linux require root. Re-run with sudo (e.g. `sudo cascade start`) or grant CAP_SYS_ADMIN. Original error: {}",
            stderr.trim()
        );
    }
    anyhow::bail!("mount -t nfs failed: {}", stderr.trim());
}

/// Build the Linux `mount -t nfs` command for the cascade server.
///
/// Extracted from `mount_nfs` so tests can assert argument construction
/// without invoking the kernel (which would need root and a live server).
#[cfg(target_os = "linux")]
pub(super) fn linux_nfs_mount_command(mount_point: &Path, port: u16) -> std::process::Command {
    let options = format!("port={port},mountport={port},proto=tcp,vers=3");
    let mut cmd = std::process::Command::new("mount");
    cmd.arg("-t")
        .arg("nfs")
        .arg("-o")
        .arg(&options)
        .arg("127.0.0.1:/")
        .arg(mount_point);
    cmd
}

/// Detect whether a Linux `mount` invocation failed because we lacked
/// root. Linux `mount` writes "must be root", "permission denied", or
/// "only root" depending on distro and version; we check all three plus
/// the EACCES exit code (32 on util-linux ≥ 2.34).
#[cfg(target_os = "linux")]
pub(super) fn is_linux_permission_error(stderr: &str, exit_code: Option<i32>) -> bool {
    let lower = stderr.to_lowercase();
    if lower.contains("must be root")
        || lower.contains("permission denied")
        || lower.contains("only root")
        || lower.contains("operation not permitted")
    {
        return true;
    }
    // util-linux convention: exit code 32 means "mount failure" with
    // permission as the most common cause when running unprivileged.
    matches!(exit_code, Some(32))
}

/// NFS mount stub for platforms with neither macOS nor Linux support.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
#[allow(clippy::missing_const_for_fn, clippy::unused_self, dead_code)]
pub(super) fn mount_nfs(_mount_point: &Path, _port: u16, _nfs: &NfsConfig) -> Result<()> {
    anyhow::bail!("NFS mounting is not supported on this platform yet");
}
