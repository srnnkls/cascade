use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use cascade_engine::engine::{EngineConfig, NativeEngine};
use cascade_presenter_nfs::nfs::server::{NfsCacheMode, NfsServer, NfsServerConfig};

use super::backends::cli_backend_factory;
use super::helpers::{ensure_directory, is_mounted, write_pid_file};
use super::{
    CliContext, PresenterResources, ResolvedP2pConfig, WebOptions, finish_shutdown, mark_web_ready,
    start_web, stop_web, wait_for_shutdown_signal,
};

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

    let server_config = NfsServerConfig {
        bind_addr: "127.0.0.1:0".parse()?,
        export_path: "/".to_string(),
        cache_mode: NfsCacheMode::default(),
    };
    let server = match NfsServer::start(server_config, nfs_ctx).await {
        Ok(s) => s,
        Err(e) => {
            resources.shutdown();
            return Err(e).context("failed to start NFS server");
        }
    };

    let nfs_port = server.local_addr.port();
    tracing::info!(port = nfs_port, "NFS server started");

    if !no_mount && let Err(e) = mount_nfs(mount_path, nfs_port) {
        resources.shutdown();
        return Err(e);
    }

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

/// Mount NFS filesystem using the OS mount command (macOS).
///
/// On macOS 26+, `NFSv3` mounts fail with permission errors. `NFSv4` works
/// without admin escalation (matching FUSE-T's approach). We try v4 first,
/// then fall back to v3 with osascript escalation.
#[cfg(target_os = "macos")]
pub(super) fn mount_nfs(mount_point: &Path, port: u16) -> Result<()> {
    ensure_directory(mount_point, "NFS mount point")?;

    let nfs_spec = "127.0.0.1:/".to_string();
    tracing::info!(spec = %nfs_spec, port, mount = %mount_point.display(), "mounting NFS");

    // Try `NFSv4` first — works on macOS 26+ without admin privileges.
    let v4_output = std::process::Command::new("/sbin/mount_nfs")
        .arg("-o")
        .arg(format!("vers=4,resvport,port={port}"))
        .arg(&nfs_spec)
        .arg(mount_point)
        .output();

    match v4_output {
        Ok(output) if output.status.success() => {
            tracing::info!(path = %mount_point.display(), "`NFSv4` mounted");
            return Ok(());
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::debug!(
                path = %mount_point.display(),
                error = %stderr,
                "`NFSv4` mount failed, falling back to v3"
            );
        }
        Err(e) => {
            tracing::debug!(
                path = %mount_point.display(),
                error = %e,
                "mount_nfs command failed for v4, falling back to v3"
            );
        }
    }

    // Fall back to `NFSv3`.
    let output = std::process::Command::new("/sbin/mount_nfs")
        .arg("-o")
        .arg(format!("rw,resvport,port={port}"))
        .arg(&nfs_spec)
        .arg(mount_point)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if is_mounted(mount_point) {
            tracing::warn!(path = %mount_point.display(), "already mounted");
            return Ok(());
        }

        // Retry with administrator privileges on permission errors.
        if is_permission_error(&stderr) {
            tracing::info!(path = %mount_point.display(), "retrying mount with administrator privileges");
            let escalated = osascript_mount_command(mount_point, port).output()?;
            if escalated.status.success() {
                tracing::info!(path = %mount_point.display(), "NFS mounted (via administrator privileges)");
                return Ok(());
            }
            let esc_stderr = String::from_utf8_lossy(&escalated.stderr);
            anyhow::bail!("mount_nfs failed even with administrator privileges: {esc_stderr}");
        }

        anyhow::bail!("mount_nfs failed: {stderr}");
    }

    tracing::info!(path = %mount_point.display(), "`NFSv3` mounted");
    Ok(())
}

/// Check if a `mount_nfs` stderr indicates a permission error.
#[cfg(target_os = "macos")]
pub(super) fn is_permission_error(stderr: &str) -> bool {
    stderr.contains("Operation not permitted")
}

/// Build an osascript command that mounts NFS with administrator privileges.
#[cfg(target_os = "macos")]
pub(super) fn osascript_mount_command(mount_point: &Path, port: u16) -> std::process::Command {
    let mount_cmd = format!(
        "/sbin/mount_nfs -o rw,resvport,port={port} 127.0.0.1:/ {mp}",
        mp = mount_point.display()
    );
    let script = format!("do shell script \"{mount_cmd}\" with administrator privileges");
    let mut cmd = std::process::Command::new("osascript");
    cmd.arg("-e").arg(&script);
    cmd
}

/// Mount the cascade NFS server on Linux using `mount -t nfs`.
///
/// The cascade NFS server speaks `NFSv3`, so we always request v3 and
/// connect over TCP. `mount(8)` on Linux requires root for NFS mounts;
/// when we hit `EACCES` or a "must be root" / "permission denied" stderr
/// we surface a clear hint to retry with `sudo` rather than letting the
/// raw `mount` error reach the user.
#[cfg(target_os = "linux")]
pub(super) fn mount_nfs(mount_point: &Path, port: u16) -> Result<()> {
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
pub(super) fn mount_nfs(_mount_point: &Path, _port: u16) -> Result<()> {
    anyhow::bail!("NFS mounting is not supported on this platform yet");
}
