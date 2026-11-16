use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};

use cascade_config::ResolvedConfig;
use cascade_engine::engine::NativeEngine;

use super::backends::load_main_config;

/// The resolved `.cascade` config of the mount root.
pub(super) fn root_config(mount_path: &Path) -> ResolvedConfig {
    cascade_config::merge::resolve(mount_path, mount_path)
}

/// Applies the pin rules of the root `.cascade` config to the engine's
/// content cache; a platform without a content cache has none to apply.
pub(super) async fn apply_root_pins(engine: &NativeEngine, root: &ResolvedConfig) -> Result<()> {
    #[cfg(unix)]
    engine
        .apply_pin_rules(root)
        .await
        .context("apply the pin rules of the root .cascade config")?;
    #[cfg(not(unix))]
    let _ = (engine, root);
    Ok(())
}

/// Resolve a mount point path, expanding ~ and environment variables.
pub(super) fn resolve_mount_path(path: &str) -> PathBuf {
    let expanded = shellexpand::tilde(path).to_string();
    PathBuf::from(expanded)
}

/// Resolve the mount path from config.toml, falling back to ~/Cloud.
#[cfg_attr(not(any(unix, windows)), allow(dead_code))]
pub(super) fn resolve_mount_path_from_config(config_dir: &Path) -> PathBuf {
    let main_config = load_main_config(config_dir).ok();
    let configured = main_config.as_ref().and_then(|c| {
        let p = c.mount.point.trim().to_string();
        (!p.is_empty()).then_some(p)
    });
    configured.map_or_else(
        || {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join("Cloud")
        },
        |p| resolve_mount_path(&p),
    )
}

/// Ensure a path exists as a directory, with a message that names the path.
///
/// On macOS a stale `WebDAV` mount can leave the path in a state where
/// `stat` and other syscalls hang against the dead server. We attempt
/// `create_dir_all` first and only fall back to `stat`-style checks
/// after confirming the path is not a live mount entry — that way a
/// broken mount produces a clear actionable error instead of hanging.
pub(super) fn ensure_directory(path: &Path, label: &str) -> Result<()> {
    match std::fs::create_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // The path exists. If it's still a live mount entry the
            // eviction earlier in startup didn't take, and any `stat`
            // call would hang against the dead server. Surface that
            // explicitly with recovery instructions.
            #[cfg(target_os = "macos")]
            if path_is_in_mount_table(path) {
                anyhow::bail!(
                    "{label} {} is still listed as an active mount — please run `sudo umount -f {}` and try again",
                    path.display(),
                    path.display()
                );
            }
            // Safe to `stat` now: not a mount, so the syscall returns
            // immediately. Reject the case where the path is a regular
            // file rather than a directory.
            if path.is_dir() {
                Ok(())
            } else {
                anyhow::bail!("{label} {} exists but is not a directory", path.display());
            }
        }
        Err(e) => Err(e).with_context(|| format!("failed to create {label} at {}", path.display())),
    }
}

/// Return true if `path` appears in the `mount` table output. Used to
/// distinguish "EEXIST because of a stale mount we couldn't evict" from
/// "EEXIST because the directory already exists as expected".
#[cfg(target_os = "macos")]
pub(super) fn path_is_in_mount_table(path: &Path) -> bool {
    let path_str = path.to_string_lossy();
    let Ok(output) = std::process::Command::new("/sbin/mount").output() else {
        return false;
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.lines().any(|line| line.contains(&*path_str))
}

/// Read a text file with the file path included in the error message.
pub(super) fn read_text_file(path: &Path, label: &str) -> Result<String> {
    std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {label} at {}", path.display()))
}

/// Write the current process ID to the PID file with a path-aware error.
pub(super) fn write_pid_file(path: &Path) -> Result<()> {
    std::fs::write(path, std::process::id().to_string())
        .with_context(|| format!("failed to write PID file at {}", path.display()))
}

// ---------------------------------------------------------------------------
// macOS mount helpers
// ---------------------------------------------------------------------------

/// Run a subprocess with a hard timeout. If the child does not exit
/// within `timeout`, it is killed and `None` is returned. Used to keep
/// daemon startup snappy even when unmount commands stall on a dead
/// `WebDAV` server (which can otherwise hang for minutes).
#[cfg(target_os = "macos")]
pub(super) fn run_with_timeout(
    cmd: &mut std::process::Command,
    timeout: std::time::Duration,
) -> Option<std::process::Output> {
    use std::process::Stdio;

    let Ok(mut child) = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn() else {
        return None;
    };

    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_status)) => {
                return child.wait_with_output().ok();
            }
            Ok(None) => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(_) => return None,
        }
    }
}

/// Remove stale `WebDAV` or NFS mounts on the given path left behind by a
/// previous Cascade run that exited without clean shutdown.
///
/// Detects mounts whose remote endpoint is `http://127.0.0.1:` (`WebDAV`) or
/// `127.0.0.1:/` (NFS) — Cascade-only patterns — so it never touches mounts
/// belonging to other programs.
///
/// Attempts, in order: `umount -f`, then `diskutil unmount force`. Each
/// command runs with a hard timeout because dead `WebDAV` mounts can make
/// kernel syscalls (including the unmount path itself) hang indefinitely
/// while the VFS waits for a server that will never respond.
#[cfg(target_os = "macos")]
pub(super) fn evict_stale_mount(mount_point: &Path) {
    let path_str = mount_point.to_string_lossy();
    let cmd_timeout = std::time::Duration::from_secs(5);

    let output = match std::process::Command::new("/sbin/mount").output() {
        Ok(o) => String::from_utf8_lossy(&o.stdout).to_string(),
        Err(_) => return,
    };

    // Collect lines that mention our mount point *and* look like a Cascade
    // WebDAV or NFS mount (localhost endpoint).
    let stale: Vec<&str> = output
        .lines()
        .filter(|line| line.contains(&*path_str))
        .filter(|line| {
            line.contains("http://127.0.0.1:")
                || line.contains("http://localhost:")
                || line.contains("127.0.0.1:/")
        })
        .collect();

    if stale.is_empty() {
        return;
    }

    for entry in &stale {
        tracing::warn!(path = %path_str, entry, "evicting stale mount");
    }

    // `umount -f` (force) skips the "flush dirty buffers" path, which is
    // what hangs on a dead WebDAV server. Loop because stacked mounts on
    // the same path need one call per layer.
    for _ in 0..=stale.len() {
        let mut cmd = std::process::Command::new("/sbin/umount");
        cmd.arg("-f").arg(mount_point);
        match run_with_timeout(&mut cmd, cmd_timeout) {
            Some(o) if o.status.success() => {
                tracing::info!(path = %path_str, "stale mount evicted via umount -f");
            }
            Some(_) => break, // umount returned non-zero: nothing more to remove
            None => {
                tracing::warn!(path = %path_str, "umount -f timed out; falling back to diskutil");
                break;
            }
        }
    }

    // Verify by re-reading the mount table — if anything matching is left
    // (stacked or stuck) try the heavier `diskutil unmount force`.
    if path_is_in_mount_table(mount_point) {
        let mut cmd = std::process::Command::new("/usr/sbin/diskutil");
        cmd.args(["unmount", "force"]).arg(mount_point);
        match run_with_timeout(&mut cmd, cmd_timeout) {
            Some(o) if o.status.success() => {
                tracing::info!(path = %path_str, "stale mount force-evicted via diskutil");
            }
            Some(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                tracing::warn!(path = %path_str, stderr = %stderr, "diskutil force unmount failed");
            }
            None => {
                tracing::warn!(path = %path_str, "diskutil unmount force timed out");
            }
        }
    }
}

/// Mount `WebDAV` filesystem using the OS mount command (macOS).
/// Uses `/sbin/mount_webdav` which does not require root.
#[cfg(target_os = "macos")]
pub(super) fn mount_webdav(mount_point: &Path, port: u16) -> Result<()> {
    ensure_directory(mount_point, "WebDAV mount point")?;

    let url = format!("http://localhost:{port}/");
    tracing::info!(url = %url, mount = %mount_point.display(), "mounting WebDAV");

    let output = std::process::Command::new("/sbin/mount_webdav")
        .arg(&url)
        .arg(mount_point)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if is_mounted(mount_point) {
            tracing::warn!(path = %mount_point.display(), "already mounted");
            return Ok(());
        }
        anyhow::bail!("mount_webdav failed: {stderr}");
    }

    tracing::info!(path = %mount_point.display(), "WebDAV mounted");
    Ok(())
}

/// Unmount a filesystem at the given path.
#[cfg(target_os = "macos")]
pub(super) fn unmount_path(mount_point: &Path) -> Result<()> {
    if !is_mounted(mount_point) {
        tracing::debug!(path = %mount_point.display(), "not mounted, skipping unmount");
        return Ok(());
    }

    tracing::info!(path = %mount_point.display(), "unmounting");

    let output = std::process::Command::new("/usr/bin/diskutil")
        .arg("unmount")
        .arg(mount_point)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::warn!(error = %stderr, "unmount failed (will be cleaned up on exit)");
    }

    Ok(())
}

/// Unmount the NFS filesystem on Linux using `umount`.
///
/// Tries the plain `umount <path>` first; if the kernel reports the
/// device as busy we retry with `-l` (lazy detach) so the daemon can
/// always exit cleanly even when a shell still has the mount as `cwd`.
/// Permission and "not mounted" failures are logged at debug rather
/// than propagated — shutdown should not block on an already-detached
/// mount.
#[cfg(target_os = "linux")]
#[allow(clippy::unnecessary_wraps)]
// Returns `Result<()>` to share the signature with the macOS/Windows impls;
// shutdown is best-effort and never propagates errors on Linux.
pub(super) fn unmount_path(mount_point: &Path) -> Result<()> {
    tracing::info!(path = %mount_point.display(), "unmounting NFS");

    let output = std::process::Command::new("umount")
        .arg(mount_point)
        .output();

    match output {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            if stderr.contains("not mounted") || stderr.contains("not found") {
                tracing::debug!(path = %mount_point.display(), "not mounted, skipping unmount");
                return Ok(());
            }
            // Try a lazy unmount on busy/permission failures so the
            // daemon doesn't get stuck waiting for shells to leave.
            let lazy = std::process::Command::new("umount")
                .arg("-l")
                .arg(mount_point)
                .output();
            match lazy {
                Ok(l) if l.status.success() => {
                    tracing::info!(path = %mount_point.display(), "NFS lazily unmounted");
                    Ok(())
                }
                Ok(l) => {
                    let lazy_err = String::from_utf8_lossy(&l.stderr);
                    tracing::warn!(
                        error = %stderr.trim(),
                        lazy_error = %lazy_err.trim(),
                        "umount failed (will be cleaned up on exit)"
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(error = %e, "lazy umount failed to spawn");
                    Ok(())
                }
            }
        }
        Err(e) => {
            tracing::debug!(error = %e, "umount failed to spawn (may already be unmounted)");
            Ok(())
        }
    }
}

/// Unmount stub for platforms with neither macOS, Linux, nor Windows
/// implementations.
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
#[allow(clippy::unnecessary_wraps, clippy::missing_const_for_fn)]
pub(super) fn unmount_path(_mount_point: &Path) -> Result<()> {
    Ok(())
}

/// Check if a path is already mounted.
#[cfg(target_os = "macos")]
pub(super) fn is_mounted(path: &Path) -> bool {
    let output = std::process::Command::new("/sbin/mount").output();

    let mounts = match output {
        Ok(o) => String::from_utf8_lossy(&o.stdout).to_string(),
        Err(_) => return false,
    };
    mounts.contains(&*path.to_string_lossy())
}

/// Mount the `WebDAV` server as a network drive on Windows via the
/// built-in `WebClient` service (`net use`).
///
/// `mount_point` is used as a display string; the actual mount target
/// is whichever drive letter Windows assigns when `*` is passed. The
/// drive letter is reported in the log.
#[cfg(target_os = "windows")]
pub(super) fn mount_webdav(mount_point: &Path, port: u16) -> Result<()> {
    let url = format!("http://localhost:{port}/");
    tracing::info!(url = %url, "mounting WebDAV via `net use`");

    let output = std::process::Command::new("net")
        .args(["use", "*", &url])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        anyhow::bail!("net use failed (is the WebClient service running?): {stdout}{stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    tracing::info!(
        path = %mount_point.display(),
        output = %stdout.trim(),
        "WebDAV mounted on Windows"
    );
    Ok(())
}

/// Unmount the most recently assigned `WebDAV` drive on Windows.
///
/// `net use <url> /delete` removes by URL rather than by drive letter,
/// which matches how we created the mount with `*`.
#[cfg(target_os = "windows")]
pub(super) fn unmount_path(_mount_point: &Path) -> Result<()> {
    // Best-effort: enumerate all drives mapped to a localhost WebDAV URL
    // and detach each. The mount point passed here is the cascade-logical
    // path, not the assigned drive letter, so we can't address it directly.
    let output = std::process::Command::new("net").arg("use").output()?;
    if !output.status.success() {
        return Ok(());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.contains("http://localhost:") || line.contains("http://127.0.0.1:") {
            // Extract the URL (last whitespace-separated token).
            if let Some(url) = line.split_whitespace().last() {
                let _ = std::process::Command::new("net")
                    .args(["use", url, "/delete", "/y"])
                    .output();
            }
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
#[allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps, dead_code)]
pub(super) fn mount_webdav(_mount_point: &Path, _port: u16) -> Result<()> {
    anyhow::bail!("WebDAV mounting is not supported on this platform yet");
}

#[cfg(not(target_os = "macos"))]
#[allow(clippy::missing_const_for_fn, dead_code)]
pub(super) fn is_mounted(_path: &Path) -> bool {
    false
}
