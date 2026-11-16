use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use cascade_engine::engine::NativeEngine;
#[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
use cascade_engine::presenter::VfsPresenter;
use cascade_presenter_webdav::WebDavPresenter;

#[cfg(test)]
use super::init::CascadeConfig;
use super::init::{P2pConfig, WebConfig};
use super::{CliContext, is_process_alive};

mod backends;
#[cfg(target_os = "macos")]
mod fileprovider;
mod helpers;
mod nfs;

pub(super) use backends::{load_main_config, rebuild_backends};
#[cfg(target_os = "macos")]
use fileprovider::try_fileprovider;
use helpers::{
    apply_root_pins, engine_config, ensure_directory, evict_stale_mount, mount_webdav,
    read_text_file, resolve_mount_path, resolve_mount_path_from_config, root_config, unmount_path,
    write_pid_file,
};
use nfs::try_nfs;
#[cfg(all(test, target_os = "linux"))]
use nfs::{is_linux_permission_error, linux_nfs_mount_command};
#[cfg(all(test, target_os = "macos"))]
use nfs::{is_permission_error, osascript_mount_command};

// ---------------------------------------------------------------------------
// HTTP API (PWA) wiring
// ---------------------------------------------------------------------------

/// Default HTTP API bind — loopback only.
const WEB_DEFAULT_BIND: &str = "127.0.0.1:7842";
/// Default HTTP API request timeout in seconds (one hour).
const WEB_DEFAULT_TIMEOUT_SECS: u64 = 3600;
/// Default HTTP API maximum request body size in bytes (1 GiB).
const WEB_DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024 * 1024;

/// The HTTP API flags supplied on the `cascade start` command line.
#[derive(Debug, Clone, Default)]
pub struct WebFlags {
    /// `--web`: serve the HTTP API (overrides `[web].enabled`).
    pub enable: bool,
    /// `--web-bind`: bind address override.
    pub bind: Option<String>,
    /// `--web-bundle-url`: advertised PWA bundle URL.
    pub bundle_url: Option<String>,
    /// `--web-cors-origin`: additional CORS origins (repeatable).
    pub cors_origins: Vec<String>,
}

/// The resolved HTTP API configuration, merging `config.toml` `[web]` with the
/// CLI flags. Always present so the `web` parameter is used in both feature
/// builds; the actual server is feature-gated.
///
/// All fields but `enabled` are read only by the `web`-feature server; without
/// the feature the struct still carries them (so `resolve_web_options` and the
/// CLI surface are identical across builds), hence the feature-conditional
/// dead-code allowance.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "web"), allow(dead_code))]
struct WebOptions {
    enabled: bool,
    bind: String,
    bundle_url: Option<String>,
    cors_origins: Vec<String>,
    request_timeout_secs: u64,
    max_body_bytes: usize,
}

/// Merge the `[web]` config table with the CLI flags. A flag overrides the
/// config value; `--web` and `[web].enabled` are OR-ed; CORS origins from both
/// sources accumulate.
fn resolve_web_options(cfg: &WebConfig, flags: &WebFlags) -> WebOptions {
    let mut cors_origins = cfg.cors_origins.clone();
    cors_origins.extend(flags.cors_origins.iter().cloned());
    WebOptions {
        enabled: flags.enable || cfg.enabled,
        bind: flags
            .bind
            .clone()
            .or_else(|| cfg.bind.clone())
            .unwrap_or_else(|| WEB_DEFAULT_BIND.to_owned()),
        bundle_url: flags.bundle_url.clone().or_else(|| cfg.bundle_url.clone()),
        cors_origins,
        request_timeout_secs: cfg.request_timeout_secs.unwrap_or(WEB_DEFAULT_TIMEOUT_SECS),
        max_body_bytes: cfg.max_body_bytes.unwrap_or(WEB_DEFAULT_MAX_BODY_BYTES),
    }
}

/// A running HTTP API server plus the readiness handle the daemon flips once the
/// data plane is up.
#[cfg(feature = "web")]
struct WebRuntime {
    handle: cascade_web_api::RouterHandle,
    readiness: cascade_web_api::Readiness,
}

/// What [`start_web`] returns: a running server (or `None` when disabled) in the
/// `web` build, or a unit placeholder otherwise. The alias keeps the presenter
/// call sites identical across feature builds.
#[cfg(feature = "web")]
type WebRuntimeOpt = Option<WebRuntime>;
/// See [`WebRuntimeOpt`] — the placeholder form when the `web` feature is off.
#[cfg(not(feature = "web"))]
type WebRuntimeOpt = Option<()>;

/// Validate the resolved web options and build the bind configuration without
/// touching the network, so a misconfigured `[web]` fails fast at startup —
/// before any mount is attempted — rather than after a presenter has come up.
///
/// Catches an unparseable bind address, the `0.0.0.0`-without-`bundle_url`
/// footgun, and a wildcard CORS origin (refused by `BindConfig::new`).
#[cfg(feature = "web")]
fn validated_web_config(
    web: &WebOptions,
) -> Result<(std::net::SocketAddr, cascade_web_api::state::BindConfig)> {
    let bind: std::net::SocketAddr = web
        .bind
        .parse()
        .with_context(|| format!("[web] bind `{}` is not a valid socket address", web.bind))?;
    if bind.ip().is_unspecified() && web.bundle_url.is_none() {
        anyhow::bail!(
            "[web] bind {bind} exposes the API on all interfaces but no bundle_url is set; \
             refusing to start. A bearer-auth API on a public interface without TLS leaks \
             credentials — put a TLS-terminating reverse proxy in front and set bundle_url."
        );
    }
    let bind_config = cascade_web_api::state::BindConfig::new(
        bind,
        web.bundle_url.clone(),
        web.cors_origins.clone(),
        web.request_timeout_secs,
        web.max_body_bytes,
        env!("CARGO_PKG_VERSION").to_owned(),
        option_env!("CASCADE_BUILD_SHA").map(ToOwned::to_owned),
    )
    .context("invalid [web] configuration")?;
    Ok((bind, bind_config))
}

/// Build the HTTP API state from a live engine and spawn the server, when the
/// `web` feature is compiled in and the operator enabled it.
#[cfg(feature = "web")]
async fn start_web(engine: &Arc<NativeEngine>, web: &WebOptions) -> Result<WebRuntimeOpt> {
    use cascade_web_api::state::{AppState, NodeIdentity, Readiness};

    if !web.enabled {
        return Ok(None);
    }

    let identity = engine.device_identity().cloned().context(
        "the HTTP API requires a device identity; configure a P2P backend or omit --web",
    )?;
    let (bind, bind_config) = validated_web_config(web)?;
    if bind.ip().is_unspecified() {
        tracing::warn!(
            %bind,
            "serving the HTTP API on a public interface — ensure a TLS-terminating reverse \
             proxy fronts it; bearer tokens must never cross the wire in clear"
        );
    }

    let readiness = Readiness::new(chrono::Utc::now());
    let state = AppState::new(
        engine.clone(),
        NodeIdentity::new(identity),
        bind_config,
        readiness.clone(),
    );
    let handle = cascade_web_api::serve(state)
        .await
        .context("could not bind the HTTP API socket")?;
    tracing::info!(addr = %handle.local_addr(), "HTTP API serving");
    println!("  HTTP API: http://{}", handle.local_addr());
    Ok(Some(WebRuntime { handle, readiness }))
}

/// Stub form when the `web` feature is off: refuse loudly if the operator asked
/// for the API, otherwise do nothing.
#[cfg(not(feature = "web"))]
#[allow(clippy::unused_async)]
async fn start_web(_engine: &Arc<NativeEngine>, web: &WebOptions) -> Result<WebRuntimeOpt> {
    if web.enabled {
        anyhow::bail!(
            "the HTTP API was requested (--web or [web].enabled) but this binary was built \
             without the `web` feature; rebuild with `cargo build --features web`"
        );
    }
    Ok(None)
}

/// Flip the data-plane readiness bit once the presenter is up, so the F3-gated
/// data routes begin serving. A no-op without the `web` feature.
#[cfg(feature = "web")]
fn mark_web_ready(runtime: &WebRuntimeOpt) {
    if let Some(runtime) = runtime {
        runtime.readiness.set_data_plane_ready(true);
    }
}

/// See [`mark_web_ready`] — the no-op form when the `web` feature is off.
#[cfg(not(feature = "web"))]
#[allow(clippy::trivially_copy_pass_by_ref)] // must match the #[cfg(web)] signature
const fn mark_web_ready(_runtime: &WebRuntimeOpt) {}

/// Gracefully stop the HTTP API server on daemon shutdown, draining in-flight
/// requests. A no-op without the `web` feature.
#[cfg(feature = "web")]
async fn stop_web(runtime: WebRuntimeOpt) {
    if let Some(runtime) = runtime {
        runtime.handle.stop().await;
    }
}

/// See [`stop_web`] — the no-op form when the `web` feature is off.
#[cfg(not(feature = "web"))]
#[allow(clippy::unused_async)]
async fn stop_web(_runtime: WebRuntimeOpt) {}

/// Resolved optimisation-layer P2P configuration threaded into every
/// `EngineConfig` at daemon startup.
///
/// Bundles the three new `EngineConfig` fields so they can be passed
/// to each `try_*` presenter constructor without repeating the same three
/// arguments on every call.
#[derive(Debug, Clone)]
struct ResolvedP2pConfig {
    enable_p2p: bool,
    posture: Option<cascade_p2p::DiscoveryReach>,
    relay_endpoints: Vec<std::net::SocketAddr>,
    relay_shared_secret: Option<[u8; 32]>,
}

/// Parse the `[p2p]` table from `config.toml` into the fields that the engine's
/// P2P bridge needs.
///
/// Fails loudly on a malformed `posture` value (typos should surface at startup,
/// not silently default to a less-exposed posture the operator did not intend)
/// and on a malformed relay secret (wrong length or non-hex characters).
fn resolve_p2p_bridge_config(cfg: &P2pConfig) -> Result<ResolvedP2pConfig> {
    let posture = cfg.posture.as_deref().map(parse_posture_str).transpose()?;

    let relay_endpoints = cfg
        .relay_endpoint
        .as_deref()
        .map(|ep| {
            // Accept a DNS hostname or a Docker service name, not only a literal
            // IP:port — resolved here at config load so compose service names
            // like `relay:9999` work. The relay is dialled by the resolved
            // address; restart the daemon if the relay's address later changes.
            use std::net::ToSocketAddrs as _;
            ep.to_socket_addrs()
                .with_context(|| {
                    format!("[p2p] relay_endpoint `{ep}` is not a resolvable host:port")
                })?
                .next()
                .with_context(|| format!("[p2p] relay_endpoint `{ep}` resolved to no addresses"))
        })
        .transpose()?
        .map(|addr| vec![addr])
        .unwrap_or_default();

    let relay_shared_secret = cfg
        .relay_shared_secret
        .as_deref()
        .map(parse_relay_secret_hex)
        .transpose()?;

    Ok(ResolvedP2pConfig {
        enable_p2p: cfg.enabled,
        posture,
        relay_endpoints,
        relay_shared_secret,
    })
}

/// Parse a posture string from the config file into a `DiscoveryReach`.
///
/// Fails loudly rather than silently defaulting: an operator who typed `publik`
/// deserves a clear error rather than discovering the node quietly confined to
/// the LAN when they meant WAN, or the reverse.
fn parse_posture_str(s: &str) -> Result<cascade_p2p::DiscoveryReach> {
    match s {
        "lan-only" => Ok(cascade_p2p::DiscoveryReach::LanOnly),
        "private" => Ok(cascade_p2p::DiscoveryReach::Private),
        "public" => Ok(cascade_p2p::DiscoveryReach::Public),
        other => anyhow::bail!(
            "[p2p] posture `{other}` is not valid; expected one of: lan-only, private, public"
        ),
    }
}

/// Parse a 64-character hex relay shared secret into a 32-byte array.
///
/// The relay HMAC key is exactly 32 bytes (64 hex chars). Catching a
/// malformed value at startup is better than a confusing runtime 401.
fn parse_relay_secret_hex(hex: &str) -> Result<[u8; 32]> {
    if hex.len() != 64 {
        anyhow::bail!(
            "[p2p] relay_shared_secret must be exactly 64 hex characters (32 bytes), got {}",
            hex.len()
        );
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let pair_start = i
            .checked_mul(2)
            .ok_or_else(|| anyhow::anyhow!("relay_shared_secret index overflow"))?;
        let pair_end = pair_start
            .checked_add(2)
            .ok_or_else(|| anyhow::anyhow!("relay_shared_secret index overflow"))?;
        let pair = hex.get(pair_start..pair_end).ok_or_else(|| {
            anyhow::anyhow!("relay_shared_secret hex slice out of range at position {pair_start}")
        })?;
        *byte = u8::from_str_radix(pair, 16)
            .with_context(|| format!("invalid hex pair `{pair}` in relay_shared_secret"))?;
    }
    Ok(out)
}

/// Park until the process is asked to shut down.
///
/// On Unix this resolves on `SIGINT` (Ctrl+C) **or** `SIGTERM`. `SIGTERM` is
/// what `cascade stop`, launchd, and systemd send to stop the daemon; catching
/// it lets the caller run its graceful unmount-and-cleanup path rather than
/// being terminated abruptly with the mount left stale. On non-Unix platforms
/// `SIGTERM` has no equivalent, so only Ctrl+C is awaited.
#[cfg(unix)]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = interrupt.recv() => {}
        _ = terminate.recv() => {}
    }
    Ok(())
}

/// Park until the process is asked to shut down (Ctrl+C on non-Unix platforms,
/// which have no `SIGTERM` equivalent).
#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

/// Resources held by a running presenter attempt.
///
/// When a mount strategy fails, [`PresenterResources::shutdown`] must be
/// called to stop every component that was started during the attempt
/// (sync task, presenter, engine background tasks).  This prevents leaked
/// tasks from an earlier attempt surviving into the next fallback.
struct PresenterResources {
    /// The engine instance (dropped last so cancel signals reach tasks). Held as
    /// an `Arc` so the management-plane dispatcher wired into the P2P backend at
    /// startup ([`NativeEngine::wire_manage_dispatch`]) can share ownership.
    engine: Arc<NativeEngine>,
    /// Join handle for the cache-manager background task.
    engine_handle: cascade_engine::engine::EngineHandle,
    /// Join handle for the sync runner task.
    sync_handle: tokio::task::JoinHandle<()>,
}

impl PresenterResources {
    /// Shut down all components in reverse start order: abort the sync runner,
    /// then signal the engine to cancel, which quiesces the cache-manager task.
    fn shutdown(self) {
        self.sync_handle.abort();
        self.engine.shutdown();
        // Abort the cache-manager task immediately rather than waiting up to a
        // full sweep interval for it to observe the cancel flag `shutdown` set.
        self.engine_handle.abort();
        drop(self.engine_handle);
        // `engine` is dropped here, releasing the VFS tree and cancel token.
    }
}

/// Upper bound on the daemon's own unmount-and-teardown after a stop signal,
/// before it forces an exit.
///
/// Sits comfortably inside the 5 s grace `cascade stop` allows after `SIGTERM`,
/// so in a wedged shutdown the daemon self-terminates before the external
/// `SIGKILL` escalation fires, while still guaranteeing it never hangs — for
/// example on a macOS `webdavfs` unmount that stalls because the in-process
/// `WebDAV` server it is talking to is itself tearing down.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

/// Run the post-signal shutdown sequence, bounded by [`SHUTDOWN_GRACE`].
///
/// The OS unmount is a blocking subprocess (`diskutil`/`umount`/`net use`), so
/// it runs on a blocking thread rather than a runtime worker: unmounting a
/// self-hosted mount makes the kernel client issue its final requests to the
/// in-process presenter server, which must stay responsive to answer them, and
/// occupying a worker with the blocking call can wedge that exchange.
/// `teardown` then stops the presenter and engine. If the whole sequence does
/// not finish within the grace period the daemon removes its PID file and
/// forces an exit rather than hanging with an unkillable process and a stale
/// mount — which the next `cascade start` reclaims, and which `cascade stop`
/// also clears once the process is gone.
async fn finish_shutdown(
    pid_path: &Path,
    mount_path: Option<PathBuf>,
    teardown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    let sequence = async move {
        if let Some(mount_path) = mount_path {
            let _ = tokio::task::spawn_blocking(move || unmount_path(&mount_path)).await;
        }
        teardown.await;
    };

    if tokio::time::timeout(SHUTDOWN_GRACE, sequence)
        .await
        .is_err()
    {
        tracing::warn!(
            grace_secs = SHUTDOWN_GRACE.as_secs(),
            "graceful shutdown timed out; forcing exit"
        );
        let _ = std::fs::remove_file(pid_path);
        std::process::exit(0);
    }

    let _ = std::fs::remove_file(pid_path);
    tracing::info!("Cascade stopped.");
    Ok(())
}

/// Start the Cascade daemon.
///
/// `p2p_override` lets the CLI flag (`--p2p` / `--no-p2p`) override the
/// `[p2p].enabled` value from `config.toml`. `None` falls through to
/// whatever the config file says. `app_group` serves File Provider protocol
/// v2 and the NFS volume from one engine, as `--app-group` selects, and
/// implies `file_provider`.
pub async fn start(
    ctx: &CliContext,
    mount_point: Option<&str>,
    no_mount: bool,
    p2p_override: Option<bool>,
    file_provider: bool,
    app_group: bool,
    web_flags: WebFlags,
) -> Result<()> {
    tracing::info!("Starting Cascade daemon");
    let file_provider = file_provider || app_group;

    ensure_directory(&ctx.config_dir, "config directory")?;

    // Bail early if the daemon is already running.
    if ctx.pid_path.exists() {
        let raw = read_text_file(&ctx.pid_path, "PID file")?;
        if let Ok(pid) = raw.trim().parse::<u32>() {
            if is_process_alive(pid) {
                anyhow::bail!("Cascade is already running (PID {pid}). Run `cascade stop` first.");
            }
            // Stale PID file — clean it up and continue.
            let _ = std::fs::remove_file(&ctx.pid_path);
        }
    }

    // Read main config.toml written by `cascade init`.
    let main_config = load_main_config(&ctx.config_dir)?;

    // Resolve P2P config; misconfigured posture or relay values surface here before any mount attempt.
    let mut p2p_config = resolve_p2p_bridge_config(&main_config.p2p)?;

    // --p2p / --no-p2p overrides the [p2p].enabled value from config.toml.
    if let Some(override_val) = p2p_override {
        p2p_config.enable_p2p = override_val;
    }

    if p2p_config.enable_p2p {
        tracing::info!("P2P optimisation layer enabled");
    }

    // Resolve the HTTP API config from the [web] table and the CLI flags. The
    // server is spawned inside whichever presenter strategy succeeds.
    let web_options = resolve_web_options(&main_config.web, &web_flags);
    if web_options.enabled {
        tracing::info!("HTTP API (PWA) enabled");
        // Fail fast on a misconfigured API before mounting anything.
        #[cfg(feature = "web")]
        validated_web_config(&web_options).context("[web] configuration is invalid")?;
        #[cfg(not(feature = "web"))]
        anyhow::bail!(
            "the HTTP API was requested (--web or [web].enabled) but this binary was built \
             without the `web` feature; rebuild with `cargo build --features web`"
        );
    }

    // Create backends from config.
    //
    // A daemon with nothing to mount has no work to do, so it exits cleanly
    // rather than bailing with an error. This matters when `cascade start` is
    // run by an OS background service (`cascade service`): a non-zero exit
    // would make launchd / systemd treat the start as a crash and restart it
    // in a tight loop. Exiting 0 lets the service stay idle until a backend is
    // added and the service is restarted. The advice to run `cascade init` is
    // surfaced for the interactive operator who started the daemon by hand.
    if main_config.backends.is_empty() {
        tracing::info!("No backends configured; nothing to mount.");
        println!("No backends configured. Run `cascade init` to set one up, then start again.");
        return Ok(());
    }

    // Build the single long-lived pooled HTTP client on the daemon's stable
    // main runtime (the Drive TLS deadlock fix). All Drive backends share it.
    let shared_http: Arc<dyn cascade_engine::portable::HttpClient> =
        Arc::new(cascade_engine::portable::native::ReqwestClient::new());

    // Resolve mount point: CLI arg > config.toml [mount].point > ~/Cloud.
    let mount_path = if let Some(p) = mount_point {
        resolve_mount_path(p)
    } else {
        let configured = main_config.mount.point.trim().to_string();
        if configured.is_empty() {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join("Cloud")
        } else {
            resolve_mount_path(&configured)
        }
    };

    // Clean up any stale mount left behind by a previous run that
    // exited without unmounting (e.g. kill -9, tmux kill, panic).
    #[cfg(target_os = "macos")]
    evict_stale_mount(&mount_path);

    ensure_directory(&mount_path, "mount point")?;

    // Override knob for containers and tests: CASCADE_PRESENTER=webdav
    // forces the WebDAV presenter on any platform. Useful for Linux
    // containers where /dev/fuse may be unavailable and NFS needs root.
    if std::env::var("CASCADE_PRESENTER").as_deref() == Ok("webdav") {
        let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
        tracing::info!(
            strategy = "webdav-forced",
            "honouring CASCADE_PRESENTER=webdav"
        );
        return try_webdav(
            ctx,
            &mount_path,
            backends,
            no_mount,
            &p2p_config,
            &web_options,
        )
        .await;
    }

    // `--file-provider` is an explicit opt-in to the macOS File Provider
    // bridge. It is an alternative to FSKit, not a fallback in the FSKit
    // chain — both are macOS-native and self-mounting, so trying both would
    // be meaningless. When requested, it is the only strategy attempted.
    #[cfg(not(target_os = "macos"))]
    if file_provider {
        anyhow::bail!("--file-provider is only supported on macOS");
    }

    #[cfg(target_os = "macos")]
    if file_provider {
        let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
        tracing::info!(strategy = "fileprovider", "attempting File Provider mount");
        return try_fileprovider(
            ctx,
            &mount_path,
            backends,
            no_mount,
            app_group,
            &p2p_config,
            &web_options,
        )
        .await;
    }

    // On macOS: FSKit first (native, kext-free, POSIX), then WebDAV (no
    // root needed), then NFSv4/v3 fallback.  NFS already has the v4 → v3 →
    // escalation chain inside mount_nfs().
    //
    // FSKit self-mounts via the Swift extension and cannot be used without
    // mounting, so it is skipped when --no-mount is set.
    #[cfg(target_os = "macos")]
    {
        // Attempt 1: FSKit (skipped if --no-mount since FSKit always self-mounts).
        if !no_mount {
            let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
            tracing::info!(strategy = "fskit", "attempting FSKit mount");
            match try_fskit(ctx, &mount_path, backends, &p2p_config, &web_options).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(error = %e, "FSKit mount failed, falling back to WebDAV");
                    drop(e);
                }
            }
        }

        // Attempt 2: WebDAV.
        let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
        tracing::info!(strategy = "webdav", "attempting WebDAV mount");
        match try_webdav(
            ctx,
            &mount_path,
            backends,
            no_mount,
            &p2p_config,
            &web_options,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "WebDAV mount failed, falling back to NFS");
                drop(e);
            }
        }

        // Attempt 3: NFS (v4 → v3 escalation inside mount_nfs).
        let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
        try_nfs(
            ctx,
            &mount_path,
            backends,
            no_mount,
            &p2p_config,
            &web_options,
        )
        .await
    }

    #[cfg(target_os = "linux")]
    {
        // When --no-mount is set, skip FUSE entirely (it always mounts and
        // the entrypoint's seed mode relies on this gate to stay unprivileged).
        // Fall straight through to WebDAV, which honours no_mount and runs
        // without /dev/fuse or SYS_ADMIN. Otherwise (the operator wants a real
        // mount, and CASCADE_PRESENTER=webdav was not forced above) try FUSE
        // first, then NFS.
        if no_mount {
            // Headless seed mode used by the Docker entrypoint: run the WebDAV
            // server without mounting.
            let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
            tracing::info!(
                strategy = "webdav-seed",
                "--no-mount set on Linux; starting WebDAV server in seed mode"
            );
            try_webdav(
                ctx,
                &mount_path,
                backends,
                no_mount,
                &p2p_config,
                &web_options,
            )
            .await
        } else {
            let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
            tracing::info!(strategy = "fuse", "attempting FUSE mount");
            match try_fuse(ctx, &mount_path, backends, &p2p_config, &web_options).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(error = %e, "FUSE mount failed, falling back to NFS");
                    drop(e);
                }
            }

            let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
            try_nfs(
                ctx,
                &mount_path,
                backends,
                no_mount,
                &p2p_config,
                &web_options,
            )
            .await
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Attempt 1: ProjFS (native, no admin required).
        //
        // The presenter implements the full callback table and serves
        // on-demand reads through the engine-backed content provider.
        // ProjFS self-mounts via the Win32 ProjectedFileSystem API and
        // cannot be used without mounting, so it is skipped when
        // --no-mount is set.
        if !no_mount {
            let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
            tracing::info!(strategy = "projfs", "attempting ProjFS mount");
            match try_projfs(ctx, &mount_path, backends, &p2p_config, &web_options).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(error = %e, "ProjFS mount failed, falling back to WebDAV");
                    drop(e);
                }
            }
        }

        // Attempt 2: WebDAV via the built-in WebClient service.
        let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
        tracing::info!(strategy = "webdav", "attempting WebDAV mount");
        try_webdav(
            ctx,
            &mount_path,
            backends,
            no_mount,
            &p2p_config,
            &web_options,
        )
        .await
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let backends = rebuild_backends(&main_config, &ctx.config_dir, shared_http.clone())?;
        try_nfs(
            ctx,
            &mount_path,
            backends,
            no_mount,
            &p2p_config,
            &web_options,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// macOS presenters
// ---------------------------------------------------------------------------

/// Try to mount via the `FSKit` presenter.
///
/// On success, blocks until Ctrl+C and then shuts down.  On failure at any
/// stage (engine start, presenter start, mount command) every resource that
/// was started is stopped before the error is returned, so the caller can
/// safely attempt the next fallback.
#[cfg(target_os = "macos")]
async fn try_fskit(
    ctx: &CliContext,
    mount_path: &Path,
    backends: Vec<cascade_engine::backend::MountedBackend>,
    p2p: &ResolvedP2pConfig,
    web: &WebOptions,
) -> Result<()> {
    let paths = ctx.app_paths()?;
    let root = root_config(mount_path);

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
        cascade_presenter_fskit::FSKitPresenter::from_default_socket()?
            .with_mount_point(mount_path),
    );

    let sync_runner = engine.create_sync_runner(presenter.clone());
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

    if let Err(e) = presenter.start(mount_path).await {
        resources.shutdown();
        return Err(e).with_context(|| {
            format!(
                "failed to start FSKit presenter at {}",
                mount_path.display()
            )
        });
    }

    // FSKit presenter.start() triggers the mount internally via the Swift
    // extension; no separate OS mount command is needed.

    if let Err(e) = write_pid_file(&ctx.pid_path) {
        resources.shutdown();
        return Err(e);
    }

    println!("Cascade started (FSKit).");
    println!("  Mount point: {}", mount_path.display());
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

    finish_shutdown(&ctx.pid_path, Some(mount_path.to_path_buf()), async move {
        if let Err(e) = presenter.stop().await {
            tracing::warn!(error = %e, "FSKit presenter stop returned an error");
        }
        resources.shutdown();
    })
    .await
}

/// Try to mount via the `ProjFS` presenter (Windows only).
///
/// `ProjFS` (Projected File System) is the native Windows equivalent of
/// `FSKit` on macOS and FUSE on Linux. The presenter self-mounts via
/// `PrjStartVirtualizing` — there is no separate OS mount command — so
/// this function does not need a `no_mount` parameter and is skipped at
/// the dispatch level when `--no-mount` is set.
///
/// Same shutdown guarantees as the other presenter functions: on any
/// failure during engine, presenter, or mount setup, every started
/// resource is stopped before the error is returned.
#[cfg(target_os = "windows")]
async fn try_projfs(
    ctx: &CliContext,
    mount_path: &Path,
    backends: Vec<cascade_engine::backend::MountedBackend>,
    p2p: &ResolvedP2pConfig,
    web: &WebOptions,
) -> Result<()> {
    // Extract the raw backend Arc list for the content provider before the
    // MountedBackend list is consumed by EngineConfig. Cloning is cheap —
    // just Arc reference-count bumps.
    let backends_for_provider: Vec<Arc<dyn cascade_engine::backend::Backend>> =
        backends.iter().map(|mb| mb.backend.clone()).collect();

    let paths = ctx.app_paths()?;

    let root = root_config(mount_path);

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

    // Capture the daemon's runtime handle while still on it. The ProjFS
    // GetFileData callback runs on a kernel thread outside any runtime; the
    // provider uses this handle to `block_on` the cold-path download.
    //
    // The daemon's runtime is built explicitly with
    // `tokio::runtime::Runtime::new()` in `main.rs` and owned by `main`'s
    // stack for the whole life of `block_on(args.run(..))`, so it provably
    // outlives every ProjFS callback: the daemon does not return from this
    // function (it parks on `ctrl_c` below) until shutdown, by which point
    // the presenter has been stopped and no further callbacks can fire.
    //
    // `Runtime::new()` builds a multi-thread runtime. That flavour matters:
    // the provider's cold-path bridge falls back to `block_in_place` when it
    // detects it is already on a runtime worker, and `block_in_place` is only
    // valid on the multi-thread scheduler. A current-thread runtime would
    // make that fallback panic, so assert the flavour here where the daemon
    // wiring is in view rather than discovering it at a callback.
    let handle = tokio::runtime::Handle::current();
    debug_assert_eq!(
        handle.runtime_flavor(),
        tokio::runtime::RuntimeFlavor::MultiThread,
        "ProjFS provider requires the daemon's multi-thread runtime; \
         see crates/cascade/src/main.rs (Runtime::new)"
    );

    // The content provider serves on-demand reads. Each read fetches only
    // the requested byte range straight from the owning backend, so there
    // is no whole-file cache directory to provision.
    let provider = Arc::new(super::projfs_provider::EngineContentProvider::new(
        engine.vfs().clone(),
        engine.db().clone(),
        backends_for_provider,
        handle,
    ));

    // Do not set `with_root`: the sync runner labels each top-level item
    // with its backend's own root id (e.g. `gdrive:root`), not a single
    // shared root. Pinning one root id would filter out every backend but
    // one. Leaving the root unset uses the presenter's loose-root
    // fallback, treating every item in the map as eligible — which is
    // exactly what the multi-backend daemon populates.
    let presenter = Arc::new(
        cascade_presenter_projfs::ProjFsPresenter::new(mount_path).with_content_provider(provider),
    );

    let sync_runner = engine.create_sync_runner(presenter.clone());
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

    if let Err(e) = presenter.start(mount_path).await {
        resources.shutdown();
        return Err(e).with_context(|| {
            format!(
                "failed to start ProjFS presenter at {}",
                mount_path.display()
            )
        });
    }

    // ProjFS self-mounts via PrjStartVirtualizing; no separate OS mount
    // command is needed.

    if let Err(e) = write_pid_file(&ctx.pid_path) {
        resources.shutdown();
        return Err(e);
    }

    println!("Cascade started (ProjFS).");
    println!("  Mount point: {}", mount_path.display());
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

    finish_shutdown(&ctx.pid_path, None, async move {
        if let Err(e) = presenter.stop().await {
            tracing::warn!(error = %e, "ProjFS presenter stop returned an error");
        }
        resources.shutdown();
    })
    .await
}

/// Try to mount via the `WebDAV` presenter.
///
/// Same shutdown guarantees as the FSKit/FUSE presenter functions: on
/// any failure during engine, presenter, or mount setup, every started
/// resource is stopped before the error is returned. The `WebDAV`
/// server runs on every platform; only the OS-level mount command
/// varies. macOS uses `mount_webdav`, Windows uses `net use` against
/// the built-in `WebClient` service, and Linux falls through to
/// FUSE/NFS rather than using `mount.davfs` (which requires root).
async fn try_webdav(
    ctx: &CliContext,
    mount_path: &Path,
    backends: Vec<cascade_engine::backend::MountedBackend>,
    no_mount: bool,
    p2p: &ResolvedP2pConfig,
    web: &WebOptions,
) -> Result<()> {
    let paths = ctx.app_paths()?;
    let root = root_config(mount_path);

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

    let mut presenter = WebDavPresenter::new(mount_path);
    if let Ok(bind) = std::env::var("CASCADE_WEBDAV_BIND") {
        presenter = presenter.with_bind_addr(bind);
    }

    // Pass the mount table (the engine's `VfsTree` children, longest-prefix
    // first) and the child backends so `WebDavServer` routes every method by
    // VFS mount path, not the first path segment. See `with_mounts`.
    let mounts = {
        let vfs = engine
            .vfs()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        vfs.children().to_vec()
    };
    let all_backends = mounts.iter().map(|(_, b)| b.clone()).collect();
    presenter.with_backends(all_backends).await;
    presenter.with_mounts(mounts).await;
    presenter.with_db(engine.db().clone());

    let presenter = Arc::new(presenter);
    let items = presenter.items().clone();

    let sync_runner = engine.create_sync_runner(presenter.clone());
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

    if let Err(e) = presenter.start(mount_path).await {
        resources.shutdown();
        return Err(e).with_context(|| {
            format!(
                "failed to start WebDAV presenter at {}",
                mount_path.display()
            )
        });
    }

    let webdav_port = {
        let guard = presenter.server.lock().await;
        guard
            .as_ref()
            .map(cascade_presenter_webdav::server::WebDavServer::port)
            .context("WebDAV server not started")?
    };

    if no_mount {
        println!("Cascade started (WebDAV, no-mount).");
        println!("  WebDAV URL: http://localhost:{webdav_port}");
    } else {
        if let Err(e) = mount_webdav(mount_path, webdav_port) {
            resources.shutdown();
            return Err(e);
        }
        println!("Cascade started (WebDAV).");
        println!("  Mount point: {}", mount_path.display());
        println!("  WebDAV port: {webdav_port}");
    }
    println!("  PID: {}", std::process::id());

    if let Err(e) = write_pid_file(&ctx.pid_path) {
        resources.shutdown();
        return Err(e);
    }

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
        if let Err(e) = presenter.stop().await {
            tracing::warn!(error = %e, "WebDAV presenter stop returned an error");
        }
        drop(items);
        resources.shutdown();
    })
    .await
}

/// Try to mount via the FUSE presenter (Linux only).
///
/// Same shutdown guarantees as the macOS presenter functions: on any
/// failure during engine, presenter, or mount setup, every started
/// resource is stopped before the error is returned.
///
/// This function is only called when `--no-mount` is not set. The `start()`
/// caller gates the FUSE attempt behind `!no_mount` so that seed mode (the
/// Docker container default) is a genuine unprivileged path: FUSE always mounts
/// and cannot honour `--no-mount` internally, so the gate lives at the call
/// site rather than inside this function.
#[cfg(target_os = "linux")]
async fn try_fuse(
    ctx: &CliContext,
    mount_path: &Path,
    backends: Vec<cascade_engine::backend::MountedBackend>,
    p2p: &ResolvedP2pConfig,
    web: &WebOptions,
) -> Result<()> {
    let paths = ctx.app_paths()?;
    let root = root_config(mount_path);

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

    let root_id = cascade_engine::vfs::neutral_root_item_id();
    let presenter = Arc::new(
        cascade_presenter_fuse::FusePresenter::with_vfs(root_id, engine.vfs().clone())
            .with_mount_point(mount_path)
            .with_db(engine.db().clone()),
    );

    let sync_runner = engine.create_sync_runner(presenter.clone());
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

    if let Err(e) = presenter.start(mount_path).await {
        resources.shutdown();
        return Err(e).with_context(|| {
            format!("failed to start FUSE presenter at {}", mount_path.display())
        });
    }

    if let Err(e) = write_pid_file(&ctx.pid_path) {
        resources.shutdown();
        return Err(e);
    }

    println!("Cascade started (FUSE).");
    println!("  Mount point: {}", mount_path.display());
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

    finish_shutdown(&ctx.pid_path, None, async move {
        if let Err(e) = presenter.stop().await {
            tracing::warn!(error = %e, "FUSE presenter stop returned an error");
        }
        resources.shutdown();
    })
    .await
}

// ---------------------------------------------------------------------------
// Stop
// ---------------------------------------------------------------------------

/// Poll until the process with `pid` is gone or `grace` elapses.
///
/// Returns `true` if the process exited within the grace period. The number of
/// polls is derived from `grace / poll` so the timing is expressed once, in the
/// caller's units, rather than as a magic iteration count.
#[cfg(unix)]
fn wait_for_pid_exit(pid: u32, grace: std::time::Duration, poll: std::time::Duration) -> bool {
    let polls = grace.as_millis().div_ceil(poll.as_millis().max(1));
    for _ in 0..polls {
        if !is_process_alive(pid) {
            return true;
        }
        std::thread::sleep(poll);
    }
    !is_process_alive(pid)
}

/// Stop the Cascade daemon.
#[cfg(unix)]
pub fn stop(ctx: &CliContext) -> anyhow::Result<()> {
    if !ctx.pid_path.exists() {
        println!("Cascade is not running.");
        return Ok(());
    }

    let raw = read_text_file(&ctx.pid_path, "PID file")?;
    let pid_u32: u32 = raw.trim().parse().with_context(|| {
        format!(
            "invalid PID in {}: {:?}",
            ctx.pid_path.display(),
            raw.trim()
        )
    })?;
    let pid_signed =
        i32::try_from(pid_u32).with_context(|| format!("PID {pid_u32} overflows i32"))?;
    let pid = nix::unistd::Pid::from_raw(pid_signed);

    match nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM) {
        Ok(()) => {}
        Err(nix::errno::Errno::ESRCH) => {
            // Process no longer exists — clean up stale PID file.
            let _ = std::fs::remove_file(&ctx.pid_path);
            println!("Cascade is not running.");
            return Ok(());
        }
        Err(e) => {
            return Err(anyhow::anyhow!(
                "failed to send SIGTERM to PID {pid_u32}: {e}"
            ));
        }
    }

    // Wait for the daemon to exit after SIGTERM. It runs a bounded graceful
    // shutdown of its own (see `SHUTDOWN_GRACE`), so it normally exits well
    // inside this window.
    let poll = std::time::Duration::from_millis(500);
    let term_grace = std::time::Duration::from_secs(5);
    let mut exited = wait_for_pid_exit(pid_u32, term_grace, poll);

    if !exited {
        // The SIGTERM grace elapsed — escalate to SIGKILL so `stop` actually
        // stops the daemon rather than reporting success and leaving an orphan
        // (the Windows path force-kills here too). SIGKILL cannot be caught, so
        // all that remains is to wait for the kernel to reap the process.
        eprintln!(
            "Warning: PID {pid_u32} did not exit within {}s of SIGTERM; sending SIGKILL.",
            term_grace.as_secs()
        );
        match nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(e) => {
                return Err(anyhow::anyhow!("failed to SIGKILL PID {pid_u32}: {e}"));
            }
        }
        let kill_grace = std::time::Duration::from_secs(2);
        exited = wait_for_pid_exit(pid_u32, kill_grace, poll);
        if !exited {
            eprintln!("Warning: PID {pid_u32} still present after SIGKILL.");
        }
    }

    let _ = std::fs::remove_file(&ctx.pid_path);

    // Clean up a stale mount only now the daemon is gone, so this never races
    // the daemon's own unmount. A graceful exit has already unmounted; this
    // covers the SIGKILL case, where it could not.
    let mount_path = resolve_mount_path_from_config(&ctx.config_dir);
    if mount_path.is_dir()
        && let Err(e) = unmount_path(&mount_path)
    {
        tracing::debug!(error = %e, "unmount after stop failed (may already be unmounted)");
    }

    println!("Cascade stopped.");
    Ok(())
}

/// Stop the Cascade daemon (Windows).
///
/// Reads the PID file, sends `taskkill /PID`, polls up to 5 s, then
/// force-kills with `taskkill /F`. Removes the PID file and any Cascade
/// `WebDAV` mounts. A stale PID file (process already gone) is success.
#[cfg(windows)]
pub fn stop(ctx: &CliContext) -> anyhow::Result<()> {
    if !ctx.pid_path.exists() {
        println!("Cascade is not running.");
        return Ok(());
    }

    let raw = read_text_file(&ctx.pid_path, "PID file")?;
    let pid: u32 = raw.trim().parse().with_context(|| {
        format!(
            "invalid PID in {}: {:?}",
            ctx.pid_path.display(),
            raw.trim()
        )
    })?;

    if !is_process_alive(pid) {
        // Process already gone — clean up the stale PID file and exit.
        let _ = std::fs::remove_file(&ctx.pid_path);
        println!("Cascade is not running.");
        return Ok(());
    }

    // Graceful first: `taskkill` without `/F` posts WM_CLOSE which Cascade
    // can react to like a Ctrl+C. Ignore non-zero exit — the process may
    // have died between our liveness check and this call, or the OS may
    // report "not found".
    let _ = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string()])
        .output();

    // Poll for up to 5 seconds for the process to exit (10 × 500 ms).
    let mut exited = false;
    for _ in 0..10 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        if !is_process_alive(pid) {
            exited = true;
            break;
        }
    }

    if !exited {
        // Force-kill: `taskkill /F` issues `TerminateProcess`. This wins
        // unless the PID is somehow unkillable. "Process not found"
        // stderr is treated as success because the goal — process gone —
        // is already met.
        let output = std::process::Command::new("taskkill")
            .args(["/F", "/PID", &pid.to_string()])
            .output()
            .with_context(|| format!("failed to force-kill PID {pid} via taskkill"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let combined = format!("{stdout}{stderr}");
            if !is_windows_process_not_found(&combined) {
                return Err(anyhow::anyhow!(
                    "taskkill /F /PID {pid} failed: {}",
                    combined.trim()
                ));
            }
            tracing::debug!(
                pid,
                "taskkill reported process not found; treating as already exited"
            );
        }
    }

    let _ = std::fs::remove_file(&ctx.pid_path);

    // Detach any localhost WebDAV mounts that look like ours.
    let mount_path = resolve_mount_path_from_config(&ctx.config_dir);
    if let Err(e) = unmount_path(&mount_path) {
        tracing::debug!(error = %e, "unmount after stop failed (may already be unmounted)");
    }

    println!("Cascade stopped.");
    Ok(())
}

/// Detect whether `taskkill` output indicates the target PID no longer
/// exists. `taskkill` writes localised messages but the English token
/// "not found" (with various surrounding text) is consistent enough to
/// match against, and matches the format documented by Microsoft.
#[cfg(windows)]
fn is_windows_process_not_found(output: &str) -> bool {
    let lower = output.to_lowercase();
    lower.contains("not found")
        || lower.contains("no running instance")
        || lower.contains("no tasks running")
}

/// Stop the Cascade daemon on platforms that genuinely lack an implementation.
#[cfg(not(any(unix, windows)))]
pub fn stop(_ctx: &CliContext) -> anyhow::Result<()> {
    anyhow::bail!("cascade stop is not supported on this platform yet");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
#[path = "mount_tests.rs"]
mod tests;
