use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};

use crate::cli::init::{BackendConfig, CascadeConfig};

/// The daemon's [`BackendFactory`](cascade_engine::backend::BackendFactory)
/// implementation — the composition-edge adapter that lets the engine build a
/// backend at runtime (the management-plane `BackendAdd` path) without depending
/// on any concrete backend crate. It parses the supplied TOML fragment and
/// routes to the same per-type [`create_backend`] factories the daemon uses at
/// startup.
pub(super) struct CliBackendFactory;

impl cascade_engine::backend::BackendFactory for CliBackendFactory {
    fn create(
        &self,
        name: &str,
        backend_type: &str,
        config_toml: &str,
    ) -> Result<Arc<dyn cascade_engine::backend::Backend>> {
        let config: toml::Value = toml::from_str(config_toml)
            .with_context(|| format!("parsing config for backend `{name}`"))?;
        // The management-plane hot-reload path does not carry the daemon's
        // shared HTTP client; pass None so the gdrive backend builds its own
        // default pooled client (still on the daemon's runtime, so no stranding).
        let backend = create_backend(name, backend_type, &config, None)?;
        Ok(Arc::from(backend))
    }
}

/// A shared [`CliBackendFactory`] for injection into an [`EngineConfig`](cascade_engine::engine::EngineConfig).
pub(super) fn cli_backend_factory() -> Arc<dyn cascade_engine::backend::BackendFactory> {
    Arc::new(CliBackendFactory)
}

/// Instantiate a backend by type, using its per-backend TOML config.
///
/// When `shared_http` is `Some`, the gdrive backend uses the daemon's shared
/// pooled client; when `None` (the management-plane hot-reload path), it builds
/// its own default pooled client. All other backend types ignore it.
pub(super) fn create_backend(
    name: &str,
    backend_type: &str,
    config: &toml::Value,
    shared_http: Option<Arc<dyn cascade_engine::portable::HttpClient>>,
) -> Result<Box<dyn cascade_engine::backend::Backend>> {
    match backend_type {
        "gdrive" => {
            let store = Arc::new(cascade_backend_gdrive::token_store::PlatformTokenStore);
            match shared_http {
                Some(http) => {
                    cascade_backend_gdrive::create_backend_with_store_and_http(config, store, http)
                }
                None => cascade_backend_gdrive::create_backend_with_store(config, store),
            }
            .with_context(|| format!("failed to create gdrive backend `{name}`"))
        }
        "s3" => cascade_backend_s3::create_backend(config)
            .with_context(|| format!("failed to create s3 backend `{name}`")),
        "local" => cascade_backend_local::create_backend(config)
            .with_context(|| format!("failed to create local backend `{name}`")),
        "p2p" => cascade_backend_p2p::create_backend(config)
            .with_context(|| format!("failed to create p2p backend `{name}`")),
        other => anyhow::bail!("unsupported backend type: {other}"),
    }
}

/// Rebuild backends from the main config in deterministic alphabetical order.
/// Fails loudly on an empty or duplicate mount path.
pub(in crate::cli) fn rebuild_backends(
    main_config: &CascadeConfig,
    config_dir: &Path,
    shared_http: Arc<dyn cascade_engine::portable::HttpClient>,
) -> Result<Vec<cascade_engine::backend::MountedBackend>> {
    let mut names: Vec<&String> = main_config.backends.keys().collect();
    names.sort_unstable();

    // Validate all mounts before creating any backends so config errors surface immediately.
    let mut resolved: Vec<(&String, BackendConfig, String)> = Vec::with_capacity(names.len());
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in &names {
        let cfg: BackendConfig = main_config
            .backends
            .get(name.as_str())
            .ok_or_else(|| anyhow::anyhow!("backend `{name}` disappeared during iteration"))?
            .clone()
            .try_into()
            .with_context(|| format!("invalid config for backend `{name}`"))?;
        let mount_str = cfg
            .mount
            .as_deref()
            .unwrap_or(name.as_str())
            .trim()
            .to_string();
        if mount_str.is_empty() {
            anyhow::bail!(
                "backend `{name}` has an empty mount path; remove the `mount` key or set a non-empty value"
            );
        }
        let dedup = if mount_str == "/" {
            String::new()
        } else {
            mount_str.clone()
        };
        if !seen.insert(dedup) {
            anyhow::bail!(
                "duplicate mount path for backend `{name}`: \"{mount_str}\" is already used"
            );
        }
        resolved.push((name, cfg, mount_str));
    }

    let mut mounted: Vec<cascade_engine::backend::MountedBackend> =
        Vec::with_capacity(resolved.len());
    for (name, cfg, mount_str) in resolved {
        let per_backend_config = load_backend_config(config_dir, name)?;
        let backend = create_backend(
            name,
            &cfg.backend_type,
            &per_backend_config,
            Some(shared_http.clone()),
        )?;
        mounted.push(cascade_engine::backend::MountedBackend::new(
            Some(mount_str),
            Arc::from(backend),
        ));
    }
    Ok(mounted)
}

/// Load the top-level `config.toml` from the config directory.
pub(in crate::cli) fn load_main_config(config_dir: &Path) -> Result<CascadeConfig> {
    let config_path = config_dir.join("config.toml");
    if config_path.exists() {
        let contents = std::fs::read_to_string(&config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?;
        let config: CascadeConfig = toml::from_str(&contents)
            .with_context(|| format!("failed to parse {}", config_path.display()))?;
        tracing::info!(path = ?config_path, "Loaded main config");
        Ok(config)
    } else {
        anyhow::bail!(
            "Config file not found at {}. Run `cascade init` to create it.",
            config_path.display()
        );
    }
}

/// Load a backend config from the config directory.
pub(super) fn load_backend_config(config_dir: &Path, name: &str) -> Result<toml::Value> {
    let config_path = config_dir.join(format!("{name}.toml"));
    if !config_path.exists() {
        anyhow::bail!(
            "Backend config not found: {}. Re-run `cascade init` or create it manually.",
            config_path.display()
        );
    }
    let contents = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let value: toml::Value = toml::from_str(&contents)
        .with_context(|| format!("failed to parse {}", config_path.display()))?;
    tracing::info!(path = ?config_path, "Loaded backend config");
    Ok(value)
}
