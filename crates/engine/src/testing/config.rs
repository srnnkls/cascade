//! `.cascade` rules as a daemon reads them from its mount-point directory.

use cascade_config::ResolvedConfig;
use cascade_config::merge::resolve_from_configs;
use cascade_config::parse::{gitignore, toml};

#[must_use]
pub fn mount_point_config(gitignore_text: &str) -> ResolvedConfig {
    resolve_from_configs(vec![gitignore::parse(gitignore_text)])
}

/// The rules of a `.cascade.toml` in the mount-point directory.
///
/// # Errors
///
/// Fails when `toml_text` is not a valid `.cascade.toml`.
pub fn mount_point_toml_config(toml_text: &str) -> anyhow::Result<ResolvedConfig> {
    Ok(resolve_from_configs(vec![toml::parse(toml_text)?]))
}
