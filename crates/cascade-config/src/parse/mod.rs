//! Parsers for the `.cascade` formats.

pub mod gitignore;
pub mod json;
pub mod toml;
pub mod yaml;

use std::path::{Path, PathBuf};

use anyhow::Context as _;

use crate::types::CascadeConfig;

/// Trait for loading config from a directory.
/// Production implementations walk the filesystem; tests use in-memory maps.
pub trait ConfigLoader {
    fn load(&self, dir: &std::path::Path) -> Option<CascadeConfig>;
}

/// The shared TOML configs of a directory, of which at most one may exist.
const SHARED_CONFIGS: [&str; 2] = [".cascade.toml", "cascade.toml"];
/// The local TOML configs of a directory, of which at most one may exist.
const LOCAL_CONFIGS: [&str; 2] = [".cascade.local.toml", "cascade.local.toml"];
/// Structured configs that are no longer read.
const RETIRED_CONFIGS: [&str; 2] = [".cascade.yaml", ".cascade.json"];

/// Parse the config files of a directory, merging in order: the
/// gitignore-style `.cascade`, the shared TOML config, the local TOML config.
///
/// A TOML config that fails to parse is skipped. A directory holding both
/// shared or both local configs, or a YAML or JSON config, has none of its
/// TOML configs applied, and a warning names the files.
#[must_use]
pub fn load_dir(dir: &Path) -> Option<CascadeConfig> {
    let mut layers: Vec<CascadeConfig> = read_dot_cascade(dir).into_iter().collect();
    match toml_configs(dir) {
        Ok(files) => layers.extend(files.iter().filter_map(|path| read_toml(path).ok())),
        Err(conflict) => tracing::warn!(
            dir = %dir.display(),
            "{conflict}; no TOML config of this directory is applied"
        ),
    }
    merged(layers)
}

/// Parse the config files of a directory like [`load_dir`], failing on any
/// TOML config that cannot be read or parsed and on the file combinations
/// [`load_dir`] skips.
///
/// # Errors
///
/// Fails naming the file and the offending key or value, or naming the
/// conflicting files.
pub fn load_dir_strict(dir: &Path) -> anyhow::Result<Option<CascadeConfig>> {
    let files = toml_configs(dir).map_err(anyhow::Error::msg)?;
    let mut layers: Vec<CascadeConfig> = read_dot_cascade(dir).into_iter().collect();
    for path in files {
        layers.push(read_toml(&path)?);
    }
    Ok(merged(layers))
}

/// The TOML configs of `dir` in the order they apply, or the reason none
/// of them applies.
fn toml_configs(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let present = |names: &[&str]| -> Vec<PathBuf> {
        names
            .iter()
            .map(|name| dir.join(name))
            .filter(|path| path.exists())
            .collect()
    };
    let shared = present(&SHARED_CONFIGS);
    let local = present(&LOCAL_CONFIGS);

    let mut conflicts: Vec<String> = present(&RETIRED_CONFIGS)
        .iter()
        .map(|path| format!("{} is no longer read; use cascade.toml", path.display()))
        .collect();
    for (kind, files) in [("shared", &shared), ("local", &local)] {
        if let [first, second] = files.as_slice() {
            conflicts.push(format!(
                "{} and {} are both {kind} configs; keep one",
                first.display(),
                second.display()
            ));
        }
    }
    if conflicts.is_empty() {
        Ok(shared.into_iter().chain(local).collect())
    } else {
        Err(conflicts.join("; "))
    }
}

fn read_dot_cascade(dir: &Path) -> Option<CascadeConfig> {
    std::fs::read_to_string(dir.join(".cascade"))
        .ok()
        .map(|content| gitignore::parse(&content))
}

fn read_toml(path: &Path) -> anyhow::Result<CascadeConfig> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    toml::parse(&content).with_context(|| format!("invalid config {}", path.display()))
}

fn merged(layers: Vec<CascadeConfig>) -> Option<CascadeConfig> {
    layers.into_iter().reduce(|mut config, layer| {
        config.merge(layer);
        config
    })
}
