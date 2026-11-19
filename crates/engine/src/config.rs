//! Config integration — resolves `.cascade` config for file filtering.
//!
//! The engine uses `cascade-config` to resolve directory-walk configs,
//! then applies ignore rules when processing backend changes.

use cascade_config::ResolvedConfig;
use cascade_expr::context::EvalContext;
use cascade_expr::eval;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// Manages resolved configs for the VFS tree.
///
/// Configs are resolved lazily and cached. The mount root is used as
/// the starting point for the directory walk.
pub struct ConfigResolver {
    mount_root: PathBuf,
    cache: RwLock<Vec<(PathBuf, ResolvedConfig)>>,
}

impl std::fmt::Debug for ConfigResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigResolver")
            .field("mount_root", &self.mount_root)
            .finish_non_exhaustive()
    }
}

impl ConfigResolver {
    #[must_use]
    pub const fn new(mount_root: PathBuf) -> Self {
        Self {
            mount_root,
            cache: RwLock::new(Vec::new()),
        }
    }

    /// Check if a file at the given path should be ignored based on the
    /// resolved `.cascade` config for its parent directory.
    ///
    /// If an `EvalContext` is provided, conditional rules (those with `conditions`
    /// in the `.cascade` file) are evaluated against it. Rules with conditions
    /// that evaluate to false are skipped.
    pub fn is_ignored(&self, file_path: &Path, is_dir: bool) -> bool {
        self.is_ignored_with_context(file_path, is_dir, None)
    }

    /// Check if a file should be ignored, evaluating conditional rules against
    /// the provided context. The file is matched by its path relative to the
    /// mount root, through [`ResolvedConfig::is_ignored`].
    pub fn is_ignored_with_context(
        &self,
        file_path: &Path,
        is_dir: bool,
        ctx: Option<&EvalContext>,
    ) -> bool {
        let parent = file_path.parent().unwrap_or_else(|| Path::new("/"));
        let mut config = self.resolve_for_dir(parent);
        if let Some(ctx) = ctx {
            config
                .ignores
                .retain(|rule| rule.conditions.iter().all(|cond| condition_met(cond, ctx)));
        }
        let relative = file_path
            .strip_prefix(&self.mount_root)
            .unwrap_or(file_path);
        config.is_ignored(&relative.to_string_lossy(), is_dir)
    }

    /// Resolve the config for a directory, using the cache if available.
    fn resolve_for_dir(&self, dir: &Path) -> ResolvedConfig {
        // Check cache first.
        {
            let cache = self
                .cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((_, config)) = cache.iter().find(|(p, _)| p == dir) {
                return config.clone();
            }
        }

        // Resolve from the cascade-config crate.
        let config = cascade_config::merge::resolve(&self.mount_root, dir);

        // Cache the result.
        {
            let mut cache = self
                .cache
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cache.push((dir.to_path_buf(), config.clone()));
        }

        config
    }

    /// Invalidate cached configs for a directory and its children.
    /// Call this when `.cascade` files are created or modified.
    pub fn invalidate(&self, dir: &Path) {
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.retain(|(p, _)| !p.starts_with(dir));
    }
}

/// Whether the conditional expression `cond` holds in `ctx`; one that does
/// not parse is logged and does not hold.
fn condition_met(cond: &str, ctx: &EvalContext) -> bool {
    match eval::parse_expr(cond) {
        Ok(expr) => eval::evaluate(&expr, ctx),
        Err(e) => {
            tracing::warn!(expr = %cond, error = %e, "failed to parse condition");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_resolver_caches_results() {
        let resolver = ConfigResolver::new(PathBuf::from("/tmp/test-mount"));
        // Both calls should succeed (even if there's no actual .cascade file).
        let path = Path::new("/tmp/test-mount/Documents/notes.txt");
        let _ = resolver.is_ignored(path, false);
        let _ = resolver.is_ignored(path, false);

        let cache = resolver
            .cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn config_resolver_invalidate() {
        let resolver = ConfigResolver::new(PathBuf::from("/tmp/test-mount"));
        let path = Path::new("/tmp/test-mount/Documents/notes.txt");
        let _ = resolver.is_ignored(path, false);

        resolver.invalidate(Path::new("/tmp/test-mount/Documents"));

        let cache = resolver
            .cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(cache.is_empty());
    }
}
