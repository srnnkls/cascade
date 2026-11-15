use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};

/// File name of the File Provider RPC socket.
const SOCKET_FILE_NAME: &str = "fp.sock";

/// Longest socket path `bind(2)` accepts on macOS: `sun_path` is `char[104]`
/// and must hold the terminating NUL.
const MAX_SOCKET_PATH_BYTES: usize = 103;

/// Filesystem locations of one daemon instance.
///
/// Configuration stays in the `--config` directory. State, cache, staging
/// and the socket live in the App Group container when `--app-group` is
/// given, so the File Provider extension can reach them, and in the config
/// directory otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPaths {
    pub config_dir: PathBuf,
    pub db_path: PathBuf,
    pub pid_path: PathBuf,
    pub cache_dir: PathBuf,
    pub staging_dir: PathBuf,
    pub socket_path: PathBuf,
}

impl AppPaths {
    /// Resolve paths from the raw `--config` value (may contain `~`, may name
    /// a config file) and the optional `--app-group` container.
    pub fn resolve(config_flag: &str, app_group: Option<&Path>) -> Result<Self> {
        let config_path = PathBuf::from(shellexpand::tilde(config_flag).as_ref());
        let config_dir = if config_path.is_file() {
            config_path
                .parent()
                .context("--config path has no parent directory")?
                .to_path_buf()
        } else {
            config_path
        };
        let state_dir = app_group.map_or_else(|| config_dir.clone(), Path::to_path_buf);
        Ok(Self::in_dirs(config_dir, &state_dir))
    }

    /// Lay out the paths for a resolved config directory and the directory
    /// holding state, cache, staging and the socket.
    pub fn in_dirs(config_dir: PathBuf, state_dir: &Path) -> Self {
        Self {
            db_path: state_dir.join("state.db"),
            pid_path: state_dir.join("cascade.pid"),
            cache_dir: state_dir.join("cache"),
            staging_dir: state_dir.join("staging"),
            socket_path: state_dir.join(SOCKET_FILE_NAME),
            config_dir,
        }
    }

    /// The File Provider socket path, rejected when `bind(2)` cannot accept it.
    pub fn bindable_socket_path(&self) -> Result<&Path> {
        if self.socket_path.as_os_str().as_encoded_bytes().len() > MAX_SOCKET_PATH_BYTES {
            bail!(
                "socket path {} exceeds {MAX_SOCKET_PATH_BYTES} bytes; use a shorter --app-group or --config directory",
                self.socket_path.display()
            );
        }
        Ok(&self.socket_path)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::AppPaths;

    const SUN_PATH_BYTES: usize = 104;

    #[test]
    fn app_group_places_socket_staging_and_cache_in_the_container() {
        let group = Path::new(
            "/Users/firstname.lastname-long/Library/Group Containers/ABCDE12345.group.io.cascade.shared",
        );

        let paths = AppPaths::resolve("/nonexistent/cascade-config", Some(group)).unwrap();

        assert_eq!(paths.socket_path, group.join("fp.sock"));
        assert!(paths.socket_path.as_os_str().len() <= SUN_PATH_BYTES);
        assert_eq!(paths.staging_dir, group.join("staging"));
        assert!(paths.cache_dir.starts_with(group));
        assert!(paths.db_path.starts_with(group));
    }

    #[test]
    fn app_group_whose_socket_exceeds_sun_path_is_rejected() {
        let group: PathBuf = Path::new("/Users/someone/Library/Group Containers")
            .join(format!("ABCDE12345.group.{}", "a".repeat(SUN_PATH_BYTES)));

        let paths = AppPaths::resolve("/nonexistent/cascade-config", Some(&group)).unwrap();

        assert!(paths.bindable_socket_path().is_err());
    }

    #[test]
    fn config_dir_without_app_group_keeps_every_path_under_it() {
        let config = Path::new("/nonexistent/cascade-config");

        let paths = AppPaths::resolve("/nonexistent/cascade-config", None).unwrap();

        assert_eq!(paths.config_dir, config);
        assert_eq!(paths.db_path, config.join("state.db"));
        assert_eq!(paths.cache_dir, config.join("cache"));
        assert!(paths.staging_dir.starts_with(config));
        assert_eq!(paths.socket_path.parent(), Some(config));
    }
}
