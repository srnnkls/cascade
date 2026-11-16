//! A daemon's on-disk state and its start sequence, reopened by every daemon
//! run of a test.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;

use crate::backend::Backend;
use crate::content::cache::ContentCache;
use crate::content::replica::{Replica, ReplicaStore};
use crate::content::transfer::TransferConfig;
use crate::db::StateDb;

/// The state database file and the cache, staging and incoming directories
/// under one root.
#[derive(Debug, Clone)]
pub struct Disk {
    db_path: PathBuf,
    cache_dir: PathBuf,
    staging_dir: PathBuf,
    incoming: PathBuf,
    config: TransferConfig,
}

impl Disk {
    /// The state of a daemon under `root` opening its cache with `config`.
    ///
    /// # Errors
    ///
    /// Fails when the incoming directory cannot be created.
    pub fn at(root: &Path, config: TransferConfig) -> Result<Self> {
        let incoming = root.join("incoming");
        std::fs::create_dir_all(&incoming)?;
        Ok(Self {
            db_path: root.join("state.db"),
            cache_dir: root.join("cache"),
            staging_dir: root.join("staging"),
            incoming,
            config,
        })
    }

    /// Directory the cache stages handoff clones in.
    #[must_use]
    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    /// A fresh handle of the state database and its replica store.
    ///
    /// # Errors
    ///
    /// Fails when the database cannot be opened.
    pub fn replicas(&self) -> Result<(Arc<StateDb>, ReplicaStore)> {
        let db = Arc::new(StateDb::open(&self.db_path)?);
        let store = ReplicaStore::open(Arc::clone(&db))?;
        Ok((db, store))
    }

    /// Opens the cache on a fresh database handle and registers `backend`,
    /// without recovery.
    ///
    /// # Errors
    ///
    /// Fails when the database, the cache or the registration fails.
    pub fn open(&self, backend: Arc<dyn Backend>) -> Result<Daemon> {
        let (db, store) = self.replicas()?;
        let cache = ContentCache::open(
            store.clone(),
            self.cache_dir.clone(),
            self.staging_dir.clone(),
            self.config,
        )?;
        cache.register(Arc::clone(&backend))?;
        Ok(Daemon {
            db,
            store,
            cache,
            backend,
            incoming: self.incoming.clone(),
        })
    }

    /// Opens the cache and runs start-up recovery, as the daemon does.
    ///
    /// # Errors
    ///
    /// Fails when opening or recovery fails.
    pub async fn start(&self, backend: Arc<dyn Backend>) -> Result<Daemon> {
        let daemon = self.open(backend)?;
        daemon.cache.recover().await?;
        Ok(daemon)
    }
}

/// One daemon run over a [`Disk`].
pub struct Daemon {
    pub db: Arc<StateDb>,
    pub store: ReplicaStore,
    pub cache: ContentCache,
    pub backend: Arc<dyn Backend>,
    incoming: PathBuf,
}

impl std::fmt::Debug for Daemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Daemon")
            .field("cache", &self.cache)
            .field("backend", &self.backend.id())
            .finish_non_exhaustive()
    }
}

impl Daemon {
    /// The replica of the registered backend.
    ///
    /// # Errors
    ///
    /// Fails when the backend's root cannot be recorded.
    pub fn replica(&self) -> Result<Replica> {
        self.store.replica(Arc::clone(&self.backend))
    }

    /// The uid of the item at the root-relative `path`.
    ///
    /// # Errors
    ///
    /// Fails when a listing or the state database fails.
    pub async fn resolve(&self, path: &str) -> Result<Option<u64>> {
        self.replica()?.resolve(Path::new(path)).await
    }

    /// Writes `data` where the presenter hands a save over, under a name no
    /// other incoming file holds.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be written.
    pub fn incoming_file(&self, data: &[u8]) -> Result<PathBuf> {
        let mut n = 0_usize;
        loop {
            let path = self.incoming.join(format!("save-{n}"));
            if !path.try_exists()? {
                std::fs::write(&path, data)?;
                return Ok(path);
            }
            n += 1;
        }
    }
}
