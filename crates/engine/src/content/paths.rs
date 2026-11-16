//! Path access for presenters that name items by path, such as NFS.

use std::path::Path;

use anyhow::{Result, anyhow};

use super::cache::ContentCache;
use super::replica::ReplicaItem;

impl ContentCache {
    /// Whether the backend with id `backend_id` is registered.
    #[must_use]
    pub fn serves(&self, backend_id: &str) -> bool {
        self.replicas()
            .iter()
            .any(|replica| replica.backend_id() == backend_id)
    }

    /// The uid of the item at `path`, relative to the root of the registered
    /// backend `backend_id`, listing and registering folders along the way
    /// whose child is missing. Returns `None` when no such item exists.
    ///
    /// # Errors
    ///
    /// Fails when the backend is not registered, a listing fails or the
    /// state database fails.
    pub async fn resolve(&self, backend_id: &str, path: &Path) -> Result<Option<u64>> {
        let replica = self
            .replicas()
            .into_iter()
            .find(|replica| replica.backend_id() == backend_id)
            .ok_or_else(|| anyhow!("backend {backend_id} is not registered"))?;
        self.shared().remote.call(replica.resolve(path)).await
    }

    /// Lists a folder from its backend, records the result and adds the
    /// folder to the scan set.
    ///
    /// # Errors
    ///
    /// Fails when the folder is unknown, its backend is not registered or
    /// the listing fails.
    pub async fn list(&self, folder_uid: u64) -> Result<Vec<ReplicaItem>> {
        let replica = self.replica(folder_uid)?;
        self.shared().remote.call(replica.list(folder_uid)).await
    }
}
