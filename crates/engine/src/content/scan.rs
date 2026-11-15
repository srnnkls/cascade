//! Rescan of the replica's scan set against the backend.

use std::collections::HashSet;

use anyhow::Result;

use super::replica::{Replica, ReplicaItem};
use crate::backend::BackendError;
use crate::types::{FileEntry, Revision};

/// Rescan every listed folder and every folder holding a materialised item or
/// a cached object, parents first.
///
/// Each listing carries the revision of every child, so materialised and
/// cached files are stat-ed with it. Removals are applied after all listings,
/// so an item moved between scanned folders keeps its uid. A folder that is
/// gone on the backend is removed by listing the nearest ancestor that still
/// exists, whether or not that ancestor is in the scan set.
///
/// Returns whether the scan advanced the replica's revision.
///
/// # Errors
///
/// Fails when a listing fails for a reason other than a missing folder, or
/// when the state database fails.
pub async fn scan(replica: &Replica) -> Result<bool> {
    let db = replica.db();
    let before = db.replica_meta()?.revision;
    let mut listed = Vec::new();
    let mut visited = HashSet::new();
    for folder_uid in db.replica_scan_folders(replica.backend_id())? {
        let mut next = db.replica_item(folder_uid)?;
        while let Some(folder) = next.take() {
            if !visited.insert(folder.uid) {
                break;
            }
            match fetch_existing(replica, &folder).await? {
                Some(children) => {
                    let seen =
                        db.upsert_replica_children(replica.backend_id(), folder.uid, &children)?;
                    listed.push((folder.uid, seen));
                }
                None => {
                    next = match folder.parent_uid {
                        Some(parent_uid) => db.replica_item(parent_uid)?,
                        None => None,
                    };
                }
            }
        }
    }
    for (folder_uid, seen) in listed {
        db.remove_replica_children_except(folder_uid, &seen)?;
    }
    Ok(db.replica_meta()?.revision != before)
}

/// The children of `folder`, or `None` when the folder is gone on the backend.
async fn fetch_existing(
    replica: &Replica,
    folder: &ReplicaItem,
) -> Result<Option<Vec<(FileEntry, Revision)>>> {
    match replica.fetch_children(folder).await {
        Ok(children) => Ok(Some(children)),
        Err(err) if matches!(err.downcast_ref(), Some(BackendError::NotFound(_))) => Ok(None),
        Err(err) => Err(err),
    }
}
