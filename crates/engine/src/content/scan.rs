//! Rescan of the replica's scan set against the backend.

use std::collections::HashSet;

use anyhow::Result;

use super::replica::Replica;

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
        listed.extend(replica.list_existing(folder_uid, &mut visited).await?);
    }
    for (folder_uid, seen) in listed {
        db.remove_replica_children_except(folder_uid, &seen)?;
    }
    Ok(db.replica_meta()?.revision != before)
}
