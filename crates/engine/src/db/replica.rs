//! Replica index: queries over `replica_items`, `replica_tombstones` and
//! `replica_meta`.

use std::collections::HashSet;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use data_encoding::HEXLOWER;
use rusqlite::{OptionalExtension, Row, Transaction, params};
use sha2::{Digest, Sha256};

use super::StateDb;
use super::content::{id_to_sql, optional_u64_column, time_column, time_to_sql, u64_column};
use crate::types::{FileEntry, Revision};

const ITEM_COLUMNS: &str = "uid, parent_uid, name, kind, native_id, stable_id, size, mtime, \
     remote_size, remote_mtime, remote_ctime, content_version, metadata_version, uploaded, \
     pinned, listed_at IS NOT NULL";

/// Kind of a replica item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    /// A regular file.
    File,
    /// A folder.
    Folder,
}

impl ItemKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Folder => "folder",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "file" => Some(Self::File),
            "folder" => Some(Self::Folder),
            _ => None,
        }
    }

    const fn of(entry: &FileEntry) -> Self {
        if entry.is_dir {
            Self::Folder
        } else {
            Self::File
        }
    }
}

/// One item of the replica, identified by a daemon-assigned uid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaItem {
    /// Daemon-assigned identifier, independent of the item's path.
    pub uid: u64,
    /// Uid of the containing folder; `None` for the root.
    pub parent_uid: Option<u64>,
    /// Name within the parent folder.
    pub name: String,
    /// File or folder.
    pub kind: ItemKind,
    /// Backend id used to address the item in backend calls.
    pub native_id: String,
    /// Backend id that survives rename and move.
    pub stable_id: String,
    /// Presented size in bytes.
    pub size: Option<u64>,
    /// Presented last-write time.
    pub mtime: Option<DateTime<Utc>>,
    /// Revision last seen on the backend.
    pub remote_revision: Revision,
    /// Daemon-minted token of the content the item presents.
    pub content_version: String,
    /// Hash of name, parent and attributes.
    pub metadata_version: String,
    /// Whether the presented content is on the backend.
    pub uploaded: bool,
    /// Whether the item is pinned.
    pub pinned: bool,
    /// Whether the folder was listed by a presenter.
    pub listed: bool,
}

/// Store identity and revision counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaMeta {
    /// Random identity of this store.
    pub store_id: String,
    /// Revision of the latest change.
    pub revision: u64,
    /// Highest revision whose tombstones were pruned.
    pub floor: u64,
}

/// Items and tombstones newer than a revision, read in one snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaChanges {
    /// Store identity and counters at the time of the read.
    pub meta: ReplicaMeta,
    /// Items changed after the revision.
    pub updated: Vec<ReplicaItem>,
    /// Uids deleted after the revision.
    pub deleted: Vec<u64>,
}

fn item_from_row(row: &Row<'_>) -> rusqlite::Result<ReplicaItem> {
    let kind: String = row.get(3)?;
    let stable_id: String = row.get(5)?;
    Ok(ReplicaItem {
        uid: u64_column(row, 0)?,
        parent_uid: optional_u64_column(row, 1)?,
        name: row.get(2)?,
        kind: ItemKind::parse(&kind).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                format!("unknown item kind {kind:?}").into(),
            )
        })?,
        native_id: row.get(4)?,
        size: optional_u64_column(row, 6)?,
        mtime: time_column(row, 7)?,
        remote_revision: Revision {
            stable_id: stable_id.clone(),
            size: optional_u64_column(row, 8)?,
            last_write_time: time_column(row, 9)?,
            change_time: time_column(row, 10)?,
        },
        stable_id,
        content_version: row.get(11)?,
        metadata_version: row.get(12)?,
        uploaded: row.get(13)?,
        pinned: row.get(14)?,
        listed: row.get(15)?,
    })
}

fn meta_in(tx: &rusqlite::Connection) -> Result<ReplicaMeta> {
    Ok(tx.query_row(
        "SELECT store_id, revision, floor FROM replica_meta WHERE id = 1",
        [],
        |row| {
            Ok(ReplicaMeta {
                store_id: row.get(0)?,
                revision: u64_column(row, 1)?,
                floor: u64_column(row, 2)?,
            })
        },
    )?)
}

fn metadata_version(
    name: &str,
    parent_uid: Option<u64>,
    kind: ItemKind,
    size: Option<u64>,
    mtime: Option<DateTime<Utc>>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update([0]);
    hasher.update(parent_uid.unwrap_or_default().to_le_bytes());
    hasher.update(kind.as_str().as_bytes());
    hasher.update(size.unwrap_or_default().to_le_bytes());
    hasher.update(time_to_sql(mtime).unwrap_or_default().to_le_bytes());
    HEXLOWER.encode(&hasher.finalize())
}

/// The revision a write transaction assigns to every row it changes,
/// committed to `replica_meta` only when a row used it.
struct PendingRevision {
    next: u64,
    content_version: String,
    used: bool,
}

impl PendingRevision {
    fn open(tx: &Transaction<'_>) -> Result<Self> {
        let meta = meta_in(tx)?;
        let next = meta.revision + 1;
        Ok(Self {
            next,
            content_version: format!("{}.{next}", meta.store_id),
            used: false,
        })
    }

    fn take(&mut self) -> Result<i64> {
        self.used = true;
        id_to_sql(self.next)
    }

    fn commit(self, tx: &Transaction<'_>) -> Result<()> {
        if self.used {
            tx.execute(
                "UPDATE replica_meta SET revision = ?1 WHERE id = 1",
                [id_to_sql(self.next)?],
            )?;
        }
        Ok(())
    }
}

/// Uids are never reused: the next one is above every uid the database
/// still references.
fn next_uid(tx: &Transaction<'_>) -> Result<i64> {
    Ok(tx.query_row(
        "SELECT COALESCE(MAX(m), 0) + 1 FROM (
             SELECT MAX(uid) AS m FROM replica_items
             UNION ALL SELECT MAX(uid) FROM replica_tombstones
             UNION ALL SELECT MAX(uid) FROM content_objects
             UNION ALL SELECT MAX(uid) FROM materialized_items)",
        [],
        |row| row.get(0),
    )?)
}

fn item_by_stable_id(
    tx: &Transaction<'_>,
    backend_id: &str,
    stable_id: &str,
) -> Result<Option<ReplicaItem>> {
    Ok(tx
        .query_row(
            &format!(
                "SELECT {ITEM_COLUMNS} FROM replica_items WHERE backend_id = ?1 AND stable_id = ?2"
            ),
            params![backend_id, stable_id],
            item_from_row,
        )
        .optional()?)
}

fn has_dirty_object(tx: &Transaction<'_>, uid: u64) -> Result<bool> {
    Ok(tx.query_row(
        "SELECT EXISTS (SELECT 1 FROM content_objects WHERE uid = ?1 AND dirty)",
        [id_to_sql(uid)?],
        |row| row.get(0),
    )?)
}

/// Uids of the items with a dirty object and of every folder above them.
fn dirty_items_with_ancestors(tx: &Transaction<'_>) -> Result<HashSet<u64>> {
    let mut statement = tx.prepare(
        "WITH RECURSIVE kept(uid) AS (
             SELECT i.uid FROM replica_items i
                 JOIN content_objects o ON o.uid = i.uid WHERE o.dirty
             UNION
             SELECT i.parent_uid FROM replica_items i JOIN kept k ON i.uid = k.uid
                 WHERE i.parent_uid IS NOT NULL)
         SELECT uid FROM kept",
    )?;
    let uids = statement
        .query_map([], |row| u64_column(row, 0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(uids)
}

fn insert_item(
    tx: &Transaction<'_>,
    backend_id: &str,
    parent_uid: u64,
    entry: &FileEntry,
    revision: &Revision,
    pending: &mut PendingRevision,
) -> Result<u64> {
    let uid = next_uid(tx)?;
    let kind = ItemKind::of(entry);
    let item_revision = pending.take()?;
    tx.execute(
        "INSERT INTO replica_items (uid, backend_id, stable_id, native_id, parent_uid, name,
             kind, size, mtime, ctime, remote_size, remote_mtime, remote_ctime, content_version,
             metadata_version, revision)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        params![
            uid,
            backend_id,
            revision.stable_id,
            entry.id.native_id(),
            id_to_sql(parent_uid)?,
            entry.name,
            kind.as_str(),
            entry.size.map(id_to_sql).transpose()?,
            time_to_sql(entry.mod_time),
            time_to_sql(revision.change_time),
            revision.size.map(id_to_sql).transpose()?,
            time_to_sql(revision.last_write_time),
            time_to_sql(revision.change_time),
            pending.content_version,
            metadata_version(
                &entry.name,
                Some(parent_uid),
                kind,
                entry.size,
                entry.mod_time
            ),
            item_revision,
        ],
    )?;
    u64::try_from(uid).map_err(|e| anyhow!("uid: {e}"))
}

/// Follow name and parent only, keeping the content version and remote
/// revision of the local edit.
fn update_dirty_item(
    tx: &Transaction<'_>,
    item: &ReplicaItem,
    parent_uid: u64,
    entry: &FileEntry,
    pending: &mut PendingRevision,
) -> Result<()> {
    let version = metadata_version(
        &entry.name,
        Some(parent_uid),
        item.kind,
        item.size,
        item.mtime,
    );
    let revision = if version == item.metadata_version {
        None
    } else {
        Some(pending.take()?)
    };
    tx.execute(
        "UPDATE replica_items SET name = ?2, parent_uid = ?3, native_id = ?4,
             metadata_version = ?5, revision = COALESCE(?6, revision)
         WHERE uid = ?1",
        params![
            id_to_sql(item.uid)?,
            entry.name,
            id_to_sql(parent_uid)?,
            entry.id.native_id(),
            version,
            revision,
        ],
    )?;
    Ok(())
}

/// A new content version is minted when size or last-write time changed,
/// or when change time moved while name and parent stayed the same.
fn content_changed(item: &ReplicaItem, parent_uid: u64, entry: &FileEntry, now: &Revision) -> bool {
    let kind = ItemKind::of(entry);
    let before = &item.remote_revision;
    let moved = item.name != entry.name || item.parent_uid != Some(parent_uid);
    kind != item.kind
        || (kind == ItemKind::File
            && (now.size != before.size
                || now.last_write_time != before.last_write_time
                || (now.change_time != before.change_time && !moved)))
}

fn update_clean_item(
    tx: &Transaction<'_>,
    item: &ReplicaItem,
    parent_uid: u64,
    entry: &FileEntry,
    now: &Revision,
    pending: &mut PendingRevision,
) -> Result<()> {
    let kind = ItemKind::of(entry);
    let content = content_changed(item, parent_uid, entry, now);
    let version = metadata_version(
        &entry.name,
        Some(parent_uid),
        kind,
        entry.size,
        entry.mod_time,
    );
    let content_version = content.then(|| pending.content_version.clone());
    let revision = if content || version != item.metadata_version {
        Some(pending.take()?)
    } else {
        None
    };
    tx.execute(
        "UPDATE replica_items SET name = ?2, parent_uid = ?3, native_id = ?4, kind = ?5,
             size = ?6, mtime = ?7, ctime = ?8, remote_size = ?9, remote_mtime = ?10,
             remote_ctime = ?11, content_version = COALESCE(?12, content_version),
             metadata_version = ?13, revision = COALESCE(?14, revision)
         WHERE uid = ?1",
        params![
            id_to_sql(item.uid)?,
            entry.name,
            id_to_sql(parent_uid)?,
            entry.id.native_id(),
            kind.as_str(),
            entry.size.map(id_to_sql).transpose()?,
            time_to_sql(entry.mod_time),
            time_to_sql(now.change_time),
            now.size.map(id_to_sql).transpose()?,
            time_to_sql(now.last_write_time),
            time_to_sql(now.change_time),
            content_version,
            version,
            revision,
        ],
    )?;
    Ok(())
}

impl StateDb {
    /// Return the store identity and revision counters.
    pub fn replica_meta(&self) -> Result<ReplicaMeta> {
        let conn = self.lock_conn()?;
        meta_in(&conn)
    }

    /// Return the uid of the backend's root folder, registering it on first use.
    pub fn ensure_replica_root(&self, backend_id: &str, native_id: &str) -> Result<u64> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let existing = tx
            .query_row(
                "SELECT uid FROM replica_items WHERE backend_id = ?1 AND parent_uid IS NULL",
                [backend_id],
                |row| u64_column(row, 0),
            )
            .optional()?;
        if let Some(uid) = existing {
            return Ok(uid);
        }
        let mut pending = PendingRevision::open(&tx)?;
        let uid = next_uid(&tx)?;
        let root_revision = pending.take()?;
        tx.execute(
            "INSERT INTO replica_items (uid, backend_id, stable_id, native_id, name, kind,
                 content_version, metadata_version, revision)
             VALUES (?1, ?2, ?3, ?3, '', ?4, ?5, ?6, ?7)",
            params![
                uid,
                backend_id,
                native_id,
                ItemKind::Folder.as_str(),
                pending.content_version,
                metadata_version("", None, ItemKind::Folder, None, None),
                root_revision,
            ],
        )?;
        pending.commit(&tx)?;
        tx.commit()?;
        u64::try_from(uid).map_err(|e| anyhow!("uid: {e}"))
    }

    /// Return the item with the given uid.
    pub fn replica_item(&self, uid: u64) -> Result<Option<ReplicaItem>> {
        let conn = self.lock_conn()?;
        Ok(conn
            .query_row(
                &format!("SELECT {ITEM_COLUMNS} FROM replica_items WHERE uid = ?1"),
                [id_to_sql(uid)?],
                item_from_row,
            )
            .optional()?)
    }

    /// Return the child of a folder with the given name.
    pub fn replica_child(&self, parent_uid: u64, name: &str) -> Result<Option<ReplicaItem>> {
        let conn = self.lock_conn()?;
        Ok(conn
            .query_row(
                &format!(
                    "SELECT {ITEM_COLUMNS} FROM replica_items
                     WHERE parent_uid = ?1 AND name = ?2 ORDER BY uid LIMIT 1"
                ),
                params![id_to_sql(parent_uid)?, name],
                item_from_row,
            )
            .optional()?)
    }

    /// Return the children of a folder in name order.
    pub fn replica_children(&self, parent_uid: u64) -> Result<Vec<ReplicaItem>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {ITEM_COLUMNS} FROM replica_items WHERE parent_uid = ?1 ORDER BY name, uid"
        ))?;
        let items = statement
            .query_map([id_to_sql(parent_uid)?], item_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(items)
    }

    /// Register or update the listed children of a folder by stable id and
    /// return their uids in listing order. Changed rows move to a new
    /// revision; an item with a dirty object follows name and parent only.
    pub fn upsert_replica_children(
        &self,
        backend_id: &str,
        parent_uid: u64,
        listing: &[(FileEntry, Revision)],
    ) -> Result<Vec<u64>> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let mut pending = PendingRevision::open(&tx)?;
        let mut uids = Vec::with_capacity(listing.len());
        for (entry, revision) in listing {
            let uid = match item_by_stable_id(&tx, backend_id, &revision.stable_id)? {
                None => insert_item(&tx, backend_id, parent_uid, entry, revision, &mut pending)?,
                Some(item) if has_dirty_object(&tx, item.uid)? => {
                    update_dirty_item(&tx, &item, parent_uid, entry, &mut pending)?;
                    item.uid
                }
                Some(item) => {
                    update_clean_item(&tx, &item, parent_uid, entry, revision, &mut pending)?;
                    item.uid
                }
            };
            uids.push(uid);
        }
        pending.commit(&tx)?;
        tx.commit()?;
        Ok(uids)
    }

    /// Delete the children of a folder not in `keep`, with their descendants,
    /// leaving a tombstone for each. Items with a dirty object stay at every
    /// depth, together with the folders leading to them. Returns the deleted
    /// uids.
    pub fn remove_replica_children_except(
        &self,
        parent_uid: u64,
        keep: &[u64],
    ) -> Result<Vec<u64>> {
        let keep: HashSet<u64> = keep.iter().copied().collect();
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let dirty_paths = dirty_items_with_ancestors(&tx)?;
        let gone: Vec<u64> = {
            let mut statement =
                tx.prepare("SELECT uid FROM replica_items WHERE parent_uid = ?1")?;
            statement
                .query_map([id_to_sql(parent_uid)?], |row| u64_column(row, 0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .into_iter()
                .filter(|uid| !keep.contains(uid))
                .collect()
        };
        let mut pending = PendingRevision::open(&tx)?;
        let mut deleted = Vec::new();
        for uid in gone {
            let doomed: Vec<(u64, Option<u64>)> = {
                let mut statement = tx.prepare(
                    "WITH RECURSIVE doomed(uid, parent_uid) AS (
                         SELECT uid, parent_uid FROM replica_items WHERE uid = ?1
                         UNION ALL
                         SELECT i.uid, i.parent_uid FROM replica_items i
                         JOIN doomed d ON i.parent_uid = d.uid)
                     SELECT uid, parent_uid FROM doomed",
                )?;
                statement
                    .query_map([id_to_sql(uid)?], |row| {
                        Ok((u64_column(row, 0)?, optional_u64_column(row, 1)?))
                    })?
                    .collect::<rusqlite::Result<_>>()?
            };
            let doomed: Vec<_> = doomed
                .into_iter()
                .filter(|(doomed_uid, _)| !dirty_paths.contains(doomed_uid))
                .collect();
            if doomed.is_empty() {
                continue;
            }
            let revision = pending.take()?;
            for (doomed_uid, doomed_parent) in doomed {
                tx.execute(
                    "INSERT OR REPLACE INTO replica_tombstones (uid, parent_uid, revision)
                     VALUES (?1, ?2, ?3)",
                    params![
                        id_to_sql(doomed_uid)?,
                        doomed_parent.map(id_to_sql).transpose()?,
                        revision
                    ],
                )?;
                tx.execute(
                    "DELETE FROM replica_items WHERE uid = ?1",
                    [id_to_sql(doomed_uid)?],
                )?;
                deleted.push(doomed_uid);
            }
        }
        pending.commit(&tx)?;
        tx.commit()?;
        Ok(deleted)
    }

    /// Record that a presenter listed the folder, adding it to the scan set.
    pub fn mark_replica_listed(&self, uid: u64, at: DateTime<Utc>) -> Result<()> {
        let conn = self.lock_conn()?;
        let changed = conn.execute(
            "UPDATE replica_items SET listed_at = ?2 WHERE uid = ?1",
            params![id_to_sql(uid)?, time_to_sql(Some(at))],
        )?;
        if changed == 0 {
            return Err(anyhow!("no replica item {uid}"));
        }
        Ok(())
    }

    /// Return the scan set of a backend, parents first: every listed folder
    /// and every folder holding a materialised item or a cached object.
    pub fn replica_scan_folders(&self, backend_id: &str) -> Result<Vec<u64>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare(
            "WITH RECURSIVE
                 seeds(uid) AS (
                     SELECT uid FROM replica_items WHERE listed_at IS NOT NULL
                     UNION SELECT i.parent_uid FROM replica_items i
                         JOIN materialized_items m ON m.uid = i.uid
                     UNION SELECT i.parent_uid FROM replica_items i
                         JOIN content_objects o ON o.uid = i.uid),
                 depth(uid, level) AS (
                     SELECT uid, 0 FROM replica_items
                         WHERE backend_id = ?1 AND parent_uid IS NULL
                     UNION ALL
                     SELECT i.uid, d.level + 1 FROM replica_items i
                         JOIN depth d ON i.parent_uid = d.uid
                         WHERE i.kind = 'folder')
             SELECT d.uid FROM depth d JOIN seeds s ON s.uid = d.uid ORDER BY d.level, d.uid",
        )?;
        let uids = statement
            .query_map([backend_id], |row| u64_column(row, 0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(uids)
    }

    /// Return the items and tombstones newer than `revision` together with
    /// the counters they were read under.
    pub fn replica_changes_since(&self, revision: u64) -> Result<ReplicaChanges> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let meta = meta_in(&tx)?;
        let since = id_to_sql(revision)?;
        let updated = {
            let mut statement = tx.prepare(&format!(
                "SELECT {ITEM_COLUMNS} FROM replica_items WHERE revision > ?1 ORDER BY uid"
            ))?;
            statement
                .query_map([since], item_from_row)?
                .collect::<rusqlite::Result<_>>()?
        };
        let deleted = {
            let mut statement =
                tx.prepare("SELECT uid FROM replica_tombstones WHERE revision > ?1 ORDER BY uid")?;
            statement
                .query_map([since], |row| u64_column(row, 0))?
                .collect::<rusqlite::Result<_>>()?
        };
        tx.commit()?;
        Ok(ReplicaChanges {
            meta,
            updated,
            deleted,
        })
    }

    /// Drop tombstones up to and including `revision` and raise the floor to it.
    pub fn prune_replica_tombstones(&self, revision: u64) -> Result<()> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let through = id_to_sql(revision)?;
        tx.execute(
            "DELETE FROM replica_tombstones WHERE revision <= ?1",
            [through],
        )?;
        tx.execute(
            "UPDATE replica_meta SET floor = MAX(floor, ?1) WHERE id = 1",
            [through],
        )?;
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;
