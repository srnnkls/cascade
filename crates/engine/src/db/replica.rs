//! Replica index: queries over `replica_items`, `replica_tombstones` and
//! `replica_meta`.

use std::collections::HashSet;
use std::ops::Range;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use data_encoding::HEXLOWER;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use sha2::{Digest, Sha256};

use super::StateDb;
use super::content::{
    ContentState, RangeTable, id_to_sql, merge_range_in, object_id_to_sql, optional_u64_column,
    subtree_objects_in, time_column, time_to_sql, truncate_ranges_in, u64_column,
};
use crate::content::ObjectId;
use crate::types::{FileEntry, Revision};

const ITEM_COLUMNS: &str = "uid, parent_uid, name, kind, native_id, stable_id, size, mtime, \
     remote_size, remote_mtime, remote_ctime, content_version, metadata_version, uploaded, \
     pinned OR rule_pinned, listed_at IS NOT NULL";

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
    /// Whether the item is pinned explicitly or by a pin rule.
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

/// The item whose dirty object recorded an upload that became the file
/// `stable_id`, re-linked to that file: its rename-over happened and the
/// bookkeeping is still due.
fn relink_recorded_upload(
    tx: &Transaction<'_>,
    backend_id: &str,
    entry: &FileEntry,
    stable_id: &str,
) -> Result<Option<ReplicaItem>> {
    let uid = tx
        .query_row(
            "SELECT i.uid FROM replica_items i JOIN content_objects o ON o.uid = i.uid
             WHERE i.backend_id = ?1 AND o.dirty AND o.upload_file_id = ?2",
            params![backend_id, stable_id],
            |row| u64_column(row, 0),
        )
        .optional()?;
    let Some(uid) = uid else {
        return Ok(None);
    };
    tx.execute(
        "UPDATE replica_items SET stable_id = ?2, native_id = ?3 WHERE uid = ?1",
        params![id_to_sql(uid)?, stable_id, entry.id.native_id()],
    )?;
    item_in(tx, uid).map(Some)
}

fn has_dirty_object(tx: &Transaction<'_>, uid: u64) -> Result<bool> {
    Ok(tx.query_row(
        "SELECT EXISTS (SELECT 1 FROM content_objects WHERE uid = ?1 AND dirty)",
        [id_to_sql(uid)?],
        |row| row.get(0),
    )?)
}

/// Uids of the items with a dirty object and of every folder above them.
fn dirty_items_with_ancestors(conn: &Connection) -> Result<HashSet<u64>> {
    let mut statement = conn.prepare(
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

/// Delete `uid` with its descendants, except those in `keep`, leaving a
/// tombstone for each. Returns the deleted uids.
fn tombstone_subtree(
    tx: &Transaction<'_>,
    uid: u64,
    keep: &HashSet<u64>,
    pending: &mut PendingRevision,
) -> Result<Vec<u64>> {
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
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .filter(|(doomed_uid, _)| !keep.contains(doomed_uid))
            .collect()
    };
    if doomed.is_empty() {
        return Ok(Vec::new());
    }
    let revision = pending.take()?;
    let mut deleted = Vec::with_capacity(doomed.len());
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
    Ok(deleted)
}

fn item_in(tx: &Transaction<'_>, uid: u64) -> Result<ReplicaItem> {
    tx.query_row(
        &format!("SELECT {ITEM_COLUMNS} FROM replica_items WHERE uid = ?1"),
        [id_to_sql(uid)?],
        item_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("no replica item {uid}"))
}

/// The source of an item's pin: an item is pinned while either holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PinSource {
    /// Pinned on the item itself, as Keep Downloaded does.
    Explicit,
    /// Pinned because a pin rule matches the item.
    Rule,
}

impl PinSource {
    const fn column(self) -> &'static str {
        match self {
            Self::Explicit => "pinned",
            Self::Rule => "rule_pinned",
        }
    }
}

/// The explicit and the rule pin of an item.
fn pins_in(tx: &Transaction<'_>, uid: u64) -> Result<(bool, bool)> {
    tx.query_row(
        "SELECT pinned, rule_pinned FROM replica_items WHERE uid = ?1",
        [id_to_sql(uid)?],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()?
    .ok_or_else(|| anyhow!("no replica item {uid}"))
}

fn set_pin_in(tx: &Transaction<'_>, uid: u64, source: PinSource, pinned: bool) -> Result<()> {
    let mut pending = PendingRevision::open(tx)?;
    tx.execute(
        &format!(
            "UPDATE replica_items SET {} = ?2, revision = ?3 WHERE uid = ?1",
            source.column()
        ),
        params![id_to_sql(uid)?, pinned, pending.take()?],
    )?;
    pending.commit(tx)
}

/// Whether pinning the unpinned `item` keeps the presented sizes of all
/// pinned items, each counted once, within `max_pinned_bytes`.
fn pin_fits_in(tx: &Transaction<'_>, item: &ReplicaItem, max_pinned_bytes: u64) -> Result<bool> {
    let others: u64 = tx.query_row(
        "SELECT COALESCE(SUM(size), 0) FROM replica_items
         WHERE (pinned OR rule_pinned) AND uid != ?1",
        [id_to_sql(item.uid)?],
        |row| u64_column(row, 0),
    )?;
    Ok(others.saturating_add(item.size.unwrap_or_default()) <= max_pinned_bytes)
}

/// Items a reconciliation of the rule pins pinned, refused and unpinned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PinReport {
    /// Items pinned, in the order they were given.
    pub pinned: Vec<u64>,
    /// Matching items left unpinned because the budget was used.
    pub refused: Vec<u64>,
    /// Items no rule matches any more and that are not pinned explicitly.
    pub unpinned: Vec<u64>,
}

/// Point the item at the backend file `entry` with `revision`, keeping its
/// uid and content version. Any other item holding that file, or the item's
/// name in its folder, is removed: the file replaced it.
fn relink_in(
    tx: &Transaction<'_>,
    item: &ReplicaItem,
    entry: &FileEntry,
    revision: &Revision,
    pending: &mut PendingRevision,
) -> Result<()> {
    let uid = id_to_sql(item.uid)?;
    let backend_id: String = tx.query_row(
        "SELECT backend_id FROM replica_items WHERE uid = ?1",
        [uid],
        |row| row.get(0),
    )?;
    let displaced: Vec<u64> = {
        let mut statement = tx.prepare(
            "SELECT uid FROM replica_items WHERE uid != ?1
                 AND ((backend_id = ?2 AND stable_id = ?3) OR (parent_uid = ?4 AND name = ?5))",
        )?;
        statement
            .query_map(
                params![
                    uid,
                    backend_id,
                    revision.stable_id,
                    item.parent_uid.map(id_to_sql).transpose()?,
                    item.name
                ],
                |row| u64_column(row, 0),
            )?
            .collect::<rusqlite::Result<_>>()?
    };
    let mut keep = dirty_items_with_ancestors(tx)?;
    keep.insert(item.uid);
    for other in displaced {
        tombstone_subtree(tx, other, &keep, pending)?;
    }
    tx.execute(
        "UPDATE replica_items SET stable_id = ?2, native_id = ?3, remote_size = ?4,
             remote_mtime = ?5, remote_ctime = ?6, revision = ?7
         WHERE uid = ?1",
        params![
            uid,
            revision.stable_id,
            entry.id.native_id(),
            revision.size.map(id_to_sql).transpose()?,
            time_to_sql(revision.last_write_time),
            time_to_sql(revision.change_time),
            pending.take()?,
        ],
    )?;
    Ok(())
}

/// Delete every object of `uid` other than `kept` and return their ids.
fn supersede_in(tx: &Transaction<'_>, uid: u64, kept: ObjectId) -> Result<Vec<ObjectId>> {
    let uid = id_to_sql(uid)?;
    let kept = object_id_to_sql(kept)?;
    let superseded: Vec<ObjectId> = {
        let mut statement =
            tx.prepare("SELECT object_id FROM content_objects WHERE uid = ?1 AND object_id != ?2")?;
        statement
            .query_map(params![uid, kept], |row| {
                u64_column(row, 0).map(ObjectId::new)
            })?
            .collect::<rusqlite::Result<_>>()?
    };
    tx.execute(
        "DELETE FROM content_objects WHERE uid = ?1 AND object_id != ?2",
        params![uid, kept],
    )?;
    Ok(superseded)
}

/// Size of the edited object `object_id` of `uid`.
fn edited_size_in(tx: &Transaction<'_>, uid: u64, object_id: ObjectId) -> Result<u64> {
    tx.query_row(
        "SELECT size FROM content_objects WHERE object_id = ?1 AND uid = ?2 AND edited",
        params![object_id_to_sql(object_id)?, id_to_sql(uid)?],
        |row| u64_column(row, 0),
    )
    .optional()?
    .ok_or_else(|| anyhow!("no edited content object {object_id} of item {uid}"))
}

/// Set the size of the edited object and the presented size and last-write
/// time of its item, leaving content version and revision for the save.
fn resize_edit_in(
    tx: &Transaction<'_>,
    uid: u64,
    object_id: ObjectId,
    size: u64,
    mtime: DateTime<Utc>,
) -> Result<ReplicaItem> {
    tx.execute(
        "UPDATE content_objects SET size = ?2 WHERE object_id = ?1",
        params![object_id_to_sql(object_id)?, id_to_sql(size)?],
    )?;
    tx.execute(
        "UPDATE replica_items SET size = ?2, mtime = ?3 WHERE uid = ?1",
        params![id_to_sql(uid)?, id_to_sql(size)?, time_to_sql(Some(mtime))],
    )?;
    item_in(tx, uid)
}

impl StateDb {
    /// Return the uids of the items with a dirty object and of every folder
    /// above them: the items that stay while their upload is pending.
    pub fn dirty_items_with_ancestors(&self) -> Result<HashSet<u64>> {
        let conn = self.lock_conn()?;
        dirty_items_with_ancestors(&conn)
    }

    /// Accept a local save held by `object_id`: the object becomes the
    /// item's only, dirty and complete object, and the item takes a new
    /// content version, the saved size and last-write time and
    /// `uploaded = false`. Returns the item and the objects the save
    /// superseded, whose files the caller removes.
    pub fn accept_content_save(
        &self,
        uid: u64,
        object_id: ObjectId,
        size: u64,
        mtime: DateTime<Utc>,
    ) -> Result<(ReplicaItem, Vec<ObjectId>)> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let mut pending = PendingRevision::open(&tx)?;
        let content_version = pending.content_version.clone();
        let item_revision = pending.take()?;
        let item = item_in(&tx, uid)?;
        let sql_uid = id_to_sql(uid)?;
        let object = id_to_sql(object_id.get())?;
        let changed = tx.execute(
            "UPDATE content_objects SET dirty = TRUE, edited = FALSE, state = ?3,
                 content_version = ?4, size = ?5
             WHERE object_id = ?1 AND uid = ?2",
            params![
                object,
                sql_uid,
                ContentState::Complete.as_str(),
                content_version,
                id_to_sql(size)?
            ],
        )?;
        if changed == 0 {
            return Err(anyhow!("no content object {object_id} of item {uid}"));
        }
        tx.execute("DELETE FROM content_written WHERE object_id = ?1", [object])?;
        let superseded = supersede_in(&tx, uid, object_id)?;
        tx.execute(
            "UPDATE replica_items SET content_version = ?2, size = ?3, mtime = ?4,
                 uploaded = FALSE, metadata_version = ?5, revision = ?6
             WHERE uid = ?1",
            params![
                sql_uid,
                content_version,
                id_to_sql(size)?,
                time_to_sql(Some(mtime)),
                metadata_version(
                    &item.name,
                    item.parent_uid,
                    item.kind,
                    Some(size),
                    Some(mtime)
                ),
                item_revision,
            ],
        )?;
        let saved = item_in(&tx, uid)?;
        pending.commit(&tx)?;
        tx.commit()?;
        Ok((saved, superseded))
    }

    /// Start an in-place edit in `object_id`, whose file already holds the
    /// bytes of `present`: the object becomes the item's only object, dirty
    /// and edited, and the item is marked not uploaded. Returns the objects
    /// the edit superseded, whose files the caller removes.
    pub fn begin_content_edit(
        &self,
        uid: u64,
        object_id: ObjectId,
        present: &[Range<u64>],
    ) -> Result<Vec<ObjectId>> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let changed = tx.execute(
            "UPDATE content_objects SET dirty = TRUE, edited = TRUE, state = ?3
             WHERE object_id = ?1 AND uid = ?2",
            params![
                object_id_to_sql(object_id)?,
                id_to_sql(uid)?,
                ContentState::Partial.as_str()
            ],
        )?;
        if changed == 0 {
            return Err(anyhow!("no content object {object_id} of item {uid}"));
        }
        for range in present {
            merge_range_in(&tx, RangeTable::Present, object_id, range.clone())?;
        }
        let superseded = supersede_in(&tx, uid, object_id)?;
        tx.execute(
            "UPDATE replica_items SET uploaded = FALSE WHERE uid = ?1",
            [id_to_sql(uid)?],
        )?;
        tx.commit()?;
        Ok(superseded)
    }

    /// Record bytes written into the edited object at `written`: the range
    /// is written and present, and the object and the item grow to its end.
    /// Returns the item.
    pub fn record_content_write(
        &self,
        uid: u64,
        object_id: ObjectId,
        written: Range<u64>,
        mtime: DateTime<Utc>,
    ) -> Result<ReplicaItem> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let size = edited_size_in(&tx, uid, object_id)?.max(written.end);
        merge_range_in(&tx, RangeTable::Written, object_id, written.clone())?;
        merge_range_in(&tx, RangeTable::Present, object_id, written)?;
        let item = resize_edit_in(&tx, uid, object_id, size, mtime)?;
        tx.commit()?;
        Ok(item)
    }

    /// Record a size change of the edited object: ranges past `size` are
    /// cut, and growth is written as zeros. Returns the item.
    pub fn truncate_content_edit(
        &self,
        uid: u64,
        object_id: ObjectId,
        size: u64,
        mtime: DateTime<Utc>,
    ) -> Result<ReplicaItem> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let old = edited_size_in(&tx, uid, object_id)?;
        for table in [RangeTable::Written, RangeTable::Present] {
            if size < old {
                truncate_ranges_in(&tx, table, object_id, size)?;
            } else {
                merge_range_in(&tx, table, object_id, old..size)?;
            }
        }
        let item = resize_edit_in(&tx, uid, object_id, size, mtime)?;
        tx.commit()?;
        Ok(item)
    }

    /// Move the edited object onto `revision`: only its written ranges stay
    /// present, so every other byte is fetched from that revision.
    pub fn rebase_content_edit(&self, object_id: ObjectId, revision: &Revision) -> Result<()> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let object = object_id_to_sql(object_id)?;
        tx.execute("DELETE FROM content_ranges WHERE object_id = ?1", [object])?;
        tx.execute(
            "INSERT INTO content_ranges (object_id, start, \"end\")
             SELECT object_id, start, \"end\" FROM content_written WHERE object_id = ?1",
            [object],
        )?;
        let changed = tx.execute(
            "UPDATE content_objects SET source_stable_id = ?2, source_size = ?3,
                 source_mtime = ?4, source_ctime = ?5
             WHERE object_id = ?1 AND edited",
            params![
                object,
                revision.stable_id,
                revision.size.map(id_to_sql).transpose()?,
                time_to_sql(revision.last_write_time),
                time_to_sql(revision.change_time),
            ],
        )?;
        if changed == 0 {
            return Err(anyhow!("no edited content object {object_id}"));
        }
        tx.commit()?;
        Ok(())
    }

    /// Record a finished rename-over: the item is re-linked to the renamed
    /// file, and when `object_id` is still its dirty object and not edited,
    /// the object becomes clean with the renamed file's revision as its
    /// source and the item is marked uploaded. Returns whether the object
    /// became clean.
    pub fn finish_content_upload(
        &self,
        uid: u64,
        object_id: ObjectId,
        entry: &FileEntry,
        revision: &Revision,
    ) -> Result<bool> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let mut pending = PendingRevision::open(&tx)?;
        let item = item_in(&tx, uid)?;
        relink_in(&tx, &item, entry, revision, &mut pending)?;
        let cleaned = tx.execute(
            "UPDATE content_objects SET dirty = FALSE, state = ?3, upload_temp_name = NULL,
                 upload_file_id = NULL, source_stable_id = ?4, source_size = ?5,
                 source_mtime = ?6, source_ctime = ?7
             WHERE object_id = ?1 AND uid = ?2 AND dirty AND NOT edited",
            params![
                id_to_sql(object_id.get())?,
                id_to_sql(uid)?,
                ContentState::Complete.as_str(),
                revision.stable_id,
                revision.size.map(id_to_sql).transpose()?,
                time_to_sql(revision.last_write_time),
                time_to_sql(revision.change_time),
            ],
        )? > 0;
        if cleaned {
            tx.execute(
                "UPDATE replica_items SET uploaded = TRUE WHERE uid = ?1",
                [id_to_sql(uid)?],
            )?;
        }
        pending.commit(&tx)?;
        tx.commit()?;
        Ok(cleaned)
    }

    /// Point a folder item at a folder recreated on the backend.
    pub fn relink_replica_item(
        &self,
        uid: u64,
        entry: &FileEntry,
        revision: &Revision,
    ) -> Result<()> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let mut pending = PendingRevision::open(&tx)?;
        let item = item_in(&tx, uid)?;
        relink_in(&tx, &item, entry, revision, &mut pending)?;
        pending.commit(&tx)?;
        tx.commit()?;
        Ok(())
    }

    /// Mark an item and its descendants as being deleted, before the backend
    /// delete is issued.
    pub fn mark_replica_delete(&self, uid: u64) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT OR IGNORE INTO replica_deletes (uid) VALUES (?1)",
            [id_to_sql(uid)?],
        )?;
        Ok(())
    }

    /// Clear the delete mark of an item whose backend delete failed.
    pub fn clear_replica_delete(&self, uid: u64) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "DELETE FROM replica_deletes WHERE uid = ?1",
            [id_to_sql(uid)?],
        )?;
        Ok(())
    }

    /// Return the items marked as being deleted, in uid order.
    pub fn replica_deletes(&self) -> Result<Vec<u64>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare("SELECT uid FROM replica_deletes ORDER BY uid")?;
        let uids = statement
            .query_map([], |row| u64_column(row, 0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(uids)
    }

    /// Whether the item or one of its ancestors is marked as being deleted.
    pub fn replica_item_deleting(&self, uid: u64) -> Result<bool> {
        let conn = self.lock_conn()?;
        Ok(conn.query_row(
            "WITH RECURSIVE ancestors(uid) AS (
                 SELECT ?1
                 UNION ALL
                 SELECT i.parent_uid FROM replica_items i JOIN ancestors a ON i.uid = a.uid
                 WHERE i.parent_uid IS NOT NULL)
             SELECT EXISTS (SELECT 1 FROM replica_deletes d JOIN ancestors a ON d.uid = a.uid)",
            [id_to_sql(uid)?],
            |row| row.get(0),
        )?)
    }

    /// Delete an item with its descendants and their cached objects, leaving
    /// tombstones, and clear its delete mark. Returns the deleted objects,
    /// whose files the caller removes.
    pub fn remove_replica_item(&self, uid: u64) -> Result<Vec<ObjectId>> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let objects: Vec<ObjectId> = subtree_objects_in(&tx, uid)?
            .into_iter()
            .map(|object| object.object_id)
            .collect();
        for object in &objects {
            tx.execute(
                "DELETE FROM content_objects WHERE object_id = ?1",
                [id_to_sql(object.get())?],
            )?;
        }
        tx.execute(
            "DELETE FROM replica_deletes WHERE uid = ?1",
            [id_to_sql(uid)?],
        )?;
        let mut pending = PendingRevision::open(&tx)?;
        tombstone_subtree(&tx, uid, &HashSet::new(), &mut pending)?;
        pending.commit(&tx)?;
        tx.commit()?;
        Ok(objects)
    }

    /// Return the store identity and revision counters.
    pub fn replica_meta(&self) -> Result<ReplicaMeta> {
        let conn = self.lock_conn()?;
        meta_in(&conn)
    }

    /// Whether the last backend call failed as unreachable.
    pub fn remote_unreachable(&self) -> Result<bool> {
        let conn = self.lock_conn()?;
        Ok(conn.query_row(
            "SELECT remote_unreachable FROM replica_meta WHERE id = 1",
            [],
            |row| row.get(0),
        )?)
    }

    /// Record whether the last backend call failed as unreachable.
    pub fn set_remote_unreachable(&self, unreachable: bool) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE replica_meta SET remote_unreachable = ?1 WHERE id = 1",
            [unreachable],
        )?;
        Ok(())
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

    /// Return every item of a backend in uid order.
    pub fn replica_items_of(&self, backend_id: &str) -> Result<Vec<ReplicaItem>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {ITEM_COLUMNS} FROM replica_items WHERE backend_id = ?1 ORDER BY uid"
        ))?;
        let items = statement
            .query_map([backend_id], item_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(items)
    }

    /// Return the working set in uid order: every pinned and every
    /// materialised item.
    pub fn replica_working_set(&self) -> Result<Vec<ReplicaItem>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {ITEM_COLUMNS} FROM replica_items
             WHERE pinned OR rule_pinned OR uid IN (SELECT uid FROM materialized_items)
             ORDER BY uid"
        ))?;
        let items = statement
            .query_map([], item_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(items)
    }

    /// Pin an item explicitly unless the presented sizes of all pinned
    /// items would then exceed `max_pinned_bytes`. Returns whether the item
    /// is pinned; a changed flag moves the item to a new revision.
    pub fn pin_replica_item(&self, uid: u64, max_pinned_bytes: u64) -> Result<bool> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let item = item_in(&tx, uid)?;
        let (explicit, _) = pins_in(&tx, uid)?;
        if explicit {
            return Ok(true);
        }
        if !item.pinned && !pin_fits_in(&tx, &item, max_pinned_bytes)? {
            return Ok(false);
        }
        set_pin_in(&tx, uid, PinSource::Explicit, true)?;
        tx.commit()?;
        Ok(true)
    }

    /// Clear the explicit pin of an item, leaving a rule pin; a changed flag
    /// moves the item to a new revision.
    pub fn unpin_replica_item(&self, uid: u64) -> Result<()> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        if pins_in(&tx, uid)?.0 {
            set_pin_in(&tx, uid, PinSource::Explicit, false)?;
            tx.commit()?;
        }
        Ok(())
    }

    /// Make `wanted` the items pinned by rules: clears the rule pin of every
    /// other item, then pins the items of `wanted` in order while the
    /// presented sizes of all pinned items stay within `max_pinned_bytes`;
    /// once one is refused, the rest are refused as well. A changed flag
    /// moves the item to a new revision.
    pub fn reconcile_rule_pins(&self, wanted: &[u64], max_pinned_bytes: u64) -> Result<PinReport> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let wanted_set: HashSet<u64> = wanted.iter().copied().collect();
        let held: Vec<(u64, bool)> = {
            let mut statement =
                tx.prepare("SELECT uid, pinned FROM replica_items WHERE rule_pinned ORDER BY uid")?;
            statement
                .query_map([], |row| Ok((u64_column(row, 0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?
        };
        let mut report = PinReport::default();
        for (uid, explicit) in held {
            if !wanted_set.contains(&uid) {
                set_pin_in(&tx, uid, PinSource::Rule, false)?;
                if !explicit {
                    report.unpinned.push(uid);
                }
            }
        }
        for &uid in wanted {
            let item = item_in(&tx, uid)?;
            let (explicit, by_rule) = pins_in(&tx, uid)?;
            if by_rule {
                continue;
            }
            if explicit {
                set_pin_in(&tx, uid, PinSource::Rule, true)?;
            } else if report.refused.is_empty() && pin_fits_in(&tx, &item, max_pinned_bytes)? {
                set_pin_in(&tx, uid, PinSource::Rule, true)?;
                report.pinned.push(uid);
            } else {
                report.refused.push(uid);
            }
        }
        tx.commit()?;
        Ok(report)
    }

    /// Return the id of the backend holding the item with the given uid.
    pub fn replica_item_backend(&self, uid: u64) -> Result<Option<String>> {
        let conn = self.lock_conn()?;
        Ok(conn
            .query_row(
                "SELECT backend_id FROM replica_items WHERE uid = ?1",
                [id_to_sql(uid)?],
                |row| row.get(0),
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
            let known = match item_by_stable_id(&tx, backend_id, &revision.stable_id)? {
                None => relink_recorded_upload(&tx, backend_id, entry, &revision.stable_id)?,
                found => found,
            };
            let uid = match known {
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
            deleted.extend(tombstone_subtree(&tx, uid, &dirty_paths, &mut pending)?);
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

    /// Return the items newer than `revision` in revision order and the
    /// tombstones newer than it, together with the counters they were read
    /// under.
    pub fn replica_changes_since(&self, revision: u64) -> Result<ReplicaChanges> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let meta = meta_in(&tx)?;
        let since = id_to_sql(revision)?;
        let updated = {
            let mut statement = tx.prepare(&format!(
                "SELECT {ITEM_COLUMNS} FROM replica_items WHERE revision > ?1 ORDER BY revision, uid"
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
