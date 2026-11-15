//! Content index: queries over `content_objects`, `content_ranges` and
//! `materialized_items`.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::MutexGuard;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Row, params};

use super::StateDb;
use crate::content::ObjectId;
use crate::content::range_map::RangeMap;
use crate::types::Revision;

const OBJECT_COLUMNS: &str = "o.object_id, o.uid, o.content_version, o.source_stable_id, \
     o.source_size, o.source_mtime, o.source_ctime, o.size, o.cache_path, o.state, o.dirty, \
     o.upload_temp_name, o.upload_file_id, o.last_access";

const POPULATED_BYTES: &str = "COALESCE((SELECT SUM(r.\"end\" - r.start) FROM content_ranges r \
     WHERE r.object_id = o.object_id), 0)";

/// Fill state of a cached object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentState {
    /// Some ranges are still missing.
    Partial,
    /// Every byte of the object is present.
    Complete,
}

impl ContentState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Partial => "partial",
            Self::Complete => "complete",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "partial" => Some(Self::Partial),
            "complete" => Some(Self::Complete),
            _ => None,
        }
    }
}

/// An upload in flight: the temporary name it is written to and the file id
/// the backend assigned to it, recorded before the rename over the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRecord {
    /// Temporary name the upload is written to (`.cascade-tmp-*`).
    pub temp_name: String,
    /// Backend file id of the uploaded temporary file.
    pub file_id: String,
}

/// Fields of a cached object at creation.
#[derive(Debug, Clone, Copy)]
pub struct NewContentObject<'a> {
    /// Replica item the object caches.
    pub uid: u64,
    /// Content version the object holds.
    pub content_version: &'a str,
    /// Backend revision read when the download started.
    pub source_revision: &'a Revision,
    /// Logical size of the object in bytes.
    pub size: u64,
    /// Path of the sparse cache file.
    pub cache_path: &'a Path,
}

/// One cached object and its write-back state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentObject {
    /// Object id, also the name of its cache file in the object store.
    pub object_id: ObjectId,
    /// Replica item the object caches.
    pub uid: u64,
    /// Content version the object holds.
    pub content_version: String,
    /// Backend revision read when the download started.
    pub source_revision: Revision,
    /// Logical size of the object in bytes.
    pub size: u64,
    /// Path of the sparse cache file.
    pub cache_path: PathBuf,
    /// Fill state.
    pub state: ContentState,
    /// Whether the object holds an edit not yet uploaded.
    pub dirty: bool,
    /// Upload recorded before its rename, if any.
    pub upload: Option<UploadRecord>,
    /// Recency persisted by the eviction sweep.
    pub last_access: Option<DateTime<Utc>>,
}

/// A clean, unpinned object and the bytes it populates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionCandidate {
    /// The cached object.
    pub object: ContentObject,
    /// Bytes covered by its fetched ranges, each counted once.
    pub populated_bytes: u64,
}

pub(super) fn id_to_sql(id: u64) -> Result<i64> {
    i64::try_from(id).map_err(|_| anyhow!("id {id} exceeds the SQLite integer range"))
}

fn object_id_to_sql(id: ObjectId) -> Result<i64> {
    id_to_sql(id.get())
}

fn object_id_column(row: &Row<'_>, idx: usize) -> rusqlite::Result<ObjectId> {
    u64_column(row, idx).map(ObjectId::new)
}

pub(super) fn u64_column(row: &Row<'_>, idx: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(idx)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(idx, value))
}

pub(super) fn optional_u64_column(row: &Row<'_>, idx: usize) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(idx)?
        .map(|value| {
            u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(idx, value))
        })
        .transpose()
}

pub(super) fn time_to_sql(time: Option<DateTime<Utc>>) -> Option<i64> {
    time.and_then(|t| t.timestamp_nanos_opt())
}

pub(super) fn time_column(row: &Row<'_>, idx: usize) -> rusqlite::Result<Option<DateTime<Utc>>> {
    Ok(row
        .get::<_, Option<i64>>(idx)?
        .map(DateTime::from_timestamp_nanos))
}

fn object_from_row(row: &Row<'_>) -> rusqlite::Result<ContentObject> {
    let state: String = row.get(9)?;
    let temp_name: Option<String> = row.get(11)?;
    let file_id: Option<String> = row.get(12)?;
    Ok(ContentObject {
        object_id: object_id_column(row, 0)?,
        uid: u64_column(row, 1)?,
        content_version: row.get(2)?,
        source_revision: Revision {
            stable_id: row.get(3)?,
            size: optional_u64_column(row, 4)?,
            last_write_time: time_column(row, 5)?,
            change_time: time_column(row, 6)?,
        },
        size: u64_column(row, 7)?,
        cache_path: PathBuf::from(row.get::<_, String>(8)?),
        state: ContentState::parse(&state).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Text,
                format!("unknown content state {state:?}").into(),
            )
        })?,
        dirty: row.get(10)?,
        upload: temp_name
            .zip(file_id)
            .map(|(temp_name, file_id)| UploadRecord { temp_name, file_id }),
        last_access: time_column(row, 13)?,
    })
}

impl StateDb {
    pub(super) fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|e| anyhow!("lock poisoned: {e}"))
    }

    fn update_object(&self, sql: &str, params: impl rusqlite::Params) -> Result<()> {
        let conn = self.lock_conn()?;
        let changed = conn.execute(sql, params)?;
        if changed == 0 {
            return Err(anyhow!("no content object matched"));
        }
        Ok(())
    }

    /// Insert a partial, clean object and return its object id.
    pub fn insert_content_object(&self, object: &NewContentObject<'_>) -> Result<ObjectId> {
        let conn = self.lock_conn()?;
        let revision = object.source_revision;
        conn.execute(
            "INSERT INTO content_objects (uid, content_version, source_stable_id, source_size,
                 source_mtime, source_ctime, size, cache_path, state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                id_to_sql(object.uid)?,
                object.content_version,
                revision.stable_id,
                revision.size.map(id_to_sql).transpose()?,
                time_to_sql(revision.last_write_time),
                time_to_sql(revision.change_time),
                id_to_sql(object.size)?,
                object.cache_path.to_string_lossy(),
                ContentState::Partial.as_str(),
            ],
        )?;
        u64::try_from(conn.last_insert_rowid())
            .map(ObjectId::new)
            .map_err(|e| anyhow!("object id: {e}"))
    }

    /// Return the object with the given id.
    pub fn content_object(&self, object_id: ObjectId) -> Result<Option<ContentObject>> {
        let conn = self.lock_conn()?;
        Ok(conn
            .query_row(
                &format!("SELECT {OBJECT_COLUMNS} FROM content_objects o WHERE o.object_id = ?1"),
                [object_id_to_sql(object_id)?],
                object_from_row,
            )
            .optional()?)
    }

    /// Return the objects caching the given item, oldest first.
    pub fn content_objects_of(&self, uid: u64) -> Result<Vec<ContentObject>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {OBJECT_COLUMNS} FROM content_objects o WHERE o.uid = ?1 ORDER BY o.object_id"
        ))?;
        let objects = statement
            .query_map([id_to_sql(uid)?], object_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(objects)
    }

    /// Return every dirty object, for rebuilding the upload timers at start.
    pub fn dirty_content_objects(&self) -> Result<Vec<ContentObject>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {OBJECT_COLUMNS} FROM content_objects o WHERE o.dirty ORDER BY o.object_id"
        ))?;
        let objects = statement
            .query_map([], object_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(objects)
    }

    /// Record a fetched range, merging it with overlapping and adjacent ranges.
    pub fn add_content_range(&self, object_id: ObjectId, range: Range<u64>) -> Result<()> {
        if range.is_empty() {
            return Ok(());
        }
        let object = object_id_to_sql(object_id)?;
        let start = id_to_sql(range.start)?;
        let end = id_to_sql(range.end)?;
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let mut merged = RangeMap::default();
        {
            let mut touching = tx.prepare(
                "SELECT start, \"end\" FROM content_ranges
                 WHERE object_id = ?1 AND start <= ?3 AND \"end\" >= ?2",
            )?;
            let rows = touching.query_map(params![object, start, end], |row| {
                Ok(u64_column(row, 0)?..u64_column(row, 1)?)
            })?;
            for row in rows {
                merged.insert(row?);
            }
        }
        merged.insert(range);
        tx.execute(
            "DELETE FROM content_ranges WHERE object_id = ?1 AND start <= ?3 AND \"end\" >= ?2",
            params![object, start, end],
        )?;
        {
            let mut insert = tx.prepare(
                "INSERT INTO content_ranges (object_id, start, \"end\") VALUES (?1, ?2, ?3)",
            )?;
            for stored in merged.ranges() {
                insert.execute(params![
                    object,
                    id_to_sql(stored.start)?,
                    id_to_sql(stored.end)?
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Return the fetched ranges of an object, merged and in offset order.
    pub fn content_ranges(&self, object_id: ObjectId) -> Result<Vec<Range<u64>>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare(
            "SELECT start, \"end\" FROM content_ranges WHERE object_id = ?1 ORDER BY start",
        )?;
        let ranges = statement
            .query_map([object_id_to_sql(object_id)?], |row| {
                Ok(u64_column(row, 0)?..u64_column(row, 1)?)
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(ranges)
    }

    /// Mark an object as holding every byte.
    pub fn mark_content_complete(&self, object_id: ObjectId) -> Result<()> {
        self.update_object(
            "UPDATE content_objects SET state = ?2 WHERE object_id = ?1",
            params![
                object_id_to_sql(object_id)?,
                ContentState::Complete.as_str()
            ],
        )
    }

    /// Mark an object as holding an accepted save with the given content
    /// version and size.
    pub fn mark_content_dirty(
        &self,
        object_id: ObjectId,
        content_version: &str,
        size: u64,
    ) -> Result<()> {
        self.update_object(
            "UPDATE content_objects SET dirty = TRUE, content_version = ?2, size = ?3
             WHERE object_id = ?1",
            params![
                object_id_to_sql(object_id)?,
                content_version,
                id_to_sql(size)?
            ],
        )
    }

    /// Record the temporary name and file id of an upload before its rename.
    pub fn record_content_upload(&self, object_id: ObjectId, upload: &UploadRecord) -> Result<()> {
        self.update_object(
            "UPDATE content_objects SET upload_temp_name = ?2, upload_file_id = ?3
             WHERE object_id = ?1",
            params![
                object_id_to_sql(object_id)?,
                upload.temp_name,
                upload.file_id
            ],
        )
    }

    /// Mark an object clean and clear its upload record.
    pub fn mark_content_clean(&self, object_id: ObjectId) -> Result<()> {
        self.update_object(
            "UPDATE content_objects SET dirty = FALSE, upload_temp_name = NULL,
                 upload_file_id = NULL
             WHERE object_id = ?1",
            [object_id_to_sql(object_id)?],
        )
    }

    /// Persist the recency of an object.
    pub fn set_content_last_access(&self, object_id: ObjectId, at: DateTime<Utc>) -> Result<()> {
        self.update_object(
            "UPDATE content_objects SET last_access = ?2 WHERE object_id = ?1",
            params![object_id_to_sql(object_id)?, time_to_sql(Some(at))],
        )
    }

    /// Delete an object and its ranges.
    pub fn delete_content_object(&self, object_id: ObjectId) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "DELETE FROM content_objects WHERE object_id = ?1",
            [object_id_to_sql(object_id)?],
        )?;
        Ok(())
    }

    /// Return clean objects of unpinned items, least recently used first, with
    /// their populated bytes. Reader guards are not visible here; the caller
    /// skips objects that hold one.
    pub fn content_eviction_candidates(&self) -> Result<Vec<EvictionCandidate>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {OBJECT_COLUMNS}, {POPULATED_BYTES}
             FROM content_objects o LEFT JOIN replica_items i ON i.uid = o.uid
             WHERE NOT o.dirty AND NOT COALESCE(i.pinned, FALSE)
             ORDER BY o.last_access IS NOT NULL, o.last_access, o.object_id"
        ))?;
        let candidates = statement
            .query_map([], |row| {
                Ok(EvictionCandidate {
                    object: object_from_row(row)?,
                    populated_bytes: u64_column(row, 14)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(candidates)
    }

    /// Return the bytes populated across all objects, each counted once.
    pub fn content_populated_bytes(&self) -> Result<u64> {
        let conn = self.lock_conn()?;
        Ok(conn.query_row(
            "SELECT COALESCE(SUM(\"end\" - start), 0) FROM content_ranges",
            [],
            |row| u64_column(row, 0),
        )?)
    }

    /// Replace the set of items the system reports as materialised.
    pub fn replace_materialized_items(&self, uids: &[u64]) -> Result<()> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM materialized_items", [])?;
        {
            let mut insert =
                tx.prepare("INSERT OR IGNORE INTO materialized_items (uid) VALUES (?1)")?;
            for &uid in uids {
                insert.execute([id_to_sql(uid)?])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Return the materialised items in uid order.
    pub fn materialized_items(&self) -> Result<Vec<u64>> {
        let conn = self.lock_conn()?;
        let mut statement = conn.prepare("SELECT uid FROM materialized_items ORDER BY uid")?;
        let uids = statement
            .query_map([], |row| u64_column(row, 0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(uids)
    }
}

#[cfg(test)]
#[path = "content_tests.rs"]
mod tests;
