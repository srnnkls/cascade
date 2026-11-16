#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::ops::Range;

use chrono::DateTime;

use super::{ContentState, EvictionCandidate, NewContentObject, UploadRecord};
use crate::content::ObjectId;
use crate::db::StateDb;
use crate::types::{FileEntry, ItemId, Revision};

fn revision(stable_id: &str) -> Revision {
    Revision {
        stable_id: stable_id.to_owned(),
        size: Some(100),
        last_write_time: Some(DateTime::from_timestamp_nanos(1_700_000_000_123_456_789)),
        change_time: Some(DateTime::from_timestamp_nanos(1_700_000_001_000_000_100)),
    }
}

fn insert(db: &StateDb, uid: u64, size: u64) -> ObjectId {
    let source = revision(&format!("stable-{uid}"));
    db.insert_content_object(&NewContentObject {
        uid,
        content_version: "v1",
        source_revision: &source,
        size,
    })
    .unwrap()
}

fn add_item(db: &StateDb, uid: u64, pinned: bool) {
    let conn = db.conn.lock().unwrap();
    conn.execute(
        "INSERT INTO replica_items (uid, backend_id, stable_id, native_id, name, kind,
             content_version, metadata_version, pinned, revision)
         VALUES (?1, 'nas', ?1, ?1, 'f', 'file', 'v1', 'm1', ?2, 1)",
        rusqlite::params![i64::try_from(uid).unwrap(), pinned],
    )
    .unwrap();
}

/// Accepts a save of `size` bytes held by `object` into the item `uid`.
fn save(db: &StateDb, uid: u64, object: ObjectId, size: u64) -> String {
    add_item(db, uid, false);
    let (saved, _) = db
        .accept_content_save(uid, object, size, DateTime::from_timestamp_nanos(5))
        .unwrap();
    saved.content_version
}

fn uploaded(native_id: &str) -> (FileEntry, Revision) {
    let entry = FileEntry::file(
        ItemId::new("nas", native_id),
        ItemId::new("nas", "root"),
        "f".to_owned(),
    );
    (entry, revision(native_id))
}

fn pin(db: &StateDb, uid: u64) {
    add_item(db, uid, true);
}

#[test]
fn inserted_object_round_trips_as_partial_and_clean() {
    let db = StateDb::open_in_memory().unwrap();
    let id = insert(&db, 3, 100);

    let object = db.content_object(id).unwrap().unwrap();
    assert_eq!(object.uid, 3);
    assert_eq!(object.content_version, "v1");
    assert_eq!(object.source_revision, revision("stable-3"));
    assert_eq!(object.size, 100);
    assert_eq!(object.state, ContentState::Partial);
    assert!(!object.dirty);
    assert_eq!(object.upload, None);
    assert_eq!(object.last_access, None);

    assert_eq!(
        db.content_object(ObjectId::new(id.get() + 1)).unwrap(),
        None
    );
    let second = insert(&db, 3, 50);
    let of_item: Vec<ObjectId> = db
        .content_objects_of(3)
        .unwrap()
        .iter()
        .map(|o| o.object_id)
        .collect();
    assert_eq!(of_item, [id, second]);
    db.mark_content_complete(id).unwrap();
    assert_eq!(
        db.content_object(id).unwrap().unwrap().state,
        ContentState::Complete
    );
}

#[test]
fn ranges_merge_on_insert_and_count_populated_bytes_once() {
    let db = StateDb::open_in_memory().unwrap();
    let id = insert(&db, 1, 100);
    let other = insert(&db, 2, 100);

    db.add_content_range(id, 20..30).unwrap();
    db.add_content_range(id, 0..10).unwrap();
    db.add_content_range(id, 10..15).unwrap();
    db.add_content_range(id, 25..40).unwrap();
    db.add_content_range(id, 50..60).unwrap();
    db.add_content_range(id, 45..55).unwrap();
    db.add_content_range(id, 70..70).unwrap();
    db.add_content_range(other, 0..5).unwrap();

    assert_eq!(db.content_ranges(id).unwrap(), [0..15, 20..40, 45..60]);
    assert_eq!(
        db.content_ranges(other).unwrap(),
        [Range { start: 0, end: 5 }]
    );
    assert_eq!(db.content_populated_bytes().unwrap(), 15 + 20 + 15 + 5);

    db.delete_content_object(id).unwrap();
    assert_eq!(db.content_object(id).unwrap(), None);
    assert!(db.content_ranges(id).unwrap().is_empty());
    assert_eq!(db.content_populated_bytes().unwrap(), 5);
}

#[test]
fn write_back_state_records_upload_before_clean() {
    let db = StateDb::open_in_memory().unwrap();
    let id = insert(&db, 1, 100);
    let untouched = insert(&db, 2, 100);

    let version = save(&db, 1, id, 120);
    let upload = UploadRecord {
        temp_name: ".cascade-tmp-1".to_owned(),
        file_id: "file-77".to_owned(),
    };
    db.record_content_upload(id, &upload).unwrap();

    let dirty = db.dirty_content_objects().unwrap();
    assert_eq!(dirty.len(), 1);
    let object = dirty.first().unwrap();
    assert_eq!(object.object_id, id);
    assert!(object.dirty);
    assert_eq!(object.content_version, version);
    assert_eq!(object.size, 120);
    assert_eq!(object.upload.as_ref(), Some(&upload));

    let (entry, file) = uploaded("file-77");
    assert!(db.finish_content_upload(1, id, &entry, &file).unwrap());
    let clean = db.content_object(id).unwrap().unwrap();
    assert!(!clean.dirty);
    assert_eq!(clean.upload, None);
    assert_eq!(clean.content_version, version);
    assert!(db.dirty_content_objects().unwrap().is_empty());
    assert!(!db.content_object(untouched).unwrap().unwrap().dirty);

    assert!(
        !db.finish_content_upload(1, ObjectId::new(untouched.get() + 1), &entry, &file)
            .unwrap()
    );
}

#[test]
fn eviction_candidates_are_clean_unpinned_and_least_recent_first() {
    let db = StateDb::open_in_memory().unwrap();
    let recent = insert(&db, 1, 100);
    let old = insert(&db, 2, 100);
    let never = insert(&db, 3, 100);
    let dirty = insert(&db, 4, 100);
    let pinned = insert(&db, 5, 100);

    db.persist_content_recency(&[
        (recent, DateTime::from_timestamp_nanos(2_000)),
        (old, DateTime::from_timestamp_nanos(1_000)),
    ])
    .unwrap();
    db.add_content_range(old, 0..10).unwrap();
    db.add_content_range(old, 5..30).unwrap();
    save(&db, 4, dirty, 100);
    pin(&db, 5);

    let candidates = db.content_eviction_candidates().unwrap();
    let order: Vec<(ObjectId, u64)> = candidates
        .iter()
        .map(
            |EvictionCandidate {
                 object,
                 populated_bytes,
             }| (object.object_id, *populated_bytes),
        )
        .collect();
    assert_eq!(order, [(never, 0), (old, 30), (recent, 0)]);
    assert!(!order.iter().any(|(id, _)| *id == pinned));
    assert_eq!(
        candidates.last().unwrap().object.last_access,
        Some(DateTime::from_timestamp_nanos(2_000))
    );
}

#[test]
fn materialized_set_is_replaced_whole() {
    let db = StateDb::open_in_memory().unwrap();
    assert!(db.materialized_items().unwrap().is_empty());

    db.replace_materialized_items(&[9, 3, 3, 5]).unwrap();
    assert_eq!(db.materialized_items().unwrap(), [3, 5, 9]);

    db.replace_materialized_items(&[5]).unwrap();
    assert_eq!(db.materialized_items().unwrap(), [5]);

    db.replace_materialized_items(&[]).unwrap();
    assert!(db.materialized_items().unwrap().is_empty());
}
