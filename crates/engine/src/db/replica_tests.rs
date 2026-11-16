#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::DateTime;

use super::ItemKind;
use crate::content::ObjectId;
use crate::db::StateDb;
use crate::db::content::NewContentObject;
use crate::types::{FileEntry, ItemId, Revision};

const BACKEND: &str = "nas";

fn listed(
    native_id: &str,
    name: &str,
    is_dir: bool,
    size: u64,
    ctime: i64,
) -> (FileEntry, Revision) {
    let id = ItemId::new(BACKEND, native_id);
    let parent = ItemId::new(BACKEND, "root");
    let mut entry = if is_dir {
        FileEntry::dir(id, parent, name.to_owned())
    } else {
        FileEntry::file(id, parent, name.to_owned())
    };
    let mtime = DateTime::from_timestamp_nanos(1_700_000_000_000_000_000);
    entry.size = (!is_dir).then_some(size);
    entry.mod_time = Some(mtime);
    let revision = Revision {
        stable_id: format!("stable-{native_id}"),
        size: entry.size,
        last_write_time: Some(mtime),
        change_time: Some(DateTime::from_timestamp_nanos(ctime)),
    };
    (entry, revision)
}

fn dirty(db: &StateDb, uid: u64) -> ObjectId {
    let source = Revision {
        stable_id: String::new(),
        size: None,
        last_write_time: None,
        change_time: None,
    };
    let object = db
        .insert_content_object(&NewContentObject {
            uid,
            content_version: "local",
            source_revision: &source,
            size: 1,
        })
        .unwrap();
    db.mark_content_dirty(object, "local", 1).unwrap();
    object
}

#[test]
fn root_is_registered_once_per_backend() {
    let db = StateDb::open_in_memory().unwrap();
    let root = db.ensure_replica_root(BACKEND, "root").unwrap();
    assert_eq!(db.ensure_replica_root(BACKEND, "root").unwrap(), root);
    let item = db.replica_item(root).unwrap().unwrap();
    assert_eq!((item.kind, item.parent_uid), (ItemKind::Folder, None));
    assert_eq!(db.replica_meta().unwrap().revision, 1);
}

#[test]
fn upsert_follows_stable_id_and_mints_versions_only_for_content_changes() {
    let db = StateDb::open_in_memory().unwrap();
    let root = db.ensure_replica_root(BACKEND, "root").unwrap();
    let uids = db
        .upsert_replica_children(BACKEND, root, &[listed("a", "a.txt", false, 3, 10)])
        .unwrap();
    let before = db.replica_item(uids[0]).unwrap().unwrap();
    let revision = db.replica_meta().unwrap().revision;

    let same = db
        .upsert_replica_children(BACKEND, root, &[listed("a", "a.txt", false, 3, 10)])
        .unwrap();
    assert_eq!(same, uids);
    assert_eq!(
        db.replica_meta().unwrap().revision,
        revision,
        "no change, no revision"
    );

    db.upsert_replica_children(BACKEND, root, &[listed("a", "b.txt", false, 3, 20)])
        .unwrap();
    let renamed = db.replica_item(uids[0]).unwrap().unwrap();
    assert_eq!(renamed.name, "b.txt");
    assert_eq!(renamed.content_version, before.content_version);
    assert_ne!(renamed.metadata_version, before.metadata_version);

    db.upsert_replica_children(BACKEND, root, &[listed("a", "b.txt", false, 3, 30)])
        .unwrap();
    let edited = db.replica_item(uids[0]).unwrap().unwrap();
    assert_ne!(edited.content_version, renamed.content_version);
    assert_eq!(
        edited.remote_revision.change_time,
        Some(DateTime::from_timestamp_nanos(30))
    );
}

#[test]
fn dirty_items_follow_name_only_and_survive_removal() {
    let db = StateDb::open_in_memory().unwrap();
    let root = db.ensure_replica_root(BACKEND, "root").unwrap();
    let uids = db
        .upsert_replica_children(BACKEND, root, &[listed("a", "a.txt", false, 3, 10)])
        .unwrap();
    dirty(&db, uids[0]);
    let before = db.replica_item(uids[0]).unwrap().unwrap();

    db.upsert_replica_children(BACKEND, root, &[listed("a", "c.txt", false, 9, 40)])
        .unwrap();
    let after = db.replica_item(uids[0]).unwrap().unwrap();
    assert_eq!(after.name, "c.txt");
    assert_eq!(after.content_version, before.content_version);
    assert_eq!(after.remote_revision, before.remote_revision);

    assert!(
        db.remove_replica_children_except(root, &[])
            .unwrap()
            .is_empty()
    );
    assert!(db.replica_item(uids[0]).unwrap().is_some());
}

#[test]
fn removal_keeps_nested_dirty_items_with_their_ancestors_and_tombstones_clean_siblings() {
    let db = StateDb::open_in_memory().unwrap();
    let root = db.ensure_replica_root(BACKEND, "root").unwrap();
    let gone = db
        .upsert_replica_children(BACKEND, root, &[listed("g", "Gone", true, 0, 10)])
        .unwrap()[0];
    let level_one = db
        .upsert_replica_children(
            BACKEND,
            gone,
            &[
                listed("d1", "dirty-1.txt", false, 1, 10),
                listed("c1", "clean-1.txt", false, 1, 10),
                listed("s", "Sub", true, 0, 10),
            ],
        )
        .unwrap();
    let (dirty_one, clean_one, sub) = (level_one[0], level_one[1], level_one[2]);
    let level_two = db
        .upsert_replica_children(
            BACKEND,
            sub,
            &[
                listed("d2", "dirty-2.txt", false, 1, 10),
                listed("c2", "clean-2.txt", false, 1, 10),
            ],
        )
        .unwrap();
    let (dirty_two, clean_two) = (level_two[0], level_two[1]);
    dirty(&db, dirty_one);
    dirty(&db, dirty_two);

    let mut deleted = db.remove_replica_children_except(root, &[]).unwrap();
    deleted.sort_unstable();
    let mut expected = vec![clean_one, clean_two];
    expected.sort_unstable();
    assert_eq!(deleted, expected);
    for uid in [gone, sub, dirty_one, dirty_two] {
        assert!(db.replica_item(uid).unwrap().is_some(), "uid {uid} removed");
    }
    assert_eq!(
        db.replica_item(dirty_two).unwrap().unwrap().parent_uid,
        Some(sub)
    );
    assert_eq!(
        db.replica_item(sub).unwrap().unwrap().parent_uid,
        Some(gone)
    );
}

#[test]
fn removal_tombstones_descendants_and_pruning_raises_the_floor() {
    let db = StateDb::open_in_memory().unwrap();
    let root = db.ensure_replica_root(BACKEND, "root").unwrap();
    let folder = db
        .upsert_replica_children(BACKEND, root, &[listed("d", "Docs", true, 0, 10)])
        .unwrap()[0];
    let file = db
        .upsert_replica_children(BACKEND, folder, &[listed("f", "f.txt", false, 1, 10)])
        .unwrap()[0];
    let anchor = db.replica_meta().unwrap().revision;

    let mut deleted = db.remove_replica_children_except(root, &[]).unwrap();
    deleted.sort_unstable();
    assert_eq!(deleted, [folder, file]);
    let changes = db.replica_changes_since(anchor).unwrap();
    assert!(changes.updated.is_empty());
    assert_eq!(changes.deleted, [folder, file]);

    let next = db
        .upsert_replica_children(BACKEND, root, &[listed("g", "g.txt", false, 1, 10)])
        .unwrap()[0];
    assert!(next > file, "uids are not reused");

    db.prune_replica_tombstones(changes.meta.revision).unwrap();
    let meta = db.replica_meta().unwrap();
    assert_eq!(meta.floor, changes.meta.revision);
    assert!(db.replica_changes_since(anchor).unwrap().deleted.is_empty());
}

#[test]
fn scan_set_holds_listed_and_cached_folders_parents_first() {
    let db = StateDb::open_in_memory().unwrap();
    let root = db.ensure_replica_root(BACKEND, "root").unwrap();
    let docs = db
        .upsert_replica_children(BACKEND, root, &[listed("d", "Docs", true, 0, 10)])
        .unwrap()[0];
    let inner = db
        .upsert_replica_children(BACKEND, docs, &[listed("i", "Inner", true, 0, 10)])
        .unwrap()[0];
    let cached = db
        .upsert_replica_children(BACKEND, docs, &[listed("c", "c.txt", false, 1, 10)])
        .unwrap()[0];
    assert!(db.replica_scan_folders(BACKEND).unwrap().is_empty());

    db.mark_replica_listed(inner, DateTime::from_timestamp_nanos(1))
        .unwrap();
    dirty(&db, cached);
    assert_eq!(db.replica_scan_folders(BACKEND).unwrap(), [docs, inner]);
    assert_eq!(
        db.replica_child(docs, "c.txt")
            .unwrap()
            .map(|item| item.uid),
        Some(cached)
    );
}
