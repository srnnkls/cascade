#![cfg(feature = "native")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;

use cascade_engine::backend::Backend;
use cascade_engine::content::anchor::SyncAnchor;
use cascade_engine::content::replica::{Changes, ReplicaItem, ReplicaStore};
use cascade_engine::content::scan::scan;
use cascade_engine::db::StateDb;
use cascade_engine::db::content::NewContentObject;
use cascade_engine::testing::MockBackend;

const MAX_ANCHOR_BYTES: usize = 500;

fn uid_named(items: &[ReplicaItem], name: &str) -> u64 {
    items
        .iter()
        .find(|item| item.name == name)
        .unwrap_or_else(|| panic!("{name} missing from listing"))
        .uid
}

fn changes_since(store: &ReplicaStore, anchor: &SyncAnchor) -> (Vec<String>, Vec<u64>, Vec<u64>) {
    match store.changes(anchor).unwrap() {
        Changes::Since(set) => {
            let mut names: Vec<String> = set.updated.iter().map(|i| i.name.clone()).collect();
            names.sort();
            let mut uids: Vec<u64> = set.updated.iter().map(|i| i.uid).collect();
            uids.sort_unstable();
            let mut deleted = set.deleted;
            deleted.sort_unstable();
            (names, uids, deleted)
        }
        Changes::Expired => panic!("anchor from this store above the floor expired"),
    }
}

fn open_store() -> (Arc<StateDb>, ReplicaStore) {
    let db = Arc::new(StateDb::open_in_memory().unwrap());
    let store = ReplicaStore::open(Arc::clone(&db)).unwrap();
    (db, store)
}

#[tokio::test]
async fn scan_of_listed_folder_follows_nas_rename_by_id_and_tombstones_delete() {
    let share = Arc::new(MockBackend::new(11));
    share.add_file("Projects/Docs/report.txt", b"report v1");
    share.add_file("Projects/Docs/notes.txt", b"notes");
    share.add_file("Projects/Docs/.cascade-tmp-7", b"partial");
    let (_db, store) = open_store();
    let replica = store.replica(share.clone()).unwrap();

    let docs = replica
        .resolve(Path::new("Projects/Docs"))
        .await
        .unwrap()
        .expect("listed folder resolves");
    let projects = replica
        .resolve(Path::new("Projects"))
        .await
        .unwrap()
        .expect("ancestor registered by resolve");
    assert_eq!(
        store.item(docs).unwrap().unwrap().parent_uid,
        Some(projects)
    );

    let listed = replica.list(docs).await.unwrap();
    let mut names: Vec<&str> = listed.iter().map(|i| i.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["notes.txt", "report.txt"],
        "temporary upload names are skipped"
    );
    let report = uid_named(&listed, "report.txt");
    let notes = uid_named(&listed, "notes.txt");
    let report_version = store.item(report).unwrap().unwrap().content_version;
    let anchor = store.current_anchor().unwrap();

    share
        .rename("Projects/Docs/report.txt", "Projects/Docs/summary.txt")
        .unwrap();
    share.remove("Projects/Docs/notes.txt").unwrap();
    share.add_file("Projects/Docs/report.txt", b"another file");
    share.add_file("Projects/Docs/.cascade-tmp-8", b"partial");
    scan(&replica).await.unwrap();

    let renamed = store.item(report).unwrap().unwrap();
    assert_eq!(renamed.name, "summary.txt");
    assert_eq!(renamed.parent_uid, Some(docs));
    assert_eq!(
        renamed.content_version, report_version,
        "a rename must not look like a content change"
    );
    assert!(store.item(notes).unwrap().is_none());

    let new_report = replica
        .resolve(Path::new("Projects/Docs/report.txt"))
        .await
        .unwrap()
        .expect("new file at the old path");
    assert_ne!(new_report, report, "a uid must not derive from the path");
    assert_eq!(
        replica
            .resolve(Path::new("Projects/Docs/summary.txt"))
            .await
            .unwrap(),
        Some(report)
    );

    let (names, uids, deleted) = changes_since(&store, &anchor);
    assert_eq!(names, ["report.txt", "summary.txt"]);
    let mut expected = vec![report, new_report];
    expected.sort_unstable();
    assert_eq!(uids, expected);
    assert_eq!(deleted, [notes]);
}

#[tokio::test]
async fn scan_removes_gone_folder_whose_parent_was_never_listed() {
    let share = Arc::new(MockBackend::new(13));
    share.add_file("Projects/Docs/report.txt", b"report");
    share.add_file("Projects/keep.txt", b"keep");
    let (_db, store) = open_store();
    let replica = store.replica(share.clone()).unwrap();
    let docs = replica
        .resolve(Path::new("Projects/Docs"))
        .await
        .unwrap()
        .unwrap();
    let projects = store.item(docs).unwrap().unwrap().parent_uid.unwrap();
    let report = uid_named(&replica.list(docs).await.unwrap(), "report.txt");
    assert!(
        !store.item(projects).unwrap().unwrap().listed,
        "the parent is registered by resolve only"
    );
    let anchor = store.current_anchor().unwrap();

    share.remove("Projects/Docs").unwrap();
    assert!(scan(&replica).await.unwrap());

    assert!(store.item(docs).unwrap().is_none());
    assert!(store.item(report).unwrap().is_none());
    assert!(store.item(projects).unwrap().is_some());
    let (_, _, deleted) = changes_since(&store, &anchor);
    let mut expected = vec![docs, report];
    expected.sort_unstable();
    assert_eq!(deleted, expected);
}

#[tokio::test]
async fn anchors_from_another_store_or_below_the_floor_expire() {
    let share = Arc::new(MockBackend::new(17));
    share.add_file("Docs/a.txt", b"a");
    share.add_file("Docs/b.txt", b"b");
    let (_ours_db, ours_store) = open_store();
    let (_theirs_db, theirs_store) = open_store();
    let ours = ours_store.replica(share.clone()).unwrap();
    theirs_store.replica(share.clone()).unwrap();
    let docs = ours.resolve(Path::new("Docs")).await.unwrap().unwrap();
    let listed = ours.list(docs).await.unwrap();
    let a = uid_named(&listed, "a.txt");

    let before_delete = ours_store.current_anchor().unwrap();
    let encoded = before_delete.to_bytes();
    assert!(
        encoded.len() <= MAX_ANCHOR_BYTES,
        "anchor is {} bytes",
        encoded.len()
    );
    assert_eq!(SyncAnchor::from_bytes(&encoded).unwrap(), before_delete);
    assert!(
        matches!(
            theirs_store.changes(&before_delete).unwrap(),
            Changes::Expired
        ),
        "an anchor minted by another store must expire"
    );

    share.remove("Docs/a.txt").unwrap();
    scan(&ours).await.unwrap();
    let after_delete = ours_store.current_anchor().unwrap();
    let (_, _, deleted) = changes_since(&ours_store, &before_delete);
    assert_eq!(deleted, [a]);

    ours_store
        .prune_tombstones_through(after_delete.revision())
        .unwrap();
    assert!(
        matches!(
            ours_store.changes(&before_delete).unwrap(),
            Changes::Expired
        ),
        "an anchor below the tombstone floor must expire"
    );
    let (names, _, deleted) = changes_since(&ours_store, &after_delete);
    assert!(names.is_empty() && deleted.is_empty());
}

#[tokio::test]
async fn scan_mints_version_for_timestamp_preserving_edit_and_only_follows_dirty_items() {
    let share = Arc::new(MockBackend::new(19));
    let photo_entry = share.add_file("Docs/photo.raw", b"aaaa");
    let draft_entry = share.add_file("Docs/draft.txt", b"draft v1");
    let (db, store) = open_store();
    let replica = store.replica(share.clone()).unwrap();
    let docs = replica.resolve(Path::new("Docs")).await.unwrap().unwrap();
    let listed = replica.list(docs).await.unwrap();
    let photo = uid_named(&listed, "photo.raw");
    let draft = uid_named(&listed, "draft.txt");
    for (uid, native_id) in [
        (photo, photo_entry.id.native_id()),
        (draft, draft_entry.id.native_id()),
    ] {
        let item = store.item(uid).unwrap().unwrap();
        let entry = share
            .metadata(Path::new(&format!("Docs/{}", item.name)))
            .await
            .unwrap();
        let revision = share.revision(&entry).await.unwrap();
        assert_eq!(revision.stable_id, native_id);
        let object = db
            .insert_content_object(&NewContentObject {
                uid,
                content_version: &item.content_version,
                source_revision: &revision,
                size: entry.size.unwrap(),
            })
            .unwrap();
        db.mark_content_complete(object).unwrap();
        if uid == draft {
            db.accept_content_save(uid, object, 8, chrono::Utc::now())
                .unwrap();
        }
    }
    let photo_version = store.item(photo).unwrap().unwrap().content_version;
    let draft_version = store.item(draft).unwrap().unwrap().content_version;
    let anchor = store.current_anchor().unwrap();

    share
        .rewrite_keeping_size_and_mtime("Docs/photo.raw", b"bbbb")
        .unwrap();
    share.add_file("Docs/draft.txt", b"edited on the NAS meanwhile");
    share
        .rename("Docs/draft.txt", "Docs/draft-final.txt")
        .unwrap();
    scan(&replica).await.unwrap();

    let photo_after = store.item(photo).unwrap().unwrap();
    assert_ne!(
        photo_after.content_version, photo_version,
        "a moved change time with size and last-write time preserved is a content change"
    );
    let draft_after = store.item(draft).unwrap().unwrap();
    assert_eq!(draft_after.name, "draft-final.txt");
    assert_eq!(draft_after.parent_uid, Some(docs));
    assert_eq!(
        draft_after.content_version, draft_version,
        "a dirty item's content version stays with the local edit"
    );
    let (names, _, _) = changes_since(&store, &anchor);
    assert_eq!(names, ["draft-final.txt", "photo.raw"]);
}
