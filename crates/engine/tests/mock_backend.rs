#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cascade_engine::backend::{Backend, BackendError, TransientError};
use cascade_engine::testing::{Fault, MockBackend, MockOp};
use cascade_engine::types::FileId;

fn file_id(entry: &cascade_engine::types::FileEntry) -> FileId {
    FileId(entry.id.0.clone())
}

#[tokio::test]
async fn revisions_keep_the_stable_id_across_rename_update_and_replace() {
    let mock = MockBackend::new(7);
    let original = mock.add_file("docs/report.txt", b"version one");
    let temp = mock.add_file("docs/.cascade-tmp-report.txt", b"version two, longer");

    let first = mock.revision(&original).await.unwrap();
    assert_eq!(first.size, Some(11));

    let renamed = mock
        .move_entry(Path::new("docs/report.txt"), Path::new("docs/final.txt"))
        .await
        .unwrap();
    let after_rename = mock.revision(&renamed).await.unwrap();
    assert_eq!(after_rename.stable_id, first.stable_id);

    let updated = mock.update(&file_id(&renamed), b"edited").await.unwrap();
    let after_update = mock.revision(&updated).await.unwrap();
    assert_eq!(after_update.stable_id, first.stable_id);
    assert_eq!(after_update.size, Some(6));
    assert_ne!(after_update.last_write_time, after_rename.last_write_time);

    let temp_stable_id = mock.revision(&temp).await.unwrap().stable_id;
    assert_ne!(temp_stable_id, first.stable_id);
    let (replaced, revision) = mock
        .replace(
            Path::new("docs/.cascade-tmp-report.txt"),
            Path::new("docs/final.txt"),
        )
        .await
        .unwrap();
    assert_eq!(revision.stable_id, temp_stable_id);
    assert_eq!(revision.size, Some(19));
    assert_eq!(
        mock.download(&replaced).await.unwrap(),
        b"version two, longer"
    );
    let source_gone = mock
        .metadata(Path::new("docs/.cascade-tmp-report.txt"))
        .await
        .unwrap_err();
    assert!(matches!(
        source_gone.downcast_ref::<BackendError>(),
        Some(BackendError::NotFound(_))
    ));
}

#[tokio::test]
async fn in_place_edit_moves_change_time_only_and_parent_change_time_follows_membership() {
    let mock = MockBackend::new(5);
    let photo = mock.add_file("docs/photo.raw", b"aaaa");
    let docs = mock.metadata(Path::new("docs")).await.unwrap();
    let photo_before = mock.revision(&photo).await.unwrap();
    let docs_before = mock.revision(&docs).await.unwrap();

    mock.rewrite_keeping_size_and_mtime("docs/photo.raw", b"bbbb")
        .unwrap();
    let photo_after = mock.revision(&photo).await.unwrap();
    assert_eq!(photo_after.size, photo_before.size);
    assert_eq!(photo_after.last_write_time, photo_before.last_write_time);
    assert_ne!(photo_after.change_time, photo_before.change_time);
    assert_eq!(mock.download(&photo).await.unwrap(), b"bbbb");
    assert_eq!(
        mock.revision(&docs).await.unwrap().change_time,
        docs_before.change_time,
        "a child content edit leaves the parent's change time"
    );

    let mut previous = docs_before.change_time;
    let mut assert_parent_moved = async |step: &str| {
        let now = mock.revision(&docs).await.unwrap().change_time;
        assert_ne!(now, previous, "{step} must move the parent's change time");
        previous = now;
    };
    mock.add_folder("docs/inner").unwrap();
    assert_parent_moved("a child folder create").await;
    mock.rename("docs/photo.raw", "docs/picture.raw").unwrap();
    assert_parent_moved("a child rename").await;
    mock.remove("docs/picture.raw").unwrap();
    assert_parent_moved("a child delete").await;
}

#[tokio::test(start_paused = true)]
async fn injected_faults_hit_one_call_and_stop_points_hold_the_remote_unchanged() {
    let mock = Arc::new(MockBackend::new(3));
    let target = mock.add_file("a.txt", b"old");
    mock.add_file("tmp-a.txt", b"new");

    mock.inject(MockOp::Download, Fault::Unreachable);
    let err = mock.download(&target).await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<TransientError>(),
        Some(TransientError::Unreachable(_))
    ));
    assert_eq!(mock.download(&target).await.unwrap(), b"old");

    mock.inject(MockOp::Replace, Fault::Busy);
    let err = mock
        .replace(Path::new("tmp-a.txt"), Path::new("a.txt"))
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<TransientError>(),
        Some(TransientError::Busy(_))
    ));
    assert_eq!(mock.download(&target).await.unwrap(), b"old");
    mock.metadata(Path::new("tmp-a.txt")).await.unwrap();

    mock.inject(MockOp::Metadata, Fault::Delay(Duration::from_secs(30)));
    let started = tokio::time::Instant::now();
    mock.metadata(Path::new("a.txt")).await.unwrap();
    assert!(started.elapsed() >= Duration::from_secs(30));

    let stop = mock.stop_at(MockOp::Replace);
    let replacing = {
        let mock = Arc::clone(&mock);
        tokio::spawn(async move {
            mock.replace(Path::new("tmp-a.txt"), Path::new("a.txt"))
                .await
                .unwrap()
        })
    };
    stop.reached().await;
    let held = mock.metadata(Path::new("a.txt")).await.unwrap();
    assert_eq!(mock.download(&held).await.unwrap(), b"old");
    stop.release();
    let (replaced, _) = replacing.await.unwrap();
    assert_eq!(mock.download(&replaced).await.unwrap(), b"new");
}

async fn completion_order(seed: u64) -> Vec<usize> {
    let mock = Arc::new(MockBackend::new(seed));
    let entries: Vec<_> = (0..5)
        .map(|index| mock.add_file(&format!("f{index}"), &[u8::try_from(index).unwrap()]))
        .collect();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = tokio::task::JoinSet::new();
    for (index, entry) in entries.into_iter().enumerate() {
        let mock = Arc::clone(&mock);
        let order = Arc::clone(&order);
        tasks.spawn(async move {
            let data = mock.download(&entry).await.unwrap();
            assert_eq!(data, [u8::try_from(index).unwrap()]);
            order.lock().unwrap().push(index);
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }
    Arc::try_unwrap(order).unwrap().into_inner().unwrap()
}

#[tokio::test]
async fn reply_order_is_reproducible_per_seed_and_varies_across_seeds() {
    let mut distinct = BTreeSet::new();
    for seed in 0..16 {
        let first = completion_order(seed).await;
        let second = completion_order(seed).await;
        assert_eq!(first, second, "seed {seed} reordered differently on replay");
        distinct.insert(first);
    }
    assert!(
        distinct.len() > 1,
        "every seed replied in the same order: {distinct:?}"
    );
}
