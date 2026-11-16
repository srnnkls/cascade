#![cfg(all(feature = "native", unix))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cascade_engine::backend::Backend;
use cascade_engine::content::replica::is_temp_upload;
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::testing::{
    BackendLog, Daemon, Disk, Fault, MockBackend, MockOp, RecordingBackend,
};

const SEEDS: [u64; 4] = [3, 17, 29, 64];
const TARGET: &str = "docs/report.txt";
/// Longer than any write-back delay plus a few backoff rounds capped at five
/// minutes.
const SETTLE: Duration = Duration::from_mins(30);

fn recording(share: &Arc<MockBackend>) -> Arc<RecordingBackend> {
    RecordingBackend::new(share, &BackendLog::default())
}

fn freezing_after(share: &Arc<MockBackend>, op: MockOp) -> Arc<RecordingBackend> {
    RecordingBackend::freezing_after(share, &BackendLog::default(), op)
}

/// A daemon's on-disk state under a temporary root.
fn disk() -> (tempfile::TempDir, Disk) {
    let root = tempfile::tempdir().unwrap();
    let disk = Disk::at(root.path(), TransferConfig::default()).unwrap();
    (root, disk)
}

async fn start(disk: &Disk, backend: Arc<RecordingBackend>) -> Daemon {
    disk.start(backend).await.unwrap()
}

async fn uid_of(daemon: &Daemon, path: &str) -> u64 {
    daemon
        .resolve(path)
        .await
        .unwrap()
        .expect("seeded file resolves")
}

/// Saves `data` as the system does: the content URL is unlinked once the
/// save returns. Returns the content version the reply carries.
async fn save(daemon: &Daemon, uid: u64, data: &[u8]) -> String {
    let incoming = daemon.incoming_file(data).unwrap();
    let item = daemon.cache.save(uid, &incoming).await.unwrap();
    let _ = std::fs::remove_file(&incoming);
    assert!(!item.uploaded, "a save replies before its upload");
    assert_eq!(
        daemon
            .cache
            .read(uid, 0, u32::try_from(data.len()).unwrap())
            .await
            .unwrap(),
        data,
        "reads of a saved item serve the unuploaded edit"
    );
    item.content_version
}

fn assert_clean(daemon: &Daemon, uid: u64) {
    let dirty: Vec<_> = daemon
        .db
        .dirty_content_objects()
        .unwrap()
        .into_iter()
        .filter(|object| object.uid == uid)
        .collect();
    assert!(dirty.is_empty(), "object still dirty: {dirty:?}");
    assert!(daemon.store.item(uid).unwrap().unwrap().uploaded);
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap()
}

async fn remote_bytes(share: &MockBackend, path: &str) -> Vec<u8> {
    let entry = share.metadata(Path::new(path)).await.unwrap();
    share.download(&entry).await.unwrap()
}

async fn remote_names(share: &MockBackend, folder: &str) -> Vec<String> {
    let folder = share.metadata(Path::new(folder)).await.unwrap();
    let mut names: Vec<String> = share
        .list_children(folder.id.native_id())
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    names.sort();
    names
}

fn name_of(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}

/// Each upload goes to a `.cascade-tmp-*` name beside the target and is
/// renamed over it by exactly one replace.
fn assert_uploaded_through_temp_names(recording: &RecordingBackend, expected: &[&[u8]]) {
    let uploads = recording.log().uploads();
    let replaces: Vec<(PathBuf, PathBuf)> = recording
        .log()
        .placed()
        .into_iter()
        .map(|placed| (placed.source, placed.target))
        .collect();
    let contents: Vec<&[u8]> = uploads.iter().map(|u| u.data.as_slice()).collect();
    assert_eq!(contents, expected, "one upload per final content");
    assert_eq!(replaces.len(), uploads.len(), "one replace per upload");
    for (upload, (source, target)) in uploads.iter().zip(&replaces) {
        assert!(
            is_temp_upload(&name_of(&upload.path)),
            "uploaded to {}, not a temporary name",
            upload.path.display()
        );
        assert_eq!(upload.path.parent(), Path::new(TARGET).parent());
        assert_eq!(source, &upload.path);
        assert_eq!(target, Path::new(TARGET));
    }
}

#[tokio::test(start_paused = true)]
async fn successive_saves_are_owned_before_the_reply_and_uploaded_once_per_final_content() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file(TARGET, b"original");
        share.add_file("docs/paused.txt", b"original");
        let recording = recording(&share);
        let (_root, disk) = disk();
        let daemon = start(&disk, recording.clone()).await;
        let uid = uid_of(&daemon, TARGET).await;
        daemon.cache.fetch(uid, None).await.unwrap();
        let fetches_after_open = recording.log().fetched().len();

        let first = save(&daemon, uid, b"first save").await;
        tokio::time::sleep(Duration::from_mins(1)).await;
        assert_eq!(remote_bytes(&share, TARGET).await, b"first save", "{seed}");
        assert_eq!(
            daemon.store.item(uid).unwrap().unwrap().content_version,
            first,
            "{seed}: the upload must keep the save's content version"
        );

        let second = save(&daemon, uid, b"second save, longer").await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            remote_bytes(&share, TARGET).await,
            b"second save, longer",
            "{seed}"
        );
        assert_uploaded_through_temp_names(&recording, &[b"first save", b"second save, longer"]);
        assert_eq!(
            daemon.store.item(uid).unwrap().unwrap().content_version,
            second,
            "{seed}"
        );
        assert_eq!(
            recording.log().fetched().len(),
            fetches_after_open,
            "{seed}: the item was downloaded again after its own upload"
        );
        assert_clean(&daemon, uid);
        assert_eq!(
            remote_names(&share, "docs").await,
            ["paused.txt", "report.txt"],
            "{seed}"
        );

        let paused = uid_of(&daemon, "docs/paused.txt").await;
        let uploads_before = recording.log().uploads().len();
        for _ in 0..3 {
            share.inject(MockOp::Upload, Fault::Unreachable);
        }
        save(&daemon, paused, b"paused one").await;
        save(&daemon, paused, b"paused two").await;
        tokio::time::sleep(SETTLE).await;
        let paused_uploads: Vec<Vec<u8>> = recording
            .log()
            .uploads()
            .into_iter()
            .skip(uploads_before)
            .map(|upload| upload.data)
            .collect();
        assert_eq!(
            paused_uploads,
            [b"paused two".to_vec()],
            "{seed}: two saves while the NAS is paused make one upload"
        );
        assert_eq!(
            remote_bytes(&share, "docs/paused.txt").await,
            b"paused two",
            "{seed}"
        );
        assert_clean(&daemon, paused);
    }
}

#[test]
fn a_restart_after_the_rename_marks_clean_without_a_second_upload() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file(TARGET, b"original");
        let (_root, disk) = disk();

        let killed = freezing_after(&share, MockOp::Replace);
        let (uid, temp_id) = runtime().block_on(async {
            let daemon = start(&disk, killed.clone()).await;
            let uid = uid_of(&daemon, TARGET).await;
            save(&daemon, uid, b"local edit").await;
            killed.frozen().await;
            let upload = killed.log().uploads().pop().unwrap();
            let objects = daemon.db.content_objects_of(uid).unwrap();
            let recorded = objects
                .iter()
                .find(|object| object.dirty)
                .and_then(|object| object.upload.clone())
                .expect("a dirty object records its upload before the rename");
            assert_eq!(recorded.temp_name, name_of(&upload.path), "{seed}");
            assert_eq!(recorded.file_id, upload.file_id, "{seed}");
            (uid, upload.file_id)
        });
        assert_eq!(
            runtime().block_on(remote_bytes(&share, TARGET)),
            b"local edit"
        );

        let restarted = recording(&share);
        runtime().block_on(async {
            let daemon = start(&disk, restarted.clone()).await;
            tokio::time::sleep(SETTLE).await;
            assert!(
                restarted.log().uploads().is_empty(),
                "{seed}: a finished rename was uploaded again"
            );
            assert_clean(&daemon, uid);
            let item = daemon.store.item(uid).unwrap().unwrap();
            assert_eq!(item.stable_id, temp_id, "{seed}: uid not re-linked");
            assert_eq!(remote_bytes(&share, TARGET).await, b"local edit");
            assert_eq!(remote_names(&share, "docs").await, ["report.txt"]);
        });
    }
}

#[test]
fn a_restart_after_a_kill_during_the_temporary_upload_removes_it_before_the_first_scan() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file(TARGET, b"original");
        let (_root, disk) = disk();

        let killed = freezing_after(&share, MockOp::Upload);
        let (uid, orphan) = runtime().block_on(async {
            let daemon = start(&disk, killed.clone()).await;
            let uid = uid_of(&daemon, TARGET).await;
            save(&daemon, uid, b"local edit").await;
            killed.frozen().await;
            (uid, killed.log().uploads().pop().unwrap().path)
        });
        assert!(is_temp_upload(&name_of(&orphan)), "{orphan:?}");

        let restarted = recording(&share);
        runtime().block_on(async {
            let held = share.stop_at(MockOp::Upload);
            let daemon = start(&disk, restarted.clone()).await;
            assert!(
                share.metadata(&orphan).await.is_err(),
                "{seed}: {orphan:?} survived start-up recovery"
            );
            let replica = daemon.store.replica(daemon.backend.clone()).unwrap();
            let folder = daemon.store.item(uid).unwrap().unwrap().parent_uid.unwrap();
            for item in replica.list(folder).await.unwrap() {
                assert!(!is_temp_upload(&item.name), "{seed}: listed {}", item.name);
            }
            held.release();
            tokio::time::sleep(SETTLE).await;

            assert_eq!(remote_bytes(&share, TARGET).await, b"local edit");
            assert_eq!(remote_names(&share, "docs").await, ["report.txt"]);
            assert_clean(&daemon, uid);
            for item in replica.list(folder).await.unwrap() {
                assert!(!is_temp_upload(&item.name), "{seed}: listed {}", item.name);
            }
        });
    }
}

#[derive(Debug, Clone, Copy)]
enum RemoteChange {
    RewrittenInPlace,
    ReplacedByRename,
    Deleted,
}

#[tokio::test(start_paused = true)]
async fn a_local_save_wins_over_a_remote_change_made_after_open() {
    for change in [
        RemoteChange::RewrittenInPlace,
        RemoteChange::ReplacedByRename,
        RemoteChange::Deleted,
    ] {
        for seed in SEEDS {
            let share = Arc::new(MockBackend::new(seed));
            share.add_file(TARGET, b"original");
            let recording = recording(&share);
            let (_root, disk) = disk();
            let daemon = start(&disk, recording.clone()).await;
            let uid = uid_of(&daemon, TARGET).await;
            daemon.cache.fetch(uid, None).await.unwrap();

            match change {
                RemoteChange::RewrittenInPlace => {
                    share.add_file(TARGET, b"remote edit");
                }
                RemoteChange::ReplacedByRename => {
                    share.add_file("docs/.report.txt.sb-save", b"remote edit");
                    share.remove(TARGET).unwrap();
                    share.rename("docs/.report.txt.sb-save", TARGET).unwrap();
                }
                RemoteChange::Deleted => share.remove(TARGET).unwrap(),
            }

            save(&daemon, uid, b"local edit").await;
            tokio::time::sleep(SETTLE).await;

            assert_eq!(
                remote_bytes(&share, TARGET).await,
                b"local edit",
                "{change:?} {seed}"
            );
            assert_eq!(
                remote_names(&share, "docs").await,
                ["report.txt"],
                "{change:?} {seed}: another file was created"
            );
            assert_uploaded_through_temp_names(&recording, &[b"local edit"]);
            assert_clean(&daemon, uid);
        }
    }
}
