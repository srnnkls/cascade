//! TR-15 write-back regression suite on the seeded mock backend.
//!
//! A kill is a dropped current-thread runtime; the restart reopens the state
//! database, the replica store and the content cache from disk and runs
//! [`ContentCache::recover`]. Every row of [`TRANSITIONS`] has a kill test,
//! every pair of save, upload, scan, rename and delete has an overlap test,
//! and the cases of `resources/writeback-cases.md` and the write-back rows of
//! the design's Verification table follow.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{TRANSITIONS, Transition, WRITE_BACK_DELAY, WriteBackEvent as E, WriteBackState as S};
use crate::backend::{Backend, TransientError};
use crate::content::ObjectId;
use crate::content::replica::{ReplicaItem, is_temp_upload};
use crate::content::scan::scan;
use crate::content::transfer::TransferConfig;
use crate::db::content::ContentObject;
use crate::testing::{
    BackendLog, Daemon, Disk, Fault, MockBackend, MockOp, RecordingBackend as Recording, StopPoint,
    Upload,
};

const SEEDS: [u64; 4] = [3, 17, 29, 64];
const FOLDER: &str = "docs";
const TARGET: &str = "docs/report.txt";
const OTHER: &str = "docs/other.txt";
const ORIGINAL: &[u8] = b"original";
const EDIT: &[u8] = b"local edit";
const SECOND: &[u8] = b"second local edit, longer";
/// [`ORIGINAL`] with `XY` written at 2.
const WRITTEN: &[u8] = b"orXYinal";
/// [`WRITTEN`] with `QQ` written at 4.
const WRITTEN_TWICE: &[u8] = b"orXYQQal";
/// [`EDIT`] with `L` written at 0.
const EDIT_WRITTEN: &[u8] = b"Local edit";
const RENAMED_NAME: &str = "renamed.txt";
const RENAMED: &str = "docs/renamed.txt";
const ARCHIVE: &str = "archive";
const ARCHIVED: &str = "archive/report.txt";
/// Longer than any write-back delay plus several backoff rounds capped at
/// five minutes.
const SETTLE: Duration = Duration::from_mins(30);
/// Doubling backoff cap of FR-7.
const BACKOFF_CAP: Duration = Duration::from_mins(5);
/// Polls a held operation may take before another event runs.
const YIELDS: usize = 256;

fn uploads(log: &BackendLog) -> Vec<Upload> {
    log.uploads()
}

fn placed(log: &BackendLog) -> Vec<Vec<u8>> {
    log.placed().into_iter().map(|placed| placed.data).collect()
}

fn placed_count(log: &BackendLog, data: &[u8]) -> usize {
    placed(log).iter().filter(|placed| *placed == data).count()
}

fn calls(log: &BackendLog, op: MockOp) -> usize {
    log.calls(op)
}

impl Daemon {
    async fn uid(&self, path: &str) -> u64 {
        self.resolve(path)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{path} resolves"))
    }

    fn item(&self, uid: u64) -> Option<ReplicaItem> {
        self.store.item(uid).unwrap()
    }

    fn folder_uid(&self, uid: u64) -> u64 {
        self.item(uid).unwrap().parent_uid.unwrap()
    }

    /// Lists the item's folder as a presenter does, adding it to the scan
    /// set, and returns the listed names.
    async fn list_folder_of(&self, uid: u64) -> Vec<String> {
        let folder = self.folder_uid(uid);
        let mut names: Vec<String> = self
            .replica()
            .unwrap()
            .list(folder)
            .await
            .unwrap()
            .into_iter()
            .map(|item| item.name)
            .collect();
        names.sort();
        names
    }

    async fn scan(&self) {
        scan(&self.replica().unwrap()).await.unwrap();
    }

    /// Saves `data` as the system does and returns the reply's content
    /// version; the content URL is unlinked once the save returns.
    async fn save(&self, uid: u64, data: &[u8]) -> String {
        let incoming = self.incoming_file(data).unwrap();
        let item = self.cache.save(uid, &incoming).await.unwrap();
        std::fs::remove_file(&incoming).unwrap();
        assert!(!item.uploaded, "a save replies before its upload");
        item.content_version
    }

    /// Writes `data` at `offset` as an NFS WRITE does; the reply comes
    /// before any upload.
    async fn write(&self, uid: u64, offset: u64, data: &[u8]) {
        let item = self.cache.write_at(uid, offset, data).await.unwrap();
        assert!(!item.uploaded, "a write replies before its upload");
    }

    fn edited_object(&self, uid: u64) -> Option<ContentObject> {
        self.dirty_object(uid).filter(|object| object.edited)
    }

    fn spawn_save(&self, uid: u64, data: &[u8]) -> JoinHandle<anyhow::Result<ReplicaItem>> {
        let incoming = self.incoming_file(data).unwrap();
        let cache = self.cache.clone();
        tokio::spawn(async move { cache.save(uid, &incoming).await })
    }

    fn spawn_rename(&self, uid: u64, name: &str) -> JoinHandle<anyhow::Result<ReplicaItem>> {
        let cache = self.cache.clone();
        let folder = self.folder_uid(uid);
        let name = name.to_owned();
        tokio::spawn(async move { cache.rename(uid, folder, &name).await })
    }

    fn spawn_delete(&self, uid: u64) -> JoinHandle<anyhow::Result<()>> {
        let cache = self.cache.clone();
        tokio::spawn(async move { cache.delete(uid).await })
    }

    fn spawn_scan(&self) -> JoinHandle<anyhow::Result<bool>> {
        let replica = self.replica().unwrap();
        tokio::spawn(async move { scan(&replica).await })
    }

    fn dirty_objects(&self) -> Vec<ContentObject> {
        self.db.dirty_content_objects().unwrap()
    }

    fn dirty_object(&self, uid: u64) -> Option<ContentObject> {
        self.dirty_objects()
            .into_iter()
            .find(|object| object.uid == uid)
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap()
}

/// Lets spawned tasks run up to their next blocking point without moving
/// the paused clock.
async fn let_run() {
    for _ in 0..YIELDS {
        tokio::task::yield_now().await;
    }
}

/// Waits on the paused clock, in small steps, until `done` holds.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + SETTLE;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn remote_bytes(share: &MockBackend, path: &str) -> Option<Vec<u8>> {
    let entry = share.metadata(Path::new(path)).await.ok()?;
    Some(share.download(&entry).await.unwrap())
}

async fn remote_id(share: &MockBackend, path: &str) -> String {
    share
        .metadata(Path::new(path))
        .await
        .unwrap()
        .id
        .native_id()
        .to_owned()
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

fn name_of(path: &str) -> String {
    Path::new(path)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

struct Case {
    seed: u64,
    share: Arc<MockBackend>,
    disk: Disk,
    log: BackendLog,
    objects: PathBuf,
    _root: tempfile::TempDir,
}

impl Case {
    fn new(seed: u64) -> Self {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file(TARGET, ORIGINAL);
        let root = tempfile::tempdir().unwrap();
        Self {
            seed,
            share,
            disk: Disk::at(root.path(), TransferConfig::default()).unwrap(),
            log: BackendLog::default(),
            objects: root.path().join("cache"),
            _root: root,
        }
    }

    fn recording(&self) -> Arc<Recording> {
        Recording::new(&self.share, &self.log)
    }

    /// Starts a daemon and returns it with the target's uid, its folder
    /// listed.
    async fn start(&self, backend: Arc<Recording>) -> (Daemon, u64) {
        let daemon = self.disk.start(backend).await.unwrap();
        let uid = daemon.uid(TARGET).await;
        daemon.list_folder_of(uid).await;
        (daemon, uid)
    }

    /// Restarts after a kill, lets the write-back settle and checks the
    /// outcome every restart must reach.
    fn restart_and_settle(&self, uid: u64, outcome: &Outcome) {
        runtime().block_on(async {
            let daemon = self.disk.start(self.recording()).await.unwrap();
            tokio::time::sleep(SETTLE).await;
            self.assert_outcome(&daemon, uid, outcome).await;
        });
    }

    async fn assert_outcome(&self, daemon: &Daemon, uid: u64, outcome: &Outcome) {
        let seed = self.seed;
        assert!(
            daemon.dirty_objects().is_empty(),
            "{seed}: dirty objects left: {:?}",
            daemon.dirty_objects()
        );
        match outcome {
            Outcome::At { path, data } => {
                let folder = Path::new(path).parent().unwrap().to_str().unwrap();
                assert_eq!(
                    remote_names(&self.share, folder).await,
                    [name_of(path)],
                    "{seed}: the folder holds exactly the item, no temporary file or duplicate"
                );
                assert_eq!(
                    remote_bytes(&self.share, path).await.as_deref(),
                    Some(*data),
                    "{seed}: remote content at {path}"
                );
                let item = daemon
                    .item(uid)
                    .unwrap_or_else(|| panic!("{seed}: uid {uid} lost"));
                assert_eq!(item.name, name_of(path), "{seed}: item name");
                assert!(item.uploaded, "{seed}: item not marked uploaded");
                assert_eq!(
                    item.stable_id,
                    remote_id(&self.share, path).await,
                    "{seed}: uid not linked to the remote file"
                );
                assert_eq!(
                    daemon.list_folder_of(uid).await,
                    [name_of(path)],
                    "{seed}: listing"
                );
            }
            Outcome::Gone => {
                assert_eq!(
                    remote_names(&self.share, FOLDER).await,
                    Vec::<String>::new(),
                    "{seed}: the deleted file was recreated or a temporary file is left"
                );
                daemon.scan().await;
                assert!(daemon.item(uid).is_none(), "{seed}: deleted item listed");
            }
        }
    }
}

enum Outcome {
    /// The item is clean at `path` holding `data`, alone in its folder.
    At {
        path: &'static str,
        data: &'static [u8],
    },
    /// The item is gone from the backend and the replica.
    Gone,
}

const fn at(path: &'static str, data: &'static [u8]) -> Outcome {
    Outcome::At { path, data }
}

/// Saves `EDIT` and waits until its rename-over failed once with busy: the
/// record is committed, the temporary file exists and the item's operation
/// lock is free.
async fn into_recorded(case: &Case, daemon: &Daemon, uid: u64) {
    case.share.inject(MockOp::Replace, Fault::Busy);
    daemon.save(uid, EDIT).await;
    until("a busy rename-over", || {
        calls(&case.log, MockOp::Replace) >= 1
    })
    .await;
    let_run().await;
    let record = daemon
        .dirty_object(uid)
        .and_then(|object| object.upload)
        .expect("the record survives a failed rename-over");
    assert!(is_temp_upload(&record.temp_name), "{}", record.temp_name);
    assert_eq!(
        remote_bytes(&case.share, TARGET).await.as_deref(),
        Some(ORIGINAL),
        "{}: a failed rename-over touched the target",
        case.seed
    );
}

/// Saves `EDIT` and holds its temporary upload before it is applied.
async fn into_uploading(case: &Case, daemon: &Daemon, uid: u64) -> StopPoint {
    let held = case.share.stop_at(MockOp::Upload);
    daemon.save(uid, EDIT).await;
    held.reached().await;
    held
}

macro_rules! kill_tests {
    ($($test:ident: $from:ident $event:ident $to:ident => $scenario:ident;)*) => {
        fn kill_scenario(row: Transition) -> Option<fn(u64)> {
            match (row.from, row.event, row.to) {
                $((S::$from, E::$event, S::$to) => Some($scenario),)*
                _ => None,
            }
        }

        $(
            #[test]
            fn $test() {
                run_kill_row(Transition { from: S::$from, event: E::$event, to: S::$to }, $scenario);
            }
        )*
    };
}

fn run_kill_row(row: Transition, scenario: fn(u64)) {
    assert!(
        TRANSITIONS.contains(&row),
        "{row:?} is not a row of the transition table"
    );
    for seed in SEEDS {
        scenario(seed);
    }
}

#[test]
fn every_transition_has_a_kill_test() {
    let missing: Vec<&Transition> = TRANSITIONS
        .iter()
        .filter(|row| kill_scenario(**row).is_none())
        .collect();
    assert!(missing.is_empty(), "rows without a kill test: {missing:?}");
}

kill_tests! {
    kill_clean_save: Clean Save Dirty => clean_save;
    kill_clean_rename: Clean Rename Clean => clean_rename;
    kill_clean_delete: Clean Delete Deleting => clean_delete;
    kill_clean_reparent: Clean Reparent Clean => clean_reparent;
    kill_clean_scan: Clean Scan Clean => clean_scan;
    kill_clean_restart: Clean Restart Clean => clean_restart;
    kill_dirty_save: Dirty Save Dirty => dirty_save;
    kill_dirty_timer_due: Dirty TimerDue Uploading => dirty_timer_due;
    kill_dirty_rename: Dirty Rename Dirty => dirty_rename;
    kill_dirty_delete: Dirty Delete Deleting => dirty_delete;
    kill_dirty_reparent: Dirty Reparent Dirty => dirty_reparent;
    kill_dirty_scan: Dirty Scan Dirty => dirty_scan;
    kill_dirty_restart: Dirty Restart Dirty => dirty_restart;
    kill_clean_write: Clean Write Editing => clean_write;
    kill_dirty_write: Dirty Write Editing => dirty_write;
    kill_uploading_write: Uploading Write Editing => uploading_write;
    kill_recorded_write: Recorded Write Editing => recorded_write;
    kill_editing_write: Editing Write Editing => editing_write;
    kill_editing_save: Editing Save Dirty => editing_save;
    kill_editing_timer_due_uploading: Editing TimerDue Uploading => editing_timer_due_uploading;
    kill_editing_timer_due_editing: Editing TimerDue Editing => editing_timer_due_editing;
    kill_editing_rename: Editing Rename Editing => editing_rename;
    kill_editing_reparent: Editing Reparent Editing => editing_reparent;
    kill_editing_delete: Editing Delete Deleting => editing_delete;
    kill_editing_scan: Editing Scan Editing => editing_scan;
    kill_editing_restart: Editing Restart Editing => editing_restart;
    kill_deleting_delete_failed_editing: Deleting DeleteFailed Editing => deleting_delete_failed_editing;
    kill_uploading_save: Uploading Save Dirty => uploading_save;
    kill_uploading_upload_failed: Uploading UploadFailed Dirty => uploading_upload_failed;
    kill_uploading_temp_uploaded: Uploading TempUploaded Recorded => uploading_temp_uploaded;
    kill_uploading_rename: Uploading Rename Uploading => uploading_rename;
    kill_uploading_delete: Uploading Delete Deleting => uploading_delete;
    kill_uploading_reparent: Uploading Reparent Uploading => uploading_reparent;
    kill_uploading_scan: Uploading Scan Uploading => uploading_scan;
    kill_uploading_restart: Uploading Restart Dirty => uploading_restart;
    kill_recorded_save: Recorded Save Dirty => recorded_save;
    kill_recorded_rename_over: Recorded RenameOver Renamed => recorded_rename_over;
    kill_recorded_rename_failed: Recorded RenameFailed Recorded => recorded_rename_failed;
    kill_recorded_rename: Recorded Rename Recorded => recorded_rename;
    kill_recorded_delete: Recorded Delete Deleting => recorded_delete;
    kill_recorded_reparent: Recorded Reparent Dirty => recorded_reparent;
    kill_recorded_scan: Recorded Scan Recorded => recorded_scan;
    kill_recorded_restart: Recorded Restart Recorded => recorded_restart;
    kill_recorded_timer_due_renamed: Recorded TimerDue Renamed => recorded_timer_due_renamed;
    kill_recorded_timer_due_recorded: Recorded TimerDue Recorded => recorded_timer_due_recorded;
    kill_renamed_bookkeeping: Renamed Bookkeeping Clean => renamed_bookkeeping;
    kill_renamed_scan: Renamed Scan Renamed => renamed_scan;
    kill_renamed_restart: Renamed Restart Clean => renamed_restart;
    kill_deleting_remote_deleted: Deleting RemoteDeleted Deleted => deleting_remote_deleted;
    kill_deleting_delete_failed_clean: Deleting DeleteFailed Clean => deleting_delete_failed_clean;
    kill_deleting_delete_failed_dirty: Deleting DeleteFailed Dirty => deleting_delete_failed_dirty;
    kill_deleting_restart: Deleting Restart Deleted => deleting_restart;
    kill_deleted_restart: Deleted Restart Deleted => deleted_restart;
    kill_deleting_restart_clean: Deleting Restart Clean => deleting_restart_clean;
    kill_deleting_restart_dirty: Deleting Restart Dirty => deleting_restart_dirty;
    kill_deleting_restart_editing: Deleting Restart Editing => deleting_restart_editing;
    kill_deleting_delete_failed_recorded: Deleting DeleteFailed Recorded => deleting_delete_failed_recorded;
    kill_deleting_restart_recorded: Deleting Restart Recorded => deleting_restart_recorded;
}

fn clean_save(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.cache.fetch(uid, None).await.unwrap();
        daemon.save(uid, EDIT).await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}: FR-7 exactly once");
}

fn clean_rename(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let folder = daemon.folder_uid(uid);
        daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap();
        uid
    });
    case.restart_and_settle(uid, &at(RENAMED, ORIGINAL));
    assert!(uploads(&case.log).is_empty(), "{seed}: a rename uploaded");
}

fn clean_delete(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.cache.delete(uid).await.unwrap();
        uid
    });
    case.restart_and_settle(uid, &Outcome::Gone);
    assert!(uploads(&case.log).is_empty(), "{seed}");
}

fn clean_scan(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let before = daemon.item(uid).unwrap();
        case.share.rename(TARGET, RENAMED).unwrap();
        daemon.scan().await;
        let after = daemon.item(uid).expect("a remote rename keeps the uid");
        assert_eq!(after.name, RENAMED_NAME, "{seed}");
        assert_eq!(
            after.content_version, before.content_version,
            "{seed}: TR-4"
        );
        uid
    });
    case.restart_and_settle(uid, &at(RENAMED, ORIGINAL));
    assert!(uploads(&case.log).is_empty(), "{seed}");
}

fn clean_restart(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.cache.fetch(uid, None).await.unwrap();
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, ORIGINAL));
    assert!(
        uploads(&case.log).is_empty(),
        "{seed}: a clean object was uploaded"
    );
}

fn dirty_save(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.save(uid, EDIT).await;
        daemon.save(uid, SECOND).await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, SECOND));
    assert_eq!(placed(&case.log), [SECOND], "{seed}: AC-25");
    assert!(
        uploads(&case.log)
            .iter()
            .all(|upload| upload.data == SECOND),
        "{seed}: the superseded save was uploaded"
    );
}

fn dirty_timer_due(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_uploading(&case, &daemon, uid).await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

fn dirty_rename(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.save(uid, EDIT).await;
        let folder = daemon.folder_uid(uid);
        daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap();
        uid
    });
    case.restart_and_settle(uid, &at(RENAMED, EDIT));
    assert_eq!(
        placed(&case.log),
        [EDIT],
        "{seed}: the pending upload completes once"
    );
}

fn dirty_delete(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.save(uid, EDIT).await;
        daemon.cache.delete(uid).await.unwrap();
        uid
    });
    case.restart_and_settle(uid, &Outcome::Gone);
    assert!(
        placed(&case.log).is_empty(),
        "{seed}: a deleted dirty item was uploaded"
    );
}

fn dirty_scan(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let version = daemon.save(uid, EDIT).await;
        let before = daemon.item(uid).unwrap();
        case.share.rename(TARGET, RENAMED).unwrap();
        daemon.scan().await;
        let after = daemon.item(uid).expect("the dirty item keeps its uid");
        assert_eq!(
            after.name, RENAMED_NAME,
            "{seed}: name followed by stable id"
        );
        assert_eq!(
            after.content_version, version,
            "{seed}: scan minted a version"
        );
        assert_eq!(
            after.remote_revision, before.remote_revision,
            "{seed}: scan changed remote_revision of a dirty item"
        );
        assert!(daemon.dirty_object(uid).is_some(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(RENAMED, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

fn dirty_restart(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.save(uid, EDIT).await;
        uid
    });
    for _ in 0..3 {
        case.share.inject(MockOp::Upload, Fault::Unreachable);
    }
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}: AC-8");
}

fn uploading_save(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_uploading(&case, &daemon, uid).await;
        daemon.save(uid, SECOND).await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, SECOND));
    assert_eq!(placed(&case.log), [SECOND], "{seed}");
}

fn uploading_upload_failed(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        case.share.inject(MockOp::Upload, Fault::Unreachable);
        daemon.save(uid, EDIT).await;
        until("a failed upload", || calls(&case.log, MockOp::Upload) >= 1).await;
        let_run().await;
        assert!(
            daemon.dirty_object(uid).is_some(),
            "{seed}: a failed upload cleaned"
        );
        assert_eq!(
            remote_bytes(&case.share, TARGET).await.as_deref(),
            Some(ORIGINAL),
            "{seed}"
        );
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

/// Kill after the record was committed, before the rename-over: recovery
/// resumes the temporary file without a second upload.
fn uploading_temp_uploaded(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let held = case.share.stop_at(MockOp::Replace);
        daemon.save(uid, EDIT).await;
        held.reached().await;
        let upload = uploads(&case.log)
            .pop()
            .expect("the temporary upload applied");
        let record = daemon
            .dirty_object(uid)
            .and_then(|object| object.upload)
            .expect("TR-11: the record precedes the rename");
        assert_eq!(
            record.temp_name,
            upload.path.file_name().unwrap().to_string_lossy(),
            "{seed}"
        );
        assert_eq!(record.file_id, upload.file_id, "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
    assert_eq!(
        uploads(&case.log).len(),
        1,
        "{seed}: a recorded temporary file was uploaded again"
    );
}

fn uploading_rename(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_uploading(&case, &daemon, uid).await;
        let folder = daemon.folder_uid(uid);
        daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap();
        uid
    });
    case.restart_and_settle(uid, &at(RENAMED, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

fn uploading_delete(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_uploading(&case, &daemon, uid).await;
        daemon.cache.delete(uid).await.unwrap();
        uid
    });
    case.restart_and_settle(uid, &Outcome::Gone);
    assert!(placed(&case.log).is_empty(), "{seed}");
}

fn uploading_scan(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_uploading(&case, &daemon, uid).await;
        let version = daemon.item(uid).unwrap().content_version;
        daemon.scan().await;
        assert_eq!(daemon.item(uid).unwrap().content_version, version, "{seed}");
        assert_eq!(
            daemon.list_folder_of(uid).await,
            [name_of(TARGET)],
            "{seed}"
        );
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

/// Kill after the temporary file was written and before its record was
/// committed: recovery removes the orphan and the edit is uploaded again.
fn uploading_restart(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Upload);
        let (daemon, uid) = case.start(Arc::clone(&killed)).await;
        daemon.save(uid, EDIT).await;
        killed.frozen().await;
        uid
    });
    let orphan = uploads(&case.log).pop().unwrap().path;
    runtime().block_on(async {
        let held = case.share.stop_at(MockOp::Upload);
        let daemon = case.disk.start(case.recording()).await.unwrap();
        assert!(
            case.share.metadata(&orphan).await.is_err(),
            "{seed}: TR-12 orphan survived recovery"
        );
        held.release();
        tokio::time::sleep(SETTLE).await;
        case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
    });
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

fn recorded_save(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        daemon.save(uid, SECOND).await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, SECOND));
    assert_eq!(placed(&case.log), [SECOND], "{seed}");
}

fn recorded_rename_over(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Replace);
        let (daemon, uid) = case.start(Arc::clone(&killed)).await;
        daemon.save(uid, EDIT).await;
        killed.frozen().await;
        uid
    });
    let temp_id = uploads(&case.log).pop().unwrap().file_id;
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(uploads(&case.log).len(), 1, "{seed}: AC-19 second upload");
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
    runtime().block_on(async {
        let daemon = case.disk.open(case.recording()).unwrap();
        assert_eq!(
            daemon.item(uid).unwrap().stable_id,
            temp_id,
            "{seed}: uid not re-linked"
        );
    });
}

fn recorded_rename_failed(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        assert!(!daemon.item(uid).unwrap().uploaded, "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
    assert_eq!(
        uploads(&case.log).len(),
        1,
        "{seed}: recovery resumes the recorded temporary file"
    );
}

fn recorded_rename(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        let folder = daemon.folder_uid(uid);
        daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap();
        uid
    });
    case.restart_and_settle(uid, &at(RENAMED, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

fn recorded_delete(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        daemon.cache.delete(uid).await.unwrap();
        uid
    });
    case.restart_and_settle(uid, &Outcome::Gone);
    assert!(placed(&case.log).is_empty(), "{seed}");
}

fn recorded_scan(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        let before = daemon.item(uid).unwrap();
        daemon.scan().await;
        let after = daemon.item(uid).unwrap();
        assert_eq!(after.content_version, before.content_version, "{seed}");
        assert_eq!(after.remote_revision, before.remote_revision, "{seed}");
        assert_eq!(
            daemon.list_folder_of(uid).await,
            [name_of(TARGET)],
            "{seed}: TR-12 temporary file listed"
        );
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
    assert_eq!(uploads(&case.log).len(), 1, "{seed}");
}

fn recorded_restart(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let held = case.share.stop_at(MockOp::Replace);
        daemon.save(uid, EDIT).await;
        held.reached().await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
    assert_eq!(
        uploads(&case.log).len(),
        1,
        "{seed}: a recorded upload was repeated"
    );
}

/// A rename-over the backend applied whose reply was lost: the retry finds
/// the recorded file id under the target and settles without a second
/// upload or rename-over.
fn recorded_timer_due_renamed(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let backend = case.recording();
        backend.fail_reply_after(MockOp::Replace);
        let (daemon, uid) = case.start(backend).await;
        daemon.save(uid, EDIT).await;
        until("the retry settles the object", || {
            daemon.dirty_object(uid).is_none()
        })
        .await;
        assert_eq!(
            calls(&case.log, MockOp::Replace),
            1,
            "{seed}: second rename-over"
        );
        assert!(daemon.item(uid).unwrap().uploaded, "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(uploads(&case.log).len(), 1, "{seed}: second upload");
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
    assert_eq!(
        calls(&case.log, MockOp::Replace),
        1,
        "{seed}: second rename-over"
    );
}

/// A rename-over that failed leaves the temporary file: the retry renames it
/// over the target again without a second upload.
fn recorded_timer_due_recorded(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        until("the retry settles the object", || {
            daemon.dirty_object(uid).is_none()
        })
        .await;
        assert_eq!(
            calls(&case.log, MockOp::Replace),
            2,
            "{seed}: the rename-over is retried once"
        );
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(uploads(&case.log).len(), 1, "{seed}: second upload");
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

fn renamed_bookkeeping(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let version = daemon.save(uid, EDIT).await;
        until("the upload", || daemon.dirty_object(uid).is_none()).await;
        let item = daemon.item(uid).unwrap();
        assert!(item.uploaded, "{seed}");
        assert_eq!(
            item.content_version, version,
            "{seed}: upload minted a version"
        );
        let target = case.share.metadata(Path::new(TARGET)).await.unwrap();
        let revision = case.share.revision(&target).await.unwrap();
        assert_eq!(
            item.remote_revision, revision,
            "{seed}: remote_revision not updated"
        );
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(uploads(&case.log).len(), 1, "{seed}");
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

/// Case 18: a scan between the rename-over and the bookkeeping keeps the
/// uid and does not turn the renamed file into a second item.
fn renamed_scan(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Replace);
        let (daemon, uid) = case.start(Arc::clone(&killed)).await;
        let version = daemon.save(uid, EDIT).await;
        killed.frozen().await;
        daemon.scan().await;
        let folder = daemon.folder_uid(uid);
        let items = daemon.db.replica_children(folder).unwrap();
        let named: Vec<u64> = items
            .iter()
            .filter(|item| item.name == name_of(TARGET))
            .map(|item| item.uid)
            .collect();
        assert_eq!(named, [uid], "{seed}: the renamed file became another item");
        assert_eq!(daemon.item(uid).unwrap().content_version, version, "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(uploads(&case.log).len(), 1, "{seed}: second upload");
}

/// Kill after the rename-over with the backend unreachable at the first
/// recovery call: the object still becomes clean without a second upload.
fn renamed_restart(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Replace);
        let (daemon, uid) = case.start(Arc::clone(&killed)).await;
        daemon.save(uid, EDIT).await;
        killed.frozen().await;
        uid
    });
    case.share.inject(MockOp::Metadata, Fault::Unreachable);
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(uploads(&case.log).len(), 1, "{seed}: second upload");
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

/// Case 4: a kill after the backend delete succeeded and before the replica
/// item was removed does not recreate the file.
fn deleted_restart(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Delete);
        let (daemon, uid) = case.start(Arc::clone(&killed)).await;
        daemon.save(uid, EDIT).await;
        let deleting = daemon.spawn_delete(uid);
        killed.frozen().await;
        assert!(!deleting.is_finished());
        uid
    });
    case.restart_and_settle(uid, &Outcome::Gone);
    assert!(placed(&case.log).is_empty(), "{seed}");
}

/// Runs one overlap scenario per seed on a live daemon and checks the
/// settled outcome.
fn overlap(scenario: impl AsyncFn(&Case, &Daemon, u64) -> Outcome) {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            let outcome = scenario(&case, &daemon, uid).await;
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &outcome).await;
        });
    }
}

#[test]
fn overlap_save_save() {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            let first = daemon.spawn_save(uid, EDIT);
            let second = daemon.spawn_save(uid, SECOND);
            let first = first.await.unwrap().unwrap();
            let second = second.await.unwrap().unwrap();
            assert_ne!(first.content_version, second.content_version, "{seed}");
            let current = daemon.item(uid).unwrap().content_version;
            let (winner, loser): (&'static [u8], &'static [u8]) =
                if current == second.content_version {
                    (SECOND, EDIT)
                } else {
                    assert_eq!(current, first.content_version, "{seed}");
                    (EDIT, SECOND)
                };
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &at(TARGET, winner)).await;
            assert_eq!(placed_count(&case.log, winner), 1, "{seed}");
            assert_eq!(placed_count(&case.log, loser), 0, "{seed}");
        });
    }
}

#[test]
fn overlap_save_upload_save_during_temporary_upload() {
    overlap(async |case, daemon, uid| {
        let held = into_uploading(case, daemon, uid).await;
        daemon.save(uid, SECOND).await;
        held.release();
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            placed(&case.log),
            [SECOND],
            "{}: superseded upload placed",
            case.seed
        );
        at(TARGET, SECOND)
    });
}

#[test]
fn overlap_save_upload_save_after_rename_over_issued() {
    overlap(async |case, daemon, uid| {
        let held = case.share.stop_at(MockOp::Replace);
        daemon.save(uid, EDIT).await;
        held.reached().await;
        let saving = daemon.spawn_save(uid, SECOND);
        let_run().await;
        held.release();
        let saved = saving.await.unwrap().unwrap();
        assert!(!saved.uploaded, "{}", case.seed);
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            placed(&case.log),
            [EDIT, SECOND],
            "{}: second upload",
            case.seed
        );
        at(TARGET, SECOND)
    });
}

#[test]
fn overlap_save_scan() {
    overlap(async |case, daemon, uid| {
        daemon.cache.fetch(uid, None).await.unwrap();
        let held = case.share.stop_at(MockOp::ListChildren);
        let scanning = daemon.spawn_scan();
        held.reached().await;
        let version = daemon.save(uid, EDIT).await;
        held.release();
        scanning.await.unwrap().unwrap();
        assert_eq!(
            daemon.item(uid).unwrap().content_version,
            version,
            "{}: scan replaced the save's version",
            case.seed
        );
        assert!(daemon.dirty_object(uid).is_some(), "{}", case.seed);
        let version_before_rescan = daemon.item(uid).unwrap().content_version;
        let held = case.share.stop_at(MockOp::Upload);
        held.reached().await;
        let scanning = daemon.spawn_scan();
        scanning.await.unwrap().unwrap();
        assert_eq!(
            daemon.item(uid).unwrap().content_version,
            version_before_rescan,
            "{}",
            case.seed
        );
        held.release();
        at(TARGET, EDIT)
    });
}

#[test]
fn overlap_save_rename() {
    overlap(async |case, daemon, uid| {
        let held = case.share.stop_at(MockOp::Move);
        let renaming = daemon.spawn_rename(uid, RENAMED_NAME);
        held.reached().await;
        let saving = daemon.spawn_save(uid, EDIT);
        let_run().await;
        held.release();
        renaming.await.unwrap().unwrap();
        saving.await.unwrap().unwrap();
        tokio::time::sleep(SETTLE).await;
        assert_eq!(placed(&case.log), [EDIT], "{}", case.seed);
        at(RENAMED, EDIT)
    });
}

#[test]
fn overlap_save_delete() {
    overlap(async |case, daemon, uid| {
        daemon.cache.fetch(uid, None).await.unwrap();
        let held = case.share.stop_at(MockOp::Delete);
        let deleting = daemon.spawn_delete(uid);
        held.reached().await;
        let saving = daemon.spawn_save(uid, EDIT);
        let_run().await;
        held.release();
        deleting.await.unwrap().unwrap();
        let _ = saving.await.unwrap();
        tokio::time::sleep(SETTLE).await;
        assert!(
            placed(&case.log).is_empty(),
            "{}: a delete was undone",
            case.seed
        );
        Outcome::Gone
    });
}

#[test]
fn overlap_upload_upload() {
    for seed in SEEDS {
        let case = Case::new(seed);
        case.share.add_file(OTHER, ORIGINAL);
        runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            let other = daemon.uid(OTHER).await;
            let held = case.share.stop_at(MockOp::Upload);
            daemon.save(uid, EDIT).await;
            daemon.save(other, SECOND).await;
            held.reached().await;
            until("one of two uploads", || {
                daemon.dirty_object(uid).is_none() || daemon.dirty_object(other).is_none()
            })
            .await;
            held.release();
            tokio::time::sleep(SETTLE).await;
            assert!(daemon.dirty_objects().is_empty(), "{seed}");
            assert_eq!(
                remote_names(&case.share, FOLDER).await,
                [name_of(OTHER), name_of(TARGET)],
                "{seed}"
            );
            assert_eq!(
                remote_bytes(&case.share, TARGET).await.as_deref(),
                Some(EDIT)
            );
            assert_eq!(
                remote_bytes(&case.share, OTHER).await.as_deref(),
                Some(SECOND)
            );
            assert_eq!(placed_count(&case.log, EDIT), 1, "{seed}");
            assert_eq!(placed_count(&case.log, SECOND), 1, "{seed}");
        });
    }
}

#[test]
fn overlap_upload_scan() {
    overlap(async |case, daemon, uid| {
        let held = into_uploading(case, daemon, uid).await;
        let version = daemon.item(uid).unwrap().content_version;
        daemon.scan().await;
        held.release();
        let held = case.share.stop_at(MockOp::Replace);
        held.reached().await;
        daemon.scan().await;
        assert_eq!(
            daemon.list_folder_of(uid).await,
            [name_of(TARGET)],
            "{}: temporary file listed",
            case.seed
        );
        assert_eq!(
            daemon.item(uid).unwrap().content_version,
            version,
            "{}",
            case.seed
        );
        held.release();
        at(TARGET, EDIT)
    });
}

#[test]
fn overlap_upload_rename_during_temporary_upload() {
    overlap(async |case, daemon, uid| {
        let held = into_uploading(case, daemon, uid).await;
        let folder = daemon.folder_uid(uid);
        daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap();
        held.release();
        tokio::time::sleep(SETTLE).await;
        assert_eq!(placed(&case.log), [EDIT], "{}", case.seed);
        at(RENAMED, EDIT)
    });
}

#[test]
fn overlap_upload_rename_during_rename_over() {
    overlap(async |case, daemon, uid| {
        let held = case.share.stop_at(MockOp::Replace);
        daemon.save(uid, EDIT).await;
        held.reached().await;
        let renaming = daemon.spawn_rename(uid, RENAMED_NAME);
        let_run().await;
        held.release();
        renaming.await.unwrap().unwrap();
        tokio::time::sleep(SETTLE).await;
        assert_eq!(placed_count(&case.log, EDIT), 1, "{}", case.seed);
        at(RENAMED, EDIT)
    });
}

#[test]
fn overlap_upload_rename_held_while_upload_starts() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::Move);
        let renaming = daemon.spawn_rename(uid, RENAMED_NAME);
        held.reached().await;
        until("the temporary upload", || !uploads(&case.log).is_empty()).await;
        held.release();
        renaming.await.unwrap().unwrap();
        tokio::time::sleep(SETTLE).await;
        assert_eq!(placed_count(&case.log, EDIT), 1, "{}", case.seed);
        at(RENAMED, EDIT)
    });
}

#[test]
fn overlap_upload_delete_during_temporary_upload() {
    overlap(async |case, daemon, uid| {
        let held = into_uploading(case, daemon, uid).await;
        daemon.cache.delete(uid).await.unwrap();
        held.release();
        tokio::time::sleep(SETTLE).await;
        assert!(placed(&case.log).is_empty(), "{}", case.seed);
        Outcome::Gone
    });
}

#[test]
fn overlap_upload_delete_during_rename_over() {
    overlap(async |case, daemon, uid| {
        let held = case.share.stop_at(MockOp::Replace);
        daemon.save(uid, EDIT).await;
        held.reached().await;
        let deleting = daemon.spawn_delete(uid);
        let_run().await;
        held.release();
        deleting.await.unwrap().unwrap();
        Outcome::Gone
    });
}

#[test]
fn overlap_upload_delete_held_while_upload_runs() {
    overlap(async |case, daemon, uid| {
        let upload = into_uploading(case, daemon, uid).await;
        let held = case.share.stop_at(MockOp::Delete);
        let deleting = daemon.spawn_delete(uid);
        held.reached().await;
        upload.release();
        let_run().await;
        held.release();
        deleting.await.unwrap().unwrap();
        Outcome::Gone
    });
}

#[test]
fn overlap_scan_scan() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        case.share.rename(TARGET, RENAMED).unwrap();
        let held = case.share.stop_at(MockOp::ListChildren);
        let first = daemon.spawn_scan();
        held.reached().await;
        daemon.scan().await;
        held.release();
        first.await.unwrap().unwrap();
        let folder = daemon.folder_uid(uid);
        let uids: Vec<u64> = daemon
            .db
            .replica_children(folder)
            .unwrap()
            .into_iter()
            .map(|item| item.uid)
            .collect();
        assert_eq!(
            uids,
            [uid],
            "{}: concurrent scans split the item",
            case.seed
        );
        at(RENAMED, EDIT)
    });
}

#[test]
fn overlap_scan_rename() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::ListChildren);
        let scanning = daemon.spawn_scan();
        held.reached().await;
        let folder = daemon.folder_uid(uid);
        daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap();
        held.release();
        scanning.await.unwrap().unwrap();

        let held = case.share.stop_at(MockOp::Move);
        let renaming = daemon.spawn_rename(uid, "third.txt");
        held.reached().await;
        daemon.scan().await;
        held.release();
        renaming.await.unwrap().unwrap();
        daemon.scan().await;
        assert_eq!(daemon.item(uid).unwrap().name, "third.txt", "{}", case.seed);
        at("docs/third.txt", EDIT)
    });
}

#[test]
fn overlap_scan_delete() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::ListChildren);
        let scanning = daemon.spawn_scan();
        held.reached().await;
        daemon.cache.delete(uid).await.unwrap();
        held.release();
        scanning.await.unwrap().unwrap();
        assert!(
            daemon.item(uid).is_none(),
            "{}: scan resurrected",
            case.seed
        );
        Outcome::Gone
    });
}

#[test]
fn overlap_scan_delete_held() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::Delete);
        let deleting = daemon.spawn_delete(uid);
        held.reached().await;
        daemon.scan().await;
        held.release();
        deleting.await.unwrap().unwrap();
        daemon.scan().await;
        assert!(daemon.item(uid).is_none(), "{}", case.seed);
        let folder = daemon
            .replica()
            .unwrap()
            .resolve(Path::new(FOLDER))
            .await
            .unwrap()
            .unwrap();
        assert!(
            daemon.db.replica_children(folder).unwrap().is_empty(),
            "{}: the deleted file is listed under another uid",
            case.seed
        );
        Outcome::Gone
    });
}

#[test]
fn overlap_rename_rename() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::Move);
        let first = daemon.spawn_rename(uid, RENAMED_NAME);
        held.reached().await;
        let second = daemon.spawn_rename(uid, "third.txt");
        let_run().await;
        held.release();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(
            daemon.item(uid).unwrap().name,
            "third.txt",
            "{}: the later rename applies last",
            case.seed
        );
        at("docs/third.txt", EDIT)
    });
}

#[test]
fn overlap_rename_delete() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::Move);
        let renaming = daemon.spawn_rename(uid, RENAMED_NAME);
        held.reached().await;
        let deleting = daemon.spawn_delete(uid);
        let_run().await;
        held.release();
        renaming.await.unwrap().unwrap();
        deleting.await.unwrap().unwrap();
        Outcome::Gone
    });
}

#[test]
fn overlap_rename_delete_held() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::Delete);
        let deleting = daemon.spawn_delete(uid);
        held.reached().await;
        let renaming = daemon.spawn_rename(uid, RENAMED_NAME);
        let_run().await;
        held.release();
        deleting.await.unwrap().unwrap();
        assert!(
            renaming.await.unwrap().is_err(),
            "{}: a rename of a deleted item succeeded",
            case.seed
        );
        Outcome::Gone
    });
}

#[test]
fn overlap_delete_delete() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::Delete);
        let first = daemon.spawn_delete(uid);
        held.reached().await;
        let second = daemon.spawn_delete(uid);
        let_run().await;
        held.release();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        Outcome::Gone
    });
}

/// A delete arriving between the rename-over and the clean bookkeeping
/// deletes the file for good.
#[test]
fn case_delete_between_rename_over_and_bookkeeping() {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let backend = case.recording();
            let after = backend.pause_after(MockOp::Replace);
            let (daemon, uid) = case.start(backend).await;
            daemon.save(uid, EDIT).await;
            after.reached.notified().await;
            let deleting = daemon.spawn_delete(uid);
            let_run().await;
            after.released.notify_one();
            deleting.await.unwrap().unwrap();
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &Outcome::Gone).await;
        });
    }
}

/// A scan between the rename-over and the bookkeeping, without a kill.
#[test]
fn case_scan_between_rename_over_and_bookkeeping() {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let backend = case.recording();
            let after = backend.pause_after(MockOp::Replace);
            let (daemon, uid) = case.start(backend).await;
            daemon.save(uid, EDIT).await;
            after.reached.notified().await;
            daemon.scan().await;
            let folder = daemon.folder_uid(uid);
            let uids: Vec<u64> = daemon
                .db
                .replica_children(folder)
                .unwrap()
                .into_iter()
                .map(|item| item.uid)
                .collect();
            assert_eq!(uids, [uid], "{seed}: the renamed file became another item");
            after.released.notify_one();
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
            assert_eq!(uploads(&case.log).len(), 1, "{seed}");
        });
    }
}

/// A dirty item renamed on the remote is uploaded to its new path with no
/// duplicate at the old one.
#[test]
fn case_remote_rename_of_a_dirty_item() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        case.share.rename(TARGET, RENAMED).unwrap();
        tokio::time::sleep(SETTLE).await;
        assert_eq!(placed(&case.log), [EDIT], "{}", case.seed);
        at(RENAMED, EDIT)
    });
}

/// The parent folder of a dirty item deleted on the remote: folder and file
/// are recreated.
#[test]
fn case_remote_delete_of_the_parent_folder() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        case.share.remove(FOLDER).unwrap();
        daemon.scan().await;
        assert!(
            daemon.item(uid).is_some(),
            "{}: dirty item dropped by scan",
            case.seed
        );
        at(TARGET, EDIT)
    });
}

/// Another client replaces the target by rename between the temporary
/// upload and the rename-over: the local edit wins and the uid re-links to
/// the daemon's file id.
#[test]
fn case_target_replaced_between_temporary_upload_and_rename_over() {
    overlap(async |case, daemon, uid| {
        let held = case.share.stop_at(MockOp::Replace);
        daemon.save(uid, EDIT).await;
        held.reached().await;
        case.share
            .add_file("docs/.report.txt.sb-save", b"remote edit");
        case.share.remove(TARGET).unwrap();
        case.share
            .rename("docs/.report.txt.sb-save", TARGET)
            .unwrap();
        held.release();
        tokio::time::sleep(SETTLE).await;
        let temp_id = uploads(&case.log).pop().unwrap().file_id;
        assert_eq!(
            daemon.item(uid).unwrap().stable_id,
            temp_id,
            "{}",
            case.seed
        );
        at(TARGET, EDIT)
    });
}

/// A save arriving during start-up recovery, before the first scan.
#[test]
fn case_save_during_startup_recovery() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            daemon.save(uid, EDIT).await;
            uid
        });
        runtime().block_on(async {
            let daemon = case.disk.open(case.recording()).unwrap();
            let held = case.share.stop_at(MockOp::ListChildren);
            let cache = daemon.cache.clone();
            let recovering = tokio::spawn(async move { cache.recover().await });
            held.reached().await;
            let saving = daemon.spawn_save(uid, SECOND);
            let_run().await;
            held.release();
            recovering.await.unwrap().unwrap();
            saving.await.unwrap().unwrap();
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &at(TARGET, SECOND)).await;
        });
        assert_eq!(placed(&case.log), [SECOND], "{seed}");
    }
}

/// A save arriving while a download of the same item runs: download bytes
/// never overwrite the dirty object.
#[test]
fn case_save_during_download() {
    overlap(async |case, daemon, uid| {
        let held = case.share.stop_at(MockOp::Download);
        let cache = daemon.cache.clone();
        let fetching = tokio::spawn(async move { cache.fetch(uid, None).await });
        held.reached().await;
        daemon.save(uid, EDIT).await;
        held.release();
        let _ = fetching.await.unwrap();
        let len = u32::try_from(EDIT.len()).unwrap();
        assert_eq!(
            daemon.cache.read(uid, 0, len).await.unwrap(),
            EDIT,
            "{}",
            case.seed
        );
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            daemon.cache.read(uid, 0, len).await.unwrap(),
            EDIT,
            "{}",
            case.seed
        );
        at(TARGET, EDIT)
    });
}

/// A busy retry overlapping a re-save makes one final upload with the
/// latest content.
#[test]
fn case_busy_retry_overlapping_a_resave() {
    overlap(async |case, daemon, uid| {
        into_recorded(case, daemon, uid).await;
        daemon.save(uid, SECOND).await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(placed(&case.log), [SECOND], "{}", case.seed);
        at(TARGET, SECOND)
    });
}

/// A rename-over answered busy keeps the object dirty and the target
/// untouched, and retries with a doubling delay capped at five minutes.
#[test]
fn case_busy_rename_over_retries_with_doubling_backoff() {
    const BUSY: usize = 9;
    overlap(async |case, daemon, uid| {
        for _ in 0..BUSY {
            case.share.inject(MockOp::Replace, Fault::Busy);
        }
        daemon.save(uid, EDIT).await;
        until("the busy attempts", || {
            calls(&case.log, MockOp::Replace) >= BUSY
        })
        .await;
        assert!(daemon.dirty_object(uid).is_some(), "{}", case.seed);
        assert!(!daemon.item(uid).unwrap().uploaded, "{}", case.seed);
        assert_eq!(
            remote_bytes(&case.share, TARGET).await.as_deref(),
            Some(ORIGINAL),
            "{}",
            case.seed
        );
        tokio::time::sleep(SETTLE).await;
        let attempts = case.log.replace_attempts();
        let gaps: Vec<Duration> = attempts.windows(2).map(|w| w[1] - w[0]).collect();
        let slack = Duration::from_secs(1);
        for pair in gaps.windows(2) {
            let expected = (pair[0] * 2).min(BACKOFF_CAP);
            assert!(
                pair[1] + slack >= expected && pair[1] <= expected + slack,
                "{}: retry gaps {gaps:?} do not double up to five minutes",
                case.seed
            );
        }
        assert!(
            gaps.last().is_some_and(|gap| *gap + slack >= BACKOFF_CAP),
            "{}: gaps {gaps:?} never reach the cap",
            case.seed
        );
        assert_eq!(placed(&case.log), [EDIT], "{}", case.seed);
        at(TARGET, EDIT)
    });
}

/// FR-8 and AC-28: a delete of an item changed on the remote removes it.
#[test]
fn case_delete_of_an_item_changed_on_the_remote() {
    overlap(async |case, daemon, uid| {
        daemon.cache.fetch(uid, None).await.unwrap();
        case.share.add_file(TARGET, b"remote edit");
        daemon.cache.delete(uid).await.unwrap();
        Outcome::Gone
    });
}

/// FR-7: renames, deletes and folder creates fail with
/// [`TransientError::Unreachable`] when the backend is off, apply nothing,
/// and apply when issued again.
#[test]
fn case_direct_operations_fail_unreachable_and_apply_when_reissued() {
    overlap(async |case, daemon, uid| {
        let folder = daemon.folder_uid(uid);

        case.share.inject(MockOp::Move, Fault::Unreachable);
        let error = daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<TransientError>(),
                Some(TransientError::Unreachable(_))
            ),
            "{}: {error:#}",
            case.seed
        );
        assert_eq!(
            daemon.item(uid).unwrap().name,
            name_of(TARGET),
            "{}",
            case.seed
        );
        assert_eq!(remote_names(&case.share, FOLDER).await, [name_of(TARGET)]);
        daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap();

        case.share.inject(MockOp::CreateDir, Fault::Unreachable);
        let error = daemon.cache.create_folder(folder, "sub").await.unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<TransientError>(),
                Some(TransientError::Unreachable(_))
            ),
            "{}: {error:#}",
            case.seed
        );
        assert_eq!(remote_names(&case.share, FOLDER).await, [RENAMED_NAME]);
        let sub = daemon.cache.create_folder(folder, "sub").await.unwrap();
        daemon.cache.delete(sub.uid).await.unwrap();

        case.share.inject(MockOp::Delete, Fault::Unreachable);
        let error = daemon.cache.delete(uid).await.unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<TransientError>(),
                Some(TransientError::Unreachable(_))
            ),
            "{}: {error:#}",
            case.seed
        );
        assert!(daemon.item(uid).is_some(), "{}", case.seed);
        assert_eq!(remote_names(&case.share, FOLDER).await, [RENAMED_NAME]);
        at(RENAMED, ORIGINAL)
    });
}

/// FR-14 at the cache level: a rename to a name the backend cannot store is
/// refused before anything is applied.
#[test]
fn case_rename_to_a_rejected_name_applies_nothing() {
    overlap(async |case, daemon, uid| {
        let folder = daemon.folder_uid(uid);
        assert!(daemon.cache.rename(uid, folder, "a:b").await.is_err());
        assert!(daemon.cache.create_folder(folder, "a?b").await.is_err());
        assert_eq!(calls(&case.log, MockOp::Move), 0, "{}", case.seed);
        assert_eq!(calls(&case.log, MockOp::CreateDir), 0, "{}", case.seed);
        at(TARGET, ORIGINAL)
    });
}

/// TR-6: a delete for an unknown uid succeeds.
#[test]
fn case_delete_of_an_unknown_uid_succeeds() {
    overlap(async |_case, daemon, uid| {
        daemon.cache.delete(uid + 1_000_000).await.unwrap();
        at(TARGET, ORIGINAL)
    });
}

/// TR-12: a reparent of an item whose temporary file is recorded in its
/// old folder, then a kill: recovery leaves no temporary file anywhere.
#[test]
fn case_reparent_of_a_recorded_item_then_kill_leaves_no_temporary_file() {
    for seed in SEEDS {
        let case = Case::new(seed);
        case.share.add_folder("docs/sub").unwrap();
        let uid = runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            into_recorded(&case, &daemon, uid).await;
            let sub = daemon.uid("docs/sub").await;
            daemon
                .cache
                .rename(uid, sub, &name_of(TARGET))
                .await
                .unwrap();
            uid
        });
        runtime().block_on(async {
            let daemon = case.disk.start(case.recording()).await.unwrap();
            tokio::time::sleep(SETTLE).await;
            assert!(daemon.dirty_objects().is_empty(), "{seed}");
            assert_eq!(
                remote_names(&case.share, FOLDER).await,
                ["sub"],
                "{seed}: temporary file left"
            );
            assert_eq!(
                remote_names(&case.share, "docs/sub").await,
                [name_of(TARGET)],
                "{seed}"
            );
            assert_eq!(
                remote_bytes(&case.share, "docs/sub/report.txt")
                    .await
                    .as_deref(),
                Some(EDIT),
                "{seed}"
            );
            assert!(daemon.item(uid).unwrap().uploaded, "{seed}");
        });
        assert_eq!(placed(&case.log), [EDIT], "{seed}");
    }
}

fn assert_temporary_file_in(case: &Case, folder: &str) {
    assert!(
        runtime()
            .block_on(remote_names(&case.share, folder))
            .iter()
            .any(|name| is_temp_upload(name)),
        "{}: the upload did not land in {folder}",
        case.seed
    );
}

/// TR-12: an upload in flight during a reparent lands in the old folder and
/// the daemon is killed before it checks the parent; recovery removes the
/// temporary file from the old folder.
#[test]
fn case_upload_landing_after_a_reparent_then_kill_leaves_no_temporary_file() {
    for seed in SEEDS {
        let case = with_archive(seed);
        let uid = runtime().block_on(async {
            let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Upload);
            let (daemon, uid) = case.start(Arc::clone(&killed)).await;
            let held = into_uploading(&case, &daemon, uid).await;
            reparent_to_archive(&daemon, uid).await;
            held.release();
            killed.frozen().await;
            uid
        });
        assert_temporary_file_in(&case, FOLDER);
        case.restart_and_settle(uid, &at(ARCHIVED, EDIT));
        assert_left_folder_empty(&case);
    }
}

/// TR-12: an upload in flight during a delete lands after the item was
/// removed and the daemon is killed before it checks the item; recovery
/// removes the temporary file.
#[test]
fn case_upload_landing_after_a_delete_then_kill_leaves_no_temporary_file() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = runtime().block_on(async {
            let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Upload);
            let (daemon, uid) = case.start(Arc::clone(&killed)).await;
            let held = into_uploading(&case, &daemon, uid).await;
            daemon.cache.delete(uid).await.unwrap();
            assert!(daemon.item(uid).is_none(), "{seed}");
            held.release();
            killed.frozen().await;
            uid
        });
        assert_temporary_file_in(&case, FOLDER);
        case.restart_and_settle(uid, &Outcome::Gone);
        assert!(placed(&case.log).is_empty(), "{seed}");
    }
}

const CALL_TIMEOUT: Duration = Duration::from_secs(15);

fn four_request_sizes_of_content() -> Vec<u8> {
    let request = usize::try_from(TransferConfig::default().request_size).unwrap();
    (0..=3 * request)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect()
}

#[test]
fn case_slow_upload_of_several_pieces_completes_in_one_attempt() {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            let mut signals = daemon.cache.subscribe();
            let data = four_request_sizes_of_content();
            case.share
                .inject(MockOp::Upload, Fault::Delay(CALL_TIMEOUT * 3));
            daemon.save(uid, &data).await;
            tokio::time::sleep(SETTLE).await;
            assert_eq!(calls(&case.log, MockOp::Upload), 1, "{seed}: retried");
            assert!(
                signals.try_recv().is_err(),
                "{seed}: the slow upload was reported unreachable"
            );
            assert_eq!(
                remote_bytes(&case.share, TARGET).await,
                Some(data),
                "{seed}"
            );
            assert!(daemon.dirty_objects().is_empty(), "{seed}");
        });
    }
}

#[test]
fn case_upload_past_its_proportional_deadline_is_unreachable_and_retried() {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            let mut signals = daemon.cache.subscribe();
            let data = four_request_sizes_of_content();
            case.share
                .inject(MockOp::Upload, Fault::Delay(CALL_TIMEOUT * 5));
            daemon.save(uid, &data).await;
            tokio::time::sleep(SETTLE).await;
            assert_eq!(calls(&case.log, MockOp::Upload), 2, "{seed}: not retried");
            assert!(
                matches!(
                    signals.try_recv(),
                    Ok(crate::content::transfer::CacheSignal::ErrorResolved)
                ),
                "{seed}: the timed-out upload was not reported unreachable"
            );
            assert_eq!(placed_count(&case.log, &data), 1, "{seed}");
            assert!(daemon.dirty_objects().is_empty(), "{seed}");
        });
    }
}

async fn reparent_to_archive(daemon: &Daemon, uid: u64) {
    let archive = daemon.uid(ARCHIVE).await;
    daemon
        .cache
        .rename(uid, archive, &name_of(TARGET))
        .await
        .unwrap();
}

fn with_archive(seed: u64) -> Case {
    let case = Case::new(seed);
    case.share.add_folder(ARCHIVE).unwrap();
    case
}

fn assert_left_folder_empty(case: &Case) {
    assert_eq!(
        runtime().block_on(remote_names(&case.share, FOLDER)),
        Vec::<String>::new(),
        "{}: a temporary file is left in the folder the item left",
        case.seed
    );
}

fn clean_reparent(seed: u64) {
    let case = with_archive(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        reparent_to_archive(&daemon, uid).await;
        uid
    });
    case.restart_and_settle(uid, &at(ARCHIVED, ORIGINAL));
    assert_left_folder_empty(&case);
    assert!(uploads(&case.log).is_empty(), "{seed}: a reparent uploaded");
}

/// A save after a busy rename-over supersedes the recorded object; the
/// reparent still removes the temporary file it left in the old folder.
fn dirty_reparent(seed: u64) {
    let case = with_archive(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        daemon.save(uid, SECOND).await;
        reparent_to_archive(&daemon, uid).await;
        uid
    });
    case.restart_and_settle(uid, &at(ARCHIVED, SECOND));
    assert_left_folder_empty(&case);
    assert_eq!(placed(&case.log), [SECOND], "{seed}");
}

/// The upload in flight during a reparent lands in the old folder and its
/// rename-over would fail busy.
fn uploading_reparent(seed: u64) {
    let case = with_archive(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        case.share.inject(MockOp::Replace, Fault::Busy);
        let held = into_uploading(&case, &daemon, uid).await;
        reparent_to_archive(&daemon, uid).await;
        held.release();
        tokio::time::sleep(Duration::from_secs(1)).await;
        uid
    });
    case.restart_and_settle(uid, &at(ARCHIVED, EDIT));
    assert_left_folder_empty(&case);
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

fn recorded_reparent(seed: u64) {
    let case = with_archive(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        reparent_to_archive(&daemon, uid).await;
        assert!(daemon.dirty_object(uid).unwrap().upload.is_none(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(ARCHIVED, EDIT));
    assert_left_folder_empty(&case);
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

/// Kill after the backend delete of a folder holding a dirty file.
fn deleting_remote_deleted(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Delete);
        let (daemon, uid) = case.start(Arc::clone(&killed)).await;
        daemon.save(uid, EDIT).await;
        let deleting = daemon.spawn_delete(daemon.folder_uid(uid));
        killed.frozen().await;
        assert!(!deleting.is_finished());
        uid
    });
    runtime().block_on(async {
        let daemon = case.disk.start(case.recording()).await.unwrap();
        tokio::time::sleep(SETTLE).await;
        assert!(
            case.share.metadata(Path::new(FOLDER)).await.is_err(),
            "{seed}: the deleted folder was recreated"
        );
        assert!(daemon.dirty_objects().is_empty(), "{seed}");
        assert!(daemon.item(uid).is_none(), "{seed}: deleted child kept");
    });
    assert!(placed(&case.log).is_empty(), "{seed}");
}

fn deleting_delete_failed_clean(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.cache.fetch(uid, None).await.unwrap();
        case.share.inject(MockOp::Delete, Fault::Unreachable);
        assert!(daemon.cache.delete(uid).await.is_err(), "{seed}");
        assert!(daemon.item(uid).is_some(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, ORIGINAL));
    assert!(uploads(&case.log).is_empty(), "{seed}");
}

/// FR-7: a delete of a dirty item failing unreachable keeps the edit, which
/// a restart uploads.
fn deleting_delete_failed_dirty(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.save(uid, EDIT).await;
        case.share.inject(MockOp::Delete, Fault::Unreachable);
        assert!(daemon.cache.delete(uid).await.is_err(), "{seed}");
        let len = u32::try_from(EDIT.len()).unwrap();
        assert_eq!(
            daemon.cache.read(uid, 0, len).await.unwrap(),
            EDIT,
            "{seed}"
        );
        assert!(daemon.dirty_object(uid).is_some(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT));
    assert_eq!(placed(&case.log), [EDIT], "{seed}");
}

/// Kill while the backend delete of a dirty item is held before it applies.
fn deleting_restart(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::Delete);
        let _deleting = daemon.spawn_delete(uid);
        held.reached().await;
        uid
    });
    case.restart_and_settle(uid, &Outcome::Gone);
    assert!(placed(&case.log).is_empty(), "{seed}");
}

#[test]
fn case_delete_of_a_dirty_item_while_unreachable_keeps_the_edit_and_uploads_it() {
    overlap(async |case, daemon, uid| {
        daemon.save(uid, EDIT).await;
        case.share.inject(MockOp::Delete, Fault::Unreachable);
        assert!(daemon.cache.delete(uid).await.is_err(), "{}", case.seed);
        let len = u32::try_from(EDIT.len()).unwrap();
        assert_eq!(
            daemon.cache.read(uid, 0, len).await.unwrap(),
            EDIT,
            "{}",
            case.seed
        );
        tokio::time::sleep(SETTLE).await;
        assert_eq!(placed(&case.log), [EDIT], "{}", case.seed);
        at(TARGET, EDIT)
    });
}

#[test]
fn case_at_most_the_permitted_uploads_run_at_once() {
    const FILES: usize = 7;
    for seed in SEEDS {
        let case = Case::new(seed);
        for n in 0..FILES {
            case.share.add_file(&format!("docs/f{n}.txt"), ORIGINAL);
            case.share
                .inject(MockOp::Upload, Fault::Delay(Duration::from_secs(1)));
        }
        runtime().block_on(async {
            let (daemon, _) = case.start(case.recording()).await;
            for n in 0..FILES {
                let uid = daemon.uid(&format!("docs/f{n}.txt")).await;
                daemon.save(uid, EDIT).await;
            }
            tokio::time::sleep(SETTLE).await;
            assert!(daemon.dirty_objects().is_empty(), "{seed}");
            assert_eq!(placed_count(&case.log, EDIT), FILES, "{seed}");
            assert_eq!(
                case.log.max_uploading(),
                TransferConfig::default().upload_transfers,
                "{seed}: uploads at once"
            );
        });
    }
}

#[test]
fn case_a_scan_started_before_recovery_waits_for_it() {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            daemon.save(uid, EDIT).await;
        });
        runtime().block_on(async {
            let daemon = case.disk.open(case.recording()).unwrap();
            let listings = calls(&case.log, MockOp::ListChildren);
            let cache = daemon.cache.clone();
            let scanning = tokio::spawn(async move { cache.scan().await });
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(!scanning.is_finished(), "{seed}: scanned before recovery");
            assert_eq!(
                calls(&case.log, MockOp::ListChildren),
                listings,
                "{seed}: listed before recovery"
            );
            daemon.cache.recover().await.unwrap();
            scanning.await.unwrap().unwrap();
            assert!(calls(&case.log, MockOp::ListChildren) > listings, "{seed}");
        });
    }
}

#[test]
fn case_operation_locks_of_settled_items_are_released() {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            daemon.save(uid, EDIT).await;
            let folder = daemon.folder_uid(uid);
            daemon
                .cache
                .rename(uid, folder, RENAMED_NAME)
                .await
                .unwrap();
            tokio::time::sleep(SETTLE).await;
            assert!(daemon.dirty_objects().is_empty(), "{seed}");
            let locks = daemon.cache.shared().writeback.locks.lock().unwrap().len();
            assert_eq!(locks, 0, "{seed}: operation locks kept");
        });
    }
}

/// A write into an object never fetched calls no backend before its reply.
fn clean_write(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let version = daemon.item(uid).unwrap().content_version;
        let fetched = case.log.fetched();
        daemon.write(uid, 2, b"XY").await;
        assert_eq!(case.log.fetched(), fetched, "{seed}: the write fetched");
        assert!(daemon.edited_object(uid).is_some(), "{seed}");
        assert_eq!(
            daemon.item(uid).unwrap().content_version,
            version,
            "{seed}: a write minted a version before the writes paused"
        );
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, WRITTEN));
    assert_eq!(placed(&case.log), [WRITTEN], "{seed}: FR-7 exactly once");
}

fn dirty_write(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.save(uid, EDIT).await;
        daemon.write(uid, 0, b"L").await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT_WRITTEN));
    assert_eq!(placed(&case.log), [EDIT_WRITTEN], "{seed}");
}

fn uploading_write(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_uploading(&case, &daemon, uid).await;
        daemon.write(uid, 0, b"L").await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT_WRITTEN));
    assert_eq!(placed(&case.log), [EDIT_WRITTEN], "{seed}");
}

fn recorded_write(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        into_recorded(&case, &daemon, uid).await;
        daemon.write(uid, 0, b"L").await;
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, EDIT_WRITTEN));
    assert_eq!(placed(&case.log), [EDIT_WRITTEN], "{seed}");
}

/// Writes in a burst land in one edited object and one content version.
fn editing_write(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.write(uid, 2, b"XY").await;
        let object = daemon.edited_object(uid).unwrap();
        daemon.write(uid, 4, b"QQ").await;
        assert_eq!(
            daemon.edited_object(uid).unwrap().object_id,
            object.object_id,
            "{seed}: a further write left the edited object"
        );
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, WRITTEN_TWICE));
    assert_eq!(placed(&case.log), [WRITTEN_TWICE], "{seed}");
}

fn editing_save(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.write(uid, 2, b"XY").await;
        daemon.save(uid, SECOND).await;
        assert!(daemon.edited_object(uid).is_none(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, SECOND));
    assert_eq!(placed(&case.log), [SECOND], "{seed}");
}

/// Kill while the upload of a sealed edit is held: the edit was accepted
/// as a save with a new content version first.
fn editing_timer_due_uploading(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let version = daemon.item(uid).unwrap().content_version;
        let held = case.share.stop_at(MockOp::Upload);
        daemon.write(uid, 2, b"XY").await;
        held.reached().await;
        assert!(
            daemon.edited_object(uid).is_none(),
            "{seed}: edit not sealed"
        );
        assert!(daemon.dirty_object(uid).is_some(), "{seed}");
        assert_ne!(
            daemon.item(uid).unwrap().content_version,
            version,
            "{seed}: the sealed edit minted no version"
        );
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, WRITTEN));
    assert_eq!(placed(&case.log), [WRITTEN], "{seed}");
}

/// The fetch of the missing ranges fails unreachable; the edit stays.
fn editing_timer_due_editing(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        let before = calls(&case.log, MockOp::Metadata);
        case.share.inject(MockOp::Metadata, Fault::Unreachable);
        daemon.write(uid, 2, b"XY").await;
        until("a failed fill", || {
            calls(&case.log, MockOp::Metadata) > before
        })
        .await;
        let_run().await;
        assert!(daemon.edited_object(uid).is_some(), "{seed}: edit lost");
        assert!(uploads(&case.log).is_empty(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, WRITTEN));
    assert_eq!(placed(&case.log), [WRITTEN], "{seed}");
}

fn editing_rename(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.write(uid, 2, b"XY").await;
        let folder = daemon.folder_uid(uid);
        daemon
            .cache
            .rename(uid, folder, RENAMED_NAME)
            .await
            .unwrap();
        assert!(daemon.edited_object(uid).is_some(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(RENAMED, WRITTEN));
    assert_eq!(placed(&case.log), [WRITTEN], "{seed}");
}

fn editing_reparent(seed: u64) {
    let case = with_archive(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.write(uid, 2, b"XY").await;
        reparent_to_archive(&daemon, uid).await;
        assert!(daemon.edited_object(uid).is_some(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(ARCHIVED, WRITTEN));
    assert_left_folder_empty(&case);
    assert_eq!(placed(&case.log), [WRITTEN], "{seed}");
}

fn editing_delete(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.write(uid, 2, b"XY").await;
        daemon.cache.delete(uid).await.unwrap();
        uid
    });
    case.restart_and_settle(uid, &Outcome::Gone);
    assert!(
        placed(&case.log).is_empty(),
        "{seed}: a deleted edited item was uploaded"
    );
}

fn editing_scan(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.write(uid, 2, b"XY").await;
        let before = daemon.item(uid).unwrap();
        case.share.rename(TARGET, RENAMED).unwrap();
        daemon.scan().await;
        let after = daemon.item(uid).expect("the edited item keeps its uid");
        assert_eq!(
            after.name, RENAMED_NAME,
            "{seed}: name followed by stable id"
        );
        assert_eq!(
            after.content_version, before.content_version,
            "{seed}: scan minted a version"
        );
        assert!(daemon.edited_object(uid).is_some(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(RENAMED, WRITTEN));
    assert_eq!(placed(&case.log), [WRITTEN], "{seed}");
}

/// A kill between writes and the upload loses no written range of an
/// object whose other ranges were never fetched.
fn editing_restart(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.write(uid, 0, b"O").await;
        daemon.write(uid, 6, b"AL").await;
        assert!(case.log.fetched().is_empty(), "{seed}: a write fetched");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, b"OriginAL"));
    assert_eq!(placed(&case.log), [b"OriginAL"], "{seed}");
}

/// A write queued behind the operation lock while the timer's seal fetches
/// the edit's missing ranges: every written byte reaches the remote and no
/// object ends clean while edited.
#[test]
fn case_write_queued_behind_the_seal_of_an_edit() {
    overlap(async |case, daemon, uid| {
        let held = case.share.stop_at(MockOp::Download);
        daemon.write(uid, 2, b"XY").await;
        held.reached().await;
        let cache = daemon.cache.clone();
        let writing = tokio::spawn(async move { cache.write_at(uid, 4, b"QQ").await });
        let_run().await;
        held.release();
        writing.await.unwrap().unwrap();
        tokio::time::sleep(SETTLE).await;
        let objects = daemon.db.content_objects_of(uid).unwrap();
        assert!(
            objects.iter().all(|object| object.dirty || !object.edited),
            "{}: an object ended clean with edited set: {objects:?}",
            case.seed
        );
        assert_eq!(
            placed(&case.log).last().map(Vec::as_slice),
            Some(WRITTEN_TWICE),
            "{}",
            case.seed
        );
        at(TARGET, WRITTEN_TWICE)
    });
}

/// FR-7: a delete of an edited item failing unreachable keeps the edit,
/// which a restart uploads.
fn deleting_delete_failed_editing(seed: u64) {
    let case = Case::new(seed);
    let uid = runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.write(uid, 2, b"XY").await;
        case.share.inject(MockOp::Delete, Fault::Unreachable);
        assert!(daemon.cache.delete(uid).await.is_err(), "{seed}");
        let len = u32::try_from(WRITTEN.len()).unwrap();
        assert_eq!(
            daemon.cache.read(uid, 0, len).await.unwrap(),
            WRITTEN,
            "{seed}"
        );
        assert!(daemon.edited_object(uid).is_some(), "{seed}");
        uid
    });
    case.restart_and_settle(uid, &at(TARGET, WRITTEN));
    assert_eq!(placed(&case.log), [WRITTEN], "{seed}");
}

/// Faults a backend that cannot take a delete at recovery answers with.
const RECOVERY_FAULTS: [Fault; 2] = [Fault::Unreachable, Fault::Busy];

impl Case {
    fn object_file(&self, object_id: ObjectId) -> PathBuf {
        self.objects.join(object_id.to_string())
    }

    /// Durable state of a marked, unapplied delete of a target holding [`EDIT`].
    fn assert_delete_pending(&self, uid: u64) -> Vec<ObjectId> {
        let seed = self.seed;
        let (db, store) = self.disk.replicas().unwrap();
        assert_eq!(db.replica_deletes().unwrap(), [uid], "{seed}: mark");
        assert_eq!(
            self.share.content(Path::new(TARGET)).as_deref(),
            Some(ORIGINAL),
            "{seed}: the backend file"
        );
        assert!(store.item(uid).unwrap().is_some(), "{seed}: replica row");
        let objects = db.content_objects_of(uid).unwrap();
        let dirty = objects
            .iter()
            .find(|object| object.dirty)
            .unwrap_or_else(|| panic!("{seed}: the dirty object is kept"));
        assert_eq!(
            std::fs::read(self.object_file(dirty.object_id)).unwrap(),
            EDIT,
            "{seed}: object file"
        );
        objects.iter().map(|object| object.object_id).collect()
    }

    /// Restarts with the backend failing the next delete with `fault`:
    /// recovery tries the marked delete once and drops its mark.
    async fn restart_with_delete_failing(&self, backend: Arc<Recording>, fault: Fault) -> Daemon {
        let seed = self.seed;
        self.share.inject(MockOp::Delete, fault);
        let deletes = calls(&self.log, MockOp::Delete);
        let daemon = self.disk.start(backend).await.unwrap();
        assert_eq!(
            calls(&self.log, MockOp::Delete),
            deletes + 1,
            "{seed} {fault:?}: recovery issued no delete"
        );
        assert!(
            daemon.db.replica_deletes().unwrap().is_empty(),
            "{seed} {fault:?}: recovery kept the mark of a delete it could not finish"
        );
        daemon
    }

    /// Lets the daemon settle and checks that no delete reached the backend
    /// meanwhile.
    async fn settle_without_deletes(&self, what: &str) {
        let deletes = calls(&self.log, MockOp::Delete);
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            calls(&self.log, MockOp::Delete),
            deletes,
            "{} {what}: a delete no caller issued reached the backend",
            self.seed
        );
    }
}

async fn assert_reads(daemon: &Daemon, uid: u64, data: &[u8], what: &str) {
    let len = u32::try_from(data.len()).unwrap();
    assert_eq!(
        daemon.cache.read(uid, 0, len).await.unwrap(),
        data,
        "{what}"
    );
}

/// Saves [`EDIT`], kills while the delete of the item `deleted` picks is held.
fn kill_during_delete(case: &Case, deleted: fn(&Daemon, u64) -> u64) -> u64 {
    runtime().block_on(async {
        let (daemon, uid) = case.start(case.recording()).await;
        daemon.save(uid, EDIT).await;
        let held = case.share.stop_at(MockOp::Delete);
        let _deleting = daemon.spawn_delete(deleted(&daemon, uid));
        held.reached().await;
        uid
    })
}

const fn the_item(_: &Daemon, uid: u64) -> u64 {
    uid
}

fn its_folder(daemon: &Daemon, uid: u64) -> u64 {
    daemon.folder_uid(uid)
}

/// FR-7, AD-20: a clean item whose marked delete recovery could not finish
/// is listed, read and evicted as any clean item, and nothing deletes it.
fn deleting_restart_clean(seed: u64) {
    for fault in RECOVERY_FAULTS {
        let case = Case::new(seed);
        let what = format!("{seed} {fault:?}");
        let uid = runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            daemon.cache.fetch(uid, None).await.unwrap();
            let held = case.share.stop_at(MockOp::Delete);
            let _deleting = daemon.spawn_delete(uid);
            held.reached().await;
            uid
        });
        runtime().block_on(async {
            let daemon = case
                .restart_with_delete_failing(case.recording(), fault)
                .await;
            assert_eq!(
                daemon.list_folder_of(uid).await,
                [name_of(TARGET)],
                "{what}: listing"
            );
            assert_reads(&daemon, uid, ORIGINAL, &what).await;
            daemon.cache.set_max_size(0);
            assert!(
                daemon.cache.sweep().await.unwrap() > 0,
                "{what}: eviction skipped the item"
            );
            assert!(
                daemon.db.content_objects_of(uid).unwrap().is_empty(),
                "{what}: object kept"
            );
            case.settle_without_deletes(&what).await;
            case.assert_outcome(&daemon, uid, &at(TARGET, ORIGINAL))
                .await;
        });
        assert!(uploads(&case.log).is_empty(), "{what}");
    }
}

/// FR-7, FR-8, AD-19: a dirty item whose marked delete recovery could not
/// finish keeps its object, its timer uploads it and nothing deletes it.
fn deleting_restart_dirty(seed: u64) {
    for fault in RECOVERY_FAULTS {
        let case = Case::new(seed);
        let what = format!("{seed} {fault:?}");
        let uid = kill_during_delete(&case, the_item);
        let objects = case.assert_delete_pending(uid);
        runtime().block_on(async {
            let daemon = case
                .restart_with_delete_failing(case.recording(), fault)
                .await;
            let dirty = daemon
                .dirty_object(uid)
                .unwrap_or_else(|| panic!("{what}: dirty object lost"));
            assert!(objects.contains(&dirty.object_id), "{what}");
            assert_eq!(
                daemon.list_folder_of(uid).await,
                [name_of(TARGET)],
                "{what}: listing"
            );
            assert_reads(&daemon, uid, EDIT, &what).await;
            tokio::time::sleep(WRITE_BACK_DELAY + Duration::from_secs(1)).await;
            assert_eq!(
                placed(&case.log),
                [EDIT],
                "{what}: the timer of the dirty object was not armed at recovery"
            );
            case.settle_without_deletes(&what).await;
            case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
        });
        assert_eq!(placed(&case.log), [EDIT], "{what}");
    }
}

/// FR-7: an edited item whose marked delete recovery could not finish keeps
/// its edit, which its timer uploads.
fn deleting_restart_editing(seed: u64) {
    for fault in RECOVERY_FAULTS {
        let case = Case::new(seed);
        let what = format!("{seed} {fault:?}");
        let uid = runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            daemon.write(uid, 2, b"XY").await;
            let held = case.share.stop_at(MockOp::Delete);
            let _deleting = daemon.spawn_delete(uid);
            held.reached().await;
            uid
        });
        runtime().block_on(async {
            let daemon = case
                .restart_with_delete_failing(case.recording(), fault)
                .await;
            assert!(daemon.edited_object(uid).is_some(), "{what}: edit lost");
            assert_reads(&daemon, uid, WRITTEN, &what).await;
            case.settle_without_deletes(&what).await;
            case.assert_outcome(&daemon, uid, &at(TARGET, WRITTEN))
                .await;
        });
        assert_eq!(placed(&case.log), [WRITTEN], "{what}");
    }
}

/// FR-7: a delete issued again after recovery dropped the mark and failing
/// unreachable keeps the edit; nothing deletes the item later.
#[test]
fn overlap_a_delete_reissued_after_recovery_dropped_its_mark_fails_and_keeps_the_edit() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = kill_during_delete(&case, the_item);
        runtime().block_on(async {
            let daemon = case
                .restart_with_delete_failing(case.recording(), Fault::Unreachable)
                .await;
            case.share.inject(MockOp::Delete, Fault::Unreachable);
            assert!(daemon.cache.delete(uid).await.is_err(), "{seed}");
            assert!(daemon.db.replica_deletes().unwrap().is_empty(), "{seed}");
            case.settle_without_deletes("after the failed delete").await;
            case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
        });
        assert_eq!(placed(&case.log), [EDIT], "{seed}");
    }
}

/// FR-8: a scan after recovery dropped a delete lists the item under its uid;
/// nothing deletes it while the scan is held or after.
#[test]
fn overlap_a_scan_after_recovery_dropped_a_delete_lists_the_item_and_deletes_nothing() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = kill_during_delete(&case, the_item);
        runtime().block_on(async {
            let backend = case.recording();
            let daemon = case
                .restart_with_delete_failing(Arc::clone(&backend), Fault::Unreachable)
                .await;
            let deletes = calls(&case.log, MockOp::Delete);
            let listed = backend.pause_after(MockOp::ListChildren);
            let scanning = daemon.spawn_scan();
            listed.reached.notified().await;
            tokio::time::sleep(WRITE_BACK_DELAY.saturating_sub(Duration::from_secs(1))).await;
            assert_eq!(
                calls(&case.log, MockOp::Delete),
                deletes,
                "{seed}: a delete ran while the scan was held"
            );
            listed.released.notify_one();
            scanning.await.unwrap().unwrap();
            assert_eq!(
                daemon.uid(TARGET).await,
                uid,
                "{seed}: listed under another uid"
            );
            case.settle_without_deletes("after the scan").await;
            case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
        });
        assert_eq!(placed(&case.log), [EDIT], "{seed}");
    }
}

async fn assert_not_resurrected(case: &Case, daemon: &Daemon, uid: u64) {
    let seed = case.seed;
    daemon.scan().await;
    assert!(daemon.item(uid).is_none(), "{seed}: scan resurrected");
    let folder = daemon
        .replica()
        .unwrap()
        .resolve(Path::new(FOLDER))
        .await
        .unwrap()
        .unwrap();
    assert!(
        daemon.db.replica_children(folder).unwrap().is_empty(),
        "{seed}: the deleted file is listed under another uid"
    );
}

/// FR-8: a delete issued again after recovery dropped the mark, held while a
/// scan runs, completes; the scan does not bring the item back.
#[test]
fn overlap_a_delete_reissued_after_recovery_held_while_a_scan_runs_completes() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = kill_during_delete(&case, the_item);
        runtime().block_on(async {
            let daemon = case
                .restart_with_delete_failing(case.recording(), Fault::Unreachable)
                .await;
            let held = case.share.stop_at(MockOp::Delete);
            let deleting = daemon.spawn_delete(uid);
            held.reached().await;
            daemon.scan().await;
            held.release();
            let deleted = deleting.await.unwrap();
            assert!(deleted.is_ok(), "{seed}: {deleted:?}");
            assert_not_resurrected(&case, &daemon, uid).await;
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &Outcome::Gone).await;
        });
        assert!(placed(&case.log).is_empty(), "{seed}");
    }
}

/// FR-7: a save after recovery dropped a delete is uploaded, not dropped.
#[test]
fn overlap_a_save_after_recovery_dropped_a_delete_is_uploaded() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = kill_during_delete(&case, the_item);
        runtime().block_on(async {
            let daemon = case
                .restart_with_delete_failing(case.recording(), Fault::Unreachable)
                .await;
            daemon.save(uid, SECOND).await;
            case.settle_without_deletes("after the save").await;
            case.assert_outcome(&daemon, uid, &at(TARGET, SECOND)).await;
        });
        assert_eq!(placed(&case.log), [SECOND], "{seed}");
    }
}

/// FR-7: an NFS write after recovery dropped a delete is uploaded, not dropped.
#[test]
fn overlap_a_write_after_recovery_dropped_a_delete_is_uploaded() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = kill_during_delete(&case, the_item);
        runtime().block_on(async {
            let daemon = case
                .restart_with_delete_failing(case.recording(), Fault::Unreachable)
                .await;
            daemon.write(uid, 0, b"L").await;
            case.settle_without_deletes("after the write").await;
            case.assert_outcome(&daemon, uid, &at(TARGET, EDIT_WRITTEN))
                .await;
        });
        assert_eq!(placed(&case.log), [EDIT_WRITTEN], "{seed}");
    }
}

/// FR-7: a save into a file of a folder whose delete recovery dropped is
/// uploaded into the kept folder.
#[test]
fn overlap_a_save_into_the_subtree_of_a_folder_delete_recovery_dropped_is_uploaded() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = kill_during_delete(&case, its_folder);
        runtime().block_on(async {
            let daemon = case
                .restart_with_delete_failing(case.recording(), Fault::Unreachable)
                .await;
            let folder = daemon.folder_uid(uid);
            daemon.save(uid, SECOND).await;
            case.settle_without_deletes("after the save").await;
            assert!(daemon.item(folder).is_some(), "{seed}: folder dropped");
            case.assert_outcome(&daemon, uid, &at(TARGET, SECOND)).await;
        });
        assert_eq!(placed(&case.log), [SECOND], "{seed}");
    }
}

/// Kills after the backend applied the delete of a dirty item and makes the
/// first metadata call of the restart, recovery's, fail unreachable.
fn kill_after_an_applied_delete(case: &Case) -> u64 {
    let uid = runtime().block_on(async {
        let killed = Recording::freezing_after(&case.share, &case.log, MockOp::Delete);
        let (daemon, uid) = case.start(Arc::clone(&killed)).await;
        daemon.save(uid, EDIT).await;
        let _deleting = daemon.spawn_delete(uid);
        killed.frozen().await;
        uid
    });
    assert_eq!(case.share.content(Path::new(TARGET)), None, "{}", case.seed);
    case.share.inject(MockOp::Metadata, Fault::Unreachable);
    uid
}

/// FR-8: a file the backend deleted before the kill, whose delete recovery
/// could not finish, is recreated by the upload of its dirty object.
#[test]
fn case_an_applied_delete_recovery_cannot_finish_is_undone_by_the_upload_of_the_edit() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = kill_after_an_applied_delete(&case);
        runtime().block_on(async {
            let daemon = case.disk.start(case.recording()).await.unwrap();
            assert!(
                daemon.db.replica_deletes().unwrap().is_empty(),
                "{seed}: recovery kept the mark"
            );
            case.settle_without_deletes("after recovery").await;
            case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
        });
        assert_eq!(placed(&case.log), [EDIT], "{seed}");
    }
}

/// FR-8: the same item deleted again by the caller ends deleted.
#[test]
fn case_an_applied_delete_recovery_cannot_finish_ends_deleted_when_deleted_again() {
    for seed in SEEDS {
        let case = Case::new(seed);
        let uid = kill_after_an_applied_delete(&case);
        runtime().block_on(async {
            let daemon = case.disk.start(case.recording()).await.unwrap();
            assert!(
                daemon.db.replica_deletes().unwrap().is_empty(),
                "{seed}: recovery kept the mark"
            );
            let deleted = daemon.cache.delete(uid).await;
            assert!(deleted.is_ok(), "{seed}: {deleted:?}");
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &Outcome::Gone).await;
        });
        assert!(placed(&case.log).is_empty(), "{seed}");
    }
}

fn recorded_temp(daemon: &Daemon, uid: u64, folder: &str) -> String {
    let record = daemon
        .dirty_object(uid)
        .and_then(|object| object.upload)
        .expect("the item has a recorded temporary upload");
    format!("{folder}/{}", record.temp_name)
}

async fn start_recorded(case: &Case, backend: &Arc<Recording>) -> (Daemon, u64, String) {
    let (daemon, uid) = case.start(Arc::clone(backend)).await;
    into_recorded(case, &daemon, uid).await;
    let temp = recorded_temp(&daemon, uid, FOLDER);
    (daemon, uid, temp)
}

async fn assert_delete_undone(case: &Case, daemon: &Daemon, uid: u64, what: &str) {
    assert!(
        daemon.db.replica_deletes().unwrap().is_empty(),
        "{what}: mark kept"
    );
    assert_eq!(
        case.share.content(Path::new(TARGET)).as_deref(),
        Some(ORIGINAL),
        "{what}: the target left the backend"
    );
    assert_eq!(
        daemon.list_folder_of(uid).await,
        [name_of(TARGET)],
        "{what}: listing"
    );
    assert!(
        daemon.dirty_object(uid).is_some(),
        "{what}: dirty object lost"
    );
    assert_reads(daemon, uid, EDIT, what).await;
}

impl Case {
    /// Durable state at a kill of a recorded item holding [`EDIT`] whose
    /// delete did not reach the target: the `marks`, the record naming
    /// `temp`, the temporary file holding `temp_data`, the target, the
    /// replica row and the dirty object.
    fn assert_recorded_at_kill(
        &self,
        uid: u64,
        temp: &str,
        marks: &[u64],
        temp_data: Option<&[u8]>,
    ) {
        let seed = self.seed;
        let (db, store) = self.disk.replicas().unwrap();
        assert_eq!(db.replica_deletes().unwrap(), marks, "{seed}: marks");
        assert_eq!(
            self.share.content(Path::new(TARGET)).as_deref(),
            Some(ORIGINAL),
            "{seed}: the target on the backend"
        );
        assert_eq!(
            self.share.content(Path::new(temp)).as_deref(),
            temp_data,
            "{seed}: the temporary file on the backend"
        );
        assert!(store.item(uid).unwrap().is_some(), "{seed}: replica row");
        let dirty = db
            .content_objects_of(uid)
            .unwrap()
            .into_iter()
            .find(|object| object.dirty)
            .unwrap_or_else(|| panic!("{seed}: the dirty object is kept"));
        assert_eq!(
            dirty.upload.map(|record| record.temp_name),
            Some(name_of(temp)),
            "{seed}: upload record"
        );
        assert_eq!(
            std::fs::read(self.object_file(dirty.object_id)).unwrap(),
            EDIT,
            "{seed}: object file"
        );
    }
}

fn assert_target_never_deleted(case: &Case, what: &str) {
    assert!(
        !case.log.deleted().contains(&TARGET.to_owned()),
        "{what}: the daemon deleted the target: {:?}",
        case.log.deleted()
    );
}

/// Records [`EDIT`] and deletes the item with the removal of its temporary
/// upload failing with `fault`: the delete fails before the target.
async fn delete_failing_at_the_temporary_upload(
    case: &Case,
    fault: Fault,
    what: &str,
) -> (Daemon, u64, String) {
    let backend = case.recording();
    let (daemon, uid, temp) = start_recorded(case, &backend).await;
    backend.fail_delete_of(&temp, fault);
    let deleted = daemon.cache.delete(uid).await;
    assert!(deleted.is_err(), "{what}: {deleted:?}");
    assert_eq!(
        case.log.deleted(),
        Vec::<String>::new(),
        "{what}: a delete reached the backend"
    );
    let mut expected = vec![name_of(&temp), name_of(TARGET)];
    expected.sort();
    assert_eq!(remote_names(&case.share, FOLDER).await, expected, "{what}");
    assert_delete_undone(case, &daemon, uid, what).await;
    (daemon, uid, temp)
}

/// Kill after a delete of a recorded item failed removing its temporary
/// upload; the restart renames the recorded upload over the target.
fn deleting_delete_failed_recorded(seed: u64) {
    for fault in RECOVERY_FAULTS {
        let case = Case::new(seed);
        let what = format!("{seed} {fault:?}");
        let (uid, temp) = runtime().block_on(async {
            let (_daemon, uid, temp) =
                delete_failing_at_the_temporary_upload(&case, fault, &what).await;
            (uid, temp)
        });
        case.assert_recorded_at_kill(uid, &temp, &[], Some(EDIT));
        runtime().block_on(async {
            let daemon = case.disk.start(case.recording()).await.unwrap();
            case.settle_without_deletes(&what).await;
            case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
        });
        assert_eq!(placed(&case.log), [EDIT], "{what}");
        assert_target_never_deleted(&case, &what);
    }
}

/// Kill inside the removal of a recorded item's temporary upload, after the
/// backend removed it and before the record was cleared; the restart cannot
/// reach the backend, drops the mark and uploads the edit afresh instead of
/// renaming the missing temporary file.
fn deleting_restart_recorded(seed: u64) {
    for fault in RECOVERY_FAULTS {
        let case = Case::new(seed);
        let what = format!("{seed} {fault:?}");
        let (uid, temp) =
            kill_during_a_delete_with_a_temporary_upload(&case, KilledAfter::TemporaryRemoved);
        case.assert_recorded_at_kill(uid, &temp, &[uid], None);
        let uploaded = uploads(&case.log).len();
        let replaced = calls(&case.log, MockOp::Replace);
        restart_failing_keeps_the_edit(&case, uid, fault);
        assert_eq!(
            uploads(&case.log).len(),
            uploaded + 1,
            "{what}: uploads after the restart"
        );
        assert_eq!(
            calls(&case.log, MockOp::Replace),
            replaced + 1,
            "{what}: rename-overs after the restart"
        );
        assert_target_never_deleted(&case, &what);
    }
}

/// R4a
#[test]
fn case_a_delete_whose_temporary_upload_cannot_be_removed_fails_before_the_target_is_deleted() {
    for seed in SEEDS {
        for fault in RECOVERY_FAULTS {
            let case = Case::new(seed);
            let what = format!("{seed} {fault:?}");
            runtime().block_on(async {
                let (daemon, uid, _temp) =
                    delete_failing_at_the_temporary_upload(&case, fault, &what).await;
                case.settle_without_deletes(&what).await;
                case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
            });
            assert_eq!(placed(&case.log), [EDIT], "{what}");
        }
    }
}

/// R4a
#[test]
fn case_a_delete_reissued_after_its_temporary_upload_could_not_be_removed_completes() {
    for seed in SEEDS {
        for fault in RECOVERY_FAULTS {
            let case = Case::new(seed);
            let what = format!("{seed} {fault:?}");
            runtime().block_on(async {
                let backend = case.recording();
                let (daemon, uid, temp) = start_recorded(&case, &backend).await;
                backend.fail_delete_of(&temp, fault);
                let deleted = daemon.cache.delete(uid).await;
                assert!(deleted.is_err(), "{what}: {deleted:?}");
                let deleted = daemon.cache.delete(uid).await;
                assert!(deleted.is_ok(), "{what}: {deleted:?}");
                assert_eq!(
                    case.log.deleted(),
                    [temp.clone(), TARGET.to_owned()],
                    "{what}: deletes applied"
                );
                assert!(daemon.item(uid).is_none(), "{what}: replica row");
                tokio::time::sleep(SETTLE).await;
                case.assert_outcome(&daemon, uid, &Outcome::Gone).await;
            });
            assert!(placed(&case.log).is_empty(), "{what}");
        }
    }
}

/// R4b, R4e
#[test]
fn case_a_delete_whose_target_delete_fails_after_the_temporary_upload_was_removed_keeps_the_edit() {
    for seed in SEEDS {
        for fault in RECOVERY_FAULTS {
            let case = Case::new(seed);
            let what = format!("{seed} {fault:?}");
            runtime().block_on(async {
                let backend = case.recording();
                let (daemon, uid, temp) = start_recorded(&case, &backend).await;
                backend.fail_delete_of(TARGET, fault);
                let deleted = daemon.cache.delete(uid).await;
                assert!(deleted.is_err(), "{what}: {deleted:?}");
                assert_eq!(case.log.deleted(), [temp], "{what}: deletes applied");
                assert_eq!(
                    remote_names(&case.share, FOLDER).await,
                    [name_of(TARGET)],
                    "{what}: a temporary name is left"
                );
                assert_delete_undone(&case, &daemon, uid, &what).await;
                assert!(
                    daemon
                        .dirty_object(uid)
                        .and_then(|object| object.upload)
                        .is_none(),
                    "{what}: the record points at the removed temporary upload"
                );
                let uploaded = uploads(&case.log).len();
                case.settle_without_deletes(&what).await;
                assert_eq!(
                    uploads(&case.log).len(),
                    uploaded + 1,
                    "{what}: the edit was not uploaded again"
                );
                case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
            });
            assert_eq!(placed(&case.log), [EDIT], "{what}");
        }
    }
}

/// R4c
#[test]
fn case_a_delete_removes_the_temporary_upload_before_the_target() {
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let backend = case.recording();
            let (daemon, uid, temp) = start_recorded(&case, &backend).await;
            let objects: Vec<ObjectId> = daemon
                .db
                .content_objects_of(uid)
                .unwrap()
                .iter()
                .map(|object| object.object_id)
                .collect();
            let deleted = daemon.cache.delete(uid).await;
            assert!(deleted.is_ok(), "{seed}: {deleted:?}");
            assert_eq!(
                case.log.deleted(),
                [temp, TARGET.to_owned()],
                "{seed}: deletes applied"
            );
            assert_eq!(
                remote_names(&case.share, FOLDER).await,
                Vec::<String>::new(),
                "{seed}"
            );
            assert!(daemon.db.replica_deletes().unwrap().is_empty(), "{seed}");
            assert!(daemon.item(uid).is_none(), "{seed}: replica row");
            assert!(
                daemon.db.content_objects_of(uid).unwrap().is_empty(),
                "{seed}: object rows"
            );
            for object in objects {
                assert!(
                    !case.object_file(object).exists(),
                    "{seed}: object file {object} left"
                );
            }
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &Outcome::Gone).await;
        });
        assert!(placed(&case.log).is_empty(), "{seed}");
    }
}

/// R4c
#[test]
fn case_a_delete_in_a_never_listed_folder_removes_the_temporary_upload_before_the_target() {
    const INBOX: &str = "docs/inbox";
    for seed in SEEDS {
        let case = Case::new(seed);
        runtime().block_on(async {
            let (daemon, uid) = case.start(case.recording()).await;
            let inbox = daemon
                .cache
                .create_folder(daemon.folder_uid(uid), &name_of(INBOX))
                .await
                .unwrap()
                .uid;
            daemon
                .cache
                .rename(uid, inbox, &name_of(TARGET))
                .await
                .unwrap();
            case.share.inject(MockOp::Replace, Fault::Busy);
            daemon.save(uid, EDIT).await;
            until("a busy rename-over", || {
                calls(&case.log, MockOp::Replace) >= 1
            })
            .await;
            let_run().await;
            let temp = recorded_temp(&daemon, uid, INBOX);
            let mut expected = vec![name_of(&temp), name_of(TARGET)];
            expected.sort();
            assert_eq!(
                remote_names(&case.share, INBOX).await,
                expected,
                "{seed}: the temporary upload is not in the created folder"
            );
            let deleted = daemon.cache.delete(uid).await;
            assert!(deleted.is_ok(), "{seed}: {deleted:?}");
            assert!(
                !daemon.item(inbox).unwrap().listed,
                "{seed}: the created folder was listed"
            );
            assert!(
                !daemon
                    .db
                    .replica_scan_folders(daemon.backend.id())
                    .unwrap()
                    .contains(&inbox),
                "{seed}: the created folder is in the scan set"
            );
            assert_eq!(
                case.log.deleted(),
                [temp, format!("{INBOX}/{}", name_of(TARGET))],
                "{seed}: deletes applied"
            );
            assert_eq!(
                remote_names(&case.share, INBOX).await,
                Vec::<String>::new(),
                "{seed}: a name is left in the never-listed folder"
            );
            assert!(daemon.item(uid).is_none(), "{seed}: replica row");
            assert!(daemon.db.replica_deletes().unwrap().is_empty(), "{seed}");
            tokio::time::sleep(SETTLE).await;
            assert_eq!(
                remote_names(&case.share, INBOX).await,
                Vec::<String>::new(),
                "{seed}: a name came back in the never-listed folder"
            );
            assert!(daemon.item(inbox).is_some(), "{seed}: folder lost");
        });
        assert!(placed(&case.log).is_empty(), "{seed}");
    }
}

#[derive(Debug, Clone, Copy)]
enum KilledAfter {
    TemporaryRemoved,
    TargetDeleted,
}

fn kill_during_a_delete_with_a_temporary_upload(
    case: &Case,
    killed_after: KilledAfter,
) -> (u64, String) {
    let seed = case.seed;
    runtime().block_on(async {
        let backend = case.recording();
        let (daemon, uid, temp) = start_recorded(case, &backend).await;
        match killed_after {
            KilledAfter::TemporaryRemoved => backend.freeze_after_delete_of(&temp),
            KilledAfter::TargetDeleted => backend.freeze_after_delete_of(TARGET),
        }
        let _deleting = daemon.spawn_delete(uid);
        backend.frozen().await;
        match killed_after {
            KilledAfter::TemporaryRemoved => {
                assert_eq!(
                    case.log.deleted(),
                    std::slice::from_ref(&temp),
                    "{seed}: deletes at the kill"
                );
                assert_eq!(
                    remote_names(&case.share, FOLDER).await,
                    [name_of(TARGET)],
                    "{seed}: backend at the kill"
                );
            }
            KilledAfter::TargetDeleted => {
                assert_eq!(
                    case.log.deleted(),
                    [temp.clone(), TARGET.to_owned()],
                    "{seed}: deletes at the kill"
                );
                assert_eq!(
                    remote_names(&case.share, FOLDER).await,
                    Vec::<String>::new(),
                    "{seed}: backend at the kill"
                );
            }
        }
        (uid, temp)
    })
}

/// R4d, R2
fn restart_reachable_finishes_the_delete(killed_after: KilledAfter) {
    for seed in SEEDS {
        let case = Case::new(seed);
        let (uid, _temp) = kill_during_a_delete_with_a_temporary_upload(&case, killed_after);
        runtime().block_on(async {
            let daemon = case.disk.start(case.recording()).await.unwrap();
            assert!(
                daemon.db.replica_deletes().unwrap().is_empty(),
                "{seed}: recovery kept the mark"
            );
            assert!(daemon.item(uid).is_none(), "{seed}: replica row");
            tokio::time::sleep(SETTLE).await;
            case.assert_outcome(&daemon, uid, &Outcome::Gone).await;
        });
        assert!(placed(&case.log).is_empty(), "{seed}");
    }
}

/// R4d, R3: a restart whose first backend call fails with `fault` drops the
/// mark and uploads the edit.
fn restart_failing_keeps_the_edit(case: &Case, uid: u64, fault: Fault) {
    let what = format!("{} {fault:?}", case.seed);
    case.share.inject(MockOp::Metadata, fault);
    runtime().block_on(async {
        let daemon = case.disk.start(case.recording()).await.unwrap();
        assert!(
            daemon.db.replica_deletes().unwrap().is_empty(),
            "{what}: recovery kept the mark"
        );
        assert!(daemon.dirty_object(uid).is_some(), "{what}: edit lost");
        case.settle_without_deletes(&what).await;
        case.assert_outcome(&daemon, uid, &at(TARGET, EDIT)).await;
    });
    assert_eq!(placed(&case.log), [EDIT], "{what}");
}

fn restart_unreachable_keeps_the_edit(killed_after: KilledAfter) {
    for seed in SEEDS {
        let case = Case::new(seed);
        let (uid, _temp) = kill_during_a_delete_with_a_temporary_upload(&case, killed_after);
        restart_failing_keeps_the_edit(&case, uid, Fault::Unreachable);
    }
}

#[test]
fn case_a_delete_killed_after_its_temporary_upload_was_removed_is_finished_by_recovery() {
    restart_reachable_finishes_the_delete(KilledAfter::TemporaryRemoved);
}

#[test]
fn case_a_delete_killed_after_its_temporary_upload_was_removed_unreachable_at_restart_keeps_the_edit()
 {
    restart_unreachable_keeps_the_edit(KilledAfter::TemporaryRemoved);
}

#[test]
fn case_a_delete_killed_after_both_backend_deletes_is_finished_by_recovery() {
    restart_reachable_finishes_the_delete(KilledAfter::TargetDeleted);
}

#[test]
fn case_a_delete_killed_after_both_backend_deletes_unreachable_at_restart_keeps_the_edit() {
    restart_unreachable_keeps_the_edit(KilledAfter::TargetDeleted);
}
