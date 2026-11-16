use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast::error::TryRecvError;
use tokio::time::Instant;

use super::{ContentCache, RangeRequest};
use crate::backend::{Backend, TransientError};
use crate::content::replica::ReplicaStore;
use crate::content::transfer::{CacheSignal, TransferConfig, TransferError};
use crate::db::StateDb;
use crate::db::content::ContentState;
use crate::testing::{Fault, MockBackend, MockOp};

const KIB: u64 = 1024;
const CHUNK: u64 = 64 * KIB;
const SLOW_READ: Duration = Duration::from_secs(1);

fn pattern(len: u64, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap() ^ salt)
        .collect()
}

fn slow_reads(share: &MockBackend) {
    for _ in 0..512 {
        share.inject(MockOp::Download, Fault::Delay(SLOW_READ));
    }
}

struct Fixture {
    db: Arc<StateDb>,
    store: ReplicaStore,
    share: Arc<MockBackend>,
    cache: ContentCache,
    cache_dir: tempfile::TempDir,
    _staging_dir: tempfile::TempDir,
}

impl Fixture {
    fn new(config: TransferConfig) -> Self {
        let db = Arc::new(StateDb::open_in_memory().unwrap());
        let store = ReplicaStore::open(Arc::clone(&db)).unwrap();
        let share = Arc::new(MockBackend::new(7));
        let cache_dir = tempfile::tempdir().unwrap();
        let staging_dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn Backend> = share.clone();
        let cache = ContentCache::open(
            store.clone(),
            cache_dir.path().to_path_buf(),
            staging_dir.path().to_path_buf(),
            config,
        )
        .unwrap();
        cache.register(backend).unwrap();
        Self {
            db,
            store,
            share,
            cache,
            cache_dir,
            _staging_dir: staging_dir,
        }
    }

    async fn add(&self, path: &str, data: &[u8]) -> u64 {
        self.share.add_file(path, data);
        let backend: Arc<dyn Backend> = self.share.clone();
        self.store
            .replica(backend)
            .unwrap()
            .resolve(Path::new(path))
            .await
            .unwrap()
            .unwrap()
    }

    fn version(&self, uid: u64) -> String {
        self.store.item(uid).unwrap().unwrap().content_version
    }
}

fn config() -> TransferConfig {
    TransferConfig {
        read_ahead: CHUNK,
        request_size: CHUNK,
        ..TransferConfig::default()
    }
}

fn request(uid: u64, offset: u64) -> RangeRequest {
    RangeRequest {
        uid,
        requested_version: None,
        strict: false,
        offset,
        length: 4 * KIB,
        alignment: 4 * KIB,
    }
}

#[tokio::test(start_paused = true)]
async fn waiter_ranges_are_served_before_the_sequential_fill() {
    let fixture = Fixture::new(config());
    let uid = fixture.add("big.bin", &pattern(32 * CHUNK, 1)).await;
    slow_reads(&fixture.share);
    let started = Instant::now();

    let (late, early) = (request(uid, 28 * CHUNK), request(uid, 4 * CHUNK));
    let (late, early) = tokio::join!(
        fixture.cache.fetch_range(&late),
        fixture.cache.fetch_range(&early),
    );

    late.unwrap();
    early.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "both waiters took {:?}; the sequential fill reached them first",
        started.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn a_download_nobody_waits_on_stops_when_idle() {
    let fixture = Fixture::new(TransferConfig {
        idle_stop: Duration::from_secs(5),
        ..config()
    });
    let uid = fixture.add("idle.bin", &pattern(64 * CHUNK, 2)).await;
    slow_reads(&fixture.share);

    fixture.cache.fetch_range(&request(uid, 0)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(10)).await;
    let object = fixture.db.content_objects_of(uid).unwrap().remove(0);
    let stopped = fixture.db.content_ranges(object.object_id).unwrap();
    tokio::time::sleep(Duration::from_mins(2)).await;

    assert_eq!(
        fixture.db.content_ranges(object.object_id).unwrap(),
        stopped
    );
    assert_eq!(
        fixture
            .db
            .content_object(object.object_id)
            .unwrap()
            .unwrap()
            .state,
        ContentState::Partial
    );
}

#[tokio::test(start_paused = true)]
async fn too_many_errors_fail_the_waiters() {
    let fixture = Fixture::new(TransferConfig {
        max_errors: 2,
        ..config()
    });
    let uid = fixture.add("flaky.bin", &pattern(4 * CHUNK, 3)).await;
    for _ in 0..3 {
        fixture.share.inject(MockOp::Download, Fault::Busy);
    }

    let error = fixture
        .cache
        .fetch_range(&request(uid, 0))
        .await
        .unwrap_err();

    assert!(
        matches!(
            error.downcast_ref::<TransientError>(),
            None | Some(TransientError::Busy(_))
        ),
        "{error:#}"
    );
    fixture.cache.fetch_range(&request(uid, 0)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_first_success_after_an_unreachable_failure_resolves_the_error() {
    let fixture = Fixture::new(config());
    let uid = fixture.add("nas.bin", &pattern(4 * CHUNK, 4)).await;
    let mut signals = fixture.cache.subscribe();
    fixture.share.inject(MockOp::Download, Fault::Unreachable);

    let error = fixture
        .cache
        .fetch_range(&request(uid, 0))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.downcast_ref::<TransientError>(),
            Some(TransientError::Unreachable(_))
        ),
        "{error:#}"
    );
    assert_eq!(signals.try_recv(), Err(TryRecvError::Empty));

    fixture.cache.fetch_range(&request(uid, 0)).await.unwrap();
    fixture
        .cache
        .fetch_range(&request(uid, CHUNK))
        .await
        .unwrap();

    assert_eq!(signals.try_recv(), Ok(CacheSignal::ErrorResolved));
    assert_eq!(signals.try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test(start_paused = true)]
async fn a_complete_object_is_handed_out_without_the_backend() {
    let fixture = Fixture::new(config());
    let data = pattern(4 * CHUNK, 5);
    let uid = fixture.add("done.bin", &data).await;
    let whole = fixture.cache.fetch(uid, None).await.unwrap();
    for op in [
        MockOp::Metadata,
        MockOp::Revision,
        MockOp::Download,
        MockOp::ListChildren,
    ] {
        fixture.share.inject(op, Fault::Unreachable);
    }

    let again = fixture
        .cache
        .fetch_range(&request(uid, 2 * CHUNK))
        .await
        .unwrap();
    let read = fixture.cache.read(uid, CHUNK, 100).await.unwrap();

    assert_eq!(again.content_version, whole.content_version);
    assert_eq!(std::fs::read(&again.path).unwrap(), data);
    assert_eq!(read, data.get(64 * 1024..64 * 1024 + 100).unwrap());
}

#[tokio::test(start_paused = true)]
async fn a_change_seen_after_the_last_read_discards_the_object() {
    let fixture = Fixture::new(config());
    let uid = fixture.add("moving.bin", &pattern(4 * CHUNK, 6)).await;
    let old_version = fixture.version(uid);
    slow_reads(&fixture.share);

    fixture.cache.fetch_range(&request(uid, 0)).await.unwrap();
    fixture
        .share
        .rewrite_keeping_size_and_mtime("moving.bin", &pattern(4 * CHUNK, 7))
        .unwrap();
    tokio::time::sleep(Duration::from_mins(1)).await;

    assert!(fixture.db.content_objects_of(uid).unwrap().is_empty());
    assert_ne!(fixture.version(uid), old_version);
}

fn assert_no_object_of(fixture: &Fixture, uid: u64, version: &str) {
    for object in fixture.db.content_objects_of(uid).unwrap() {
        assert_ne!(
            object.content_version, version,
            "the object of the replaced version was kept"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_read_across_pieces_of_two_versions_discards_the_object() {
    let fixture = Fixture::new(config());
    let old = pattern(4 * CHUNK, 8);
    let new = pattern(4 * CHUNK, 9);
    let uid = fixture.add("torn.bin", &old).await;
    let old_version = fixture.version(uid);
    slow_reads(&fixture.share);

    fixture.cache.fetch_range(&request(uid, 0)).await.unwrap();
    fixture
        .share
        .rewrite_keeping_size_and_mtime("torn.bin", &new)
        .unwrap();
    tokio::time::sleep(SLOW_READ + SLOW_READ / 2).await;
    let read = fixture.cache.read(uid, CHUNK - 100, 200).await.unwrap();

    let span = usize::try_from(CHUNK - 100).unwrap()..usize::try_from(CHUNK + 100).unwrap();
    assert_eq!(read, new.get(span).unwrap(), "a read mixed two versions");
    assert_no_object_of(&fixture, uid, &old_version);
}

#[tokio::test(start_paused = true)]
async fn a_remote_shrunk_during_a_download_restarts_on_the_new_version() {
    let fixture = Fixture::new(config());
    let uid = fixture.add("shrinking.bin", &pattern(4 * CHUNK, 10)).await;
    let old_version = fixture.version(uid);
    slow_reads(&fixture.share);

    fixture.cache.fetch_range(&request(uid, 0)).await.unwrap();
    fixture.share.add_file("shrinking.bin", &pattern(CHUNK, 11));
    let tail = fixture.cache.fetch_range(&request(uid, 3 * CHUNK)).await;

    match tail {
        Ok(handoff) => assert_ne!(handoff.content_version, old_version),
        Err(error) => assert!(
            matches!(
                error.downcast_ref::<TransferError>(),
                Some(TransferError::VersionNoLongerAvailable)
            ),
            "{error:#}"
        ),
    }
    assert_no_object_of(&fixture, uid, &old_version);
}

#[tokio::test(start_paused = true)]
async fn a_failed_object_file_create_leaves_no_row() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::new(config());
    let uid = fixture.add("blocked.bin", &pattern(4 * CHUNK, 12)).await;
    let cache_dir = fixture.cache_dir.path();
    std::fs::set_permissions(cache_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let result = fixture.cache.fetch_range(&request(uid, 0)).await;
    std::fs::set_permissions(cache_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(result.is_err());
    assert!(fixture.db.content_objects_of(uid).unwrap().is_empty());
}
