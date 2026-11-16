#![cfg(all(feature = "native", unix))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cascade_engine::backend::Backend;
use cascade_engine::content::cache::{ContentCache, Handoff, RangeRequest};
use cascade_engine::content::replica::ReplicaStore;
use cascade_engine::content::transfer::{TransferConfig, TransferError};
use cascade_engine::db::StateDb;
use cascade_engine::db::content::ContentState;
use cascade_engine::testing::{Fault, MockBackend, MockOp};
use cascade_engine::types::{Change, Cursor, FileEntry, FileId, Quota, Revision};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const ALIGNMENT: u64 = 4 * KIB;
const REQUEST_SIZE: u64 = 64 * KIB;
const READ_AHEAD: u64 = 256 * KIB;
const SLOW_READ: Duration = Duration::from_secs(1);

/// Forwards to the mock and records every byte range fetched from it.
#[derive(Debug)]
struct Counting {
    inner: Arc<MockBackend>,
    id: String,
    fetched: Mutex<Vec<Range<u64>>>,
}

impl Counting {
    fn new(inner: &Arc<MockBackend>) -> Arc<Self> {
        Self::named(inner, inner.id())
    }

    /// Forwards to `inner` under the backend id `id`.
    fn named(inner: &Arc<MockBackend>, id: &str) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::clone(inner),
            id: id.to_owned(),
            fetched: Mutex::new(Vec::new()),
        })
    }

    fn fetched(&self) -> Vec<Range<u64>> {
        self.fetched.lock().unwrap().clone()
    }

    fn fetched_bytes(&self) -> u64 {
        self.fetched()
            .iter()
            .map(|range| range.end.saturating_sub(range.start))
            .sum()
    }
}

#[async_trait]
impl Backend for Counting {
    fn id(&self) -> &str {
        &self.id
    }

    fn display_name(&self) -> &str {
        self.inner.display_name()
    }

    async fn quota(&self) -> anyhow::Result<Option<Quota>> {
        self.inner.quota().await
    }

    async fn changes(&self, cursor: Option<&Cursor>) -> anyhow::Result<(Vec<Change>, Cursor)> {
        self.inner.changes(cursor).await
    }

    async fn metadata(&self, path: &Path) -> anyhow::Result<FileEntry> {
        self.inner.metadata(path).await
    }

    async fn download(&self, file: &FileEntry) -> anyhow::Result<Vec<u8>> {
        self.fetched.lock().unwrap().push(0..u64::MAX);
        self.inner.download(file).await
    }

    async fn read_range(
        &self,
        file: &FileEntry,
        offset: u64,
        length: u32,
    ) -> anyhow::Result<Vec<u8>> {
        let bytes = self.inner.read_range(file, offset, length).await?;
        let len = u64::try_from(bytes.len()).unwrap();
        self.fetched.lock().unwrap().push(offset..offset + len);
        Ok(bytes)
    }

    async fn upload(
        &self,
        path: &Path,
        data: &[u8],
        parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        self.inner.upload(path, data, parent_id).await
    }

    async fn update(&self, file_id: &FileId, data: &[u8]) -> anyhow::Result<FileEntry> {
        self.inner.update(file_id, data).await
    }

    async fn create_dir(&self, path: &Path) -> anyhow::Result<FileEntry> {
        self.inner.create_dir(path).await
    }

    async fn create_dir_with_parent(
        &self,
        name: &str,
        parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        self.inner.create_dir_with_parent(name, parent_id).await
    }

    async fn delete(&self, file: &FileEntry) -> anyhow::Result<()> {
        self.inner.delete(file).await
    }

    async fn move_entry(&self, src: &Path, dst: &Path) -> anyhow::Result<FileEntry> {
        self.inner.move_entry(src, dst).await
    }

    async fn move_by_id(
        &self,
        src_id: &FileId,
        dst_parent_id: &FileId,
        new_name: &str,
    ) -> anyhow::Result<FileEntry> {
        self.inner.move_by_id(src_id, dst_parent_id, new_name).await
    }

    async fn list_children(&self, parent_native_id: &str) -> anyhow::Result<Vec<FileEntry>> {
        self.inner.list_children(parent_native_id).await
    }

    async fn revision(&self, entry: &FileEntry) -> anyhow::Result<Revision> {
        self.inner.revision(entry).await
    }

    async fn replace(&self, source: &Path, target: &Path) -> anyhow::Result<(FileEntry, Revision)> {
        self.inner.replace(source, target).await
    }

    async fn poll_interval(&self) -> Option<Duration> {
        self.inner.poll_interval().await
    }
}

fn pattern(len: u64, salt: u64) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from((i.wrapping_mul(2_654_435_761) >> 7 ^ salt) & 0xff).unwrap())
        .collect()
}

fn slice(data: &[u8], range: Range<u64>) -> &[u8] {
    data.get(usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap())
        .unwrap()
}

fn read_file(path: &Path, range: Range<u64>) -> Vec<u8> {
    let file = std::fs::File::open(path).unwrap();
    let mut buf = vec![0; usize::try_from(range.end - range.start).unwrap()];
    file.read_exact_at(&mut buf, range.start).unwrap();
    buf
}

const fn overlaps(a: &Range<u64>, b: &Range<u64>) -> bool {
    a.start < b.end && b.start < a.end
}

fn range_request(uid: u64, version: Option<&str>, strict: bool, offset: u64) -> RangeRequest {
    RangeRequest {
        uid,
        requested_version: version.map(str::to_owned),
        strict,
        offset,
        length: ALIGNMENT,
        alignment: ALIGNMENT,
    }
}

/// The first download call answers at once; every later one takes
/// [`SLOW_READ`], so a download is still running when the test acts.
fn slow_after_first_read(share: &MockBackend) {
    share.inject(MockOp::Download, Fault::Delay(Duration::ZERO));
    for _ in 0..256 {
        share.inject(MockOp::Download, Fault::Delay(SLOW_READ));
    }
}

struct Fixture {
    db: Arc<StateDb>,
    store: ReplicaStore,
    cache_dir: tempfile::TempDir,
    staging_dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let db = Arc::new(StateDb::open_in_memory().unwrap());
        let store = ReplicaStore::open(Arc::clone(&db)).unwrap();
        Self {
            db,
            store,
            cache_dir: tempfile::tempdir().unwrap(),
            staging_dir: tempfile::tempdir().unwrap(),
        }
    }

    async fn uid(&self, backend: Arc<dyn Backend>, path: &str) -> u64 {
        self.store
            .replica(backend)
            .unwrap()
            .resolve(Path::new(path))
            .await
            .unwrap()
            .expect("seeded file resolves")
    }

    fn content_version(&self, uid: u64) -> String {
        self.store.item(uid).unwrap().unwrap().content_version
    }

    fn cache(&self, backend: Arc<dyn Backend>) -> ContentCache {
        let config = TransferConfig {
            read_ahead: READ_AHEAD,
            request_size: REQUEST_SIZE,
            ..TransferConfig::default()
        };
        let cache = ContentCache::open(
            self.store.clone(),
            self.cache_dir.path().to_path_buf(),
            self.staging_dir.path().to_path_buf(),
            config,
        )
        .unwrap();
        cache.register(backend).unwrap();
        cache
    }

    fn staging(&self) -> PathBuf {
        self.staging_dir.path().to_path_buf()
    }
}

#[tokio::test(start_paused = true)]
async fn ranged_read_fetches_only_its_window_and_a_concurrent_reader_shares_the_fetch() {
    let size = 8 * MIB;
    let data = pattern(size, 0x5a);
    let share = Arc::new(MockBackend::new(17));
    share.add_file("media/movie.mkv", &data);
    let counting = Counting::new(&share);
    let fixture = Fixture::new();
    let uid = fixture.uid(counting.clone(), "media/movie.mkv").await;
    let cache = fixture.cache(counting.clone());

    let first = 6 * MIB + 1000..6 * MIB + 101_000;
    let second = 6 * MIB + 50_000..6 * MIB + 90_000;
    let len = |range: &Range<u64>| u32::try_from(range.end - range.start).unwrap();
    let stop = share.stop_at(MockOp::Download);
    let (first_bytes, second_bytes) =
        tokio::join!(cache.read(uid, first.start, len(&first)), async {
            stop.reached().await;
            let (second_bytes, ()) =
                tokio::join!(cache.read(uid, second.start, len(&second)), async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    stop.release();
                });
            second_bytes
        });
    assert_eq!(first_bytes.unwrap(), slice(&data, first.clone()));
    assert_eq!(second_bytes.unwrap(), slice(&data, second.clone()));

    tokio::time::sleep(Duration::from_mins(1)).await;
    let fetched = counting.fetched();
    let window = first.start.saturating_sub(REQUEST_SIZE)..first.end + READ_AHEAD + REQUEST_SIZE;
    for range in &fetched {
        assert!(
            range.start >= window.start && range.end <= window.end,
            "fetched {range:?} outside the read and its read-ahead window {window:?}"
        );
    }
    for (index, a) in fetched.iter().enumerate() {
        for b in fetched.iter().skip(index + 1) {
            assert!(
                !overlaps(a, b),
                "bytes {a:?} and {b:?} were fetched twice instead of shared: {fetched:?}"
            );
        }
    }
    let fetched_bytes = counting.fetched_bytes();
    assert!(
        fetched_bytes < 2 * MIB,
        "a 100 KB read fetched {fetched_bytes} bytes; no full download may start"
    );

    let again = cache.read(uid, first.start, len(&first)).await.unwrap();
    assert_eq!(again, slice(&data, first));
    assert_eq!(
        counting.fetched_bytes(),
        fetched_bytes,
        "a cached range is read again from the backend"
    );
}

#[derive(Debug, Clone, Copy)]
enum RemoteChange {
    InPlaceKeepingSizeAndMtime,
    ReplacedByRename,
}

#[tokio::test(start_paused = true)]
async fn remote_changed_during_download_never_yields_mixed_content() {
    for change in [
        RemoteChange::InPlaceKeepingSizeAndMtime,
        RemoteChange::ReplacedByRename,
    ] {
        let size = MIB;
        let old = pattern(size, 0x11);
        let new = pattern(size, 0xe4);
        let share = Arc::new(MockBackend::new(29));
        share.add_file("docs/report.bin", &old);
        let fixture = Fixture::new();
        let uid = fixture.uid(share.clone(), "docs/report.bin").await;
        let old_version = fixture.content_version(uid);
        let cache = fixture.cache(share.clone());
        slow_after_first_read(&share);

        let head: Handoff = cache
            .fetch_range(&range_request(uid, Some(&old_version), false, 0))
            .await
            .unwrap();
        assert!(head.path.starts_with(fixture.staging()), "{change:?}");
        assert_eq!(head.content_version, old_version, "{change:?}");
        assert_eq!(
            read_file(&head.path, 0..ALIGNMENT),
            slice(&old, 0..ALIGNMENT)
        );

        match change {
            RemoteChange::InPlaceKeepingSizeAndMtime => share
                .rewrite_keeping_size_and_mtime("docs/report.bin", &new)
                .unwrap(),
            RemoteChange::ReplacedByRename => {
                share.add_file("docs/.report.bin.new", &new);
                share.remove("docs/report.bin").unwrap();
                share
                    .rename("docs/.report.bin.new", "docs/report.bin")
                    .unwrap();
            }
        }

        let tail_offset = size - ALIGNMENT;
        let tail = cache
            .fetch_range(&range_request(uid, None, false, tail_offset))
            .await;
        match (change, tail) {
            (_, Ok(tail)) => {
                assert!(
                    tail.range.start <= tail_offset && tail.range.end >= size,
                    "{change:?}: {:?}",
                    tail.range
                );
                assert_eq!(
                    read_file(&tail.path, tail_offset..size),
                    slice(&new, tail_offset..size),
                    "{change:?}: the tail must be the new version's bytes"
                );
                assert_eq!(
                    read_file(&tail.path, 0..ALIGNMENT),
                    slice(&new, 0..ALIGNMENT),
                    "{change:?}: a handoff mixes bytes of both versions"
                );
                assert_ne!(tail.content_version, old_version, "{change:?}");
                if matches!(change, RemoteChange::InPlaceKeepingSizeAndMtime) {
                    assert_eq!(fixture.content_version(uid), tail.content_version);
                }
            }
            (RemoteChange::InPlaceKeepingSizeAndMtime, Err(error)) => {
                panic!("a non-strict fetch must restart on the new version: {error:#}")
            }
            (RemoteChange::ReplacedByRename, Err(_)) => {}
        }

        assert_eq!(
            read_file(&head.path, 0..ALIGNMENT),
            slice(&old, 0..ALIGNMENT),
            "{change:?}: a handed-out clone changed after the remote did"
        );
        for object in fixture.db.content_objects_of(uid).unwrap() {
            assert_ne!(
                object.content_version, old_version,
                "{change:?}: the object of the replaced version was kept"
            );
        }

        if matches!(change, RemoteChange::InPlaceKeepingSizeAndMtime) {
            let error = cache
                .fetch_range(&range_request(uid, Some(&old_version), true, 0))
                .await
                .unwrap_err();
            assert!(
                matches!(
                    error.downcast_ref::<TransferError>(),
                    Some(TransferError::VersionNoLongerAvailable)
                ),
                "a strict fetch of the gone version: {error:#}"
            );
        }
    }
}

#[test]
fn restart_after_kill_fetches_only_ranges_absent_from_the_database() {
    let size = 4 * MIB;
    let data = pattern(size, 0x3c);
    let share = Arc::new(MockBackend::new(41));
    share.add_file("video/clip.mov", &data);
    let fixture = Fixture::new();
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap()
    };

    let (uid, object_id, recorded) = runtime().block_on(async {
        let uid = fixture.uid(share.clone(), "video/clip.mov").await;
        let cache = fixture.cache(share.clone());
        slow_after_first_read(&share);
        cache
            .fetch_range(&range_request(uid, None, false, 0))
            .await
            .unwrap();
        let objects = fixture.db.content_objects_of(uid).unwrap();
        assert_eq!(objects.len(), 1, "one object per download");
        let object = objects.first().unwrap();
        assert_eq!(object.state, ContentState::Partial);
        let recorded = fixture.db.content_ranges(object.object_id).unwrap();
        (uid, object.object_id, recorded)
    });
    assert!(
        recorded
            .iter()
            .any(|range| range.start == 0 && range.end >= ALIGNMENT),
        "a handed-out range is recorded before the kill: {recorded:?}"
    );

    let counting = Counting::new(&share);
    runtime().block_on(async {
        let cache = fixture.cache(counting.clone());
        let whole = cache.fetch(uid, None).await.unwrap();
        assert_eq!(whole.range, 0..size);
        assert_eq!(read_file(&whole.path, 0..size), data);
    });

    let fetched = counting.fetched();
    assert!(!fetched.is_empty(), "the missing ranges were not fetched");
    for range in &fetched {
        for present in &recorded {
            assert!(
                !overlaps(range, present),
                "fetched {range:?} again although {present:?} was in the database"
            );
        }
    }
    let objects = fixture.db.content_objects_of(uid).unwrap();
    assert_eq!(objects.len(), 1);
    let object = objects.first().unwrap();
    assert_eq!(
        object.object_id, object_id,
        "the download resumes its object"
    );
    assert_eq!(object.state, ContentState::Complete);
}

#[tokio::test(start_paused = true)]
async fn an_item_is_read_from_the_backend_that_holds_it() {
    let size = 256 * KIB;
    let a_data = pattern(size, 0x0a);
    let b_data = pattern(size, 0xb0);
    let a_share = Arc::new(MockBackend::new(51));
    let b_share = Arc::new(MockBackend::new(53));
    a_share.add_file("docs/same.bin", &a_data);
    b_share.add_file("docs/same.bin", &b_data);
    let a = Counting::named(&a_share, "nas-a");
    let b = Counting::named(&b_share, "nas-b");
    let fixture = Fixture::new();
    let a_uid = fixture.uid(a.clone(), "docs/same.bin").await;
    let b_uid = fixture.uid(b.clone(), "docs/same.bin").await;
    let a_docs = fixture
        .store
        .item(a_uid)
        .unwrap()
        .unwrap()
        .parent_uid
        .unwrap();
    let a_rows = || {
        (
            fixture.store.item(a_uid).unwrap(),
            fixture.db.replica_children(a_docs).unwrap(),
        )
    };
    let before = a_rows();

    let only_a = fixture.cache(a.clone());
    let unregistered = only_a.read(b_uid, 0, 4096).await;
    assert!(
        unregistered.is_err(),
        "an item of an unregistered backend was read through another backend"
    );
    assert!(b.fetched().is_empty());

    let cache = fixture.cache(a.clone());
    cache.register(b.clone()).unwrap();
    let read = cache
        .read(b_uid, 0, u32::try_from(size).unwrap())
        .await
        .unwrap();
    let whole = cache.fetch(b_uid, None).await.unwrap();

    assert_eq!(read, b_data);
    assert_eq!(read_file(&whole.path, 0..size), b_data);
    assert!(a.fetched().is_empty(), "B's item was fetched from A");
    assert_eq!(a_rows(), before, "A's replica rows changed");
}
