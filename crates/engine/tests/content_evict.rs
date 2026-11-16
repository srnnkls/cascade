#![cfg(all(feature = "native", unix))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cascade_config::types::{PinRule, ResolvedConfig};
use cascade_engine::backend::{Backend, MountedBackend};
use cascade_engine::cache::manager::{CacheManager, CacheManagerConfig};
use cascade_engine::content::evict::EvictError;
use cascade_engine::content::replica::ReplicaStore;
use cascade_engine::content::transfer::{CacheSignal, TransferConfig};
use cascade_engine::db::content::ContentState;
use cascade_engine::engine::{Engine, EngineConfig};
use cascade_engine::portable::native::{SqliteStorage, TokioRuntimeHandle};
use cascade_engine::testing::{
    BackendLog, Daemon, Disk, MockBackend, MockOp, RecordingBackend, StopPoint,
};
use tokio::task::JoinHandle;

const SEEDS: [u64; 4] = [3, 17, 29, 64];
/// A divisor of every ranged read the tests make, so a read fetches only the
/// bytes it names.
const REQUEST_SIZE: u64 = 20;
/// Longer than the idle stop of a download, so no transfer outlives the
/// access that started it.
const BETWEEN_ACCESSES: Duration = Duration::from_mins(2);

fn pattern(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap() ^ salt)
        .collect()
}

struct Fixture {
    root: tempfile::TempDir,
    disk: Disk,
    share: Arc<MockBackend>,
    log: BackendLog,
    daemon: Daemon,
}

impl Fixture {
    async fn start(share: &Arc<MockBackend>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = TransferConfig {
            read_ahead: 0,
            request_size: REQUEST_SIZE,
            call_timeout: Duration::from_hours(1),
            ..TransferConfig::default()
        };
        let disk = Disk::at(root.path(), config).unwrap();
        let log = BackendLog::default();
        let daemon = disk
            .start(RecordingBackend::new(share, &log))
            .await
            .unwrap();
        Self {
            root,
            disk,
            share: Arc::clone(share),
            log,
            daemon,
        }
    }

    /// Stops the daemon and starts it again over the same disk.
    async fn restart(&mut self) {
        let backend = RecordingBackend::new(&self.share, &self.log);
        self.daemon = self.disk.start(backend).await.unwrap();
    }

    async fn uid(&self, path: &str) -> u64 {
        self.daemon
            .resolve(path)
            .await
            .unwrap()
            .expect("seeded file resolves")
    }

    fn object_files(&self, uid: u64) -> Vec<PathBuf> {
        self.daemon
            .db
            .content_objects_of(uid)
            .unwrap()
            .into_iter()
            .map(|object| {
                self.root
                    .path()
                    .join("cache")
                    .join(object.object_id.to_string())
            })
            .collect()
    }

    fn cached(&self, uid: u64) -> bool {
        !self.daemon.db.content_objects_of(uid).unwrap().is_empty()
    }

    fn populated(&self) -> u64 {
        self.daemon.db.content_populated_bytes().unwrap()
    }

    async fn save(&self, uid: u64, data: &[u8]) {
        let incoming = self.daemon.incoming_file(data).unwrap();
        self.daemon.cache.save(uid, &incoming).await.unwrap();
        let _ = std::fs::remove_file(&incoming);
    }
}

fn assert_files_gone(files: &[PathBuf], what: &str) {
    for file in files {
        assert!(!file.exists(), "{what}: {} still on disk", file.display());
    }
}

#[tokio::test(start_paused = true)]
async fn a_sweep_removes_clean_closed_unpinned_objects_in_lru_order_counting_populated_bytes() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        for name in ["pinned", "a", "c", "d"] {
            share.add_file(&format!("lru/{name}"), &pattern(100, 1));
        }
        share.add_file("lru/b", &pattern(1000, 2));
        share.add_file("guarded/g", &pattern(100, 3));
        share.add_file("dirty/w", &pattern(100, 4));
        let fixture = Fixture::start(&share).await;
        let cache = &fixture.daemon.cache;
        let pinned = fixture.uid("lru/pinned").await;
        let [a, b, c, d] = [
            fixture.uid("lru/a").await,
            fixture.uid("lru/b").await,
            fixture.uid("lru/c").await,
            fixture.uid("lru/d").await,
        ];

        cache.set_max_size(10_000);
        cache.set_pinned(pinned, true).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.fetch(a, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        assert_eq!(cache.read(b, 0, 100).await.unwrap(), pattern(100, 2));
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.fetch(c, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.fetch(d, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        assert_eq!(cache.read(a, 0, 10).await.unwrap(), pattern(10, 1));
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        assert_eq!(
            fixture.populated(),
            500,
            "{seed}: pinned, a, c and d hold 100 bytes each, b 100 of 1000"
        );
        let evicted_files = [fixture.object_files(b), fixture.object_files(c)].concat();

        cache.set_max_size(350);
        let freed = cache.sweep().await.unwrap();

        assert_eq!(
            freed, 200,
            "{seed}: freed bytes are the populated bytes of b and c"
        );
        assert_eq!(fixture.populated(), 300, "{seed}");
        assert!(!fixture.cached(b), "{seed}: least recently used b kept");
        assert!(
            !fixture.cached(c),
            "{seed}: c kept although still over the limit"
        );
        assert_files_gone(&evicted_files, &format!("{seed}"));
        assert!(fixture.cached(a), "{seed}: a was read last and must stay");
        assert!(
            fixture.cached(d),
            "{seed}: d evicted after the limit was met"
        );
        assert!(
            fixture.cached(pinned),
            "{seed}: the pinned object was evicted"
        );
        let last_access = |uid: u64| {
            fixture
                .daemon
                .db
                .content_objects_of(uid)
                .unwrap()
                .first()
                .and_then(|object| object.last_access)
                .unwrap_or_else(|| panic!("{seed}: recency of {uid} not persisted"))
        };
        assert!(
            last_access(d) < last_access(a),
            "{seed}: the persisted recency misses the read of a"
        );

        let dirty = fixture.uid("dirty/w").await;
        cache.fetch(dirty, None).await.unwrap();
        let upload = share.stop_at(MockOp::Upload);
        fixture.save(dirty, b"unuploaded edit").await;
        let guarded = fixture.uid("guarded/g").await;
        assert_eq!(cache.read(guarded, 0, 40).await.unwrap(), pattern(40, 3));
        let download = share.stop_at(MockOp::Download);
        let reader = tokio::spawn({
            let cache = cache.clone();
            async move { cache.read(guarded, 60, 40).await }
        });
        download.reached().await;

        cache.set_max_size(0);
        cache.sweep().await.unwrap();

        assert!(
            !fixture.cached(a),
            "{seed}: a clean object survived a zero limit"
        );
        assert!(
            !fixture.cached(d),
            "{seed}: a clean object survived a zero limit"
        );
        assert!(
            fixture.cached(pinned),
            "{seed}: the pinned object was evicted"
        );
        assert!(
            fixture
                .daemon
                .db
                .dirty_content_objects()
                .unwrap()
                .iter()
                .any(|object| object.uid == dirty),
            "{seed}: the dirty object was evicted"
        );
        assert!(
            fixture.cached(guarded),
            "{seed}: an object with a live reader was evicted"
        );
        download.release();
        assert_eq!(
            reader.await.unwrap().unwrap(),
            pattern(100, 3).get(60..).unwrap(),
            "{seed}"
        );
        upload.release();
    }
}

#[tokio::test(start_paused = true)]
async fn a_pin_fetches_without_a_read_survives_a_zero_limit_and_is_refused_over_the_maximum() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("pins/kept", &pattern(600, 5));
        share.add_file("pins/loose", &pattern(100, 6));
        share.add_file("pins/over", &pattern(500, 7));
        let fixture = Fixture::start(&share).await;
        let cache = &fixture.daemon.cache;
        let kept = fixture.uid("pins/kept").await;
        let loose = fixture.uid("pins/loose").await;
        let over = fixture.uid("pins/over").await;
        cache.set_max_size(1000);

        cache.set_pinned(kept, true).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;

        assert!(
            fixture.daemon.store.item(kept).unwrap().unwrap().pinned,
            "{seed}"
        );
        let objects = fixture.daemon.db.content_objects_of(kept).unwrap();
        assert!(
            objects
                .iter()
                .any(|object| object.state == ContentState::Complete && object.size == 600),
            "{seed}: a pinned item is downloaded without being read: {objects:?}"
        );

        cache.fetch(loose, None).await.unwrap();
        cache.set_max_size(0);
        cache.sweep().await.unwrap();
        assert!(
            !fixture.cached(loose),
            "{seed}: the unpinned object survived"
        );
        assert_eq!(
            fixture.populated(),
            600,
            "{seed}: the pinned object lost bytes"
        );

        cache.set_max_size(1000);
        let refused = cache
            .set_pinned(over, true)
            .await
            .expect_err("pinned bytes would exceed the cache maximum");
        assert_eq!(
            refused.downcast_ref::<EvictError>(),
            Some(&EvictError::OutOfSpace),
            "{seed}: {refused:#}"
        );
        assert!(
            !fixture.daemon.store.item(over).unwrap().unwrap().pinned,
            "{seed}: a refused pin left the item pinned"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_materialised_object_is_signalled_not_freed_and_a_targeted_evict_removes_a_clean_one() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        for name in ["materialised", "plain", "removed", "edited"] {
            share.add_file(&format!("docs/{name}"), &pattern(100, 8));
        }
        let fixture = Fixture::start(&share).await;
        let cache = &fixture.daemon.cache;
        let materialised = fixture.uid("docs/materialised").await;
        let plain = fixture.uid("docs/plain").await;
        cache.fetch(materialised, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.fetch(plain, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.set_materialized(&[materialised]).await.unwrap();
        let materialised_files = fixture.object_files(materialised);
        let mut signals = cache.subscribe();

        cache.set_max_size(150);
        let freed = cache.sweep().await.unwrap();

        assert_eq!(
            freed, 100,
            "{seed}: only the plain object's bytes are freed"
        );
        assert!(
            !fixture.cached(plain),
            "{seed}: the sweep skipped the next candidate"
        );
        assert!(
            fixture.cached(materialised),
            "{seed}: a materialised object was deleted before leaving the set"
        );
        assert!(
            materialised_files.iter().all(|file| file.exists()),
            "{seed}"
        );
        assert_eq!(fixture.populated(), 100, "{seed}");
        let raised: Vec<CacheSignal> = std::iter::from_fn(|| signals.try_recv().ok()).collect();
        assert!(
            raised.contains(&CacheSignal::Evict(materialised)),
            "{seed}: no evict signal for the materialised item: {raised:?}"
        );

        cache.set_materialized(&[]).await.unwrap();
        assert!(
            !fixture.cached(materialised),
            "{seed}: the object stayed after its item left the materialised set"
        );
        assert_files_gone(&materialised_files, &format!("{seed}"));
        assert_eq!(fixture.populated(), 0, "{seed}");

        let removed = fixture.uid("docs/removed").await;
        cache.fetch(removed, None).await.unwrap();
        let removed_files = fixture.object_files(removed);
        cache.set_max_size(10_000);
        cache.evict(removed).await.unwrap();
        assert!(
            fixture
                .daemon
                .db
                .content_objects_of(removed)
                .unwrap()
                .is_empty(),
            "{seed}: a content_objects row remains after Remove Download"
        );
        assert_files_gone(&removed_files, &format!("{seed}"));

        let edited = fixture.uid("docs/edited").await;
        cache.fetch(edited, None).await.unwrap();
        let upload = share.stop_at(MockOp::Upload);
        fixture.save(edited, b"unuploaded edit").await;
        let busy = cache
            .evict(edited)
            .await
            .expect_err("a dirty object cannot be evicted");
        assert_eq!(
            busy.downcast_ref::<EvictError>(),
            Some(&EvictError::Busy),
            "{seed}: {busy:#}"
        );
        assert!(
            fixture
                .daemon
                .db
                .dirty_content_objects()
                .unwrap()
                .iter()
                .any(|object| object.uid == edited),
            "{seed}: the dirty object was removed"
        );
        upload.release();
    }
}

fn is_pinned(fixture: &Fixture, uid: u64) -> bool {
    fixture.daemon.store.item(uid).unwrap().unwrap().pinned
}

fn complete(fixture: &Fixture, uid: u64, size: u64) -> bool {
    fixture
        .daemon
        .db
        .content_objects_of(uid)
        .unwrap()
        .iter()
        .any(|object| object.state == ContentState::Complete && object.size == size)
}

fn busy(result: anyhow::Result<()>, what: &str) {
    let error = result.expect_err(what);
    assert_eq!(
        error.downcast_ref::<EvictError>(),
        Some(&EvictError::Busy),
        "{what}: {error:#}"
    );
}

/// Holds a reader of `uid` inside its object: the first 40 bytes are read,
/// the read of the last 40 waits at the stop point of its download.
async fn hold_reader(
    fixture: &Fixture,
    uid: u64,
    salt: u8,
) -> (StopPoint, JoinHandle<anyhow::Result<Vec<u8>>>) {
    let cache = &fixture.daemon.cache;
    assert_eq!(cache.read(uid, 0, 40).await.unwrap(), pattern(40, salt));
    let download = fixture.share.stop_at(MockOp::Download);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { cache.read(uid, 60, 40).await }
    });
    download.reached().await;
    (download, reader)
}

#[tokio::test(start_paused = true)]
async fn unpinning_clears_the_flag_and_makes_the_object_evictable_again() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("pins/kept", &pattern(300, 9));
        let fixture = Fixture::start(&share).await;
        let cache = &fixture.daemon.cache;
        let kept = fixture.uid("pins/kept").await;
        cache.set_max_size(1000);
        cache.set_pinned(kept, true).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        assert!(complete(&fixture, kept, 300), "{seed}: pin not fetched");

        cache.set_pinned(kept, false).await.unwrap();

        assert!(!is_pinned(&fixture, kept), "{seed}: the flag stayed set");
        cache.set_max_size(0);
        assert_eq!(cache.sweep().await.unwrap(), 300, "{seed}");
        assert!(
            !fixture.cached(kept),
            "{seed}: an unpinned object survived a zero limit"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn pin_rules_pin_matches_in_path_order_until_the_budget_and_unpin_their_matches() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        for name in ["a", "b", "c"] {
            share.add_file(&format!("rules/{name}"), &pattern(400, 10));
        }
        share.add_file("other/x", &pattern(100, 11));
        let fixture = Fixture::start(&share).await;
        let cache = &fixture.daemon.cache;
        let other = fixture.uid("other/x").await;
        let c = fixture.uid("rules/c").await;
        let b = fixture.uid("rules/b").await;
        let a = fixture.uid("rules/a").await;
        cache.set_max_size(1000);

        let report = cache
            .reconcile_rule_pins(|_, path| path.starts_with("rules"))
            .unwrap();

        assert_eq!(report.pinned, [a, b], "{seed}: pinned in path order");
        assert_eq!(report.refused, [c], "{seed}: the match over the budget");
        assert!(is_pinned(&fixture, a) && is_pinned(&fixture, b), "{seed}");
        assert!(!is_pinned(&fixture, c), "{seed}: a refused match is pinned");
        assert!(!is_pinned(&fixture, other), "{seed}: a non-match is pinned");
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        assert!(
            complete(&fixture, a, 400) && complete(&fixture, b, 400),
            "{seed}: items pinned by a rule are not downloaded"
        );

        let unpinned = cache
            .reconcile_rule_pins(|_, path| {
                path.starts_with("rules") && path != Path::new("rules/a")
            })
            .unwrap()
            .unpinned;

        assert_eq!(unpinned, [a], "{seed}");
        assert!(!is_pinned(&fixture, a), "{seed}");
        assert!(is_pinned(&fixture, b), "{seed}: a non-match was unpinned");
    }
}

fn manager(fixture: &Fixture, config: CacheManagerConfig) -> CacheManager<TokioRuntimeHandle> {
    let runtime = TokioRuntimeHandle::current();
    let storage = SqliteStorage::new(Arc::clone(&fixture.daemon.db), runtime.clone());
    let manager = CacheManager::new(Arc::new(storage), runtime, config);
    manager.attach_content(
        fixture.daemon.cache.clone(),
        vec![(
            fixture.daemon.backend.id().to_owned(),
            PathBuf::from("/share"),
        )],
    );
    manager
}

fn config_rule(path: &str) -> PinRule {
    PinRule {
        path: path.to_owned(),
        conditions: Vec::new(),
    }
}

#[tokio::test(start_paused = true)]
async fn the_cache_manager_applies_stored_and_config_pins_minus_unpins_and_sweeps_the_content_cache()
 {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("stored/s", &pattern(100, 12));
        share.add_file("config/k", &pattern(100, 13));
        share.add_file("config/skip/u", &pattern(100, 14));
        share.add_file("loose/l", &pattern(100, 15));
        let fixture = Fixture::start(&share).await;
        let [stored, kept, skipped, loose] = [
            fixture.uid("stored/s").await,
            fixture.uid("config/k").await,
            fixture.uid("config/skip/u").await,
            fixture.uid("loose/l").await,
        ];
        let manager = manager(
            &fixture,
            CacheManagerConfig {
                max_size: Some(1000),
                ..CacheManagerConfig::default()
            },
        );
        assert_eq!(fixture.daemon.cache.max_size(), 1000, "{seed}");

        manager.pin("share/stored", true).await.unwrap();
        let report = manager
            .apply_pins(
                &[config_rule("/share/config/")],
                &[config_rule("share/config/skip")],
            )
            .await
            .unwrap()
            .expect("a content cache is attached");

        assert!(is_pinned(&fixture, stored), "{seed}: stored rule");
        assert!(is_pinned(&fixture, kept), "{seed}: .cascade pin");
        assert!(
            !is_pinned(&fixture, skipped),
            "{seed}: .cascade unpin ignored"
        );
        assert!(!is_pinned(&fixture, loose), "{seed}: unmatched item pinned");
        assert!(report.refused.is_empty(), "{seed}: {report:?}");

        assert!(manager.unpin("share/stored").await.unwrap(), "{seed}");
        assert!(
            !is_pinned(&fixture, stored),
            "{seed}: removing the rule left its item pinned"
        );

        tokio::time::sleep(BETWEEN_ACCESSES).await;
        fixture.daemon.cache.fetch(loose, None).await.unwrap();
        fixture.daemon.cache.set_max_size(100);
        let freed = manager.evict().await.unwrap().bytes_freed;
        assert_eq!(fixture.populated(), 100, "{seed}: the sweep did not run");
        assert!(
            complete(&fixture, kept, 100),
            "{seed}: the pinned item left"
        );
        assert!(!fixture.cached(loose), "{seed}");
        assert!(!fixture.cached(stored), "{seed}: the unpinned item stayed");
        assert_eq!(
            freed, 200,
            "{seed}: freed bytes are the populated bytes of stored and loose"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_engine_applies_the_pin_rules_of_a_resolved_config_to_its_content_cache() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("config/k", &pattern(100, 16));
        share.add_file("loose/l", &pattern(100, 17));
        let root = tempfile::tempdir().unwrap();
        let backend = RecordingBackend::new(&share, &BackendLog::default());
        let engine = Engine::new(EngineConfig {
            db_path: root.path().join("state.db"),
            mount_point: root.path().join("mount"),
            backends: vec![MountedBackend::new(
                Some("share".to_owned()),
                backend.clone(),
            )],
            cache_dir: Some(root.path().join("cache")),
            staging_dir: Some(root.path().join("staging")),
            cache: None,
            backend_factory: None,
            enable_p2p: false,
            p2p_data_dir: None,
            p2p_posture: None,
            p2p_relay_endpoints: Vec::new(),
            p2p_relay_shared_secret: None,
        })
        .unwrap();
        let cache = engine.content_cache().unwrap();
        cache.set_max_size(1000);
        let replica = ReplicaStore::open(Arc::clone(engine.db()))
            .unwrap()
            .replica(backend)
            .unwrap();
        let kept = replica
            .resolve(Path::new("config/k"))
            .await
            .unwrap()
            .unwrap();
        let loose = replica
            .resolve(Path::new("loose/l"))
            .await
            .unwrap()
            .unwrap();
        let resolved = ResolvedConfig {
            pins: vec![config_rule("share/config")],
            ..ResolvedConfig::default()
        };

        let report = engine
            .apply_pin_rules(&resolved)
            .await
            .unwrap()
            .expect("the content cache is configured");

        assert_eq!(report.pinned, [kept], "{seed}");
        let pinned = |uid: u64| engine.db().replica_item(uid).unwrap().unwrap().pinned;
        assert!(pinned(kept), "{seed}");
        assert!(!pinned(loose), "{seed}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_engine_configured_with_a_cache_maximum_sweeps_under_it_and_refuses_a_pin_above_it() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("docs/a", &pattern(100, 30));
        share.add_file("docs/b", &pattern(100, 31));
        share.add_file("docs/big", &pattern(300, 32));
        let root = tempfile::tempdir().unwrap();
        let backend = RecordingBackend::new(&share, &BackendLog::default());
        let engine = Engine::new(EngineConfig {
            db_path: root.path().join("state.db"),
            mount_point: root.path().join("mount"),
            backends: vec![MountedBackend::new(
                Some("share".to_owned()),
                backend.clone(),
            )],
            cache_dir: Some(root.path().join("cache")),
            staging_dir: Some(root.path().join("staging")),
            cache: Some(serde_json::from_str(r#"{"max_size": "150B"}"#).unwrap()),
            backend_factory: None,
            enable_p2p: false,
            p2p_data_dir: None,
            p2p_posture: None,
            p2p_relay_endpoints: Vec::new(),
            p2p_relay_shared_secret: None,
        })
        .unwrap();
        let cache = engine.content_cache().unwrap();
        let replica = ReplicaStore::open(Arc::clone(engine.db()))
            .unwrap()
            .replica(backend)
            .unwrap();
        let mut uids = Vec::new();
        for path in ["docs/a", "docs/b", "docs/big"] {
            uids.push(replica.resolve(Path::new(path)).await.unwrap().unwrap());
        }
        let [a, b, big] = uids.try_into().unwrap();
        cache.fetch(a, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.fetch(b, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;

        assert_eq!(cache.sweep().await.unwrap(), 100, "{seed}: freed bytes");
        let cached = |uid: u64| !engine.db().content_objects_of(uid).unwrap().is_empty();
        assert!(!cached(a), "{seed}: the least recently used object stayed");
        assert!(cached(b), "{seed}: the sweep went below the maximum");
        let refused = cache
            .set_pinned(big, true)
            .await
            .expect_err("pinned bytes would exceed the configured maximum");
        assert_eq!(
            refused.downcast_ref::<EvictError>(),
            Some(&EvictError::OutOfSpace),
            "{seed}: {refused:#}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn remove_download_of_a_materialised_clean_closed_item_deletes_its_object() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("docs/m", &pattern(100, 18));
        let fixture = Fixture::start(&share).await;
        let cache = &fixture.daemon.cache;
        let item = fixture.uid("docs/m").await;
        cache.fetch(item, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.set_materialized(&[item]).await.unwrap();
        let files = fixture.object_files(item);
        cache.set_max_size(10_000);

        cache.evict(item).await.unwrap();

        assert!(
            !fixture.cached(item),
            "{seed}: a content_objects row remains after Remove Download"
        );
        assert_files_gone(&files, &format!("{seed}"));
        assert_eq!(fixture.populated(), 0, "{seed}");
    }
}

#[tokio::test(start_paused = true)]
async fn evict_of_an_object_with_a_live_reader_is_busy() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("open/g", &pattern(100, 19));
        let fixture = Fixture::start(&share).await;
        let cache = &fixture.daemon.cache;
        let open = fixture.uid("open/g").await;
        let (download, reader) = hold_reader(&fixture, open, 19).await;

        busy(cache.evict(open).await, &format!("{seed}: open object"));

        assert!(fixture.cached(open), "{seed}: an open object was evicted");
        download.release();
        assert_eq!(
            reader.await.unwrap().unwrap(),
            pattern(100, 19).get(60..).unwrap(),
            "{seed}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn recency_persisted_by_a_sweep_orders_the_sweep_after_a_restart() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("lru/x", &pattern(100, 20));
        share.add_file("lru/y", &pattern(100, 21));
        let mut fixture = Fixture::start(&share).await;
        let x = fixture.uid("lru/x").await;
        let y = fixture.uid("lru/y").await;
        let cache = fixture.daemon.cache.clone();
        cache.fetch(x, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.fetch(y, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        assert_eq!(cache.read(x, 0, 10).await.unwrap(), pattern(10, 20));
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.set_max_size(10_000);
        assert_eq!(cache.sweep().await.unwrap(), 0, "{seed}");
        drop(cache);

        fixture.restart().await;
        fixture.daemon.cache.set_max_size(100);
        fixture.daemon.cache.sweep().await.unwrap();

        assert!(
            fixture.cached(x),
            "{seed}: x was read last before the restart"
        );
        assert!(
            !fixture.cached(y),
            "{seed}: y was least recently used before the restart"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn an_item_being_deleted_is_left_to_its_delete_by_sweep_and_evict() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("gone/d", &pattern(100, 22));
        let fixture = Fixture::start(&share).await;
        let cache = &fixture.daemon.cache;
        let deleting = fixture.uid("gone/d").await;
        cache.fetch(deleting, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        let files = fixture.object_files(deleting);
        fixture.daemon.db.mark_replica_delete(deleting).unwrap();

        cache.set_max_size(0);
        assert_eq!(cache.sweep().await.unwrap(), 0, "{seed}");
        busy(
            cache.evict(deleting).await,
            &format!("{seed}: evict of an item being deleted"),
        );

        assert!(
            fixture.cached(deleting),
            "{seed}: object of a deleting item removed"
        );
        assert!(files.iter().all(|file| file.exists()), "{seed}");

        fixture.daemon.db.clear_replica_delete(deleting).unwrap();
        assert_eq!(cache.sweep().await.unwrap(), 100, "{seed}");
        assert!(!fixture.cached(deleting), "{seed}");
    }
}

const TARGET: &str = "docs/report.txt";
const RENAMED_NAME: &str = "renamed.txt";
const RENAMED: &str = "docs/renamed.txt";
const EDIT: &[u8] = b"local edit";
/// Longer than any write-back delay plus several backoff rounds.
const SETTLE: Duration = Duration::from_mins(30);
/// Polls a held operation may take before another event runs.
const YIELDS: usize = 256;

async fn let_run() {
    for _ in 0..YIELDS {
        tokio::task::yield_now().await;
    }
}

async fn remote_bytes(share: &MockBackend, path: &str) -> Option<Vec<u8>> {
    let entry = share.metadata(Path::new(path)).await.ok()?;
    Some(share.download(&entry).await.unwrap())
}

fn placed_count(log: &BackendLog, data: &[u8]) -> usize {
    log.placed()
        .into_iter()
        .filter(|placed| placed.data == data)
        .count()
}

fn dirty_files(fixture: &Fixture, uid: u64) -> Vec<PathBuf> {
    fixture
        .daemon
        .db
        .dirty_content_objects()
        .unwrap()
        .into_iter()
        .filter(|object| object.uid == uid)
        .map(|object| {
            fixture
                .root
                .path()
                .join("cache")
                .join(object.object_id.to_string())
        })
        .collect()
}

/// Settles the write-back and asserts the edit was uploaded to `TARGET`
/// exactly once.
async fn assert_edit_uploaded_once(fixture: &Fixture, seed: u64) {
    tokio::time::sleep(SETTLE).await;
    assert!(
        fixture
            .daemon
            .db
            .dirty_content_objects()
            .unwrap()
            .is_empty(),
        "{seed}: the edit was not uploaded"
    );
    assert_eq!(
        remote_bytes(&fixture.share, TARGET).await.as_deref(),
        Some(EDIT),
        "{seed}: the edit was lost"
    );
    assert_eq!(
        placed_count(&fixture.log, EDIT),
        1,
        "{seed}: uploads of the edit"
    );
}

async fn overlap_fixture(seed: u64) -> (Fixture, u64) {
    let share = Arc::new(MockBackend::new(seed));
    share.add_file(TARGET, &pattern(100, 23));
    let fixture = Fixture::start(&share).await;
    let uid = fixture.uid(TARGET).await;
    (fixture, uid)
}

#[tokio::test(start_paused = true)]
async fn an_eviction_sweep_racing_a_save_on_a_clean_guarded_object_keeps_the_edit() {
    for seed in SEEDS {
        let (fixture, uid) = overlap_fixture(seed).await;
        let cache = fixture.daemon.cache.clone();
        let (download, reader) = hold_reader(&fixture, uid, 23).await;
        let upload = fixture.share.stop_at(MockOp::Upload);
        cache.set_max_size(0);

        let incoming = fixture.daemon.incoming_file(EDIT).unwrap();
        let saving = tokio::spawn({
            let cache = cache.clone();
            async move { cache.save(uid, &incoming).await }
        });
        let sweeping = tokio::spawn({
            let cache = cache.clone();
            async move { cache.sweep().await }
        });
        let_run().await;
        download.release();
        saving.await.unwrap().unwrap();
        sweeping.await.unwrap().unwrap();
        let _ = reader.await.unwrap();
        cache.sweep().await.unwrap();

        let files = dirty_files(&fixture, uid);
        assert!(!files.is_empty(), "{seed}: the edit's object was evicted");
        assert!(
            files.iter().all(|file| file.exists()),
            "{seed}: the object file of the edit was removed"
        );
        upload.reached().await;
        upload.release();
        assert_edit_uploaded_once(&fixture, seed).await;
    }
}

#[tokio::test(start_paused = true)]
async fn eviction_during_an_upload_keeps_the_dirty_object_and_uploads_once() {
    for seed in SEEDS {
        let (fixture, uid) = overlap_fixture(seed).await;
        let cache = &fixture.daemon.cache;
        cache.fetch(uid, None).await.unwrap();
        let upload = fixture.share.stop_at(MockOp::Upload);
        fixture.save(uid, EDIT).await;
        upload.reached().await;

        cache.set_max_size(0);
        cache.sweep().await.unwrap();
        busy(cache.evict(uid).await, &format!("{seed}: uploading object"));

        let files = dirty_files(&fixture, uid);
        assert!(
            !files.is_empty(),
            "{seed}: the uploading object was evicted"
        );
        assert!(files.iter().all(|file| file.exists()), "{seed}");
        upload.release();
        assert_edit_uploaded_once(&fixture, seed).await;
    }
}

#[tokio::test(start_paused = true)]
async fn eviction_during_a_scan_removes_the_clean_object_and_keeps_the_item() {
    for seed in SEEDS {
        let (fixture, uid) = overlap_fixture(seed).await;
        let cache = fixture.daemon.cache.clone();
        cache.fetch(uid, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        let listing = fixture.share.stop_at(MockOp::ListChildren);
        let scanning = tokio::spawn({
            let cache = cache.clone();
            async move { cache.scan().await }
        });
        listing.reached().await;

        cache.set_max_size(0);
        assert_eq!(cache.sweep().await.unwrap(), 100, "{seed}");
        listing.release();
        scanning.await.unwrap().unwrap();

        assert!(!fixture.cached(uid), "{seed}: the scan restored the object");
        assert_eq!(fixture.populated(), 0, "{seed}");
        assert_eq!(
            cache.read(uid, 0, 100).await.unwrap(),
            pattern(100, 23),
            "{seed}: the item is unreadable after the overlap"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn eviction_during_a_rename_leaves_the_item_readable_under_its_new_name() {
    for seed in SEEDS {
        let (fixture, uid) = overlap_fixture(seed).await;
        let cache = fixture.daemon.cache.clone();
        cache.fetch(uid, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        let folder = fixture
            .daemon
            .store
            .item(uid)
            .unwrap()
            .unwrap()
            .parent_uid
            .unwrap();
        let moving = fixture.share.stop_at(MockOp::Move);
        let renaming = tokio::spawn({
            let cache = cache.clone();
            async move { cache.rename(uid, folder, RENAMED_NAME).await }
        });
        moving.reached().await;

        cache.set_max_size(0);
        let sweeping = tokio::spawn({
            let cache = cache.clone();
            async move { cache.sweep().await }
        });
        let_run().await;
        moving.release();
        let renamed = renaming.await.unwrap().unwrap();
        sweeping.await.unwrap().unwrap();

        assert_eq!(renamed.name, RENAMED_NAME, "{seed}");
        assert_eq!(
            remote_bytes(&fixture.share, RENAMED).await,
            Some(pattern(100, 23)),
            "{seed}"
        );
        assert_eq!(
            cache.read(uid, 0, 100).await.unwrap(),
            pattern(100, 23),
            "{seed}: the item is unreadable under its new name"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_saved_object_counts_its_save_as_an_access_and_outlives_an_older_read() {
    for seed in SEEDS {
        let (fixture, uid) = overlap_fixture(seed).await;
        fixture.share.add_file("old/r", &pattern(100, 24));
        let cache = &fixture.daemon.cache;
        let older = fixture.uid("old/r").await;
        cache.fetch(older, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        fixture.save(uid, EDIT).await;
        assert_edit_uploaded_once(&fixture, seed).await;

        cache.set_max_size(u64::try_from(EDIT.len()).unwrap());
        cache.sweep().await.unwrap();

        assert!(fixture.cached(uid), "{seed}: the saved object was evicted");
        assert!(
            !fixture.cached(older),
            "{seed}: the older read object stayed"
        );
    }
}

/// Holds the slot of `uid`, whose object is partial and has no transfer
/// after a restart: its read waits under the slot lock at the stop point of
/// the metadata lookup that resumes the download.
async fn hold_slot(
    fixture: &Fixture,
    uid: u64,
) -> (StopPoint, JoinHandle<anyhow::Result<Vec<u8>>>) {
    let lookup = fixture.share.stop_at(MockOp::Metadata);
    let reader = tokio::spawn({
        let cache = fixture.daemon.cache.clone();
        async move { cache.read(uid, 0, 10).await }
    });
    lookup.reached().await;
    (lookup, reader)
}

#[tokio::test(start_paused = true)]
async fn two_concurrent_sweeps_remove_only_the_objects_needed_to_meet_the_limit() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        for name in ["a", "b", "c"] {
            share.add_file(&format!("lru/{name}"), &pattern(100, 25));
        }
        let mut fixture = Fixture::start(&share).await;
        let [a, b, c] = [
            fixture.uid("lru/a").await,
            fixture.uid("lru/b").await,
            fixture.uid("lru/c").await,
        ];
        let cache = fixture.daemon.cache.clone();
        assert_eq!(cache.read(a, 0, 40).await.unwrap(), pattern(40, 25));
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.fetch(b, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.fetch(c, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        cache.set_max_size(10_000);
        cache.sweep().await.unwrap();
        drop(cache);
        fixture.restart().await;
        let cache = fixture.daemon.cache.clone();
        let objects: Vec<_> = [a, b, c]
            .into_iter()
            .flat_map(|uid| fixture.daemon.db.content_objects_of(uid).unwrap())
            .map(|object| object.object_id)
            .collect();
        assert_eq!(fixture.populated(), 240, "{seed}");
        let (lookup, reader) = hold_slot(&fixture, a).await;

        cache.set_max_size(200);
        let sweeps: Vec<_> = (0..2)
            .map(|_| {
                let cache = cache.clone();
                tokio::spawn(async move { cache.sweep().await })
            })
            .collect();
        let_run().await;
        lookup.release();
        let mut freed = 0;
        for sweep in sweeps {
            freed += sweep.await.unwrap().unwrap();
        }
        assert_eq!(reader.await.unwrap().unwrap(), pattern(10, 25), "{seed}");

        let removed = objects
            .iter()
            .filter(|&&id| fixture.daemon.db.content_object(id).unwrap().is_none())
            .count();
        assert_eq!(removed, 1, "{seed}: objects removed by two sweeps");
        assert!(
            freed == 40 || freed == 100,
            "{seed}: freed {freed} is not the populated bytes of one object"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn an_explicit_pin_survives_the_removal_of_a_stored_rule_that_covers_it() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("kept/k", &pattern(100, 26));
        let fixture = Fixture::start(&share).await;
        let kept = fixture.uid("kept/k").await;
        let manager = manager(
            &fixture,
            CacheManagerConfig {
                max_size: Some(1000),
                ..CacheManagerConfig::default()
            },
        );
        fixture.daemon.cache.set_pinned(kept, true).await.unwrap();

        manager.pin("share/kept", true).await.unwrap();
        assert!(manager.unpin("share/kept").await.unwrap(), "{seed}");

        assert!(
            is_pinned(&fixture, kept),
            "{seed}: removing a rule cleared an explicit pin"
        );
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        fixture.daemon.cache.set_max_size(0);
        fixture.daemon.cache.sweep().await.unwrap();
        assert!(
            complete(&fixture, kept, 100),
            "{seed}: the explicitly pinned item was evicted"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_stored_rule_added_after_config_pins_keeps_the_config_unpins() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("config/k", &pattern(100, 27));
        share.add_file("config/skip/u", &pattern(100, 28));
        let fixture = Fixture::start(&share).await;
        let kept = fixture.uid("config/k").await;
        let skipped = fixture.uid("config/skip/u").await;
        let manager = manager(
            &fixture,
            CacheManagerConfig {
                max_size: Some(1000),
                ..CacheManagerConfig::default()
            },
        );
        manager
            .apply_pins(
                &[config_rule("share/config")],
                &[config_rule("share/config/skip")],
            )
            .await
            .unwrap();

        manager.pin("share/config", true).await.unwrap();

        assert!(is_pinned(&fixture, kept), "{seed}");
        assert!(
            !is_pinned(&fixture, skipped),
            "{seed}: a stored rule pinned an item the .cascade unpins exclude"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_stored_rule_with_conditions_is_not_applied() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("cond/c", &pattern(100, 33));
        let fixture = Fixture::start(&share).await;
        let conditional = fixture.uid("cond/c").await;
        let manager = manager(
            &fixture,
            CacheManagerConfig {
                max_size: Some(1000),
                ..CacheManagerConfig::default()
            },
        );
        fixture
            .daemon
            .db
            .add_pin_rule("share/cond", true, Some("network.metered == false"))
            .unwrap();

        manager.apply_pins(&[], &[]).await.unwrap();

        assert!(
            !is_pinned(&fixture, conditional),
            "{seed}: a stored rule with conditions pinned its match"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn removing_a_non_recursive_stored_rule_keeps_descendants_it_never_pinned() {
    for seed in SEEDS {
        let share = Arc::new(MockBackend::new(seed));
        share.add_file("dir/x", &pattern(100, 29));
        let fixture = Fixture::start(&share).await;
        let below = fixture.uid("dir/x").await;
        let manager = manager(
            &fixture,
            CacheManagerConfig {
                max_size: Some(1000),
                ..CacheManagerConfig::default()
            },
        );
        manager
            .apply_pins(&[config_rule("share/dir")], &[])
            .await
            .unwrap();
        assert!(is_pinned(&fixture, below), "{seed}");

        manager.pin("share/dir", false).await.unwrap();
        assert!(manager.unpin("share/dir").await.unwrap(), "{seed}");

        assert!(
            is_pinned(&fixture, below),
            "{seed}: removing a non-recursive rule unpinned a descendant"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn eviction_during_a_delete_counts_the_object_once_and_leaves_nothing() {
    for seed in SEEDS {
        let (fixture, uid) = overlap_fixture(seed).await;
        let cache = fixture.daemon.cache.clone();
        cache.fetch(uid, None).await.unwrap();
        tokio::time::sleep(BETWEEN_ACCESSES).await;
        let files = fixture.object_files(uid);
        let deleting_remote = fixture.share.stop_at(MockOp::Delete);
        let deleting = tokio::spawn({
            let cache = cache.clone();
            async move { cache.delete(uid).await }
        });
        deleting_remote.reached().await;

        cache.set_max_size(0);
        assert_eq!(
            cache.sweep().await.unwrap(),
            0,
            "{seed}: the sweep freed the object of an item being deleted"
        );
        deleting_remote.release();
        deleting.await.unwrap().unwrap();

        assert!(!fixture.cached(uid), "{seed}");
        assert_files_gone(&files, &format!("{seed}"));
        assert_eq!(fixture.populated(), 0, "{seed}");
        assert!(
            remote_bytes(&fixture.share, TARGET).await.is_none(),
            "{seed}"
        );
        assert!(fixture.daemon.store.item(uid).unwrap().is_none(), "{seed}");
    }
}
