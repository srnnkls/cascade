//! `.cascade` ignore rules applied to the content cache at daemon start:
//! ignored items leave the replica with their objects and stay out of it,
//! an edit under a newly ignored path is uploaded before its item leaves,
//! pin rules reach no ignored item, and temporary upload names are exempt.
#![cfg(all(feature = "native", unix))]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use cascade_config::types::{PinRule, ResolvedConfig};
use cascade_engine::backend::{Backend, MountedBackend};
use cascade_engine::content::names::NameError;
use cascade_engine::content::replica::{Changes, Replica, ReplicaStore, is_temp_upload};
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::engine::{EngineConfig, NativeEngine};
use cascade_engine::testing::{
    BackendLog, Disk, MockBackend, MockOp, RecordingBackend, mount_point_config,
    mount_point_toml_config,
};

const SEED: u64 = 17;
/// Mount prefix of the backend in the VFS, distinct from its id `mock`.
const PREFIX: &str = "share";
/// Longer than the write-back delay plus a few retry rounds.
const SETTLE: Duration = Duration::from_mins(30);

fn pin(path: &str) -> PinRule {
    PinRule {
        path: path.to_owned(),
        conditions: Vec::new(),
    }
}

fn engine_over(root: &Path, backend: Arc<dyn Backend>) -> Result<NativeEngine> {
    NativeEngine::new(EngineConfig {
        db_path: root.join("state.db"),
        mount_point: root.join("mount"),
        backends: vec![MountedBackend::new(Some(PREFIX.to_owned()), backend)],
        cache_dir: Some(root.join("cache")),
        staging_dir: Some(root.join("staging")),
        cache: None,
        backend_factory: None,
        enable_p2p: false,
        p2p_data_dir: None,
        p2p_posture: None,
        p2p_relay_endpoints: Vec::new(),
        p2p_relay_shared_secret: None,
    })
}

async fn uid_at(replica: &Replica, path: &str) -> Result<u64> {
    replica
        .resolve(Path::new(path))
        .await?
        .with_context(|| format!("{path} is not on the share"))
}

fn names(items: &[cascade_engine::content::replica::ReplicaItem]) -> Vec<&str> {
    items.iter().map(|item| item.name.as_str()).collect()
}

async fn on_share(share: &MockBackend, path: &str) -> Result<Vec<u8>> {
    let entry = share.metadata(Path::new(path)).await?;
    share.download(&entry).await
}

/// Every path on the share, folders included, sorted.
async fn share_tree(share: &MockBackend) -> Result<Vec<String>> {
    let mut found = Vec::new();
    let mut pending = vec![String::new()];
    while let Some(dir) = pending.pop() {
        let entry = share.metadata(Path::new(&dir)).await?;
        for child in share.list_children(entry.id.native_id()).await? {
            let path = if dir.is_empty() {
                child.name.clone()
            } else {
                format!("{dir}/{}", child.name)
            };
            if child.is_dir {
                pending.push(path.clone());
            }
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}

/// Files of the share layout the folder-move tests rename and move.
const MOVE_LAYOUT: [&str; 10] = [
    "only/cache.tmp",
    "deep/scratch/inner.txt",
    "deep/kept.txt",
    "proj/mirror/m.txt",
    "proj/src.txt",
    "b/keep.txt",
    "mover/x.log",
    "staging/build/out.txt",
    "plain/a.txt",
    "plain/sub/b.txt",
];

#[tokio::test(start_paused = true)]
async fn a_folder_move_carrying_or_landing_an_ignored_entry_is_refused() -> Result<()> {
    let share = Arc::new(MockBackend::new(SEED));
    for path in MOVE_LAYOUT {
        share.add_file(path, path.as_bytes());
    }
    let root = tempfile::tempdir()?;
    let log = BackendLog::default();
    let backend: Arc<dyn Backend> = RecordingBackend::new(&share, &log);
    let engine = engine_over(root.path(), Arc::clone(&backend))?;
    let cache = engine.content_cache()?;
    cache.recover().await?;
    let replica = ReplicaStore::open(Arc::clone(engine.db()))?.replica(backend)?;
    let mount = replica.root_uid();

    engine
        .apply_ignore_rules(&mount_point_config(
            "*.tmp\nscratch\n/share/proj/mirror/\nshare/b/**/*.log\n/share/a/build/\n",
        ))
        .await?;

    let mut failures = Vec::new();
    for (case, folder, parent, name) in [
        ("(a) only/cache.tmp under *.tmp", "only", "", "only2"),
        ("(a) deep/scratch under scratch", "deep", "", "deep2"),
        (
            "(b) proj/mirror under /share/proj/mirror/",
            "proj",
            "",
            "proj2",
        ),
        (
            "(c) x.log lands under share/b/**/*.log",
            "mover",
            "b",
            "mover",
        ),
        ("(c) build/ lands on /share/a/build/", "staging", "", "a"),
    ] {
        let tree = share_tree(&share).await?;
        let moves = log.calls(MockOp::Move);
        let uid = uid_at(&replica, folder).await?;
        let parent_uid = if parent.is_empty() {
            mount
        } else {
            uid_at(&replica, parent).await?
        };
        let outcome = cache.rename(uid, parent_uid, name).await;
        let refused = outcome
            .as_ref()
            .err()
            .and_then(anyhow::Error::downcast_ref::<NameError>);
        if !matches!(refused, Some(NameError::Ignored(_))) {
            let answer = outcome.map(|item| item.name);
            failures.push(format!("{case}: answered {answer:?}"));
        }
        if log.calls(MockOp::Move) != moves {
            failures.push(format!("{case}: the backend took a Move"));
        }
        let after = share_tree(&share).await?;
        if after != tree {
            failures.push(format!("{case}: the share changed to {after:?}"));
        }
    }

    let moves = log.calls(MockOp::Move);
    let plain = uid_at(&replica, "plain").await?;
    match cache.rename(plain, mount, "plain2").await {
        Ok(item) if item.name == "plain2" => {}
        outcome => {
            let answer = outcome.map(|item| item.name);
            failures.push(format!("(d) guard: answered {answer:?}"));
        }
    }
    let tree = share_tree(&share).await?;
    if log.calls(MockOp::Move) != moves + 1
        || !tree.contains(&"plain2/sub/b.txt".to_owned())
        || tree.iter().any(|path| path.starts_with("plain/"))
    {
        failures.push(format!("(d) guard: the share holds {tree:?}"));
    }

    ensure!(failures.is_empty(), "{failures:#?}");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_ignore_rule_with_conditions_has_no_effect_on_the_content_cache() -> Result<()> {
    let share = Arc::new(MockBackend::new(SEED));
    share.add_file("clip.mov", b"movie");
    share.add_file("scratch/s.txt", b"scratch");
    let root = tempfile::tempdir()?;
    let backend: Arc<dyn Backend> = RecordingBackend::new(&share, &BackendLog::default());
    let engine = engine_over(root.path(), Arc::clone(&backend))?;
    let cache = engine.content_cache()?;
    cache.recover().await?;
    let replica = ReplicaStore::open(Arc::clone(engine.db()))?.replica(backend)?;
    let mount = replica.root_uid();
    let clip = uid_at(&replica, "clip.mov").await?;
    let scratch = uid_at(&replica, "scratch").await?;
    cache.fetch(clip, None).await?;

    engine
        .apply_ignore_rules(&mount_point_toml_config(
            r#"
[[ignore]]
pattern = "*.mov"
conditions = ["network.metered"]

[[ignore]]
pattern = "scratch"
"#,
        )?)
        .await?;

    ensure!(
        engine.db().replica_item(scratch)?.is_none(),
        "the unconditional rule left its folder in the replica"
    );
    assert_eq!(names(&cache.list(mount).await?), ["clip.mov"]);
    ensure!(
        engine.db().replica_item(clip)?.is_some(),
        "a conditional rule removed the replica row"
    );
    ensure!(
        !engine.db().content_objects_of(clip)?.is_empty(),
        "a conditional rule dropped the cached object"
    );
    let created = cache.create_folder(mount, "new.mov").await?;
    ensure!(created.name == "new.mov", "created {created:?}");
    ensure!(
        share.metadata(Path::new("new.mov")).await.is_ok(),
        "the create under a conditional rule did not reach the share"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn ignore_rules_at_start_remove_the_ignored_rows_and_objects_and_keep_them_out() -> Result<()>
{
    let share = Arc::new(MockBackend::new(SEED));
    share.add_file("keep/a.txt", b"kept");
    share.add_file("top.txt", b"top");
    share.add_file("Mutagen/x/y.bin", b"replicated elsewhere");
    share.add_file("Mutagen/z.txt", b"cached");
    share.add_file("logs/debug.log", b"log");
    share.add_file("logs/notes.txt", b"notes");
    let root = tempfile::tempdir()?;
    let backend: Arc<dyn Backend> = RecordingBackend::new(&share, &BackendLog::default());
    let engine = engine_over(root.path(), Arc::clone(&backend))?;
    let cache = engine.content_cache()?;
    cache.recover().await?;
    let replica = ReplicaStore::open(Arc::clone(engine.db()))?.replica(backend)?;
    let mount = replica.root_uid();
    let mutagen = uid_at(&replica, "Mutagen").await?;
    let ignored = [
        mutagen,
        uid_at(&replica, "Mutagen/x").await?,
        uid_at(&replica, "Mutagen/x/y.bin").await?,
        uid_at(&replica, "Mutagen/z.txt").await?,
        uid_at(&replica, "logs/debug.log").await?,
    ];
    let cached = uid_at(&replica, "Mutagen/z.txt").await?;
    let debug_log = uid_at(&replica, "logs/debug.log").await?;
    let logs = uid_at(&replica, "logs").await?;
    let kept = [
        uid_at(&replica, "keep").await?,
        uid_at(&replica, "keep/a.txt").await?,
        uid_at(&replica, "top.txt").await?,
        logs,
        uid_at(&replica, "logs/notes.txt").await?,
    ];
    cache.list(mount).await?;
    cache.list(mutagen).await?;
    cache.fetch(cached, None).await?;
    ensure!(
        !engine.db().content_objects_of(cached)?.is_empty(),
        "the fetch left no object"
    );
    let anchor = cache.current_anchor()?;

    engine
        .apply_ignore_rules(&mount_point_config(
            "Mutagen/\n/share/logs/*.log\n/keep/a.txt\n",
        ))
        .await?;

    for uid in ignored {
        ensure!(
            engine.db().replica_item(uid)?.is_none(),
            "ignored item {uid} kept its replica row"
        );
    }
    ensure!(
        engine.db().content_objects_of(cached)?.is_empty(),
        "the object of an ignored item stayed"
    );
    for uid in kept {
        ensure!(
            engine.db().replica_item(uid)?.is_some(),
            "item {uid} outside every rule left the replica"
        );
    }
    let Changes::Since(changes) = cache.changes(&anchor)? else {
        bail!("the anchor taken before the rules expired");
    };
    for uid in [mutagen, debug_log] {
        ensure!(
            changes.deleted.contains(&uid),
            "no tombstone for ignored item {uid}: {:?}",
            changes.deleted
        );
    }
    assert_eq!(
        names(&cache.list(mount).await?),
        ["keep", "logs", "top.txt"]
    );
    assert_eq!(names(&cache.list(logs).await?), ["notes.txt"]);

    share.add_file("Mutagen/later.txt", b"later");
    share.add_file("logs/later.log", b"later");
    cache.scan().await?;
    ensure!(
        engine.db().replica_child(mount, "Mutagen")?.is_none(),
        "a scan inserted the ignored folder again"
    );
    ensure!(
        engine.db().replica_child(logs, "later.log")?.is_none(),
        "a scan inserted an ignored file"
    );
    ensure!(
        cache
            .resolve("mock", Path::new("Mutagen/later.txt"))
            .await?
            .is_none(),
        "an ignored path resolves through the cache"
    );
    Ok(())
}

#[test]
fn an_edit_under_a_path_ignored_at_the_next_start_is_uploaded_before_its_item_leaves() -> Result<()>
{
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
    };
    let share = Arc::new(MockBackend::new(SEED));
    share.add_file("Mutagen/z.txt", b"before");
    let root = tempfile::tempdir()?;
    let disk = Disk::at(root.path(), TransferConfig::default())?;
    let backend: Arc<dyn Backend> = RecordingBackend::new(&share, &BackendLog::default());

    let edited = runtime()?.block_on(async {
        let daemon = disk.start(Arc::clone(&backend)).await?;
        let edited = daemon
            .resolve("Mutagen/z.txt")
            .await?
            .context("z.txt is not on the share")?;
        let incoming = daemon.incoming_file(b"edited on the Mac")?;
        daemon.cache.save(edited, &incoming).await?;
        anyhow::Ok(edited)
    })?;
    ensure!(
        runtime()?.block_on(on_share(&share, "Mutagen/z.txt"))? == b"before",
        "the edit was uploaded before the daemon stopped"
    );

    runtime()?.block_on(async {
        let daemon = disk.open(Arc::clone(&backend))?;
        let mount = daemon.replica()?.root_uid();
        daemon
            .cache
            .apply_ignore_rules(
                &mount_point_config("Mutagen/\n"),
                &[(backend.id().to_owned(), PathBuf::from(PREFIX))],
            )
            .await?;
        daemon.cache.recover().await?;

        ensure!(
            daemon.store.item(edited)?.is_some(),
            "the edited item left before its upload"
        );
        ensure!(
            daemon
                .db
                .dirty_content_objects()?
                .iter()
                .any(|object| object.uid == edited),
            "the dirty object was dropped"
        );

        tokio::time::sleep(SETTLE).await;

        ensure!(
            on_share(&share, "Mutagen/z.txt").await? == b"edited on the Mac",
            "the edit under the ignored path was not uploaded"
        );
        ensure!(
            daemon.store.item(edited)?.is_none(),
            "the item stayed after its upload finished"
        );
        ensure!(
            daemon.db.content_objects_of(edited)?.is_empty(),
            "the object stayed after its upload finished"
        );
        ensure!(
            daemon.db.replica_child(mount, "Mutagen")?.is_none(),
            "the ignored folder stayed"
        );
        Ok(())
    })
}

#[tokio::test(start_paused = true)]
async fn pin_rules_reach_no_ignored_item_and_temporary_uploads_pass_a_matching_rule() -> Result<()>
{
    let share = Arc::new(MockBackend::new(SEED));
    share.add_file("Mutagen/big.bin", &[7; 4096]);
    share.add_file("keep/k.txt", b"original");
    let root = tempfile::tempdir()?;
    let log = BackendLog::default();
    let backend: Arc<dyn Backend> = RecordingBackend::new(&share, &log);
    let engine = engine_over(root.path(), Arc::clone(&backend))?;
    let cache = engine.content_cache()?;
    cache.set_max_size(1 << 20);
    cache.recover().await?;
    let replica = ReplicaStore::open(Arc::clone(engine.db()))?.replica(backend)?;
    uid_at(&replica, "Mutagen/big.bin").await?;
    let kept = uid_at(&replica, "keep/k.txt").await?;

    engine
        .apply_ignore_rules(&mount_point_config("Mutagen/\n.*\n"))
        .await?;
    let report = engine
        .apply_pin_rules(&ResolvedConfig {
            pins: vec![pin("share/Mutagen")],
            ..ResolvedConfig::default()
        })
        .await?
        .context("the content cache is configured")?;
    tokio::time::sleep(Duration::from_mins(1)).await;

    ensure!(
        report.pinned.is_empty(),
        "a pin rule on an ignored path pinned {:?}",
        report.pinned
    );
    ensure!(
        log.calls(MockOp::Download) == 0 && log.fetched().is_empty(),
        "a pin rule on an ignored path fetched {:?}",
        log.fetched()
    );

    let incoming = root.path().join("incoming");
    std::fs::write(&incoming, b"saved on the Mac")?;
    cache.save(kept, &incoming).await?;
    tokio::time::sleep(SETTLE).await;

    ensure!(
        on_share(&share, "keep/k.txt").await? == b"saved on the Mac",
        "the save was not uploaded under a rule matching its temporary name"
    );
    let uploads = log.uploads();
    ensure!(
        uploads.iter().any(|upload| upload
            .path
            .file_name()
            .is_some_and(|name| is_temp_upload(&name.to_string_lossy()))),
        "no upload went through a temporary name: {uploads:?}"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_config_without_ignore_rules_leaves_the_replica_unchanged() -> Result<()> {
    let share = Arc::new(MockBackend::new(SEED));
    share.add_file("docs/a.txt", b"a");
    share.add_file("Mutagen/b.txt", b"b");
    let root = tempfile::tempdir()?;
    let backend: Arc<dyn Backend> = RecordingBackend::new(&share, &BackendLog::default());
    let engine = engine_over(root.path(), Arc::clone(&backend))?;
    let cache = engine.content_cache()?;
    cache.recover().await?;
    let replica = ReplicaStore::open(Arc::clone(engine.db()))?.replica(backend)?;
    let mount = replica.root_uid();
    uid_at(&replica, "Mutagen/b.txt").await?;
    let before = cache.list(mount).await?;
    let anchor = cache.current_anchor()?;

    engine
        .apply_ignore_rules(&ResolvedConfig {
            pins: vec![pin("share/docs")],
            ..ResolvedConfig::default()
        })
        .await?;

    let Changes::Since(changes) = cache.changes(&anchor)? else {
        bail!("the anchor expired");
    };
    ensure!(
        changes.updated.is_empty() && changes.deleted.is_empty(),
        "a config without ignore rules changed the replica: {changes:?}"
    );
    assert_eq!(cache.list(mount).await?, before);
    Ok(())
}
