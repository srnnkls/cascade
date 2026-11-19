//! `ContentCache::sync`: makes an item's cached object durable and changes
//! nothing the cache records about it.
#![cfg(all(feature = "native", unix))]

use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context as _, Result, ensure};
use cascade_engine::backend::BackendError;
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::db::content::ContentObject;
use cascade_engine::testing::{BackendLog, Daemon, Disk, MockBackend, RecordingBackend};

const TARGET: &str = "docs/report.txt";
const ORIGINAL: &[u8] = b"original";
/// A uid no daemon over a fresh database assigns.
const UNKNOWN_UID: u64 = 0xA5A5_A5A5_A5A5;

/// What the cache records about one item and the bytes it serves.
#[derive(Debug, PartialEq, Eq)]
struct Recorded {
    objects: Vec<ContentObject>,
    ranges: Vec<Vec<Range<u64>>>,
    content: Vec<u8>,
}

async fn daemon() -> Result<(tempfile::TempDir, Daemon, u64)> {
    let share = Arc::new(MockBackend::new(7));
    share.add_file(TARGET, ORIGINAL);
    let root = tempfile::tempdir()?;
    let disk = Disk::at(root.path(), TransferConfig::default())?;
    let daemon = disk
        .start(RecordingBackend::new(&share, &BackendLog::default()))
        .await?;
    let uid = daemon
        .resolve(TARGET)
        .await?
        .context("the seeded file resolves")?;
    Ok((root, daemon, uid))
}

async fn recorded(daemon: &Daemon, uid: u64) -> Result<Recorded> {
    let objects = daemon.db.content_objects_of(uid)?;
    let ranges = objects
        .iter()
        .map(|object| daemon.db.content_ranges(object.object_id))
        .collect::<Result<Vec<_>>>()?;
    let content = daemon
        .cache
        .read(uid, 0, u32::try_from(ORIGINAL.len())?)
        .await?;
    Ok(Recorded {
        objects,
        ranges,
        content,
    })
}

fn is_missing_item(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<BackendError>(),
        Some(BackendError::NotFound(_))
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_of_an_item_with_an_edited_object_succeeds_and_leaves_content_ranges_and_dirty_state()
-> Result<()> {
    let (_root, daemon, uid) = daemon().await?;
    daemon.cache.fetch(uid, None).await?;
    daemon.cache.write_at(uid, 2, b"XY").await?;
    let before = recorded(&daemon, uid).await?;
    ensure!(
        before.objects.iter().any(|object| object.edited),
        "the write left no edited object: {before:?}"
    );

    daemon.cache.sync(uid).await?;

    assert_eq!(recorded(&daemon, uid).await?, before);
    assert_eq!(before.content, b"orXYinal");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_of_an_item_with_a_dirty_object_succeeds_and_leaves_content_ranges_and_dirty_state()
-> Result<()> {
    let (_root, daemon, uid) = daemon().await?;
    let incoming = daemon.incoming_file(b"saved!!!")?;
    daemon.cache.save(uid, &incoming).await?;
    std::fs::remove_file(&incoming)?;
    let before = recorded(&daemon, uid).await?;
    ensure!(
        before.objects.iter().any(|object| object.dirty),
        "the save left no dirty object: {before:?}"
    );

    daemon.cache.sync(uid).await?;

    assert_eq!(recorded(&daemon, uid).await?, before);
    assert_eq!(before.content, b"saved!!!");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_of_an_item_without_an_object_succeeds_and_creates_none() -> Result<()> {
    let (_root, daemon, uid) = daemon().await?;
    ensure!(
        daemon.db.content_objects_of(uid)?.is_empty(),
        "the item has an object before any read"
    );

    daemon.cache.sync(uid).await?;

    assert_eq!(daemon.db.content_objects_of(uid)?, Vec::new());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_of_an_unknown_uid_fails_as_a_missing_item_does_elsewhere_in_the_cache() -> Result<()>
{
    let (_root, daemon, _uid) = daemon().await?;
    ensure!(
        daemon.store.item(UNKNOWN_UID)?.is_none(),
        "the fixture assigned the unknown uid"
    );
    let elsewhere = daemon
        .cache
        .write_at(UNKNOWN_UID, 0, b"X")
        .await
        .err()
        .context("write_at of an unknown uid succeeded")?;
    ensure!(
        is_missing_item(&elsewhere),
        "write_at of an unknown uid: {elsewhere:#}"
    );

    let error = daemon
        .cache
        .sync(UNKNOWN_UID)
        .await
        .err()
        .context("sync of an unknown uid succeeded")?;

    ensure!(
        is_missing_item(&error),
        "sync of an unknown uid failed otherwise: {error:#}"
    );
    Ok(())
}
