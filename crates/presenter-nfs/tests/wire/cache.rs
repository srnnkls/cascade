//! The content cache served over `NFSv3`: ranged reads with read-ahead,
//! write-back of WRITE and SETATTR size, REMOVE, RENAME and MKDIR applied
//! directly, listings through the replica and names SMB cannot hold.

use anyhow::{Result, ensure};
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::testing::{Fault, MockOp, StopPoint};
use cascade_presenter_nfs::nfs::server::NfsCacheMode;
use nfs3_client::nfs3_types::nfs3::{createhow3, createverf3, ftype3, nfsstat3, sattr3};

use crate::support::client::{Client, code};
use crate::support::fixture::{
    Fixture, Layout, Mount, QUIET, SETTLE, SETTLE_AFTER_RETRY, eventually, eventually_within,
    names_in,
};
use crate::support::{MIB, REJECTED, covers, edited, fetched_bytes, other_pattern, pattern, slice};

use std::time::Duration;

/// Lets a call issued on another connection reach the point where it waits.
const LET_RUN: Duration = Duration::from_millis(300);

async fn reached_in_time(stop: &StopPoint, what: &str) -> Result<()> {
    tokio::time::timeout(SETTLE, stop.reached())
        .await
        .map_err(|_| anyhow::anyhow!("{what} never started"))
}

#[tokio::test(flavor = "multi_thread")]
async fn read_at_a_high_offset_of_an_unlisted_folder_fetches_the_window_plus_read_ahead()
-> Result<()> {
    const SIZE: u64 = 32 * MIB;
    const OFFSET: u64 = 24 * MIB;
    const LEN: u32 = 1024 * 1024;
    const READ_AHEAD: u64 = 2 * MIB;
    let fixture = Fixture::new(TransferConfig {
        read_ahead: READ_AHEAD,
        request_size: MIB,
        ..TransferConfig::default()
    })
    .await?;
    let data = pattern(SIZE);
    fixture.mock.add_file("media/2026/big.bin", &data);
    let mut nfs = fixture.client().await?;

    let fh = nfs.walk("media/2026/big.bin").await?;
    let bytes = nfs.read(&fh, OFFSET, LEN).await?;

    ensure!(
        bytes == slice(&data, OFFSET..OFFSET + u64::from(LEN))?,
        "READ returned other bytes"
    );
    let window = OFFSET..OFFSET + u64::from(LEN) + READ_AHEAD;
    eventually("the read-ahead window after the read is fetched", || {
        covers(&fixture.log.fetched(), &window)
    })
    .await?;
    let fetched = fixture.log.fetched();
    ensure!(
        fetched
            .iter()
            .all(|range| range.start >= OFFSET - MIB && range.end <= window.end + MIB),
        "fetched outside the read and its read-ahead: {fetched:?}"
    );
    ensure!(
        fetched_bytes(&fetched) <= u64::from(LEN) + READ_AHEAD + MIB,
        "fetched {} bytes for a {LEN}-byte read: {fetched:?}",
        fetched_bytes(&fetched)
    );

    let again = nfs.read(&fh, OFFSET, LEN).await?;

    ensure!(again == bytes, "a second READ returned other bytes");
    assert_eq!(
        fixture.log.fetched(),
        fetched,
        "a read of cached bytes went to the backend"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn write_and_set_size_reach_the_backend_by_write_back_while_rename_and_remove_apply_directly()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let report = nfs.walk("docs/report.txt").await?;

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);

    assert_eq!(fixture.uploaded(), 0, "WRITE uploaded before its reply");
    assert_eq!(nfs.read(&report, 0, 8).await?, b"orXYinal");
    assert_eq!(
        fixture.on_backend("docs/report.txt").as_deref(),
        Some(b"original".as_slice())
    );

    assert_eq!(nfs.set_size(&report, 4).await?, nfsstat3::NFS3_OK);

    assert_eq!(
        fixture.uploaded(),
        0,
        "SETATTR size uploaded before its reply"
    );
    assert_eq!(nfs.read(&report, 0, 4).await?, b"orXY");
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    assert_eq!(fixture.placed(), [b"orXY".to_vec()]);
    assert_eq!(fixture.log.calls(MockOp::Update), 0);
    assert_eq!(
        fixture.on_backend("docs/report.txt").as_deref(),
        Some(b"orXY".as_slice())
    );

    assert_eq!(
        nfs.rename(&docs, "report.txt", &docs, "final.txt").await?,
        nfsstat3::NFS3_OK
    );

    assert_eq!(
        fixture.on_backend("docs/final.txt").as_deref(),
        Some(b"orXY".as_slice())
    );
    assert_eq!(fixture.on_backend("docs/report.txt"), None);

    assert_eq!(nfs.remove(&docs, "other.txt").await?, nfsstat3::NFS3_OK);

    assert_eq!(fixture.on_backend("docs/other.txt"), None);
    assert_eq!(
        code(&nfs.lookup(&docs, "other.txt").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    assert_eq!(fixture.log.calls(MockOp::Delete), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn names_smb_cannot_hold_are_refused_on_create_mkdir_and_rename_before_anything_is_applied()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let root = nfs.root();
    let docs = nfs.walk("docs").await?;

    for c in REJECTED {
        let name = format!("bad{c}name");

        assert_eq!(
            nfs.create(&root, &name).await?,
            nfsstat3::NFS3ERR_INVAL,
            "CREATE {name}"
        );
        assert_eq!(
            nfs.create(&docs, &name).await?,
            nfsstat3::NFS3ERR_INVAL,
            "CREATE docs/{name}"
        );
        assert_eq!(
            nfs.mkdir(&docs, &name).await?,
            nfsstat3::NFS3ERR_INVAL,
            "MKDIR docs/{name}"
        );
        assert_eq!(
            nfs.rename(&docs, "report.txt", &docs, &name).await?,
            nfsstat3::NFS3ERR_INVAL,
            "RENAME to {name}"
        );
        assert_eq!(
            code(&nfs.lookup(&docs, &name).await?),
            nfsstat3::NFS3ERR_NOENT,
            "{name}"
        );
        assert_eq!(fixture.on_backend(&name), None);
        assert_eq!(fixture.on_backend(&format!("docs/{name}")), None);
    }

    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.uploaded(), 0);
    assert_eq!(fixture.log.calls(MockOp::CreateDir), 0);
    assert_eq!(fixture.log.calls(MockOp::Move), 0);
    assert_eq!(fixture.names_on_backend("docs").await?, ["report.txt"]);
    assert_eq!(
        fixture.on_backend("docs/report.txt").as_deref(),
        Some(b"original".as_slice())
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "report.txt").await?),
        nfsstat3::NFS3_OK
    );
    Ok(())
}

const PARTIAL_SIZE: u64 = 4 * MIB;
const PARTIAL_EDIT_AT: u64 = 3 * MIB;

fn partial_config() -> TransferConfig {
    TransferConfig {
        read_ahead: MIB,
        request_size: MIB,
        ..TransferConfig::default()
    }
}

/// A file of [`PARTIAL_SIZE`] whose first bytes only are cached.
async fn partially_cached(
    fixture: &Fixture,
    nfs: &mut Client,
    data: &[u8],
) -> Result<nfs3_client::nfs3_types::nfs3::nfs_fh3> {
    fixture.mock.add_file("media/big.bin", data);
    let fh = nfs.walk("media/big.bin").await?;
    ensure!(nfs.read(&fh, 0, 4096).await? == slice(data, 0..4096)?);
    eventually("the first read and its read-ahead are fetched", || {
        covers(&fixture.log.fetched(), &(0..MIB))
    })
    .await?;
    ensure!(
        !covers(&fixture.log.fetched(), &(0..PARTIAL_SIZE)),
        "precondition: the object is partial, fetched {:?}",
        fixture.log.fetched()
    );
    Ok(fh)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_into_a_partial_object_replies_before_the_missing_ranges_are_fetched_and_uploads_one_version_despite_a_later_remote_change()
-> Result<()> {
    let fixture = Fixture::new(partial_config()).await?;
    let mut nfs = fixture.client().await?;
    let original = pattern(PARTIAL_SIZE);
    let changed = other_pattern(PARTIAL_SIZE);
    let fh = partially_cached(&fixture, &mut nfs, &original).await?;

    assert_eq!(
        nfs.write(&fh, PARTIAL_EDIT_AT, b"XY").await?,
        nfsstat3::NFS3_OK
    );

    ensure!(
        !covers(&fixture.log.fetched(), &(0..PARTIAL_SIZE)),
        "WRITE waited for the missing ranges: {:?}",
        fixture.log.fetched()
    );
    fixture
        .mock
        .rewrite_keeping_size_and_mtime("media/big.bin", &changed)?;
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    let placed = fixture.placed();
    assert_eq!(placed.len(), 1, "uploads: {}", placed.len());
    let upload = placed.first().map(Vec::as_slice);
    let on_original = edited(&original, PARTIAL_EDIT_AT, b"XY")?;
    let on_changed = edited(&changed, PARTIAL_EDIT_AT, b"XY")?;
    ensure!(
        upload == Some(on_original.as_slice()) || upload == Some(on_changed.as_slice()),
        "the upload mixes two remote versions"
    );
    ensure!(fixture.on_backend("media/big.bin").as_deref() == upload);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_into_a_partial_object_after_a_remote_change_uploads_one_version_never_a_mix()
-> Result<()> {
    let fixture = Fixture::new(partial_config()).await?;
    let mut nfs = fixture.client().await?;
    let original = pattern(PARTIAL_SIZE);
    let changed = other_pattern(PARTIAL_SIZE);
    let fh = partially_cached(&fixture, &mut nfs, &original).await?;
    fixture
        .mock
        .rewrite_keeping_size_and_mtime("media/big.bin", &changed)?;

    assert_eq!(
        nfs.write(&fh, PARTIAL_EDIT_AT, b"XY").await?,
        nfsstat3::NFS3_OK
    );

    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    let placed = fixture.placed();
    assert_eq!(placed.len(), 1);
    let upload = placed.first().map(Vec::as_slice);
    let on_original = edited(&original, PARTIAL_EDIT_AT, b"XY")?;
    let on_changed = edited(&changed, PARTIAL_EDIT_AT, b"XY")?;
    ensure!(
        upload == Some(on_original.as_slice()) || upload == Some(on_changed.as_slice()),
        "mixed upload"
    );
    let len = u32::try_from(PARTIAL_SIZE)?;
    let mut read = Vec::new();
    let mut offset = 0;
    while offset < u64::from(len) {
        read.extend(nfs.read(&fh, offset, 1024 * 1024).await?);
        offset += MIB;
    }
    ensure!(
        Some(read.as_slice()) == upload,
        "READ after the WRITE disagrees with the uploaded content"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_through_nfs_while_the_upload_runs_places_the_content_under_the_new_name()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let mut second = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let report = nfs.walk("docs/report.txt").await?;
    let uploading = fixture.mock.stop_at(MockOp::Upload);

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);
    reached_in_time(&uploading, "the upload").await?;
    let renaming = {
        let docs = docs.clone();
        tokio::spawn(async move { second.rename(&docs, "report.txt", &docs, "final.txt").await })
    };
    tokio::time::sleep(LET_RUN).await;
    uploading.release();

    assert_eq!(renaming.await??, nfsstat3::NFS3_OK);
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    let placed = fixture.log.placed();
    assert_eq!(placed.len(), 1, "{placed:?}");
    ensure!(
        placed
            .first()
            .and_then(|placed| placed.target.file_name())
            .is_some_and(|name| name == "final.txt"),
        "{placed:?}"
    );
    assert_eq!(
        fixture.on_backend("docs/final.txt").as_deref(),
        Some(b"orXYinal".as_slice())
    );
    assert_eq!(fixture.names_on_backend("docs").await?, ["final.txt"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_through_nfs_of_a_dirty_item_uploads_once_under_the_new_name() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let report = nfs.walk("docs/report.txt").await?;

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);
    assert_eq!(
        nfs.rename(&docs, "report.txt", &docs, "final.txt").await?,
        nfsstat3::NFS3_OK
    );

    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"orXYinal".to_vec()]);
    assert_eq!(
        fixture.on_backend("docs/final.txt").as_deref(),
        Some(b"orXYinal".as_slice())
    );
    assert_eq!(fixture.names_on_backend("docs").await?, ["final.txt"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_during_the_upload_of_an_earlier_write_leaves_one_upload_of_the_latest_content()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let mut second = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;
    let uploading = fixture.mock.stop_at(MockOp::Upload);

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);
    reached_in_time(&uploading, "the upload").await?;
    let writing = {
        let report = report.clone();
        tokio::spawn(async move { second.write(&report, 4, b"QQ").await })
    };
    tokio::time::sleep(LET_RUN).await;
    uploading.release();

    assert_eq!(writing.await??, nfsstat3::NFS3_OK);
    assert_eq!(nfs.read(&report, 0, 8).await?, b"orXYQQal");
    eventually("write-back places the latest content", || {
        fixture
            .log
            .placed()
            .iter()
            .any(|placed| placed.data == b"orXYQQal")
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"orXYQQal".to_vec()]);
    assert_eq!(fixture.names_on_backend("docs").await?, ["report.txt"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scan_seeing_a_remote_change_after_a_write_keeps_and_uploads_the_local_edit() -> Result<()>
{
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    assert_eq!(code(&nfs.names(&docs).await?), nfsstat3::NFS3_OK);
    let report = nfs.walk("docs/report.txt").await?;

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);
    fixture
        .mock
        .rewrite_keeping_size_and_mtime("docs/report.txt", b"REMOTE!!")?;
    fixture.daemon.cache.scan().await?;

    let read = nfs.read(&report, 0, 8).await?;
    ensure!(
        read == b"orXYinal" || read == b"REXYTE!!",
        "READ mixes two remote versions or lost the edit: {read:?}"
    );
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), std::slice::from_ref(&read));
    assert_eq!(fixture.on_backend("docs/report.txt"), Some(read));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_remove_through_nfs_of_a_dirty_item_cancels_its_upload() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let report = nfs.walk("docs/report.txt").await?;

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);
    assert_eq!(nfs.remove(&docs, "report.txt").await?, nfsstat3::NFS3_OK);

    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.names_on_backend("docs").await?, ["other.txt"]);
    ensure!(
        fixture.log.placed().is_empty(),
        "{:?}",
        fixture.log.placed()
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "report.txt").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_remove_through_nfs_while_the_upload_runs_leaves_neither_the_file_nor_its_temporary_upload()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let mut nfs = fixture.client().await?;
    let mut second = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let report = nfs.walk("docs/report.txt").await?;
    let uploading = fixture.mock.stop_at(MockOp::Upload);

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);
    reached_in_time(&uploading, "the upload").await?;
    let removing = {
        let docs = docs.clone();
        tokio::spawn(async move { second.remove(&docs, "report.txt").await })
    };
    tokio::time::sleep(LET_RUN).await;
    uploading.release();

    assert_eq!(removing.await??, nfsstat3::NFS3_OK);
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.names_on_backend("docs").await?, ["other.txt"]);
    ensure!(
        fixture.log.placed().is_empty(),
        "{:?}",
        fixture.log.placed()
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "report.txt").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn interleaved_writes_and_saves_upload_once_with_the_latest_content() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);
    fixture.save("docs/report.txt", b"fresh content").await?;
    assert_eq!(nfs.write(&report, 0, b"Z").await?, nfsstat3::NFS3_OK);

    assert_eq!(
        fixture.uploaded(),
        0,
        "uploaded before the write-back delay"
    );
    assert_eq!(nfs.read(&report, 0, 16).await?, b"Zresh content");
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"Zresh content".to_vec()]);
    assert_eq!(
        fixture.on_backend("docs/report.txt").as_deref(),
        Some(b"Zresh content".as_slice())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn create_then_write_makes_a_file_that_write_back_uploads() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(nfs.create(&docs, "new.txt").await?, nfsstat3::NFS3_OK);
    let new = match nfs.lookup(&docs, "new.txt").await? {
        Ok(fh) => fh,
        Err(code) => anyhow::bail!("the created file is not found: {code:?}"),
    };
    assert_eq!(nfs.write(&new, 0, b"fresh").await?, nfsstat3::NFS3_OK);

    eventually("write-back uploads the created file", || {
        fixture
            .log
            .placed()
            .iter()
            .any(|placed| placed.data == b"fresh")
    })
    .await?;
    assert_eq!(
        fixture.on_backend("docs/new.txt").as_deref(),
        Some(b"fresh".as_slice())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rename_remove_and_mkdir_with_the_remote_unreachable_answer_io_and_apply_nothing()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    fixture.mock.inject(MockOp::Move, Fault::Unreachable);
    assert_eq!(
        nfs.rename(&docs, "report.txt", &docs, "final.txt").await?,
        nfsstat3::NFS3ERR_IO
    );
    fixture.mock.inject(MockOp::Delete, Fault::Unreachable);
    assert_eq!(nfs.remove(&docs, "other.txt").await?, nfsstat3::NFS3ERR_IO);
    fixture.mock.inject(MockOp::CreateDir, Fault::Unreachable);
    assert_eq!(nfs.mkdir(&docs, "new").await?, nfsstat3::NFS3ERR_IO);

    assert_eq!(
        fixture.names_on_backend("docs").await?,
        ["other.txt", "report.txt"]
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "report.txt").await?),
        nfsstat3::NFS3_OK
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "other.txt").await?),
        nfsstat3::NFS3_OK
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "final.txt").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "new").await?),
        nfsstat3::NFS3ERR_NOENT
    );

    assert_eq!(
        nfs.rename(&docs, "report.txt", &docs, "final.txt").await?,
        nfsstat3::NFS3_OK,
        "the reissued RENAME"
    );
    assert_eq!(
        nfs.remove(&docs, "other.txt").await?,
        nfsstat3::NFS3_OK,
        "the reissued REMOVE"
    );
    assert_eq!(
        nfs.mkdir(&docs, "new").await?,
        nfsstat3::NFS3_OK,
        "the reissued MKDIR"
    );
    assert_eq!(
        fixture.names_on_backend("docs").await?,
        ["final.txt", "new"]
    );
    Ok(())
}

/// Faults enough for every retry a single READ may make.
const UNREACHABLE_CALLS: usize = 16;

#[tokio::test(flavor = "multi_thread")]
async fn with_the_remote_unreachable_a_read_of_cached_content_succeeds_and_of_uncached_content_answers_io()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/cached.txt", b"in the cache");
    fixture.mock.add_file("docs/remote.txt", b"only remote!");
    let mut nfs = fixture.client().await?;
    let cached = nfs.walk("docs/cached.txt").await?;
    let remote = nfs.walk("docs/remote.txt").await?;
    assert_eq!(nfs.read(&cached, 0, 12).await?, b"in the cache");
    eventually("the whole file is cached", || {
        covers(&fixture.log.fetched(), &(0..12))
    })
    .await?;
    for _ in 0..UNREACHABLE_CALLS {
        for op in [MockOp::Download, MockOp::Metadata, MockOp::Revision] {
            fixture.mock.inject(op, Fault::Unreachable);
        }
    }

    assert_eq!(nfs.read(&cached, 0, 12).await?, b"in the cache");
    assert_eq!(
        code(&nfs.read_status(&remote, 0, 12).await?),
        nfsstat3::NFS3ERR_IO
    );
    assert_eq!(fixture.uploaded(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_while_the_remote_is_unreachable_is_accepted_and_uploaded_after_it_returns()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;
    assert_eq!(nfs.read(&report, 0, 8).await?, b"original");
    eventually("the whole file is cached", || {
        covers(&fixture.log.fetched(), &(0..8))
    })
    .await?;
    for op in [MockOp::Download, MockOp::Upload] {
        fixture.mock.inject(op, Fault::Unreachable);
    }

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);

    assert_eq!(nfs.read(&report, 0, 8).await?, b"orXYinal");
    eventually_within(SETTLE_AFTER_RETRY, "write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    assert_eq!(fixture.placed(), [b"orXYinal".to_vec()]);
    assert_eq!(
        fixture.on_backend("docs/report.txt").as_deref(),
        Some(b"orXYinal".as_slice())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_small_write_into_a_large_uncached_file_replies_without_downloading_it() -> Result<()> {
    const SIZE: u64 = 16 * MIB;
    const AT: u64 = 12 * MIB;
    let fixture = Fixture::new(partial_config()).await?;
    let data = pattern(SIZE);
    fixture.mock.add_file("media/large.bin", &data);
    let mut nfs = fixture.client().await?;
    let fh = nfs.walk("media/large.bin").await?;
    let edit = other_pattern(4096);

    assert_eq!(nfs.write(&fh, AT, &edit).await?, nfsstat3::NFS3_OK);

    assert_eq!(fixture.log.fetched(), [], "WRITE fetched file content");
    assert_eq!(fixture.log.calls(MockOp::Download), 0);
    ensure!(
        nfs.read(&fh, AT, 4096).await? == edit,
        "READ of the written bytes"
    );
    assert_eq!(
        fixture.log.fetched(),
        [],
        "a READ of written bytes went to the backend"
    );
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    let expected = edited(&data, AT, &edit)?;
    let placed = fixture.placed();
    assert_eq!(placed.len(), 1);
    ensure!(placed.first() == Some(&expected), "uploaded content");
    ensure!(fixture.on_backend("media/large.bin") == Some(expected));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sequential_writes_mint_one_content_version_and_upload_once() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;
    let uid = fixture.uid("docs/report.txt").await?;
    let item = |uid| -> Result<_> {
        fixture
            .daemon
            .cache
            .item(uid)?
            .ok_or_else(|| anyhow::anyhow!("item {uid} is gone"))
    };
    let before = item(uid)?;
    let revision = fixture.daemon.db.replica_meta()?.revision;

    for (offset, chunk) in [(0, b"OR"), (2, b"IG"), (4, b"IN"), (6, b"AL"), (8, b"!!")] {
        assert_eq!(nfs.write(&report, offset, chunk).await?, nfsstat3::NFS3_OK);
        let now = item(uid)?;
        assert_eq!(
            now.content_version, before.content_version,
            "a WRITE minted a content version"
        );
        assert_eq!(now.size, Some(8.max(offset + 2)));
    }
    assert_eq!(
        fixture.daemon.db.replica_meta()?.revision,
        revision,
        "a WRITE advanced the replica revision"
    );

    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"ORIGINAL!!".to_vec()]);
    let after = item(uid)?;
    assert_ne!(after.content_version, before.content_version);
    ensure!(after.uploaded, "the item is not marked uploaded");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_into_an_uncached_file_while_the_remote_is_unreachable_is_accepted_and_uploaded_after_it_returns()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;
    for op in [MockOp::Metadata, MockOp::Download] {
        fixture.mock.inject(op, Fault::Unreachable);
    }

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);

    assert_eq!(fixture.uploaded(), 0);
    eventually_within(SETTLE_AFTER_RETRY, "write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    assert_eq!(fixture.placed(), [b"orXYinal".to_vec()]);
    assert_eq!(
        fixture.on_backend("docs/report.txt").as_deref(),
        Some(b"orXYinal".as_slice())
    );
    assert_eq!(nfs.read(&report, 0, 8).await?, b"orXYinal");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_lists_a_never_listed_folder_through_the_replica_without_temporary_uploads()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/a.txt", b"a");
    fixture.mock.add_file("docs/b.txt", b"b");
    fixture
        .mock
        .add_file("docs/.cascade-tmp-42", b"partial upload");
    fixture.mock.add_file("docs/sub/c.txt", b"c");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(
        nfs.names(&docs).await?,
        Ok(vec!["a.txt".into(), "b.txt".into(), "sub".into()])
    );
    let plus: Vec<String> = match nfs.listing(&docs).await? {
        Ok(entries) => entries.into_iter().map(|entry| entry.name).collect(),
        Err(code) => anyhow::bail!("READDIRPLUS: {code:?}"),
    };
    ensure!(
        !plus.iter().any(|name| name.starts_with(".cascade-tmp-")),
        "READDIRPLUS lists a temporary upload: {plus:?}"
    );
    assert_eq!(
        code(&nfs.lookup(&docs, ".cascade-tmp-42").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "missing.txt").await?),
        nfsstat3::NFS3ERR_NOENT
    );

    fixture.mock.add_file("docs/d.txt", b"d");
    fixture
        .mock
        .add_file("docs/.cascade-tmp-43", b"partial upload");
    ensure!(
        fixture.daemon.cache.scan().await?,
        "the listed folder is not in the scan set"
    );

    assert_eq!(
        nfs.names(&docs).await?,
        Ok(vec![
            "a.txt".into(),
            "b.txt".into(),
            "d.txt".into(),
            "sub".into()
        ])
    );
    Ok(())
}

const fn projects_mount() -> Layout {
    Layout::Mounting(Mount {
        prefix: "/docs/projects",
        id: "projects",
        registered_in_cache: true,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_of_a_folder_holding_a_nested_mount_lists_the_mount() -> Result<()> {
    let fixture = Fixture::build(
        TransferConfig::default(),
        projects_mount(),
        NfsCacheMode::default(),
    )
    .await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.other.add_file("plan.txt", b"the plan");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(
        nfs.names(&docs).await?,
        Ok(vec!["projects".into(), "report.txt".into()])
    );
    let plan = nfs.walk("docs/projects/plan.txt").await?;
    assert_eq!(nfs.read(&plan, 0, 8).await?, b"the plan");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_of_a_cached_folder_keeps_a_backend_entry_named_like_a_nested_mount() -> Result<()>
{
    let fixture = Fixture::build(
        TransferConfig::default(),
        projects_mount(),
        NfsCacheMode::default(),
    )
    .await?;
    fixture.mock.add_file("docs/projects", b"a file");
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    let entries = match nfs.listing(&docs).await? {
        Ok(entries) => entries,
        Err(code) => anyhow::bail!("READDIRPLUS: {code:?}"),
    };

    let mut listed: Vec<(String, Option<ftype3>)> = entries
        .into_iter()
        .map(|entry| (entry.name, entry.kind))
        .collect();
    listed.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        listed,
        [
            ("projects".to_owned(), Some(ftype3::NF3REG)),
            ("report.txt".to_owned(), Some(ftype3::NF3REG)),
        ],
        "a cached listing applies the shadow rule of merge_listing"
    );
    Ok(())
}

const MKDIR_ONTO_MOUNT: nfsstat3 = nfsstat3::NFS3ERR_EXIST;
const RENAME_ONTO_MOUNT: nfsstat3 = nfsstat3::NFS3ERR_EXIST;

/// The root backend holding `docs/report.txt` and, mounted at
/// `docs/projects`, a nested backend holding `nested`.
async fn projects_beside_a_report(nested: &[&str]) -> Result<Fixture> {
    let fixture = Fixture::build(
        TransferConfig::default(),
        projects_mount(),
        NfsCacheMode::default(),
    )
    .await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    for name in nested {
        fixture.other.add_file(name, b"the plan");
    }
    Ok(fixture)
}

/// Asserts neither backend took a delete or an upload, both keep their
/// names, and the nested root still lists `nested`.
async fn assert_both_backends_untouched(
    fixture: &Fixture,
    nfs: &mut Client,
    nested: &[&str],
) -> Result<()> {
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        fixture.log.calls(MockOp::Delete),
        0,
        "deletes on the root backend"
    );
    assert_eq!(
        fixture.other_log.calls(MockOp::Delete),
        0,
        "deletes on the nested backend"
    );
    assert_eq!(fixture.uploaded(), 0, "uploads to the root backend");
    assert_eq!(fixture.names_on_backend("docs").await?, ["report.txt"]);
    assert_eq!(names_in(&fixture.other, "").await?, nested);
    let projects = nfs.walk("docs/projects").await?;
    assert_eq!(
        nfs.names(&projects).await?,
        Ok(nested.iter().map(|&name| name.to_owned()).collect())
    );
    Ok(())
}

async fn remove_and_rmdir_of_the_mount_point_answer_an_error_and_delete_nothing(
    nested: &[&str],
) -> Result<()> {
    let fixture = projects_beside_a_report(nested).await?;
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    let removed = nfs.remove(&docs, "projects").await?;
    let rmdired = nfs.rmdir(&docs, "projects").await?;

    assert_ne!(removed, nfsstat3::NFS3_OK, "REMOVE of the mount point");
    assert_ne!(rmdired, nfsstat3::NFS3_OK, "RMDIR of the mount point");
    assert_both_backends_untouched(&fixture, &mut nfs, nested).await
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_and_rmdir_of_an_empty_nested_mount_points_name_answer_an_error_and_delete_on_neither_backend()
-> Result<()> {
    remove_and_rmdir_of_the_mount_point_answer_an_error_and_delete_nothing(&[]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_and_rmdir_of_a_never_listed_nested_mount_points_name_answer_an_error_and_delete_on_neither_backend()
-> Result<()> {
    remove_and_rmdir_of_the_mount_point_answer_an_error_and_delete_nothing(&["plan.txt"]).await
}

/// CREATE of a nested mount point's name answers EXIST in every mode: RFC
/// 1813 section 3.3.8 lets UNCHECKED skip only the check for a duplicate
/// file, and a regular file cannot take the place of a directory, so
/// UNCHECKED answers `NFS3ERR_EXIST` as the Linux server does for an
/// existing name that is not a regular file.
#[tokio::test(flavor = "multi_thread")]
async fn create_of_a_nested_mount_points_name_answers_exist_in_every_mode_and_writes_nothing()
-> Result<()> {
    let fixture = projects_beside_a_report(&["plan.txt"]).await?;
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    let mut answers = Vec::new();
    for (mode, how) in [
        ("UNCHECKED", createhow3::UNCHECKED(sattr3::default())),
        ("GUARDED", createhow3::GUARDED(sattr3::default())),
        ("EXCLUSIVE", createhow3::EXCLUSIVE(createverf3([7; 8]))),
    ] {
        answers.push((mode, code(&nfs.create_how(&docs, "projects", how).await?)));
    }

    assert_eq!(
        answers,
        [
            ("UNCHECKED", nfsstat3::NFS3ERR_EXIST),
            ("GUARDED", nfsstat3::NFS3ERR_EXIST),
            ("EXCLUSIVE", nfsstat3::NFS3ERR_EXIST),
        ],
        "CREATE of the mount point's name"
    );
    ensure!(
        fixture.daemon.resolve("docs/projects").await?.is_none(),
        "docs/projects entered the root backend's replica"
    );
    assert_both_backends_untouched(&fixture, &mut nfs, &["plan.txt"]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn mkdir_and_rename_onto_a_nested_mount_points_name_keep_their_answers_and_change_nothing()
-> Result<()> {
    let fixture = projects_beside_a_report(&["plan.txt"]).await?;
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    let made = nfs.mkdir(&docs, "projects").await?;
    let renamed = nfs.rename(&docs, "report.txt", &docs, "projects").await?;

    assert_eq!(made, MKDIR_ONTO_MOUNT, "MKDIR of the mount point");
    assert_eq!(renamed, RENAME_ONTO_MOUNT, "RENAME onto the mount point");
    assert_both_backends_untouched(&fixture, &mut nfs, &["plan.txt"]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_on_a_backend_the_cache_does_not_serve_is_served_by_that_backend() -> Result<()> {
    let fixture = Fixture::build(
        TransferConfig::default(),
        Layout::Mounting(Mount {
            prefix: "/archive",
            id: "archive",
            registered_in_cache: false,
        }),
        NfsCacheMode::default(),
    )
    .await?;
    fixture.other.add_file("2025/notes.txt", b"old notes!!!");
    let mut nfs = fixture.client().await?;
    let year = nfs.walk("archive/2025").await?;

    assert_eq!(nfs.names(&year).await?, Ok(vec!["notes.txt".to_owned()]));
    let notes = nfs.walk("archive/2025/notes.txt").await?;
    assert_eq!(nfs.read(&notes, 0, 12).await?, b"old notes!!!");
    assert_eq!(fixture.log.fetched(), [], "the cached backend was asked");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_mode_off_refuses_every_mutating_procedure_with_rofs_although_a_content_cache_is_attached()
-> Result<()> {
    let fixture =
        Fixture::build(TransferConfig::default(), Layout::Root, NfsCacheMode::Off).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/sub/inner.txt", b"inner");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let report = nfs.walk("docs/report.txt").await?;

    let refused = [
        ("WRITE", nfs.write(&report, 2, b"XY").await?),
        ("SETATTR size", nfs.set_size(&report, 2).await?),
        ("CREATE", nfs.create(&docs, "new.txt").await?),
        (
            "CREATE GUARDED",
            code(
                &nfs.create_how(&docs, "guarded.txt", createhow3::GUARDED(sattr3::default()))
                    .await?,
            ),
        ),
        ("MKDIR", nfs.mkdir(&docs, "new").await?),
        ("REMOVE", nfs.remove(&docs, "report.txt").await?),
        ("RMDIR", nfs.rmdir(&docs, "sub").await?),
        (
            "RENAME",
            nfs.rename(&docs, "report.txt", &docs, "final.txt").await?,
        ),
    ];

    for (procedure, status) in refused {
        assert_eq!(status, nfsstat3::NFS3ERR_ROFS, "{procedure}");
    }
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.uploaded(), 0);
    assert_eq!(nfs.read(&report, 0, 8).await?, b"original");
    assert_eq!(
        fixture.names_on_backend("docs").await?,
        ["report.txt", "sub"]
    );
    assert_eq!(fixture.names_on_backend("docs/sub").await?, ["inner.txt"]);
    Ok(())
}
