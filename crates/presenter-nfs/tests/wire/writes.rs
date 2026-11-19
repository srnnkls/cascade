//! WRITE lands in the cache before its reply with the stability it has,
//! COMMIT, 1 MiB transfers, and a slow backend call delaying no other
//! item on the connection.

use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::testing::MockOp;
use nfs3_client::nfs3_types::nfs3::{nfsstat3, stable_how};
use tokio::net::TcpStream;

use crate::support::client::code;
use crate::support::fixture::{Fixture, QUIET, SETTLE, eventually};
use crate::support::pipeline::{next_reply, send_read};
use crate::support::{MIB, covers, pattern};

const ONE_MIB: u32 = 1024 * 1024;
/// Bound on a reply that no backend call delays.
const PROMPT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn a_write_is_in_the_cache_before_its_reply() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;
    let uid = fixture.uid("docs/report.txt").await?;

    assert_eq!(nfs.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);

    let objects = fixture.daemon.db.content_objects_of(uid)?;
    let object = objects
        .first()
        .context("no cache object after the WRITE reply")?;
    ensure!(
        object.dirty || object.edited,
        "the object holds the WRITE but is recorded clean: {object:?}"
    );
    let ranges = fixture.daemon.db.content_ranges(object.object_id)?;
    ensure!(
        covers(&ranges, &(2..4)),
        "written range not recorded: {ranges:?}"
    );
    let mut written = [0u8; 2];
    let read = fixture
        .objects()?
        .read_at(object.object_id, 2, &mut written)?;
    assert_eq!(
        written.get(..read),
        Some(b"XY".as_slice()),
        "the object file lacks the written bytes"
    );
    assert_eq!(fixture.uploaded(), 0, "WRITE uploaded before its reply");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_are_answered_with_the_stability_they_have_and_commit_succeeds() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;

    let unstable = nfs
        .write_stable(&report, 0, b"O", stable_how::UNSTABLE)
        .await?;
    let data_sync = nfs
        .write_stable(&report, 1, b"R", stable_how::DATA_SYNC)
        .await?;
    let file_sync = nfs
        .write_stable(&report, 2, b"I", stable_how::FILE_SYNC)
        .await?;
    let commit = nfs.commit(&report).await?;

    let (Ok(unstable), Ok(data_sync), Ok(file_sync), Ok(commit_verf)) =
        (unstable, data_sync, file_sync, commit)
    else {
        bail!("a WRITE or COMMIT failed");
    };
    assert_eq!(unstable.count, 1);
    ensure!(
        matches!(
            data_sync.committed,
            stable_how::DATA_SYNC | stable_how::FILE_SYNC
        ),
        "DATA_SYNC WRITE answered {}",
        data_sync.committed
    );
    assert_eq!(
        file_sync.committed,
        stable_how::FILE_SYNC,
        "FILE_SYNC WRITE"
    );
    assert_eq!(
        unstable.verf, commit_verf,
        "the COMMIT verifier differs from the WRITE verifier of the same server"
    );
    assert_eq!(nfs.read(&report, 0, 8).await?, b"ORIginal");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn one_mib_read_and_write_are_accepted_and_advertised() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    let original = pattern(4 * MIB);
    fixture.mock.add_file("media/big.bin", &original);
    let mut nfs = fixture.client().await?;
    let big = nfs.walk("media/big.bin").await?;

    let info = match nfs.fsinfo(&nfs.root()).await? {
        Ok(info) => info,
        Err(code) => bail!("FSINFO: {code:?}"),
    };
    ensure!(info.rtmax >= ONE_MIB, "rtmax {}", info.rtmax);
    ensure!(info.wtmax >= ONE_MIB, "wtmax {}", info.wtmax);

    let read = nfs.read(&big, MIB, ONE_MIB).await?;
    ensure!(
        read.as_slice()
            == original
                .get(1024 * 1024..2 * 1024 * 1024)
                .context("slice")?,
        "1 MiB READ returned {} other bytes",
        read.len()
    );
    let block = vec![0x5A_u8; 1024 * 1024];
    let written = nfs
        .write_stable(&big, 2 * MIB, &block, stable_how::UNSTABLE)
        .await?;
    match written {
        Ok(written) => assert_eq!(written.count, ONE_MIB, "1 MiB WRITE count"),
        Err(code) => bail!("1 MiB WRITE: {code:?}"),
    }
    ensure!(
        nfs.read(&big, 2 * MIB, ONE_MIB).await? == block,
        "READ after the 1 MiB WRITE"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_five_quiet_seconds_apart_from_the_next_upload_once_as_one_save() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;

    for (offset, chunk) in [(0, b"OR"), (2, b"IG"), (4, b"IN"), (6, b"AL")] {
        assert_eq!(nfs.write(&report, offset, chunk).await?, nfsstat3::NFS3_OK);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(fixture.uploaded(), 0, "uploaded while writes kept coming");
    }

    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"ORIGINAL".to_vec()]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backend_call_held_for_one_item_does_not_delay_another_item_on_the_same_connection()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/slow.bin", &pattern(64 * 1024));
    fixture.mock.add_file("docs/fast.txt", b"fast reply");
    let mut nfs = fixture.client().await?;
    let slow = nfs.walk("docs/slow.bin").await?;
    let fast = nfs.walk("docs/fast.txt").await?;
    let mut stream = TcpStream::connect(fixture.addr()?).await?;
    stream.set_nodelay(true)?;
    let held = fixture.mock.stop_at(MockOp::Download);

    send_read(&mut stream, 1, &slow, 0, 64 * 1024).await?;
    tokio::time::timeout(SETTLE, held.reached())
        .await
        .context("the slow READ never reached the backend")?;
    send_read(&mut stream, 2, &fast, 0, 10).await?;
    let first = next_reply(&mut stream, PROMPT).await;
    held.release();

    let first = first.context("the second READ waited behind the held backend call")?;
    assert_eq!(
        first.xid, 2,
        "the held READ was answered before its backend call returned"
    );
    assert_eq!(first.status, nfsstat3::NFS3_OK);
    let second = next_reply(&mut stream, SETTLE).await?;
    assert_eq!((second.xid, second.status), (1, nfsstat3::NFS3_OK));
    assert_eq!(
        code(&nfs.read_status(&fast, 0, 10).await?),
        nfsstat3::NFS3_OK
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_of_a_handle_whose_item_is_gone_answers_stale() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/gone.txt", b"soon gone");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let gone = nfs.walk("docs/gone.txt").await?;
    assert_eq!(nfs.write(&gone, 0, b"XY").await?, nfsstat3::NFS3_OK);
    assert_eq!(nfs.remove(&docs, "gone.txt").await?, nfsstat3::NFS3_OK);

    assert_eq!(code(&nfs.commit(&gone).await?), nfsstat3::NFS3ERR_STALE);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_sync_write_reads_back_after_an_abrupt_stop_and_a_restart_on_the_same_database()
-> Result<()> {
    let mut fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;

    let written = nfs
        .write_stable(&report, 2, b"XY", stable_how::FILE_SYNC)
        .await?;
    ensure!(
        matches!(written, Ok(ref ok) if ok.committed == stable_how::FILE_SYNC),
        "FILE_SYNC WRITE answered {written:?}"
    );
    fixture.restart().await?;

    let mut after = fixture.client().await?;
    assert_eq!(after.read(&report, 0, 8).await?, b"orXYinal");
    Ok(())
}
