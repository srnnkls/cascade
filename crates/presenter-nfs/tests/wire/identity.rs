//! Handles and file ids from item uids, their generation across restarts,
//! and stopping and running presenters side by side.

use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use cascade_engine::content::transfer::TransferConfig;
use nfs3_client::nfs3_types::nfs3::{nfs_fh3, nfsstat3};
use tokio::net::TcpStream;

use crate::support::client::{Client, code};
use crate::support::fixture::{Fixture, eventually};

/// Bytes of the handle generation the server prefixes to every handle.
const GENERATION_LEN: usize = 8;
/// Bound on a stopped server's listener closing.
const CLOSE: Duration = Duration::from_secs(5);
/// Window in which a stopped server must not answer.
const SILENCE: Duration = Duration::from_secs(2);

fn generation(fh: &nfs_fh3) -> Result<&[u8]> {
    fh.data
        .get(..GENERATION_LEN)
        .context("handle shorter than its generation")
}

/// Asserts `fh` is the generation followed by `uid` in one fixed byte order.
fn assert_handle_carries(fh: &nfs_fh3, uid: u64, what: &str) -> Result<()> {
    ensure!(
        fh.data.len() == GENERATION_LEN + 8,
        "{what}: handle of {} bytes, expected the generation and an 8-byte uid",
        fh.data.len()
    );
    let id = fh.data.get(GENERATION_LEN..).context("handle id")?;
    ensure!(
        id == uid.to_be_bytes() || id == uid.to_le_bytes(),
        "{what}: handle id {id:02x?} is not uid {uid}"
    );
    Ok(())
}

async fn fileid(nfs: &mut Client, fh: &nfs_fh3) -> Result<u64> {
    match nfs.getattr(fh).await? {
        Ok(attrs) => Ok(attrs.fileid),
        Err(code) => bail!("GETATTR: {code:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn handles_and_file_ids_are_the_item_uids() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let mut nfs = fixture.client().await?;

    let docs = nfs.walk("docs").await?;
    let report = nfs.walk("docs/report.txt").await?;

    let docs_uid = fixture.uid("docs").await?;
    let report_uid = fixture.uid("docs/report.txt").await?;
    assert_handle_carries(&docs, docs_uid, "docs")?;
    assert_handle_carries(&report, report_uid, "docs/report.txt")?;
    assert_eq!(
        generation(&docs)?,
        generation(&nfs.root())?,
        "generation of docs"
    );
    assert_eq!(fileid(&mut nfs, &docs).await?, docs_uid);
    assert_eq!(fileid(&mut nfs, &report).await?, report_uid);
    let listed = match nfs.listing(&docs).await? {
        Ok(entries) => entries,
        Err(code) => bail!("READDIRPLUS: {code:?}"),
    };
    let entry = listed
        .iter()
        .find(|entry| entry.name == "report.txt")
        .context("report.txt not listed")?;
    assert_eq!(entry.fileid, report_uid, "READDIRPLUS fileid");
    assert_eq!(entry.handle.as_ref(), Some(&report), "READDIRPLUS handle");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handles_stay_valid_across_a_restart_on_the_same_database() -> Result<()> {
    let mut fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let root = nfs.root();
    let docs = nfs.walk("docs").await?;
    let report = nfs.walk("docs/report.txt").await?;
    let report_uid = fixture.uid("docs/report.txt").await?;

    fixture.restart().await?;
    let mut after = fixture.client().await?;

    assert_eq!(after.root(), root, "the export root handle changed");
    assert_eq!(
        after.walk("docs/report.txt").await?,
        report,
        "LOOKUP minted another handle"
    );
    assert_eq!(fileid(&mut after, &report).await?, report_uid);
    assert_eq!(after.read(&report, 0, 8).await?, b"original");
    assert_eq!(
        after.names(&docs).await?,
        Ok(vec!["report.txt".to_owned()]),
        "READDIR of a handle from before the restart"
    );
    assert_eq!(after.write(&report, 2, b"XY").await?, nfsstat3::NFS3_OK);
    eventually("the write after the restart reaches the backend", || {
        fixture.on_backend("docs/report.txt").as_deref() == Some(b"orXYinal".as_slice())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_state_database_mints_handles_of_another_generation() -> Result<()> {
    let first = Fixture::new(TransferConfig::default()).await?;
    let second = Fixture::new(TransferConfig::default()).await?;
    for fixture in [&first, &second] {
        fixture.mock.add_file("docs/report.txt", b"original");
    }
    let mut old = first.client().await?;
    let mut new = second.client().await?;

    let old_report = old.walk("docs/report.txt").await?;
    let new_report = new.walk("docs/report.txt").await?;

    assert_ne!(
        generation(&old_report)?,
        generation(&new_report)?,
        "two state databases share a handle generation"
    );
    let status = code(&new.getattr(&old_report).await?);
    ensure!(
        matches!(
            status,
            nfsstat3::NFS3ERR_STALE | nfsstat3::NFS3ERR_BADHANDLE
        ),
        "a handle of another database answered {status:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_presenter_accepts_no_connection_and_serves_no_later_request() -> Result<()> {
    let mut fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let addr = fixture.addr()?;
    let mut nfs = fixture.client().await?;
    let report = nfs.walk("docs/report.txt").await?;

    fixture.stop()?;

    let deadline = tokio::time::Instant::now() + CLOSE;
    while TcpStream::connect(addr).await.is_ok() {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the stopped presenter still accepts connections after {CLOSE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let late = tokio::time::timeout(SILENCE, nfs.read_status(&report, 0, 8)).await;
    ensure!(
        !matches!(late, Ok(Ok(Ok(_)))),
        "a connection of the stopped presenter served a READ: {late:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn two_presenters_in_one_process_serve_side_by_side_and_stop_independently() -> Result<()> {
    let mut first = Fixture::new(TransferConfig::default()).await?;
    let second = Fixture::new(TransferConfig::default()).await?;
    first.mock.add_file("one.txt", b"first!");
    second.mock.add_file("two.txt", b"second");
    ensure!(
        first.addr()?.port() != second.addr()?.port(),
        "one port for two presenters"
    );
    let mut one = first.client().await?;
    let mut two = second.client().await?;

    assert_eq!(
        one.names(&one.root()).await?,
        Ok(vec!["one.txt".to_owned()])
    );
    assert_eq!(
        two.names(&two.root()).await?,
        Ok(vec!["two.txt".to_owned()])
    );

    first.stop()?;

    let file = two.walk("two.txt").await?;
    assert_eq!(two.read(&file, 0, 6).await?, b"second");
    let mut again = second.client().await?;
    assert_eq!(
        again.names(&again.root()).await?,
        Ok(vec!["two.txt".to_owned()])
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handle_taken_before_its_folder_is_renamed_on_the_backend_reads_the_file_after_a_scan()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/sub/a.txt", b"content");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let sub = nfs.walk("docs/sub").await?;
    assert_eq!(nfs.names(&docs).await?, Ok(vec!["sub".to_owned()]));
    assert_eq!(nfs.names(&sub).await?, Ok(vec!["a.txt".to_owned()]));
    let before = nfs.walk("docs/sub/a.txt").await?;
    let uid = fixture.uid("docs/sub/a.txt").await?;

    fixture.mock.rename("docs/sub", "docs/renamed")?;
    fixture.daemon.cache.scan().await?;

    assert_eq!(nfs.read(&before, 0, 7).await?, b"content");
    assert_eq!(fileid(&mut nfs, &before).await?, uid);
    let after = nfs.walk("docs/renamed/a.txt").await?;
    assert_eq!(fileid(&mut nfs, &after).await?, uid);
    assert_eq!(
        code(&nfs.lookup(&docs, "sub").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    Ok(())
}
