//! RFC 1813 status codes the content-cache presenter answers.

use anyhow::{Context as _, Result, ensure};
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::testing::MockOp;
use nfs3_client::nfs3_types::nfs3::{createhow3, createverf3, nfs_fh3, nfsstat3, sattr3};
use nfs3_client::nfs3_types::xdr_codec::Opaque;

use crate::support::client::{Client, code};
use crate::support::fixture::{Fixture, QUIET};

/// Bytes of the handle generation the server prefixes to every handle.
const GENERATION_LEN: usize = 8;
/// An item uid no fixture assigns, the same in either byte order.
const UNKNOWN_UID: [u8; 8] = [0xA5; 8];

const fn handle(bytes: Vec<u8>) -> nfs_fh3 {
    nfs_fh3 {
        data: Opaque::owned(bytes),
    }
}

/// A well-formed handle of this server naming an item it does not hold.
fn unknown_handle(root: &nfs_fh3) -> Result<nfs_fh3> {
    let generation = root
        .data
        .get(..GENERATION_LEN)
        .context("the root handle is shorter than its generation")?;
    let mut bytes = generation.to_vec();
    bytes.extend_from_slice(&UNKNOWN_UID);
    Ok(handle(bytes))
}

/// A root-level `report.txt` and `docs/report.txt`.
async fn two_reports() -> Result<Fixture> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("report.txt", b"at the root");
    fixture.mock.add_file("docs/report.txt", b"original");
    Ok(fixture)
}

fn backend_mutations(fixture: &Fixture) -> usize {
    fixture.uploaded()
        + [MockOp::CreateDir, MockOp::Delete, MockOp::Move]
            .into_iter()
            .map(|op| fixture.log.calls(op))
            .sum::<usize>()
}

/// The status of every procedure that takes `fh` as its object or directory,
/// RENAME both as source and as target directory.
async fn statuses_under(
    nfs: &mut Client,
    fh: &nfs_fh3,
    docs: &nfs_fh3,
) -> Result<Vec<(&'static str, nfsstat3)>> {
    Ok(vec![
        ("GETATTR", code(&nfs.getattr(fh).await?)),
        ("SETATTR", nfs.set_size(fh, 0).await?),
        ("LOOKUP", code(&nfs.lookup(fh, "report.txt").await?)),
        ("ACCESS", code(&nfs.access(fh).await?)),
        ("READ", code(&nfs.read_status(fh, 0, 8).await?)),
        ("WRITE", nfs.write(fh, 0, b"XY").await?),
        ("CREATE", nfs.create(fh, "fresh.txt").await?),
        ("MKDIR", nfs.mkdir(fh, "new").await?),
        ("REMOVE", nfs.remove(fh, "report.txt").await?),
        ("RMDIR", nfs.rmdir(fh, "docs").await?),
        (
            "RENAME from",
            nfs.rename(fh, "report.txt", docs, "moved.txt").await?,
        ),
        (
            "RENAME into",
            nfs.rename(docs, "report.txt", fh, "report.txt").await?,
        ),
        ("READDIR", code(&nfs.names(fh).await?)),
        ("READDIRPLUS", code(&nfs.listing(fh).await?)),
        ("FSSTAT", code(&nfs.fsstat(fh).await?)),
        ("FSINFO", code(&nfs.fsinfo(fh).await?)),
        ("PATHCONF", code(&nfs.pathconf(fh).await?)),
        ("COMMIT", code(&nfs.commit(fh).await?)),
    ])
}

/// Nothing a call under a foreign handle sent reached the root, `docs` or
/// the backend.
async fn assert_nothing_applied(fixture: &Fixture) -> Result<()> {
    tokio::time::sleep(QUIET).await;
    fixture.assert_root_report_untouched().await?;
    for name in ["fresh.txt", "new", "moved.txt"] {
        fixture.assert_absent_at_root(name).await?;
    }
    assert_eq!(
        fixture.on_backend("docs/report.txt").as_deref(),
        Some(b"original".as_slice())
    );
    assert_eq!(fixture.on_backend("docs/moved.txt"), None);
    assert_eq!(backend_mutations(fixture), 0, "{:?}", fixture.log.placed());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_name_answers_noent_on_lookup_remove_rmdir_and_rename_source() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(
        code(&nfs.lookup(&docs, "missing.txt").await?),
        nfsstat3::NFS3ERR_NOENT,
        "LOOKUP"
    );
    assert_eq!(
        nfs.remove(&docs, "missing.txt").await?,
        nfsstat3::NFS3ERR_NOENT,
        "REMOVE"
    );
    assert_eq!(
        nfs.rmdir(&docs, "missing").await?,
        nfsstat3::NFS3ERR_NOENT,
        "RMDIR"
    );
    assert_eq!(
        nfs.rename(&docs, "missing.txt", &docs, "found.txt").await?,
        nfsstat3::NFS3ERR_NOENT,
        "RENAME source"
    );

    assert_eq!(fixture.log.calls(MockOp::Delete), 0);
    assert_eq!(fixture.log.calls(MockOp::Move), 0);
    assert_eq!(fixture.names_on_backend("docs").await?, ["report.txt"]);
    Ok(())
}

/// Procedures `nfs3_server` answers OK whatever `getattr` of the handle gives:
/// they read nothing from the item and change nothing.
const ANSWERED_WITHOUT_THE_ITEM: [&str; 3] = ["ACCESS", "FSSTAT", "PATHCONF"];

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_handle_answers_stale_on_every_procedure_reading_or_changing_the_item_carries_no_attributes_and_applies_nothing()
-> Result<()> {
    let fixture = two_reports().await?;
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let unknown = unknown_handle(&nfs.root())?;

    let statuses = statuses_under(&mut nfs, &unknown, &docs).await?;
    let attributes = [
        ("ACCESS", nfs.access(&unknown).await?),
        ("FSSTAT", nfs.fsstat(&unknown).await?),
        ("PATHCONF", nfs.pathconf(&unknown).await?),
    ];

    let wrong: Vec<_> = statuses
        .iter()
        .filter(|(procedure, status)| {
            !ANSWERED_WITHOUT_THE_ITEM.contains(procedure) && *status != nfsstat3::NFS3ERR_STALE
        })
        .collect();
    ensure!(
        wrong.is_empty(),
        "not STALE under an unknown handle: {wrong:?}"
    );
    let carrying: Vec<_> = attributes
        .iter()
        .filter(|(_, reply)| matches!(reply, Ok(Some(_))))
        .collect();
    ensure!(
        carrying.is_empty(),
        "attributes of some item under an unknown handle: {carrying:?}"
    );
    assert_nothing_applied(&fixture).await
}

#[tokio::test(flavor = "multi_thread")]
async fn the_handle_of_a_removed_item_answers_stale() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/gone.txt", b"soon gone");
    fixture.mock.add_file("docs/old/inner.txt", b"inner");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let gone = nfs.walk("docs/gone.txt").await?;
    let inner = nfs.walk("docs/old/inner.txt").await?;
    let old = nfs.walk("docs/old").await?;

    assert_eq!(nfs.remove(&docs, "gone.txt").await?, nfsstat3::NFS3_OK);
    assert_eq!(nfs.remove(&old, "inner.txt").await?, nfsstat3::NFS3_OK);
    assert_eq!(nfs.rmdir(&docs, "old").await?, nfsstat3::NFS3_OK);

    assert_eq!(
        code(&nfs.getattr(&gone).await?),
        nfsstat3::NFS3ERR_STALE,
        "GETATTR of a file"
    );
    assert_eq!(
        code(&nfs.read_status(&inner, 0, 5).await?),
        nfsstat3::NFS3ERR_STALE,
        "READ"
    );
    assert_eq!(
        nfs.write(&gone, 0, b"XY").await?,
        nfsstat3::NFS3ERR_STALE,
        "WRITE"
    );
    assert_eq!(
        code(&nfs.names(&old).await?),
        nfsstat3::NFS3ERR_STALE,
        "READDIR of a folder"
    );
    assert_eq!(
        nfs.create(&old, "fresh.txt").await?,
        nfsstat3::NFS3ERR_STALE,
        "CREATE"
    );
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        fixture.names_on_backend("docs").await?,
        Vec::<String>::new()
    );
    assert_eq!(fixture.uploaded(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handles_the_server_never_issued_never_resolve_to_the_export_root() -> Result<()> {
    let fixture = two_reports().await?;
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let root = nfs.root();
    let mut foreign_generation = root.data.to_vec();
    if let Some(first) = foreign_generation.first_mut() {
        *first ^= 0xFF;
    }
    let mut short = root
        .data
        .get(..GENERATION_LEN)
        .context("root handle")?
        .to_vec();
    short.extend_from_slice(&[1, 2, 3]);

    for unknown in [
        handle(Vec::new()),
        handle(vec![1, 2, 3]),
        handle(short),
        handle(foreign_generation),
        handle(vec![0x5A; 64]),
    ] {
        for (procedure, status) in statuses_under(&mut nfs, &unknown, &docs).await? {
            assert_ne!(
                status,
                nfsstat3::NFS3_OK,
                "{procedure} under {:?}",
                unknown.data
            );
        }
    }

    assert_nothing_applied(&fixture).await
}

#[tokio::test(flavor = "multi_thread")]
async fn rmdir_of_a_non_empty_folder_answers_notempty_and_keeps_it() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/populated/child.txt", b"alive");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(
        nfs.rmdir(&docs, "populated").await?,
        nfsstat3::NFS3ERR_NOTEMPTY
    );

    assert_eq!(fixture.log.calls(MockOp::Delete), 0);
    assert_eq!(
        fixture.on_backend("docs/populated/child.txt").as_deref(),
        Some(b"alive".as_slice())
    );
    let populated = nfs.walk("docs/populated").await?;
    assert_eq!(
        nfs.names(&populated).await?,
        Ok(vec!["child.txt".to_owned()])
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn create_guarded_or_exclusive_of_an_existing_name_answers_exist_and_keeps_the_content()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/lock.txt", b"held");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    let guarded = nfs
        .create_how(&docs, "lock.txt", createhow3::GUARDED(sattr3::default()))
        .await?;
    let exclusive = nfs
        .create_how(
            &docs,
            "lock.txt",
            createhow3::EXCLUSIVE(createverf3([7; 8])),
        )
        .await?;

    assert_eq!(code(&guarded), nfsstat3::NFS3ERR_EXIST, "GUARDED");
    assert_eq!(code(&exclusive), nfsstat3::NFS3ERR_EXIST, "EXCLUSIVE");
    let lock = nfs.walk("docs/lock.txt").await?;
    assert_eq!(nfs.read(&lock, 0, 4).await?, b"held");
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.uploaded(), 0);
    assert_eq!(
        fixture.on_backend("docs/lock.txt").as_deref(),
        Some(b"held".as_slice())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_or_rmdir_of_a_non_empty_folder_answers_notempty_and_of_an_empty_folder_or_a_file_removes_exactly_that_entry()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/populated/child.txt", b"alive");
    fixture.mock.add_folder("docs/empty-by-remove")?;
    fixture.mock.add_folder("docs/empty-by-rmdir")?;
    fixture.mock.add_file("docs/file-by-remove.txt", b"one");
    fixture.mock.add_file("docs/file-by-rmdir.txt", b"two");
    fixture.mock.add_file("docs/kept.txt", b"kept");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(
        nfs.remove(&docs, "populated").await?,
        nfsstat3::NFS3ERR_NOTEMPTY,
        "REMOVE of a non-empty folder"
    );
    assert_eq!(
        nfs.rmdir(&docs, "populated").await?,
        nfsstat3::NFS3ERR_NOTEMPTY,
        "RMDIR of a non-empty folder"
    );
    assert_eq!(fixture.log.calls(MockOp::Delete), 0);
    assert_eq!(
        fixture.on_backend("docs/populated/child.txt").as_deref(),
        Some(b"alive".as_slice())
    );

    let mut remaining = vec![
        "empty-by-remove",
        "empty-by-rmdir",
        "file-by-remove.txt",
        "file-by-rmdir.txt",
        "kept.txt",
        "populated",
    ];
    for (procedure, entry) in [
        ("REMOVE", "empty-by-remove"),
        ("RMDIR", "empty-by-rmdir"),
        ("REMOVE", "file-by-remove.txt"),
        ("RMDIR", "file-by-rmdir.txt"),
    ] {
        let status = if procedure == "REMOVE" {
            nfs.remove(&docs, entry).await?
        } else {
            nfs.rmdir(&docs, entry).await?
        };
        assert_eq!(status, nfsstat3::NFS3_OK, "{procedure} {entry}");
        remaining.retain(|name| *name != entry);
        assert_eq!(
            fixture.names_on_backend("docs").await?,
            remaining,
            "backend after {procedure} {entry}"
        );
        assert_eq!(
            nfs.names(&docs).await?,
            Ok(remaining.iter().map(|name| (*name).to_owned()).collect()),
            "READDIR after {procedure} {entry}"
        );
    }
    assert_eq!(
        fixture.on_backend("docs/kept.txt").as_deref(),
        Some(b"kept".as_slice())
    );
    Ok(())
}
