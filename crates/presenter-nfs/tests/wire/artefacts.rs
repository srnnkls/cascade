//! Names the macOS NFS client creates on its own: `AppleDouble` `._*`
//! companions and the `.nfs.*` name of a file deleted while open.

use anyhow::{Context as _, Result, ensure};
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::testing::MockOp;
use nfs3_client::nfs3_types::nfs3::{createhow3, nfs_fh3, nfsstat3, sattr3};

use crate::support::client::{Client, code};
use crate::support::fixture::{Fixture, QUIET, tree_of};

/// The name the macOS client renames an open file to when it is removed.
const SILLY: &str = ".nfs.0000a1b2.0004";

#[tokio::test(flavor = "multi_thread")]
async fn appledouble_names_never_reach_the_backend_or_the_replica() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    let _ = nfs.create(&docs, "._report.txt").await?;
    if let Ok(companion) = nfs.lookup(&docs, "._report.txt").await? {
        let _ = nfs
            .write(&companion, 0, b"\x00\x05\x16\x07 resource fork")
            .await?;
    }
    let _ = nfs.create(&nfs.root(), "._docs").await?;

    tokio::time::sleep(QUIET).await;
    let on_backend = tree_of(&fixture.mock, "").await?;
    let leaked: Vec<&String> = on_backend
        .iter()
        .filter(|path| {
            path.rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with("._"))
        })
        .collect();
    ensure!(
        leaked.is_empty(),
        "AppleDouble names on the backend: {leaked:?}"
    );
    for path in ["docs/._report.txt", "._docs"] {
        ensure!(
            fixture.daemon.resolve(path).await?.is_none(),
            "{path} entered the replica"
        );
    }
    assert_eq!(
        fixture.on_backend("docs/report.txt").as_deref(),
        Some(b"original".as_slice())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_removed_while_open_reads_until_its_silly_name_is_removed_and_leaves_no_nfs_name()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/held.txt", b"still readable");
    fixture.mock.add_file("docs/other.txt", b"other");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let held = nfs.walk("docs/held.txt").await?;

    assert_eq!(
        nfs.rename(&docs, "held.txt", &docs, SILLY).await?,
        nfsstat3::NFS3_OK
    );
    assert_eq!(
        nfs.read(&held, 0, 14).await?,
        b"still readable",
        "READ after the rename"
    );
    assert_eq!(nfs.remove(&docs, SILLY).await?, nfsstat3::NFS3_OK);

    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.names_on_backend("docs").await?, ["other.txt"]);
    assert_eq!(
        code(&nfs.lookup(&docs, SILLY).await?),
        nfsstat3::NFS3ERR_NOENT
    );
    assert_eq!(
        code(&nfs.lookup(&docs, "held.txt").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    Ok(())
}

/// Extended attributes as the macOS client writes them into a companion.
const ATTRIBUTES: &[u8] = b"\x00\x05\x16\x07 extended attributes";

/// Creates the companion `name` in `dir` holding [`ATTRIBUTES`].
async fn companion(nfs: &mut Client, dir: &nfs_fh3, name: &str) -> Result<nfs_fh3> {
    let handle = match nfs
        .create_how(dir, name, createhow3::UNCHECKED(sattr3::default()))
        .await?
    {
        Ok(handle) => handle.context("CREATE of a companion returned no handle")?,
        Err(code) => anyhow::bail!("CREATE {name}: {code:?}"),
    };
    assert_eq!(nfs.write(&handle, 0, ATTRIBUTES).await?, nfsstat3::NFS3_OK);
    Ok(handle)
}

async fn attributes_of(nfs: &mut Client, dir: &nfs_fh3, name: &str) -> Result<Vec<u8>> {
    let handle = nfs
        .lookup(dir, name)
        .await?
        .map_err(|code| anyhow::anyhow!("LOOKUP {name}: {code:?}"))?;
    nfs.read(&handle, 0, 1024).await
}

#[tokio::test(flavor = "multi_thread")]
async fn rmdir_of_a_folder_whose_only_backend_children_are_appledouble_names_removes_it_with_them()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture
        .mock
        .add_file("docs/old/._report.txt", b"written over SMB");
    fixture
        .mock
        .add_file("docs/old/._notes.txt", b"written over SMB");
    fixture.mock.add_file("docs/keep.txt", b"kept");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let old = nfs.walk("docs/old").await?;
    assert_eq!(nfs.names(&old).await?, Ok(vec![]), "the folder lists empty");

    assert_eq!(nfs.rmdir(&docs, "old").await?, nfsstat3::NFS3_OK);

    assert_eq!(tree_of(&fixture.mock, "docs").await?, ["docs/keep.txt"]);
    assert_eq!(
        code(&nfs.lookup(&docs, "old").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    assert_eq!(nfs.names(&docs).await?, Ok(vec!["keep.txt".to_owned()]));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rmdir_of_a_folder_holding_an_appledouble_name_and_an_ordinary_file_answers_notempty_and_removes_nothing()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture
        .mock
        .add_file("docs/mixed/._report.txt", b"written over SMB");
    fixture.mock.add_file("docs/mixed/report.txt", b"report");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(nfs.rmdir(&docs, "mixed").await?, nfsstat3::NFS3ERR_NOTEMPTY);

    tokio::time::sleep(QUIET).await;
    assert_eq!(
        fixture.log.calls(MockOp::Delete),
        0,
        "deletes on the backend"
    );
    assert_eq!(
        tree_of(&fixture.mock, "docs").await?,
        [
            "docs/mixed",
            "docs/mixed/._report.txt",
            "docs/mixed/report.txt"
        ]
    );
    Ok(())
}

/// RMDIR of `docs/old` holding a `._archive` folder answers NOTEMPTY and
/// leaves the backend tree `expected` untouched.
async fn assert_rmdir_keeps_appledouble_named_folder(
    fixture: &Fixture,
    expected: &[&str],
) -> Result<()> {
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(nfs.rmdir(&docs, "old").await?, nfsstat3::NFS3ERR_NOTEMPTY);

    tokio::time::sleep(QUIET).await;
    assert_eq!(
        fixture.log.calls(MockOp::Delete),
        0,
        "deletes on the backend"
    );
    assert_eq!(tree_of(&fixture.mock, "docs").await?, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rmdir_of_a_folder_holding_an_appledouble_named_folder_with_files_answers_notempty_and_removes_nothing()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture
        .mock
        .add_file("docs/old/._archive/keep.txt", b"kept");
    assert_rmdir_keeps_appledouble_named_folder(
        &fixture,
        &[
            "docs/old",
            "docs/old/._archive",
            "docs/old/._archive/keep.txt",
        ],
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn rmdir_of_a_folder_holding_an_empty_appledouble_named_folder_answers_notempty_and_removes_nothing()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_folder("docs/old/._archive")?;
    assert_rmdir_keeps_appledouble_named_folder(&fixture, &["docs/old", "docs/old/._archive"]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn rmdir_of_a_folder_holding_a_companion_and_an_appledouble_named_folder_answers_notempty_and_removes_nothing()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/old/._x", b"written over SMB");
    fixture
        .mock
        .add_file("docs/old/._archive/keep.txt", b"kept");
    assert_rmdir_keeps_appledouble_named_folder(
        &fixture,
        &[
            "docs/old",
            "docs/old/._archive",
            "docs/old/._archive/keep.txt",
            "docs/old/._x",
        ],
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn removing_a_file_drops_its_companion_and_a_new_file_of_the_name_has_none() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/a.txt", b"content");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    let held = companion(&mut nfs, &docs, "._a.txt").await?;

    assert_eq!(nfs.remove(&docs, "a.txt").await?, nfsstat3::NFS3_OK);

    assert_eq!(
        code(&nfs.lookup(&docs, "._a.txt").await?),
        nfsstat3::NFS3ERR_NOENT,
        "LOOKUP of the companion after the remove"
    );
    assert_eq!(
        code(&nfs.read_status(&held, 0, 1024).await?),
        nfsstat3::NFS3ERR_STALE,
        "READ through the companion's handle after the remove"
    );
    assert_eq!(nfs.create(&docs, "a.txt").await?, nfsstat3::NFS3_OK);
    assert_eq!(
        code(&nfs.lookup(&docs, "._a.txt").await?),
        nfsstat3::NFS3ERR_NOENT,
        "LOOKUP of the companion of the new a.txt"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn removing_a_folder_drops_its_companion_and_the_companions_in_it() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/keep.txt", b"kept");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    assert_eq!(nfs.mkdir(&docs, "sub").await?, nfsstat3::NFS3_OK);
    let sub = nfs.walk("docs/sub").await?;
    let inner = companion(&mut nfs, &sub, "._a.txt").await?;
    let own = companion(&mut nfs, &docs, "._sub").await?;

    assert_eq!(nfs.rmdir(&docs, "sub").await?, nfsstat3::NFS3_OK);

    assert_eq!(
        code(&nfs.lookup(&docs, "._sub").await?),
        nfsstat3::NFS3ERR_NOENT,
        "LOOKUP of the folder's companion after the remove"
    );
    for (what, handle) in [("in the folder", &inner), ("of the folder", &own)] {
        assert_eq!(
            code(&nfs.read_status(handle, 0, 1024).await?),
            nfsstat3::NFS3ERR_STALE,
            "READ through the handle of the companion {what}"
        );
    }
    Ok(())
}

/// The macOS client renames `._a.txt` to `._b.txt` itself right after it
/// renamed `a.txt` to `b.txt`.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_rename_and_the_clients_companion_rename_the_attributes_are_under_the_new_name_only()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/a.txt", b"content");
    let mut nfs = fixture.client().await?;
    let docs = nfs.walk("docs").await?;
    companion(&mut nfs, &docs, "._a.txt").await?;

    assert_eq!(
        nfs.rename(&docs, "a.txt", &docs, "b.txt").await?,
        nfsstat3::NFS3_OK
    );
    assert_eq!(
        nfs.rename(&docs, "._a.txt", &docs, "._b.txt").await?,
        nfsstat3::NFS3_OK,
        "the client's companion rename"
    );

    assert_eq!(attributes_of(&mut nfs, &docs, "._b.txt").await?, ATTRIBUTES);
    assert_eq!(
        code(&nfs.lookup(&docs, "._a.txt").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    assert_eq!(nfs.create(&docs, "a.txt").await?, nfsstat3::NFS3_OK);
    assert_eq!(
        code(&nfs.lookup(&docs, "._a.txt").await?),
        nfsstat3::NFS3ERR_NOENT,
        "LOOKUP of the companion of the new a.txt"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn renaming_a_folder_keeps_the_companions_of_its_children_reachable() -> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/sub/a.txt", b"content");
    let mut nfs = fixture.client().await?;
    let sub = nfs.walk("docs/sub").await?;
    companion(&mut nfs, &sub, "._a.txt").await?;
    let docs = nfs.walk("docs").await?;

    assert_eq!(
        nfs.rename(&docs, "sub", &docs, "moved").await?,
        nfsstat3::NFS3_OK
    );

    let moved = nfs.walk("docs/moved").await?;
    assert_eq!(
        attributes_of(&mut nfs, &moved, "._a.txt").await?,
        ATTRIBUTES
    );
    Ok(())
}
