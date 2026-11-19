//! `.cascade` ignore rules over `NFSv3`: an ignored item is neither listed
//! nor looked up, and nothing is created, renamed or deleted at an ignored
//! path on the backend.

use anyhow::{Result, ensure};
use cascade_engine::content::transfer::TransferConfig;
use cascade_presenter_nfs::nfs::server::NfsCacheMode;
use nfs3_client::nfs3_types::nfs3::nfsstat3;

use crate::support::client::code;
use cascade_engine::testing::MockOp;

use crate::support::fixture::{Fixture, Layout, NAS, names_in, tree_of};

async fn named_share(files: &[&str]) -> Result<Fixture> {
    let fixture = Fixture::build(
        TransferConfig::default(),
        Layout::Named { cached: true },
        NfsCacheMode::default(),
    )
    .await?;
    for path in files {
        fixture.mock.add_file(path, path.as_bytes());
    }
    Ok(fixture)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ignored_item_is_neither_listed_nor_looked_up() -> Result<()> {
    let fixture = named_share(&[
        "keep.txt",
        "Mutagen/x.bin",
        "logs/debug.log",
        "logs/notes.txt",
    ])
    .await?;
    for path in ["Mutagen/x.bin", "logs/debug.log", "logs/notes.txt"] {
        fixture.uid(path).await?;
    }

    fixture.ignore("Mutagen/\n/nas/logs/*.log\n").await?;

    let mut nfs = fixture.client().await?;
    let nas = nfs.walk(NAS).await?;
    assert_eq!(
        nfs.names(&nas).await?,
        Ok(vec!["keep.txt".to_owned(), "logs".to_owned()]),
        "READDIR"
    );
    let plus: Vec<String> = match nfs.listing(&nas).await? {
        Ok(entries) => entries.into_iter().map(|entry| entry.name).collect(),
        Err(status) => anyhow::bail!("READDIRPLUS {NAS}: {status:?}"),
    };
    ensure!(
        !plus.iter().any(|name| name == "Mutagen"),
        "READDIRPLUS listed the ignored folder: {plus:?}"
    );
    assert_eq!(
        code(&nfs.lookup(&nas, "Mutagen").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    let logs = nfs.walk("nas/logs").await?;
    assert_eq!(nfs.names(&logs).await?, Ok(vec!["notes.txt".to_owned()]));
    assert_eq!(
        code(&nfs.lookup(&logs, "debug.log").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_is_created_renamed_or_deleted_at_an_ignored_path() -> Result<()> {
    let fixture = named_share(&[
        "report.txt",
        "sub/inner.txt",
        "only/cache.tmp",
        "mixed/keep.txt",
        "mixed/cache.tmp",
    ])
    .await?;

    fixture.ignore("*.tmp\n/nas/build\n").await?;

    let mut nfs = fixture.client().await?;
    let nas = nfs.walk(NAS).await?;
    let sub = nfs.walk("nas/sub").await?;
    assert_eq!(
        nfs.rename(&nas, "report.txt", &nas, "report.tmp").await?,
        nfsstat3::NFS3ERR_ACCES,
        "RENAME onto an ignored name"
    );
    assert_eq!(
        nfs.rename(&nas, "report.txt", &sub, "moved.tmp").await?,
        nfsstat3::NFS3ERR_ACCES,
        "RENAME into a folder under an ignored name"
    );
    assert_eq!(
        nfs.rename(&nas, "sub", &nas, "build").await?,
        nfsstat3::NFS3ERR_ACCES,
        "RENAME onto an anchored ignored path"
    );
    ensure!(
        fixture.on_backend("report.txt").is_some(),
        "report.txt moved"
    );
    assert_eq!(
        nfs.create(&nas, "scratch.tmp").await?,
        nfsstat3::NFS3ERR_ACCES,
        "CREATE"
    );
    assert_eq!(
        nfs.mkdir(&nas, "build").await?,
        nfsstat3::NFS3ERR_ACCES,
        "MKDIR"
    );
    assert_eq!(
        nfs.symlink(&nas, "link.tmp", "report.txt").await?,
        nfsstat3::NFS3ERR_ACCES,
        "SYMLINK"
    );
    assert_eq!(
        fixture.names_on_backend("").await?,
        ["mixed", "only", "report.txt", "sub"],
        "an ignored name reached the backend root"
    );
    assert_eq!(
        names_in(&fixture.mock, "sub").await?,
        ["inner.txt"],
        "a move under an ignored name reached the backend"
    );
    assert_eq!(nfs.mkdir(&sub, "build").await?, nfsstat3::NFS3_OK);
    assert_eq!(
        names_in(&fixture.mock, "sub").await?,
        ["build", "inner.txt"],
        "an anchored rule refused the name below the mount"
    );

    let only = nfs.walk("nas/only").await?;
    assert_eq!(nfs.names(&only).await?, Ok(Vec::new()));
    assert_eq!(nfs.rmdir(&nas, "only").await?, nfsstat3::NFS3ERR_NOTEMPTY);
    ensure!(
        fixture.on_backend("only/cache.tmp").is_some(),
        "RMDIR deleted an ignored entry"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_folder_rename_carrying_or_landing_an_ignored_entry_is_refused() -> Result<()> {
    let fixture = named_share(&[
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
    ])
    .await?;

    fixture
        .ignore("*.tmp\nscratch\n/nas/proj/mirror/\nnas/b/**/*.log\n/nas/a/build/\n")
        .await?;

    let mut nfs = fixture.client().await?;
    let nas = nfs.walk(NAS).await?;
    let b = nfs.walk("nas/b").await?;
    let mut failures = Vec::new();
    for (case, folder, to, name) in [
        ("(a) only/cache.tmp under *.tmp", "only", &nas, "only2"),
        ("(a) deep/scratch under scratch", "deep", &nas, "deep2"),
        (
            "(b) proj/mirror under /nas/proj/mirror/",
            "proj",
            &nas,
            "proj2",
        ),
        ("(c) x.log lands under nas/b/**/*.log", "mover", &b, "mover"),
        ("(c) build/ lands on /nas/a/build/", "staging", &nas, "a"),
    ] {
        let tree = tree_of(&fixture.mock, "").await?;
        let moves = fixture.log.calls(MockOp::Move);
        let status = nfs.rename(&nas, folder, to, name).await?;
        if status != nfsstat3::NFS3ERR_ACCES {
            failures.push(format!("{case}: RENAME answered {status:?}"));
        }
        if fixture.log.calls(MockOp::Move) != moves {
            failures.push(format!("{case}: the backend took a Move"));
        }
        let after = tree_of(&fixture.mock, "").await?;
        if after != tree {
            failures.push(format!("{case}: the backend changed to {after:?}"));
        }
    }

    let moves = fixture.log.calls(MockOp::Move);
    let status = nfs.rename(&nas, "plain", &nas, "plain2").await?;
    if status != nfsstat3::NFS3_OK {
        failures.push(format!("(d) guard: RENAME answered {status:?}"));
    }
    let tree = tree_of(&fixture.mock, "").await?;
    if fixture.log.calls(MockOp::Move) != moves + 1
        || !tree.contains(&"plain2/sub/b.txt".to_owned())
        || tree.iter().any(|path| path.starts_with("plain/"))
    {
        failures.push(format!("(d) guard: the backend holds {tree:?}"));
    }

    ensure!(failures.is_empty(), "{failures:#?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rule_applies_to_a_name_in_another_case() -> Result<()> {
    let fixture = named_share(&["keep.txt", "mutagen/x.bin"]).await?;

    fixture.ignore("Mutagen/\n*.tmp\n").await?;

    let mut nfs = fixture.client().await?;
    let nas = nfs.walk(NAS).await?;
    assert_eq!(
        nfs.names(&nas).await?,
        Ok(vec!["keep.txt".to_owned()]),
        "READDIR listed a folder ignored in another case"
    );
    assert_eq!(
        code(&nfs.lookup(&nas, "mutagen").await?),
        nfsstat3::NFS3ERR_NOENT
    );
    assert_eq!(
        nfs.create(&nas, "Scratch.TMP").await?,
        nfsstat3::NFS3ERR_ACCES,
        "CREATE"
    );
    assert_eq!(
        nfs.mkdir(&nas, "MUTAGEN").await?,
        nfsstat3::NFS3ERR_ACCES,
        "MKDIR"
    );
    assert_eq!(
        fixture.names_on_backend("").await?,
        ["keep.txt", "mutagen"],
        "a name ignored in another case reached the backend root"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_config_without_ignore_rules_leaves_listings_and_writes_unchanged() -> Result<()> {
    let fixture = named_share(&["Mutagen/x.bin", "scratch.tmp"]).await?;
    let mut nfs = fixture.client().await?;
    let nas = nfs.walk(NAS).await?;
    let before = nfs.names(&nas).await?;

    fixture.ignore("# no ignore rules\n").await?;

    assert_eq!(nfs.names(&nas).await?, before);
    assert_eq!(nfs.create(&nas, "new.tmp").await?, nfsstat3::NFS3_OK);
    ensure!(
        fixture
            .names_on_backend("")
            .await?
            .contains(&"new.tmp".to_owned()),
        "CREATE did not reach the backend"
    );
    Ok(())
}
