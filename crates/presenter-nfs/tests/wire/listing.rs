//! READDIR and READDIRPLUS paging through a folder of 3000 entries.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail, ensure};
use cascade_engine::content::transfer::TransferConfig;
use nfs3_client::nfs3_types::nfs3::{cookieverf3, nfs_fh3, nfsstat3};

use crate::support::client::{Client, Page};
use crate::support::fixture::Fixture;

const ENTRIES: usize = 3000;
/// Pages small enough that the folder takes dozens of them.
const DIRCOUNT: u32 = 4096;
const MAXCOUNT: u32 = 8192;
/// Pages between two changes of the folder.
const CHANGE_EVERY: usize = 3;

#[derive(Debug, Clone, Copy)]
enum Procedure {
    Readdir,
    Readdirplus,
}

fn entry_name(n: usize) -> String {
    format!("entry-{n:04}.txt")
}

async fn big_folder() -> Result<(Fixture, Client, nfs_fh3)> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    for n in 0..ENTRIES {
        fixture
            .mock
            .add_file(&format!("big/{}", entry_name(n)), b"x");
    }
    let mut nfs = fixture.client().await?;
    let big = nfs.walk("big").await?;
    Ok((fixture, nfs, big))
}

async fn page(
    nfs: &mut Client,
    procedure: Procedure,
    dir: &nfs_fh3,
    cookie: u64,
    verf: cookieverf3,
) -> Result<Page> {
    let outcome = match procedure {
        Procedure::Readdir => nfs.readdir_page(dir, cookie, verf, DIRCOUNT).await?,
        Procedure::Readdirplus => {
            nfs.readdirplus_page(dir, cookie, verf, DIRCOUNT, MAXCOUNT)
                .await?
        }
    };
    match outcome {
        Ok(page) => Ok(page),
        Err(code) => bail!("{procedure:?} at cookie {cookie}: {code:?}"),
    }
}

/// Changes the folder between pages: removes one name already listed and
/// one not yet listed, and creates a new one.
#[derive(Debug, Default)]
struct Churn {
    removed: BTreeSet<String>,
    created: BTreeSet<String>,
}

impl Churn {
    async fn change(
        &mut self,
        nfs: &mut Client,
        dir: &nfs_fh3,
        seen: &BTreeMap<String, usize>,
    ) -> Result<()> {
        let listed = seen.keys().find(|name| self.removable(name)).cloned();
        let unlisted = (0..ENTRIES)
            .rev()
            .map(entry_name)
            .find(|name| !seen.contains_key(name) && self.removable(name));
        for name in listed.into_iter().chain(unlisted) {
            ensure!(
                nfs.remove(dir, &name).await? == nfsstat3::NFS3_OK,
                "REMOVE {name}"
            );
            self.removed.insert(name);
        }
        let fresh = format!("fresh-{:03}.txt", self.created.len());
        ensure!(
            nfs.create(dir, &fresh).await? == nfsstat3::NFS3_OK,
            "CREATE {fresh}"
        );
        self.created.insert(fresh);
        Ok(())
    }

    fn removable(&self, name: &str) -> bool {
        name.starts_with("entry-") && !self.removed.contains(name)
    }
}

/// Pages through `dir` from the first cookie, changing the folder every
/// [`CHANGE_EVERY`] pages when `churn` is given; returns how often each name
/// was listed and the number of pages.
async fn page_through(
    nfs: &mut Client,
    procedure: Procedure,
    dir: &nfs_fh3,
    mut churn: Option<&mut Churn>,
) -> Result<(BTreeMap<String, usize>, usize)> {
    let mut seen = BTreeMap::new();
    let mut cookie = 0;
    let mut verf = cookieverf3::default();
    let mut pages = 0;
    loop {
        let page = page(nfs, procedure, dir, cookie, verf).await?;
        pages += 1;
        ensure!(
            page.eof || !page.entries.is_empty(),
            "{procedure:?} page {pages} is empty and not the last"
        );
        for entry in &page.entries {
            if entry.name != "." && entry.name != ".." {
                *seen.entry(entry.name.clone()).or_default() += 1;
            }
        }
        if let Some(last) = page.entries.last() {
            cookie = last.cookie;
        }
        verf = page.verf;
        if page.eof {
            return Ok((seen, pages));
        }
        ensure!(pages <= ENTRIES, "{procedure:?} never reached eof");
        if let Some(churn) = churn.as_deref_mut()
            && pages % CHANGE_EVERY == 0
        {
            churn.change(nfs, dir, &seen).await?;
        }
    }
}

fn repeated(seen: &BTreeMap<String, usize>) -> Vec<(&String, &usize)> {
    seen.iter().filter(|(_, count)| **count > 1).collect()
}

async fn pages_completely_without_repetition(procedure: Procedure) -> Result<()> {
    let (_fixture, mut nfs, big) = big_folder().await?;

    let (seen, pages) = page_through(&mut nfs, procedure, &big, None).await?;

    ensure!(pages > CHANGE_EVERY, "the folder fits in {pages} pages");
    ensure!(
        repeated(&seen).is_empty(),
        "listed twice: {:?}",
        repeated(&seen)
    );
    let expected: BTreeSet<String> = (0..ENTRIES).map(entry_name).collect();
    let listed: BTreeSet<String> = seen.into_keys().collect();
    let missing: Vec<_> = expected.difference(&listed).collect();
    let extra: Vec<_> = listed.difference(&expected).collect();
    ensure!(
        missing.is_empty() && extra.is_empty(),
        "{} missing, first {:?}; extra {extra:?}",
        missing.len(),
        missing.first()
    );
    Ok(())
}

async fn cookies_stay_valid_while_the_folder_changes(procedure: Procedure) -> Result<()> {
    let (_fixture, mut nfs, big) = big_folder().await?;
    let mut churn = Churn::default();

    let (seen, pages) = page_through(&mut nfs, procedure, &big, Some(&mut churn)).await?;

    ensure!(pages > CHANGE_EVERY, "the folder fits in {pages} pages");
    ensure!(!churn.removed.is_empty(), "the folder never changed");
    ensure!(
        repeated(&seen).is_empty(),
        "listed twice: {:?}",
        repeated(&seen)
    );
    let throughout: Vec<String> = (0..ENTRIES)
        .map(entry_name)
        .filter(|name| !churn.removed.contains(name))
        .collect();
    let skipped: Vec<&String> = throughout
        .iter()
        .filter(|name| !seen.contains_key(*name))
        .collect();
    ensure!(
        skipped.is_empty(),
        "{} entries present throughout were skipped, first {:?}",
        skipped.len(),
        skipped.first()
    );
    let unknown: Vec<&String> = seen
        .keys()
        .filter(|name| !name.starts_with("entry-") && !churn.created.contains(*name))
        .collect();
    ensure!(
        unknown.is_empty(),
        "listed names never in the folder: {unknown:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn readdirplus_pages_through_3000_entries_completely_without_repetition() -> Result<()> {
    pages_completely_without_repetition(Procedure::Readdirplus).await
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_pages_through_3000_entries_completely_without_repetition() -> Result<()> {
    pages_completely_without_repetition(Procedure::Readdir).await
}

#[tokio::test(flavor = "multi_thread")]
async fn readdirplus_cookies_stay_valid_while_entries_are_added_and_removed_between_pages()
-> Result<()> {
    cookies_stay_valid_while_the_folder_changes(Procedure::Readdirplus).await
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_cookies_stay_valid_while_entries_are_added_and_removed_between_pages() -> Result<()>
{
    cookies_stay_valid_while_the_folder_changes(Procedure::Readdir).await
}
