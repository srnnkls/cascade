//! The earlier VFS path without a content cache, as `cascade start` serves
//! it over the local backend: the engine-facing rules of WRITE, CREATE,
//! SETATTR, MKDIR, REMOVE, RMDIR and RENAME observed through `NFSv3`.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use async_trait::async_trait;
use cascade_engine::backend::{Backend, BackendError};
use cascade_engine::types::{Change, Cursor, FileEntry, FileId, Quota};
use cascade_engine::vfs::VfsTree;
use cascade_presenter_nfs::NfsPresenter;
use cascade_presenter_nfs::nfs::server::{NfsCacheMode, NfsServer};
use nfs3_client::nfs3_types::nfs3::{createhow3, nfs_fh3, nfsstat3, sattr3};
use nfs3_client::nfs3_types::xdr_codec::Opaque;

use crate::support::client::{Client, code};
use crate::support::fixture::{serve, stop};

/// A presenter over one backend at the root of the VFS tree.
struct Served {
    server: Option<NfsServer>,
}

impl Served {
    async fn start(backend: Arc<dyn Backend>, mode: NfsCacheMode) -> Result<Self> {
        let presenter = NfsPresenter::with_vfs(
            "/mnt/cascade-test",
            Arc::new(RwLock::new(VfsTree::new(backend))),
        );
        let server = serve(Arc::clone(presenter.context()), mode, 0).await?;
        Ok(Self {
            server: Some(server),
        })
    }

    async fn client(&self) -> Result<Client> {
        Client::mount(self.server.as_ref().context("stopped")?.local_addr).await
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            let _ = stop(server);
        }
    }
}

/// A presenter over the local backend rooted at a fresh directory.
async fn local(mode: NfsCacheMode) -> Result<(tempfile::TempDir, Served, Client)> {
    let dir = tempfile::tempdir()?;
    let mut table = toml::map::Map::new();
    table.insert(
        "root_path".to_owned(),
        toml::Value::String(dir.path().to_string_lossy().into_owned()),
    );
    table.insert("id".to_owned(), toml::Value::String("local".to_owned()));
    let backend: Arc<dyn Backend> =
        cascade_backend_local::create_backend(&toml::Value::Table(table))?.into();
    let served = Served::start(backend, mode).await?;
    let nfs = served.client().await?;
    Ok((dir, served, nfs))
}

async fn size(nfs: &mut Client, fh: &nfs_fh3) -> Result<u64> {
    match nfs.getattr(fh).await? {
        Ok(attrs) => Ok(attrs.size),
        Err(code) => bail!("GETATTR: {code:?}"),
    }
}

/// CREATE UNCHECKED of `name` in `dir`, then its handle by LOOKUP.
async fn created(nfs: &mut Client, dir: &nfs_fh3, name: &str) -> Result<nfs_fh3> {
    ensure!(
        nfs.create(dir, name).await? == nfsstat3::NFS3_OK,
        "CREATE {name}"
    );
    match nfs.lookup(dir, name).await? {
        Ok(fh) => Ok(fh),
        Err(code) => bail!("LOOKUP {name} after CREATE: {code:?}"),
    }
}

/// The directory the local backend keeps its manifest in at its root
/// (`crates/backend-local`, `.cascade-cache/manifest.jsonl`).
const LOCAL_BACKEND_DIR: &str = ".cascade-cache";

/// The names in `dir` but the local backend's own directory.
fn user_names(dir: &Path) -> Result<Vec<String>> {
    let mut names = sorted_names(dir)?;
    names.retain(|name| name != LOCAL_BACKEND_DIR);
    Ok(names)
}

fn sorted_names(dir: &Path) -> Result<Vec<String>> {
    let mut names = std::fs::read_dir(dir)?
        .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_set_the_size_getattr_reports_and_a_write_past_the_end_extends_the_file()
-> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    let root = nfs.root();
    let note = created(&mut nfs, &root, "note.txt").await?;
    let sparse = created(&mut nfs, &root, "sparse.bin").await?;

    assert_eq!(
        nfs.write(&note, 0, b"hello world").await?,
        nfsstat3::NFS3_OK
    );
    assert_eq!(nfs.write(&sparse, 8, b"DATA").await?, nfsstat3::NFS3_OK);

    assert_eq!(size(&mut nfs, &note).await?, 11);
    assert_eq!(size(&mut nfs, &sparse).await?, 12);
    assert_eq!(std::fs::read(dir.path().join("note.txt"))?, b"hello world");
    assert_eq!(
        std::fs::read(dir.path().join("sparse.bin"))?,
        b"\0\0\0\0\0\0\0\0DATA"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn create_makes_an_empty_file_lookup_finds() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    let root = nfs.root();

    let fresh = created(&mut nfs, &root, "fresh.txt").await?;

    assert_eq!(size(&mut nfs, &fresh).await?, 0);
    assert_eq!(std::fs::read(dir.path().join("fresh.txt"))?, b"");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn setattr_size_truncates_the_file() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    std::fs::write(dir.path().join("big.txt"), b"0123456789")?;
    let big = nfs.walk("big.txt").await?;

    assert_eq!(nfs.set_size(&big, 4).await?, nfsstat3::NFS3_OK);

    assert_eq!(size(&mut nfs, &big).await?, 4);
    assert_eq!(std::fs::read(dir.path().join("big.txt"))?, b"0123");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mkdir_remove_and_rmdir_apply_on_the_backend() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    let root = nfs.root();

    assert_eq!(nfs.mkdir(&root, "sub").await?, nfsstat3::NFS3_OK);
    ensure!(dir.path().join("sub").is_dir(), "MKDIR made no folder");
    let sub = nfs.walk("sub").await?;
    let inner = created(&mut nfs, &sub, "inner.txt").await?;
    assert_eq!(nfs.write(&inner, 0, b"x").await?, nfsstat3::NFS3_OK);
    assert_eq!(nfs.remove(&sub, "inner.txt").await?, nfsstat3::NFS3_OK);
    ensure!(
        !dir.path().join("sub/inner.txt").exists(),
        "REMOVE left the file"
    );
    assert_eq!(nfs.rmdir(&root, "sub").await?, nfsstat3::NFS3_OK);

    ensure!(!dir.path().join("sub").exists(), "RMDIR left the folder");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn create_over_an_existing_name_answers_exist_when_guarded_and_keeps_the_content_when_unchecked()
-> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    std::fs::write(dir.path().join("lock.txt"), b"held")?;
    std::fs::write(dir.path().join("keep.txt"), b"0123456789")?;
    let root = nfs.root();

    let guarded = nfs
        .create_how(&root, "lock.txt", createhow3::GUARDED(sattr3::default()))
        .await?;
    let unchecked = nfs.create(&root, "keep.txt").await?;

    assert_eq!(code(&guarded), nfsstat3::NFS3ERR_EXIST, "GUARDED");
    assert_eq!(unchecked, nfsstat3::NFS3_OK, "UNCHECKED");
    assert_eq!(std::fs::read(dir.path().join("lock.txt"))?, b"held");
    assert_eq!(std::fs::read(dir.path().join("keep.txt"))?, b"0123456789");
    let keep = nfs.walk("keep.txt").await?;
    assert_eq!(size(&mut nfs, &keep).await?, 10);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rmdir_of_a_non_empty_folder_answers_notempty_and_keeps_its_contents() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    std::fs::create_dir(dir.path().join("populated"))?;
    std::fs::write(dir.path().join("populated/child.txt"), b"alive")?;
    let root = nfs.root();

    assert_eq!(
        nfs.rmdir(&root, "populated").await?,
        nfsstat3::NFS3ERR_NOTEMPTY
    );

    ensure!(dir.path().join("populated").is_dir(), "the folder is gone");
    assert_eq!(
        std::fs::read(dir.path().join("populated/child.txt"))?,
        b"alive"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rename_moves_the_file() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    std::fs::write(dir.path().join("old.txt"), b"payload")?;
    let root = nfs.root();

    assert_eq!(
        nfs.rename(&root, "old.txt", &root, "new.txt").await?,
        nfsstat3::NFS3_OK
    );

    assert_eq!(nfs.names(&root).await?, Ok(vec!["new.txt".to_owned()]));
    assert_eq!(user_names(dir.path())?, ["new.txt"]);
    assert_eq!(std::fs::read(dir.path().join("new.txt"))?, b"payload");
    let new = nfs.walk("new.txt").await?;
    assert_eq!(size(&mut nfs, &new).await?, 7);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_local_backends_own_directory_is_neither_listed_nor_found() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    std::fs::write(dir.path().join("old.txt"), b"payload")?;
    let root = nfs.root();
    assert_eq!(
        nfs.rename(&root, "old.txt", &root, "new.txt").await?,
        nfsstat3::NFS3_OK
    );
    ensure!(
        dir.path().join(LOCAL_BACKEND_DIR).is_dir(),
        "the local backend wrote no {LOCAL_BACKEND_DIR} to its root"
    );

    let listed: Vec<String> = match nfs.listing(&root).await? {
        Ok(entries) => entries.into_iter().map(|entry| entry.name).collect(),
        Err(code) => bail!("READDIRPLUS: {code:?}"),
    };
    assert_eq!(listed, ["new.txt"], "READDIRPLUS");
    assert_eq!(
        nfs.names(&root).await?,
        Ok(vec!["new.txt".to_owned()]),
        "READDIR"
    );
    assert_eq!(
        code(&nfs.lookup(&root, LOCAL_BACKEND_DIR).await?),
        nfsstat3::NFS3ERR_NOENT,
        "LOOKUP {LOCAL_BACKEND_DIR}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_export_answers_rofs_on_write_and_create() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Off).await?;
    std::fs::write(dir.path().join("blocked.txt"), b"keep")?;
    let root = nfs.root();
    let blocked = nfs.walk("blocked.txt").await?;

    assert_eq!(
        nfs.write(&blocked, 0, b"nope").await?,
        nfsstat3::NFS3ERR_ROFS,
        "WRITE"
    );
    assert_eq!(
        nfs.create(&root, "nope.txt").await?,
        nfsstat3::NFS3ERR_ROFS,
        "CREATE"
    );

    assert_eq!(sorted_names(dir.path())?, ["blocked.txt"]);
    assert_eq!(std::fs::read(dir.path().join("blocked.txt"))?, b"keep");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rmdir_of_a_missing_folder_answers_noent() -> Result<()> {
    let (_dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    let root = nfs.root();

    assert_eq!(nfs.rmdir(&root, "ghost").await?, nfsstat3::NFS3ERR_NOENT);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mutations_under_a_directory_handle_the_server_does_not_know_answer_stale_and_leave_the_root()
-> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    std::fs::write(dir.path().join("report.txt"), b"at the root")?;
    std::fs::create_dir(dir.path().join("docs"))?;
    std::fs::write(dir.path().join("docs/report.txt"), b"in docs")?;
    let root = nfs.root();
    let docs = nfs.walk("docs").await?;
    let generation = root.data.get(..8).context("root handle")?;
    let mut bytes = generation.to_vec();
    bytes.resize(root.data.len().max(16), 0xA5);
    let stale = nfs_fh3 {
        data: Opaque::owned(bytes),
    };

    for (what, status) in [
        ("CREATE", nfs.create(&stale, "fresh.txt").await?),
        ("MKDIR", nfs.mkdir(&stale, "new").await?),
        (
            "RENAME from",
            nfs.rename(&stale, "report.txt", &docs, "moved.txt").await?,
        ),
        (
            "RENAME into",
            nfs.rename(&docs, "report.txt", &stale, "report.txt")
                .await?,
        ),
    ] {
        assert_eq!(status, nfsstat3::NFS3ERR_STALE, "{what}");
    }

    assert_eq!(sorted_names(dir.path())?, ["docs", "report.txt"]);
    assert_eq!(
        std::fs::read(dir.path().join("report.txt"))?,
        b"at the root"
    );
    assert_eq!(
        std::fs::read(dir.path().join("docs/report.txt"))?,
        b"in docs"
    );
    ensure!(
        !dir.path().join("docs/moved.txt").exists(),
        "RENAME from moved"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn names_with_a_separator_or_a_dot_segment_are_refused_and_create_nothing() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    std::fs::create_dir(dir.path().join("docs"))?;
    let docs = nfs.walk("docs").await?;

    for name in ["a/b", "..", ".", ""] {
        assert_ne!(
            nfs.create(&docs, name).await?,
            nfsstat3::NFS3_OK,
            "CREATE {name:?}"
        );
        assert_ne!(
            nfs.mkdir(&docs, name).await?,
            nfsstat3::NFS3_OK,
            "MKDIR {name:?}"
        );
    }

    assert_eq!(sorted_names(dir.path())?, ["docs"]);
    assert_eq!(
        sorted_names(&dir.path().join("docs"))?,
        Vec::<String>::new()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_returns_the_requested_range_and_eof_only_on_the_final_bytes() -> Result<()> {
    let (dir, _served, mut nfs) = local(NfsCacheMode::Minimal).await?;
    std::fs::write(dir.path().join("f.bin"), b"hello world")?;
    let file = nfs.walk("f.bin").await?;

    let middle = nfs.read_status(&file, 6, 3).await?;
    let tail = nfs.read_status(&file, 6, 5).await?;

    assert_eq!(middle, Ok((b"wor".to_vec(), false)));
    assert_eq!(tail, Ok((b"world".to_vec(), true)));
    Ok(())
}

/// A backend whose `metadata` fails with a category other than `NotFound`
/// and that records whether content was written.
#[derive(Debug)]
struct ForbiddenMetadata {
    wrote: Arc<AtomicBool>,
}

#[async_trait]
impl Backend for ForbiddenMetadata {
    fn id(&self) -> &'static str {
        "flaky"
    }
    fn display_name(&self) -> &'static str {
        "Flaky"
    }
    async fn quota(&self) -> anyhow::Result<Option<Quota>> {
        Ok(None)
    }
    async fn changes(&self, _cursor: Option<&Cursor>) -> anyhow::Result<(Vec<Change>, Cursor)> {
        Ok((vec![], Cursor("flaky".to_owned())))
    }
    async fn metadata(&self, _path: &Path) -> anyhow::Result<FileEntry> {
        Err(BackendError::Forbidden("transient HEAD failure".to_owned()).into())
    }
    async fn download(&self, _file: &FileEntry) -> anyhow::Result<Vec<u8>> {
        anyhow::bail!("unused")
    }
    async fn upload(
        &self,
        _path: &Path,
        _data: &[u8],
        _parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        self.wrote.store(true, Ordering::SeqCst);
        anyhow::bail!("upload reached through a failed metadata lookup")
    }
    async fn update(&self, _file_id: &FileId, _data: &[u8]) -> anyhow::Result<FileEntry> {
        self.wrote.store(true, Ordering::SeqCst);
        anyhow::bail!("update reached through a failed metadata lookup")
    }
    async fn create_dir(&self, _path: &Path) -> anyhow::Result<FileEntry> {
        anyhow::bail!("unused")
    }
    async fn delete(&self, _file: &FileEntry) -> anyhow::Result<()> {
        anyhow::bail!("unused")
    }
    async fn move_entry(&self, _src: &Path, _dst: &Path) -> anyhow::Result<FileEntry> {
        anyhow::bail!("unused")
    }
    async fn poll_interval(&self) -> Option<Duration> {
        None
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_metadata_lookup_is_answered_as_an_error_and_never_writes_content() -> Result<()> {
    let wrote = Arc::new(AtomicBool::new(false));
    let served = Served::start(
        Arc::new(ForbiddenMetadata {
            wrote: Arc::clone(&wrote),
        }),
        NfsCacheMode::Minimal,
    )
    .await?;
    let mut nfs = served.client().await?;
    let root = nfs.root();

    let status = nfs.create(&root, "data.bin").await?;

    assert_ne!(
        status,
        nfsstat3::NFS3_OK,
        "CREATE through a failed metadata lookup"
    );
    ensure!(
        !wrote.load(Ordering::SeqCst),
        "content was written through a non-NotFound metadata error"
    );
    Ok(())
}
