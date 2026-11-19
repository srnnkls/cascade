//! The macOS kernel NFS client on the content-cache presenter, mounted as
//! the current user with the daemon's option line: file operations and the
//! commands macOS tools issue through the volume, and a presenter restart
//! under the live mount.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail, ensure};
use cascade_engine::content::transfer::TransferConfig;
use cascade_presenter_nfs::nfs::server::macos_mount_options;

use crate::support::fixture::{Fixture, QUIET, SETTLE, eventually, tree_of};
use crate::support::{MIB, other_pattern, pattern, slice};

/// Bound on one filesystem call or command on the volume.
const STEP: Duration = Duration::from_mins(1);

/// Unmounts the mount point on drop, forcibly if a plain unmount fails.
#[derive(Debug)]
struct Mounted(PathBuf);

impl Drop for Mounted {
    fn drop(&mut self) {
        if is_mounted(&self.0) {
            let _ = Command::new("/sbin/umount").arg(&self.0).status();
        }
        if is_mounted(&self.0) {
            let _ = Command::new("/usr/sbin/diskutil")
                .args(["unmount", "force"])
                .arg(&self.0)
                .status();
        }
    }
}

/// A mounted volume; the mount goes before its mount point directory.
#[derive(Debug)]
struct Volume {
    mounted: Mounted,
    _point: tempfile::TempDir,
}

impl Volume {
    fn path(&self, relative: &str) -> PathBuf {
        if relative.is_empty() {
            self.mounted.0.clone()
        } else {
            self.mounted.0.join(relative)
        }
    }
}

fn is_mounted(point: &Path) -> bool {
    let canonical = point.canonicalize().unwrap_or_else(|_| point.to_path_buf());
    let needle = format!(" on {} ", canonical.display());
    Command::new("/sbin/mount")
        .output()
        .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).contains(&needle))
}

/// Runs `step` on its own thread so a hung NFS call cannot hang the test.
fn bounded<T: Send + 'static>(
    name: &str,
    step: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(step());
    });
    rx.recv_timeout(STEP)
        .with_context(|| format!("{name}: no result within {STEP:?}"))?
        .with_context(|| name.to_owned())
}

fn wait_with_deadline(mut child: Child, what: &str) -> Result<Output> {
    let deadline = Instant::now() + STEP;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(child.wait_with_output()?);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let out = child.wait_with_output()?;
            bail!(
                "{what} still running after {STEP:?}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

async fn mount(fixture: &Fixture) -> Result<Volume> {
    ensure!(cfg!(target_os = "macos"), "macOS only");
    let point_dir = tempfile::tempdir()?;
    let point = point_dir.path().canonicalize()?;
    let options = macos_mount_options(fixture.addr()?.port());
    let volume = Volume {
        mounted: Mounted(point.clone()),
        _point: point_dir,
    };
    tokio::task::spawn_blocking(move || -> Result<()> {
        let child = Command::new("/sbin/mount_nfs")
            .args(["-o", &options, "127.0.0.1:/"])
            .arg(&point)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let out = wait_with_deadline(child, "mount_nfs")?;
        ensure!(
            out.status.success(),
            "mount_nfs -o {options}: {} {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
        Ok(())
    })
    .await??;
    ensure!(
        is_mounted(&volume.mounted.0),
        "mount_nfs succeeded but nothing is mounted"
    );
    Ok(volume)
}

fn names_in(dir: &Path) -> Result<Vec<String>> {
    let mut names = std::fs::read_dir(dir)?
        .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

fn list(volume: &Volume, relative: &str) -> Result<Vec<String>> {
    let dir = volume.path(relative);
    bounded(&format!("list {relative:?}"), move || names_in(&dir))
}

fn placed_under(fixture: &Fixture, name: &str) -> Vec<Vec<u8>> {
    fixture
        .log
        .placed()
        .into_iter()
        .filter(|placed| placed.target.file_name().is_some_and(|n| n == name))
        .map(|placed| placed.data)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs macOS /sbin/mount_nfs mounting a loopback NFSv3 server as the current user without administrator rights"]
async fn the_mounted_volume_lists_reads_writes_truncates_renames_and_deletes_through_the_cache()
-> Result<()> {
    const SIZE: u64 = 32 * MIB;
    const OFFSET: u64 = 24 * MIB;
    let fixture = Fixture::new(TransferConfig {
        read_ahead: 2 * MIB,
        request_size: MIB,
        ..TransferConfig::default()
    })
    .await?;
    let big = pattern(SIZE);
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("media/big.bin", &big);
    let volume = mount(&fixture)
        .await
        .context("mount the volume as the current user")?;

    assert_eq!(list(&volume, "")?, ["docs", "media"]);

    let path = volume.path("media/big.bin");
    let window = bounded("ranged read", move || {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(OFFSET))?;
        let mut window = vec![0; 1024 * 1024];
        file.read_exact(&mut window)?;
        Ok(window)
    })?;
    ensure!(
        window == slice(&big, OFFSET..OFFSET + MIB)?,
        "the ranged read returned other bytes"
    );
    let fetched: u64 = fixture
        .log
        .fetched()
        .iter()
        .map(|r| r.end.min(SIZE) - r.start)
        .sum();
    ensure!(
        fetched < SIZE / 2,
        "a 1 MiB read fetched {fetched} bytes of a {SIZE}-byte file"
    );

    let eight = other_pattern(8 * MIB);
    let (path, data) = (volume.path("docs/eight.bin"), eight.clone());
    let read_back = bounded("write 8 MiB", move || {
        std::fs::write(&path, &data)?;
        Ok(std::fs::read(&path)?)
    })?;
    ensure!(read_back == eight, "reading the 8 MiB file back");
    eventually("the 8 MiB file is uploaded", || {
        !placed_under(&fixture, "eight.bin").is_empty()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    let uploads = placed_under(&fixture, "eight.bin");
    assert_eq!(uploads.len(), 1, "uploads of the 8 MiB file");
    ensure!(
        uploads.first() == Some(&eight),
        "uploaded content of the 8 MiB file"
    );
    ensure!(fixture.on_backend("docs/eight.bin") == Some(eight));

    let path = volume.path("docs/report.txt");
    bounded("truncate", move || {
        Ok(OpenOptions::new().write(true).open(path)?.set_len(4)?)
    })?;
    eventually("the truncation reaches the backend", || {
        fixture.on_backend("docs/report.txt").as_deref() == Some(b"orig".as_slice())
    })
    .await?;

    let (from, to) = (
        volume.path("docs/report.txt"),
        volume.path("docs/final.txt"),
    );
    bounded("rename", move || Ok(std::fs::rename(from, to)?))?;
    assert_eq!(
        fixture.on_backend("docs/final.txt").as_deref(),
        Some(b"orig".as_slice())
    );
    assert_eq!(fixture.on_backend("docs/report.txt"), None);

    let path = volume.path("docs/new");
    bounded("mkdir", move || Ok(std::fs::create_dir(path)?))?;
    ensure!(
        fixture
            .names_on_backend("docs")
            .await?
            .contains(&"new".to_owned())
    );
    let path = volume.path("docs/new");
    bounded("rmdir", move || Ok(std::fs::remove_dir(path)?))?;
    let path = volume.path("docs/final.txt");
    bounded("delete", move || Ok(std::fs::remove_file(path)?))?;
    assert_eq!(fixture.names_on_backend("docs").await?, ["eight.bin"]);
    Ok(())
}

fn shell(cwd: &Path, script: &str, src: &Path) -> Result<Output> {
    let child = Command::new("/bin/sh")
        .args(["-c", script])
        .env("SRC", src)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    wait_with_deadline(child, script)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs macOS /sbin/mount_nfs mounting a loopback NFSv3 server as the current user without administrator rights"]
async fn macos_tools_create_copy_and_overwrite_without_errors_and_no_appledouble_name_leaves_the_mac()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/existing.txt", b"existing");
    let scratch = tempfile::tempdir()?;
    let src = scratch.path().join("src.txt");
    std::fs::write(&src, b"source content\n")?;
    let tagged = Command::new("/usr/bin/xattr")
        .args(["-w", "com.cascade.test", "tagged"])
        .arg(&src)
        .status()?;
    ensure!(tagged.success(), "xattr -w on the source");
    let volume = mount(&fixture)
        .await
        .context("mount the volume as the current user")?;
    let docs = volume.path("docs");

    for script in [
        r#"cp "$SRC" copied.txt"#,
        r#"cp -p "$SRC" kept.txt"#,
        "echo redirected > redirected.txt",
        "touch touched.txt",
        "touch existing.txt",
        "mv copied.txt moved.txt",
        r#"cp "$SRC" kept.txt"#,
        r#"cp -p "$SRC" existing.txt"#,
        "echo again > redirected.txt",
    ] {
        let (cwd, src) = (docs.clone(), src.clone());
        let out = bounded(script, move || shell(&cwd, script, &src))?;
        ensure!(
            out.status.success() && out.stderr.is_empty(),
            "{script}: {} {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }

    eventually("the files reach the backend", || {
        fixture.on_backend("docs/redirected.txt").as_deref() == Some(b"again\n".as_slice())
            && fixture.on_backend("docs/moved.txt").is_some()
    })
    .await?;
    tokio::time::sleep(QUIET).await;
    for (name, content) in [
        ("moved.txt", b"source content\n".as_slice()),
        ("kept.txt", b"source content\n"),
        ("existing.txt", b"source content\n"),
        ("redirected.txt", b"again\n"),
        ("touched.txt", b""),
    ] {
        assert_eq!(
            fixture.on_backend(&format!("docs/{name}")).as_deref(),
            Some(content),
            "{name}"
        );
    }
    let leaked: Vec<String> = tree_of(&fixture.mock, "")
        .await?
        .into_iter()
        .filter(|path| path.rsplit('/').next().is_some_and(|n| n.starts_with("._")))
        .collect();
    ensure!(
        leaked.is_empty(),
        "AppleDouble names on the backend: {leaked:?}"
    );
    let docs_uid = fixture.uid("docs").await?;
    let in_replica: Vec<String> = fixture
        .daemon
        .replica()?
        .list(docs_uid)
        .await?
        .into_iter()
        .map(|item| item.name)
        .filter(|name| name.starts_with("._"))
        .collect();
    ensure!(
        in_replica.is_empty(),
        "AppleDouble names in the replica: {in_replica:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs macOS /sbin/mount_nfs mounting a loopback NFSv3 server as the current user without administrator rights"]
async fn a_file_deleted_while_open_reads_until_closed_and_leaves_no_nfs_name_on_the_backend()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/held.txt", b"still readable");
    fixture.mock.add_file("docs/other.txt", b"other");
    let volume = mount(&fixture)
        .await
        .context("mount the volume as the current user")?;

    let path = volume.path("docs/held.txt");
    let read = bounded("read an open file after its deletion", move || {
        let mut held = File::open(&path)?;
        std::fs::remove_file(&path)?;
        let mut content = String::new();
        held.read_to_string(&mut content)?;
        drop(held);
        Ok(content)
    })?;

    assert_eq!(read, "still readable");
    let mut names = Vec::new();
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        names = fixture.names_on_backend("docs").await.unwrap_or(names);
        if names == ["other.txt"] || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(names, ["other.txt"], "names on the backend after the close");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs macOS /sbin/mount_nfs mounting a loopback NFSv3 server as the current user without administrator rights"]
async fn a_folder_of_3000_entries_lists_completely_through_the_volume() -> Result<()> {
    const ENTRIES: usize = 3000;
    let fixture = Fixture::new(TransferConfig::default()).await?;
    let expected: BTreeSet<String> = (0..ENTRIES).map(|n| format!("entry-{n:04}.txt")).collect();
    for name in &expected {
        fixture.mock.add_file(&format!("big/{name}"), b"x");
    }
    let volume = mount(&fixture)
        .await
        .context("mount the volume as the current user")?;

    let listed = list(&volume, "big")?;

    assert_eq!(listed.len(), ENTRIES, "entries listed");
    ensure!(
        listed.into_iter().collect::<BTreeSet<_>>() == expected,
        "the listing differs from the folder"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs macOS /sbin/mount_nfs mounting a loopback NFSv3 server as the current user without administrator rights"]
async fn a_presenter_restart_under_the_live_mount_keeps_listing_reading_writing_and_a_shell_working_directory()
-> Result<()> {
    let mut fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/report.txt", b"original");
    let volume = mount(&fixture)
        .await
        .context("mount the volume as the current user")?;
    let docs = volume.path("docs");
    let dir = {
        let docs = docs.clone();
        bounded("open the folder", move || Ok(File::open(docs)?))?
    };
    let mut waiting = {
        let docs = docs.clone();
        bounded("start a shell inside the volume", move || {
            Ok(Command::new("/bin/sh")
                .args(["-c", "read go; ls; cat report.txt"])
                .current_dir(docs)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?)
        })?
    };

    fixture.restart().await?;

    let mut stdin = waiting.stdin.take().context("shell stdin")?;
    let out = bounded("the shell after the restart", move || {
        stdin.write_all(b"go\n")?;
        drop(stdin);
        wait_with_deadline(waiting, "the shell")
    })?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    ensure!(
        out.status.success() && out.stderr.is_empty(),
        "shell in the volume after the restart: {} {}",
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    );
    ensure!(
        stdout.contains("report.txt") && stdout.contains("original"),
        "shell output {stdout:?}"
    );
    let is_dir = bounded("fstat the folder fd", move || Ok(dir.metadata()?.is_dir()))?;
    ensure!(is_dir, "the folder fd no longer names a folder");
    assert_eq!(list(&volume, "docs")?, ["report.txt"]);
    let path = volume.path("docs/report.txt");
    assert_eq!(
        bounded("read after the restart", move || Ok(std::fs::read(path)?))?,
        b"original"
    );
    let path = volume.path("docs/after.txt");
    bounded("write after the restart", move || {
        Ok(std::fs::write(path, b"after the restart")?)
    })?;
    eventually("the write after the restart reaches the backend", || {
        fixture.on_backend("docs/after.txt").as_deref() == Some(b"after the restart".as_slice())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs macOS /sbin/mount_nfs mounting a loopback NFSv3 server as the current user without administrator rights"]
async fn an_extended_attribute_follows_its_file_through_mv_and_a_new_file_of_the_old_name_has_none()
-> Result<()> {
    let fixture = Fixture::new(TransferConfig::default()).await?;
    fixture.mock.add_file("docs/a.txt", b"content");
    let volume = mount(&fixture)
        .await
        .context("mount the volume as the current user")?;
    let docs = volume.path("docs");
    let scratch = tempfile::tempdir()?;
    let run = |script: &'static str| {
        let (cwd, src) = (docs.clone(), scratch.path().to_path_buf());
        bounded(script, move || {
            let out = shell(&cwd, script, &src)?;
            ensure!(
                out.status.success() && out.stderr.is_empty(),
                "{script}: {} {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            );
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        })
    };

    run("xattr -w com.cascade.test tagged a.txt")?;
    run("mv a.txt b.txt")?;
    let moved = run("xattr -l b.txt")?;
    run("touch a.txt")?;
    let reused = run("xattr -l a.txt")?;

    ensure!(
        moved.lines().any(|line| line == "com.cascade.test: tagged"),
        "xattr -l b.txt: {moved:?}"
    );
    ensure!(
        !reused.contains("com.cascade.test"),
        "xattr -l of the new a.txt: {reused:?}"
    );
    Ok(())
}
