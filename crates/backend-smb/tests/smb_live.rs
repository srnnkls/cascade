//! Live tests; need `CASCADE_SMB_TEST=host:port/share/user/password` naming a share with `big.bin` (> 4 MiB) and `sub/hello.txt` (`hello from samba\n`).
//! Tests in `container` drive the Docker container `cascade-smb`; all others need only the share.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cascade_engine::backend::{Backend, TransientError};
use cascade_engine::types::Change;
use tokio::sync::RwLock;

const MIB: u32 = 1024 * 1024;
const OPERATION_BUDGET: Duration = Duration::from_secs(15);
const WATCHDOG: Duration = Duration::from_secs(20);

/// Held shared by tests that only use the share, exclusively by tests that pause or restart it.
static SHARE: RwLock<()> = RwLock::const_new(());

struct ShareSpec {
    host: String,
    port: i64,
    share: String,
    user: String,
    password: String,
}

fn share_spec() -> ShareSpec {
    let spec =
        std::env::var("CASCADE_SMB_TEST").expect("CASCADE_SMB_TEST=host:port/share/user/password");
    let mut parts = spec.splitn(4, '/');
    let (address, share, user, password) = (
        parts.next().unwrap(),
        parts.next().unwrap(),
        parts.next().unwrap(),
        parts.next().unwrap(),
    );
    let (host, port) = address.split_once(':').unwrap();
    ShareSpec {
        host: host.to_string(),
        port: port.parse().unwrap(),
        share: share.to_string(),
        user: user.to_string(),
        password: password.to_string(),
    }
}

fn backend_for(spec: &ShareSpec) -> Box<dyn Backend> {
    let config = toml::toml! {
        id = "nas"
        host = (spec.host.clone())
        port = (spec.port)
        share = (spec.share.clone())
        username = (spec.user.clone())
        password = (spec.password.clone())
    };
    cascade_backend_smb::create_backend(&toml::Value::Table(config)).unwrap()
}

fn backend() -> Box<dyn Backend> {
    backend_for(&share_spec())
}

fn assert_unreachable(err: &anyhow::Error) {
    assert!(
        matches!(
            err.downcast_ref::<TransientError>(),
            Some(TransientError::Unreachable(_))
        ),
        "expected TransientError::Unreachable, got {err:#}"
    );
}

#[tokio::test]
#[ignore = "requires a live SMB share"]
async fn lists_root_and_subdirectory() {
    let _share = SHARE.read().await;
    let backend = backend();
    let root = backend.list_children("/").await.unwrap();
    let big = root.iter().find(|e| e.name == "big.bin").unwrap();
    assert!(!big.is_dir);
    assert_eq!(big.parent_id.native_id(), "/");
    assert!(big.mod_time.is_some());
    assert!(root.iter().any(|e| e.name == "sub" && e.is_dir));

    let sub = backend.list_children("sub").await.unwrap();
    let hello = sub.iter().find(|e| e.name == "hello.txt").unwrap();
    assert_eq!(hello.id.native_id(), "sub/hello.txt");
    assert_eq!(hello.parent_id.native_id(), "sub");
}

#[tokio::test]
#[ignore = "requires a live SMB share"]
async fn metadata_and_download_small_file() {
    let _share = SHARE.read().await;
    let backend = backend();
    let entry = backend.metadata(Path::new("sub/hello.txt")).await.unwrap();
    assert_eq!(entry.size, Some(17));
    assert_eq!(
        backend.download(&entry).await.unwrap(),
        b"hello from samba\n"
    );

    let dir = backend.metadata(Path::new("/sub")).await.unwrap();
    assert!(dir.is_dir);
}

#[tokio::test]
#[ignore = "requires a live SMB share"]
async fn read_range_matches_full_download() {
    let _share = SHARE.read().await;
    let backend = backend();
    let entry = backend.metadata(Path::new("big.bin")).await.unwrap();
    let full = backend.download(&entry).await.unwrap();
    assert_eq!(Some(full.len() as u64), entry.size);

    let offset = 3 * 1024 * 1024;
    let range = backend
        .read_range(&entry, offset as u64, 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(range, full[offset..offset + 1024 * 1024]);

    let tail = backend
        .read_range(&entry, full.len() as u64 - 10, 100)
        .await
        .unwrap();
    assert_eq!(tail, full[full.len() - 10..]);
    assert!(
        backend
            .read_range(&entry, full.len() as u64, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires a live SMB share"]
async fn changes_lists_parents_before_children_and_honours_cursor() {
    let _share = SHARE.read().await;
    let backend = backend();
    let (changes, cursor) = backend.changes(None).await.unwrap();
    let ids: Vec<String> = changes
        .iter()
        .map(|c| match c {
            Change::Created(e) => e.id.native_id().to_string(),
            other => panic!("unexpected change {other:?}"),
        })
        .collect();
    let sub = ids.iter().position(|id| id == "sub").unwrap();
    let hello = ids.iter().position(|id| id == "sub/hello.txt").unwrap();
    assert!(sub < hello);

    let (again, _) = backend.changes(Some(&cursor)).await.unwrap();
    assert!(again.is_empty(), "unchanged share reported {again:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a live SMB share"]
async fn sixteen_concurrent_whole_file_reads_return_correct_bytes() {
    let _share = SHARE.read().await;
    let backend: Arc<dyn Backend> = Arc::from(backend());
    let entry = backend.metadata(Path::new("big.bin")).await.unwrap();
    let full = Arc::new(backend.download(&entry).await.unwrap());
    let length = u32::try_from(full.len()).unwrap();
    assert!(length >= 8 * MIB, "big.bin must hold at least 8 MiB");

    let reads = (0..16).map(|_| {
        let backend = Arc::clone(&backend);
        let entry = entry.clone();
        tokio::spawn(async move { backend.read_range(&entry, 0, length).await })
    });
    let results = tokio::time::timeout(
        Duration::from_mins(1),
        futures_util::future::join_all(reads),
    )
    .await
    .expect("sixteen concurrent 8 MiB reads hung");
    for result in results {
        let bytes = result.unwrap().unwrap();
        assert!(bytes == *full, "concurrent read returned wrong bytes");
    }
}

#[tokio::test]
async fn refused_connection_is_reported_unreachable() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let backend = backend_for(&ShareSpec {
        host: "127.0.0.1".to_string(),
        port: i64::from(port),
        share: "data".to_string(),
        user: "cascade".to_string(),
        password: "cascade".to_string(),
    });

    let started = Instant::now();
    let result = tokio::time::timeout(WATCHDOG, backend.list_children("/"))
        .await
        .expect("listing a refused server never finished");
    assert!(started.elapsed() <= OPERATION_BUDGET);
    assert_unreachable(&result.unwrap_err());
}

mod container {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use cascade_engine::backend::{Backend, BackendError, TransientError};
    use cascade_engine::types::{FileEntry, FileId, Revision};
    use chrono::{DateTime, TimeZone, Utc};

    use super::{
        MIB, OPERATION_BUDGET, SHARE, ShareSpec, WATCHDOG, assert_unreachable, backend,
        backend_for, share_spec,
    };

    const CONTAINER: &str = "cascade-smb";

    async fn docker(args: &[&str]) {
        let status = tokio::process::Command::new("docker")
            .args(args)
            .status()
            .await
            .unwrap();
        assert!(status.success(), "docker {args:?} failed: {status}");
    }

    async fn wait_healthy() {
        let deadline = Instant::now() + Duration::from_mins(1);
        loop {
            let output = tokio::process::Command::new("docker")
                .args(["inspect", "-f", "{{.State.Health.Status}}", CONTAINER])
                .output()
                .await
                .unwrap();
            if String::from_utf8_lossy(&output.stdout).trim() == "healthy" {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{CONTAINER} did not become healthy"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Runs `script` in the share root as the share's owner, outside SMB.
    async fn on_share(script: &str) {
        docker(&[
            "exec",
            "-u",
            "1000:1000",
            "-e",
            "TZ=UTC",
            "-w",
            "/storage",
            CONTAINER,
            "sh",
            "-c",
            script,
        ])
        .await;
    }

    fn fixed_write_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap()
    }

    async fn listed(backend: &dyn Backend, dir: &str, name: &str) -> (FileEntry, Revision) {
        backend
            .list_children_with_revisions(dir)
            .await
            .unwrap()
            .into_iter()
            .find(|(entry, _)| entry.name == name)
            .unwrap_or_else(|| panic!("{dir}/{name} not listed"))
    }

    /// Change time on the share has second granularity on some filesystems.
    async fn let_change_time_advance() {
        tokio::time::sleep(Duration::from_millis(1100)).await;
    }

    #[tokio::test]
    #[ignore = "requires a live SMB share"]
    async fn rename_on_share_keeps_stable_id_and_moves_only_change_time() {
        let _share = SHARE.write().await;
        on_share(
            "rm -rf identity-rename && mkdir identity-rename \
             && printf 'renamed content\\n' > identity-rename/a.txt \
             && touch -d '2020-01-02 03:04:05' identity-rename/a.txt",
        )
        .await;
        let backend = backend();

        let (entry, before) = listed(&*backend, "identity-rename", "a.txt").await;
        assert_eq!(before.size, Some(16));
        assert_eq!(before.last_write_time, Some(fixed_write_time()));
        assert!(before.change_time.is_some(), "listing lacks change time");
        assert!(
            !before.stable_id.contains("a.txt"),
            "stable id {:?} derives from the path",
            before.stable_id
        );
        assert_eq!(
            backend.revision(&entry).await.unwrap(),
            before,
            "revision read from the file differs from the listing"
        );

        let_change_time_advance().await;
        on_share("mv identity-rename/a.txt identity-rename/b.txt").await;

        let (renamed, after) = listed(&*backend, "identity-rename", "b.txt").await;
        assert_eq!(
            after.stable_id, before.stable_id,
            "rename changed the stable id"
        );
        assert_eq!(after.size, before.size);
        assert_eq!(after.last_write_time, before.last_write_time);
        assert!(
            after.change_time > before.change_time,
            "rename did not move change time: {:?} -> {:?}",
            before.change_time,
            after.change_time
        );
        assert_eq!(backend.revision(&renamed).await.unwrap(), after);

        on_share("rm -rf identity-rename").await;
    }

    #[tokio::test]
    #[ignore = "requires a live SMB share"]
    async fn replace_by_rename_is_a_new_id_and_timestamp_restoring_edit_moves_change_time() {
        let _share = SHARE.write().await;
        on_share(
            "rm -rf identity-replace && mkdir identity-replace \
             && printf 'original\\n' > identity-replace/target.txt \
             && printf 'replacement\\n' > identity-replace/incoming.txt \
             && touch -d '2020-01-02 03:04:05' identity-replace/target.txt",
        )
        .await;
        let backend = backend();

        let (_, target) = listed(&*backend, "identity-replace", "target.txt").await;
        let (_, incoming) = listed(&*backend, "identity-replace", "incoming.txt").await;
        assert_ne!(target.stable_id, incoming.stable_id);

        let_change_time_advance().await;
        on_share(
            "printf 'edited!!\\n' > identity-replace/target.txt \
             && touch -d '2020-01-02 03:04:05' identity-replace/target.txt",
        )
        .await;
        let (_, edited) = listed(&*backend, "identity-replace", "target.txt").await;
        assert_eq!(
            edited.stable_id, target.stable_id,
            "in-place edit changed the stable id"
        );
        assert_eq!(edited.size, target.size);
        assert_eq!(edited.last_write_time, Some(fixed_write_time()));
        assert!(
            edited.change_time > target.change_time,
            "timestamp-restoring edit did not move change time: {:?} -> {:?}",
            target.change_time,
            edited.change_time
        );

        on_share("mv identity-replace/incoming.txt identity-replace/target.txt").await;
        let (_, replaced) = listed(&*backend, "identity-replace", "target.txt").await;
        assert_ne!(
            replaced.stable_id, target.stable_id,
            "replace-by-rename kept the replaced file's id"
        );
        assert_eq!(replaced.stable_id, incoming.stable_id);

        on_share("rm -rf identity-replace").await;
    }

    fn assert_not_found(err: &anyhow::Error) {
        assert!(
            matches!(err.downcast_ref(), Some(BackendError::NotFound(_))),
            "expected BackendError::NotFound, got {err:#}"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live SMB share"]
    async fn deleted_folder_is_reported_not_found() {
        let _share = SHARE.write().await;
        on_share("rm -rf deleted-folder && mkdir deleted-folder").await;
        let backend = backend();
        assert!(
            backend
                .list_children_with_revisions("deleted-folder")
                .await
                .unwrap()
                .is_empty()
        );

        on_share("rm -rf deleted-folder").await;

        assert_not_found(
            &backend
                .list_children_with_revisions("deleted-folder")
                .await
                .unwrap_err(),
        );
        assert_not_found(
            &backend
                .metadata(Path::new("deleted-folder"))
                .await
                .unwrap_err(),
        );
        assert_not_found(
            &backend
                .metadata(Path::new("deleted-folder/child"))
                .await
                .unwrap_err(),
        );
    }

    /// Unpauses the container when dropped, so a failing test leaves the share serving.
    struct Paused;

    impl Paused {
        async fn new() -> Self {
            docker(&["pause", CONTAINER]).await;
            Self
        }
    }

    impl Drop for Paused {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["unpause", CONTAINER])
                .status();
        }
    }

    #[tokio::test]
    #[ignore = "requires a live SMB share"]
    async fn paused_server_fails_unreachable_within_budget_and_service_resumes() {
        let _share = SHARE.write().await;
        let spec = share_spec();
        let backend = backend_for(&spec);
        backend.list_children("/").await.unwrap();

        let paused = Paused::new().await;

        let started = Instant::now();
        let result = tokio::time::timeout(WATCHDOG, backend.metadata(Path::new("sub/hello.txt")))
            .await
            .expect("metadata on a paused server never finished");
        let elapsed = started.elapsed();
        assert!(
            elapsed <= OPERATION_BUDGET,
            "metadata took {elapsed:?} on a paused server"
        );
        assert_unreachable(&result.unwrap_err());

        let fresh = backend_for(&spec);
        let started = Instant::now();
        let result = tokio::time::timeout(WATCHDOG, fresh.list_children("/"))
            .await
            .expect("first connect to a paused server never finished");
        let elapsed = started.elapsed();
        assert!(
            elapsed <= OPERATION_BUDGET,
            "connect took {elapsed:?} on a paused server"
        );
        assert_unreachable(&result.unwrap_err());

        drop(paused);

        let entry = tokio::time::timeout(
            OPERATION_BUDGET,
            backend.metadata(Path::new("sub/hello.txt")),
        )
        .await
        .expect("metadata after unpause did not finish within budget")
        .expect("service did not resume after unpause");
        assert_eq!(entry.size, Some(17));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live SMB share"]
    async fn read_during_server_restart_finishes_within_budget_and_next_read_succeeds() {
        let _share = SHARE.write().await;
        let backend: Arc<dyn Backend> = Arc::from(backend());
        let entry = backend.metadata(Path::new("big.bin")).await.unwrap();
        let full = backend.download(&entry).await.unwrap();
        let size = full.len() as u64;

        let stop = Arc::new(AtomicBool::new(false));
        let reader = {
            let backend = Arc::clone(&backend);
            let entry = entry.clone();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut slowest = Duration::ZERO;
                let mut offset = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    let outcome =
                        tokio::time::timeout(WATCHDOG, backend.read_range(&entry, offset, MIB))
                            .await;
                    slowest = slowest.max(started.elapsed());
                    match outcome {
                        Err(_) => return Err("read during restart never finished".to_string()),
                        Ok(Ok(_)) => offset = (offset + u64::from(MIB)) % size,
                        Ok(Err(_)) => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                }
                Ok(slowest)
            })
        };

        tokio::time::sleep(Duration::from_millis(300)).await;
        docker(&["restart", "--timeout", "0", CONTAINER]).await;
        wait_healthy().await;
        stop.store(true, Ordering::Relaxed);

        let slowest = reader.await.unwrap().unwrap();
        assert!(
            slowest <= OPERATION_BUDGET,
            "a read during the restart took {slowest:?}"
        );

        let offset = 2 * MIB as usize;
        let bytes = tokio::time::timeout(
            OPERATION_BUDGET,
            backend.read_range(&entry, offset as u64, MIB),
        )
        .await
        .expect("read after restart did not finish within budget")
        .expect("read after restart failed");
        assert!(
            bytes == full[offset..offset + MIB as usize],
            "read after restart returned wrong bytes"
        );
    }

    /// Output of `script` run in the share root as the share's owner, outside SMB.
    async fn share_output(script: &str) -> String {
        let output = tokio::process::Command::new("docker")
            .args([
                "exec",
                "-u",
                "1000:1000",
                "-e",
                "TZ=UTC",
                "-w",
                "/storage",
                CONTAINER,
                "sh",
                "-c",
                script,
            ])
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{script} failed: {output:?}");
        String::from_utf8(output.stdout).unwrap()
    }

    fn local_file(dir: &Path, name: &str, content: &[u8], modified: DateTime<Utc>) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(modified.into())
            .unwrap();
        path
    }

    fn parent_id(native_id: &str) -> FileId {
        FileId(format!("nas:{native_id}"))
    }

    #[tokio::test]
    #[ignore = "requires a live SMB share"]
    async fn save_to_temporary_name_then_replace_over_target_returns_renamed_revision() {
        let _share = SHARE.write().await;
        on_share(
            "rm -rf write-replace && mkdir write-replace \
             && printf 'nas version\\n' > write-replace/target.txt",
        )
        .await;
        let backend = backend();
        let local = std::env::temp_dir().join(format!("cascade-smb-write-{}", std::process::id()));
        std::fs::create_dir_all(&local).unwrap();
        let (_, old_target) = listed(&*backend, "write-replace", "target.txt").await;

        let first_write = Utc.with_ymd_and_hms(2021, 3, 4, 5, 6, 7).unwrap();
        let first = local_file(&local, "first", b"first save\n", first_write);
        let temp = backend
            .upload_from_path(
                Path::new("write-replace/.cascade-tmp-42"),
                &first,
                &parent_id("write-replace"),
            )
            .await
            .unwrap();
        assert_eq!(temp.id.native_id(), "write-replace/.cascade-tmp-42");
        assert_eq!(
            share_output("cat write-replace/.cascade-tmp-42").await,
            "first save\n"
        );
        assert_eq!(
            share_output("cat write-replace/target.txt").await,
            "nas version\n",
            "upload to the temporary name touched the target"
        );

        let second_write = Utc.with_ymd_and_hms(2022, 5, 6, 7, 8, 9).unwrap();
        let second = local_file(&local, "second", b"second, longer save\n", second_write);
        let temp = backend
            .update_from_path(&FileId(temp.id.0.clone()), &second)
            .await
            .unwrap();
        let (_, temp_revision) = listed(&*backend, "write-replace", ".cascade-tmp-42").await;
        assert_eq!(temp_revision.last_write_time, Some(second_write));

        let (renamed, revision) = backend
            .replace(
                Path::new(temp.id.native_id()),
                Path::new("write-replace/target.txt"),
            )
            .await
            .unwrap();
        assert_eq!(renamed.id.native_id(), "write-replace/target.txt");
        assert_eq!(revision.stable_id, temp_revision.stable_id);
        assert_ne!(revision.stable_id, old_target.stable_id);
        assert_eq!(revision.size, Some(20));
        assert_eq!(revision.last_write_time, Some(second_write));
        assert_eq!(
            share_output("cat write-replace/target.txt").await,
            "second, longer save\n"
        );
        assert_eq!(share_output("ls -A write-replace").await, "target.txt\n");

        std::fs::remove_dir_all(&local).unwrap();
        on_share("rm -rf write-replace").await;
    }

    /// Opens `native_id` on a second SMB client and keeps the handle open.
    async fn hold_open(spec: &ShareSpec, native_id: &str) -> (smb::Client, smb::Resource) {
        let client = smb::Client::new(smb::ClientConfig {
            dfs: false,
            connection: smb::ConnectionConfig {
                port: Some(u16::try_from(spec.port).unwrap()),
                ..Default::default()
            },
            ..Default::default()
        });
        let share: smb::UncPath = format!(r"\\{}\{}", spec.host, spec.share).parse().unwrap();
        client
            .share_connect(&share, &spec.user, spec.password.clone())
            .await
            .unwrap();
        let access = smb::FileAccessMask::new()
            .with_file_read_data(true)
            .with_synchronize(true);
        let handle = client
            .create_file(
                &share.with_path(&native_id.replace('/', "\\")),
                &smb::FileCreateArgs::make_open_existing(access),
            )
            .await
            .unwrap();
        (client, handle)
    }

    #[tokio::test]
    #[ignore = "requires a live SMB share"]
    async fn replace_over_target_held_open_elsewhere_is_busy_and_changes_nothing() {
        let _share = SHARE.write().await;
        on_share(
            "rm -rf write-busy && mkdir write-busy \
             && printf 'held\\n' > write-busy/target.txt \
             && printf 'incoming\\n' > write-busy/.cascade-tmp-7",
        )
        .await;
        let spec = share_spec();
        let backend = backend_for(&spec);
        let (client, handle) = hold_open(&spec, "write-busy/target.txt").await;

        let err = backend
            .replace(
                Path::new("write-busy/.cascade-tmp-7"),
                Path::new("write-busy/target.txt"),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<TransientError>(),
                Some(TransientError::Busy(_))
            ),
            "expected TransientError::Busy, got {err:#}"
        );

        if let smb::Resource::File(file) = handle {
            file.close().await.unwrap();
        }
        client.close().await.unwrap();
        assert_eq!(share_output("cat write-busy/target.txt").await, "held\n");
        assert_eq!(
            share_output("cat write-busy/.cascade-tmp-7").await,
            "incoming\n"
        );

        on_share("rm -rf write-busy").await;
    }

    #[tokio::test]
    #[ignore = "requires a live SMB share"]
    async fn folder_create_rename_move_and_recursive_delete_reject_names_smb_cannot_hold() {
        let _share = SHARE.write().await;
        on_share("rm -rf write-tree").await;
        let backend = backend();

        let root = backend.create_dir(Path::new("write-tree")).await.unwrap();
        assert!(root.is_dir);
        let dest = backend
            .create_dir(Path::new("write-tree/dest"))
            .await
            .unwrap();
        assert_eq!(dest.id.native_id(), "write-tree/dest");
        backend
            .upload(
                Path::new("write-tree/a.txt"),
                b"moving\n",
                &parent_id("write-tree"),
            )
            .await
            .unwrap();
        let (_, before) = listed(&*backend, "write-tree", "a.txt").await;

        let moved = backend
            .move_entry(
                Path::new("write-tree/a.txt"),
                Path::new("write-tree/dest/b.txt"),
            )
            .await
            .unwrap();
        assert_eq!(moved.id.native_id(), "write-tree/dest/b.txt");
        assert_eq!(moved.parent_id.native_id(), "write-tree/dest");
        let (_, after) = listed(&*backend, "write-tree/dest", "b.txt").await;
        assert_eq!(
            after.stable_id, before.stable_id,
            "move changed the stable id"
        );
        assert_eq!(share_output("cat write-tree/dest/b.txt").await, "moving\n");

        for name in ["a?b", "a*b", "a<b", "a>b", "a\"b", "a\\b", "a|b", "a:b"] {
            let dir = format!("write-tree/dir {name}");
            let upload = format!("write-tree/file {name}");
            let renamed = format!("write-tree/dest/renamed {name}");
            for err in [
                backend.create_dir(Path::new(&dir)).await.unwrap_err(),
                backend
                    .upload(Path::new(&upload), b"x", &parent_id("write-tree"))
                    .await
                    .unwrap_err(),
                backend
                    .move_entry(Path::new("write-tree/dest/b.txt"), Path::new(&renamed))
                    .await
                    .unwrap_err(),
            ] {
                assert!(
                    err.downcast_ref::<TransientError>().is_none(),
                    "name {name:?} rejected as transient: {err:#}"
                );
            }
        }
        assert_eq!(
            share_output("cd write-tree && find . | sort").await,
            ".\n./dest\n./dest/b.txt\n",
            "a rejected name reached the share"
        );

        let tree = backend.metadata(Path::new("write-tree")).await.unwrap();
        backend.delete(&tree).await.unwrap();
        assert_eq!(share_output("ls -A").await, "big.bin\nsub\n");
        assert_not_found(&backend.metadata(Path::new("write-tree")).await.unwrap_err());
    }
}
