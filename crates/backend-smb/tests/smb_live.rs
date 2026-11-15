//! Live tests; need `CASCADE_SMB_TEST=host:port/share/user/password` naming a share with `big.bin` (> 4 MiB) and `sub/hello.txt` (`hello from samba\n`).
//! Tests that pause or restart the share drive the Docker container `cascade-smb`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use cascade_engine::backend::{Backend, TransientError};
use cascade_engine::types::Change;
use tokio::sync::RwLock;

const CONTAINER: &str = "cascade-smb";
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
                    tokio::time::timeout(WATCHDOG, backend.read_range(&entry, offset, MIB)).await;
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
