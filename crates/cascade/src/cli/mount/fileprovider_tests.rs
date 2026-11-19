//! Tests of the `--app-group` start mode: one engine and one content cache
//! behind the File Provider v2 socket and the NFS handlers.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cascade_engine::backend::{Backend, MountedBackend};
use cascade_engine::protocol::{Request, encode_message};
use cascade_engine::testing::{BackendLog, MockBackend, MockOp, RecordingBackend};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use super::AppGroupDaemon;
use crate::cli::CliContext;
use crate::cli::init::{BackendConfig, CascadeConfig, P2pConfig};
use crate::cli::mount::backends::rebuild_backends;
use crate::cli::mount::resolve_p2p_bridge_config;

const DEADLINE: Duration = Duration::from_secs(10);
const WRITE_BACK_DEADLINE: Duration = Duration::from_secs(30);
const TEST_SCAN_INTERVAL: Duration = Duration::from_millis(200);

struct Dirs {
    config: TempDir,
    app_group: TempDir,
    mount: TempDir,
}

impl Dirs {
    fn new() -> Self {
        Self {
            config: tempfile::tempdir().unwrap(),
            app_group: tempfile::tempdir().unwrap(),
            mount: tempfile::tempdir().unwrap(),
        }
    }

    fn ctx(&self) -> CliContext {
        CliContext::resolve(
            self.config.path().to_str().unwrap(),
            Some(self.app_group.path()),
        )
        .unwrap()
    }

    async fn start(&self, backends: Vec<MountedBackend>) -> AppGroupDaemon {
        let p2p = resolve_p2p_bridge_config(&P2pConfig::default()).unwrap();
        AppGroupDaemon::start(
            &self.ctx(),
            self.mount.path(),
            backends,
            true,
            &p2p,
            TEST_SCAN_INTERVAL,
        )
        .await
        .unwrap()
    }

    fn incoming(&self, data: &[u8]) -> PathBuf {
        let dir = self.app_group.path().join("incoming");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("saved.txt");
        std::fs::write(&path, data).unwrap();
        path
    }
}

/// A mock share mounted as `nas`, recording every backend call.
fn mock_share() -> (Arc<MockBackend>, BackendLog, Vec<MountedBackend>) {
    let share = Arc::new(MockBackend::new(7));
    let log = BackendLog::default();
    let backend: Arc<dyn Backend> = RecordingBackend::named(&share, &log, "nas");
    let mounted = vec![MountedBackend::new(Some("nas".to_owned()), backend)];
    (share, log, mounted)
}

async fn call(socket: &Path, method: &str, params: Value) -> Value {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    let request = Request {
        id: 1,
        method: method.to_owned(),
        params,
    };
    stream
        .write_all(&encode_message(&request).unwrap())
        .await
        .unwrap();
    let response = tokio::time::timeout(DEADLINE, async {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
        stream.read_exact(&mut body).await.unwrap();
        serde_json::from_slice::<Value>(&body).unwrap()
    })
    .await
    .expect("no response from the daemon");
    assert!(
        response.get("error").is_none(),
        "{method} failed: {response}"
    );
    response["result"].clone()
}

fn names(items: &Value) -> Vec<String> {
    items
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["name"].as_str().unwrap().to_owned())
        .collect()
}

fn uid_named(items: &Value, name: &str) -> String {
    items
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["name"] == name)
        .unwrap_or_else(|| panic!("no item {name} in {items}"))["uid"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn eventually(what: &str, deadline: Duration, mut check: impl AsyncFnMut() -> bool) {
    let reached = tokio::time::timeout(deadline, async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "{what} did not happen within {deadline:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn app_group_start_serves_protocol_v2_with_the_configured_backend_on_fp_sock() {
    let dirs = Dirs::new();
    let share_root = tempfile::tempdir().unwrap();
    std::fs::write(
        dirs.config.path().join("nas.toml"),
        format!(
            "type = \"local\"\nid = \"nas\"\nroot_path = \"{}\"\n",
            share_root.path().display()
        ),
    )
    .unwrap();
    let mut config = CascadeConfig::default();
    config.backends.insert(
        "nas".to_owned(),
        toml::Value::try_from(&BackendConfig {
            backend_type: "local".to_owned(),
            mount: None,
            account: None,
        })
        .unwrap(),
    );
    let http: Arc<dyn cascade_engine::portable::HttpClient> =
        Arc::new(cascade_engine::portable::native::ReqwestClient::new());
    let backends = rebuild_backends(&config, dirs.config.path(), http).unwrap();

    let daemon = dirs.start(backends).await;

    let socket = dirs.app_group.path().join("fp.sock");
    assert_eq!(daemon.socket_path(), socket);
    assert!(socket.as_os_str().len() <= 104);
    let hello = call(&socket, "hello", json!({"version": 2})).await;
    assert_eq!(hello["version"], 2);
    let root = call(&socket, "list", json!({"uid": hello["root_uid"]})).await;
    assert_eq!(names(&root["items"]), vec!["nas".to_owned()]);

    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_fetched_through_the_socket_is_read_through_nfs_without_a_second_download() {
    let dirs = Dirs::new();
    let (share, log, backends) = mock_share();
    let data: Vec<u8> = (0..300_000_u32)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    share.add_file("song.bin", &data);
    let daemon = dirs.start(backends).await;
    let socket = daemon.socket_path().to_path_buf();

    let mounts = call(&socket, "list", json!({"uid": "root"})).await;
    let nas = uid_named(&mounts["items"], "nas");
    let listed = call(&socket, "list", json!({"uid": nas})).await;
    let song = uid_named(&listed["items"], "song.bin");
    let fetched = call(&socket, "fetch", json!({"op_id": "op-1", "uid": song})).await;
    assert_eq!(
        std::fs::read(fetched["path"].as_str().unwrap()).unwrap(),
        data
    );
    let fetched_before = log.fetched();

    let len = u32::try_from(data.len()).unwrap();
    let (bytes, eof) = daemon
        .nfs()
        .read_range("/nas/song.bin", 0, len)
        .await
        .unwrap();

    assert_eq!(bytes, data);
    assert!(eof);
    assert_eq!(
        log.fetched(),
        fetched_before,
        "the NFS read went to the backend instead of the shared content cache"
    );

    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn app_group_start_recovers_then_scans_and_writes_back_in_the_background() {
    let dirs = Dirs::new();
    let (share, _log, backends) = mock_share();
    share.add_file("report.txt", b"first draft");
    let daemon = dirs.start(backends).await;
    let socket = daemon.socket_path().to_path_buf();

    let mounts = call(&socket, "list", json!({"uid": "root"})).await;
    let nas = uid_named(&mounts["items"], "nas");
    let listed = call(&socket, "list", json!({"uid": nas})).await;
    let report = uid_named(&listed["items"], "report.txt");
    let anchor = call(&socket, "currentAnchor", json!({})).await["anchor"].clone();

    share.add_file("later.txt", b"added on the NAS");
    eventually("the scanner reporting later.txt", DEADLINE, async || {
        let changes = call(&socket, "changes", json!({"anchor": anchor})).await;
        names(&changes["updated"]).contains(&"later.txt".to_owned())
    })
    .await;

    let saved = dirs.incoming(b"edited on the Mac");
    call(
        &socket,
        "modify",
        json!({
            "op_id": "op-2", "uid": report, "changed_fields": ["contents"],
            "attributes": {}, "contents_path": saved,
        }),
    )
    .await;
    eventually(
        "the write-back worker uploading report.txt",
        WRITE_BACK_DEADLINE,
        async || {
            let Ok(entry) = share.metadata(Path::new("report.txt")).await else {
                return false;
            };
            share.download(&entry).await.ok().as_deref() == Some(b"edited on the Mac".as_slice())
        },
    )
    .await;

    daemon.shutdown().await;
}

const SUN_PATH_BYTES: usize = 103;
const EDIT: &[u8] = b"edited on the Mac";

fn p2p() -> crate::cli::mount::ResolvedP2pConfig {
    resolve_p2p_bridge_config(&P2pConfig::default()).unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Backend calls a running scanner makes.
fn scan_calls(log: &BackendLog) -> usize {
    [
        MockOp::Changes,
        MockOp::Metadata,
        MockOp::ListChildren,
        MockOp::Revision,
    ]
    .into_iter()
    .map(|op| log.calls(op))
    .sum()
}

async fn list_mount(socket: &Path, mount: &str) -> Value {
    let mounts = call(socket, "list", json!({"uid": "root"})).await;
    let uid = uid_named(&mounts["items"], mount);
    call(socket, "list", json!({"uid": uid})).await["items"].clone()
}

#[test]
fn recovery_removes_an_orphaned_upload_before_start_returns_and_uploads_the_edit_once() {
    let dirs = Dirs::new();
    let share = Arc::new(MockBackend::new(11));
    let log = BackendLog::default();
    share.add_file("report.txt", b"first draft");

    let first_run = runtime();
    let killed = RecordingBackend::freezing_after(&share, &log, MockOp::Upload);
    let daemon = first_run.block_on(async {
        let backend: Arc<dyn Backend> = killed.clone();
        let daemon = dirs
            .start(vec![MountedBackend::new(Some("nas".to_owned()), backend)])
            .await;
        let socket = daemon.socket_path().to_path_buf();
        let report = uid_named(&list_mount(&socket, "mock").await, "report.txt");
        let saved = dirs.incoming(EDIT);
        call(
            &socket,
            "modify",
            json!({
                "op_id": "op-1", "uid": report, "changed_fields": ["contents"],
                "attributes": {}, "contents_path": saved,
            }),
        )
        .await;
        tokio::time::timeout(WRITE_BACK_DEADLINE, killed.frozen())
            .await
            .expect("the upload to the temporary name never ran");
        daemon
    });
    drop(first_run);
    drop(daemon);
    let orphan = log.uploads().pop().unwrap().path;
    assert!(
        share.content(&orphan).is_some(),
        "the killed run left no temporary file at {}",
        orphan.display()
    );

    let second_run = runtime();
    second_run.block_on(async {
        let backend: Arc<dyn Backend> = RecordingBackend::new(&share, &log);
        let daemon = dirs
            .start(vec![MountedBackend::new(Some("nas".to_owned()), backend)])
            .await;
        assert!(
            share.content(&orphan).is_none(),
            "TR-12: start returned before recovery removed {}",
            orphan.display()
        );
        let socket = daemon.socket_path().to_path_buf();
        let listed = names(&list_mount(&socket, "mock").await);
        assert!(
            listed.iter().all(|name| !name.starts_with(".cascade-tmp-")),
            "a temporary upload was listed: {listed:?}"
        );
        eventually(
            "the recovered edit reaching the NAS",
            WRITE_BACK_DEADLINE,
            async || share.content(Path::new("report.txt")).as_deref() == Some(EDIT),
        )
        .await;
        tokio::time::sleep(TEST_SCAN_INTERVAL * 5).await;
        daemon.shutdown().await;
    });
    let placed: Vec<_> = log
        .placed()
        .into_iter()
        .filter(|placed| placed.target == Path::new("report.txt"))
        .map(|placed| placed.data)
        .collect();
    assert_eq!(placed, vec![EDIT.to_vec()], "the edit was not placed once");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_removes_the_socket_stops_every_task_and_a_second_start_serves() {
    let dirs = Dirs::new();
    let (share, log, backends) = mock_share();
    share.add_file("report.txt", b"first draft");
    let daemon = dirs.start(backends).await;
    let socket = daemon.socket_path().to_path_buf();
    list_mount(&socket, "nas").await;
    let calls_at_start = scan_calls(&log);
    eventually(
        "the scanner rescanning the listed folder",
        DEADLINE,
        async || scan_calls(&log) > calls_at_start,
    )
    .await;
    let nfs_addr = daemon.nfs_server.local_addr;

    daemon.shutdown().await;

    assert!(!socket.exists(), "fp.sock outlived shutdown");
    assert!(UnixStream::connect(&socket).await.is_err());
    eventually("the NFS port closing", DEADLINE, async || {
        tokio::net::TcpStream::connect(nfs_addr).await.is_err()
    })
    .await;
    let calls_after_shutdown = scan_calls(&log);
    share.add_file("later.txt", b"added on the NAS");
    tokio::time::sleep(TEST_SCAN_INTERVAL * 5).await;
    assert_eq!(
        scan_calls(&log),
        calls_after_shutdown,
        "a task kept calling the backend after shutdown"
    );

    let (_share, _log, backends) = {
        let log = BackendLog::default();
        let backend: Arc<dyn Backend> = RecordingBackend::named(&share, &log, "nas");
        (
            Arc::clone(&share),
            log,
            vec![MountedBackend::new(Some("nas".to_owned()), backend)],
        )
    };
    let again = dirs.start(backends).await;
    let hello = call(again.socket_path(), "hello", json!({"version": 2})).await;
    assert_eq!(hello["version"], 2);
    let listed = names(&list_mount(again.socket_path(), "nas").await);
    assert!(listed.contains(&"later.txt".to_owned()), "{listed:?}");
    again.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn start_refuses_a_socket_another_listener_holds_and_a_later_start_serves() {
    let dirs = Dirs::new();
    let held_path = dirs.app_group.path().join("fp.sock");
    let holder = std::os::unix::net::UnixListener::bind(&held_path).unwrap();
    holder.set_nonblocking(true).unwrap();
    let (_share, _log, backends) = mock_share();

    let started = AppGroupDaemon::start(
        &dirs.ctx(),
        dirs.mount.path(),
        backends,
        true,
        &p2p(),
        TEST_SCAN_INTERVAL,
    )
    .await;

    let error = match started {
        Ok(daemon) => {
            daemon.shutdown().await;
            panic!("start took over a socket another listener holds");
        }
        Err(error) => format!("{error:#}"),
    };
    assert!(
        error.contains("fp.sock"),
        "error does not name the socket: {error}"
    );
    let _client = std::os::unix::net::UnixStream::connect(&held_path)
        .expect("the holder's socket was removed");
    assert!(
        holder.accept().is_ok(),
        "a connection to fp.sock no longer reaches the holder"
    );

    drop(holder);
    std::fs::remove_file(&held_path).unwrap();
    let (_share, _log, backends) = mock_share();
    let daemon = dirs.start(backends).await;
    let hello = call(daemon.socket_path(), "hello", json!({"version": 2})).await;
    assert_eq!(hello["version"], 2);
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn start_with_a_socket_path_beyond_sun_path_fails_naming_it() {
    let base = tempfile::tempdir().unwrap();
    let base_len = base.path().as_os_str().len();
    let group = base
        .path()
        .join("g".repeat(SUN_PATH_BYTES.saturating_sub(base_len)));
    std::fs::create_dir_all(&group).unwrap();
    let config = tempfile::tempdir().unwrap();
    let mount = tempfile::tempdir().unwrap();
    let ctx = CliContext::resolve(config.path().to_str().unwrap(), Some(&group)).unwrap();
    let (_share, _log, backends) = mock_share();

    let started = AppGroupDaemon::start(
        &ctx,
        mount.path(),
        backends,
        true,
        &p2p(),
        TEST_SCAN_INTERVAL,
    )
    .await;

    let error = match started {
        Ok(daemon) => {
            daemon.shutdown().await;
            panic!("start bound a socket path longer than {SUN_PATH_BYTES} bytes");
        }
        Err(error) => format!("{error:#}"),
    };
    assert!(
        error.contains("fp.sock"),
        "error does not name the socket: {error}"
    );
    assert!(!group.join("fp.sock").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn eviction_sweep_brings_populated_bytes_under_the_root_cascade_max_size() {
    const MAX_SIZE: u64 = 100_000;
    let dirs = Dirs::new();
    std::fs::write(
        dirs.mount.path().join(".cascade.toml"),
        "[cache]\nmax_size = \"100KB\"\n",
    )
    .unwrap();
    let (share, _log, backends) = mock_share();
    let data = vec![7_u8; 80_000];
    share.add_file("a.bin", &data);
    share.add_file("b.bin", &data);
    let daemon = dirs.start(backends).await;
    let socket = daemon.socket_path().to_path_buf();
    let listed = list_mount(&socket, "nas").await;
    for (op, name) in [("op-a", "a.bin"), ("op-b", "b.bin")] {
        let uid = uid_named(&listed, name);
        call(&socket, "fetch", json!({"op_id": op, "uid": uid})).await;
    }
    let db = Arc::clone(daemon.engine.db());
    assert!(db.content_populated_bytes().unwrap() > MAX_SIZE);

    daemon
        .engine
        .content_cache()
        .unwrap()
        .sweep()
        .await
        .unwrap();

    let populated = db.content_populated_bytes().unwrap();
    assert!(
        populated <= MAX_SIZE,
        "{populated} bytes stay cached over the configured {MAX_SIZE}"
    );
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn content_cache_is_one_cache_with_every_mounted_backend_registered() {
    let dirs = Dirs::new();
    let nas = Arc::new(MockBackend::new(3));
    let photos = Arc::new(MockBackend::new(5));
    let log = BackendLog::default();
    nas.add_file("report.txt", b"report");
    photos.add_file("cat.jpg", b"cat");
    let nas_backend: Arc<dyn Backend> = RecordingBackend::named(&nas, &log, "nas");
    let photos_backend: Arc<dyn Backend> = RecordingBackend::named(&photos, &log, "photos");
    let backends = vec![
        MountedBackend::new(Some("nas".to_owned()), nas_backend),
        MountedBackend::new(Some("photos".to_owned()), photos_backend),
    ];
    let daemon = dirs.start(backends).await;

    let first = daemon.engine.content_cache().unwrap();
    first.set_max_size(4242);
    let second = daemon.engine.content_cache().unwrap();

    assert_eq!(
        second.max_size(),
        4242,
        "a second call opened another cache"
    );
    let mut registered: Vec<String> = second
        .mounts()
        .into_iter()
        .map(|mount| mount.backend_id)
        .collect();
    registered.sort();
    assert_eq!(registered, vec!["nas".to_owned(), "photos".to_owned()]);
    let socket = daemon.socket_path().to_path_buf();
    assert_eq!(
        names(&list_mount(&socket, "photos").await),
        vec!["cat.jpg".to_owned()]
    );
    daemon.shutdown().await;
}
