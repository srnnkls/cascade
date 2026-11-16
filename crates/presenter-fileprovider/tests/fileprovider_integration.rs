#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::string_slice
)]
//! Integration tests for the macOS File Provider presenter.
//!
//! Exercises the bridge protocol, VfsItem↔FileProviderItem round-trips,
//! request building, and error handling. macOS-specific socket operations
//! are gated behind #[`cfg(target_os` = "macos")].

use cascade_engine::protocol::{Request, Response, decode_message, encode_message};
use cascade_engine::types::{CacheState, ItemId, VfsItem};
use cascade_presenter_fileprovider::bridge::FileProviderBridge;
use cascade_presenter_fileprovider::items::FileProviderItem;
use chrono::{TimeZone, Utc};
use serde_json::json;

// ---------------------------------------------------------------------------
// Task 2: Bridge round-trip tests
// ---------------------------------------------------------------------------

#[test]
fn bridge_builds_valid_protocol_request() {
    let bridge = FileProviderBridge::new("/tmp/cascade-test.sock");
    let request = bridge.build_request("upsertItem", json!({"id": "gdrive:abc"}));

    assert_eq!(request.method, "upsertItem");
    assert_eq!(request.params, json!({"id": "gdrive:abc"}));
}

#[test]
fn bridge_request_ids_monotonically_increase() {
    let bridge = FileProviderBridge::new("/tmp/cascade-test.sock");
    let r1 = bridge.build_request("method1", json!({}));
    let r2 = bridge.build_request("method2", json!({}));
    let r3 = bridge.build_request("method3", json!({}));

    assert!(r1.id < r2.id);
    assert!(r2.id < r3.id);
}

#[test]
fn bridge_request_encodes_with_length_prefix() {
    let bridge = FileProviderBridge::new("/tmp/cascade-test.sock");
    let request = bridge.build_request("deleteItem", json!({"id": "local:test"}));
    let encoded = encode_message(&request).unwrap();

    // First 4 bytes are big-endian length.
    assert!(encoded.len() > 4);
    let len = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
    assert_eq!(encoded.len(), 4 + len as usize);

    // Decode round-trips.
    let (consumed, decoded): (usize, Request) = decode_message(&encoded).unwrap().unwrap();
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded.id, request.id);
    assert_eq!(decoded.method, "deleteItem");
    assert_eq!(decoded.params, json!({"id": "local:test"}));
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn bridge_round_trips_over_unix_socket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    let socket_path = std::env::temp_dir().join(format!(
        "cascade-fp-integration-{}-roundtrip.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path).unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let body_len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await.unwrap();

        let request: Request = serde_json::from_slice(&body).unwrap();
        assert_eq!(request.method, "upsertItem");

        let response = Response::ok(request.id, json!({"status": "ok"}));
        let encoded = encode_message(&response).unwrap();
        stream.write_all(&encoded).await.unwrap();
    });

    let bridge = FileProviderBridge::new(&socket_path);
    let result: serde_json::Value = bridge
        .request("upsertItem", json!({"item": {"id": "test:file1"}}))
        .await
        .unwrap();

    assert_eq!(result, json!({"status": "ok"}));
    server.await.unwrap();
    std::fs::remove_file(&socket_path).unwrap();
}

// ---------------------------------------------------------------------------
// VfsItem ↔ FileProviderItem round-trip
// ---------------------------------------------------------------------------

fn sample_vfs_item() -> VfsItem {
    VfsItem {
        id: ItemId::new("gdrive", "file123"),
        parent_id: ItemId::new("gdrive", "root"),
        path: "gdrive/report.pdf".to_string(),
        name: "report.pdf".to_string(),
        is_dir: false,
        size: Some(8192),
        mod_time: Some(Utc.with_ymd_and_hms(2026, 5, 28, 10, 30, 0).unwrap()),
        cache_state: CacheState::Cached,
        mime_type: Some("application/pdf".to_string()),
    }
}

#[test]
fn vfs_item_to_file_provider_item_preserves_fields() {
    let item = sample_vfs_item();
    let fp_item = FileProviderItem::from(item);

    assert_eq!(fp_item.id, "gdrive:file123");
    assert_eq!(fp_item.parent_id, "gdrive:root");
    assert_eq!(fp_item.filename, "report.pdf");
    assert!(!fp_item.is_directory);
    assert_eq!(fp_item.size, Some(8192));
    assert_eq!(fp_item.content_type, Some("application/pdf".to_string()));
    assert_eq!(fp_item.cache_state, CacheState::Cached);
    assert!(fp_item.last_modified.is_some());
}

#[test]
fn file_provider_item_round_trips_to_vfs_item() {
    let original = sample_vfs_item();
    let fp_item = FileProviderItem::from(original.clone());
    let round_tripped = VfsItem::try_from(fp_item).unwrap();

    assert_eq!(round_tripped.id, original.id);
    assert_eq!(round_tripped.parent_id, original.parent_id);
    assert_eq!(round_tripped.name, original.name);
    assert_eq!(round_tripped.is_dir, original.is_dir);
    assert_eq!(round_tripped.size, original.size);
    assert_eq!(round_tripped.mod_time, original.mod_time);
    assert_eq!(round_tripped.cache_state, original.cache_state);
    assert_eq!(round_tripped.mime_type, original.mime_type);
}

#[test]
fn directory_vfs_item_round_trips() {
    let dir_item = VfsItem {
        id: ItemId::new("local", "photos"),
        parent_id: ItemId::new("local", "/"),
        path: "local/photos".to_string(),
        name: "photos".to_string(),
        is_dir: true,
        size: None,
        mod_time: Some(Utc.with_ymd_and_hms(2026, 1, 15, 0, 0, 0).unwrap()),
        cache_state: CacheState::Online,
        mime_type: None,
    };

    let fp_item = FileProviderItem::from(dir_item);
    assert!(fp_item.is_directory);
    assert_eq!(fp_item.size, None);

    let round_tripped = VfsItem::try_from(fp_item).unwrap();
    assert!(round_tripped.is_dir);
    assert_eq!(round_tripped.name, "photos");
}

#[test]
fn file_provider_item_rejects_invalid_date() {
    let mut fp_item = FileProviderItem::from(sample_vfs_item());
    fp_item.last_modified = Some("not-a-valid-date".to_string());

    let result = VfsItem::try_from(fp_item);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid File Provider modification date"),
    );
}

// ---------------------------------------------------------------------------
// Enumerator simulation via protocol
// ---------------------------------------------------------------------------

#[test]
fn enumerate_request_builds_correct_protocol_message() {
    let bridge = FileProviderBridge::new("/tmp/cascade-test.sock");
    let request = bridge.build_request(
        "enumerate",
        json!({
            "parent_id": "gdrive:root",
            "page_size": 100
        }),
    );

    assert_eq!(request.method, "enumerate");
    assert_eq!(
        request.params,
        json!({"parent_id": "gdrive:root", "page_size": 100}),
    );
}

#[test]
fn enumerate_changes_request_builds_correct_protocol_message() {
    let bridge = FileProviderBridge::new("/tmp/cascade-test.sock");
    let request = bridge.build_request(
        "enumerateChanges",
        json!({
            "parent_id": "gdrive:root",
            "since_cursor": "EjQ",
        }),
    );

    assert_eq!(request.method, "enumerateChanges");
    assert_eq!(
        request.params,
        json!({"parent_id": "gdrive:root", "since_cursor": "EjQ"}),
    );
}

#[test]
fn enumerate_changes_response_decodes_full_envelope() {
    // Simulate the JSON shape the Rust server emits for a populated
    // delta and verify both directions of the wire decode.
    let response = Response::ok(
        17,
        json!({
            "added_or_modified": [
                {
                    "id": "gdrive:file2",
                    "parent_id": "gdrive:root",
                    "filename": "new.txt",
                    "is_directory": false,
                    "size": 12,
                    "content_type": "text/plain",
                    "last_modified": null,
                    "cache_state": "online",
                }
            ],
            "deleted": ["gdrive:gone"],
            "new_cursor": "EjQ",
        }),
    );

    let encoded = encode_message(&response).unwrap();
    let (_, decoded): (usize, Response) = decode_message(&encoded).unwrap().unwrap();

    let result = decoded.result.unwrap();
    assert_eq!(result["new_cursor"], "EjQ");
    let added = result["added_or_modified"].as_array().unwrap();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0]["filename"], "new.txt");
    let deleted = result["deleted"].as_array().unwrap();
    assert_eq!(deleted[0], "gdrive:gone");
}

#[test]
fn enumerate_response_decodes_children() {
    let response = Response::ok(
        1,
        json!({
            "items": [
                {"id": "gdrive:file1", "name": "a.txt", "is_dir": false},
                {"id": "gdrive:dir1", "name": "subdir", "is_dir": true},
            ]
        }),
    );

    let encoded = encode_message(&response).unwrap();
    let (_, decoded): (usize, Response) = decode_message(&encoded).unwrap().unwrap();

    let binding = decoded.result.unwrap();
    let items = binding.get("items").unwrap().as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["name"], "a.txt");
    assert_eq!(items[1]["is_dir"], true);
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[test]
fn protocol_error_response_decodes() {
    let response = Response::error(42, "item not found: gdrive:nonexistent");

    let encoded = encode_message(&response).unwrap();
    let (_, decoded): (usize, Response) = decode_message(&encoded).unwrap().unwrap();

    assert!(decoded.result.is_none());
    assert_eq!(
        decoded.error,
        Some("item not found: gdrive:nonexistent".to_string())
    );
    assert_eq!(decoded.id, 42);
}

#[test]
fn invalid_json_body_produces_decode_error() {
    let len = 5u32.to_be_bytes();
    let mut buf = len.to_vec();
    buf.extend_from_slice(b"{bad}");

    let result: Result<(usize, Request), _> = decode_message(&buf)
        .ok()
        .flatten()
        .map_or_else(|| Err(anyhow::anyhow!("decode failed")), Ok);
    assert!(result.is_err(), "malformed JSON should fail to decode");
}

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn presenter_operations_fail_outside_macos() {
    use cascade_engine::presenter::VfsPresenter;
    use cascade_presenter_fileprovider::FileProviderPresenter;
    use std::path::Path;

    let presenter = FileProviderPresenter::new("/tmp/cascade-test.sock");
    let id = ItemId::new("gdrive", "file1");
    let item = VfsItem {
        id: id.clone(),
        parent_id: ItemId::new("gdrive", "root"),
        name: "test.txt".to_string(),
        path: "test.txt".to_string(),
        is_dir: false,
        size: Some(100),
        mod_time: None,
        cache_state: CacheState::Online,
        mime_type: None,
    };

    // All presenter operations should fail outside macOS.
    assert!(presenter.upsert_item(item.clone()).await.is_err());
    assert!(presenter.delete_item(&id).await.is_err());
    assert!(presenter.start(Path::new("/mnt/cascade")).await.is_err());
    assert!(presenter.stop().await.is_err());
}

// ---------------------------------------------------------------------------
// Property tests (proptest)
// ---------------------------------------------------------------------------

mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn vfs_item_round_trip_preserves_fields(
            name in "[a-zA-Z0-9_.\\-]{1,30}",
            size in any::<Option<u64>>(),
            is_dir: bool,
            backend_id in "[a-z]{1,10}",
            native_id in "[a-zA-Z0-9]{1,20}",
        ) {
            let item = VfsItem {
                id: ItemId::new(&backend_id, &native_id),
                parent_id: ItemId::new(&backend_id, "root"),
                path: format!("{backend_id}/{name}"),
                name,
                is_dir,
                size,
                mod_time: None,
                cache_state: CacheState::Online,
                mime_type: None,
            };

            let fp_item = FileProviderItem::from(item.clone());
            let round_tripped = VfsItem::try_from(fp_item).unwrap();

            prop_assert_eq!(round_tripped.id, item.id);
            prop_assert_eq!(round_tripped.parent_id, item.parent_id);
            prop_assert_eq!(round_tripped.name, item.name);
            prop_assert_eq!(round_tripped.is_dir, item.is_dir);
            prop_assert_eq!(round_tripped.size, item.size);
            prop_assert_eq!(round_tripped.mime_type, item.mime_type);
        }

        #[test]
        fn item_id_bridge_encoding_round_trips(
            backend in "[a-z]{1,10}",
            native in "[a-zA-Z0-9_\\-/]{1,30}",
        ) {
            let id = ItemId::new(&backend, &native);

            // Round-trip through JSON serialisation (same path as the bridge).
            let json = serde_json::to_string(&id).unwrap();
            let decoded: ItemId = serde_json::from_str(&json).unwrap();

            prop_assert_eq!(decoded.backend_id(), backend);
            prop_assert_eq!(decoded.native_id(), native);
        }

        #[test]
        fn request_encoding_round_trips(
            method in "[a-zA-Z]{1,20}",
            id: u32,
        ) {
            let request = Request {
                id,
                method: method.clone(),
                params: json!({"test": true}),
            };

            let encoded = encode_message(&request).unwrap();
            let (consumed, decoded): (usize, Request) = decode_message(&encoded)
                .unwrap()
                .unwrap();

            prop_assert_eq!(consumed, encoded.len());
            prop_assert_eq!(decoded.id, id);
            prop_assert_eq!(decoded.method, method);
        }
    }
}

#[cfg(unix)]
mod protocol_v2 {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use cascade_engine::backend::Backend;
    use cascade_engine::content::anchor::SyncAnchor;
    use cascade_engine::content::cache::ContentCache;
    use cascade_engine::content::replica::ReplicaStore;
    use cascade_engine::content::transfer::TransferConfig;
    use cascade_engine::db::StateDb;
    use cascade_engine::protocol::{Request, encode_message};
    use cascade_engine::testing::{BackendLog, Disk, Fault, MockBackend, MockOp, RecordingBackend};
    use cascade_presenter_fileprovider::server::ContentServer;
    use cascade_presenter_fileprovider::wire::FileProviderErrorCode;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    const DEADLINE: Duration = Duration::from_secs(10);
    /// Bound of a wait that spans a write-back delay.
    const WRITE_BACK_DEADLINE: Duration = Duration::from_secs(30);
    const MIB: usize = 1024 * 1024;
    const FILE_PROVIDER_DOMAIN: &str = "NSFileProviderErrorDomain";
    const COCOA_DOMAIN: &str = "NSCocoaErrorDomain";

    /// Every code the server answers and the error domain Apple defines it
    /// in (FileProvider.framework `NSFileProviderError.h`, Foundation
    /// `FoundationErrors.h`).
    const ERROR_TABLE: [(&str, &str); 17] = [
        ("notAuthenticated", FILE_PROVIDER_DOMAIN),
        ("filenameCollision", FILE_PROVIDER_DOMAIN),
        ("syncAnchorExpired", FILE_PROVIDER_DOMAIN),
        ("pageExpired", FILE_PROVIDER_DOMAIN),
        ("serverUnreachable", FILE_PROVIDER_DOMAIN),
        ("noSuchItem", FILE_PROVIDER_DOMAIN),
        ("deletionRejected", FILE_PROVIDER_DOMAIN),
        ("directoryNotEmpty", FILE_PROVIDER_DOMAIN),
        ("cannotSynchronize", FILE_PROVIDER_DOMAIN),
        ("nonEvictable", FILE_PROVIDER_DOMAIN),
        ("versionNoLongerAvailable", FILE_PROVIDER_DOMAIN),
        ("excludedFromSync", FILE_PROVIDER_DOMAIN),
        ("olderExtensionVersionRunning", FILE_PROVIDER_DOMAIN),
        ("newerExtensionVersionFound", FILE_PROVIDER_DOMAIN),
        ("fileWriteOutOfSpace", COCOA_DOMAIN),
        ("userCancelled", COCOA_DOMAIN),
        ("featureUnsupported", COCOA_DOMAIN),
    ];

    fn uid(n: u64) -> Value {
        json!(n.to_string())
    }

    fn pattern(len: usize, salt: u8) -> Vec<u8> {
        (0..len)
            .map(|i| u8::try_from(i % 251).unwrap() ^ salt)
            .collect()
    }

    async fn send(stream: &mut UnixStream, method: &str, params: Value) {
        send_as(stream, 1, method, params).await;
    }

    async fn send_as(stream: &mut UnixStream, id: u32, method: &str, params: Value) {
        let request = Request {
            id,
            method: method.to_string(),
            params,
        };
        stream
            .write_all(&encode_message(&request).unwrap())
            .await
            .unwrap();
    }

    async fn read_frame(stream: &mut UnixStream) -> Value {
        tokio::time::timeout(DEADLINE, async {
            let mut len = [0u8; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
            stream.read_exact(&mut body).await.unwrap();
            serde_json::from_slice::<Value>(&body).unwrap()
        })
        .await
        .expect("no frame from the server")
    }

    /// Serves `cache` on `socket` until the returned sender fires.
    async fn serve(socket: &Path, cache: ContentCache) -> (watch::Sender<bool>, JoinHandle<()>) {
        let (cancel, cancel_rx) = watch::channel(false);
        let server = ContentServer::new(socket, cache);
        let serving = tokio::spawn(async move {
            server.serve(cancel_rx).await.unwrap();
        });
        let mut accepting = false;
        for _ in 0..200 {
            if UnixStream::connect(socket).await.is_ok() {
                accepting = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(accepting, "server failed to bind {}", socket.display());
        (cancel, serving)
    }

    async fn call_at(socket: &Path, method: &str, params: Value) -> Value {
        let mut stream = UnixStream::connect(socket).await.unwrap();
        send(&mut stream, method, params).await;
        read_frame(&mut stream).await
    }

    fn result_of(method: &str, response: Value) -> Value {
        assert!(
            response.get("error").is_none(),
            "{method} failed: {response}"
        );
        response["result"].clone()
    }

    fn error_code_of(method: &str, response: &Value) -> String {
        assert!(
            response.get("result").is_none(),
            "{method} succeeded where it must fail: {response}"
        );
        response["error"]["code"].as_str().unwrap().to_string()
    }

    async fn subscribe_at(socket: &Path) -> UnixStream {
        let mut stream = UnixStream::connect(socket).await.unwrap();
        send(&mut stream, "subscribe", json!({})).await;
        let ack = read_frame(&mut stream).await;
        assert!(ack.get("error").is_none(), "subscribe failed: {ack}");
        stream
    }

    /// Polls `check` until it holds or `deadline` passes.
    async fn eventually(what: &str, deadline: Duration, mut check: impl AsyncFnMut() -> bool) {
        let reached = tokio::time::timeout(deadline, async {
            while !check().await {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(reached.is_ok(), "{what} did not happen within {deadline:?}");
    }

    struct Domain {
        app_group: tempfile::TempDir,
        _cache_dir: tempfile::TempDir,
        db: Arc<StateDb>,
        store: ReplicaStore,
        share: Arc<MockBackend>,
        backend: Arc<dyn Backend>,
        log: BackendLog,
        cache: ContentCache,
        root: u64,
        _cancel: watch::Sender<bool>,
    }

    impl Domain {
        async fn start() -> Self {
            let app_group = tempfile::tempdir().unwrap();
            let cache_dir = tempfile::tempdir().unwrap();
            let db = Arc::new(StateDb::open_in_memory().unwrap());
            let store = ReplicaStore::open(Arc::clone(&db)).unwrap();
            let share = Arc::new(MockBackend::new(7));
            let log = BackendLog::default();
            let backend: Arc<dyn Backend> = RecordingBackend::new(&share, &log);
            let cache = ContentCache::open(
                store.clone(),
                cache_dir.path().to_path_buf(),
                app_group.path().join("staging"),
                TransferConfig::default(),
            )
            .unwrap();
            cache.register(Arc::clone(&backend)).unwrap();
            cache.recover().await.unwrap();
            let root = store.replica(Arc::clone(&backend)).unwrap().root_uid();
            let (cancel, _serving) = serve(&app_group.path().join("fp.sock"), cache.clone()).await;

            Self {
                app_group,
                _cache_dir: cache_dir,
                db,
                store,
                share,
                backend,
                log,
                cache,
                root,
                _cancel: cancel,
            }
        }

        fn socket(&self) -> PathBuf {
            self.app_group.path().join("fp.sock")
        }

        fn staging(&self) -> PathBuf {
            self.app_group.path().join("staging")
        }

        /// Registers a second backend under `id` and returns its share.
        fn mount(&self, id: &str) -> Arc<MockBackend> {
            let share = Arc::new(MockBackend::new(13));
            let backend: Arc<dyn Backend> = RecordingBackend::named(&share, &self.log, id);
            self.cache.register(backend).unwrap();
            share
        }

        async fn add(&self, path: &str, data: &[u8]) -> u64 {
            self.share.add_file(path, data);
            self.resolve(path).await
        }

        async fn resolve(&self, path: &str) -> u64 {
            self.store
                .replica(Arc::clone(&self.backend))
                .unwrap()
                .resolve(Path::new(path))
                .await
                .unwrap()
                .unwrap()
        }

        async fn on_share(&self, path: &str) -> bool {
            self.share.metadata(Path::new(path)).await.is_ok()
        }

        async fn share_bytes(&self, path: &str) -> Option<Vec<u8>> {
            let entry = self.share.metadata(Path::new(path)).await.ok()?;
            self.share.download(&entry).await.ok()
        }

        /// A saved file the extension hands over from the App Group.
        fn incoming(&self, data: &[u8]) -> PathBuf {
            let dir = self.app_group.path().join("incoming");
            std::fs::create_dir_all(&dir).unwrap();
            let (_, path) = tempfile::NamedTempFile::new_in(&dir)
                .unwrap()
                .keep()
                .unwrap();
            std::fs::write(&path, data).unwrap();
            path
        }

        async fn connect(&self) -> UnixStream {
            UnixStream::connect(self.socket()).await.unwrap()
        }

        async fn call(&self, method: &str, params: Value) -> Value {
            call_at(&self.socket(), method, params).await
        }

        async fn ok(&self, method: &str, params: Value) -> Value {
            result_of(method, self.call(method, params).await)
        }

        async fn error_code(&self, method: &str, params: Value) -> String {
            error_code_of(method, &self.call(method, params).await)
        }

        async fn subscribe(&self) -> UnixStream {
            subscribe_at(&self.socket()).await
        }

        async fn item(&self, uid: u64) -> Value {
            self.ok("item", json!({"uid": uid.to_string()})).await["item"].clone()
        }

        async fn hello_root(&self) -> Value {
            self.ok("hello", json!({"version": 2})).await["root_uid"].clone()
        }

        async fn modify_contents(&self, op_id: &str, uid: u64, data: &[u8]) -> Value {
            let contents = self.incoming(data);
            let reply = self
                .ok(
                    "modify",
                    json!({
                        "op_id": op_id, "uid": uid.to_string(), "changed_fields": ["contents"],
                        "attributes": {}, "contents_path": contents,
                    }),
                )
                .await;
            std::fs::remove_file(contents).unwrap();
            reply
        }

        /// Pages of `method` from the first until one without `next_page`.
        async fn all_pages(&self, method: &str, mut params: Value) -> Vec<Value> {
            let mut pages = Vec::new();
            loop {
                let page = self.ok(method, params.clone()).await;
                let next = page
                    .get("next_page")
                    .cloned()
                    .filter(|next| !next.is_null());
                pages.push(page);
                match next {
                    Some(next) => params["page"] = next,
                    None => return pages,
                }
            }
        }
    }

    fn uids_of(pages: &[Value]) -> Vec<String> {
        pages
            .iter()
            .flat_map(|page| page["items"].as_array().unwrap().iter())
            .map(|item| item["uid"].as_str().unwrap().to_string())
            .collect()
    }

    async fn expect_signal(stream: &mut UnixStream, expected: &Value) {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let frame = read_frame(stream).await;
                if &frame["result"] == expected {
                    return;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no signal {expected} on the subscribe stream"));
    }

    fn names_of_files(items: &Value) -> Vec<String> {
        items
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["kind"] == "file")
            .map(|item| item["name"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn changes_returns_updates_and_tombstones_in_revision_order_and_expires_foreign_anchors()
    {
        let domain = Domain::start().await;
        let other = Domain::start().await;
        domain.share.add_file("zero.txt", b"0");

        let hello = domain.ok("hello", json!({"version": 2})).await;
        assert_eq!(hello["version"], 2);

        let listed = domain.ok("list", json!({"uid": uid(domain.root)})).await;
        assert_eq!(names_of_files(&listed["items"]), ["zero.txt"]);
        let zero = listed["items"][0]["uid"].clone();
        let item = domain.ok("item", json!({"uid": zero})).await["item"].clone();
        assert_eq!(item["uid"], zero);
        assert_eq!(item["parent_uid"], uid(domain.root));
        assert_eq!(item["name"], "zero.txt");
        assert_eq!(item["kind"], "file");
        assert_eq!(item["size"], 1);

        let start = domain.ok("currentAnchor", json!({})).await["anchor"].clone();
        domain.share.add_file("one.txt", b"1");
        assert!(domain.cache.scan().await.unwrap());
        domain.share.add_file("two.txt", b"22");
        assert!(domain.cache.scan().await.unwrap());
        domain.share.add_file("one.txt", b"111");
        assert!(domain.cache.scan().await.unwrap());
        domain.share.remove("zero.txt").unwrap();
        assert!(domain.cache.scan().await.unwrap());

        let changes = domain.ok("changes", json!({"anchor": start})).await;
        assert_eq!(
            names_of_files(&changes["updated"]),
            ["two.txt", "one.txt"],
            "updates must come in revision order, each item at its latest revision"
        );
        assert_eq!(changes["deleted"], json!([zero]));

        let caught_up = domain
            .ok("changes", json!({"anchor": changes["anchor"]}))
            .await;
        assert_eq!(caught_up["updated"], json!([]));
        assert_eq!(caught_up["deleted"], json!([]));

        let foreign = other.ok("currentAnchor", json!({})).await["anchor"].clone();
        assert_eq!(
            domain
                .error_code("changes", json!({"anchor": foreign}))
                .await,
            "syncAnchorExpired"
        );
    }

    #[tokio::test]
    async fn fetch_stages_in_the_app_group_and_a_strict_fetch_of_a_gone_version_fails() {
        let domain = Domain::start().await;
        let data = pattern(10_000, 3);
        let file = domain.add("doc.bin", &data).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;

        let fetched = domain
            .ok("fetch", json!({"op_id": "op-1", "uid": uid(file)}))
            .await;
        let path = PathBuf::from(fetched["path"].as_str().unwrap());
        assert!(
            path.starts_with(domain.staging()),
            "{} is outside the App Group staging directory",
            path.display()
        );
        assert_eq!(std::fs::read(&path).unwrap(), data);
        assert_eq!(fetched["item"]["uid"], uid(file));
        let old_version = fetched["item"]["content_version"].clone();
        assert!(old_version.is_string());

        let range = domain
            .ok(
                "fetchRange",
                json!({
                    "op_id": "op-2", "uid": uid(file), "requested_version": old_version,
                    "strict": true, "offset": 5000, "length": 100, "alignment": 4096,
                }),
            )
            .await;
        let offset = range["retrieved_offset"].as_u64().unwrap();
        let length = range["retrieved_length"].as_u64().unwrap();
        assert_eq!(offset % 4096, 0);
        assert!(
            offset <= 5000 && offset + length >= 5100,
            "{offset}+{length}"
        );
        let clone = PathBuf::from(range["path"].as_str().unwrap());
        assert!(clone.starts_with(domain.staging()));
        assert_eq!(
            std::fs::read(&clone).unwrap().get(5000..5100),
            data.get(5000..5100)
        );
        assert_eq!(range["item"]["content_version"], old_version);

        let changed = pattern(12_000, 9);
        domain.share.add_file("doc.bin", &changed);
        assert!(domain.cache.scan().await.unwrap());

        let strict = json!({
            "op_id": "op-3", "uid": uid(file), "requested_version": old_version,
            "strict": true, "offset": 0, "length": 100, "alignment": 4096,
        });
        assert_eq!(
            domain.error_code("fetchRange", strict).await,
            "versionNoLongerAvailable"
        );

        let lenient = domain
            .ok(
                "fetchRange",
                json!({
                    "op_id": "op-4", "uid": uid(file), "requested_version": old_version,
                    "strict": false, "offset": 0, "length": 100, "alignment": 4096,
                }),
            )
            .await;
        assert_ne!(lenient["item"]["content_version"], old_version);
        assert_eq!(lenient["item"]["size"], 12_000);
    }

    #[tokio::test]
    async fn mutations_apply_directly_map_errors_and_raise_signals() {
        let domain = Domain::start().await;
        let data = pattern(10_000, 5);
        let file = domain.add("report.txt", &data).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let mut signals = domain.subscribe().await;

        let excluded = json!({
            "op_id": "op-1", "parent_uid": uid(domain.root), "name": "a?b",
            "kind": "folder", "attributes": {}, "may_already_exist": false,
        });
        assert_eq!(
            domain.error_code("create", excluded).await,
            "excludedFromSync"
        );
        assert!(!domain.on_share("a?b").await);

        let created = domain
            .ok(
                "create",
                json!({
                    "op_id": "op-2", "parent_uid": uid(domain.root), "name": "made",
                    "kind": "folder", "attributes": {}, "may_already_exist": false,
                }),
            )
            .await;
        assert_eq!(created["item"]["name"], "made");
        assert_eq!(created["item"]["kind"], "folder");
        assert!(domain.on_share("made").await);
        let deleted = domain
            .ok(
                "delete",
                json!({"op_id": "op-3", "uid": created["item"]["uid"], "recursive": false}),
            )
            .await;
        assert_eq!(deleted, json!({}));
        assert!(!domain.on_share("made").await);
        assert_eq!(
            domain
                .ok(
                    "delete",
                    json!({"op_id": "op-4", "uid": "999999", "recursive": false})
                )
                .await,
            json!({})
        );

        let renamed = domain
            .ok(
                "modify",
                json!({
                    "op_id": "op-5", "uid": uid(file), "changed_fields": ["name"],
                    "name": "x:y", "parent_uid": uid(domain.root), "attributes": {},
                }),
            )
            .await;
        assert_eq!(renamed["item"]["name"], "report.txt");
        assert!(domain.on_share("report.txt").await);
        assert!(!domain.on_share("x:y").await);

        domain.share.inject(MockOp::Move, Fault::Unreachable);
        let rename = json!({
            "op_id": "op-6", "uid": uid(file), "changed_fields": ["name"],
            "name": "final.txt", "parent_uid": uid(domain.root), "attributes": {},
        });
        assert_eq!(
            domain.error_code("modify", rename.clone()).await,
            "serverUnreachable"
        );
        assert!(domain.on_share("report.txt").await);
        let applied = domain.ok("modify", rename).await;
        assert_eq!(applied["item"]["name"], "final.txt");
        assert!(domain.on_share("final.txt").await);
        expect_signal(&mut signals, &json!({"signal": "errorResolved"})).await;

        domain.share.add_file("remote.txt", b"remote");
        assert!(domain.cache.scan().await.unwrap());
        expect_signal(&mut signals, &json!({"signal": "workingSet"})).await;

        domain.cache.set_max_size(4096);
        assert_eq!(
            domain
                .error_code("setPinned", json!({"uids": [uid(file)], "pinned": true}))
                .await,
            "fileWriteOutOfSpace"
        );
        let item = domain.ok("item", json!({"uid": uid(file)})).await;
        assert_eq!(item["item"]["pinned"], false);

        domain
            .ok("fetch", json!({"op_id": "op-7", "uid": uid(file)}))
            .await;
        domain
            .ok("materializedSet", json!({"uids": [uid(file)]}))
            .await;
        domain.cache.set_max_size(0);
        domain.cache.sweep().await.unwrap();
        expect_signal(&mut signals, &json!({"signal": "evict", "uid": uid(file)})).await;
        assert!(!domain.db.content_objects_of(file).unwrap().is_empty());
        domain.ok("evict", json!({"uid": uid(file)})).await;
        assert!(domain.db.content_objects_of(file).unwrap().is_empty());
    }

    async fn held(stop: &cascade_engine::testing::StopPoint) {
        tokio::time::timeout(DEADLINE, stop.reached())
            .await
            .expect("no backend call reached the stop point");
    }

    fn frames_by_id(frames: Vec<Value>) -> BTreeMap<u64, Value> {
        frames
            .into_iter()
            .map(|frame| (frame["id"].as_u64().unwrap(), frame))
            .collect()
    }

    #[tokio::test]
    async fn one_connection_serves_several_requests_each_answered_under_its_id() {
        let domain = Domain::start().await;
        let data = pattern(10_000, 1);
        let file = domain.add("a.bin", &data).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let mut stream = domain.connect().await;

        let download = domain.share.stop_at(MockOp::Download);
        send_as(
            &mut stream,
            21,
            "fetch",
            json!({"op_id": "op-held", "uid": uid(file)}),
        )
        .await;
        held(&download).await;
        send_as(&mut stream, 22, "item", json!({"uid": uid(file)})).await;
        send_as(&mut stream, 23, "currentAnchor", json!({})).await;
        let answered = frames_by_id(vec![
            read_frame(&mut stream).await,
            read_frame(&mut stream).await,
        ]);
        assert_eq!(
            answered.keys().copied().collect::<Vec<_>>(),
            [22, 23],
            "requests behind a held fetch must be answered first: {answered:?}"
        );
        assert_eq!(answered[&22]["result"]["item"]["uid"], uid(file));
        assert!(answered[&23]["result"]["anchor"].is_string());

        download.release();
        let fetched = read_frame(&mut stream).await;
        assert_eq!(fetched["id"], 21);
        assert_eq!(fetched["result"]["item"]["uid"], uid(file));
        let path = PathBuf::from(fetched["result"]["path"].as_str().unwrap());
        assert_eq!(std::fs::read(path).unwrap(), data);

        send_as(&mut stream, 24, "item", json!({"uid": uid(domain.root)})).await;
        let again = read_frame(&mut stream).await;
        assert_eq!(again["id"], 24);
        assert_eq!(again["result"]["item"]["uid"], uid(domain.root));
    }

    #[tokio::test]
    async fn hello_with_another_version_answers_the_older_or_newer_extension_error() {
        let domain = Domain::start().await;
        assert_eq!(
            domain.error_code("hello", json!({"version": 1})).await,
            "olderExtensionVersionRunning"
        );
        assert_eq!(
            domain.error_code("hello", json!({"version": 3})).await,
            "newerExtensionVersionFound"
        );
        let hello = domain.ok("hello", json!({"version": 2})).await;
        assert_eq!(hello["version"], 2);
        assert!(hello["root_uid"].is_string());
    }

    #[tokio::test]
    async fn working_set_and_list_page_through_every_item_once_and_a_bad_page_token_expires() {
        let domain = Domain::start().await;
        for n in 0..301 {
            domain.share.add_file(&format!("f{n:03}"), b"x");
        }
        let listed = domain
            .all_pages("list", json!({"uid": uid(domain.root)}))
            .await;
        assert!(
            listed.len() > 1,
            "301 children must take more than one page"
        );
        let listed_uids = uids_of(&listed);
        let distinct: BTreeSet<&String> = listed_uids.iter().collect();
        assert_eq!(listed_uids.len(), 301);
        assert_eq!(distinct.len(), 301, "a child was listed twice");

        let (pinned, rest) = listed_uids.split_at(299);
        let materialised = &rest[0];
        let outside = &rest[1];
        domain
            .ok("setPinned", json!({"uids": pinned, "pinned": true}))
            .await;
        domain
            .ok("materializedSet", json!({"uids": [materialised]}))
            .await;

        let working = domain.all_pages("workingSet", json!({})).await;
        assert!(working.len() > 1, "300 items must take more than one page");
        let working_uids = uids_of(&working);
        let expected: BTreeSet<&String> = pinned.iter().chain([materialised]).collect();
        assert_eq!(working_uids.len(), 300, "an item was paged twice or lost");
        assert_eq!(working_uids.iter().collect::<BTreeSet<_>>(), expected);
        assert!(!working_uids.contains(outside));
        let pinned_flags: Vec<bool> = working
            .iter()
            .flat_map(|page| page["items"].as_array().unwrap().iter())
            .filter(|item| item["uid"] != json!(materialised))
            .map(|item| item["pinned"].as_bool().unwrap())
            .collect();
        assert!(pinned_flags.iter().all(|pinned| *pinned));

        assert_eq!(
            domain
                .error_code(
                    "list",
                    json!({"uid": uid(domain.root), "page": "not-a-page"})
                )
                .await,
            "pageExpired"
        );
        assert_eq!(
            domain
                .error_code("workingSet", json!({"page": "not-a-page"}))
                .await,
            "pageExpired"
        );
    }

    #[tokio::test]
    async fn changes_since_an_anchor_below_the_tombstone_floor_or_undecodable_expires() {
        let domain = Domain::start().await;
        domain.share.add_file("keep.txt", b"k");
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let before = domain.ok("currentAnchor", json!({})).await["anchor"].clone();
        domain.share.add_file("gone.txt", b"g");
        assert!(domain.cache.scan().await.unwrap());
        domain.share.remove("gone.txt").unwrap();
        assert!(domain.cache.scan().await.unwrap());
        let after = domain.ok("currentAnchor", json!({})).await["anchor"].clone();
        let pruned = SyncAnchor::from_bytes(after.as_str().unwrap().as_bytes()).unwrap();
        domain
            .store
            .prune_tombstones_through(pruned.revision())
            .unwrap();

        assert_eq!(
            domain
                .error_code("changes", json!({"anchor": before}))
                .await,
            "syncAnchorExpired"
        );
        let current = domain.ok("changes", json!({"anchor": after})).await;
        assert_eq!(current["updated"], json!([]));
        assert_eq!(current["deleted"], json!([]));
        assert_eq!(
            domain
                .error_code("changes", json!({"anchor": "no anchor at all"}))
                .await,
            "syncAnchorExpired"
        );
        assert_eq!(
            domain
                .error_code("changes", json!({"anchor": "x".repeat(600)}))
                .await,
            "syncAnchorExpired"
        );
    }

    #[tokio::test]
    async fn strict_fetch_range_of_a_partially_materialised_file_whose_remote_changed_is_refused() {
        let domain = Domain::start().await;
        let data = pattern(8 * MIB, 3);
        let file = domain.add("movie.bin", &data).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        domain
            .share
            .inject(MockOp::Download, Fault::Delay(Duration::ZERO));
        domain
            .share
            .inject(MockOp::Download, Fault::Delay(Duration::from_secs(3)));

        let first = domain
            .ok(
                "fetchRange",
                json!({
                    "op_id": "op-1", "uid": uid(file), "strict": true,
                    "offset": 0, "length": 100, "alignment": 4096,
                }),
            )
            .await;
        let old_version = first["item"]["content_version"].clone();
        let cached = domain.cache.cached_bytes(file).unwrap();
        assert!(
            cached.done < cached.total,
            "precondition: the object is only partially materialised ({cached:?})"
        );

        let changed = pattern(8 * MIB, 11);
        domain.share.add_file("movie.bin", &changed);
        let strict = json!({
            "op_id": "op-2", "uid": uid(file), "requested_version": old_version,
            "strict": true, "offset": 6 * MIB, "length": 100, "alignment": 4096,
        });
        assert_eq!(
            domain.error_code("fetchRange", strict).await,
            "versionNoLongerAvailable"
        );

        let lenient = domain
            .ok(
                "fetchRange",
                json!({
                    "op_id": "op-3", "uid": uid(file), "requested_version": old_version,
                    "strict": false, "offset": 6 * MIB, "length": 100, "alignment": 4096,
                }),
            )
            .await;
        assert_ne!(lenient["item"]["content_version"], old_version);
        let clone = std::fs::read(lenient["path"].as_str().unwrap()).unwrap();
        assert_eq!(
            clone.get(6 * MIB..6 * MIB + 100),
            changed.get(6 * MIB..6 * MIB + 100)
        );
    }

    #[tokio::test]
    async fn fetch_range_returns_an_aligned_range_covering_the_request() {
        let domain = Domain::start().await;
        let data = pattern(3 * MIB, 4);
        let file = domain.add("big.bin", &data).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let (offset, length, alignment) = (1_048_000_u64, 10_000_u64, 16_384_u64);

        let range = domain
            .ok(
                "fetchRange",
                json!({
                    "op_id": "op-1", "uid": uid(file), "offset": offset,
                    "length": length, "alignment": alignment,
                }),
            )
            .await;
        let start = range["retrieved_offset"].as_u64().unwrap();
        let end = start + range["retrieved_length"].as_u64().unwrap();
        assert_eq!(start % alignment, 0, "{start}..{end}");
        assert!(
            end % alignment == 0 || end == data.len() as u64,
            "{start}..{end} ends unaligned before the end of the file"
        );
        assert!(start <= offset && end >= offset + length, "{start}..{end}");
        let clone = std::fs::read(range["path"].as_str().unwrap()).unwrap();
        let requested = 1_048_000..1_058_000;
        assert_eq!(clone.get(requested.clone()), data.get(requested));
    }

    #[tokio::test]
    async fn create_of_a_file_with_contents_replies_before_the_upload_and_writes_back() {
        let domain = Domain::start().await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let data = pattern(5000, 6);
        let contents = domain.incoming(&data);

        let created = domain
            .ok(
                "create",
                json!({
                    "op_id": "op-1", "parent_uid": uid(domain.root), "name": "new.bin",
                    "kind": "file", "attributes": {}, "contents_path": contents,
                    "may_already_exist": false,
                }),
            )
            .await;
        std::fs::remove_file(&contents).unwrap();
        let item = &created["item"];
        assert_eq!(item["name"], "new.bin");
        assert_eq!(item["kind"], "file");
        assert_eq!(item["size"], 5000);
        assert_eq!(item["uploaded"], false);
        assert_ne!(domain.share_bytes("new.bin").await, Some(data.clone()));
        let saved_version = item["content_version"].clone();
        let new_uid: u64 = item["uid"].as_str().unwrap().parse().unwrap();

        eventually("the upload of new.bin", WRITE_BACK_DEADLINE, async || {
            domain.share_bytes("new.bin").await == Some(data.clone())
        })
        .await;
        eventually("new.bin reporting uploaded", DEADLINE, async || {
            domain.item(new_uid).await["uploaded"] == true
        })
        .await;
        assert_eq!(domain.item(new_uid).await["content_version"], saved_version);
    }

    #[tokio::test]
    async fn create_with_may_already_exist_takes_the_item_over_and_without_it_collides() {
        let domain = Domain::start().await;
        let existing = domain.add("dup.txt", b"old").await;
        domain.share.add_folder("box").unwrap();
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let folder = domain.resolve("box").await;
        let new_data = pattern(3000, 8);

        let colliding = json!({
            "op_id": "op-1", "parent_uid": uid(domain.root), "name": "dup.txt",
            "kind": "file", "attributes": {}, "contents_path": domain.incoming(&new_data),
            "may_already_exist": false,
        });
        assert_eq!(
            domain.error_code("create", colliding).await,
            "filenameCollision"
        );
        assert_eq!(
            domain
                .error_code(
                    "create",
                    json!({
                        "op_id": "op-2", "parent_uid": uid(domain.root), "name": "box",
                        "kind": "folder", "attributes": {}, "may_already_exist": false,
                    })
                )
                .await,
            "filenameCollision"
        );
        assert_eq!(domain.share_bytes("dup.txt").await, Some(b"old".to_vec()));

        let taken = domain
            .ok(
                "create",
                json!({
                    "op_id": "op-3", "parent_uid": uid(domain.root), "name": "dup.txt",
                    "kind": "file", "attributes": {}, "contents_path": domain.incoming(&new_data),
                    "may_already_exist": true,
                }),
            )
            .await;
        assert_eq!(taken["item"]["uid"], uid(existing));
        let box_again = domain
            .ok(
                "create",
                json!({
                    "op_id": "op-4", "parent_uid": uid(domain.root), "name": "box",
                    "kind": "folder", "attributes": {}, "may_already_exist": true,
                }),
            )
            .await;
        assert_eq!(box_again["item"]["uid"], uid(folder));
        eventually("the upload of dup.txt", WRITE_BACK_DEADLINE, async || {
            domain.share_bytes("dup.txt").await == Some(new_data.clone())
        })
        .await;
    }

    #[tokio::test]
    async fn create_in_a_folder_never_listed_collides_with_a_file_only_the_share_holds() {
        let domain = Domain::start().await;
        domain.share.replace_existing_on_upload();
        domain.share.add_file("unopened/report.txt", b"remote");
        domain.share.add_file("unopened/sub/keep.txt", b"kept");
        let folder = domain.resolve("unopened").await;

        let file = json!({
            "op_id": "op-1", "parent_uid": uid(folder), "name": "report.txt",
            "kind": "file", "attributes": {}, "may_already_exist": false,
        });
        assert_eq!(domain.error_code("create", file).await, "filenameCollision");
        let sub = json!({
            "op_id": "op-2", "parent_uid": uid(folder), "name": "sub",
            "kind": "folder", "attributes": {}, "may_already_exist": false,
        });
        assert_eq!(domain.error_code("create", sub).await, "filenameCollision");
        assert_eq!(
            domain.share_bytes("unopened/report.txt").await,
            Some(b"remote".to_vec())
        );
        assert_eq!(
            domain.share_bytes("unopened/sub/keep.txt").await,
            Some(b"kept".to_vec())
        );
    }

    #[tokio::test]
    async fn modify_with_new_contents_and_a_reparent_moves_the_item_and_writes_back() {
        let domain = Domain::start().await;
        let file = domain.add("report.txt", b"draft").await;
        domain.share.add_folder("sub").unwrap();
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let folder = domain.resolve("sub").await;
        let before = domain.item(file).await;
        let data = pattern(7000, 2);

        let saved = domain.modify_contents("op-1", file, &data).await;
        assert_eq!(saved["item"]["uid"], uid(file));
        assert_eq!(saved["item"]["size"], 7000);
        assert_eq!(saved["item"]["uploaded"], false);
        assert_ne!(saved["item"]["content_version"], before["content_version"]);

        let moved = domain
            .ok(
                "modify",
                json!({
                    "op_id": "op-2", "uid": uid(file), "changed_fields": ["parent_uid"],
                    "parent_uid": uid(folder), "attributes": {},
                }),
            )
            .await;
        assert_eq!(moved["item"]["uid"], uid(file));
        assert_eq!(moved["item"]["parent_uid"], uid(folder));
        assert_eq!(moved["item"]["name"], "report.txt");
        assert!(domain.on_share("sub/report.txt").await);
        assert!(!domain.on_share("report.txt").await);

        eventually(
            "the upload into sub/report.txt",
            WRITE_BACK_DEADLINE,
            async || domain.share_bytes("sub/report.txt").await == Some(data.clone()),
        )
        .await;
        assert!(!domain.on_share("report.txt").await);
    }

    #[tokio::test]
    async fn interleaved_saves_and_an_nfs_write_upload_once_with_the_latest_content() {
        let domain = Domain::start().await;
        let file = domain.add("doc.txt", &pattern(4000, 0)).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;

        domain
            .modify_contents("op-1", file, &pattern(4000, 1))
            .await;
        let second = pattern(4000, 2);
        domain.modify_contents("op-2", file, &second).await;
        domain.cache.write_at(file, 10, b"NFS").await.unwrap();
        let mut latest = second;
        latest.splice(10..13, b"NFS".iter().copied());

        eventually("doc.txt uploaded", WRITE_BACK_DEADLINE, async || {
            domain.item(file).await["uploaded"] == true
        })
        .await;
        assert_eq!(domain.share_bytes("doc.txt").await, Some(latest.clone()));
        let placed: Vec<_> = domain
            .log
            .placed()
            .into_iter()
            .filter(|placed| placed.target.ends_with("doc.txt"))
            .collect();
        assert_eq!(placed.len(), 1, "uploads: {placed:?}");
        assert_eq!(placed[0].data, latest);
    }

    #[tokio::test]
    async fn an_upload_finishing_signals_the_working_set() {
        let domain = Domain::start().await;
        let file = domain.add("note.txt", b"before").await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let mut signals = domain.subscribe().await;
        let data = pattern(2000, 7);
        let saved = domain.modify_contents("op-1", file, &data).await;
        let saved_version = saved["item"]["content_version"].clone();

        let uploaded = tokio::time::timeout(WRITE_BACK_DEADLINE, async {
            loop {
                let frame = read_frame(&mut signals).await;
                if frame["result"] == json!({"signal": "workingSet"})
                    && domain.item(file).await["uploaded"] == true
                {
                    return;
                }
            }
        })
        .await;
        assert!(
            uploaded.is_ok(),
            "no workingSet signal once the upload finished"
        );
        assert_eq!(domain.item(file).await["content_version"], saved_version);
        assert_eq!(domain.share_bytes("note.txt").await, Some(data));
    }

    #[tokio::test]
    async fn delete_of_a_folder_with_children_needs_recursive_and_roots_are_refused() {
        let domain = Domain::start().await;
        domain.share.add_file("dir/a.txt", b"a");
        domain.share.add_folder("empty").unwrap();
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let dir = domain.resolve("dir").await;
        let empty = domain.resolve("empty").await;

        assert_eq!(
            domain
                .error_code(
                    "delete",
                    json!({"op_id": "op-1", "uid": uid(dir), "recursive": false})
                )
                .await,
            "directoryNotEmpty"
        );
        assert!(domain.on_share("dir/a.txt").await);
        assert_eq!(
            domain
                .ok(
                    "delete",
                    json!({"op_id": "op-2", "uid": uid(empty), "recursive": false})
                )
                .await,
            json!({})
        );
        assert!(!domain.on_share("empty").await);
        assert_eq!(
            domain
                .ok(
                    "delete",
                    json!({"op_id": "op-3", "uid": uid(dir), "recursive": true})
                )
                .await,
            json!({})
        );
        assert!(!domain.on_share("dir/a.txt").await);
        assert!(!domain.on_share("dir").await);

        let neutral = domain.hello_root().await;
        for root in [neutral, uid(domain.root)] {
            assert_eq!(
                domain
                    .error_code(
                        "delete",
                        json!({"op_id": "op-4", "uid": root, "recursive": true})
                    )
                    .await,
                "deletionRejected",
                "deleting {root}"
            );
        }
        assert_eq!(domain.item(domain.root).await["uid"], uid(domain.root));
    }

    #[tokio::test]
    async fn set_pinned_marks_the_item_requests_its_download_and_refuses_a_pin_over_budget() {
        let domain = Domain::start().await;
        let a = domain.add("a.bin", &pattern(600 * 1024, 1)).await;
        let b = domain.add("b.bin", &pattern(600 * 1024, 2)).await;
        let huge = domain.add("huge.bin", &pattern(2 * MIB, 3)).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        domain.cache.set_max_size(MIB as u64);
        let mut signals = domain.subscribe().await;

        assert_eq!(
            domain
                .error_code("setPinned", json!({"uids": [uid(huge)], "pinned": true}))
                .await,
            "fileWriteOutOfSpace"
        );
        assert_eq!(domain.item(huge).await["pinned"], false);

        domain
            .ok("setPinned", json!({"uids": [uid(a)], "pinned": true}))
            .await;
        assert_eq!(domain.item(a).await["pinned"], true);
        expect_signal(
            &mut signals,
            &json!({"signal": "requestDownload", "uid": uid(a)}),
        )
        .await;
        let working = domain.all_pages("workingSet", json!({})).await;
        assert!(uids_of(&working).contains(&a.to_string()));

        assert_eq!(
            domain
                .error_code("setPinned", json!({"uids": [uid(b)], "pinned": true}))
                .await,
            "fileWriteOutOfSpace",
            "pinned bytes would reach 1200 KiB above a 1 MiB maximum"
        );
        assert_eq!(domain.item(b).await["pinned"], false);
        assert_eq!(domain.item(a).await["pinned"], true);

        domain
            .ok("setPinned", json!({"uids": [uid(a)], "pinned": false}))
            .await;
        assert_eq!(domain.item(a).await["pinned"], false);
    }

    #[tokio::test]
    async fn evict_of_a_dirty_item_is_refused_and_a_swept_object_goes_once_unmaterialised() {
        let domain = Domain::start().await;
        let dirty = domain.add("dirty.txt", b"dirty").await;
        let clean = domain.add("clean.txt", &pattern(4000, 5)).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;

        domain
            .modify_contents("op-1", dirty, &pattern(3000, 9))
            .await;
        assert_eq!(
            domain.error_code("evict", json!({"uid": uid(dirty)})).await,
            "nonEvictable"
        );
        assert!(!domain.db.content_objects_of(dirty).unwrap().is_empty());

        let mut signals = domain.subscribe().await;
        domain
            .ok("fetch", json!({"op_id": "op-2", "uid": uid(clean)}))
            .await;
        domain
            .ok("materializedSet", json!({"uids": [uid(clean)]}))
            .await;
        domain.cache.set_max_size(0);
        domain.cache.sweep().await.unwrap();
        expect_signal(&mut signals, &json!({"signal": "evict", "uid": uid(clean)})).await;
        assert!(
            !domain.db.content_objects_of(clean).unwrap().is_empty(),
            "the object stays while the item is materialised"
        );
        domain.ok("materializedSet", json!({"uids": []})).await;
        eventually("deletion of the swept object", DEADLINE, async || {
            domain.db.content_objects_of(clean).unwrap().is_empty()
        })
        .await;
        assert!(!domain.db.content_objects_of(dirty).unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancel_interrupts_a_running_fetch_with_user_cancelled() {
        let domain = Domain::start().await;
        let file = domain.add("slow.bin", &pattern(10_000, 1)).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let download = domain.share.stop_at(MockOp::Download);
        let mut fetching = domain.connect().await;
        send_as(
            &mut fetching,
            5,
            "fetch",
            json!({"op_id": "op-c", "uid": uid(file)}),
        )
        .await;
        held(&download).await;

        assert_eq!(
            domain.ok("cancel", json!({"op_id": "op-c"})).await,
            json!({})
        );
        let answer = read_frame(&mut fetching).await;
        download.release();
        assert_eq!(answer["id"], 5);
        assert_eq!(error_code_of("fetch", &answer), "userCancelled");
    }

    #[tokio::test]
    async fn the_progress_stream_of_a_fetch_ends_with_a_finished_frame() {
        let domain = Domain::start().await;
        let data = pattern(3 * MIB, 2);
        let file = domain.add("progress.bin", &data).await;
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let download = domain.share.stop_at(MockOp::Download);
        let mut fetching = domain.connect().await;
        send_as(
            &mut fetching,
            7,
            "fetch",
            json!({"op_id": "op-p", "uid": uid(file)}),
        )
        .await;
        held(&download).await;

        let mut progress = domain.connect().await;
        send_as(&mut progress, 8, "progress", json!({"op_id": "op-p"})).await;
        assert_eq!(
            result_of("progress", read_frame(&mut progress).await),
            json!({})
        );
        download.release();

        let mut frames = Vec::new();
        loop {
            let frame = result_of("progress", read_frame(&mut progress).await);
            let finished = frame["finished"] == true;
            frames.push(frame);
            if finished {
                break;
            }
        }
        let last = frames.last().unwrap();
        assert_eq!(last["op_id"], "op-p");
        assert_eq!(last["done"], data.len());
        assert_eq!(last["total"], data.len());
        let done: Vec<u64> = frames.iter().map(|f| f["done"].as_u64().unwrap()).collect();
        assert!(done.is_sorted(), "progress went backwards: {done:?}");
        assert!(
            frames
                .iter()
                .all(|f| f["done"].as_u64() <= f["total"].as_u64()),
            "{frames:?}"
        );
        let fetched = read_frame(&mut fetching).await;
        assert_eq!(fetched["id"], 7);
        assert!(fetched.get("error").is_none(), "{fetched}");
    }

    #[tokio::test]
    async fn the_neutral_root_lists_one_folder_per_mounted_backend() {
        let domain = Domain::start().await;
        domain.share.add_file("m.txt", b"m");
        let second = domain.mount("second");
        second.add_file("s.txt", b"s");
        let neutral = domain.hello_root().await;

        let root = domain.ok("item", json!({"uid": neutral})).await["item"].clone();
        assert_eq!(root["uid"], neutral);
        assert_eq!(root["kind"], "folder");

        let listed = domain.ok("list", json!({"uid": neutral})).await["items"].clone();
        let mounts = listed.as_array().unwrap();
        let mut names: Vec<&str> = mounts.iter().map(|m| m["name"].as_str().unwrap()).collect();
        names.sort_unstable();
        assert_eq!(names, ["mock", "second"]);
        assert!(
            mounts
                .iter()
                .all(|m| m["kind"] == "folder" && m["parent_uid"] == neutral)
        );
        let first = mounts.iter().find(|m| m["name"] == "mock").unwrap();
        assert_eq!(first["uid"], uid(domain.root));
        let second_root = mounts.iter().find(|m| m["name"] == "second").unwrap()["uid"].clone();
        assert_ne!(second_root, first["uid"]);

        let second_files = domain.ok("list", json!({"uid": second_root})).await;
        assert_eq!(names_of_files(&second_files["items"]), ["s.txt"]);
        let first_files = domain.ok("list", json!({"uid": uid(domain.root)})).await;
        assert_eq!(names_of_files(&first_files["items"]), ["m.txt"]);
        assert_eq!(domain.item(domain.root).await["parent_uid"], neutral);
    }

    #[tokio::test]
    async fn each_error_code_comes_from_its_own_failure_in_an_allowed_domain() {
        let domain = Domain::start().await;
        let mut produced: Vec<String> = Vec::new();
        let mut expect = |code: String, expected: &str| {
            assert_eq!(code, expected);
            produced.push(code);
        };

        expect(
            domain.error_code("hello", json!({"version": 1})).await,
            "olderExtensionVersionRunning",
        );
        expect(
            domain.error_code("hello", json!({"version": 3})).await,
            "newerExtensionVersionFound",
        );
        expect(
            domain.error_code("item", json!({"uid": "999999"})).await,
            "noSuchItem",
        );
        expect(
            domain.error_code("frobnicate", json!({})).await,
            "featureUnsupported",
        );
        expect(
            domain
                .error_code("changes", json!({"anchor": "no anchor"}))
                .await,
            "syncAnchorExpired",
        );
        expect(
            domain
                .error_code("list", json!({"uid": uid(domain.root), "page": "nope"}))
                .await,
            "pageExpired",
        );

        let file = domain.add("file.txt", &pattern(10_000, 1)).await;
        domain.share.add_file("dir/child.txt", b"c");
        domain.ok("list", json!({"uid": uid(domain.root)})).await;
        let dir = domain.resolve("dir").await;
        let folder = |name: &str| {
            json!({
                "op_id": "op-f", "parent_uid": uid(domain.root), "name": name,
                "kind": "folder", "attributes": {}, "may_already_exist": false,
            })
        };
        expect(
            domain.error_code("create", folder("dir")).await,
            "filenameCollision",
        );
        expect(
            domain.error_code("create", folder("a?b")).await,
            "excludedFromSync",
        );
        expect(
            domain
                .error_code(
                    "delete",
                    json!({"op_id": "op-d", "uid": domain.hello_root().await, "recursive": true}),
                )
                .await,
            "deletionRejected",
        );
        expect(
            domain
                .error_code(
                    "delete",
                    json!({"op_id": "op-d", "uid": uid(dir), "recursive": false}),
                )
                .await,
            "directoryNotEmpty",
        );

        let rename = json!({
            "op_id": "op-r", "uid": uid(file), "changed_fields": ["name"],
            "name": "renamed.txt", "parent_uid": uid(domain.root), "attributes": {},
        });
        domain.share.inject(MockOp::Move, Fault::Unreachable);
        expect(
            domain.error_code("modify", rename.clone()).await,
            "serverUnreachable",
        );
        domain.share.inject(MockOp::Move, Fault::Busy);
        expect(
            domain.error_code("modify", rename).await,
            "cannotSynchronize",
        );

        let fetched = domain
            .ok("fetch", json!({"op_id": "op-v", "uid": uid(file)}))
            .await;
        let old_version = fetched["item"]["content_version"].clone();
        domain.share.add_file("file.txt", &pattern(12_000, 2));
        assert!(domain.cache.scan().await.unwrap());
        expect(
            domain
                .error_code(
                    "fetchRange",
                    json!({
                        "op_id": "op-v2", "uid": uid(file), "requested_version": old_version,
                        "strict": true, "offset": 0, "length": 100, "alignment": 4096,
                    }),
                )
                .await,
            "versionNoLongerAvailable",
        );

        domain.cache.set_max_size(1024);
        expect(
            domain
                .error_code("setPinned", json!({"uids": [uid(file)], "pinned": true}))
                .await,
            "fileWriteOutOfSpace",
        );
        domain.cache.set_max_size(u64::MAX);

        domain.modify_contents("op-s", file, b"unsynced").await;
        expect(
            domain.error_code("evict", json!({"uid": uid(file)})).await,
            "nonEvictable",
        );

        let slow = domain.add("slow.bin", &pattern(5000, 4)).await;
        let download = domain.share.stop_at(MockOp::Download);
        let mut fetching = domain.connect().await;
        send_as(
            &mut fetching,
            3,
            "fetch",
            json!({"op_id": "op-c", "uid": uid(slow)}),
        )
        .await;
        held(&download).await;
        domain.ok("cancel", json!({"op_id": "op-c"})).await;
        let cancelled = read_frame(&mut fetching).await;
        download.release();
        expect(error_code_of("fetch", &cancelled), "userCancelled");

        let table: BTreeMap<&str, &str> = ERROR_TABLE.into_iter().collect();
        for code in &produced {
            let parsed: FileProviderErrorCode = serde_json::from_value(json!(code))
                .unwrap_or_else(|error| panic!("{code} is no FileProviderErrorCode: {error}"));
            assert_eq!(parsed.domain(), table[code.as_str()], "domain of {code}");
        }
        let produced: BTreeSet<&str> = produced.iter().map(String::as_str).collect();
        let unproduced: Vec<&str> = table
            .keys()
            .copied()
            .filter(|code| !produced.contains(code))
            .collect();
        assert_eq!(
            unproduced,
            ["notAuthenticated"],
            "codes the server answers that no failure here produced"
        );
    }

    #[tokio::test]
    async fn a_rename_refused_as_unreachable_is_applied_after_a_restart_and_error_resolved_signalled()
     {
        let root = tempfile::tempdir().unwrap();
        let disk = Disk::at(root.path(), TransferConfig::default()).unwrap();
        let socket = root.path().join("fp.sock");
        let share = Arc::new(MockBackend::new(11));
        share.add_file("report.txt", b"report");
        let backend: Arc<dyn Backend> = share.clone();

        let first = disk.start(Arc::clone(&backend)).await.unwrap();
        let (stop, serving) = serve(&socket, first.cache.clone()).await;
        let mount = first.replica().unwrap().root_uid();
        result_of(
            "list",
            call_at(&socket, "list", json!({"uid": uid(mount)})).await,
        );
        let file = first.resolve("report.txt").await.unwrap().unwrap();
        let rename = json!({
            "op_id": "op-1", "uid": uid(file), "changed_fields": ["name"],
            "name": "final.txt", "parent_uid": uid(mount), "attributes": {},
        });
        share.inject(MockOp::Move, Fault::Unreachable);
        assert_eq!(
            error_code_of("modify", &call_at(&socket, "modify", rename.clone()).await),
            "serverUnreachable"
        );
        stop.send_replace(true);
        serving.await.unwrap();
        drop(first);

        let second = disk.start(backend).await.unwrap();
        let (_stop, _serving) = serve(&socket, second.cache.clone()).await;
        let mut signals = subscribe_at(&socket).await;
        let applied = result_of("modify", call_at(&socket, "modify", rename).await);
        assert_eq!(applied["item"]["name"], "final.txt");
        assert!(share.metadata(Path::new("final.txt")).await.is_ok());
        assert!(share.metadata(Path::new("report.txt")).await.is_err());
        expect_signal(&mut signals, &json!({"signal": "errorResolved"})).await;
    }
}
