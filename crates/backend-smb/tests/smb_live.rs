//! Live tests; need `CASCADE_SMB_TEST=host:port/share/user/password` naming a share with `big.bin` (> 4 MiB) and `sub/hello.txt` (`hello from samba\n`).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::path::Path;

use cascade_engine::backend::Backend;
use cascade_engine::types::Change;

fn backend() -> Box<dyn Backend> {
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
    let config = toml::toml! {
        id = "nas"
        host = host
        port = (port.parse::<i64>().unwrap())
        share = share
        username = user
        password = password
    };
    cascade_backend_smb::create_backend(&toml::Value::Table(config)).unwrap()
}

#[tokio::test]
#[ignore = "requires a live SMB share"]
async fn lists_root_and_subdirectory() {
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
