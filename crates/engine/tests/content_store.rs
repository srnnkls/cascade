#![cfg(feature = "native")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::os::unix::fs::MetadataExt;

use cascade_engine::content::ObjectId;
use cascade_engine::content::range_map::{RangeMap, align};
use cascade_engine::content::store::{CloneOutcome, ObjectStore};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

#[test]
fn range_map_merges_reports_gaps_and_aligns_requests() {
    let mut map = RangeMap::default();
    map.insert(20..30);
    map.insert(0..10);
    map.insert(10..15);
    map.insert(25..40);
    map.insert(50..60);
    map.insert(45..55);

    assert_eq!(map.ranges(), [0..15, 20..40, 45..60]);
    assert_eq!(map.missing(5..70), vec![15..20, 40..45, 60..70]);
    assert!(map.missing(21..39).is_empty());
    assert_eq!(map.missing(100..120), vec![100..120]);

    assert_eq!(align(5000, 100, 4096, MIB), 4096..8192);
    assert_eq!(align(4000, 200, 4096, MIB), 0..8192);
    assert_eq!(align(8192, 4096, 4096, MIB), 8192..12288);
    assert_eq!(align(MIB - 10, 5, 4096, MIB), (MIB - 4096)..MIB);
    assert_eq!(align(1000, 10, 4096, 3000), 0..3000);
}

#[test]
fn object_is_sparse_and_serves_positional_writes() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectStore::open(dir.path().join("objects")).unwrap();

    let path = store.create(ObjectId::new(7), GIB).unwrap();
    let meta = std::fs::metadata(&path).unwrap();
    assert_eq!(meta.len(), GIB);
    assert!(
        meta.blocks() * 512 < 16 * MIB,
        "object of 1 GiB occupies {} bytes before any write",
        meta.blocks() * 512
    );

    let payload: Vec<u8> = (0..64 * KIB)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    store
        .write_at(ObjectId::new(7), 512 * MIB, &payload)
        .unwrap();
    store.write_at(ObjectId::new(7), GIB - 3, b"end").unwrap();

    let mut read = vec![0_u8; payload.len()];
    let n = store
        .read_at(ObjectId::new(7), 512 * MIB, &mut read)
        .unwrap();
    assert_eq!(n, payload.len());
    assert_eq!(read, payload);

    let mut read_tail = [0_u8; 3];
    store
        .read_at(ObjectId::new(7), GIB - 3, &mut read_tail)
        .unwrap();
    assert_eq!(&read_tail, b"end");
    assert_eq!(std::fs::metadata(&path).unwrap().len(), GIB);

    store.delete(ObjectId::new(7)).unwrap();
    assert!(!path.exists());
}

#[cfg(target_os = "macos")]
#[test]
fn clone_handoff_shares_blocks_with_the_object() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectStore::open(dir.path().join("objects")).unwrap();
    let size = 8 * MIB;
    store.create(ObjectId::new(3), size).unwrap();
    let content: Vec<u8> = (0..size).map(|i| u8::try_from(i % 253).unwrap()).collect();
    store.write_at(ObjectId::new(3), 0, &content).unwrap();

    let staged = dir.path().join("staging").join("handoff.bin");
    std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
    let outcome = store.clone_to(ObjectId::new(3), &staged).unwrap();

    assert_eq!(outcome, CloneOutcome::Cloned);
    assert_eq!(std::fs::read(&staged).unwrap(), content);

    store.delete(ObjectId::new(3)).unwrap();
    assert_eq!(std::fs::read(&staged).unwrap(), content);
}
