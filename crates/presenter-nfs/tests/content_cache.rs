//! The NFS presenter served from `ContentCache` on the seeded mock backend:
//! ranged reads with read-ahead (FR-4, AC-5 scaled down), write-back of WRITE
//! and SETATTR size with direct REMOVE, RENAME and MKDIR (FR-7), listings
//! through the replica, the write-back cases that involve NFS WRITE, and names
//! SMB cannot hold refused on v3 and v4 (TR-10).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use cascade_engine::backend::{Backend, MountedBackend, NullBackend};
use cascade_engine::content::transfer::TransferConfig;
use cascade_engine::content::writeback::WRITE_BACK_DELAY;
use cascade_engine::testing::{
    BackendLog, Daemon, Disk, Fault, MockBackend, MockOp, RecordingBackend,
};
use cascade_engine::vfs::{NEUTRAL_ROOT_ID, VfsTree};
use cascade_presenter_nfs::nfs::context::NfsContext;
use cascade_presenter_nfs::nfs::procedures::handle_nfs_call;
use cascade_presenter_nfs::nfs::server::NfsCacheMode;
use cascade_presenter_nfs::nfs::v4::compound::handle_compound;
use cascade_presenter_nfs::nfs::v4::state::StateManager;
use cascade_presenter_nfs::nfs::v4::xdr as xdr4;
use cascade_presenter_nfs::nfs::xdr::{
    NFS3_OK, NFS3ERR_INVAL, NFS3ERR_IO, NFS3ERR_NOENT, NFS3ERR_ROFS, NFS3PROC_CREATE,
    NFS3PROC_LOOKUP, NFS3PROC_MKDIR, NFS3PROC_READ, NFS3PROC_READDIR, NFS3PROC_REMOVE,
    NFS3PROC_RENAME, NFS3PROC_SETATTR, NFS3PROC_WRITE, NfsFh3, decode_bool, decode_fh,
    decode_string, decode_u32, decode_u64, encode_bool, encode_fh, encode_opaque, encode_string,
    encode_u32, encode_u64,
};
use tokio::task::JoinHandle;

const MIB: u64 = 1024 * 1024;
/// Bytes of a v3 `fattr3`.
const FATTR3_LEN: usize = 84;
const NFS3_CREATE_UNCHECKED: u32 = 0;
const NFS3_UNSTABLE: u32 = 0;
const REJECTED: [char; 8] = ['?', '*', '<', '>', '"', '\\', '|', ':'];
/// Longer than the write-back delay with room for a slow runner.
const SETTLE: Duration = Duration::from_secs(30);
/// Longer than an upload retried once after a failure, with room for a slow
/// runner.
const SETTLE_AFTER_RETRY: Duration = Duration::from_mins(1);
/// Long enough for an upload that should not happen to have happened.
const QUIET: Duration = WRITE_BACK_DELAY.saturating_add(Duration::from_secs(3));
/// Lets a call issued on another thread reach the point where it waits.
const LET_RUN: Duration = Duration::from_millis(300);
/// Id of a backend the daemon mounts at its default prefix.
const NAS: &str = "nas";

/// A second backend in the VFS tree, beside the root backend.
struct Mount {
    prefix: &'static str,
    id: &'static str,
    registered_in_cache: bool,
}

struct Fixture {
    _root: tempfile::TempDir,
    mock: Arc<MockBackend>,
    other: Arc<MockBackend>,
    log: BackendLog,
    daemon: Daemon,
    ctx: Arc<NfsContext>,
}

impl Fixture {
    async fn new(config: TransferConfig) -> Self {
        Self::build(config, None).await
    }

    async fn mounting(mount: Mount) -> Self {
        Self::build(TransferConfig::default(), Some(mount)).await
    }

    async fn build(config: TransferConfig, mount: Option<Mount>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockBackend::new(11));
        let other = Arc::new(MockBackend::new(13));
        let log = BackendLog::default();
        let backend: Arc<dyn Backend> = RecordingBackend::new(&mock, &log);
        let daemon = Disk::at(root.path(), config)
            .unwrap()
            .start(Arc::clone(&backend))
            .await
            .unwrap();
        let mut tree = VfsTree::new(backend);
        if let Some(mount) = mount {
            let mounted: Arc<dyn Backend> =
                RecordingBackend::named(&other, &BackendLog::default(), mount.id);
            if mount.registered_in_cache {
                daemon.cache.register(Arc::clone(&mounted)).unwrap();
            }
            tree.mount(PathBuf::from(mount.prefix), mounted);
        }
        let vfs = Arc::new(RwLock::new(tree));
        let ctx = Arc::new(NfsContext::new(vfs).with_content_cache(daemon.cache.clone()));
        Self {
            _root: root,
            mock,
            other,
            log,
            daemon,
            ctx,
        }
    }

    /// One backend named `nas` mounted under the neutral root at the prefix
    /// the engine derives for it, the content cache attached when `cached`.
    async fn named_backend(cached: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockBackend::new(11));
        let log = BackendLog::default();
        let backend: Arc<dyn Backend> = RecordingBackend::named(&mock, &log, NAS);
        let daemon = Disk::at(root.path(), TransferConfig::default())
            .unwrap()
            .start(Arc::clone(&backend))
            .await
            .unwrap();
        let mut tree = VfsTree::new(Arc::new(NullBackend::new(NEUTRAL_ROOT_ID)));
        let prefix = MountedBackend::at_default(Arc::clone(&backend)).resolve_prefix();
        tree.mount(prefix, backend);
        let ctx = NfsContext::new(Arc::new(RwLock::new(tree)));
        let ctx = if cached {
            ctx.with_content_cache(daemon.cache.clone())
        } else {
            ctx
        };
        Self {
            _root: root,
            mock,
            other: Arc::new(MockBackend::new(13)),
            log,
            daemon,
            ctx: Arc::new(ctx),
        }
    }

    fn v3(&self, proc: u32, args: &[u8]) -> Vec<u8> {
        self.v3_in(NfsCacheMode::Minimal, proc, args)
    }

    fn v3_in(&self, mode: NfsCacheMode, proc: u32, args: &[u8]) -> Vec<u8> {
        tokio::task::block_in_place(|| handle_nfs_call(proc, args, &self.ctx, mode))
    }

    /// Issues a v3 call on a blocking thread and returns its status, so the
    /// test can release a stop point the call may wait behind.
    fn spawn_v3(&self, proc: u32, args: Vec<u8>) -> JoinHandle<u32> {
        let ctx = Arc::clone(&self.ctx);
        tokio::task::spawn_blocking(move || {
            let reply = handle_nfs_call(proc, &args, &ctx, NfsCacheMode::Minimal);
            decode_u32(&reply).unwrap().0
        })
    }

    fn v4(&self, ops: &[(u32, &[u8])]) -> u32 {
        xdr4::decode_u32(&self.v4_in(NfsCacheMode::Minimal, ops))
            .unwrap()
            .0
    }

    fn v4_in(&self, mode: NfsCacheMode, ops: &[(u32, &[u8])]) -> Vec<u8> {
        let mut args = Vec::new();
        xdr4::encode_string(&mut args, "");
        xdr4::encode_u32(&mut args, 0);
        xdr4::encode_u32(&mut args, u32::try_from(ops.len()).unwrap());
        for &(opnum, op_args) in ops {
            xdr4::encode_u32(&mut args, opnum);
            args.extend_from_slice(op_args);
        }
        let state = Arc::new(StateManager::new());
        tokio::task::block_in_place(|| handle_compound(&args, &self.ctx, &state, mode))
    }

    /// A v4 COMPOUND of PUTROOTFH, a LOOKUP per segment of `dir` and `op`;
    /// returns the overall status and the reply.
    fn v4_in_dir(&self, mode: NfsCacheMode, dir: &str, op: (u32, &[u8])) -> (u32, Vec<u8>) {
        let lookups: Vec<Vec<u8>> = dir
            .split('/')
            .map(|name| {
                let mut args = Vec::new();
                xdr4::encode_string(&mut args, name);
                args
            })
            .collect();
        let mut ops: Vec<(u32, &[u8])> = vec![(xdr4::OP_PUTROOTFH, &[])];
        ops.extend(
            lookups
                .iter()
                .map(|args| (xdr4::OP_LOOKUP, args.as_slice())),
        );
        ops.push(op);
        let reply = self.v4_in(mode, &ops);
        (xdr4::decode_u32(&reply).unwrap().0, reply)
    }

    fn root_fh(&self) -> NfsFh3 {
        NfsFh3::from_item_id(&self.ctx.root_key().to_string())
    }

    fn lookup(&self, dir: &NfsFh3, name: &str) -> (u32, Option<NfsFh3>) {
        let reply = self.v3(NFS3PROC_LOOKUP, &dirop(dir, name));
        let (status, rest) = decode_u32(&reply).unwrap();
        let fh = (status == NFS3_OK).then(|| decode_fh(rest).unwrap().0);
        (status, fh)
    }

    /// Walks `path` from the export root by LOOKUP, as a client that never
    /// listed a folder on the way does.
    fn walk(&self, path: &str) -> NfsFh3 {
        path.split('/').fold(self.root_fh(), |dir, name| {
            let (status, fh) = self.lookup(&dir, name);
            assert_eq!(status, NFS3_OK, "LOOKUP {name} of {path}");
            fh.unwrap()
        })
    }

    /// READ of `count` bytes, a multiple of four, at `offset`.
    fn read(&self, fh: &NfsFh3, offset: u64, count: u32) -> Vec<u8> {
        let mut args = Vec::new();
        encode_fh(&mut args, fh);
        encode_u64(&mut args, offset);
        encode_u32(&mut args, count);
        let reply = self.v3(NFS3PROC_READ, &args);
        let (status, rest) = decode_u32(&reply).unwrap();
        assert_eq!(status, NFS3_OK, "READ at {offset}");
        let (follows, rest) = decode_bool(rest).unwrap();
        let rest = if follows { &rest[FATTR3_LEN..] } else { rest };
        let (returned, _) = decode_u32(rest).unwrap();
        let returned = usize::try_from(returned).unwrap();
        reply[reply.len() - returned..].to_vec()
    }

    fn write(&self, fh: &NfsFh3, offset: u64, data: &[u8]) -> u32 {
        decode_u32(&self.v3(NFS3PROC_WRITE, &write_args(fh, offset, data)))
            .unwrap()
            .0
    }

    fn set_size(&self, fh: &NfsFh3, size: u64) -> u32 {
        let mut args = Vec::new();
        encode_fh(&mut args, fh);
        for _ in 0..3 {
            encode_bool(&mut args, false);
        }
        encode_bool(&mut args, true);
        encode_u64(&mut args, size);
        encode_u32(&mut args, 0);
        encode_u32(&mut args, 0);
        encode_bool(&mut args, false);
        decode_u32(&self.v3(NFS3PROC_SETATTR, &args)).unwrap().0
    }

    fn create(&self, dir: &NfsFh3, name: &str) -> u32 {
        let mut args = dirop(dir, name);
        encode_u32(&mut args, NFS3_CREATE_UNCHECKED);
        for _ in 0..4 {
            encode_bool(&mut args, false);
        }
        encode_u32(&mut args, 0);
        encode_u32(&mut args, 0);
        decode_u32(&self.v3(NFS3PROC_CREATE, &args)).unwrap().0
    }

    fn rename(&self, from: &NfsFh3, from_name: &str, to: &NfsFh3, to_name: &str) -> u32 {
        let args = rename_args(from, from_name, to, to_name);
        decode_u32(&self.v3(NFS3PROC_RENAME, &args)).unwrap().0
    }

    fn remove(&self, dir: &NfsFh3, name: &str) -> u32 {
        decode_u32(&self.v3(NFS3PROC_REMOVE, &dirop(dir, name)))
            .unwrap()
            .0
    }

    fn mkdir(&self, dir: &NfsFh3, name: &str) -> u32 {
        let mut args = dirop(dir, name);
        for _ in 0..4 {
            encode_bool(&mut args, false);
        }
        encode_u32(&mut args, 0);
        encode_u32(&mut args, 0);
        decode_u32(&self.v3(NFS3PROC_MKDIR, &args)).unwrap().0
    }

    /// READDIR from the first entry; returns the status and the sorted names.
    fn readdir(&self, dir: &NfsFh3) -> (u32, Vec<String>) {
        let mut args = Vec::new();
        encode_fh(&mut args, dir);
        encode_u64(&mut args, 0);
        encode_u64(&mut args, 0);
        encode_u32(&mut args, 64 * 1024);
        let reply = self.v3(NFS3PROC_READDIR, &args);
        let (status, rest) = decode_u32(&reply).unwrap();
        if status != NFS3_OK {
            return (status, Vec::new());
        }
        let (follows, rest) = decode_bool(rest).unwrap();
        let rest = if follows { &rest[FATTR3_LEN..] } else { rest };
        let mut rest = &rest[8..];
        let mut names = Vec::new();
        loop {
            let (entry_follows, after) = decode_bool(rest).unwrap();
            if !entry_follows {
                break;
            }
            let (_fileid, after) = decode_u64(after).unwrap();
            let (name, after) = decode_string(after).unwrap();
            let (_cookie, after) = decode_u64(after).unwrap();
            names.push(name);
            rest = after;
        }
        names.sort();
        (status, names)
    }

    /// Content at `path` on the backend, read past any injected fault.
    fn on_backend(&self, path: &str) -> Option<Vec<u8>> {
        self.mock.content(Path::new(path))
    }

    /// Names in a folder of the backend, temporary uploads included.
    async fn names_on_backend(&self, folder: &str) -> Vec<String> {
        let folder = self.mock.metadata(Path::new(folder)).await.unwrap();
        let mut names: Vec<String> = self
            .mock
            .list_children(folder.id.native_id())
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.name)
            .collect();
        names.sort();
        names
    }

    fn placed(&self) -> Vec<Vec<u8>> {
        self.log
            .placed()
            .into_iter()
            .map(|placed| placed.data)
            .collect()
    }

    /// Saves `data` over the item at `path` as File Provider's `modify` does.
    async fn save(&self, path: &str, data: &[u8]) {
        let uid = self.daemon.resolve(path).await.unwrap().unwrap();
        let incoming = self.daemon.incoming_file(data).unwrap();
        self.daemon.cache.save(uid, &incoming).await.unwrap();
        std::fs::remove_file(&incoming).unwrap();
    }

    fn uploaded(&self) -> usize {
        [MockOp::Upload, MockOp::Update, MockOp::Replace]
            .into_iter()
            .map(|op| self.log.calls(op))
            .sum()
    }
}

fn dirop(dir: &NfsFh3, name: &str) -> Vec<u8> {
    let mut args = Vec::new();
    encode_fh(&mut args, dir);
    encode_string(&mut args, name);
    args
}

fn rename_args(from: &NfsFh3, from_name: &str, to: &NfsFh3, to_name: &str) -> Vec<u8> {
    let mut args = dirop(from, from_name);
    args.extend_from_slice(&dirop(to, to_name));
    args
}

fn write_args(fh: &NfsFh3, offset: u64, data: &[u8]) -> Vec<u8> {
    let mut args = Vec::new();
    encode_fh(&mut args, fh);
    encode_u64(&mut args, offset);
    encode_u32(&mut args, u32::try_from(data.len()).unwrap());
    encode_u32(&mut args, NFS3_UNSTABLE);
    encode_opaque(&mut args, data);
    args
}

fn pattern(len: u64) -> Vec<u8> {
    (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect()
}

/// Same length as [`pattern`], different bytes at every offset.
fn other_pattern(len: u64) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from((i % 251 + 100) % 256).unwrap())
        .collect()
}

fn edited(data: &[u8], offset: u64, edit: &[u8]) -> Vec<u8> {
    let mut data = data.to_vec();
    let start = usize::try_from(offset).unwrap();
    data[start..start + edit.len()].copy_from_slice(edit);
    data
}

fn anonymous_stateid(buf: &mut Vec<u8>) {
    xdr4::encode_stateid(buf, &xdr4::StateId { data: [0; 16] });
}

async fn reached_in_time(stop: &cascade_engine::testing::StopPoint, what: &str) {
    tokio::time::timeout(SETTLE, stop.reached())
        .await
        .unwrap_or_else(|_| panic!("{what} never started"));
}

fn fetched_bytes(ranges: &[Range<u64>]) -> u64 {
    ranges.iter().map(|range| range.end - range.start).sum()
}

fn covers(ranges: &[Range<u64>], wanted: &Range<u64>) -> bool {
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|range| range.start);
    let mut reached = wanted.start;
    for range in sorted {
        if range.start <= reached {
            reached = reached.max(range.end);
        }
    }
    reached >= wanted.end
}

async fn eventually(what: &str, done: impl FnMut() -> bool) {
    eventually_within(SETTLE, what, done).await;
}

async fn eventually_within(limit: Duration, what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn read_at_a_high_offset_of_an_unlisted_folder_fetches_the_window_plus_read_ahead() {
    const SIZE: u64 = 32 * MIB;
    const OFFSET: u64 = 24 * MIB;
    const LEN: u32 = 1024 * 1024;
    const READ_AHEAD: u64 = 2 * MIB;
    let fixture = Fixture::new(TransferConfig {
        read_ahead: READ_AHEAD,
        request_size: MIB,
        ..TransferConfig::default()
    })
    .await;
    let data = pattern(SIZE);
    fixture.mock.add_file("media/2026/big.bin", &data);

    let fh = fixture.walk("media/2026/big.bin");
    let bytes = fixture.read(&fh, OFFSET, LEN);

    let start = usize::try_from(OFFSET).unwrap();
    assert_eq!(bytes, data[start..start + 1024 * 1024]);
    let window = OFFSET..OFFSET + u64::from(LEN) + READ_AHEAD;
    eventually("the read-ahead window after the read is fetched", || {
        covers(&fixture.log.fetched(), &window)
    })
    .await;
    let fetched = fixture.log.fetched();
    assert!(
        fetched
            .iter()
            .all(|range| range.start >= OFFSET - MIB && range.end <= window.end + MIB),
        "fetched outside the read and its read-ahead: {fetched:?}"
    );
    assert!(
        fetched_bytes(&fetched) <= u64::from(LEN) + READ_AHEAD + MIB,
        "fetched {} bytes for a {LEN}-byte read: {fetched:?}",
        fetched_bytes(&fetched)
    );

    let again = fixture.read(&fh, OFFSET, LEN);

    assert_eq!(again, bytes);
    assert_eq!(
        fixture.log.fetched(),
        fetched,
        "a read of cached bytes went to the backend"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_and_set_size_reach_the_backend_by_write_back_while_rename_and_remove_apply_directly()
{
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let docs = fixture.walk("docs");
    let report = fixture.walk("docs/report.txt");

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);

    assert_eq!(fixture.uploaded(), 0, "WRITE uploaded before its reply");
    assert_eq!(fixture.read(&report, 0, 8), b"orXYinal");
    assert_eq!(fixture.on_backend("docs/report.txt").unwrap(), b"original");

    assert_eq!(fixture.set_size(&report, 4), NFS3_OK);

    assert_eq!(
        fixture.uploaded(),
        0,
        "SETATTR size uploaded before its reply"
    );
    assert_eq!(fixture.read(&report, 0, 4), b"orXY");
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    let placed: Vec<Vec<u8>> = fixture
        .log
        .placed()
        .into_iter()
        .map(|placed| placed.data)
        .collect();
    assert_eq!(placed, [b"orXY".to_vec()]);
    assert_eq!(fixture.log.calls(MockOp::Update), 0);
    assert_eq!(fixture.on_backend("docs/report.txt").unwrap(), b"orXY");

    assert_eq!(
        fixture.rename(&docs, "report.txt", &docs, "final.txt"),
        NFS3_OK
    );

    assert_eq!(fixture.on_backend("docs/final.txt").unwrap(), b"orXY");
    assert_eq!(fixture.on_backend("docs/report.txt"), None);

    assert_eq!(fixture.remove(&docs, "other.txt"), NFS3_OK);

    assert_eq!(fixture.on_backend("docs/other.txt"), None);
    assert_eq!(fixture.lookup(&docs, "other.txt").0, NFS3ERR_NOENT);
    assert_eq!(fixture.log.calls(MockOp::Delete), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn names_smb_cannot_hold_are_refused_on_create_and_rename_before_anything_is_applied() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let root = fixture.root_fh();
    let docs = fixture.walk("docs");
    let mut v4_docs = Vec::new();
    xdr4::encode_string(&mut v4_docs, "docs");

    for c in REJECTED {
        let name = format!("bad{c}name");

        assert_eq!(
            fixture.create(&root, &name),
            NFS3ERR_INVAL,
            "v3 CREATE {name}"
        );
        assert_eq!(
            fixture.rename(&docs, "report.txt", &docs, &name),
            NFS3ERR_INVAL,
            "v3 RENAME to {name}"
        );

        let mut create = Vec::new();
        xdr4::encode_u32(&mut create, xdr4::NF4DIR);
        xdr4::encode_string(&mut create, &name);
        xdr4::encode_attr_bitmap(&mut create, &[]);
        assert_eq!(
            fixture.v4(&[(xdr4::OP_PUTROOTFH, &[]), (xdr4::OP_CREATE, &create)]),
            xdr4::NFS4ERR_INVAL,
            "v4 CREATE {name}"
        );
        let mut rename = Vec::new();
        xdr4::encode_string(&mut rename, "report.txt");
        xdr4::encode_string(&mut rename, &name);
        assert_eq!(
            fixture.v4(&[
                (xdr4::OP_PUTROOTFH, &[]),
                (xdr4::OP_LOOKUP, &v4_docs),
                (xdr4::OP_SAVEFH, &[]),
                (xdr4::OP_RENAME, &rename),
            ]),
            xdr4::NFS4ERR_INVAL,
            "v4 RENAME to {name}"
        );

        assert_eq!(fixture.on_backend(&name), None);
        assert_eq!(fixture.on_backend(&format!("docs/{name}")), None);
    }

    assert_eq!(fixture.uploaded(), 0);
    assert_eq!(fixture.log.calls(MockOp::CreateDir), 0);
    assert_eq!(fixture.log.calls(MockOp::Move), 0);
    assert_eq!(fixture.on_backend("docs/report.txt").unwrap(), b"original");
    assert_eq!(fixture.lookup(&docs, "report.txt").0, NFS3_OK);
}

const PARTIAL_SIZE: u64 = 4 * MIB;
const PARTIAL_EDIT_AT: u64 = 3 * MIB;

/// A file of [`PARTIAL_SIZE`] whose first bytes only are cached.
async fn partially_cached(fixture: &Fixture, data: &[u8]) -> NfsFh3 {
    fixture.mock.add_file("media/big.bin", data);
    let fh = fixture.walk("media/big.bin");
    assert_eq!(fixture.read(&fh, 0, 4096), data[..4096]);
    eventually("the first read and its read-ahead are fetched", || {
        covers(&fixture.log.fetched(), &(0..MIB))
    })
    .await;
    assert!(
        !covers(&fixture.log.fetched(), &(0..PARTIAL_SIZE)),
        "precondition: the object is partial, fetched {:?}",
        fixture.log.fetched()
    );
    fh
}

fn partial_config() -> TransferConfig {
    TransferConfig {
        read_ahead: MIB,
        request_size: MIB,
        ..TransferConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_into_a_partial_object_replies_before_the_missing_ranges_are_fetched_and_uploads_one_version_despite_a_later_remote_change()
 {
    let fixture = Fixture::new(partial_config()).await;
    let original = pattern(PARTIAL_SIZE);
    let changed = other_pattern(PARTIAL_SIZE);
    let fh = partially_cached(&fixture, &original).await;

    assert_eq!(fixture.write(&fh, PARTIAL_EDIT_AT, b"XY"), NFS3_OK);

    assert!(
        !covers(&fixture.log.fetched(), &(0..PARTIAL_SIZE)),
        "WRITE waited for the missing ranges: {:?}",
        fixture.log.fetched()
    );
    fixture
        .mock
        .rewrite_keeping_size_and_mtime("media/big.bin", &changed)
        .unwrap();
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    tokio::time::sleep(QUIET).await;
    let placed = fixture.placed();
    assert_eq!(placed.len(), 1, "uploads: {}", placed.len());
    let on_original = edited(&original, PARTIAL_EDIT_AT, b"XY");
    let on_changed = edited(&changed, PARTIAL_EDIT_AT, b"XY");
    assert!(
        placed[0] == on_original || placed[0] == on_changed,
        "the upload mixes two remote versions"
    );
    assert!(fixture.on_backend("media/big.bin").unwrap() == placed[0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_into_a_partial_object_after_a_remote_change_uploads_one_version_never_a_mix() {
    let fixture = Fixture::new(partial_config()).await;
    let original = pattern(PARTIAL_SIZE);
    let changed = other_pattern(PARTIAL_SIZE);
    let fh = partially_cached(&fixture, &original).await;
    fixture
        .mock
        .rewrite_keeping_size_and_mtime("media/big.bin", &changed)
        .unwrap();

    assert_eq!(fixture.write(&fh, PARTIAL_EDIT_AT, b"XY"), NFS3_OK);

    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    let placed = fixture.placed();
    assert_eq!(placed.len(), 1);
    let on_original = edited(&original, PARTIAL_EDIT_AT, b"XY");
    let on_changed = edited(&changed, PARTIAL_EDIT_AT, b"XY");
    let from_original = placed[0]
        .iter()
        .zip(&on_original)
        .filter(|(a, b)| a == b)
        .count();
    assert!(
        placed[0] == on_original || placed[0] == on_changed,
        "mixed upload: {from_original} of {PARTIAL_SIZE} bytes from the old version"
    );
    let len = u32::try_from(PARTIAL_SIZE).unwrap();
    assert!(
        fixture.read(&fh, 0, len) == placed[0],
        "READ after the WRITE disagrees with the uploaded content"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_through_nfs_while_the_upload_runs_places_the_content_under_the_new_name() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let docs = fixture.walk("docs");
    let report = fixture.walk("docs/report.txt");
    let uploading = fixture.mock.stop_at(MockOp::Upload);

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);
    reached_in_time(&uploading, "the upload").await;
    let renaming = fixture.spawn_v3(
        NFS3PROC_RENAME,
        rename_args(&docs, "report.txt", &docs, "final.txt"),
    );
    tokio::time::sleep(LET_RUN).await;
    uploading.release();

    assert_eq!(renaming.await.unwrap(), NFS3_OK);
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    tokio::time::sleep(QUIET).await;
    let placed = fixture.log.placed();
    assert_eq!(placed.len(), 1, "{placed:?}");
    assert_eq!(placed[0].target.file_name().unwrap(), "final.txt");
    assert_eq!(fixture.on_backend("docs/final.txt").unwrap(), b"orXYinal");
    assert_eq!(fixture.names_on_backend("docs").await, ["final.txt"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_through_nfs_of_a_dirty_item_uploads_once_under_the_new_name() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let docs = fixture.walk("docs");
    let report = fixture.walk("docs/report.txt");

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);
    assert_eq!(
        fixture.rename(&docs, "report.txt", &docs, "final.txt"),
        NFS3_OK
    );

    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"orXYinal".to_vec()]);
    assert_eq!(fixture.on_backend("docs/final.txt").unwrap(), b"orXYinal");
    assert_eq!(fixture.names_on_backend("docs").await, ["final.txt"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_during_the_upload_of_an_earlier_write_leaves_one_upload_of_the_latest_content() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let report = fixture.walk("docs/report.txt");
    let uploading = fixture.mock.stop_at(MockOp::Upload);

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);
    reached_in_time(&uploading, "the upload").await;
    let writing = fixture.spawn_v3(NFS3PROC_WRITE, write_args(&report, 4, b"QQ"));
    tokio::time::sleep(LET_RUN).await;
    uploading.release();

    assert_eq!(writing.await.unwrap(), NFS3_OK);
    assert_eq!(fixture.read(&report, 0, 8), b"orXYQQal");
    eventually("write-back places the latest content", || {
        fixture
            .log
            .placed()
            .iter()
            .any(|placed| placed.data == b"orXYQQal")
    })
    .await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"orXYQQal".to_vec()]);
    assert_eq!(fixture.names_on_backend("docs").await, ["report.txt"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scan_seeing_a_remote_change_after_a_write_keeps_and_uploads_the_local_edit() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let docs = fixture.walk("docs");
    assert_eq!(fixture.readdir(&docs).0, NFS3_OK);
    let report = fixture.walk("docs/report.txt");

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);
    fixture
        .mock
        .rewrite_keeping_size_and_mtime("docs/report.txt", b"REMOTE!!")
        .unwrap();
    fixture.daemon.cache.scan().await.unwrap();

    let read = fixture.read(&report, 0, 8);
    assert!(
        read == b"orXYinal" || read == b"REXYTE!!",
        "READ mixes two remote versions or lost the edit: {read:?}"
    );
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), std::slice::from_ref(&read));
    assert_eq!(fixture.on_backend("docs/report.txt").unwrap(), read);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_remove_through_nfs_of_a_dirty_item_cancels_its_upload() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let docs = fixture.walk("docs");
    let report = fixture.walk("docs/report.txt");

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);
    assert_eq!(fixture.remove(&docs, "report.txt"), NFS3_OK);

    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.names_on_backend("docs").await, ["other.txt"]);
    assert!(
        fixture.log.placed().is_empty(),
        "{:?}",
        fixture.log.placed()
    );
    assert_eq!(fixture.lookup(&docs, "report.txt").0, NFS3ERR_NOENT);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_remove_through_nfs_while_the_upload_runs_leaves_neither_the_file_nor_its_temporary_upload()
 {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let docs = fixture.walk("docs");
    let report = fixture.walk("docs/report.txt");
    let uploading = fixture.mock.stop_at(MockOp::Upload);

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);
    reached_in_time(&uploading, "the upload").await;
    let removing = fixture.spawn_v3(NFS3PROC_REMOVE, dirop(&docs, "report.txt"));
    tokio::time::sleep(LET_RUN).await;
    uploading.release();

    assert_eq!(removing.await.unwrap(), NFS3_OK);
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.names_on_backend("docs").await, ["other.txt"]);
    assert!(
        fixture.log.placed().is_empty(),
        "{:?}",
        fixture.log.placed()
    );
    assert_eq!(fixture.lookup(&docs, "report.txt").0, NFS3ERR_NOENT);
}

#[tokio::test(flavor = "multi_thread")]
async fn interleaved_writes_and_saves_upload_once_with_the_latest_content() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let report = fixture.walk("docs/report.txt");

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);
    fixture.save("docs/report.txt", b"fresh content").await;
    assert_eq!(fixture.write(&report, 0, b"Z"), NFS3_OK);

    assert_eq!(
        fixture.uploaded(),
        0,
        "uploaded before the write-back delay"
    );
    assert_eq!(fixture.read(&report, 0, 16), b"Zresh content");
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"Zresh content".to_vec()]);
    assert_eq!(
        fixture.on_backend("docs/report.txt").unwrap(),
        b"Zresh content"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rename_remove_and_mkdir_with_the_remote_unreachable_answer_io_and_apply_nothing() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.mock.add_file("docs/other.txt", b"other!!!");
    let docs = fixture.walk("docs");

    fixture.mock.inject(MockOp::Move, Fault::Unreachable);
    assert_eq!(
        fixture.rename(&docs, "report.txt", &docs, "final.txt"),
        NFS3ERR_IO
    );
    fixture.mock.inject(MockOp::Delete, Fault::Unreachable);
    assert_eq!(fixture.remove(&docs, "other.txt"), NFS3ERR_IO);
    fixture.mock.inject(MockOp::CreateDir, Fault::Unreachable);
    assert_eq!(fixture.mkdir(&docs, "new"), NFS3ERR_IO);

    assert_eq!(
        fixture.names_on_backend("docs").await,
        ["other.txt", "report.txt"]
    );
    assert_eq!(fixture.lookup(&docs, "report.txt").0, NFS3_OK);
    assert_eq!(fixture.lookup(&docs, "other.txt").0, NFS3_OK);
    assert_eq!(fixture.lookup(&docs, "final.txt").0, NFS3ERR_NOENT);
    assert_eq!(fixture.lookup(&docs, "new").0, NFS3ERR_NOENT);

    assert_eq!(
        fixture.rename(&docs, "report.txt", &docs, "final.txt"),
        NFS3_OK,
        "the reissued RENAME"
    );
    assert_eq!(
        fixture.remove(&docs, "other.txt"),
        NFS3_OK,
        "the reissued REMOVE"
    );
    assert_eq!(fixture.mkdir(&docs, "new"), NFS3_OK, "the reissued MKDIR");
    assert_eq!(fixture.names_on_backend("docs").await, ["final.txt", "new"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_while_the_remote_is_unreachable_is_accepted_and_uploaded_after_it_returns() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let report = fixture.walk("docs/report.txt");
    assert_eq!(fixture.read(&report, 0, 8), b"original");
    eventually("the whole file is cached", || {
        covers(&fixture.log.fetched(), &(0..8))
    })
    .await;
    for op in [MockOp::Download, MockOp::Upload] {
        fixture.mock.inject(op, Fault::Unreachable);
    }

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);

    assert_eq!(fixture.read(&report, 0, 8), b"orXYinal");
    eventually_within(SETTLE_AFTER_RETRY, "write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    assert_eq!(fixture.placed(), [b"orXYinal".to_vec()]);
    assert_eq!(fixture.on_backend("docs/report.txt").unwrap(), b"orXYinal");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_small_write_into_a_large_uncached_file_replies_without_downloading_it() {
    const SIZE: u64 = 16 * MIB;
    const AT: u64 = 12 * MIB;
    let fixture = Fixture::new(partial_config()).await;
    let data = pattern(SIZE);
    fixture.mock.add_file("media/large.bin", &data);
    let fh = fixture.walk("media/large.bin");
    let edit = other_pattern(4096);

    assert_eq!(fixture.write(&fh, AT, &edit), NFS3_OK);

    assert_eq!(fixture.log.fetched(), [], "WRITE fetched file content");
    assert_eq!(fixture.log.calls(MockOp::Download), 0);
    assert_eq!(fixture.read(&fh, AT, 4096), edit);
    assert_eq!(
        fixture.log.fetched(),
        [],
        "a READ of written bytes went to the backend"
    );
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    let expected = edited(&data, AT, &edit);
    assert_eq!(fixture.placed().len(), 1);
    assert!(fixture.placed()[0] == expected, "uploaded content");
    assert!(fixture.on_backend("media/large.bin").unwrap() == expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn sequential_writes_mint_one_content_version_and_upload_once() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let report = fixture.walk("docs/report.txt");
    let uid = fixture
        .daemon
        .resolve("docs/report.txt")
        .await
        .unwrap()
        .unwrap();
    let before = fixture.daemon.cache.item(uid).unwrap().unwrap();
    let revision = fixture.daemon.db.replica_meta().unwrap().revision;

    for (offset, chunk) in [(0, b"OR"), (2, b"IG"), (4, b"IN"), (6, b"AL"), (8, b"!!")] {
        assert_eq!(fixture.write(&report, offset, chunk), NFS3_OK);
        let item = fixture.daemon.cache.item(uid).unwrap().unwrap();
        assert_eq!(
            item.content_version, before.content_version,
            "a WRITE minted a content version"
        );
        assert_eq!(item.size, Some(8.max(offset + 2)));
    }
    assert_eq!(
        fixture.daemon.db.replica_meta().unwrap().revision,
        revision,
        "a WRITE advanced the replica revision"
    );

    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.placed(), [b"ORIGINAL!!".to_vec()]);
    let after = fixture.daemon.cache.item(uid).unwrap().unwrap();
    assert_ne!(after.content_version, before.content_version);
    assert!(after.uploaded);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_into_an_uncached_file_while_the_remote_is_unreachable_is_accepted_and_uploaded_after_it_returns()
 {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let report = fixture.walk("docs/report.txt");
    for op in [MockOp::Metadata, MockOp::Download] {
        fixture.mock.inject(op, Fault::Unreachable);
    }

    assert_eq!(fixture.write(&report, 2, b"XY"), NFS3_OK);

    assert_eq!(fixture.uploaded(), 0);
    eventually_within(SETTLE_AFTER_RETRY, "write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    assert_eq!(fixture.placed(), [b"orXYinal".to_vec()]);
    assert_eq!(fixture.on_backend("docs/report.txt").unwrap(), b"orXYinal");
    assert_eq!(fixture.read(&report, 0, 8), b"orXYinal");
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_lists_a_never_listed_folder_through_the_replica_without_temporary_uploads() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/a.txt", b"a");
    fixture.mock.add_file("docs/b.txt", b"b");
    fixture
        .mock
        .add_file("docs/.cascade-tmp-42", b"partial upload");
    fixture.mock.add_file("docs/sub/c.txt", b"c");
    let docs = fixture.walk("docs");

    let (status, names) = fixture.readdir(&docs);

    assert_eq!(status, NFS3_OK);
    assert_eq!(names, ["a.txt", "b.txt", "sub"]);
    assert_eq!(fixture.lookup(&docs, ".cascade-tmp-42").0, NFS3ERR_NOENT);
    assert_eq!(fixture.lookup(&docs, "missing.txt").0, NFS3ERR_NOENT);

    fixture.mock.add_file("docs/d.txt", b"d");
    fixture
        .mock
        .add_file("docs/.cascade-tmp-43", b"partial upload");
    assert!(
        fixture.daemon.cache.scan().await.unwrap(),
        "the listed folder is not in the scan set"
    );

    assert_eq!(fixture.readdir(&docs).1, ["a.txt", "b.txt", "d.txt", "sub"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_of_a_folder_holding_a_nested_mount_lists_the_mount() {
    let fixture = Fixture::mounting(Mount {
        prefix: "/docs/projects",
        id: "projects",
        registered_in_cache: true,
    })
    .await;
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.other.add_file("plan.txt", b"the plan");
    let docs = fixture.walk("docs");

    let (status, names) = fixture.readdir(&docs);

    assert_eq!(status, NFS3_OK);
    assert_eq!(names, ["projects", "report.txt"]);
    let plan = fixture.walk("docs/projects/plan.txt");
    assert_eq!(fixture.read(&plan, 0, 8), b"the plan");
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_of_a_cached_folder_keeps_a_backend_entry_named_like_a_nested_mount() {
    let fixture = Fixture::mounting(Mount {
        prefix: "/docs/projects",
        id: "projects",
        registered_in_cache: true,
    })
    .await;
    fixture.mock.add_file("docs/projects", b"a file");
    fixture.mock.add_file("docs/report.txt", b"original");
    fixture.walk("docs");

    let entries = tokio::task::block_in_place(|| fixture.ctx.list_dir_sync("/docs")).unwrap();

    let listed: Vec<(String, bool)> = entries
        .into_iter()
        .map(|entry| (entry.name, entry.is_dir))
        .collect();
    assert_eq!(
        listed,
        [
            ("projects".to_owned(), false),
            ("report.txt".to_owned(), false)
        ],
        "a cached listing applies the shadow rule of merge_listing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_on_a_backend_the_cache_does_not_serve_is_served_by_that_backend() {
    let fixture = Fixture::mounting(Mount {
        prefix: "/archive",
        id: "archive",
        registered_in_cache: false,
    })
    .await;
    fixture.other.add_file("2025/notes.txt", b"old notes!!!");
    let year = fixture.walk("archive/2025");

    assert_eq!(
        fixture.readdir(&year),
        (NFS3_OK, vec!["notes.txt".to_owned()])
    );
    let notes = fixture.walk("archive/2025/notes.txt");
    assert_eq!(fixture.read(&notes, 0, 12), b"old notes!!!");
    assert_eq!(fixture.log.fetched(), [], "the cached backend was asked");
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_mode_off_refuses_writes_with_rofs_although_a_content_cache_is_attached() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let docs = fixture.walk("docs");
    let report = fixture.walk("docs/report.txt");

    let v3 = |proc, args: &[u8]| {
        decode_u32(&fixture.v3_in(NfsCacheMode::Off, proc, args))
            .unwrap()
            .0
    };
    assert_eq!(
        v3(NFS3PROC_WRITE, &write_args(&report, 2, b"XY")),
        NFS3ERR_ROFS
    );
    let mut create = dirop(&docs, "new.txt");
    encode_u32(&mut create, NFS3_CREATE_UNCHECKED);
    for _ in 0..4 {
        encode_bool(&mut create, false);
    }
    encode_u32(&mut create, 0);
    encode_u32(&mut create, 0);
    assert_eq!(v3(NFS3PROC_CREATE, &create), NFS3ERR_ROFS);
    let mut write = Vec::new();
    anonymous_stateid(&mut write);
    xdr4::encode_u64(&mut write, 2);
    xdr4::encode_u32(&mut write, 0);
    xdr4::encode_opaque(&mut write, b"XY");
    assert_eq!(
        fixture
            .v4_in_dir(
                NfsCacheMode::Off,
                "docs/report.txt",
                (xdr4::OP_WRITE, &write)
            )
            .0,
        xdr4::NFS4ERR_ROFS
    );

    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.uploaded(), 0);
    assert_eq!(fixture.read(&report, 0, 8), b"original");
    assert_eq!(fixture.names_on_backend("docs").await, ["report.txt"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn v4_read_write_and_set_size_go_through_the_cache() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let mut read = Vec::new();
    anonymous_stateid(&mut read);
    xdr4::encode_u64(&mut read, 0);
    xdr4::encode_u32(&mut read, 8);

    let (status, reply) = fixture.v4_in_dir(
        NfsCacheMode::Minimal,
        "docs/report.txt",
        (xdr4::OP_READ, &read),
    );

    assert_eq!(status, xdr4::NFS4_OK);
    assert_eq!(&reply[reply.len() - 8..], b"original");
    let fetched = fixture.log.fetched();
    assert!(
        !fetched.is_empty(),
        "v4 READ did not fetch through the cache"
    );

    let mut write = Vec::new();
    anonymous_stateid(&mut write);
    xdr4::encode_u64(&mut write, 2);
    xdr4::encode_u32(&mut write, 0);
    xdr4::encode_opaque(&mut write, b"XY");
    assert_eq!(
        fixture
            .v4_in_dir(
                NfsCacheMode::Minimal,
                "docs/report.txt",
                (xdr4::OP_WRITE, &write)
            )
            .0,
        xdr4::NFS4_OK
    );
    let mut setattr = Vec::new();
    anonymous_stateid(&mut setattr);
    xdr4::encode_attr_bitmap(&mut setattr, &[xdr4::FATTR4_SIZE]);
    xdr4::encode_u64(&mut setattr, 4);
    assert_eq!(
        fixture
            .v4_in_dir(
                NfsCacheMode::Minimal,
                "docs/report.txt",
                (xdr4::OP_SETATTR, &setattr)
            )
            .0,
        xdr4::NFS4_OK
    );

    assert_eq!(
        fixture.uploaded(),
        0,
        "v4 WRITE or SETATTR uploaded before its reply"
    );
    let (status, reply) = fixture.v4_in_dir(
        NfsCacheMode::Minimal,
        "docs/report.txt",
        (xdr4::OP_READ, &read),
    );
    assert_eq!(status, xdr4::NFS4_OK);
    assert_eq!(&reply[reply.len() - 4..], b"orXY");
    assert_eq!(
        fixture.log.fetched(),
        fetched,
        "a read of cached bytes went to the backend"
    );
    eventually("write-back places the edit", || {
        !fixture.log.placed().is_empty()
    })
    .await;
    assert_eq!(fixture.placed(), [b"orXY".to_vec()]);
    assert_eq!(fixture.on_backend("docs/report.txt").unwrap(), b"orXY");
}

/// `OPEN4_CREATE` in the layout `handle_open` decodes: a bare bitmap follows the open type.
fn open_create(name: &str) -> Vec<u8> {
    let mut args = Vec::new();
    xdr4::encode_u32(&mut args, 0);
    xdr4::encode_u32(&mut args, xdr4::OPEN4_SHARE_ACCESS_BOTH);
    xdr4::encode_u32(&mut args, 0);
    xdr4::encode_u64(&mut args, 1);
    xdr4::encode_opaque(&mut args, b"owner");
    xdr4::encode_u32(&mut args, xdr4::OPEN4_CREATE);
    xdr4::encode_attr_bitmap(&mut args, &[]);
    xdr4::encode_u32(&mut args, 0);
    xdr4::encode_string(&mut args, name);
    args
}

#[tokio::test(flavor = "multi_thread")]
async fn v4_open_create_makes_a_file_that_write_back_uploads() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");

    let (status, _) = fixture.v4_in_dir(
        NfsCacheMode::Minimal,
        "docs",
        (xdr4::OP_OPEN, &open_create("new.txt")),
    );

    assert_eq!(status, xdr4::NFS4_OK);
    let docs = fixture.walk("docs");
    let (lookup, new) = fixture.lookup(&docs, "new.txt");
    assert_eq!(lookup, NFS3_OK, "the created file is not found");
    assert_eq!(fixture.write(&new.unwrap(), 0, b"fresh"), NFS3_OK);
    eventually("write-back uploads the created file", || {
        fixture
            .log
            .placed()
            .iter()
            .any(|placed| placed.data == b"fresh")
    })
    .await;
    assert_eq!(fixture.on_backend("docs/new.txt").unwrap(), b"fresh");
}

#[tokio::test(flavor = "multi_thread")]
async fn v4_open_create_with_a_rejected_name_answers_inval_before_anything_is_applied() {
    let fixture = Fixture::new(TransferConfig::default()).await;
    fixture.mock.add_file("docs/report.txt", b"original");
    let docs = fixture.walk("docs");

    for c in REJECTED {
        let name = format!("bad{c}name");

        let (status, _) = fixture.v4_in_dir(
            NfsCacheMode::Minimal,
            "docs",
            (xdr4::OP_OPEN, &open_create(&name)),
        );

        assert_eq!(status, xdr4::NFS4ERR_INVAL, "v4 OPEN-create {name}");
        assert_eq!(fixture.lookup(&docs, &name).0, NFS3ERR_NOENT, "{name}");
    }
    tokio::time::sleep(QUIET).await;
    assert_eq!(fixture.uploaded(), 0);
    assert_eq!(fixture.names_on_backend("docs").await, ["report.txt"]);
}

/// FR-4, AC-13: a backend mounted at its id, as the daemon mounts it, is
/// served through NFS with and without a content cache attached.
macro_rules! named_mount_tests {
    ($($name:ident => $scenario:ident;)*) => {
        $(
            mod $name {
                #[tokio::test(flavor = "multi_thread")]
                async fn with_a_content_cache() {
                    super::$scenario(true).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                async fn without_a_content_cache() {
                    super::$scenario(false).await;
                }
            }
        )*
    };
}

named_mount_tests! {
    lookup_of_a_named_mount_from_the_root_finds_it => lookup_of_the_mount_from_the_root;
    readdir_of_the_root_lists_a_named_mount => readdir_of_the_root;
    readdir_of_a_named_mount_lists_its_share => readdir_of_the_mount;
    lookup_and_read_of_a_file_under_a_named_mount_return_its_content => lookup_and_read_under_the_mount;
    write_to_a_file_under_a_named_mount_reaches_the_backend => write_under_the_mount;
}

async fn named_mount_with_a_share(cached: bool) -> Fixture {
    let fixture = Fixture::named_backend(cached).await;
    fixture.mock.add_file("sub/hello.txt", b"hello, nas!!");
    fixture.mock.add_file("top.txt", b"top");
    fixture
}

async fn lookup_of_the_mount_from_the_root(cached: bool) {
    let fixture = named_mount_with_a_share(cached).await;

    let (status, fh) = fixture.lookup(&fixture.root_fh(), NAS);

    assert_eq!(status, NFS3_OK, "LOOKUP {NAS} from the root");
    assert_eq!(
        fixture.readdir(&fh.unwrap()).1,
        ["sub", "top.txt"],
        "the handle of {NAS} lists its share"
    );
}

async fn readdir_of_the_root(cached: bool) {
    let fixture = named_mount_with_a_share(cached).await;

    assert_eq!(
        fixture.readdir(&fixture.root_fh()),
        (NFS3_OK, vec![NAS.to_owned()])
    );
}

async fn readdir_of_the_mount(cached: bool) {
    let fixture = named_mount_with_a_share(cached).await;
    let nas = fixture.walk(NAS);

    assert_eq!(
        fixture.readdir(&nas),
        (NFS3_OK, vec!["sub".to_owned(), "top.txt".to_owned()])
    );
}

async fn lookup_and_read_under_the_mount(cached: bool) {
    let fixture = named_mount_with_a_share(cached).await;

    let hello = fixture.walk("nas/sub/hello.txt");

    assert_eq!(fixture.read(&hello, 0, 12), b"hello, nas!!");
}

async fn write_under_the_mount(cached: bool) {
    let fixture = named_mount_with_a_share(cached).await;
    let hello = fixture.walk("nas/sub/hello.txt");

    assert_eq!(fixture.write(&hello, 7, b"NAS"), NFS3_OK);

    assert_eq!(fixture.read(&hello, 0, 12), b"hello, NAS!!");
    eventually("the write reaches the backend", || {
        fixture.on_backend("sub/hello.txt").as_deref() == Some(b"hello, NAS!!".as_slice())
    })
    .await;
}
