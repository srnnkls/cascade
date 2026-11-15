#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use cascade_engine::backend::{Backend, BackendError, TransientError};
use cascade_engine::types::{Change, Cursor, FileEntry, FileId, ItemId, Quota};
use chrono::{DateTime, TimeZone, Utc};

const BACKEND: &str = "mem";

#[derive(Debug, Clone)]
struct Node {
    native_id: String,
    data: Vec<u8>,
    mod_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
enum Fault {
    None,
    Offline,
    TargetHeldOpen,
    UncategorisedNotFound,
}

/// `move_entry` refuses an existing destination.
#[derive(Debug)]
struct MemBackend {
    nodes: Mutex<BTreeMap<String, Node>>,
    next_id: Mutex<u32>,
    fault: Fault,
    uploads: Mutex<Vec<(PathBuf, Vec<u8>, FileId)>>,
}

impl MemBackend {
    const fn new(fault: Fault) -> Self {
        Self {
            nodes: Mutex::new(BTreeMap::new()),
            next_id: Mutex::new(0),
            fault,
            uploads: Mutex::new(Vec::new()),
        }
    }

    fn seed(&self, name: &str, native_id: &str, data: &[u8], mod_time: DateTime<Utc>) {
        self.nodes.lock().unwrap().insert(
            name.to_string(),
            Node {
                native_id: native_id.to_string(),
                data: data.to_vec(),
                mod_time,
            },
        );
    }

    fn mint_id(&self) -> String {
        let mut next = self.next_id.lock().unwrap();
        *next += 1;
        format!("minted-{next}")
    }

    fn entry(name: &str, node: &Node) -> FileEntry {
        let mut entry = FileEntry::file(
            ItemId::new(BACKEND, &node.native_id),
            ItemId::new(BACKEND, "root"),
            name.to_string(),
        );
        entry.size = Some(node.data.len() as u64);
        entry.mod_time = Some(node.mod_time);
        entry
    }

    fn reachable(&self) -> anyhow::Result<()> {
        if matches!(self.fault, Fault::Offline) {
            return Err(TransientError::Unreachable("server offline".into()).into());
        }
        Ok(())
    }

    fn key(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    fn name_of(&self, native_id: &str) -> Option<String> {
        self.nodes
            .lock()
            .unwrap()
            .iter()
            .find(|(_, node)| node.native_id == native_id)
            .map(|(name, _)| name.clone())
    }
}

fn at(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

#[async_trait]
impl Backend for MemBackend {
    fn id(&self) -> &'static str {
        BACKEND
    }

    fn display_name(&self) -> &'static str {
        "Memory"
    }

    async fn quota(&self) -> anyhow::Result<Option<Quota>> {
        Ok(None)
    }

    async fn changes(&self, _cursor: Option<&Cursor>) -> anyhow::Result<(Vec<Change>, Cursor)> {
        Ok((vec![], Cursor("mem".to_string())))
    }

    async fn metadata(&self, path: &Path) -> anyhow::Result<FileEntry> {
        self.reachable()?;
        let key = Self::key(path);
        let nodes = self.nodes.lock().unwrap();
        let Some(node) = nodes.get(&key) else {
            if matches!(self.fault, Fault::UncategorisedNotFound) {
                anyhow::bail!("no such file: {key}");
            }
            return Err(BackendError::NotFound(key).into());
        };
        Ok(Self::entry(&key, node))
    }

    async fn download(&self, file: &FileEntry) -> anyhow::Result<Vec<u8>> {
        self.reachable()?;
        let nodes = self.nodes.lock().unwrap();
        nodes
            .values()
            .find(|node| node.native_id == file.id.native_id())
            .map(|node| node.data.clone())
            .ok_or_else(|| BackendError::NotFound(file.id.to_string()).into())
    }

    async fn upload(
        &self,
        path: &Path,
        data: &[u8],
        parent_id: &FileId,
    ) -> anyhow::Result<FileEntry> {
        self.reachable()?;
        self.uploads
            .lock()
            .unwrap()
            .push((path.to_path_buf(), data.to_vec(), parent_id.clone()));
        let node = Node {
            native_id: self.mint_id(),
            data: data.to_vec(),
            mod_time: at(2_000),
        };
        let key = Self::key(path);
        let entry = Self::entry(&key, &node);
        self.nodes.lock().unwrap().insert(key, node);
        Ok(entry)
    }

    async fn update(&self, file_id: &FileId, data: &[u8]) -> anyhow::Result<FileEntry> {
        self.reachable()?;
        let name = self
            .name_of(file_id.native_id())
            .ok_or_else(|| BackendError::NotFound(file_id.to_string()))?;
        let mut nodes = self.nodes.lock().unwrap();
        let node = nodes.get_mut(&name).unwrap();
        node.data = data.to_vec();
        node.mod_time = at(3_000);
        Ok(Self::entry(&name, node))
    }

    async fn create_dir(&self, _path: &Path) -> anyhow::Result<FileEntry> {
        anyhow::bail!("directories unsupported")
    }

    async fn delete(&self, file: &FileEntry) -> anyhow::Result<()> {
        self.reachable()?;
        if matches!(self.fault, Fault::TargetHeldOpen) {
            return Err(TransientError::Busy(file.name.clone()).into());
        }
        let name = self
            .name_of(file.id.native_id())
            .ok_or_else(|| BackendError::NotFound(file.id.to_string()))?;
        self.nodes.lock().unwrap().remove(&name);
        Ok(())
    }

    async fn move_entry(&self, src: &Path, dst: &Path) -> anyhow::Result<FileEntry> {
        self.reachable()?;
        let (src, dst) = (Self::key(src), Self::key(dst));
        let mut nodes = self.nodes.lock().unwrap();
        if nodes.contains_key(&dst) {
            return Err(BackendError::Conflict(dst).into());
        }
        let node = nodes
            .remove(&src)
            .ok_or_else(|| BackendError::NotFound(src.clone()))?;
        let entry = Self::entry(&dst, &node);
        nodes.insert(dst, node);
        Ok(entry)
    }

    async fn list_children(&self, parent_native_id: &str) -> anyhow::Result<Vec<FileEntry>> {
        self.reachable()?;
        if parent_native_id != "root" {
            return Ok(vec![]);
        }
        let nodes = self.nodes.lock().unwrap();
        Ok(nodes
            .iter()
            .map(|(name, node)| Self::entry(name, node))
            .collect())
    }

    async fn poll_interval(&self) -> Option<Duration> {
        None
    }
}

#[tokio::test]
async fn default_replace_renames_source_over_existing_target() {
    let backend = MemBackend::new(Fault::None);
    backend.seed("report.docx", "target-id", b"old content", at(100));
    backend.seed("report.docx.tmp", "source-id", b"new", at(200));

    let (entry, revision) = backend
        .replace(Path::new("report.docx.tmp"), Path::new("report.docx"))
        .await
        .unwrap();

    assert_eq!(entry.name, "report.docx");
    assert_eq!(entry.id, ItemId::new(BACKEND, "source-id"));
    assert_eq!(revision.stable_id, "source-id");
    assert_eq!(revision.size, Some(3));
    assert_eq!(revision.last_write_time, Some(at(200)));
    assert_eq!(backend.download(&entry).await.unwrap(), b"new");

    let source_gone = backend
        .metadata(Path::new("report.docx.tmp"))
        .await
        .unwrap_err();
    assert!(matches!(
        source_gone.downcast_ref::<BackendError>(),
        Some(BackendError::NotFound(_))
    ));
    let names: Vec<String> = backend
        .list_children("root")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, ["report.docx"]);
}

#[tokio::test]
async fn transient_errors_survive_default_methods_as_downcastable_categories() {
    let held_open = MemBackend::new(Fault::TargetHeldOpen);
    held_open.seed("report.docx", "target-id", b"old content", at(100));
    held_open.seed("report.docx.tmp", "source-id", b"new", at(200));
    let busy = held_open
        .replace(Path::new("report.docx.tmp"), Path::new("report.docx"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            busy.downcast_ref::<TransientError>(),
            Some(TransientError::Busy(_))
        ),
        "replace over a target held open must surface TransientError::Busy, got {busy:?}"
    );
    let source = held_open
        .metadata(Path::new("report.docx.tmp"))
        .await
        .unwrap();
    assert_eq!(source.id, ItemId::new(BACKEND, "source-id"));

    let offline = MemBackend::new(Fault::Offline);
    let unreachable = offline
        .list_children_with_revisions("root")
        .await
        .unwrap_err();
    assert!(
        matches!(
            unreachable.downcast_ref::<TransientError>(),
            Some(TransientError::Unreachable(_))
        ),
        "an offline listing must surface TransientError::Unreachable, got {unreachable:?}"
    );
}

#[tokio::test]
async fn default_path_uploads_send_file_contents_and_listing_reports_revisions() {
    let backend = MemBackend::new(Fault::None);
    backend.seed("existing.txt", "existing-id", b"v1", at(100));
    let dir = tempfile::tempdir().unwrap();
    let staged = dir.path().join("staged");
    std::fs::write(&staged, b"uploaded bytes").unwrap();

    let parent = FileId("mem:root".to_string());
    let created = backend
        .upload_from_path(Path::new("new.txt"), &staged, &parent)
        .await
        .unwrap();
    assert_eq!(created.name, "new.txt");
    let uploads = backend.uploads.lock().unwrap().clone();
    assert_eq!(
        uploads,
        [(PathBuf::from("new.txt"), b"uploaded bytes".to_vec(), parent)]
    );

    std::fs::write(&staged, b"version two").unwrap();
    let updated = backend
        .update_from_path(&FileId("mem:existing-id".to_string()), &staged)
        .await
        .unwrap();
    assert_eq!(backend.download(&updated).await.unwrap(), b"version two");

    let listed = backend.list_children_with_revisions("root").await.unwrap();
    let summary: Vec<(String, String, Option<u64>, Option<DateTime<Utc>>)> = listed
        .into_iter()
        .map(|(entry, revision)| {
            (
                entry.name,
                revision.stable_id,
                revision.size,
                revision.last_write_time,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (
                "existing.txt".to_string(),
                "existing-id".to_string(),
                Some(11),
                Some(at(3_000))
            ),
            (
                "new.txt".to_string(),
                "minted-1".to_string(),
                Some(14),
                Some(at(2_000))
            ),
        ]
    );

    let revision = backend.revision(&created).await.unwrap();
    assert_eq!(revision.stable_id, "minted-1");
    assert_eq!(revision.size, Some(14));
}

#[tokio::test]
async fn default_replace_moves_source_when_target_lookup_fails_uncategorised() {
    let backend = MemBackend::new(Fault::UncategorisedNotFound);
    backend.seed("report.docx.tmp", "source-id", b"new", at(200));

    let (entry, revision) = backend
        .replace(Path::new("report.docx.tmp"), Path::new("report.docx"))
        .await
        .unwrap();

    assert_eq!(entry.name, "report.docx");
    assert_eq!(revision.stable_id, "source-id");
    let names: Vec<String> = backend
        .list_children("root")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, ["report.docx"]);
}
