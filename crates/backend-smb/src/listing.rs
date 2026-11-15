//! Directory listings and revisions keyed by the server's file identity.

use std::sync::Arc;

use cascade_engine::types::Revision;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use smb::binrw_util::file_time::FileTime;
use smb::{
    Directory, FileAllInformation, FileFsVolumeInformation, FileIdBothDirectoryInformation,
    ResourceHandle,
};

/// A directory entry as listed, with its revision.
pub struct Listed {
    pub name: String,
    pub is_dir: bool,
    pub revision: Revision,
}

pub fn file_time_to_utc(time: FileTime) -> Option<DateTime<Utc>> {
    (!time.is_zero()).then(|| DateTime::<Utc>::from(std::time::SystemTime::from(time)))
}

/// Identity of a file on the server, independent of its name and parent.
fn stable_id(volume_serial: u32, file_id: u64) -> String {
    format!("{volume_serial:08x}-{file_id:016x}")
}

fn revision(
    volume_serial: u32,
    file_id: u64,
    is_dir: bool,
    size: u64,
    last_write_time: FileTime,
    change_time: FileTime,
) -> Revision {
    Revision {
        stable_id: stable_id(volume_serial, file_id),
        size: (!is_dir).then_some(size),
        last_write_time: file_time_to_utc(last_write_time),
        change_time: file_time_to_utc(change_time),
    }
}

async fn volume_serial(handle: &ResourceHandle) -> smb::Result<u32> {
    Ok(handle
        .query_fs_info::<FileFsVolumeInformation>()
        .await?
        .volume_serial_number)
}

/// The entries of `dir`, without `.` and `..`.
pub async fn list(dir: &Arc<Directory>) -> anyhow::Result<Vec<Listed>> {
    let serial = volume_serial(dir).await?;
    let mut entries = Vec::new();
    let mut stream = Directory::query::<FileIdBothDirectoryInformation>(dir, "*").await?;
    while let Some(info) = stream.next().await {
        let info = info?;
        let name = info.file_name.to_string();
        if name == "." || name == ".." {
            continue;
        }
        let is_dir = info.file_attributes.directory();
        entries.push(Listed {
            name,
            is_dir,
            revision: revision(
                serial,
                info.file_id,
                is_dir,
                info.end_of_file,
                info.last_write_time,
                info.change_time,
            ),
        });
    }
    Ok(entries)
}

/// The revision of the open file or directory `handle`, and whether it is a directory.
pub async fn revision_of(handle: &ResourceHandle) -> anyhow::Result<(Revision, bool)> {
    let serial = volume_serial(handle).await?;
    let info = handle.query_info::<FileAllInformation>().await?;
    let is_dir = info.basic.file_attributes.directory();
    Ok((
        revision(
            serial,
            info.internal.index_number,
            is_dir,
            info.standard.end_of_file,
            info.basic.last_write_time,
            info.basic.change_time,
        ),
        is_dir,
    ))
}
