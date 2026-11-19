//! `AppleDouble` companions (`._<name>`), held in memory.
//!
//! The macOS NFS client keeps the extended attributes of `<name>` in them and
//! fails a copy whose companion it cannot create, whatever status the create
//! is refused with, so companions live in memory for the life of the server
//! and never reach a backend or the replica.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use cascade_engine::content::replica::{ItemKind, ReplicaItem};
use chrono::{DateTime, Utc};
use nfs3_types::nfs3::nfsstat3;

/// Whether `name` is an `AppleDouble` companion.
#[must_use]
pub fn is_appledouble(name: &str) -> bool {
    name.starts_with("._")
}

/// Whether the backend item `item` is an `AppleDouble` companion: a regular
/// file with an `AppleDouble` name.
#[must_use]
pub fn is_appledouble_item(item: &ReplicaItem) -> bool {
    item.kind == ItemKind::File && is_appledouble(&item.name)
}

/// The content of one companion.
#[derive(Debug)]
struct Sidecar {
    data: Vec<u8>,
    mtime: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct State {
    next_id: u64,
    ids: HashMap<(u64, String), u64>,
    sidecars: HashMap<u64, Sidecar>,
}

/// The companions of a server, named by an id and by the file id of their
/// folder and their name.
#[derive(Debug, Default)]
pub struct Sidecars {
    state: Mutex<State>,
}

impl Sidecars {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The id of the companion `name` in `folder`.
    pub fn lookup(&self, folder: u64, name: &str) -> Option<u64> {
        self.state().ids.get(&(folder, name.to_owned())).copied()
    }

    /// The id of the companion `name` in `folder`, created empty when
    /// absent; an existing one is [`nfsstat3::NFS3ERR_EXIST`] when
    /// `exclusive`.
    ///
    /// # Errors
    ///
    /// Fails with [`nfsstat3::NFS3ERR_EXIST`] as described.
    pub fn create(&self, folder: u64, name: &str, exclusive: bool) -> Result<u64, nfsstat3> {
        let mut state = self.state();
        let key = (folder, name.to_owned());
        if let Some(id) = state.ids.get(&key) {
            return if exclusive {
                Err(nfsstat3::NFS3ERR_EXIST)
            } else {
                Ok(*id)
            };
        }
        state.next_id = state.next_id.saturating_add(1);
        let id = state.next_id;
        state.ids.insert(key, id);
        state.sidecars.insert(
            id,
            Sidecar {
                data: Vec::new(),
                mtime: Utc::now(),
            },
        );
        Ok(id)
    }

    /// Size and last-write time of the companion `id`.
    pub fn attributes(&self, id: u64) -> Option<(u64, DateTime<Utc>)> {
        self.state().sidecars.get(&id).map(|sidecar| {
            (
                u64::try_from(sidecar.data.len()).unwrap_or(u64::MAX),
                sidecar.mtime,
            )
        })
    }

    /// Up to `count` bytes at `offset` of the companion `id` and whether
    /// they reach its end.
    pub fn read(&self, id: u64, offset: u64, count: u32) -> Option<(Vec<u8>, bool)> {
        let state = self.state();
        let data = &state.sidecars.get(&id)?.data;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(data.len());
        let end = start
            .saturating_add(usize::try_from(count).unwrap_or(usize::MAX))
            .min(data.len());
        Some((data.get(start..end)?.to_vec(), end == data.len()))
    }

    /// Writes `bytes` at `offset` of the companion `id`, extending it with
    /// zeros past its end.
    pub fn write(&self, id: u64, offset: u64, bytes: &[u8]) -> Option<()> {
        let mut state = self.state();
        let sidecar = state.sidecars.get_mut(&id)?;
        let start = usize::try_from(offset).ok()?;
        let end = start.checked_add(bytes.len())?;
        if sidecar.data.len() < end {
            sidecar.data.resize(end, 0);
        }
        sidecar.data.get_mut(start..end)?.copy_from_slice(bytes);
        sidecar.mtime = Utc::now();
        Some(())
    }

    /// Truncates or zero-extends the companion `id` to `size` bytes.
    pub fn set_size(&self, id: u64, size: u64) -> Option<()> {
        let mut state = self.state();
        let sidecar = state.sidecars.get_mut(&id)?;
        sidecar.data.resize(usize::try_from(size).ok()?, 0);
        sidecar.mtime = Utc::now();
        Some(())
    }

    /// Removes the companion `name` in `folder`; `false` when there is none.
    pub fn remove(&self, folder: u64, name: &str) -> bool {
        let mut state = self.state();
        let Some(id) = state.ids.remove(&(folder, name.to_owned())) else {
            return false;
        };
        state.sidecars.remove(&id);
        true
    }

    /// Removes every companion in `folder`.
    pub fn remove_folder(&self, folder: u64) {
        let mut state = self.state();
        let State { ids, sidecars, .. } = &mut *state;
        ids.retain(|(parent, _), id| {
            let kept = *parent != folder;
            if !kept {
                sidecars.remove(id);
            }
            kept
        });
    }

    /// Moves the companion `from_name` in `from` to `to_name` in `to`,
    /// replacing a companion there; `false` when there is none to move.
    pub fn rename(&self, from: u64, from_name: &str, to: u64, to_name: &str) -> bool {
        let mut state = self.state();
        let Some(id) = state.ids.remove(&(from, from_name.to_owned())) else {
            return false;
        };
        if let Some(replaced) = state.ids.insert((to, to_name.to_owned()), id) {
            state.sidecars.remove(&replaced);
        }
        true
    }
}
