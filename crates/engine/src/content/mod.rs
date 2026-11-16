//! Content cache: one sparse object file per cached item, with the fetched
//! byte ranges tracked separately.

use std::fmt;

use crate::backend::BackendError;

pub mod anchor;
#[cfg(unix)]
pub mod cache;
#[cfg(unix)]
pub mod evict;
#[cfg(all(test, cascade_loom, unix))]
mod loom_model;
pub mod names;
pub mod range_map;
pub mod replica;
pub mod scan;
#[cfg(unix)]
pub mod store;
#[cfg(unix)]
pub mod transfer;
#[cfg(unix)]
pub mod writeback;

/// Id of a cached object: its `content_objects` row and the name of its
/// object file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(u64);

impl ObjectId {
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Whether `error` reports an item missing on the backend.
pub(crate) fn is_not_found(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<BackendError>(),
        Some(BackendError::NotFound(_))
    )
}
