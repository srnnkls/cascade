//! Content cache: one sparse object file per cached item, with the fetched
//! byte ranges tracked separately.

use std::fmt;

pub mod anchor;
pub mod range_map;
pub mod replica;
pub mod scan;
#[cfg(unix)]
pub mod store;

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
