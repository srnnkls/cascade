//! Sync anchors: the store id and revision a change enumeration resumes from.

use anyhow::{Result, anyhow, bail};

/// Upper bound of an encoded anchor.
pub const MAX_ANCHOR_BYTES: usize = 500;

/// Position in the change history of one replica store, encoded as
/// `store_id:revision`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncAnchor {
    store_id: String,
    revision: u64,
}

impl SyncAnchor {
    pub(crate) const fn new(store_id: String, revision: u64) -> Self {
        Self { store_id, revision }
    }

    /// Identity of the store that minted the anchor.
    #[must_use]
    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    /// Revision the anchor stands at.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Encode the anchor as `store_id:revision`.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        format!("{}:{}", self.store_id, self.revision).into_bytes()
    }

    /// Decode an anchor produced by [`SyncAnchor::to_bytes`].
    ///
    /// # Errors
    ///
    /// Fails when `bytes` exceeds [`MAX_ANCHOR_BYTES`] or is not
    /// `store_id:revision`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_ANCHOR_BYTES {
            bail!(
                "sync anchor of {} bytes exceeds {MAX_ANCHOR_BYTES}",
                bytes.len()
            );
        }
        let text = std::str::from_utf8(bytes).map_err(|e| anyhow!("sync anchor: {e}"))?;
        let (store_id, revision) = text
            .rsplit_once(':')
            .filter(|(store_id, _)| !store_id.is_empty())
            .ok_or_else(|| anyhow!("sync anchor {text:?} is not store_id:revision"))?;
        let revision = revision
            .parse()
            .map_err(|e| anyhow!("sync anchor revision {revision:?}: {e}"))?;
        Ok(Self::new(store_id.to_owned(), revision))
    }
}
