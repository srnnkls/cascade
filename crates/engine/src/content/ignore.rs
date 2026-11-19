//! `.cascade` ignore rules over the content cache.
//!
//! An ignored item is not in the replica: listings and scans leave it out,
//! and the rules applied at start remove the items they match, with their
//! objects, leaving tombstones. An item whose subtree holds a dirty or edited
//! object stays until its upload finished. Creates, renames and moves onto an
//! ignored path are refused before anything is applied, and a folder whose
//! backend subtree holds an ignored entry is neither deleted nor moved, nor
//! moved where an entry of its subtree would land on an ignored path. Rules
//! with conditions are left out: the content cache does not evaluate them.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};

use anyhow::Result;
use cascade_config::{IgnoreMatcher, IgnoreRule, ResolvedConfig};

use super::cache::ContentCache;
use super::names::NameError;
use super::replica::{Ignore, ItemKind, Replica, ReplicaItem, is_temp_upload};

/// The ignore rules of the content cache and the mount prefix of each
/// backend they are matched under.
#[derive(Debug, Default)]
pub(super) struct IgnoreRules {
    matcher: Option<Arc<IgnoreMatcher>>,
    prefixes: HashMap<String, PathBuf>,
}

impl IgnoreRules {
    /// The rules as they apply to the backend `backend_id`; `None` without
    /// rules or without a mount prefix for the backend.
    pub(super) fn of(&self, backend_id: &str) -> Option<Ignore> {
        Some(Ignore {
            matcher: Arc::clone(self.matcher.as_ref()?),
            prefix: self.prefixes.get(backend_id)?.clone(),
        })
    }
}

impl ContentCache {
    /// Applies the ignore rules of `config` without conditions to every
    /// registered backend, matched at the mount prefix `mounts` pairs with
    /// the backend's id; a rule with conditions has no effect here.
    /// Called at start before [`recover`](Self::recover). The items the rules
    /// match leave the replica with their objects, leaving tombstones; an
    /// item with a dirty or edited object below it stays until its upload
    /// finished. From then on listings, scans and path resolution leave
    /// ignored items out.
    ///
    /// # Errors
    ///
    /// Fails when the state database or an object file fails.
    pub async fn apply_ignore_rules(
        &self,
        config: &ResolvedConfig,
        mounts: &[(String, PathBuf)],
    ) -> Result<()> {
        let unconditional: Vec<IgnoreRule> = config
            .ignores
            .iter()
            .filter(|rule| rule.conditions.is_empty())
            .cloned()
            .collect();
        let matcher = IgnoreMatcher::new(&unconditional);
        let rules = IgnoreRules {
            matcher: (!matcher.is_empty()).then(|| Arc::new(matcher)),
            prefixes: mounts.iter().cloned().collect(),
        };
        for replica in self.replicas() {
            replica.set_ignore(rules.of(replica.backend_id()));
        }
        *self
            .shared()
            .ignore
            .write()
            .unwrap_or_else(PoisonError::into_inner) = rules;
        self.remove_ignored().await
    }

    /// The ignore rules as they apply to the backend `backend_id`.
    pub(super) fn ignore_of(&self, backend_id: &str) -> Option<Ignore> {
        self.shared()
            .ignore
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .of(backend_id)
    }

    /// Refuses with [`NameError::Ignored`] an item at `path`, relative to the
    /// root of the backend `backend_id`, that an ignore rule matches.
    ///
    /// # Errors
    ///
    /// Fails with [`NameError::Ignored`] for an ignored path.
    pub fn check_path(&self, backend_id: &str, path: &Path, is_dir: bool) -> Result<(), NameError> {
        if self
            .ignore_of(backend_id)
            .is_some_and(|ignore| ignore.ignores(path, is_dir))
        {
            return Err(NameError::Ignored(path.display().to_string()));
        }
        Ok(())
    }

    /// Refuses with [`NameError::Ignored`] an item named `name` in `parent`
    /// that an ignore rule matches.
    pub(super) fn check_child(
        &self,
        replica: &Replica,
        parent: &ReplicaItem,
        name: &str,
        is_dir: bool,
    ) -> Result<()> {
        if let Some(path) = self.item_path(parent.uid)? {
            self.check_path(replica.backend_id(), &path.join(name), is_dir)?;
        }
        Ok(())
    }

    /// Removes the items the ignore rules match once the upload of `uid`
    /// finished, when a rule matches it.
    pub(super) async fn remove_if_ignored(&self, uid: u64) -> Result<()> {
        let (Some(item), Some(path)) = (self.item(uid)?, self.item_path(uid)?) else {
            return Ok(());
        };
        if self
            .replica(uid)?
            .ignore()
            .is_some_and(|ignore| ignore.ignores(&path, item.kind == ItemKind::Folder))
        {
            self.remove_ignored().await?;
        }
        Ok(())
    }

    /// Removes the items the ignore rules match, parents first, with their
    /// objects, leaving tombstones. An item with a dirty or edited object in
    /// its subtree stays, and so do the ignored items above it.
    pub(super) async fn remove_ignored(&self) -> Result<()> {
        for replica in self.replicas() {
            let Some(ignore) = replica.ignore() else {
                continue;
            };
            let mut ignored: Vec<(usize, u64)> = self
                .item_paths(replica.backend_id())?
                .into_iter()
                .filter(|(path, item)| {
                    item.parent_uid.is_some() && ignore.ignores(path, item.kind == ItemKind::Folder)
                })
                .map(|(path, item)| (path.components().count(), item.uid))
                .collect();
            ignored.sort_unstable();
            for (_, uid) in ignored {
                let _operation = self.shared().writeback.lock(uid).await;
                if self.item(uid)?.is_none()
                    || self.db().dirty_items_with_ancestors()?.contains(&uid)
                {
                    continue;
                }
                self.finish_delete(uid).await?;
                tracing::info!(uid, "ignored item removed from the replica");
            }
        }
        Ok(())
    }

    /// The first entry of the backend subtree of `folder`, at `from`, that
    /// an ignore rule matches at its path under `from` or at its path with
    /// the folder at `to`; both paths are relative to the backend root.
    pub(super) async fn ignored_entry(
        &self,
        replica: &Replica,
        folder: &ReplicaItem,
        from: &Path,
        to: &Path,
    ) -> Result<Option<PathBuf>> {
        let Some(ignore) = replica.ignore() else {
            return Ok(None);
        };
        let remote = &self.shared().remote;
        let mut pending = vec![(folder.native_id.clone(), PathBuf::new())];
        while let Some((native_id, relative)) = pending.pop() {
            let children = remote
                .call(replica.backend().list_children(&native_id))
                .await?;
            for entry in children.iter().filter(|entry| !is_temp_upload(&entry.name)) {
                let relative = relative.join(&entry.name);
                for path in [from.join(&relative), to.join(&relative)] {
                    if ignore.ignores(&path, entry.is_dir) {
                        return Ok(Some(path));
                    }
                }
                if entry.is_dir {
                    pending.push((entry.id.native_id().to_owned(), relative));
                }
            }
        }
        Ok(None)
    }
}
